-- Run once: .\tasks ddl
-- All tables are Iceberg format-version 2 (required for row-level deletes / upserts).
--
-- Streaming sinks commit one snapshot per checkpoint (30 s => ~2,880 snapshots/day/table).
-- Without the two metadata properties below, metadata.json files pile up forever.

CREATE DATABASE IF NOT EXISTS lakehouse.bronze;
CREATE DATABASE IF NOT EXISTS lakehouse.silver;
CREATE DATABASE IF NOT EXISTS lakehouse.gold;

-- ---------------------------------------------------------------- BRONZE
-- Raw Debezium envelopes, untouched. This is your replay source for backfills.
CREATE TABLE IF NOT EXISTS lakehouse.bronze.cdc_raw (
  `topic`           STRING,
  `kafka_partition` INT,
  `kafka_offset`    BIGINT,
  `kafka_ts`        TIMESTAMP(3),
  `payload`         STRING,
  `ingest_date`     DATE
) PARTITIONED BY (`ingest_date`) WITH (
  'format-version' = '2',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20'
);

-- ---------------------------------------------------------------- SILVER
-- Latest state per trip. UPSERT table.
-- Iceberg/Flink rule: the PK must include every partition source column, and the partition
-- column must be IMMUTABLE per key (requested_date never changes for a trip). Otherwise the
-- equality delete lands in the wrong partition and you get duplicate "latest" rows. See ADR-0002.
CREATE TABLE IF NOT EXISTS lakehouse.silver.trips (
  `trip_id`             BIGINT,
  `requested_date`      DATE,
  `rider_id`            BIGINT,
  `driver_id`           BIGINT,
  `merchant_id`         BIGINT,
  `service_type`        STRING,
  `status`              STRING,
  `pickup_lat`          DOUBLE,
  `pickup_lon`          DOUBLE,
  `dropoff_lat`         DOUBLE,
  `dropoff_lon`         DOUBLE,
  `pickup_h3`           STRING,
  `dropoff_h3`          STRING,
  `requested_at`        TIMESTAMP(3),
  `accepted_at`         TIMESTAMP(3),
  `picked_up_at`        TIMESTAMP(3),
  `completed_at`        TIMESTAMP(3),
  `quoted_eta_sec`      INT,
  `actual_duration_sec` INT,
  `fare_amount`         DOUBLE,
  `distance_km`         DOUBLE,
  `updated_at`          TIMESTAMP(3),
  PRIMARY KEY (`trip_id`, `requested_date`) NOT ENFORCED
) PARTITIONED BY (`requested_date`) WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'true',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20'
);

-- Deduplicated, append-only GPS pings. Event-date partitioned.
CREATE TABLE IF NOT EXISTS lakehouse.silver.driver_pings (
  `driver_id`  BIGINT,
  `seq`        BIGINT,
  `event_time` TIMESTAMP(3),
  `lat`        DOUBLE,
  `lon`        DOUBLE,
  `h3_r8`      STRING,
  `speed_kph`  DOUBLE,
  `event_date` DATE
) PARTITIONED BY (`event_date`) WITH (
  'format-version' = '2',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20'
);

-- Pings that arrived after their 1-minute window already closed (see 21_silver_pings.sql).
-- Audit trail + the work-list for targeted reconciliation of gold.
CREATE TABLE IF NOT EXISTS lakehouse.silver.driver_pings_late (
  `driver_id`    BIGINT,
  `seq`          BIGINT,
  `event_time`   TIMESTAMP(3),
  `lat`          DOUBLE,
  `lon`          DOUBLE,
  `watermark_at` TIMESTAMP(3),
  `late_by_sec`  INT,
  `event_date`   DATE
) PARTITIONED BY (`event_date`) WITH ('format-version' = '2');

-- Append-only version logs for SCD2 dimensions (SCD2 itself is a Trino view; see ADR-0003).
CREATE TABLE IF NOT EXISTS lakehouse.silver.driver_versions (
  `driver_id`    BIGINT,
  `version_ts`   TIMESTAMP(3),
  `op`           STRING,
  `vehicle_type` STRING,
  `city`         STRING,
  `rating`       DOUBLE,
  `status`       STRING,
  `source_lsn`   BIGINT,
  `version_date` DATE
) PARTITIONED BY (`version_date`) WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.silver.merchant_versions (
  `merchant_id`    BIGINT,
  `version_ts`     TIMESTAMP(3),
  `op`             STRING,
  `category`       STRING,
  `tier`           STRING,
  `city`           STRING,
  `lat`            DOUBLE,
  `lon`            DOUBLE,
  `h3_r8`          STRING,
  `commission_pct` DOUBLE,
  `source_lsn`     BIGINT,
  `version_date`   DATE
) PARTITIONED BY (`version_date`) WITH ('format-version' = '2');

-- ---------------------------------------------------------------- GOLD
-- 1-minute tumbling windows per H3 res-8 cell. Append-only (window emitted once, at watermark).
-- Late data is NOT folded in by the streaming job; the batch reconcile job repairs it.
CREATE TABLE IF NOT EXISTS lakehouse.gold.h3_demand_1m (
  `window_start` TIMESTAMP(3),
  `window_end`   TIMESTAMP(3),
  `h3_r8`        STRING,
  `requests`     BIGINT,
  `window_date`  DATE
) PARTITIONED BY (`window_date`) WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.h3_supply_1m (
  `window_start`   TIMESTAMP(3),
  `window_end`     TIMESTAMP(3),
  `h3_r8`          STRING,
  `active_drivers` BIGINT,
  `window_date`    DATE
) PARTITIONED BY (`window_date`) WITH ('format-version' = '2');
