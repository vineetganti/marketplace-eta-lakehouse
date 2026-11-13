-- SILVER trips: Debezium changelog -> Iceberg UPSERT (latest state per trip).
-- Delivery chain: Postgres WAL -> Debezium (at-least-once) -> Redpanda -> Flink (exactly-once
-- state) -> Iceberg two-phase commit on checkpoint. Re-delivered CDC events are harmless here
-- because an upsert of the same (trip_id, row-state) is idempotent.
SET 'pipeline.name' = 'silver-trips-upsert';

CREATE TEMPORARY TABLE src_trips (
  `trip_id`        BIGINT,
  `rider_id`       BIGINT,
  `driver_id`      BIGINT,
  `merchant_id`    BIGINT,
  `service_type`   STRING,
  `status`         STRING,
  `pickup_lat`     DOUBLE,
  `pickup_lon`     DOUBLE,
  `dropoff_lat`    DOUBLE,
  `dropoff_lon`    DOUBLE,
  `requested_at`   TIMESTAMP_LTZ(3),
  `accepted_at`    TIMESTAMP_LTZ(3),
  `picked_up_at`   TIMESTAMP_LTZ(3),
  `completed_at`   TIMESTAMP_LTZ(3),
  `quoted_eta_sec` INT,
  `fare_amount`    DOUBLE,
  `distance_km`    DOUBLE,
  `updated_at`     TIMESTAMP_LTZ(3),
  PRIMARY KEY (`trip_id`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg.public.trips',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-silver-trips',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json',
  'debezium-json.timestamp-format.standard' = 'ISO-8601',
  'debezium-json.ignore-parse-errors' = 'false'
);

CREATE TEMPORARY VIEW v_trips AS
SELECT
  `trip_id`,
  CAST(`requested_at` AS DATE)                    AS `requested_date`,
  `rider_id`,
  `driver_id`,
  `merchant_id`,
  `service_type`,
  `status`,
  `pickup_lat`,
  `pickup_lon`,
  `dropoff_lat`,
  `dropoff_lon`,
  h3_cell(`pickup_lat`,  `pickup_lon`,  8)        AS `pickup_h3`,
  h3_cell(`dropoff_lat`, `dropoff_lon`, 8)        AS `dropoff_h3`,
  CAST(`requested_at` AS TIMESTAMP(3))            AS `requested_at`,
  CAST(`accepted_at`  AS TIMESTAMP(3))            AS `accepted_at`,
  CAST(`picked_up_at` AS TIMESTAMP(3))            AS `picked_up_at`,
  CAST(`completed_at` AS TIMESTAMP(3))            AS `completed_at`,
  `quoted_eta_sec`,
  CAST(TIMESTAMPDIFF(SECOND,
         CAST(`picked_up_at` AS TIMESTAMP(3)),
         CAST(`completed_at` AS TIMESTAMP(3))) AS INT) AS `actual_duration_sec`,
  `fare_amount`,
  `distance_km`,
  CAST(`updated_at` AS TIMESTAMP(3))              AS `updated_at`
FROM src_trips;

INSERT INTO lakehouse.silver.trips
SELECT * FROM v_trips;
