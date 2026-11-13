-- GOLD: event-time tumbling windows (1 min) per H3 res-8 cell.
--   demand = trip requests created in the window     (from CDC inserts, op = 'c')
--   supply = distinct drivers that pinged in the window
--
-- Why COUNT(DISTINCT driver_id) for supply and no dedup operator here: the aggregate is
-- naturally idempotent under duplicate pings, so gold needs no dedup state at all.
-- Why op = 'c' for demand: windows need an append-only input; the debezium-json changelog
-- also carries updates. Plain-JSON envelope + filter gives exactly one row per new trip.
--
-- Watermark = max(event_time) - 30 s. A ping/request later than that falls into a window that
-- has already fired and is dropped from THIS table (but kept in silver). The nightly batch job
-- (batch/40_reconcile_gold.sql) rewrites closed partitions from silver. That is the
-- Kappa-with-repair decision in ADR-0001.
SET 'pipeline.name' = 'gold-demand-supply';

CREATE TEMPORARY TABLE src_trip_envelope (
  `op`    STRING,
  `ts_ms` BIGINT,
  `after` ROW<
            `trip_id`      BIGINT,
            `pickup_lat`   DOUBLE,
            `pickup_lon`   DOUBLE,
            `requested_at` TIMESTAMP_LTZ(3)>,
  -- `after` is NULL on deletes; fall back to connector time so the rowtime is never NULL.
  `requested_time` AS COALESCE(`after`.`requested_at`, TO_TIMESTAMP_LTZ(`ts_ms`, 3)),
  WATERMARK FOR `requested_time` AS `requested_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg.public.trips',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-gold-demand',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'false'
);

CREATE TEMPORARY TABLE src_pings (
  `driver_id`   BIGINT,
  `seq`         BIGINT,
  `event_ts_ms` BIGINT,
  `lat`         DOUBLE,
  `lon`         DOUBLE,
  `speed_kph`   DOUBLE,
  `event_time`  AS TO_TIMESTAMP_LTZ(`event_ts_ms`, 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'driver.pings',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-gold-supply',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TEMPORARY VIEW v_requests AS
SELECT
  `requested_time`,
  h3_cell(`after`.`pickup_lat`, `after`.`pickup_lon`, 8) AS `h3_r8`
FROM src_trip_envelope
WHERE `op` = 'c';

CREATE TEMPORARY VIEW v_pings AS
SELECT
  `event_time`,
  `driver_id`,
  h3_cell(`lat`, `lon`, 8) AS `h3_r8`
FROM src_pings;

EXECUTE STATEMENT SET
BEGIN

  INSERT INTO lakehouse.gold.h3_demand_1m
  SELECT
    window_start,
    window_end,
    `h3_r8`,
    COUNT(*),
    CAST(window_start AS DATE)
  FROM TABLE(TUMBLE(TABLE v_requests, DESCRIPTOR(`requested_time`), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, `h3_r8`;

  INSERT INTO lakehouse.gold.h3_supply_1m
  SELECT
    window_start,
    window_end,
    `h3_r8`,
    COUNT(DISTINCT `driver_id`),
    CAST(window_start AS DATE)
  FROM TABLE(TUMBLE(TABLE v_pings, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, `h3_r8`;

END;
