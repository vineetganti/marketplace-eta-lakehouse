-- SILVER pings: event-time source -> (a) deduplicated silver table, (b) late-arrival audit table.
-- Both sinks share ONE source + ONE watermark, so "late" is judged against the same
-- watermark the gold windows use.
SET 'pipeline.name' = 'silver-pings';

-- Dedup state is keyed on (driver_id, seq) and must be bounded. TTL trades memory for the
-- longest duplicate-delay you can still catch. Replayer injects dups up to 30 s apart; real
-- producer retries are seconds, replays can be hours. Pick consciously (DESIGN.md section 5).
SET 'table.exec.state.ttl' = '2 h';

CREATE TEMPORARY TABLE src_pings (
  `driver_id`   BIGINT,
  `seq`         BIGINT,
  `event_ts_ms` BIGINT,
  `lat`         DOUBLE,
  `lon`         DOUBLE,
  `speed_kph`   DOUBLE,
  `event_time`  AS TO_TIMESTAMP_LTZ(`event_ts_ms`, 3),
  `proc_time`   AS PROCTIME(),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'driver.pings',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-silver-pings',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

EXECUTE STATEMENT SET
BEGIN

  -- (a) keep-first dedup on (driver_id, seq). Late-but-unique pings ARE kept: the lake is the
  -- complete record; lateness only matters for the streaming gold windows.
  INSERT INTO lakehouse.silver.driver_pings
  SELECT
    `driver_id`,
    `seq`,
    CAST(`event_time` AS TIMESTAMP(3)),
    `lat`,
    `lon`,
    h3_cell(`lat`, `lon`, 8),
    `speed_kph`,
    CAST(CAST(`event_time` AS TIMESTAMP(3)) AS DATE)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY `driver_id`, `seq` ORDER BY `proc_time` ASC) AS rn
    FROM src_pings
  )
  WHERE rn = 1;

  -- (b) pings whose 1-minute window had already closed when they arrived (i.e. the gold
  -- window job would drop them). Approximation: window_end <= current watermark.
  -- Verify empirically with Experiment E2 (docs/EXPERIMENTS.md).
  INSERT INTO lakehouse.silver.driver_pings_late
  SELECT
    `driver_id`,
    `seq`,
    CAST(`event_time` AS TIMESTAMP(3)),
    `lat`,
    `lon`,
    CAST(CURRENT_WATERMARK(`event_time`) AS TIMESTAMP(3)),
    CAST(TIMESTAMPDIFF(SECOND,
           CAST(`event_time` AS TIMESTAMP(3)),
           CAST(CURRENT_WATERMARK(`event_time`) AS TIMESTAMP(3))) AS INT),
    CAST(CAST(`event_time` AS TIMESTAMP(3)) AS DATE)
  FROM src_pings
  WHERE CURRENT_WATERMARK(`event_time`) IS NOT NULL
    AND TIMESTAMPADD(MINUTE, 1, FLOOR(CAST(`event_time` AS TIMESTAMP(3)) TO MINUTE))
          <= CAST(CURRENT_WATERMARK(`event_time`) AS TIMESTAMP(3));

END;
