-- SILVER dimension version logs (inputs to SCD2). Append-only: one row per CDC change.
-- Reads the Debezium envelope as plain JSON (not debezium-json) so we keep op + source LSN.
-- Duplicates from Debezium restarts are collapsed in the Trino SCD2 view, not here, because
-- deleting history inside the stream is how you lose data silently. See ADR-0003.
SET 'pipeline.name' = 'silver-dim-versions';

CREATE TEMPORARY TABLE src_drivers (
  `op`     STRING,
  `after`  ROW<
             `driver_id`    BIGINT,
             `vehicle_type` STRING,
             `city`         STRING,
             `rating`       DOUBLE,
             `status`       STRING,
             `updated_at`   TIMESTAMP_LTZ(3)>,
  `source` ROW<`lsn` BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg.public.drivers',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-silver-drivers',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'false'
);

CREATE TEMPORARY TABLE src_merchants (
  `op`     STRING,
  `after`  ROW<
             `merchant_id`    BIGINT,
             `category`       STRING,
             `tier`           STRING,
             `city`           STRING,
             `lat`            DOUBLE,
             `lon`            DOUBLE,
             `commission_pct` DOUBLE,
             `updated_at`     TIMESTAMP_LTZ(3)>,
  `source` ROW<`lsn` BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg.public.merchants',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-silver-merchants',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'false'
);

EXECUTE STATEMENT SET
BEGIN

  -- op: r = snapshot read, c = insert, u = update. Deletes (d) have after = NULL and are not
  -- modeled yet (soft-delete via status='suspended' instead). Exercise: handle op = 'd'.
  INSERT INTO lakehouse.silver.driver_versions
  SELECT
    `after`.`driver_id`,
    CAST(`after`.`updated_at` AS TIMESTAMP(3)),
    `op`,
    `after`.`vehicle_type`,
    `after`.`city`,
    `after`.`rating`,
    `after`.`status`,
    `source`.`lsn`,
    CAST(CAST(`after`.`updated_at` AS TIMESTAMP(3)) AS DATE)
  FROM src_drivers
  WHERE `op` IN ('r', 'c', 'u') AND `after`.`driver_id` IS NOT NULL;

  INSERT INTO lakehouse.silver.merchant_versions
  SELECT
    `after`.`merchant_id`,
    CAST(`after`.`updated_at` AS TIMESTAMP(3)),
    `op`,
    `after`.`category`,
    `after`.`tier`,
    `after`.`city`,
    `after`.`lat`,
    `after`.`lon`,
    h3_cell(`after`.`lat`, `after`.`lon`, 8),
    `after`.`commission_pct`,
    `source`.`lsn`,
    CAST(CAST(`after`.`updated_at` AS TIMESTAMP(3)) AS DATE)
  FROM src_merchants
  WHERE `op` IN ('r', 'c', 'u') AND `after`.`merchant_id` IS NOT NULL;

END;
