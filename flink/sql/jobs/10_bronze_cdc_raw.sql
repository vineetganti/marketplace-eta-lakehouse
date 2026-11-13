-- BRONZE: land every Debezium event for all pg.public.* topics, byte-for-byte, with Kafka
-- coordinates. No parsing here on purpose: a parsing bug must never lose raw data.
SET 'pipeline.name' = 'bronze-cdc-raw';

CREATE TEMPORARY TABLE src_cdc_raw (
  `topic`           STRING          METADATA VIRTUAL,
  `kafka_partition` INT             METADATA FROM 'partition' VIRTUAL,
  `kafka_offset`    BIGINT          METADATA FROM 'offset'    VIRTUAL,
  `kafka_ts`        TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  `payload`         STRING
) WITH (
  'connector' = 'kafka',
  'topic-pattern' = 'pg\.public\..*',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-bronze-cdc',
  'scan.startup.mode' = 'earliest-offset',
  'scan.topic-partition-discovery.interval' = '60 s',
  'format' = 'raw'
);

INSERT INTO lakehouse.bronze.cdc_raw
SELECT
  `topic`,
  `kafka_partition`,
  `kafka_offset`,
  CAST(`kafka_ts` AS TIMESTAMP(3)),
  `payload`,
  CAST(CAST(`kafka_ts` AS TIMESTAMP(3)) AS DATE)
FROM src_cdc_raw
WHERE `payload` IS NOT NULL;
