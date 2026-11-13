-- Loaded by every sql-client session (-i). Session-scoped: catalog + UDF registration.
-- Iceberg TABLES live in the REST catalog and persist; this only (re)attaches to them.

SET 'execution.runtime-mode' = 'streaming';
-- Store/compare every timestamp in UTC. Mixed TIMESTAMP vs TIMESTAMP_LTZ is the #1 source
-- of "my windows are off by 5.5 hours" bugs (IST = UTC+5:30 on your laptop).
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- Replayer runs far faster than wall clock; idle Kafka partitions must not stall watermarks.
SET 'table.exec.source.idle-timeout' = '30 s';

CREATE CATALOG lakehouse WITH (
  'type'                  = 'iceberg',
  'catalog-type'          = 'rest',
  'uri'                   = 'http://iceberg-rest:8181',
  'warehouse'             = 's3://warehouse/',
  'io-impl'               = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'           = 'http://minio:9000',
  's3.path-style-access'  = 'true',
  'client.region'         = 'us-east-1',
  's3.access-key-id'      = 'minioadmin',
  's3.secret-access-key'  = 'minioadmin'
);

CREATE TEMPORARY SYSTEM FUNCTION h3_cell AS 'dev.lakehouse.udf.H3Cell' LANGUAGE JAVA;
