-- Table maintenance + small-file diagnostics. Run statements one at a time in `.\tasks trino`.
-- Flink commits once per checkpoint (30 s) per sink => lots of tiny files and snapshots.
-- Record BEFORE/AFTER numbers in docs/EXPERIMENTS.md (E7).

-- ---------------------------------------------------------------- diagnose
SELECT count(*)                          AS files,
       round(avg(file_size_in_bytes) / 1024.0, 1) AS avg_kb,
       sum(record_count)                 AS records
FROM iceberg.silver."driver_pings$files";

SELECT count(*) AS snapshots FROM iceberg.silver."driver_pings$snapshots";

SELECT committed_at, operation,
       element_at(summary, 'added-data-files')  AS added_files,
       element_at(summary, 'added-records')     AS added_records
FROM iceberg.silver."driver_pings$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- Upsert tables also accumulate DELETE files (equality deletes written by Flink).
SELECT content, count(*) AS files   -- 0 = data, 1 = position deletes, 2 = equality deletes
FROM iceberg.silver."trips$files"
GROUP BY content;

-- ---------------------------------------------------------------- compact
-- Append-only table: straightforward.
ALTER TABLE iceberg.silver.driver_pings EXECUTE optimize(file_size_threshold => '64MB');

-- Upsert table (equality deletes): OPEN QUESTION (E7). Try it; if Trino refuses or leaves the
-- equality deletes in place, the fallback is Spark's rewrite_data_files / Flink's
-- RewriteDataFilesAction (see docs/PLAN.md, M7 stretch).
-- ALTER TABLE iceberg.silver.trips EXECUTE optimize(file_size_threshold => '64MB');

-- ---------------------------------------------------------------- expire + clean
-- Retention is 1 minute here only because iceberg.properties lowers the safety floor.
ALTER TABLE iceberg.silver.driver_pings EXECUTE expire_snapshots(retention_threshold => '1h');
ALTER TABLE iceberg.silver.driver_pings EXECUTE remove_orphan_files(retention_threshold => '1h');

-- Time travel still works for retained snapshots; after expiry it must fail. Prove it:
-- SELECT count(*) FROM iceberg.silver.driver_pings FOR VERSION AS OF <expired_snapshot_id>;
