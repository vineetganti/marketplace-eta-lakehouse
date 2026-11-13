-- Ad-hoc correctness + freshness probes. For the automated version see scripts/reconcile.py.

-- 1. Duplicate pings in silver. MUST be empty.
SELECT driver_id, seq, count(*) AS n
FROM iceberg.silver.driver_pings
GROUP BY 1, 2
HAVING count(*) > 1
LIMIT 20;

-- 2. More than one "latest" row per trip. MUST be empty (upsert/partitioning bug detector, ADR-0002).
SELECT trip_id, count(*) AS n
FROM iceberg.silver.trips
GROUP BY 1
HAVING count(*) > 1
LIMIT 20;

-- 3. Raw CDC duplicates in bronze (same payload delivered twice, e.g. after connector restart).
SELECT topic, count(*) AS events, count(DISTINCT payload) AS distinct_payloads
FROM iceberg.bronze.cdc_raw
GROUP BY 1;

-- 4. How late is "late"? Distribution of lateness for pings the windows dropped.
SELECT width_bucket(late_by_sec, ARRAY[0, 60, 120, 300, 600, 1200]) AS bucket,
       count(*) AS pings,
       max(late_by_sec) AS max_late_s
FROM iceberg.silver.driver_pings_late
GROUP BY 1
ORDER BY 1;

-- 5. Commit cadence = end-to-end freshness floor (one snapshot per Flink checkpoint).
SELECT committed_at,
       lag(committed_at) OVER (ORDER BY committed_at) AS prev_commit,
       element_at(summary, 'added-records') AS added_records
FROM iceberg.silver."driver_pings$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- 6. Time travel: row count 10 minutes ago vs now (proves snapshot isolation + retention).
-- SELECT count(*) FROM iceberg.silver.driver_pings FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '10' MINUTE);

-- 7. Hottest cells: key skew check for H3 partitioning decisions.
SELECT h3_r8, sum(requests) AS requests
FROM iceberg.gold.h3_demand_1m
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
