-- ETA baseline + accuracy. The point is the PIPELINE (as-of data, leakage-free split), not
-- state-of-the-art ETA modelling.
--
-- Train/eval split by TIME. Default synthetic run = 2025-01-15 00:00-02:00Z, so train on the
-- first hour, evaluate on the second. For TLC multi-day runs, move the cut to a day boundary.
-- Evaluating on rows you trained on (in-sample) flatters the baseline: don't.

-- 1. Baseline: median seconds-per-km by hour of day, from the TRAIN window only.
CREATE TABLE IF NOT EXISTS iceberg.gold.eta_baseline_hour AS
SELECT hour(requested_at)                                     AS hour_of_day,
       approx_percentile(actual_duration_sec / distance_km, 0.5) AS p50_sec_per_km,
       count(*)                                               AS n
FROM iceberg.silver.trips
WHERE status = 'completed'
  AND requested_at <  TIMESTAMP '2025-01-15 01:00:00'
GROUP BY 1;

-- 2. Accuracy on the EVAL window: quoted ETA (what the rider saw) vs baseline.
WITH eval AS (
  SELECT t.trip_id,
         CAST(t.actual_duration_sec AS DOUBLE)              AS actual,
         CAST(t.quoted_eta_sec AS DOUBLE)                   AS quoted,
         t.distance_km * b.p50_sec_per_km                   AS baseline
  FROM iceberg.silver.trips t
  JOIN iceberg.gold.eta_baseline_hour b ON hour(t.requested_at) = b.hour_of_day
  WHERE t.status = 'completed'
    AND t.requested_at >= TIMESTAMP '2025-01-15 01:00:00'
)
SELECT count(*)                                         AS n,
       avg(abs(quoted   - actual) / actual)             AS mape_quoted,
       avg(abs(baseline - actual) / actual)             AS mape_baseline,
       approx_percentile(abs(quoted   - actual), 0.9)   AS p90_abs_err_quoted_sec,
       approx_percentile(abs(baseline - actual), 0.9)   AS p90_abs_err_baseline_sec
FROM eval;

-- 3. Error by driver vehicle type AS OF accept time (uses the SCD2 as-of join).
SELECT driver_vehicle_type,
       count(*) AS trips,
       avg(abs(CAST(quoted_eta_sec AS DOUBLE) - actual_duration_sec) / actual_duration_sec) AS mape_quoted
FROM iceberg.serving.v_trips_enriched
WHERE status = 'completed'
GROUP BY 1
ORDER BY trips DESC;
