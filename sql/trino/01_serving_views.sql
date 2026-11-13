-- Serving layer (Trino views over Iceberg). Run once:  .\tasks trino-views
-- If your Trino build can't create views on the REST catalog, run the SELECT bodies ad hoc;
-- nothing else depends on the views existing.

CREATE SCHEMA IF NOT EXISTS iceberg.serving;

-- ------------------------------------------------------------------ SCD2: drivers
-- Silver holds an append-only version log. Collapse duplicate deliveries (same driver + version
-- timestamp, e.g. after a Debezium restart) and derive validity intervals with LEAD().
CREATE OR REPLACE VIEW iceberg.serving.dim_driver_scd2 AS
WITH dedup AS (
  SELECT driver_id,
         version_ts,
         max_by(vehicle_type, source_lsn) AS vehicle_type,
         max_by(city,         source_lsn) AS city,
         max_by(rating,       source_lsn) AS rating,
         max_by(status,       source_lsn) AS status
  FROM iceberg.silver.driver_versions
  WHERE op IN ('r', 'c', 'u')
  GROUP BY driver_id, version_ts
)
SELECT driver_id,
       vehicle_type,
       city,
       rating,
       status,
       version_ts AS valid_from,
       COALESCE(LEAD(version_ts) OVER (PARTITION BY driver_id ORDER BY version_ts),
                TIMESTAMP '9999-12-31 23:59:59.999') AS valid_to,
       LEAD(version_ts) OVER (PARTITION BY driver_id ORDER BY version_ts) IS NULL AS is_current
FROM dedup;

-- ------------------------------------------------------------------ SCD2: merchants
CREATE OR REPLACE VIEW iceberg.serving.dim_merchant_scd2 AS
WITH dedup AS (
  SELECT merchant_id,
         version_ts,
         max_by(category,       source_lsn) AS category,
         max_by(tier,           source_lsn) AS tier,
         max_by(city,           source_lsn) AS city,
         max_by(h3_r8,          source_lsn) AS h3_r8,
         max_by(commission_pct, source_lsn) AS commission_pct
  FROM iceberg.silver.merchant_versions
  WHERE op IN ('r', 'c', 'u')
  GROUP BY merchant_id, version_ts
)
SELECT merchant_id,
       category,
       tier,
       city,
       h3_r8,
       commission_pct,
       version_ts AS valid_from,
       COALESCE(LEAD(version_ts) OVER (PARTITION BY merchant_id ORDER BY version_ts),
                TIMESTAMP '9999-12-31 23:59:59.999') AS valid_to,
       LEAD(version_ts) OVER (PARTITION BY merchant_id ORDER BY version_ts) IS NULL AS is_current
FROM dedup;

-- ------------------------------------------------------------------ point-in-time (as-of) join
-- Each trip sees the dimension row that was valid WHEN IT HAPPENED, not today's row.
-- Driver attributes are resolved at accepted_at (that's when the driver became known).
CREATE OR REPLACE VIEW iceberg.serving.v_trips_enriched AS
SELECT t.trip_id,
       t.requested_at,
       t.status,
       t.service_type,
       t.pickup_h3,
       t.dropoff_h3,
       t.quoted_eta_sec,
       t.actual_duration_sec,
       t.distance_km,
       d.vehicle_type AS driver_vehicle_type,
       d.rating       AS driver_rating_at_accept,
       d.city         AS driver_city,
       m.category     AS merchant_category,
       m.tier         AS merchant_tier_at_request
FROM iceberg.silver.trips t
LEFT JOIN iceberg.serving.dim_driver_scd2 d
       ON d.driver_id = t.driver_id
      AND t.accepted_at >= d.valid_from AND t.accepted_at < d.valid_to
LEFT JOIN iceberg.serving.dim_merchant_scd2 m
       ON m.merchant_id = t.merchant_id
      AND t.requested_at >= m.valid_from AND t.requested_at < m.valid_to;

-- ------------------------------------------------------------------ surge signal
CREATE OR REPLACE VIEW iceberg.serving.v_surge_1m AS
SELECT COALESCE(d.window_start, s.window_start) AS window_start,
       COALESCE(d.h3_r8, s.h3_r8)               AS h3_r8,
       COALESCE(d.requests, 0)                  AS requests,
       COALESCE(s.active_drivers, 0)            AS active_drivers,
       CAST(COALESCE(d.requests, 0) AS DOUBLE)
         / NULLIF(CAST(COALESCE(s.active_drivers, 0) AS DOUBLE), 0) AS demand_supply_ratio
FROM iceberg.gold.h3_demand_1m d
FULL OUTER JOIN iceberg.gold.h3_supply_1m s
  ON d.window_start = s.window_start AND d.h3_r8 = s.h3_r8;
