-- Batch reconcile: rebuild ONE closed day of gold from silver (the source of truth).
-- Usage: .\tasks submit batch/40_reconcile_gold.sql DATE=2025-01-15
--
-- INSERT OVERWRITE ... PARTITION is a single atomic Iceberg replace-partitions commit, so
-- readers see either the old or the new day, never a half-written one (compare with
-- DELETE + INSERT in Trino, which is two commits). See ADR-0001.
--
-- Run only for days the streaming jobs have finished writing. Running this while the streaming
-- job still appends to the same partition makes the commits conflict / double count (E8).
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'pipeline.name' = 'reconcile-gold-{{DATE}}';

-- Supply: distinct pinging drivers per minute per cell, recomputed from ALL silver pings
-- (including the ones the streaming windows dropped as late).
INSERT OVERWRITE lakehouse.gold.h3_supply_1m PARTITION (`window_date` = '{{DATE}}')
SELECT
  FLOOR(`event_time` TO MINUTE)                        AS `window_start`,
  TIMESTAMPADD(MINUTE, 1, FLOOR(`event_time` TO MINUTE)) AS `window_end`,
  `h3_r8`,
  COUNT(DISTINCT `driver_id`)                          AS `active_drivers`
FROM lakehouse.silver.driver_pings
WHERE `event_date` = DATE '{{DATE}}'
GROUP BY FLOOR(`event_time` TO MINUTE), `h3_r8`;

-- Demand: trip requests per minute per pickup cell, from latest-state silver trips.
INSERT OVERWRITE lakehouse.gold.h3_demand_1m PARTITION (`window_date` = '{{DATE}}')
SELECT
  FLOOR(`requested_at` TO MINUTE)                        AS `window_start`,
  TIMESTAMPADD(MINUTE, 1, FLOOR(`requested_at` TO MINUTE)) AS `window_end`,
  `pickup_h3`                                            AS `h3_r8`,
  COUNT(*)                                               AS `requests`
FROM lakehouse.silver.trips
WHERE `requested_date` = DATE '{{DATE}}'
GROUP BY FLOOR(`requested_at` TO MINUTE), `pickup_h3`;
