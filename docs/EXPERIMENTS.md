# Experiments Runbook

Each experiment: **Hypothesis** (write yours before running) -> **Do** -> **Measure** -> **Record**
(in DESIGN.md section 9, and here under *Result*). Always use a seeded replay so runs compare.

Baseline run used unless stated:

```powershell
.\tasks fresh                      # nuke + up + init + jobs (a clean stack)
.\.venv\Scripts\python -m replayer run --seed 42 --speedup 30 --duration-min 120 --manifest manifest.json
Start-Sleep -Seconds 90            # let Flink complete a few checkpoints
.\tasks reconcile
```

Checkpoint cadence is wall-clock (30 s). Speedup 30 means a 120-minute simulation takes 4
wall minutes, about 8 commits per table. Use `--speedup 5` when you need many commits.

---

## E1. Exactly-once across worker, object-store and catalog failures

- **Hypothesis:** after killing the TaskManager (or briefly stopping MinIO / the REST catalog)
  mid-run, `reconcile` still PASSes: no loss, no duplicates.
- **Do:** start the replay at `--speedup 10`; at about minute 2 run
  `.\tasks chaos taskmanager-kill`. Repeat with `minio-outage 45` and `catalog-outage 45`.
- **Measure:** time to recovery (Flink UI exceptions tab), checkpoint id restored from, orphaned
  files (`remove_orphan_files` dry look via `$files` vs object listing), `.\tasks reconcile`.
- **Record:** failures F5, F7, F8, F12.
- **Dig deeper:** which checkpoint did the job restore from? What happened to data files written
  after it but never committed?

## E2. Late data: the real cost of the watermark bound

- **Hypothesis:** with a 30 s bound and lateness uniform in [45 s, 600 s], essentially every
  injected late ping is dropped from streaming gold.
- **Do:** run the baseline. Compare `manifest.pings_late_beyond_watermark_bound` with
  `count(*)` in `silver.driver_pings_late`, and gold coverage (`reconcile` INFO line).
  Then change the bound in `30_gold...sql` and `21_silver_pings.sql` to 5 s, 120 s and 600 s and
  re-run (new `nuke` each time).
- **Measure:** % of (minute, cell) gold rows missing vs silver; gold freshness (window emits
  only when the watermark passes: larger bound = later output); late-table accuracy vs ground truth.
- **Record:** F11, F21. Plot bound vs missing% vs freshness delay: that curve is the tradeoff.
- **Refine from E0:** E0 gave 4,212 late-table pings of 4,381 injected. Hypothesis: a window closes when the
  watermark passes the end of its *minute*, so a ping delayed 50 s can still make it if it sits near the end of a
  minute; effective tolerance = bound + up to 60 s. Prediction: the absorbed count (about 165) should shift
  with the bound in the sweep. Also expect run-to-run variation in the late count (E0 surprise 2).
- **Dig deeper:** inject a single ping with an event time 1 day in the future. What happens to
  the watermark and to everything after it? (F21)

## E3. Duplicates and the dedup TTL bet

- **Hypothesis:** duplicates delivered within the TTL are removed; beyond it they leak.
- **Do:** baseline (`--dup-max-delay-s 30`) must PASS "no duplicates". Then set
  `table.exec.state.ttl` to `10 s` in `21_silver_pings.sql` and re-run with
  `--dup-max-delay-s 30 --dup-prob 0.05`.
- **Measure:** duplicates in silver (`04_verify.sql` #1) vs TTL; state size in Flink UI.
- **Record:** F9, F10. Compute memory = keys/s x TTL x bytes.

## E4. Broker loss

- **Hypothesis:** nothing is lost; jobs recover on their own.
- **Do:** replay at `--speedup 10`; `.\tasks chaos broker-loss 60` at minute 2.
- **Measure:** what did the replayer do (buffer, error, block)? Did Debezium tasks recover or
  need a manual restart (`api.http`)? Flink restarts? `reconcile`.
- **Record:** F1.

## E5. CDC duplicates and the replication slot

- **Hypothesis:** a Debezium restart re-emits some events: bronze gets duplicates, silver trips
  does not.
- **Do:** during a replay run `.\tasks chaos connect-restart` a few times.
  Run `04_verify.sql` #3 (events vs distinct payloads) and `reconcile`.
  Then stop `connect` for 5 minutes while the replay continues and watch
  `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots;`
- **Record:** F2, F3, F4.

## E6. Schema evolution

- **Hypothesis:** add-column is invisible downstream; rename silently NULLs the column.
- **Do:** during a replay run `.\tasks chaos schema-add-column`, then later
  `schema-rename-column`. Query silver for the new column / null-rate of `distance_km`.
  Evolve silver by hand: `ALTER TABLE iceberg.silver.trips ADD COLUMN surge_multiplier double`
  in Trino, update the source DDL and job, resubmit. Note what a savepoint-based restart does.
- **Measure:** null rate of each column over time; whether any job errored.
- **Record:** F14, F15, F16 and the policy table (DESIGN section 10).

## E7. Small files, compaction, snapshot expiry

- **Hypothesis:** file count grows linearly with checkpoints, not with data volume; compaction
  cuts planning time; equality deletes on `trips` are harder to compact.
- **Do:** replay at `--speedup 5 --duration-min 120` (about 24 min, about 48 commits).
  Run `02_maintenance.sql` diagnostics before and after `optimize`, `expire_snapshots`,
  `remove_orphan_files`. Time a representative query before/after (`EXPLAIN ANALYZE`).
  Try `optimize` on `silver.trips`; note result. Verify time travel to an expired snapshot fails.
- **Measure:** files, avg KB, snapshots, query wall time, equality-delete file count.
- **Note from E0:** idle periods still produce empty snapshots every 5 min. Check the full `summary` map of one
  (`element_at(summary, ...)`, never `summary['key']` in Trino) and count them in the snapshot totals.
- **Record:** F18, F19, and DESIGN section 12 question 1.

## E8. Streaming vs batch overwrite

- **Hypothesis:** overwriting a *closed* day is safe; overwriting a day the stream still writes
  conflicts or double counts.
- **Do:** (a) after the replay has finished, `.\tasks reconcile-gold -SimDate 2025-01-15`, then
  `reconcile --after-reconcile` must PASS. (b) start a slow replay and run the overwrite for the
  same date while it is running.
- **Record:** F17 and the rule for when reconcile may run.

## E9. Reprocessing and backfill

- **Hypothesis:** a shadow table rebuilt from bronze equals live silver.
- **Do:** create `silver.trips_shadow` (same DDL); write a batch job that parses
  `bronze.cdc_raw` (`JSON_VALUE(payload, '$.after.trip_id')` etc., keeping the latest by
  `source.lsn`); compare `EXCEPT` both ways. Separately replay from Kafka earliest with a new
  consumer group and time it.
- **Measure:** diff rows (expect 0), wall time for each path, what each path cannot recover.
- **Record:** DESIGN section 7 timings and ADR-0001 consequences.

## E10. H3 resolution and skew

- **Hypothesis:** resolution 9 multiplies cell count and state and thins each window; 7 hides
  hotspots; a few hot cells dominate under any resolution.
- **Do:** change `h3_cell(..., 8)` to 7 and 9 in all jobs; compare
  `count(distinct h3_r8)`, gold row counts, state size, and the top-10 cell share (`04_verify.sql` #7).
- **Record:** F20 and the final resolution choice with justification.

## E11. Poison messages

- **Hypothesis:** the job restart-loops on the bad offset forever.
- **Do:** `.\tasks chaos poison-message`; watch the job. Then try
  `'json.ignore-parse-errors' = 'true'` and measure what silently disappears. Design a DLQ
  (a raw-format source into a quarantine table).
- **Record:** F13 and the chosen policy.

## E12. Checkpoint interval trade-off

- **Hypothesis:** shorter interval = fresher data, more files, more overhead; longer = the reverse.
- **Do:** run the same replay at 10 s, 30 s, 120 s (edit `init.sql` and the compose Flink
  properties). Measure end-to-end freshness (`04_verify.sql` #5), files/hour, checkpoint
  duration and size.
- **Record:** F6, F22 and a three-row tradeoff table.

---

### Result log

| Exp | Date | Setup delta | Expected | Observed | Surprise | Evidence link |
|---|---|---|---|---|---|---|
| E0 (baseline) | 2026-11-08 | none: seed 42, 120 sim-min at 60x, 30 s watermark, 30 s checkpoints | no loss of pings or duplicates (dedup works). One latest row per trip and none lost in silver. Final status counts match manifest with silver == Postgres. Every trip CDC event landed atleast once. Gold supply == recomputed-from-silver | All correctness checks PASS: 216,000/ 216,000 pings, 2,199 dups removed, 231/231 trips = Postgres, bronze 924/924. Before repair gold missed 242 of 24,252 (minute, cell) rows (1.00%). After `reconcile-gold`, gold = 24,252 = expected (0 missing). Late table caught 4,219 pings (run 1), 4,212 (run 2) and 4,206 (run 3, 2026-11-09, clean stack) of 4,381 injected; the gold gap was 242 rows in all three runs | (1) Only 1.00% of rows missing though ~2% of pings were late: row counts understate the damage, since a late ping only removes a row if every ping in that cell-minute was late; otherwise `active_drivers` is undercounted. (2) Same seed, different late counts per run (4,219 / 4,212 / 4,206): the input is deterministic but the watermark-based classification is not. The missing-gold-row count did not move (242 in all three). (3) Replay took 181 s wall vs 120 s nominal at 60x. (4) Trino cross-check (`04_verify.sql`) agreed with `reconcile`: the late-ping lateness buckets sum to 4,212; bronze has no duplicate payloads (trips 924/924, drivers 329/329, merchants 124/124), as expected with no Debezium restart (E5 is where they should appear). (5) After the replay ended, `silver.driver_pings` kept committing an *empty* snapshot every 5 min (vs every 30 s during the replay). Hypothesis, unverified: Flink Iceberg sink's `flink.max-continuous-empty-commits` (10 x 30 s). It matters for E7 snapshot expiry and file counts. | [raw output: post-repair reconcile](../evidence/E0_reconcile_after.txt); [BUILD_LOG: Session 2, run 1](BUILD_LOG.md#session-2--2026-11-08--init-streaming-jobs-first-replay-first-reconcile); [Session 4, run 2 + repair](BUILD_LOG.md#session-4--2026-11-08--the-repair-done-in-the-right-order), [Session 6: Trino verify queries](BUILD_LOG.md#session-6--2026-11-08--trino-serving-views-and-the-verify-queries-one-sql-bug); [Session 7: run 3 (after a double-submission incident)](BUILD_LOG.md#session-7--2026-11-09--jobs-submitted-twice-a-stuck-batch-job-then-a-clean-third-run) |
| E1 (failure injection; **closed unfinished**: only case 3 has a valid result; cases 1 and 2 and the Postgres-catalog retest were skipped, see BUILD_LOG Session 11) | 2026-11-09 | seed 42, 120 sim-min at 10x, 30 s watermark, 30 s checkpoints. Three separate runs, each from `fresh`: TaskManager SIGKILL (15:43:33 UTC), MinIO stop 45 s (16:07:31), iceberg-rest stop 45 s (16:30:57) | After each failure the jobs restart from the last checkpoint and `reconcile` PASSes (no loss, no duplicates) | **Case 1 (TM kill):** `reconcile` FAILED, run 17 min after the kill: pings 57,803 of 216,000, trips 85 of 231, bronze 269 of 924; no duplicates, one row per trip. Looks like a stall, not loss; cause not established. **Case 2 (MinIO):** no result recorded. **Case 3 (catalog):** all 5 jobs in a restart loop 17+ min after the catalog returned; no checkpoint after 16:31:24; catalog error `SQLITE_BUSY`, then `CommitStateUnknownException` on each restore. Restored from chk-15 (bronze), chk-14 (trips, pings, dim versions), chk-13 (gold). Restarting the catalog at 17:07 did not recover the jobs; `/proc/locks` shows the catalog process holding a SQLite shared lock that blocks all writers. With four jobs cancelled, the one left (`silver-pings`) ran normally (checkpoints 15 to 21, no `SQLITE_BUSY`): contention, not a broken catalog. After restoring the four cancelled jobs by hand from their retained checkpoints (chk-21 dim, chk-14 trips, chk-14 gold, chk-16 bronze), `reconcile` passed every check: pings 216,000 once each, trips 231 = Postgres, bronze 924 = 924 extra 0; after `reconcile-gold`, gold 24,252 = expected. Data was safe; availability was not (about 74 min to full health). Late table 2,263 (baseline about 4,210), gold gap 227 (baseline 242): INFO only, cause untested | (1) A 49 s catalog outage caused an outage of 17+ min: the recovery path, not the failure, is the problem. (2) After `docker compose kill`, the TaskManager did not restart despite `restart: unless-stopped`: Docker treats a kill as a manual stop (measured on the rerun: `exited (137)`, `restarts=0`, 0 slots, all jobs `RESTARTING`). This also explains case 1's failed `reconcile`; it was the harness, not the pipeline. The chaos action now starts the container itself (untested). (3) I forgot to note Flink checkpoint ids; `fresh` deletes them with the MinIO volume, so only case 3's survive. (4) `wall_seconds` is 1083.3 at 10x and 180.6 at 60x: both equal about 10,835 simulated seconds, so the seeded stream spans about 180.6 sim-min, not 120 (inferred; replayer not read). (5) Cases were injected at about minute 3 (case 2) and 6 (case 3), not 2 as planned | [raw output, case 1](../evidence/E1_taskmanager_kill.txt); [case 3 catalog log](../evidence/E1_catalog_outage_catalog.log); [case 3 Flink state](../evidence/E1_catalog_outage_flink_state.txt); [BUILD_LOG Session 9](BUILD_LOG.md#session-9--2026-11-09--e1-failure-injection-taskmanager-kill-minio-outage-catalog-outage-catalog-case-left-the-jobs-in-a-restart-loop) |
