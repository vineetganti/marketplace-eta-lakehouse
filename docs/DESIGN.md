# Design Doc: Real-time Marketplace State & ETA Lakehouse

> Status: DRAFT. Sections marked **FILL** are yours: write them from measurements, not
> opinions. Pre-filled text is a starting hypothesis, not a result.

## 1. Context and goals

A ride/delivery marketplace needs (a) fresh per-area supply/demand for surge and dispatch,
(b) a trustworthy history of trip state for analytics, and (c) point-in-time-correct features
(driver and merchant attributes as they were when a trip happened) for ETA and experimentation.

**Goals**

| # | Goal | Target (local) |
|---|---|---|
| G1 | Freshness: trip state change visible in `silver.trips` | < 60 s p95 (checkpoint 30 s + commit) |
| G2 | Freshness: 1-min gold window visible | < 2 min after window close |
| G3 | Correctness: no lost, no duplicated pings in silver | 0 / 0 (reconcile PASS) |
| G4 | Correctness: exactly one latest row per trip | 0 violations |
| G5 | Recoverability: survive worker/broker/catalog/object-store loss | no loss, no dup after recovery |
| G6 | Reprocessability: rebuild any silver/gold table from retained data | result identical to live |

**Non-goals:** multi-region, HA control plane, auth, sub-second serving.

## 2. Architecture

See the diagram in the README. Three principles:

1. **State via CDC, telemetry via stream.** Trip/driver/merchant state is OLTP truth in
   Postgres; Debezium turns it into an ordered changelog. GPS pings never touch Postgres.
2. **Bronze is immutable and raw.** Parsing bugs must never destroy data. Bronze is the
   replay source when Kafka retention has expired.
3. **Streaming for speed, batch for truth.** Streaming gold favours latency and may drop late
   data; batch overwrite from silver repairs closed days (ADR-0001).

## 3. Data model

| Table | Grain | Key | Partition | Write mode |
|---|---|---|---|---|
| `bronze.cdc_raw` | one Kafka record | (topic, partition, offset) | `ingest_date` | append |
| `silver.trips` | trip (latest state) | (`trip_id`, `requested_date`) | `requested_date` | upsert |
| `silver.driver_pings` | (driver_id, seq) | (driver_id, seq) | `event_date` | append, deduped |
| `silver.driver_pings_late` | late ping | (driver_id, seq) | `event_date` | append |
| `silver.driver_versions` | CDC change | (driver_id, version_ts, lsn) | `version_date` | append |
| `silver.merchant_versions` | CDC change | (merchant_id, version_ts, lsn) | `version_date` | append |
| `gold.h3_demand_1m` | (window, H3 r8) | (window_start, h3_r8) | `window_date` | append, batch-overwritten |
| `gold.h3_supply_1m` | (window, H3 r8) | (window_start, h3_r8) | `window_date` | append, batch-overwritten |

**H3 resolution 8** (about 0.7 km^2, edge about 460 m): fine enough to see a hotspot, coarse enough
that a 1-minute window has more than a handful of events. Resolution is a tunable: see E10.

## 4. Source semantics (FILL after M1)

- Debezium envelope for insert / update / delete: *paste real examples*.
- `source.ts_ms` (database commit time) vs `ts_ms` (connector time) vs `updated_at` (our event
  time, written by the replayer in **simulated** time). Which one drives which decision, and why.
- Initial snapshot (`op = r`) vs streaming (`op = c/u/d`) and what it means for ordering.
- `REPLICA IDENTITY FULL`: what you get (before-images) and what it costs (WAL volume).

## 5. Time semantics and delivery guarantees

### 5.1 Event time vs processing time

- Event time for pings is `event_ts_ms` set by the "device"; for trips it is `requested_at`.
- Watermark = max event time seen minus **30 s** (bounded out-of-orderness).
- `table.exec.source.idle-timeout = 30 s` because the replayer leaves partitions idle and an idle
  partition otherwise freezes the watermark forever.
- Lateness taxonomy to validate in E2: (a) on time, (b) late but within the bound (absorbed),
  (c) late beyond the bound (dropped from gold windows, kept in silver, recorded in
  `driver_pings_late`).

### 5.2 Delivery chain (hypothesis; confirm in M6)

| Hop | Guarantee | Duplicate source | Where handled |
|---|---|---|---|
| Postgres WAL to Debezium | at-least-once | connector restart re-reads from last committed offset | tolerated: upserts and version-log dedup |
| Debezium to Redpanda | at-least-once | producer retry, restart | same |
| Replayer to Redpanda (pings) | at-least-once (idempotent producer removes producer-retry dups only) | injected duplicates; real upstream retries | `silver.driver_pings` keep-first dedup |
| Redpanda to Flink | exactly-once into Flink state (checkpointed offsets) | none after restore | n/a |
| Flink to Iceberg | exactly-once *visibility* (two-phase commit on checkpoint) | failed commit retry | Iceberg commit protocol |
| Gold windows | idempotent by construction (`COUNT DISTINCT`) | duplicate pings | no dedup state needed |

State the one-sentence claim you are willing to defend: *FILL*.

### 5.3 Dedup state and TTL

Keep-first dedup keeps `(driver_id, seq)` in state for `table.exec.state.ttl = 2 h`. That is a
bet that no duplicate arrives more than 2 h after the first copy. State size = pings/s x TTL x
bytes/key. Compute yours: *FILL*. What happens when the bet fails (E3): *FILL*.

## 6. Storage design

- **Upsert + partitioning rule.** Flink's Iceberg upsert writes an equality delete for the old
  row. Equality deletes only apply within the same partition, and Flink requires the PK to
  include partition columns. Therefore the partition column must be derived from the key or
  immutable. `requested_date` qualifies; `updated_at` / `status` do not (ADR-0002, E-reproduction in M2).
- **Commit cadence.** One snapshot per checkpoint per sink: 30 s => 2,880 snapshots/day/table
  and at least one file per partition per commit. Small files are a function of *wall-clock
  checkpoint count*, not data volume. Metadata cleanup properties are set on streaming tables.
- **Compaction.** Append tables: Trino `optimize`. Upsert table: equality-delete files must be
  merged away; Trino support is an open question (E7). Cadence and trigger: *FILL*.
- **Snapshot expiry** trades time-travel window against storage and metadata size. Chosen
  retention and the reason: *FILL*.
- **Why H3 cell is not a partition key:** cardinality in the thousands per day would explode
  file counts. Cluster/sort by cell inside date partitions instead (stretch: `write.distribution-mode`, sort orders).

## 7. Backfill and reprocessing: Kappa vs Lambda

| Option | Mechanism | Pros | Cons |
|---|---|---|---|
| Pure Kappa | Re-read Kafka from earliest with a new consumer group into new tables, swap | One code path | Bounded by Kafka retention (7 d); state rebuild cost |
| Lambda | Separate batch pipeline recomputes everything | Simple batch semantics | Two code paths that drift |
| **Chosen: Kappa + batch repair** | Stream for freshness; bronze/silver as truth; `INSERT OVERWRITE` closed gold partitions | One streaming path for state, small batch for gold repair, atomic swap | Reconcile must avoid open partitions (E8) |

Procedure for a silver rebuild (M8): create `*_shadow` table, replay from bronze (batch, parse
envelope with `JSON_VALUE`) or from Kafka earliest, `EXCEPT` against live (expect 0 rows),
then swap via rename or catalog pointer. Record timings: *FILL*.

## 8. SCD2 design

Silver stores an **append-only version log** per dimension. SCD2 intervals are derived in a
Trino view with `LEAD(version_ts)`; duplicate CDC deliveries are collapsed with
`GROUP BY (id, version_ts)`. Trips join dimensions **as of** `accepted_at` (driver) and
`requested_at` (merchant).

Trade-offs to articulate (ADR-0003): view-time derivation vs materialised table with `MERGE`
(cost of closing the previous row, concurrency with streaming writes, read-time cost); version
logs make late-arriving dimension changes trivially correct; deletes are not modelled (soft
delete via `status`).

## 9. Failure-mode table (the centrepiece)

Fill **Expected** *before* running the experiment; fill **Observed** and **Evidence** after.

| # | Failure | Blast radius | Expected (hypothesis) | Detect | Mitigate | Observed | Exp |
|---|---|---|---|---|---|---|---|
| F1 | Broker down 60 s | All streaming jobs, Debezium, replayer | Jobs fail or stall, restart with backoff; Debezium retries; replayer producer buffers/errs; no loss after recovery | Flink job state, Connect status, consumer lag | Producer retries/buffering, restart strategy, alert on lag | *FILL* | E4 |
| F2 | Debezium connector restart | bronze, silver dims | Some CDC events re-emitted: duplicates in bronze, none in silver trips (idempotent upsert); version log may duplicate (collapsed in view) | bronze distinct payload vs count | Idempotent sinks, view dedup | *FILL* | E5 |
| F3 | Postgres restart / failover | CDC | Slot persists; Debezium resumes. If slot dropped: gap, needs re-snapshot | slot lag, connector status | Monitor `pg_replication_slots`, WAL size alerts | *FILL* | E5 |
| F4 | Replication slot not consumed (connector down for long) | Postgres disk | WAL accumulates unboundedly | `pg_replication_slots.restart_lsn` lag | `max_slot_wal_keep_size`, alerting | *FILL* | E5 |
| F5 | Flink TaskManager SIGKILL | Running jobs | Restart from last checkpoint; no loss, no dup in Iceberg | checkpoint restore log, reconcile | Checkpoint interval, restart strategy, HA JM for prod | Not confirmed (E1 case 1, seed 42, 10x). `reconcile` run 17 min after the kill FAILED: silver had 57,803 of 216,000 pings, 85 of 231 trips, bronze 269 of 924. What was present was correct (no duplicates, one row per trip), so the lake looks like a stalled prefix, not random loss. Right after the kill `docker compose ps taskmanager` listed no container; whether it restarted, and the checkpoint id restored from, were not recorded. Cause found on the rerun (BUILD_LOG Session 10): `docker compose kill` counts as a manual stop, so Docker did not apply `restart: unless-stopped`; the TaskManager stayed `exited (137)`, `restarts=0`. With 0 TaskManagers and 0 slots every job sat in `RESTARTING` (`NoResourceAvailableException`), so the stall was the test harness, not Flink. The harness now starts the container itself after the kill; a rerun with that fix has not been done. Evidence: [raw reconcile output](../evidence/E1_taskmanager_kill.txt), [BUILD_LOG Session 9](BUILD_LOG.md#session-9--2026-11-09--e1-failure-injection-taskmanager-kill-minio-outage-catalog-outage-catalog-case-left-the-jobs-in-a-restart-loop) | E1 |
| F6 | Checkpoint timeout / failure | Commit cadence, freshness | Iceberg commits stall; data not visible until next successful checkpoint | checkpoint failure counter | Tune timeout, incremental RocksDB | *FILL* | E12 |
| F7 | Object store down during commit | Checkpoints and Iceberg | Checkpoint fails, job restarts; uncommitted data files orphaned | job restarts, orphan file count | `remove_orphan_files`, retry | Outage injected (MinIO stopped 16:07:31 to 16:08:18 UTC, 47 s) but no result recorded: the stack was wiped by `fresh` before `reconcile` was saved, so checkpoint ids and orphan files are lost. Redo needed. [BUILD_LOG Session 9](BUILD_LOG.md#session-9--2026-11-09--e1-failure-injection-taskmanager-kill-minio-outage-catalog-outage-catalog-case-left-the-jobs-in-a-restart-loop) | E1 |
| F8 | Iceberg catalog down | Commits | Commit fails and retries; job restarts; no corruption | commit exceptions | Retry/backoff, catalog HA in prod | Refuted so far (E1 case 3). A 49 s catalog stop (16:30:57 to 16:31:46 UTC) left all 5 jobs in a restart loop for 17+ minutes: no checkpoint completed after 16:31:24. Catalog log: `SQLITE_BUSY` (SQLite file locked), returned to Flink as HTTP 500, then `CommitStateUnknownException` on every restore. Jobs restored from chk-15 (bronze), chk-14 (trips, pings, dim) and chk-13 (gold). Restarting `iceberg-rest` at 17:07 did not help (still `SQLITE_BUSY`, jobs still `RESTARTING` 3 min later), so the lock is not left over from the kill. `/proc/locks` shows the catalog's own process holding a SQLite shared lock on its database, which blocks every writer (inference: a pooled connection holds a read open). The catalog has no busy timeout configured. Cancelling four of the five jobs (17:16:53) let the remaining one (`silver-pings`) run normally: checkpoints 15 to 21 in 3 minutes and zero `SQLITE_BUSY`. So the failure is contention among five committers on a single-writer SQLite catalog. After the other four jobs were restored by hand from retained checkpoints, `reconcile` passed every check (no loss, no duplicates, trips = Postgres, bronze extra 0) and `reconcile-after` gave gold 24,252 = expected. So: no corruption and no loss (hypothesis holds), but no self-recovery either (hypothesis fails): about 74 min to full health after a 49 s outage. Evidence: [reconcile](../evidence/E1_catalog_outage.txt), [after repair](../evidence/E1_catalog_outage_after.txt), [catalog log](../evidence/E1_catalog_outage_catalog.log), [Flink state](../evidence/E1_catalog_outage_flink_state.txt). [BUILD_LOG Session 9](BUILD_LOG.md#session-9--2026-11-09--e1-failure-injection-taskmanager-kill-minio-outage-catalog-outage-catalog-case-left-the-jobs-in-a-restart-loop) | E1 |
| F9 | Duplicate pings (retry/replay) | silver, gold | silver: dedup removes (within TTL); gold: unaffected (`COUNT DISTINCT`) | reconcile "no duplicates" | TTL sized to worst-case delay | *FILL* | E3 |
| F10 | Duplicate arrives after dedup TTL | silver | Leaks through as a duplicate row | reconcile | Larger TTL, or batch dedup at compaction | *FILL* | E3 |
| F11 | Late ping beyond watermark bound | gold windows | Dropped from streaming gold, present in silver and late table; batch repair fixes | late table count, reconcile INFO | Wider bound (latency cost), batch repair | Matches hypothesis (E0 baseline, seed 42, 30 s watermark). 4,381 late pings injected, all 216,000 unique pings present in silver. Streaming gold lost 242 of 24,252 (minute, cell) supply rows (1.00%). Late table caught 4,219 (run 1), 4,212 (run 2) and 4,206 (run 3) of 4,381, so the classification is approximate and varies per run (watermark timing). The gold gap was 242 rows in all three runs. `reconcile-gold` restored gold to 24,252 = expected. Evidence: [raw reconcile-after output](../evidence/E0_reconcile_after.txt), [BUILD_LOG Sessions 2 and 4](BUILD_LOG.md#session-4--2026-11-08--the-repair-done-in-the-right-order), [Session 7](BUILD_LOG.md#session-7--2026-11-09--jobs-submitted-twice-a-stuck-batch-job-then-a-clean-third-run). Bound sweep still to do in E2 | E2 |
| F12 | Out-of-order trip updates | silver.trips | Per-key order preserved (key = trip_id, same partition); upsert last-write-wins is correct | status regressions query | Partition by key | *FILL* | E1 |
| F13 | Malformed message (poison) | One job | Job fails and restart-loops on that offset (`ignore-parse-errors=false`) | job in restart loop | DLQ / skip policy decision | *FILL* | E11 |
| F14 | Add nullable column in source | silver | New field silently ignored downstream until silver is evolved | schema diff job | Explicit evolution step, contract tests | *FILL* | E6 |
| F15 | Rename/drop column in source | silver | Column becomes NULL, no error (silent data loss) | null-rate monitor | Block breaking DDL; schema registry compatibility | *FILL* | E6 |
| F16 | Type change in source | silver | Parse errors or NULLs | failures / null-rate | Compatibility rules | *FILL* | E6 |
| F17 | Streaming + batch overwrite on the same partition | gold | Commit conflict or double counting | commit exceptions, reconcile | Only overwrite closed partitions; coordinate by watermark | *FILL* | E8 |
| F18 | Small-file explosion | Query latency, metadata | Thousands of files/day/table; slow planning | `$files` count and avg size | Compaction schedule, longer checkpoint | *FILL* | E7 |
| F19 | Snapshot expiry vs long reader / time travel | Readers | Expired snapshot read fails | error on `FOR VERSION AS OF` | Retention >= longest query | *FILL* | E7 |
| F20 | Hot H3 cell / key skew | Window operator, state | One subtask holds most state | per-subtask metrics | Rebalance, salted keys, lower resolution | *FILL* | E10 |
| F21 | Clock skew in `updated_at` | Watermarks, ordering | Future timestamp pushes watermark ahead, making normal data "late" | late rate jump | Sanity-bound timestamps at ingestion | *FILL* | E2 |
| F22 | Dedup / window state growth | TaskManager memory | RocksDB growth, longer checkpoints | state size metric | TTL, state backend sizing | *FILL* | E12 |

## 10. Schema evolution policy (FILL after E6)

| Change | Safe? | Propagation observed | Policy |
|---|---|---|---|
| Add nullable column | Yes for source; silent drop downstream | FILL | FILL |
| Add NOT NULL with default | FILL | FILL | FILL |
| Rename column | No | FILL | FILL |
| Drop column | No | FILL | FILL |
| int to bigint | Usually | FILL | FILL |
| bigint to int / type change | No | FILL | FILL |
| Add enum value | App-level | FILL | FILL |

Iceberg side: add/rename/drop are metadata-only and ID-based (safe for readers); Flink
SQL sinks do not auto-evolve, so the table is evolved by hand (Trino `ALTER TABLE`) and the
job is resubmitted. Decide who owns that step.

## 11. Capacity and cost model (FILL)

- Ping rate = drivers / ping interval. Default: 300 / 10 s = 30/s sim, 1,800/s wall at 60x.
- Bytes per ping (JSON) about 110; Parquet compressed bytes per ping: *measure*.
- Storage/day = pings/day x bytes/ping + trips + gold. Compute: *FILL*.
- Checkpoint size (RocksDB incremental) and duration at steady state: *measure*.
- Scale-up thought exercise: at 1M active drivers pinging every 4 s, rate = 250k msgs/s.
  Partition count, Flink parallelism, state size, Iceberg commit rate: *FILL*.

## 12. Open questions

1. Does Trino `optimize` handle tables carrying equality deletes? If not, what is the compaction path?
2. Is the `CURRENT_WATERMARK` late classifier exactly aligned with window drops, or off by one
   window at boundaries?
3. What is the true end-to-end p95 freshness, and is it dominated by checkpoint interval?
4. Should the batch repair be triggered by the late table (targeted) or by a daily schedule?

## 13. Decisions

- [ADR-0001](adr/0001-kappa-with-batch-repair.md) Kappa with batch repair
- [ADR-0002](adr/0002-upsert-partitioning.md) Upsert table partitioning
- [ADR-0003](adr/0003-scd2-as-version-log.md) SCD2 as version log + view
