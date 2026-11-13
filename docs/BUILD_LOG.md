# Build Log — marketplace-eta-lakehouse

A running, chronological record of everything done while building this project: commands, files created or changed, and errors with their fixes. It is raw material for a learning blog post.

**How this log is maintained:** it is only updated when you say **"update log"**. On each update I append a new session entry covering the terminal commands run, files and code changed, and any errors with root cause and fix since the last update. Existing entries are never rewritten.

---

## Entry template (copied for each update)

```
## Session N — YYYY-MM-DD — <short title>

### Goal
What this stretch of work was trying to achieve.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | `...` | ok / error summary |

### Files created / changed
| File | Change | Why |
|------|--------|-----|
| `path/to/file` | created / edited | reason |

### Code highlights
Key snippets, with a line on what each does.

### Errors & debugging
**Error:** exact message
- **Context:** command or step that triggered it
- **Root cause:** what was actually wrong
- **Fix:** what resolved it
- **Lesson:** the takeaway for the blog

### Blog notes
Insights, surprises, and decisions worth writing about.
```

---

## Session 0 — 2026-11-08 — Baseline snapshot (log started)

### Goal
Start the build log. The project already existed when logging began, so this entry records the starting state. Commands run before this point were not captured.

### Project layout at log start
| Area | Files |
|------|-------|
| Orchestration | `docker-compose.yml`, `.env.example`, `tasks.cmd`, `scripts/tasks.ps1` |
| Docs | `README.md`, `docs/DESIGN.md`, `docs/PLAN.md`, `docs/EXPERIMENTS.md`, `docs/adr/0001-kappa-with-batch-repair.md`, `docs/adr/0002-upsert-partitioning.md`, `docs/adr/0003-scd2-as-version-log.md` |
| Postgres / CDC | `infra/postgres/init.sql`, `infra/debezium/pg-connector.json` |
| Flink | `infra/flink/Dockerfile`, `flink/sql/init.sql`, `flink/sql/ddl/01_iceberg_tables.sql`, `flink/sql/jobs/10_bronze_cdc_raw.sql`, `20_silver_trips.sql`, `21_silver_pings.sql`, `23_silver_dim_versions.sql`, `30_gold_demand_supply.sql`, `flink/sql/batch/40_reconcile_gold.sql` |
| Trino | `infra/trino/catalog/iceberg.properties`, `sql/trino/01_serving_views.sql`, `02_maintenance.sql`, `03_eta_analysis.sql`, `04_verify.sql` |
| Java UDF | `udf/pom.xml`, `udf/src/main/java/dev/lakehouse/udf/H3Cell.java` |
| Replayer (Python) | `replayer/__init__.py`, `__main__.py`, `cli.py`, `inject.py`, `model.py`, `sinks.py`, `synth.py`, `tlc.py` |
| Scripts | `scripts/fetch_tlc.py`, `scripts/reconcile.py` |
| Tests | `tests/test_fetch_tlc.py`, `test_inject.py`, `test_reconcile.py`, `test_synth.py`, `test_tlc.py` |
| Tooling | `pyproject.toml`, `api.http`, `.vscode/*`, `.gitignore`, `.gitattributes`, `.dockerignore` |

### Observations
- Python bytecode is compiled for CPython 3.14 with pytest 9.2.0, so tests have been run locally at least once.
- `udf/target/classes/.../H3Cell.class` exists, so the Java UDF has been compiled at least once.
- The directory is not a git repository, so there is no commit history to reconstruct earlier steps from.

### Blog notes
- Architecture so far: Postgres → Debezium CDC → Flink SQL (bronze/silver/gold) → Iceberg → Trino serving, per the ADRs (Kappa with batch repair, upsert partitioning, SCD2 as a version log).

---

## Session 1 — 2026-11-08 — First local run: venv, tests, dry replay, bringing the stack up

### Goal
Set up the Python environment, check the replayer works without Docker, then start the Docker stack for the first time.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | `.\tasks help` | Printed the target list (venv, test, up, init, jobs, replay, reconcile, chaos, ...) |
| 2 | PowerShell | `.\tasks venv` | OK. Created `.venv` and installed the replayer and dev tools as an editable install. Only a setuptools `BuildBackendWarning` about same-named folders |
| 3 | PowerShell | `.\tasks test` | OK. `18 passed in 2.42s` |
| 4 | PowerShell | `.\tasks replay-dry` | OK. Printed 20 `pg.drivers` events. Summary: `events_emitted: 20`, no pings or trips yet |
| 5 | PowerShell | `.\tasks up` | **Error.** `Docker is installed but not running` (Docker Desktop was not started) |
| 6 | PowerShell | `.\tasks up` (after starting Docker Desktop) | **Error.** MinIO image pull failed with 401. See Error 1 |
| 7 | PowerShell | `docker pull quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z` | 401 UNAUTHORIZED. Confirms the tag is not the problem |
| 8 | PowerShell | `docker pull minio/minio:RELEASE.2024-10-13T13-34-11Z` | `pull access denied ... repository does not exist` |
| 9 | PowerShell | `docker pull` for `quay.io/minio/minio:latest`, `quay.io/minio/mc:latest`, `minio/minio:latest`, `alpine/minio:latest` | All fail (401, or not found) |
| 10 | PowerShell | `docker pull docker.redpanda.com/redpandadata/redpanda:v24.2.7` | OK. Redpanda was never the problem |
| 11 | PowerShell | `docker pull cgr.dev/chainguard/minio:latest` | OK |
| 12 | PowerShell | `docker run --rm --entrypoint sh cgr.dev/chainguard/minio:latest -c "minio --version; which mc"` | minio `RELEASE.2026-10-23T20-06-51Z`. `sh` exists, `which` and `mc` do not |
| 13 | PowerShell | `docker pull cgr.dev/chainguard/minio-client:latest` | OK |
| 14 | PowerShell | `docker run --rm --entrypoint /bin/sh cgr.dev/chainguard/minio-client:latest -c "mc --version"` | **Error.** `stat /bin/sh: no such file or directory`. The image is distroless (no shell) |
| 15 | PowerShell | `docker inspect cgr.dev/chainguard/minio:latest --format '{{.Config.Entrypoint}} {{.Config.Cmd}} {{.Config.User}}'` | `[/usr/bin/minio] [] 65532`. Entrypoint is `minio`, default user is non-root |
| 16 | PowerShell | `docker compose config -q` | Exit 0. Compose file is valid |
| 17 | PowerShell | `docker compose up -d --build` | OK. All containers created. `minio-init` exited 0 and logged `Bucket created successfully local/warehouse` and `local/flink` |
| 18 | PowerShell | `docker compose ps -a` | `iceberg-rest` was `Exited (1)`. Everything else up. See Error 2 |
| 19 | PowerShell | `docker compose logs iceberg-rest --no-log-prefix` | `SQLiteException: [SQLITE_CANTOPEN] Unable to open the database file` |
| 20 | PowerShell | `docker compose up -d` (after the fix) | OK. `iceberg-rest` recreated and stayed up |
| 21 | PowerShell | `docker compose ps -a` | All 9 services up. `postgres`, `redpanda`, `connect` and `trino` report healthy. `minio-init` exited 0 as intended |

### Files created / changed
| File | Change | Why |
|------|--------|-----|
| `docker-compose.yml` (`minio` service) | edited | Swapped `quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z` for `cgr.dev/chainguard/minio:latest`. Added `user: "0"` so the named volume is writable |
| `docker-compose.yml` (`minio-init` service) | edited | Swapped the `mc` image for `cgr.dev/chainguard/minio-client:latest`. Replaced the shell script with `MC_HOST_local`, `entrypoint: ["mc"]`, `command: ["mb","-p","local/warehouse","local/flink"]` and `restart: on-failure` |
| `docker-compose.yml` (`iceberg-rest` service) | edited | Added `user: "0"` so SQLite can create its file in the `restdata` volume |
| `docs/BUILD_LOG.md` | edited | This entry |

### Code highlights
`minio` and `minio-init` after the fix:

```yaml
minio:
  image: cgr.dev/chainguard/minio:latest
  user: "0"
  command: ["server", "/data", "--console-address", ":9001"]

minio-init:
  image: cgr.dev/chainguard/minio-client:latest
  depends_on: [minio]
  restart: on-failure
  environment:
    MC_HOST_local: http://${MINIO_ROOT_USER:-minioadmin}:${MINIO_ROOT_PASSWORD:-minioadmin}@minio:9000
  entrypoint: ["mc"]
  command: ["mb", "-p", "local/warehouse", "local/flink"]
```

- `MC_HOST_<alias>` is an `mc` feature. It defines the alias from an environment variable, so no `mc alias set` step (and no shell) is needed.
- `restart: on-failure` replaces the old `until ... do sleep 1; done` loop. If MinIO isn't ready, `mc mb` exits non-zero and Docker retries. Once it exits 0, `depends_on: condition: service_completed_successfully` on `iceberg-rest` is satisfied.
- `mc mb -p` ignores buckets that already exist, so retries and re-runs are safe.

### Errors & debugging

**Error 0:** `ERROR: Docker is installed but not running. Start Docker Desktop and wait for "Engine running".`
- **Context:** first `.\tasks up`
- **Root cause:** Docker Desktop wasn't started
- **Fix:** start Docker Desktop and wait for "Engine running"
- **Lesson:** the `tasks` wrapper checks for the daemon first, which gives a clearer message than a raw compose failure.

**Error 1:** `failed to resolve reference "quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z": unexpected status from HEAD request ... 401 UNAUTHORIZED`
- **Context:** `docker compose up -d --build`. The output also showed noise: `Image lakehouse-flink:local Error pull access denied`, `No such image: ...mc...` and `...redpanda...` marked `Interrupted`
- **Diagnosis steps:**
  1. Read the image lines in `docker-compose.yml` (`Grep` for `image:|build:`).
  2. Pulled the exact tag by hand. Same 401, so compose wasn't at fault.
  3. Tried `latest`, Docker Hub `minio/minio` and `alpine/minio`. All failed, so the whole `minio` repository was unreachable, not just one old tag.
  4. Pulled Redpanda separately. It worked, so the "Interrupted" lines were collateral damage from compose aborting parallel pulls.
- **Root cause:** the MinIO project's public container images (`quay.io/minio/*`, `docker.io/minio/minio`) can no longer be pulled anonymously. Registries often return 401 for a repo that is private or removed, which looks like an auth problem but isn't one.
- **Fix:** switch to the Chainguard images (`cgr.dev/chainguard/minio` and `minio-client`), which pull anonymously. They're distroless, which caused three follow-on changes:
  - `minio-client` has no `/bin/sh` (the Error 1b exec failure below), so the shell retry script became `MC_HOST_local` plus `restart: on-failure`.
  - The default user is `65532`, so `minio` runs as root to write to the fresh named volume.
  - `latest` replaces the old date-pinned tags, so the version is no longer pinned.
- **Lesson:**
  - A 401 from a registry can mean "this doesn't exist or isn't public", not "log in".
  - Pin images, but also keep a fallback source. Pinning doesn't help if the registry removes the image.
  - The first error in compose output isn't always the culprit. "Interrupted" and "pull access denied for lakehouse-flink" were side effects. `lakehouse-flink:local` is built locally, so its pull attempt is expected to fail.
  - A distroless image is a different environment. Anything that relied on `sh` has to be redesigned.

**Error 1b:** `exec: "/bin/sh": stat /bin/sh: no such file or directory`
- **Context:** step 14, probing the `minio-client` image
- **Root cause:** distroless image with only the `mc` binary
- **Fix:** drop the shell wrapper (see Error 1)

**Error 2:** `org.sqlite.SQLiteException: [SQLITE_CANTOPEN] Unable to open the database file`
- **Context:** `iceberg-rest` exited 1 right after startup. Its `CATALOG_URI` is `jdbc:sqlite:file:/data/iceberg_catalog.db`, with `/data` mounted from the `restdata` named volume
- **Root cause:** a fresh Docker named volume is owned by root. The image runs as a non-root user, so it can't create the `.db` file. The first run was needed to surface this, because the earlier failed `up` never started this service.
- **Fix:** `user: "0"` on `iceberg-rest`, then `docker compose up -d` recreated it.
- **Lesson:** `SQLITE_CANTOPEN` usually means "can't create or write the file", so check permissions and the path. For volume problems, either run as root or `chown` the mount in an init step.
- **Possible follow-up:** running as root is a quick fix. A tidier option is a one-shot init container that does `chown` on the volume.

### Blog notes
- The stack came up in two compose edits, but both failures only appeared once real containers started. The unit tests and `replay-dry` can't catch infra drift.
- The migration from MinIO's official images to Chainguard is a good story about supply-chain fragility: pinned tag, working for months, then the registry stops serving it.
- `latest` now means MinIO `RELEASE.2026-10-23`. If any later step behaves differently from the 2024 release, that's the first suspect.
- Next steps: `.\tasks init` (topics, Debezium connector, Iceberg tables), then `.\tasks jobs`.

---

## Session 2 — 2026-11-08 — Init, streaming jobs, first replay, first reconcile

### Goal
Create the Kafka topics, Debezium connector and Iceberg tables. Submit the five streaming Flink jobs. Push two simulated hours of synthetic traffic through the pipeline, then check that the lakehouse matches what the replayer says it emitted.

These commands were run by you in your own terminal. I logged them from the output you pasted.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | `.\tasks init` | OK. Created 4 topics, registered the Debezium connector, created the Iceberg tables. Details below |
| 2 | PowerShell | `.\tasks jobs` | OK. Submitted 5 Flink SQL files, each returned a Job ID. Details below |
| 3 | PowerShell | `.\tasks replay` | OK. 2 simulated hours at 60x, `wall_seconds: 180.6` |
| 4 | PowerShell | `.\tasks reconcile` | 7 `PASS`, 2 `INFO`. No `FAIL`. See Results |

**What `.\tasks init` did, in order:**
1. Waited for Kafka Connect at `http://localhost:8083/connectors`.
2. Created topics `pg.public.trips`, `pg.public.drivers`, `pg.public.merchants` and `driver.pings`. Each has 3 partitions and 1 replica. Connect also created its own `_connect_configs` (1 partition), `_connect_offsets` (25) and `_connect_status` (5).
3. Registered the Debezium connector `pg-marketplace` from `infra/debezium/pg-connector.json`. A status call showed the connector and task 0 both `RUNNING`.
4. Waited for the Iceberg REST catalog (`:8181/v1/config`) and the Flink UI (`:8081/overview`).
5. Submitted `ddl/01_iceberg_tables.sql` through the Flink SQL client. All statements succeeded.

**Iceberg tables created** (all `format-version = 2`):

| Layer | Table | Partitioned by | Notes |
|-------|-------|----------------|-------|
| bronze | `cdc_raw` | `ingest_date` | Raw Debezium envelopes, the replay source |
| silver | `trips` | `requested_date` | Upsert table, PK `(trip_id, requested_date)` |
| silver | `driver_pings` | `event_date` | Deduplicated, append-only |
| silver | `driver_pings_late` | `event_date` | Audit trail of late pings |
| silver | `driver_versions` | `version_date` | Append-only version log (SCD2 input) |
| silver | `merchant_versions` | `version_date` | Append-only version log (SCD2 input) |
| gold | `h3_demand_1m` | `window_date` | 1-minute tumbling windows per H3 res-8 cell |
| gold | `h3_supply_1m` | `window_date` | Distinct drivers per window and cell |

**Flink jobs submitted by `.\tasks jobs`:**

| File | Job ID | What it does |
|------|--------|--------------|
| `jobs/10_bronze_cdc_raw.sql` | `59a87d0beb667616892028f5e4b465cd` | Reads all `pg.public.*` topics as raw strings into `bronze.cdc_raw` |
| `jobs/20_silver_trips.sql` | `7bbbd715da0e872f00a1ccf9a96639be` | `debezium-json` changelog into the `silver.trips` upsert table |
| `jobs/21_silver_pings.sql` | `aae9fd9529818ead25354c03200b70db` | Keep-first dedup on `(driver_id, seq)` into `silver.driver_pings`, plus the late-ping audit table |
| `jobs/23_silver_dim_versions.sql` | `162ee068e432039533f072d8800159de` | One row per CDC change into the two version-log tables |
| `jobs/30_gold_demand_supply.sql` | `d8b48b07be116d9554d7978a1becad20` | 1-minute tumbling windows for demand and supply |

### Files created / changed
No source files were changed this session. Generated by running things:

| File | Change | Why |
|------|--------|-----|
| `manifest.json` | created by `.\tasks replay` | The replayer's record of what it emitted, which `reconcile` compares against |
| `docs/BUILD_LOG.md` | edited | This entry |

### Code highlights
The design choices that the reconcile results depend on, taken from the SQL files that were submitted.

Keep-first dedup (`jobs/21_silver_pings.sql`). `ROW_NUMBER()` ordered by processing time ascending keeps the first copy of each `(driver_id, seq)`:

```sql
SELECT *, ROW_NUMBER() OVER (PARTITION BY `driver_id`, `seq` ORDER BY `proc_time` ASC) AS rn
FROM src_pings
-- ... WHERE rn = 1
```

Late-ping detection. A ping counts as late if its 1-minute window had already closed when it arrived:

```sql
WHERE CURRENT_WATERMARK(`event_time`) IS NOT NULL
  AND TIMESTAMPADD(MINUTE, 1, FLOOR(CAST(`event_time` AS TIMESTAMP(3)) TO MINUTE))
        <= CAST(CURRENT_WATERMARK(`event_time`) AS TIMESTAMP(3))
```

Gold windows with a 30-second watermark. Anything later than that is dropped from gold but kept in silver:

```sql
WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
-- ...
FROM TABLE(TUMBLE(TABLE v_pings, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE))
```

Supply uses `COUNT(DISTINCT driver_id)`, so duplicate pings can't inflate it and gold needs no dedup state. Demand filters the plain-JSON envelope to `op = 'c'`, because windows need an append-only input and the `debezium-json` changelog also carries updates.

Silver trips upsert. The partition column must be immutable per key, otherwise the equality delete lands in the wrong partition and you get duplicate "latest" rows (ADR-0002):

```sql
PRIMARY KEY (`trip_id`, `requested_date`) NOT ENFORCED
) PARTITIONED BY (`requested_date`) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')
```

### Results

**Replay summary:**

| Metric | Value |
|--------|-------|
| `events_emitted` | 219,576 |
| `pings_unique` | 216,000 |
| `pings_dup_emitted` | 2,199 |
| `pings_late_injected` | 4,381 |
| `trips_distinct` | 231 (all `completed`) |
| `wall_seconds` | 180.6 |

**Reconcile:**

| Result | Check | Numbers |
|--------|-------|---------|
| PASS | pings: no loss | silver distinct = 216,000, manifest unique = 216,000 |
| PASS | pings: no duplicates (dedup works) | rows = 216,000, distinct = 216,000, injected dups = 2,199 |
| PASS | trips: one latest row per trip | rows = 231, distinct `trip_id` = 231 |
| PASS | trips: none lost | silver = 231, manifest = 231 |
| PASS | trips: final status counts match manifest | `{'completed': 231}` both sides |
| PASS | trips: silver equals Postgres (CDC fidelity) | `{'completed': 231}` both sides |
| PASS | bronze: every trip CDC event landed | bronze = 924, emitted = 924, extra = 0 |
| INFO | late audit table (approximate) | 4,219 rows vs 4,381 injected |
| INFO | gold supply: (minute, cell) rows missing vs silver | 242 of 24,252 (1.00%) |

### Errors & debugging
No errors this session. Everything exited cleanly. Two kinds of noisy output appear on every Flink SQL client run and are harmless:

- `WARNING: Unknown module: jdk.compiler specified to --add-exports` (printed 5 times per run). The Flink image's JVM flags name a JDK module that isn't present in the runtime image. Nothing depended on it.
- `WARNING: Unable to create a system terminal, creating a dumb terminal`. The SQL client is run from a script with no TTY, so it falls back to a dumb terminal. That is also why the SQL appears echoed with `>` prompts.

**Open item, not an error:** the 1.00% of gold supply rows missing is expected. Late pings fall into windows that have already fired, so the streaming job drops them from gold while silver keeps them. `reconcile` suggests running `batch/40_reconcile_gold.sql` and re-checking with `--after-reconcile`.

**Open question:** the late audit table has 4,219 rows against 4,381 injected. The reconcile tool itself labels this check approximate. The difference of 162 is worth a look, because the late-ping condition compares against the watermark at the moment each ping is processed, and that moves as other partitions advance. This is the question Experiment E2 in `docs/EXPERIMENTS.md` is meant to answer. I haven't investigated it.

### Blog notes
- End to end it works first time: Postgres to Debezium to Redpanda to Flink to Iceberg, with 7 of 7 hard correctness checks passing. Zero ping loss, zero duplicates, and CDC fidelity confirmed against Postgres itself.
- The gold layer shows a real streaming trade-off. Streaming windows are fast but drop late data. The lake stays complete, and a batch job repairs gold afterwards. That is the Kappa-with-batch-repair idea from ADR-0001, with a measured cost of about 1% of rows.
- `2,199` injected duplicates producing `0` duplicate rows is a nice concrete number for a post about dedup.
- Raw bronze CDC counts match exactly (924 and 924). At-least-once delivery would allow extras, but none appeared in this run.
- A replay of 2 simulated hours at 60x took 181 s of wall time against 120 s nominal at 60x, about 50% over. The cause hasn't been investigated (see the EXPERIMENTS.md E0 surprise note).
- Next steps: `.\tasks reconcile-gold -SimDate 2025-01-15`, then `.\tasks reconcile-after`, then `.\tasks trino-views` and `.\tasks trino-file sql\trino\04_verify.sql`.

---

## Session 3 — 2026-11-08 — Batch gold repair, then `fresh` wiped the data before the re-check

### Goal
Repair the late-data gap in gold (the 1.00% of missing supply rows from Session 2) with the batch job, then confirm the repair with `.\tasks reconcile-after`.

These commands were run by you in your own terminal. I logged them from the output you pasted.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | `.\tasks reconcile-gold` | OK. Ran `batch/40_reconcile_gold.sql` for `2025-01-15` (the default date). Both `INSERT OVERWRITE` statements finished with `Complete execution of the SQL update statement` |
| 2 | PowerShell | `.\tasks fresh` | OK, but **destroyed all data** (see Errors & debugging). Ran nuke, then up, then init, then jobs |
| 3 | PowerShell | `.\tasks reconcile-after` | **9 checks: 4 FAIL, 4 PASS, 1 INFO. Exit 1.** Every number was 0 because the lakehouse was empty |

**What `reconcile-gold` ran** (`batch/40_reconcile_gold.sql`). Each statement atomically rebuilds one day of gold from silver:

```sql
INSERT OVERWRITE lakehouse.gold.h3_supply_1m PARTITION (`window_date` = '2025-01-15')
SELECT FLOOR(`event_time` TO MINUTE), TIMESTAMPADD(MINUTE, 1, FLOOR(`event_time` TO MINUTE)),
       `h3_r8`, COUNT(DISTINCT `driver_id`)
FROM lakehouse.silver.driver_pings
WHERE `event_date` = DATE '2025-01-15'
GROUP BY FLOOR(`event_time` TO MINUTE), `h3_r8`;
-- demand is rebuilt the same way from lakehouse.silver.trips, grouped by pickup_h3
```

- `INSERT OVERWRITE ... PARTITION` is a single atomic Iceberg replace-partitions commit. Readers see either the old day or the new day, never half of each. A Trino `DELETE` followed by `INSERT` would be two commits (ADR-0001).
- It recomputes from silver, the source of truth. That includes the pings the streaming windows dropped as late.
- Only run it for a day the streaming jobs have finished writing. Running it while a job still appends to the same partition makes the commits conflict or double count (Experiment E8).

**What `fresh` did:**
1. `down`: removed all 9 containers, the network and 4 volumes (`rpdata`, `miniodata`, `pgdata`, `restdata`). That is every piece of data: Kafka topics, the Debezium offsets, the MinIO warehouse, Postgres and the Iceberg catalog.
2. Rebuilt `lakehouse-flink:local`. Every layer was `CACHED`, so it took 3.5 s.
3. `up`: new network and volumes, all containers started. `minio-init` ran, created the buckets and exited, and `iceberg-rest` started cleanly on its new volume.
4. `init`: same 4 topics, connector `RUNNING`, same 8 Iceberg tables.
5. `jobs`: resubmitted the 5 jobs. The Job IDs are new:

| File | Job ID |
|------|--------|
| `jobs/10_bronze_cdc_raw.sql` | `51e70b9356d91d6eabfd961fdf531f2e` |
| `jobs/20_silver_trips.sql` | `a4f53bd48e1b0083285c08ba0faa9f11` |
| `jobs/21_silver_pings.sql` | `21f17a90893e277d4cf44a0c77df98ef` |
| `jobs/23_silver_dim_versions.sql` | `cd49d9fc5c34b96513c87c8f898f8676` |
| `jobs/30_gold_demand_supply.sql` | `187a94281bbdda1ad67143c9bf047135` |

**`reconcile-after` output:**

| Result | Check | Numbers | Meaning |
|--------|-------|---------|---------|
| FAIL | pings: no loss | silver = 0, manifest = 216,000 | Real mismatch: the lake is empty |
| PASS | pings: no duplicates | rows = 0, distinct = 0 | Vacuous: zero equals zero |
| PASS | trips: one latest row per trip | rows = 0, distinct = 0 | Vacuous |
| FAIL | trips: none lost | silver = 0, manifest = 231 | Real mismatch |
| FAIL | trips: final status counts | silver = `{}`, manifest = `{'completed': 231}` | Real mismatch |
| PASS | trips: silver == Postgres | both `{}` | Vacuous: both sides are empty |
| FAIL | bronze: every trip CDC event landed | bronze = 0, emitted = 924 | Real mismatch |
| INFO | late audit table | 0 rows, replayer injected 4,381 | Empty |
| PASS | gold supply == recomputed-from-silver | gold = 0, expected = 0 | Vacuous. This is the check the run was meant to verify |

### Files created / changed
| File | Change | Why |
|------|--------|-----|
| `manifest.json` | **unchanged and now stale** | It still describes the old replay (216,000 pings, 231 trips) but the data it describes is gone |
| `docs/BUILD_LOG.md` | edited | This entry |

No source files were changed this session.

### Errors & debugging

**Error:** `[FAIL] pings: no loss  silver distinct=0 manifest unique=216000` (and 3 more FAILs), then `ERROR: command failed (exit 1): ... scripts/reconcile.py --manifest manifest.json --after-reconcile`
- **Context:** `.\tasks reconcile-after`, run right after `.\tasks fresh`
- **Diagnosis:** every actual count was exactly 0 and every expected count was non-zero. A bug in the pipeline would not look like that. It means the tables are empty. The `fresh` output above lists the removed volumes, which explains why.
- **Root cause:** a sequencing mistake, not a pipeline bug. `fresh` is `nuke + up + init + jobs`, and `nuke` deletes all data. It was run between `reconcile-gold` and `reconcile-after`. `reconcile-after` compares the lakehouse against `manifest.json` from the earlier replay, but that replay's data no longer existed.
- **Fix:** nothing to fix in the code. The data has to be regenerated: run `.\tasks replay` again (it rewrites `manifest.json`), then `.\tasks reconcile`, `.\tasks reconcile-gold`, `.\tasks reconcile-after`, with no `fresh` in between.
- **Cost:** the batch repair from step 1 was thrown away before it could be verified. Whether it closes the 242 missing supply rows (1.00%) is still unanswered.
- **Lesson:**
  - **Do not run `fresh` (or `nuke`) between `reconcile-gold` and `reconcile-after`.** The pair is one experiment: repair, then verify. Wiping the data in the middle makes the verify step meaningless.
  - The `help` text already says it: `nuke ... DELETE all data (do this between replays)`. Between replays, never in the middle of one.
  - Correct order:
    1. `.\tasks fresh` (only when starting from clean)
    2. `.\tasks replay`
    3. `.\tasks reconcile`
    4. `.\tasks reconcile-gold`
    5. `.\tasks reconcile-after`
  - Be careful with `PASS` on an empty system. Four checks passed on `0 = 0`, including the gold check this run existed to test. A check that compares two empty sets passes for the wrong reason. The FAILs were the only honest signal here. A useful improvement would be for `reconcile.py` to fail loudly, or print a warning, when the manifest expects data and the lake has none, instead of letting `0 == 0` pass.
  - `manifest.json` and the lakehouse are a pair. If one is reset, regenerate the other.

### Good news from this session
- The Session 1 compose fixes hold on a completely fresh start. `minio-init` created the buckets, `iceberg-rest` came up on a brand-new root-owned volume, and `init` and `jobs` ran clean. That is a free full-stack smoke test of the whole bring-up path.
- The Flink image rebuild was fully cached: 3.5 s instead of the slow first build.

### Blog notes
- The story: a verification step can be invalidated by a harmless-looking command run in between. The failing run looked alarming (4 FAILs), but the all-zeros pattern points straight at "empty system", not "broken pipeline".
- The vacuous-`PASS` point is worth its own section. A passing check is only evidence if it could have failed.
- `INSERT OVERWRITE ... PARTITION` as an atomic repair is the key design idea behind the batch half of Kappa-with-batch-repair. This session shows it running, but not yet its effect.
- Still to measure: after a correct `replay`, `reconcile`, `reconcile-gold`, `reconcile-after` run, does the 1.00% supply gap close to zero?
- Next steps: `.\tasks replay` (about 3 minutes), `.\tasks reconcile`, `.\tasks reconcile-gold`, `.\tasks reconcile-after`. The Flink streaming jobs are already running again after `fresh`, so no resubmit is needed.

---

## Session 4 — 2026-11-08 — The repair, done in the right order

### Goal
Redo the Session 3 experiment without wiping the data in the middle: replay, check, repair gold with the batch job, then verify the repair.

These commands were run by you in your own terminal. I logged them from the output you pasted. The pasted notes before the output explained the Session 3 failure and gave this command sequence. The numbers below are all from the terminal output.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | `.\tasks replay` | OK. Same summary as Session 2: 219,576 events, 216,000 unique pings, 2,199 dups, 4,381 late, 231 trips |
| 2 | PowerShell | `Start-Sleep -Seconds 60` | Gave the streaming jobs time to finish writing and commit before checking |
| 3 | PowerShell | `.\tasks reconcile` | 7 PASS, 2 INFO. Gold supply gap: 242 of 24,252 (1.00%) missing |
| 4 | PowerShell | `.\tasks reconcile-gold` | OK. Both `INSERT OVERWRITE` statements for `2025-01-15` completed |
| 5 | PowerShell | `.\tasks reconcile-after` | **8 PASS, 1 INFO, no FAIL.** Gold check: `gold=24252 expected=24252` |

### Results

**Before and after the batch repair:**

| Check | Before (`reconcile`) | After (`reconcile-after`) |
|-------|----------------------|---------------------------|
| pings: no loss | PASS, 216,000 = 216,000 | PASS, same |
| pings: no duplicates | PASS, rows = distinct = 216,000 | PASS, same |
| trips: one latest row per trip | PASS, 231 = 231 | PASS, same |
| trips: none lost | PASS, 231 = 231 | PASS, same |
| trips: final status counts | PASS, `{'completed': 231}` | PASS, same |
| trips: silver == Postgres | PASS | PASS |
| bronze: every trip CDC event landed | PASS, 924 = 924, extra = 0 | PASS, same |
| late audit table | INFO, 4,212 rows (4,381 injected) | INFO, same |
| gold supply | INFO, **242 of 24,252 rows missing (1.00%)** | **PASS, gold = 24,252, expected = 24,252** |

- The only line that changed is the gold supply line. It went from 242 rows missing to an exact match with silver.
- The silver and bronze checks gave identical results before and after, which is right: the batch job rewrites gold only.
- The gold line passing here is meaningful, unlike Session 3. This time the lake had real data (24,252 rows, not 0), so the check could have failed and didn't.

**Run-to-run comparison with Session 2** (same seed, same events, run on a fresh stack):

| Metric | Session 2 | Session 4 |
|--------|-----------|-----------|
| Events, unique pings, dups, late injected, trips | 219,576 / 216,000 / 2,199 / 4,381 / 231 | identical |
| Silver and bronze counts | all match | all match |
| Gold supply rows missing | 242 | 242 |
| Late audit table rows | 4,219 | 4,212 |

- The late audit table varied by 7 rows (4,219 against 4,212) between two identical replays. The replayer's output is deterministic from the seed, so the variation must come from the Flink side. The late check compares each ping against the watermark at the moment it is processed, and that depends on timing. This matches the earlier note that the check is approximate.
- The missing-gold count (242) was identical both times, so that one doesn't seem timing-sensitive in this setup. I wouldn't claim it is fixed in general from two runs.
- `wall_seconds` was `180.6` in both runs, to the decimal. I have not checked why, and it is probably the simulated schedule rather than a real clock measurement.

### Files created / changed
No files were changed this session apart from this log. `manifest.json` was rewritten by `replay` with the same content, because the replay is seeded.

### Errors & debugging
No errors. The `jdk.compiler` and dumb-terminal warnings appeared again and are harmless (see Session 2).

**Not an error, but a caveat on the result:** the `reconcile-after` gold check compares gold with "recomputed from silver", and the repair job also computes gold from silver. So this check proves the batch job ran and wrote what silver says. It does not independently prove that silver is right, which is what the silver checks (against the manifest and Postgres) are for. The two layers of checks together are the evidence.

### Lessons
- **Sequence and patience.** The working order was `replay`, wait about 60 s, `reconcile`, `reconcile-gold`, `reconcile-after`, with nothing destructive in between. The 60 s sleep matters, because checking while the streaming jobs are still committing would give a falsely incomplete lake.
- **`down` against `nuke` and `fresh`.** `.\tasks down` pauses the stack and keeps the data. `.\tasks nuke` and `.\tasks fresh` delete the volumes. When in doubt, use `down`.
- **Look before you wipe.** After a repair, look at the result in Trino (for example `SELECT count(*) FROM iceberg.gold.h3_supply_1m;`) before running anything destructive. I haven't run this yet.
- **A repeatable experiment needs a seed.** The same seed gave the same events twice, so the two runs could be compared number for number.
- **Checker improvement still open:** `reconcile.py` should refuse to pass when the lake is empty. It isn't implemented. It was offered in the pasted notes and I haven't changed the script.

### Blog notes
- The headline result: the streaming layer lost about 1% of gold supply rows to late data, and one atomic `INSERT OVERWRITE` from silver brought it to an exact match. This is the Kappa-with-batch-repair idea (ADR-0001) working end to end with real numbers.
- The two-session story is worth telling together: the same check that gave four misleading PASSes on an empty lake (Session 3) gave a meaningful PASS on a full one (Session 4).
- Dedup result for a post: 2,199 duplicates injected, 0 duplicate rows in silver, and no ping lost.
- Open threads for later posts: why the late audit count is approximate (Experiment E2), and what happens if the batch job runs while streaming still writes the partition (Experiment E8).
- Next steps: look at the repaired table in Trino (`.\tasks trino`, then `.\tasks trino-views` and `.\tasks trino-file sql\trino\04_verify.sql`), and decide whether to make `reconcile.py` fail on an empty lake.

---

## Session 5 — 2026-11-08 — Saving the E0 evidence file and linking it from the docs

### Goal
Keep a raw, unedited copy of the post-repair reconcile output as evidence for experiment E0, and link it from the experiment and design docs.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | `docker compose ps --format "table {{.Service}}\t{{.Status}}"` | All 8 long-running services up for 49 minutes, so the Session 4 data was still present |
| 2 | PowerShell | `New-Item -ItemType Directory -Force evidence \| Out-Null` | Created the `evidence` folder |
| 3 | PowerShell | `.\tasks reconcile-after 2>&1 \| Out-File -Encoding utf8 evidence\E0_reconcile_after.txt` | Exit 0. 8 PASS, 1 INFO, no FAIL. Saved to the file |

- I used `Out-File -Encoding utf8` instead of the `*>` redirect. In older Windows PowerShell, `*>` can write UTF-16, which looks garbled in some editors.
- The check ran on the same data as Session 4. The output matches Session 4 exactly, including `gold=24252 expected=24252` and 4,212 late rows.
- Step 1 mattered: running the check after a `nuke` or `fresh` would have produced the empty-lake failure from Session 3.

### Files created / changed
| File | Change | Why |
|------|--------|-----|
| `evidence/E0_reconcile_after.txt` | created | Raw output of the post-repair reconcile |
| `docs/EXPERIMENTS.md` (E0 row, Evidence link column) | edited | Added a link to the raw file, before the BUILD_LOG links |
| `docs/DESIGN.md` (F11, Observed column) | edited | Added a link to the raw file |
| `docs/BUILD_LOG.md` | edited | This entry |

### Errors & debugging
No errors.

### Blog notes
- Evidence hierarchy for a post: the unedited output file is the strongest, the build log narrates it, and the design table summarises it. Each links down to the next.
- A saved file should come from a command run against the state it claims to describe. Checking that the stack was still up first is what makes this file trustworthy.

---

## Session 6 — 2026-11-08 — Trino serving views and the verify queries (one SQL bug)

### Goal
Create the Trino serving views, then run the ad-hoc verification queries in `sql/trino/04_verify.sql` against the repaired lake.

These commands were run by you in your own terminal. I logged them from the output you pasted. I checked the final state of the SQL files afterwards. The pasted notes around the output included an explanation of the results and the fix command. I mark which parts of that explanation are measured and which are hypotheses.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | `.\tasks trino-views` | OK. `CREATE SCHEMA` and 4 x `CREATE VIEW` (from `sql/trino/01_serving_views.sql`) |
| 2 | PowerShell | `.\tasks trino-file sql\trino\04_verify.sql` | Queries 1 to 4 returned. **Query 5 failed:** `Query 20261108_092844_00045_epug8 failed: Key not present in map: added-records`. Exit 1 |
| 3 | PowerShell | `foreach ($f in 'sql\trino\04_verify.sql','sql\trino\02_maintenance.sql') { (Get-Content $f -Raw) -replace "summary\['([a-z-]+)'\]", 'element_at(summary, ''$1'')' \| Set-Content $f -NoNewline }` | Rewrote every `summary['key']` in both files to `element_at(summary, 'key')` |
| 4 | PowerShell | `.\tasks trino-file sql\trino\04_verify.sql` | OK. All queries ran, nothing failed |

### Files created / changed
| File | Change | Why |
|------|--------|-----|
| `sql/trino/04_verify.sql` (query 5, line 33) | edited | `summary['added-records']` became `element_at(summary, 'added-records')` |
| `sql/trino/02_maintenance.sql` (lines 14 and 15) | edited | Same change for `added-data-files` and `added-records`. **Not run yet**, so untested |
| `docs/BUILD_LOG.md` | edited | This entry |

I confirmed with a search afterwards: no `summary[` is left in `sql/trino/`, and the three `element_at` calls are in place.

### Code highlights
The fix, in query 5 of `04_verify.sql`:

```sql
SELECT committed_at,
       lag(committed_at) OVER (ORDER BY committed_at) AS prev_commit,
       element_at(summary, 'added-records') AS added_records   -- was: summary['added-records']
FROM iceberg.silver."driver_pings$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
```

- In Trino, `map['key']` throws if the key is missing. `element_at(map, 'key')` returns `NULL` instead.
- `"driver_pings$snapshots"` is an Iceberg metadata table. Its `summary` column is a map of strings, and not every snapshot has every key.
- The regex `summary\['([a-z-]+)'\]` captures the key name and rebuilds the call around it. The `-` in the character class is needed because Iceberg keys like `added-data-files` contain hyphens.

### Results

**Query 1 (duplicate pings in silver) and query 2 (more than one latest row per trip):** both printed nothing. Both queries are supposed to return no rows, so that means zero violations.

**Query 3 (raw CDC duplicates in bronze):**

| Topic | Events | Distinct payloads |
|-------|--------|-------------------|
| `pg.public.trips` | 924 | 924 |
| `pg.public.drivers` | 329 | 329 |
| `pg.public.merchants` | 124 | 124 |

No duplicates, which is expected: Debezium was never restarted. Experiment E5 is where bronze duplicates should first appear. The trips count of 924 matches the `reconcile` bronze check.

**Query 4 (how late is "late"):** buckets come from `width_bucket(late_by_sec, ARRAY[0,60,120,300,600,1200])`.

| Bucket | Range | Pings | Max late (s) |
|--------|-------|-------|--------------|
| 1 | 0 to 60 s | 243 | 59 |
| 2 | 60 to 120 s | 475 | 119 |
| 3 | 120 to 300 s | 1,456 | 299 |
| 4 | 300 to 600 s | 2,038 | 579 |

- The buckets add up to 4,212, the same as the late-table count from `reconcile`, so the two views agree.
- The longest lateness was 579 s, under the injected maximum of 600 s. This column measures lateness against the watermark at processing time, not against the ping's original timestamp, so the numbers aren't directly comparable with the injected maximum.

**Query 5 (commit cadence, `silver.driver_pings` snapshots):**

| Commit time (UTC, 08:47 to 09:32) | Gap to previous commit | `added_records` |
|-----------------------------------|------------------------|-----------------|
| 09:32:53 down to 08:52:53 (9 commits) | about 5 min each | empty (`NULL`) |
| 08:47:53 | 30 s (previous 08:47:23) | 1,370 |

**Query 7 (hottest H3 cells in gold demand):** the top cell is `882a10725bfffff` with 9 requests, then `882a100d2dfffff` with 8. The rest are 6 or fewer.

### Errors & debugging

**Error:** `Query 20261108_092844_00045_epug8 failed: Key not present in map: added-records`
- **Context:** query 5 of `.\tasks trino-file sql\trino\04_verify.sql`
- **Root cause:** `summary['added-records']` reads a map key with subscript syntax, and Trino errors when the key is absent. Some snapshots in the table don't have that key. Query 5's own output explains which: the nine newest snapshots all have an empty `added_records`.
- **Fix:** `element_at(summary, 'added-records')`, applied to `04_verify.sql`, and to `02_maintenance.sql` which uses the same pattern. The re-run succeeded.
- **Lesson:** in Trino, use `element_at` for map lookups on anything that may be missing. Iceberg snapshot summaries are the textbook case, because they only list what a particular commit did.
- **Side effect worth knowing:** the failed query aborted the whole `trino-file` run. Query 7 never ran in the first attempt (query 6 is commented out in the file). The CLI stops at the first error.

The `Unable to create a system terminal` warning appeared again. Same cause as before (no TTY), harmless. Trino's `Debug this Compose error with Gordon` hint also showed up and is irrelevant here, because it wasn't a Compose error.

### Hypotheses (not verified)

**1. Why the empty snapshots arrive every 5 minutes.**
- Observation: once the replay finished, `silver.driver_pings` kept getting a snapshot exactly every 5 minutes with no added records, while during the replay it committed every 30 s.
- Possible explanation: the Iceberg Flink sink skips commits for checkpoints with no new data, but writes an empty snapshot after a number of consecutive empty checkpoints (I believe the setting is `flink.max-continuous-empty-commits`, default 10). With 30 s checkpoints, 10 x 30 s = 5 min, which fits. It would also explain the original error, because an empty commit has no `added-records` key in its summary.
- Not confirmed: I haven't looked at the sink settings or a snapshot's summary map. A check would be to select the full `summary` for one of the 5-minute snapshots.

**2. Why fewer pings reach the late table than were injected (4,212 against 4,381), and why the under-1-minute bucket is the smallest.**
- Possible explanation from the pasted notes: a ping's lateness only matters relative to when its 1-minute window closes. A window closes when the watermark passes the end of its minute, so a ping delayed 50 s can still land in time if it belongs near the end of a minute. The effective tolerance would then be the 30 s bound plus up to 60 s, not just 30 s.
- The notes' rough arithmetic was about 130 absorbed pings against the observed gap of about 165, so it is plausible but not proven.
- To test it: change the watermark bound and see whether the late count moves as predicted. This belongs under Experiment E2.

### Blog notes
- Two layers of checks now agree on the same number: the reconcile tool and the Trino bucket query both give 4,212 late pings.
- A small, concrete lesson on engine differences: a map subscript that "just works" in one SQL dialect throws in Trino, and `element_at` is the safe form. The error text is clear once you know what it means.
- The empty-snapshot pattern is a nice find for a post about Iceberg and streaming: a stream that is idle still produces commits, which matters for snapshot expiry and file counts. Treat it as a hypothesis until the summary map has been inspected.
- A failed statement in a batch file stops everything after it, so a bug in query 5 hid query 7.
- Next steps: run `.\tasks trino-file sql\trino\02_maintenance.sql` to test the second edited file, look at the full `summary` of a 5-minute snapshot, and use the late-ping hypothesis to design the E2 bound sweep.

---

## Session 7 — 2026-11-09 — Jobs submitted twice, a stuck batch job, then a clean third run

### Goal
Repeat the E0 baseline (fresh stack, replay, reconcile, batch repair, verify) on a new day. The run started badly: the five streaming jobs got submitted twice, which doubled the data and starved the batch job of task slots. After diagnosing it, the stack was reset and the sequence was re-run cleanly.

Times below are from the Flink containers' clock, which is UTC. The Flink REST API output in the diagnosis showed times in your local zone, which is 5 h 30 min ahead of those.

These commands were run by you in your own terminal. I logged them from the output you pasted. In the diagnosis I also queried the Flink REST API myself, and that is marked below.

### Commands run
| # | Time (UTC) | Command | Result |
|---|-----------|---------|--------|
| 1 | 14:17 | A stack start that ended with `jobs submitted` (the tail was all that was pasted) | 5 streaming jobs submitted (first set of Job IDs below) |
| 2 | after 14:18 | `.\tasks replay` | OK. Same summary as every earlier run |
| 3 | 14:28 | `.\tasks jobs` | **Mistake.** Submitted the 5 streaming jobs a second time, with new Job IDs |
| 4 | after 14:28 | `Start-Sleep -Seconds 60`, then `.\tasks reconcile` | **FAIL** on the dedup check. Exit 1. See Errors |
| 5 | 14:36 | `.\tasks reconcile-gold` | **Hung.** Output stopped after the first `INSERT OVERWRITE`. Interrupted with Ctrl key and answered `N` to "Terminate batch job (Y/N)?" |
| 6 | 14:46 | `.\tasks reconcile-gold` (retry) | Hung the same way |
| 7 | after 14:46 | Flink REST queries (run by me, see below) | 12 jobs `RUNNING` or `RESTARTING`, 0 free slots |
| 8 | 15:02 | `.\tasks fresh` | OK. Wiped and rebuilt everything. Image build fully `CACHED`. Same 4 topics, connector `RUNNING`, 8 tables, 5 jobs |
| 9 | after 15:03 | `Start-Sleep -Seconds 60`, then `.\tasks replay` | OK. Summary identical to earlier runs |
| 10 | after replay | `Start-Sleep -Seconds 60`, then `.\tasks reconcile` | 7 PASS, 2 INFO |
| 11 | 15:09 | `.\tasks reconcile-gold` | OK. Both `INSERT OVERWRITE` statements completed |
| 12 | after 15:09 | `.\tasks reconcile-after` | 8 PASS, 1 INFO, no FAIL. `gold=24252 expected=24252` |

**Streaming jobs, first submission (14:17 to 14:18):**

| Job | Job ID |
|-----|--------|
| bronze-cdc-raw | `ac2091c99eeb0c33835fb36fabaa2a31` |
| silver-trips-upsert | `2ed98d33a0abf769fdc7c259e7412e76` |
| silver-pings | `f54ed89cf02b56be9d57f914b71d4ff9` |
| silver-dim-versions | `2fe82b8cbbaef90f95f4c01b4bf3ea48` |
| gold-demand-supply | `75e92ec4ef5edbadcedea5c4a89ac1c8` |

**Second submission (14:28), the duplicates:** `876ae5c979a76c45f2e1926bbf80a53f`, `d3436a11ba24b59684df5cf2bfb9553e`, `b9547ad6d7f0797c9b34ee3131c991b7`, `69223ed02c57d081f9963de788ee5cbd` and `f3c86fcd5ca20b7bfc831dde3a1c15e3`.

**After the reset (15:02):** `29867323563a2ae20bb4754c8f289ec7`, `4779acf7bf6e1127712cfb31541b0834`, `2d1c2994ca5fba31ceb77e50c9aa11f4`, `23ab904935e27353b864a23cac91cf86` and `d18036f37c13356aa6121a5f5cb9ffda`.

**What I ran (read-only):** a query of `http://localhost:8081/overview` and `/jobs/overview`. Result:

| Item | Value |
|------|-------|
| Task slots total / available | 8 / 0 |
| Jobs running | 12 |
| Streaming jobs | 10 (two copies of each). `silver-dim-versions` and `gold-demand-supply` copies were in `RESTARTING` |
| Batch jobs | 2 x `reconcile-gold-2025-01-15`, both `RUNNING`, started about 14:36 and 14:46 |

I also checked `docker-compose.yml`: `taskmanager.numberOfTaskSlots: 8`, with a comment saying 5 streaming jobs plus headroom for the batch reconcile.

### Files created / changed
No source files were changed. Only this log was edited.

### Errors & debugging

**Error A:** `[FAIL] pings: no duplicates (dedup works)  rows=432000 distinct=216000 (injected dups=2199)`, exit 1
- **Context:** `.\tasks reconcile`, after `.\tasks jobs` had been run on top of a stack that already had the jobs running
- **Symptoms, all explained by one cause:**

| Check | Observed | Normal |
|-------|----------|--------|
| pings rows | 432,000 | 216,000 |
| bronze | 1,848 against 924 emitted (extra = 924) | extra = 0 |
| late audit table | 4,654 rows | about 4,206 to 4,219 |
| trips | 231 rows | 231 |

- **Root cause:** `.\tasks jobs` is not idempotent. Each submission creates a new job with the same source settings (`earliest-offset`, same consumer group). Both copies read the topics from the start and write to the same tables.
  - **Dedup state is per job.** Each copy kept its own first copy of every ping, so every ping was written twice.
  - **Bronze is append-only,** so it doubled.
  - **Trips are an upsert table,** so writing the same state twice changed nothing. That is the design working.
- **Why the bronze check still passed:** it allows `>=` because at-least-once delivery can legitimately produce extras. `extra=924` passed without complaint, so that check cannot catch a duplicated job.
- **Fix:** reset the stack and re-run (see Error B).
- **Lesson:** `fresh` already submits the jobs. Never run `.\tasks jobs` afterwards.

**Error B:** `reconcile-gold` hangs after printing the first `INSERT OVERWRITE`, with no "Complete execution"
- **Context:** `.\tasks reconcile-gold` with the duplicated jobs running
- **Root cause:** the TaskManager has 8 slots, with 10 streaming jobs running plus the batch job. Available slots were 0, so the batch job waited for a slot that never freed up. Both attempts left a `RUNNING` job in the cluster.
- **The `^Z` and "Terminate batch job (Y/N)?" lines** came from interrupting the wrapper (`tasks.cmd`). Answering `N` kept it going, and the job was already submitted to the cluster.
- **Fix:** removing the duplicate jobs frees the slots. I did not cancel anything individually; the whole stack was reset with `fresh` because the doubled data was unrecoverable anyway.
- **Lesson:** the cluster is sized for exactly 5 streaming jobs plus one batch job. Check slots with `Invoke-RestMethod http://localhost:8081/overview` if a Flink statement hangs.

### Results of the clean run (third baseline run)

| Check | Result |
|-------|--------|
| pings (no loss, no duplicates), 216,000 rows | PASS |
| trips (one row each, none lost, status counts, equals Postgres), 231 | PASS |
| bronze | PASS, 924 = 924, extra = 0 |
| late audit table | INFO, 4,206 rows (4,381 injected) |
| gold supply before repair | INFO, 242 of 24,252 missing (1.00%) |
| gold supply after `reconcile-gold` | PASS, 24,252 = 24,252 |

**Three runs of the same seed:**

| Run | Late audit table rows | Gold supply rows missing before repair |
|-----|-----------------------|----------------------------------------|
| Session 2 | 4,219 | 242 |
| Session 4 | 4,212 | 242 |
| Session 7 | 4,206 | 242 |

- The missing-row count is identical in all three runs. The late-table count varies by up to 13 (4,206 to 4,219).
- With three runs, "242 is stable for this seed" is now a reasonable statement. The same does not yet apply to other seeds or watermark bounds.

### Blog notes
- A good failure story: one extra command, `.\tasks jobs`, produced exactly doubled counts (432,000, 1,848) and an apparently unrelated hang, and one root cause (a second set of jobs) explained both.
- The same incident shows which parts of the design are idempotent: the trips upsert is, the append-only tables are not. It's a useful demonstration of why idempotent sinks matter.
- A check that passes by design (`>=` for at-least-once) can hide a real fault. Worth pairing it with a second check that fails on unexplained extras.
- The slot budget is a hidden coupling between the streaming and batch layers. The batch repair needs spare capacity, and when there is none it waits silently instead of failing.
- Third run on the clean stack reproduces the Session 4 repair result, which makes the E0 baseline solid.

### Next steps (recommended order)
1. **Add a guard against double submission.** Make the job-submit step in `scripts/tasks.ps1` check `http://localhost:8081/jobs/overview` and refuse (or skip) when a job of the same name is already `RUNNING`. It is small, and it prevents exactly this incident.
2. **Update the evidence docs.** Add this third run to the E0 row of `docs/EXPERIMENTS.md` and the F11 row of `docs/DESIGN.md` (late rows 4,219 / 4,212 / 4,206, missing 242 each time). Optionally save the clean `reconcile` output under `evidence/`.
3. **Run Experiment E2** (watermark bound sweep). It answers the open question in F11 and tests the window-position hypothesis from Session 6.
4. **Smaller loose ends:** run `.\tasks trino-file sql\trino\02_maintenance.sql` (edited in Session 6, never run), and inspect the full snapshot summary for one of the 5-minute empty commits.
5. **Make `reconcile.py` stricter:** fail when the manifest expects data and the lake is empty (Session 3), and flag unexplained bronze extras when no connector restart happened (this session).
6. **After that,** the failure experiments, starting with E1 (`.\tasks chaos taskmanager-kill`) and E5 (`.\tasks chaos connect-restart`, where bronze duplicates should first appear legitimately).
- Rules for every experiment: start from `.\tasks fresh`, do not run `.\tasks jobs` afterwards, wait about 60 s after the replay, and never `nuke` or `fresh` between repair and verify.

---

## Session 8 — 2026-11-09 — Guard against double job submission, third run added to the evidence rows

### Goal
Stop the Session 7 incident from happening again, then add that session's third baseline run to the E0 and F11 evidence rows.

### Commands run
| # | Shell | Command | Result |
|---|-------|---------|--------|
| 1 | PowerShell | Syntax check of `scripts/tasks.ps1` (PowerShell parser) and a count of non-ASCII bytes | 0 parse errors, 0 non-ASCII bytes. The file header requires ASCII-only |
| 2 | PowerShell | Flink REST `/jobs/overview` | 5 streaming jobs `RUNNING`, plus 2 `FINISHED` `reconcile-gold-2025-01-15` jobs |
| 3 | PowerShell | `.\tasks submit jobs/10_bronze_cdc_raw.sql` | **Refused, exit 1:** `job 'bronze-cdc-raw' is already running; submitting it again would duplicate its output ... add -Force to override.` Nothing submitted |
| 4 | PowerShell | `.\tasks jobs` | Exit 0. Skipped all 5 jobs (`'<name>' is already running`). Running jobs afterwards: still 5 |

I changed the final message of `.\tasks jobs` after test 4 and have not re-run it since.

### Files created / changed
| File | Change | Why |
|------|--------|-----|
| `scripts/tasks.ps1` | edited | Duplicate-job guard (below) |
| `docs/EXPERIMENTS.md` (E0 row) | edited | Added run 3 to Observed, Surprise (2) and the Evidence links |
| `docs/DESIGN.md` (F11, Observed column) | edited | Added run 3 and a link to Session 7 |
| `docs/BUILD_LOG.md` | edited | This entry |

### Code highlights
**The guard in `scripts/tasks.ps1`:**

```powershell
function Get-LiveJobNames {
    $jobs = (Invoke-RestMethod -Uri "$FlinkUrl/jobs/overview" -TimeoutSec 10).jobs
    $live = @($jobs | Where-Object { $_.state -notin @('FINISHED', 'FAILED', 'CANCELED') })
    return @($live | ForEach-Object { $_.name })
}

function Get-PipelineName([string]$RelPath) {
    # reads  SET 'pipeline.name' = '...';  from the SQL file
    $m = [regex]::Match([IO.File]::ReadAllText($src), "SET\s+'pipeline\.name'\s*=\s*'([^']+)'")
    if ($m.Success) { return $m.Groups[1].Value }
    return $null
}

# in Submit-Sql, for files under jobs/ unless -Force:
if ($RelPath -like 'jobs/*' -and -not $Force) {
    $pn = Get-PipelineName $RelPath
    if ($pn -and ((Get-LiveJobNames) -contains $pn)) { throw "job '$pn' is already running; ..." }
}
```

- **Job identity is the `pipeline.name`.** Every streaming job file already sets one (for example `bronze-cdc-raw`, `silver-pings`), so no SQL file needed to change.
- **`.\tasks jobs` is now idempotent.** `Submit-AllJobs` fetches the live job names once and skips any that are running. If one job has died, re-running `.\tasks jobs` resubmits only that one.
- **`-Force`** is a new script-level switch that turns the guard off.
- **Scope:** only files under `jobs/` are guarded. DDL and the batch repair file are not.
- **If Flink can't be queried,** the guard prints a warning and skips the check instead of blocking.
- A `PSUseApprovedVerbs` warning on `Render-Sql` appeared in the editor. It was already there before this change, so I left it.

### Errors & debugging
No errors. The test was run against the live stack from Session 7, which still had its 5 jobs.

### Limits of the guard
- It only blocks double submission through `tasks.ps1`. Starting a job by hand through the Flink SQL client, or in a way that gives a job a different `pipeline.name`, would bypass it.
- It does not change the slot budget. The cluster still has 8 task slots, sized for 5 streaming jobs plus one batch job.
- A job in a transient state such as `RESTARTING` counts as live and is not resubmitted. That is intentional, but it means a job stuck in a restart loop needs to be cancelled by hand.

### Evidence rows updated
Run 3 (2026-11-09, clean stack, after the double-submission incident was reset):

| Run | Late table rows | Gold rows missing before repair |
|-----|-----------------|----------------------------------|
| 1 (Session 2) | 4,219 | 242 |
| 2 (Session 4) | 4,212 | 242 |
| 3 (Session 7) | 4,206 | 242 |

- In `docs/EXPERIMENTS.md` (E0): the Observed column lists all three late counts and says the gold gap was 242 each time. Surprise (2) now says the missing-row count did not move. The Evidence cell has a new link to Session 7.
- In `docs/DESIGN.md` (F11): the Observed column has the same three counts, and the Evidence cell links Session 7.
- The existing raw-output link, `evidence/E0_reconcile_after.txt`, is still from run 2. No raw output was saved for run 3.

### Blog notes
- The incident went from diagnosis to a fix with a test in one sitting, which makes a short, complete story: symptom, root cause, guard, proof the guard works.
- Making the submit command idempotent is better than adding a warning. It removes the footgun instead of documenting it.
- Choosing an existing identity (`pipeline.name`) for the check kept the change small. There is no new state to keep in sync.

### Next steps
1. Run Experiment E2 (watermark bound sweep). Start from `.\tasks fresh`, and do not run `.\tasks jobs` afterwards.
2. Optional: save the run 3 `reconcile` and `reconcile-after` output under `evidence/` if you want raw proof for all three runs.
3. Loose ends from Session 6: run `sql\trino\02_maintenance.sql` (edited, never run) and inspect an empty 5-minute snapshot's `summary`.
4. Make `reconcile.py` fail on an empty lake and flag unexplained bronze extras.

---

## Session 9 — 2026-11-09 — E1 failure injection: TaskManager kill, MinIO outage, catalog outage (catalog case left the jobs in a restart loop)

### Goal
Run the three E1 failure cases (F5 worker, F7 object store, F8 catalog) against a seeded replay (`--seed 42 --speedup 10 --duration-min 120`) and see whether `reconcile` still passes afterwards.

### How this entry was assembled (read this before trusting the numbers)
- The commands below were run by you. I logged them from the output you pasted and from `chaos.log`.
- You did not note the Flink dashboard checkpoint ids while the cases ran. I could only query the Flink REST API (`/jobs/<id>/checkpoints`, `/jobs/<id>/exceptions`) afterwards, and only for the **stack that is still up, which is case 3**. `.\tasks fresh` between cases deleted the MinIO volume, and the checkpoints live there (`s3://flink/checkpoints/...`), so **the checkpoint ids for case 1 and case 2 are gone**.
- No `reconcile` output was pasted for case 2 or case 3. The only reconcile output is the saved file `evidence/E1_taskmanager_kill.txt` (case 1).
- All times are UTC (the Flink containers' clock). Your local zone is 5 h 30 min ahead, so the evidence file's modified time of 21:31:05 is 16:01:05 UTC.

### Commands run
| # | Time (UTC) | Case | Command | Result |
|---|-----------|------|---------|--------|
| 1 | about 15:3x | 1 | `.\tasks fresh`, then the replay in terminal A | Stack up, 5 jobs. Replay command: `python -m replayer run --source synth --seed 42 --speedup 10 --duration-min 120 --manifest manifest.json` |
| 2 | 15:43:33 | 1 | `.\tasks chaos taskmanager-kill` | `docker compose kill -s SIGKILL taskmanager`: container `Killed` in 0.9 s. Script then printed "restart policy brings it back" |
| 3 | right after 15:43:35 | 1 | `(Invoke-RestMethod .../jobs/overview).jobs ...` and `docker compose ps taskmanager` | All 5 jobs still showed `RUNNING`, **and `docker compose ps taskmanager` listed no container at all** |
| 4 | 16:01:05 | 1 | `.\tasks reconcile` (saved to `evidence/E1_taskmanager_kill.txt`) | **FAIL**, exit 1. See Results, case 1 |
| 5 | about 16:02 | 2 | `.\tasks fresh`, replay started in terminal A | Stack ready 16:03:50 (last job submitted). Same seed and speedup |
| 6 | 16:07:31 | 2 | `.\tasks chaos minio-outage 45` | MinIO stopped, started again, "minio back" at 16:08:18 (47 s) |
| 7 | after the replay | 2 | (no reconcile output was pasted) | **Case 2 outcome not recorded** |
| 8 | about 16:23 | 3 | `.\tasks fresh` (this wiped case 2 before it was checked), then the replay | Stack ready 16:24:55. Replay summary identical to the earlier one |
| 9 | 16:30:57 | 3 | `.\tasks chaos catalog-outage 45` | `iceberg-rest` stopped, started again, "catalog back" at 16:31:46 (49 s) |
| 10 | 16:48 | 3 | Flink REST queries and `docker compose logs iceberg-rest` (run by me, read-only) | All 5 jobs `RESTARTING`, 17 minutes after the catalog came back. See Results, case 3 |
| 11 | 17:06:29 | 3 | Saved the catalog log and Flink job state (run by me) | Wrote `evidence/E1_catalog_outage_catalog.log` (1.8 MB) and `evidence/E1_catalog_outage_flink_state.txt`. Jobs still `RESTARTING` 35 min after the catalog returned; bronze restored 39 times (22 at 16:48), newest checkpoint still chk-15 |
| 12 | 17:07:30 | 3 | `docker compose restart iceberg-rest`, then job states every 20 s for 3 min (run by me) | **Did not recover.** 4 of 5 jobs `RESTARTING` in every sample; `silver-pings` showed `RUNNING` once (17:09:32), then `RESTARTING` again. 51 error lines in the catalog log in the 3 minutes after the restart |
| 13 | about 17:13 | 3 | Read-only inspection of the catalog container: `ls -la /data`, `cat /proc/locks`, log grep (run by me) | `/data/iceberg_catalog.db` is 20 KB, last modified 17:07, no `-journal` or `-wal` file. `/proc/locks` shows one POSIX READ lock in SQLite's shared-lock byte range. Latest errors still `SQLITE_BUSY`. See Follow-up |
| 14 | 17:16:53 | 3 | Cancelled 4 jobs via Flink REST (`PATCH /jobs/<id>?mode=cancel`), kept `silver-pings` (run by me) | 4 jobs `CANCELED` |
| 15 | 17:17 to 17:20 | 3 | Sampled job states and `silver-pings` checkpoints every 20 s | `silver-pings` `RUNNING` throughout, checkpoint 15 to 21, 0 `SQLITE_BUSY` in the last 60 s. See Follow-up, part 5 |
| 16 | 17:20 | 3 | `docker compose run --rm --no-deps minio-init ls local/flink/checkpoints[/<job-id>]` (run by me, read-only) | Checkpoint folders of all 5 jobs still exist; bronze's holds `chk-16`, `shared/`, `taskowned/` |
| 17 | about 17:25 | 3 | Listed each job's `chk-N` folder and its files, and compared with Flink's latest completed id (run by me, read-only) | Each job has one retained checkpoint with a `_metadata` file. See Follow-up, part 6. My search of `jobs\*.sql` for restore settings failed (wrong path) |
| 18 | 17:27:36 | 3 | Wrote `flink/sql/_rendered/restore_silver_dim_versions.sql` (job SQL plus `SET 'execution.savepoint.path' = '...chk-21'`) and ran `docker compose exec -T jobmanager sql-client.sh -i init.sql -f` on it (run by me) | Job `95014d37e37d4a43e4cb4746472d9e16` submitted, `RUNNING` |
| 20 | 17:33:05 | 3 | Same method for `silver-trips-upsert`: `restore_silver_trips.sql` with `chk-14`, run through `sql-client.sh` (run by me) | Job `bd5f95a602c943b842522d3c691ca0e1` submitted, `RUNNING` |
| 21 | 17:33 to 17:35 | 3 | Sampled states and checkpoints every 20 s; counted `SQLITE_BUSY` | `restored` = chk-14 (`is_savepoint: true`); 4 checkpoints, 0 failed; 3 jobs `RUNNING`; 0 `SQLITE_BUSY` |
| 22 | 17:37:20 | 3 | Same method for `gold-demand-supply`: `restore_gold.sql` with `chk-14` (run by me) | Job `60e24cf873dde77a2183351a5499307c` submitted, `RUNNING` |
| 23 | 17:37 to 17:40 | 3 | Sampled states and checkpoints every 20 s; counted `SQLITE_BUSY` | `restored` = chk-14; 4 checkpoints, 0 failed; `silver-pings` `RESTARTING` for about 75 s then `RUNNING`; 2 `SQLITE_BUSY` traces |
| 24 | about 17:41 | 3 | Read `silver-pings` exceptions and checkpoint counts (read-only) | `CommitStateUnknownException` at 17:32:25 and 17:37:41; 54 restores, 54 of 54 checkpoints completed |
| 25 | 17:41:49 | 3 | Same method for `bronze-cdc-raw`: `restore_bronze.sql` with `chk-16` (run by me) | Job `d38896e8a8630ae482480d3351f3a058` submitted |
| 26 | 17:42 to 17:45 | 3 | Sampled states and checkpoints every 20 s; counted `SQLITE_BUSY` | `restored` = chk-16; 4 checkpoints, 0 failed; all 5 jobs `RUNNING` by 17:42:43; `gold` and `silver-pings` each restarted once and recovered; 9 `SQLITE_BUSY` traces |
| 27 | 17:47:49 | 3 | Waited 2 minutes, then `.\tasks reconcile *>&1 \| Out-File -Encoding utf8 evidence\E1_catalog_outage.txt` (run by me) | 7 PASS, 2 INFO. Gold 227 of 24,252 missing. Evidence file written |
| 28 | 17:48:13 | 3 | `.\tasks reconcile-gold` saved to `evidence/E1_catalog_outage_reconcile_gold.txt` (run by me) | Both `INSERT OVERWRITE` statements completed (the stderr noise in the file is the usual `jdk.compiler` warning) |
| 29 | 17:49:28 | 3 | `.\tasks reconcile-after` saved to `evidence/E1_catalog_outage_after.txt` (run by me) | 8 PASS, 1 INFO, no FAIL. `gold=24252 expected=24252` |
| 19 | 17:28 to 17:30 | 3 | Sampled job states and the new job's checkpoints every 20 s; counted `SQLITE_BUSY` | `restored` = chk-21 (`is_savepoint: true`); 5 checkpoints completed, 0 failed; 2 jobs `RUNNING`; 0 `SQLITE_BUSY` |

Replay summary (both runs that were pasted): `events_emitted 219576, pings_unique 216000, pings_dup_emitted 2199, pings_late_injected 4381, trips_distinct 231, wall_seconds 1083.3`.

### Files created / changed
| File | Change | Why |
|------|--------|-----|
| `evidence/E1_taskmanager_kill.txt` | created by you | Raw `reconcile` output for case 1 |
| `evidence/E1_catalog_outage_catalog.log` | created (command 11) | Full `iceberg-rest` log from before the restart, 1.8 MB |
| `evidence/E1_catalog_outage_flink_state.txt` | created (command 11) | Per-job state, checkpoint counts, restore point and latest exception at 17:06:29 UTC |
| `flink/sql/_rendered/restore_*.sql` (4 files: dim, trips, gold, bronze) | created (commands 18, 20, 22, 25) | One-off copies of the job SQL with a `SET 'execution.savepoint.path'` line; the repo's own job files are unchanged. The `_rendered` folder is generated, so they can be deleted |
| `evidence/E1_catalog_outage.txt`, `_reconcile_gold.txt`, `_after.txt` | created (commands 27 to 29) | Raw `reconcile` output for case 3 |
| `chaos.log` | appended by `tasks.ps1` | Start and end of each injected failure |
| `docs/EXPERIMENTS.md`, `docs/DESIGN.md`, `docs/BUILD_LOG.md` | edited | E1 row, F5/F7/F8 Observed cells, this entry |

### Results

**Case 1: TaskManager kill (F5). Check failed; cause not established.**
`evidence/E1_taskmanager_kill.txt` (run 17 min after the kill):

| Check | Result | Value |
|-------|--------|-------|
| pings: no loss | FAIL | silver distinct 57,803 of 216,000 |
| pings: no duplicates | PASS | 57,803 rows = 57,803 distinct |
| trips: one latest row per trip | PASS | 85 rows, 85 distinct |
| trips: none lost | FAIL | 85 of 231 |
| trips: final status counts | FAIL | completed 32, accepted 10, in_progress 42, requested 1 (expected completed 231) |
| trips: silver == Postgres | FAIL | same as above |
| bronze: every trip CDC event landed | FAIL | 269 of 924 |
| late audit table | INFO | 954 rows |
| gold supply missing | INFO | 991 of 6,531 (15.17%) |

- **What the numbers say:** the lake is a prefix of the run. Everything that is there is correct (no duplicates, one row per trip), but the jobs stopped far short of the end. Bronze at 269 of 924 and pings at 27% point to a stall, not random loss.
- **What I cannot say:** whether this was a stall that would have caught up with more time, or a TaskManager that never came back. Two facts point at the second: `docker compose ps taskmanager` showed no container a moment after the kill, and the jobs still said `RUNNING` (the JobManager had not noticed yet). `docker-compose.yml` has `restart: unless-stopped` on the taskmanager. Nothing in the paste shows whether it restarted. The Flink UI was not checked again before `fresh` wiped the stack.
- **No checkpoint id, no restart time, no exception text** was captured for this case.
- **Caution:** it is not stated whether the replay had finished when `reconcile` ran.

**Case 2: MinIO outage (F7). No result recorded.** The outage ran 16:07:31 to 16:08:18. The stack was wiped by `fresh` at about 16:23 without a `reconcile` being saved, so the lake and the checkpoints are gone.

**Case 3: catalog outage (F8). Jobs did not recover on their own.**
Timeline:

| Time (UTC) | Event |
|-----------|-------|
| 16:30:57 | `iceberg-rest` stopped |
| 16:31:02 to 16:31:24 | Last completed checkpoint of each job (table below). Nothing completes after this |
| 16:31:46 | `iceberg-rest` started again |
| 16:31:56 | First error in the catalog log: `SQLiteException: [SQLITE_BUSY] The database file is locked`, surfaced to clients as HTTP 500 `UncheckedSQLException: Unknown failure` |
| 16:35 onwards | Every job fails about once a minute with `CommitStateUnknownException`, restarts, restores, fails again |
| 16:48 | All 5 jobs `RESTARTING`; the catalog log has 436 error-like lines |

Checkpoint data from the Flink REST API at 16:48:

| Job | Checkpoints completed | Failed | Total | Restores so far | Last completed (trigger time, UTC) | Restored from |
|-----|----------------------:|-------:|------:|----------------:|-----------------------------------|---------------|
| bronze-cdc-raw | 15 | 1 | 16 | 22 | chk-15 at 16:31:24 | chk-15 |
| silver-trips-upsert | 14 | 1 | 15 | 23 | chk-14 at 16:31:02 | chk-14 |
| silver-pings | 14 | 0 | 14 | 23 | chk-14 at 16:31:24 | chk-14 |
| silver-dim-versions | 14 | 0 | 14 | 27 | chk-14 at 16:31:22 | chk-14 |
| gold-demand-supply | 13 | 8 | 21 | 35 | chk-13 at 16:31:12 | chk-13 |

- Checkpoints ran every 30 s as configured (silver-pings: chk-7 at 16:27:54 up to chk-14 at 16:31:24).
- Checkpoint paths are `s3://flink/checkpoints/<job-id>/chk-N`. The checkpoint store (MinIO) was healthy throughout; the failing dependency is the catalog.
- Every restore goes back to the same checkpoint, which contains an Iceberg commit that was never confirmed. The commit is retried, and the catalog answers 500 again.
- Gold has 8 failed checkpoints against 0 to 1 for the others. I have not looked at why.
- The exception text itself says: *"Cannot determine whether the commit was successful or not ... Retrying an already successful operation will result in duplicate records."* That is the Iceberg sink admitting it cannot tell whether the commit landed.
- The Flink exception history keeps only the 16 most recent entries, so the first exception per job is not visible any more.

### Errors & debugging

**Error A (open): all 5 jobs stuck in a restart loop after a 49 s catalog outage**
- **Context:** `.\tasks chaos catalog-outage 45` during the replay.
- **Immediate cause (observed):** the REST catalog returns 500 because its SQLite database is locked (`SQLITE_BUSY`). The catalog is the `iceberg-rest` container with a SQLite file in the `restdata` volume.
- **Why it does not clear (not established):** the lock was still in place 17 minutes after the restart. Candidates, none tested:
  1. After the restart five jobs retry their commits at once, and SQLite allows one writer. If the driver has no busy timeout, the losers get `SQLITE_BUSY` immediately and the jobs restart, so they collide again each cycle.
  2. The container was stopped while a write was in progress and left a lock or a hot journal.
- **The 45 s outage is not the whole story:** the jobs stayed down for more than 17 minutes after the catalog was back. The outage tripped a failure mode that lasts as long as the retry pattern does.
- **Lesson:** a single-writer SQLite catalog behind five concurrent committers is a weak point the design notes did not predict. The recovery path (retry the same commit) depends on the catalog accepting a burst of commits right after it comes back.
- **Related:** Session 8's guard counts `RESTARTING` as live, so `.\tasks jobs` would skip these jobs. Session 8 already listed "a job stuck in a restart loop needs to be cancelled by hand" as a limit. This is that case.

**Error B: case 1 `reconcile` FAIL** is described under Results. It is not yet known whether it is a bug in the pipeline's recovery or in the experiment's setup.

**Process errors (mine, not the pipeline's):**
- Case 2 and case 3's `fresh` ran before the result of the previous case was saved. Same shape as the Session 3 mistake, and it cost the case 2 result and the checkpoint ids of cases 1 and 2.
- Case 3's failure was injected at about minute 6 of the replay rather than minute 2, and case 2's at about minute 3.
- Checkpoint ids and the restart evidence were not noted from the Flink UI.

### Observation: `wall_seconds` is explained
`wall_seconds` is 180.6 at 60x (E0, every run) and 1083.3 at 10x (both E1 replays). Multiplying by the speedup gives 10,836 and 10,833 simulated seconds, about 180.6 simulated minutes, both times. So the replay is not running slow: the seeded event stream spans about 180.6 simulated minutes, not 120 (my inference from the arithmetic; I have not read the replayer to confirm what extends it). This closes two loose ends: "181 s against 120 s nominal" and "why exactly 180.6 every run". The code measures real time (`round(time.time() - t_wall, 1)` in `replayer/cli.py`), and with a fixed pacing the result repeats.

### Blog notes
- The headline story: the exactly-once claim looked solid in the baseline, then the first real fault (a 49-second catalog restart) produced a state in which nothing moved for 17 minutes and nobody was told. Flink reported `RESTARTING`, the data stopped at 16:31, and the only signal was the dashboard.
- "At-least-once retry plus idempotent commit" is only as good as the dependency being retried. The retry was correct; the thing it retried against could not take it.
- The Iceberg exception `CommitStateUnknownException` is worth a section: a commit can fail in a way where the client cannot know whether it landed, and the library's advice is to check before retrying.
- Process lesson: write down checkpoint ids and times while the experiment runs. They are the evidence, and `fresh` deletes them. Save the `reconcile` output before the next `fresh`.
- Do not use case 1 as a pass or a fail until its cause is known.

### Follow-up (17:06 to 17:15 UTC): saving the evidence, restarting the catalog, inspecting the lock

**1. Evidence saved (command 11).** The jobs were still stuck 35 minutes after the catalog came back. Bronze had restored 39 times (22 at 16:48) and its newest completed checkpoint was still chk-15 from 16:31:24, so nothing has been committed since then.

**2. Restarting the catalog did not fix it (command 12).**
- Restart at 17:07:30. Job states sampled every 20 s for 3 minutes: 4 of 5 `RESTARTING` every time; `silver-pings` was `RUNNING` in one sample (17:09:32) and `RESTARTING` in the next.
- 51 error lines in the catalog log in the 3 minutes after the restart. The sampled stack frames are in `JDBC3PreparedStatement.executeUpdate`, so the failing statement is a write.
- **What this rules out:** a stale lock left behind by the process that was stopped at 16:30:57. A freshly started catalog process hits the same error.

**3. Inspecting the lock (command 13, read-only).**

| Observation | Value |
|-------------|-------|
| Catalog database | `jdbc:sqlite:file:/data/iceberg_catalog.db` (from `docker-compose.yml`) |
| File in `/data` | `iceberg_catalog.db`, 20,480 bytes, modified 17:07 (the restart). No `-journal` or `-wal` file |
| Locks (`/proc/locks`) | One POSIX advisory READ lock, held by PID 1 (the catalog's own Java process), on byte range 1073741826 to 1073742335 |
| Latest errors | `SQLiteException: [SQLITE_BUSY] The database file is locked (database is locked)` |
| Job state | All 5 `RESTARTING` |

- **Reading the lock:** SQLite's "shared" lock lives at byte 1073741826 and is 510 bytes long (that range is how SQLite implements a shared lock on a POSIX file). So the catalog process itself holds a shared (read) lock on its own database. A writer needs an exclusive lock, which it cannot get while another connection holds a shared one. That matches `SQLITE_BUSY` on every `executeUpdate`.
- **Inference, not proven:** a connection in the catalog's pool is holding a read open (an unfinished statement or transaction) and never releases it, so every commit from the Flink jobs is refused. I did not see which statement holds it. The database file has not changed since 17:07, so no commit has succeeded since the restart.
- **Why this fits both observations:** it appeared again within minutes of a fresh start (so it is not left over from the kill), and the catalog's SQLite setup has no busy timeout in `docker-compose.yml`, so a writer that finds the lock held fails at once instead of waiting.
- **Still open:** the earlier idea that five jobs retrying at once start the problem is not ruled out as the trigger. What the inspection shows is that the lock, once taken, is not released.
- The one `RUNNING` sample for `silver-pings` shows a commit can sometimes get through, which is consistent with contention rather than a permanent corruption.

**4. Mistake in my inspection command:** the `for p in /proc/[0-9]*` loop was written with a quoting error and failed with a shell syntax error, so I do not have a list of which file descriptors hold the database open. The lock evidence above comes from `/proc/locks` only.

**5. Isolation test: contention confirmed (commands 14 to 16).**
- At 17:16:53 UTC I cancelled four jobs through the Flink REST API (`PATCH /jobs/<id>?mode=cancel`): `silver-dim-versions`, `gold-demand-supply`, `bronze-cdc-raw`, `silver-trips-upsert`. I left `silver-pings` running.
- Within 20 s `silver-pings` was `RUNNING` and stayed `RUNNING` in all 10 samples (17:17:14 to 17:20:14). Its checkpoint id went from 15 to 21, one about every 30 s, the first checkpoint progress since 16:31:24.
- `SQLITE_BUSY` count in the catalog log for the last 60 s: **0**. The database file was last modified 17:17:11.
- **Conclusion:** the same catalog, with the same lock-prone SQLite file, serves one committer without trouble and fails with five. The failure is contention among the jobs' commits, not a broken catalog. (One job is a sample of one; I have not yet re-added the others to see where it breaks again.)
- **Not verified:** that `silver-pings` has caught up on the data it missed. The database file shows one write at 17:17:11 and none since. That may only mean the sink has nothing new to commit now that the replay is over. `reconcile` will show.
- **My oversight, to fix before resubmitting anything:** I should have said before cancelling that a cancelled job is gone, and resubmitting it would start from `earliest-offset` with empty state. For the append-only tables (bronze, gold) that would write the whole topic a second time. I checked afterwards: `s3://flink/checkpoints/<job-id>/` still exists for all five jobs (for bronze it holds `chk-16`, `shared/`, `taskowned/`), so the checkpoints survived the cancel. Restoring from them, not starting fresh, is the safe path. I have not yet confirmed which chk directory of each job is complete (it needs a `_metadata` file), or that the SQL client can restore from it.
- Housekeeping: the two `docker compose run --rm minio-init ls ...` calls created and removed throwaway containers. Nothing else changed.

**6. The retained checkpoints are complete (command 17, read-only).** I listed each job's folder under `s3://flink/checkpoints/<job-id>/`:

| Job | State | Retained checkpoint | Has `_metadata` |
|-----|-------|--------------------|-----------------|
| bronze-cdc-raw | CANCELED | `chk-16` | yes |
| silver-trips-upsert | CANCELED | `chk-14` | yes |
| silver-dim-versions | CANCELED | `chk-21` | yes |
| gold-demand-supply | CANCELED | `chk-14` | yes |
| silver-pings | RUNNING | `chk-27` | yes |

- Only the **newest** checkpoint of each job is kept (Flink deletes older ones as new ones complete). That is enough: restoring needs only the newest.
- Some of these are newer than the ones I reported at 16:48 (for example dim is at chk-21, was chk-14). During the restart loop a job sometimes got far enough to complete a checkpoint before the next commit failure. Checkpoint ids still advanced a little even though no data was being committed.
- The cancelled jobs' checkpoints are **retained on cancel**. I did not look for the setting that does that; it is the reason this works at all.
- Still unconfirmed: that the SQL client accepts `execution.savepoint.path` pointing at a retained checkpoint for an unchanged `INSERT`. It is the first thing to try on one job.

**7. First restore: `silver-dim-versions` from `chk-21` works (commands 18 to 19).**
- Where the SQL lives: `flink/sql/jobs/*.sql`. `tasks.ps1` copies each file into `flink/sql/_rendered/` and runs it with `sql-client.sh -i /opt/flink/sql/init.sql -f ...` inside the jobmanager container (`Submit-Sql`, line 138).
- I did not edit the repo's job file. I wrote a one-off copy, `flink/sql/_rendered/restore_silver_dim_versions.sql`, which is the original file with one line added at the top:
  ```sql
  SET 'execution.savepoint.path' = 's3://flink/checkpoints/306c65eed773fba1c9440ef9ff2ae4fe/chk-21';
  ```
  and ran it with the same `sql-client.sh` command. This bypasses `.\tasks submit`, so the double-submission guard did not apply (the old job was `CANCELED`, so it would not have blocked anyway).
- Submitted 17:27:36 UTC. New job id `95014d37e37d4a43e4cb4746472d9e16`.
- **Proof it restored, not restarted from scratch:** the Flink REST API reports `restored: {"id":21, "is_savepoint":true, "external_path":".../306c65eed773fba1c9440ef9ff2ae4fe/chk-21"}` for the new job. Flink treats a retained checkpoint passed this way as a savepoint (`is_savepoint: true`).
- **Two committers are fine:** over the next 2 minutes the new job completed 5 checkpoints with 0 failed, `silver-pings` stayed `RUNNING`, and the catalog log had 0 `SQLITE_BUSY` lines.
- **Observations to keep in mind:**
  - The state restored from the checkpoint belongs to the old job id. The new job writes new checkpoints under its own id (`s3://flink/checkpoints/95014d37.../`).
  - The old job id's folder is still there. It can be removed later with `remove_orphan_files`-style housekeeping on the bucket; I have not touched it.
  - I have not verified that the rows the old job never committed have now appeared in `silver.driver_versions`. `reconcile` does not check that table, so a direct Trino count is the way.

**8. Second restore: `silver-trips-upsert` from `chk-14` works (commands 20 to 21).**
- Same method as part 7, file `flink/sql/_rendered/restore_silver_trips.sql` (copy of `20_silver_trips.sql` plus the `SET 'execution.savepoint.path'` line). Submitted 17:33:05 UTC, new job id `bd5f95a602c943b842522d3c691ca0e1`.
- Flink reports `restored` = chk-14, `is_savepoint: true`. 4 checkpoints completed, 0 failed in the first 2 minutes.
- Three jobs `RUNNING` (`silver-pings`, `silver-dim-versions`, `silver-trips-upsert`). `SQLITE_BUSY` in the last 2 minutes: **0**.
- Not yet verified: that the trips the old job never committed are now in `silver.trips` (`reconcile` will check this against Postgres once all jobs are back).

**9. Third restore: `gold-demand-supply` from `chk-14`. First sign of contention returns, and it heals (commands 22 to 24).**
- Same method, file `flink/sql/_rendered/restore_gold.sql`. Submitted 17:37:20 UTC, new job id `60e24cf873dde77a2183351a5499307c`. Flink reports `restored` = chk-14, `is_savepoint: true`. 4 checkpoints completed, 0 failed in the first 2 minutes.
- **What changed with four jobs:** `silver-pings` went `RESTARTING` at about 17:37:41 and was `RUNNING` again by 17:38:56 (about 75 s). Its exception was `CommitStateUnknownException: Service failed: 500: Unknown failure`, the same family as the loop. The catalog log had 2 `SQLITE_BUSY` stack traces in the 4 minutes up to 17:41.
- **A second, earlier episode:** the same job also hit that exception at 17:32:25, when only three jobs were involved (`silver-pings`, `silver-dim-versions`, and trips about to be submitted at 17:33:05). So collisions are not limited to four jobs; they are occasional at any count, and the earlier 0-error windows were just luck.
- **The difference from the 16:31 loop:** each time, `silver-pings` restarted once, retried its commit, and got through. Earlier, all five jobs failed again and again for over 40 minutes. Occasional collisions that heal are the retry mechanism working. The loop needs sustained collisions.
- `silver-pings` has now restored 54 times in total over the whole session (it was 23 at 16:48), with `completed` equal to `total`, 54 of 54.
- **Reading, not proven:** the number of jobs raises the chance of a collision, and the loop at 16:31 started because a burst of simultaneous commits right after the catalog came back kept colliding. I have not measured collision frequency per job count.

**10. What the two catalog errors do and do not show.** `CommitStateUnknownException` says Flink does not know whether the commit landed. In the cases here the catalog failed to take a write lock, so the commit most likely did not land, and the retry after restore is correct. I have not checked the `driver_pings` snapshot list for a double commit; `reconcile` (no duplicates check) will show if one happened.

**11. Fourth restore: `bronze-cdc-raw` from `chk-16`. All five jobs run again, with transient restarts (commands 25 to 26).**
- Same method, file `flink/sql/_rendered/restore_bronze.sql`. Submitted 17:41:49 UTC, new job id `d38896e8a8630ae482480d3351f3a058`. Flink reports `restored` = chk-16, `is_savepoint: true`. 4 checkpoints completed, 0 failed in the first 3 minutes.
- **Five jobs `RUNNING` at 17:42:43 and 17:44:43.** This is the first time all five have run since 16:31:24, 73 minutes earlier.
- **Contention at five jobs, but it heals:**
  - `gold-demand-supply` was `RESTARTING` at 17:42:23 and `RUNNING` again 20 s later.
  - `silver-pings` was `RESTARTING` from 17:43:43 to 17:44:23 (about 60 s) and `RUNNING` again at 17:44:43.
  - 9 `SQLITE_BUSY` stack traces in the 3 minutes after submission (2 in the 4 minutes before it).
- **Same five jobs, opposite outcome to 16:31:** then they failed in step and never recovered; now each fails alone, restarts once and gets through. What differs: the 16:31 loop started from a burst of commits right after the catalog came back, with all five jobs restoring at once. Now the jobs were added about 4 minutes apart, so their commit timing is spread out. That is a reading from this one run, not a tested cause.
- **This is not a fix.** The catalog still has no busy timeout and still returns 500 on collision. The system is only healthy because the replay is over and the jobs have little to commit. Under the E1 load (a busy replay) it may fail the same way again.
- Not verified yet: whether any of the restarted commits wrote data twice. `reconcile` is the check.

**12. The case 3 verdict: all checks PASS after manual recovery (commands 27 to 29).**
Order followed: `reconcile`, then `reconcile-gold`, then `reconcile-after`, with no `fresh` in between. Raw output saved as `evidence/E1_catalog_outage.txt`, `evidence/E1_catalog_outage_reconcile_gold.txt` and `evidence/E1_catalog_outage_after.txt`.

| Check | Case 1 (TM kill, 16:01) | Case 3, `reconcile` (17:47) | Case 3, `reconcile-after` (17:49) |
|-------|-------------------------|-----------------------------|-----------------------------------|
| pings no loss | FAIL 57,803 of 216,000 | PASS 216,000 | PASS 216,000 |
| pings no duplicates | PASS | PASS 216,000 = 216,000 | PASS |
| trips one row each | PASS (85) | PASS 231 | PASS 231 |
| trips none lost | FAIL 85 of 231 | PASS 231 | PASS 231 |
| trips status counts and = Postgres | FAIL | PASS `completed 231` | PASS |
| bronze (>=, at-least-once) | FAIL 269 of 924 | PASS 924 = 924, extra 0 | PASS |
| late audit table | INFO 954 | INFO 2,263 | INFO 2,263 |
| gold supply | INFO 991 of 6,531 missing | INFO 227 of 24,252 missing (0.94%) | PASS 24,252 = 24,252 |

- **Reading the verdict:** after a 49 s catalog outage, a lock-driven restart loop of over 40 minutes, four cancelled jobs, and four manual restores, the lake holds every ping once and every trip once, equal to Postgres. The exactly-once claim held. The thing that did not hold is *recovering on its own*.
- **Why that is possible:** nothing was lost because Kafka kept every event, checkpoints kept each job's read position and pending commit, and the restores resumed from exactly those. Data could not be lost while the sinks were failing because failed commits are not acknowledged. The cost was staleness: from 16:31 to about 17:45 the lake did not move.
- **Why no duplicates despite the "commit state unknown" retries:** every retried commit came from a restore, and a restore replays the pending commit once. The pings check (216,000 rows, 216,000 distinct) is the proof. Bronze has `extra=0`, so no restore wrote extra rows there.
- **The late audit table is lower than baseline** (2,263 against about 4,210 in E0) and gold missing is slightly lower (227 against 242). Both are `INFO`, not checks. Plausible cause, not tested: late-ping classification uses the watermark at the moment each ping is processed. After the stall, the backlog was processed in catch-up bursts, which moves the watermark differently from a steady replay. Same effect as the run-to-run variation seen in E0, now larger.
- **Still flapping at the end:** at 17:47:49 bronze was `RESTARTING`, and at 17:49:28 `silver-pings` was. After the final reconcile all five jobs were running or restarting, none stuck. The reconcile results do not depend on that.
- **Open:** why `silver-pings` and `bronze-cdc-raw` keep restarting occasionally; the catalog is still unfixed.

**13. Data points for the blog.**
- 49 s outage, about 74 minutes to full health (16:30:57 to about 17:45), with four of those minutes of effective work by me per job.
- Restore counts: `silver-pings` had restored 54 times by 17:40.
- The per-case summary: case 1 inconclusive (not enough evidence kept), case 2 not recorded, case 3 data safe but availability lost without manual action.

### What this session teaches (for the blog)

**1. How an Iceberg sink in Flink stays exactly-once, and where it can break.**
- Each job writes data files to object storage as it runs. Nothing is visible to readers yet.
- On every checkpoint (every 30 s here) the sink records which files belong to that checkpoint in its state.
- When the checkpoint is confirmed complete, the sink does the **commit**: one call to the catalog saying "table X now includes these files". That call is the only thing that makes the data visible.
- If the job dies between "files written" and "commit succeeded", Flink restores from the last checkpoint and **repeats the commit**. That repeat is what makes it exactly-once rather than lossy.
- The weak point is the commit call. If the catalog answers with an error that does not say whether the commit landed, Iceberg raises `CommitStateUnknownException`. Flink cannot safely continue, so it restarts the job, restores, and tries the same commit again. If the catalog keeps failing, this repeats forever. That is the loop seen here: 22 to 39 restores per job, no new data visible, no alarm.

**2. Why a SQLite catalog fails with five writers.**
- SQLite lets many connections **read** at once but only one **write**. To write, a connection must get an exclusive lock, and it cannot while any other connection holds a shared (read) lock.
- If a writer finds the lock held, SQLite either waits (when a **busy timeout** is configured) or fails immediately with `SQLITE_BUSY`. Nothing in `docker-compose.yml` sets a timeout.
- Five jobs commit about every 30 s each, and each commit reads and then writes the catalog. Collisions are likely, and without a wait every collision is a failure. A failed commit restarts a job, and the restarted jobs commit again at about the same moment: the collisions feed themselves.
- One job alone never collides with itself. That is why `silver-pings` ran cleanly the moment the others stopped.

**3. Why restarting the catalog did not help.** The lock was held by the running catalog process while it served requests, not left on disk. A restart began the same contention again within seconds. When a fix does not work, that is evidence about the cause: it ruled out "leftover lock from the kill".

**4. Cancelling is not pausing.** Cancelling a Flink job ends it. A resubmission is a new job with new state; for an append-only sink it would write the topic's data a second time. To continue where it left off you restore from a retained checkpoint or savepoint. Check what survives *before* you cancel.

**5. Why the outage was longer than the failure.** The catalog was down 49 s. The jobs stayed down 45 minutes. Recovery logic (retry the same commit, from the same checkpoint) turned a short outage into a lasting one because the retries themselves overloaded the thing that had just come back. This pattern (a retry storm) is common in distributed systems.

**6. Process lessons from this session.**
- Write down the facts while the experiment runs (checkpoint ids, times). A later `fresh` deletes them.
- Save the check output (`reconcile`) before resetting.
- State the side effects of an action before running it (my cancel of four jobs).
- Ask what an explanation predicts before accepting it. "Stale lock" predicted that a restart would fix it, and it did not.

### Next steps (the case 3 stack is still up; do not run `fresh` yet)
1. **Done:** evidence saved; catalog restarted (no recovery); lock inspected; four jobs cancelled and `silver-pings` shown to run alone; retained checkpoints confirmed complete.
2. **Done, all 4:** `silver-dim-versions` (part 7), `silver-trips-upsert` (part 8), `gold-demand-supply` (part 9), `bronze-cdc-raw` (part 11). All five jobs run. **Next:** wait about 2 more minutes so the backlog is committed, then run the `reconcile` sequence below (step 4). Use the same method (one-off copy of the job file with the `SET` line). Record at which job count `SQLITE_BUSY` returns, if it does.
3. **Fix candidates to try afterwards, one at a time:** give the SQLite connection a busy timeout, or stop pointing five committers at a single-writer SQLite file (for example a Postgres-backed JDBC catalog, which the stack already has). Record which one changes the outcome.
3a. **Done (part 12):** reconcile sequence run on the recovered stack, all PASS. Remaining from here: fix the catalog (below), then redo cases 1 and 2 properly.
4. **(Done, kept for reference) After recovery:** wait about 90 s, run `.\tasks reconcile *>&1 | Out-File -Encoding utf8 evidence\E1_catalog_outage.txt`, then `reconcile-gold` and `reconcile-after`. Check whether the unknown commits produced duplicates.
5. **Redo cases 1 and 2** from `fresh`, one at a time. This time keep the Flink UI open, note the checkpoint ids and the time each job returns to `RUNNING`, and run `docker compose ps taskmanager` every 10 s for the first minute after the kill. Save `reconcile` before the next `fresh`.
6. **Loose ends still open:** `02_maintenance.sql` untested; `reconcile.py` should fail on an empty lake and flag unexplained bronze extras; the E2 sweep.

---

## Session 10 — 2026-11-09 — Fixing the catalog and the tooling, then a rerun that stalled for the wrong reason

### Goal
Fix the problems found in Session 9 (the SQLite catalog, the missing evidence, the weak `reconcile`, the unexplained TaskManager behaviour), then rerun the three E1 failure cases with the fixes. You asked for full autonomy and for every step to be logged. Later you asked me to stop if the run was deadlocked and move on.

Times are UTC. Everything below was run by me unless it says otherwise.

### Part 1. What I fixed

**Fix 1: the Iceberg catalog now stores its metadata in Postgres, not SQLite**
- **Why:** Session 9 showed the SQLite file lock (`SQLITE_BUSY`) turning one catalog restart into a restart loop for all five jobs. SQLite allows one writer, and nothing in the stack set a busy timeout.
- **Check before choosing:** I copied the catalog's jar out of the container and looked inside it. It bundles both the SQLite and the PostgreSQL JDBC drivers (`org/postgresql/Driver.class` present), so no extra download was needed.
- **Files changed:**

| File | Change |
|------|--------|
| `infra/postgres/02-catalog-db.sql` | **New.** `CREATE DATABASE iceberg_catalog;`, with a comment saying why. Postgres runs it only when its data volume is empty (`fresh` does that) |
| `docker-compose.yml`, `postgres` service | Mounts the new file as `02-catalog-db.sql`. Healthcheck now runs `psql ... -d iceberg_catalog -c 'select 1'`, so "healthy" means the catalog database exists |
| `docker-compose.yml`, `iceberg-rest` service | `CATALOG_URI` is now `jdbc:postgresql://postgres:5432/iceberg_catalog`, plus `CATALOG_JDBC_USER` and `CATALOG_JDBC_PASSWORD`. Added `restart: unless-stopped` and `depends_on: postgres: service_healthy`. Removed `user: "0"` and the `/data` volume mount, which existed only for the SQLite file |

- **Left over:** the `restdata:` entry under the top-level `volumes:` is still declared and now unused. Harmless; I did not remove it.
- **Verified after `fresh`:** the catalog log shows `catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog ... uri=jdbc:postgresql://postgres:5432/iceberg_catalog`. In Postgres, `iceberg_catalog` has the tables `iceberg_tables` and `iceberg_namespace_properties`, and `iceberg_tables` lists all 8 lakehouse tables (bronze.cdc_raw; gold.h3_demand_1m, h3_supply_1m; silver.driver_pings, driver_pings_late, driver_versions, merchant_versions, trips). All 5 jobs `RUNNING`.
- **Not verified:** that Postgres actually fixes the failure. The catalog outage case did not run (see Part 2). So this fix is in place and the stack starts on it, but it has not been tested against the thing it was meant to fix.
- **Evidence:** `evidence/fix_fresh_postgres_catalog.txt` (output of the `fresh`).

**Fix 2: `reconcile` no longer passes when it should not** (`scripts/reconcile.py`, `tests/test_reconcile.py`)
- New check **`lake is not empty`**: FAIL if a core table has no rows while the manifest expects data. (Session 3: a wiped lake passed several checks.)
- The bronze check was `>=`, which let a duplicated job through with `extra=924` (Session 7). It is now exact: **`bronze: every trip CDC event landed, no unexplained extras`** fails on any extra. New flag `--allow-bronze-extras` for runs with a Debezium restart (E5), where extras are legitimate.
- The after-repair gold check could pass as `0 == 0` on an empty lake. It now also requires `expected > 0`.
- `tasks.ps1` now passes extra arguments through, so `.\tasks reconcile --allow-bronze-extras` works.
- 3 new unit tests (unexplained extras fail, empty lake fails, `0 == 0` gold does not pass). **Result: 21 tests passed, `ruff` clean.**
- The new check earned its keep within the hour: on the rerun it flagged `silver.trips, bronze.cdc_raw (trips)` as empty (Part 2).

**Fix 3: tooling so the evidence is not lost again** (`scripts/tasks.ps1`)

| New | What it does |
|-----|--------------|
| `Save-FlinkState` / `.\tasks flink-state [label]` | Writes job states, checkpoint counters, the last completed checkpoint path, the restore point and the latest exception of every live job to `evidence/flink_state_<label>_<time>.txt` |
| `chaos` for `taskmanager-kill`, `minio-outage`, `catalog-outage` | Saves Flink state before, right after, and after recovery. Then `Watch-Recovery` polls every 10 s and logs changes. "Recovered" means every job `RUNNING` with a checkpoint completed after the failure ended, 6 polls (60 s) in a row. Gives up after 600 s with "NOT RECOVERED". The catalog action also waits for the catalog to answer before starting the clock |
| `.\tasks cancel <pipeline-name>` | Cancels one job; its newest checkpoint is kept |
| `.\tasks restore <job file> [checkpoint]` | Resumes a cancelled job from its retained checkpoint (the manual method from Session 9, wrapped). Finds the checkpoint itself if you give none. Refuses if the job is still live |
| Help text | Documents all of the above |

- **Checked:** the file parses with 0 errors, is ASCII-only, and the lint warning for an unapproved verb is gone (I renamed the helper to `Get-NowMs`).
- **Actually exercised on the rerun:** `Save-FlinkState` (it wrote the files in `evidence/`) and `Watch-Recovery` (it logged the state changes and gave up at 600 s as designed).
- **Not yet exercised:** `restore`, `cancel`, and the catalog and MinIO versions of the chaos actions.

### Part 2. The rerun: case 1 stalled, and the cause was my test harness

**What I ran:** a script (`e1b.ps1`, kept in my scratchpad) that, for each case, does `fresh`, starts the replay at 10x, injects the failure 2 minutes in, waits for the replay and 2 more minutes, then runs `reconcile`, `reconcile-gold` and `reconcile-after`, saving output as `evidence/E1b_<case>_*`. Case 1 started on the stack I had just rebuilt.

| Time | Event |
|------|-------|
| 17:59:44 | Case `taskmanager_kill` begins |
| 18:00:30 | All 5 jobs `RUNNING`; replay started |
| 18:02:31 | `taskmanager-kill`: SIGKILL sent |
| 18:02:38 to 18:04:09 | The harness checked the container every 5 s: **`exited restarts=0` every time**, never `running` |
| 18:04:09 | Logged "taskmanager did NOT come back within 90 s" |
| 18:04:19 | All 5 jobs `RESTARTING` |
| 18:14:11 | `Watch-Recovery` gave up: **NOT RECOVERED after 600 s** |
| 18:18:37 | Replay finished |
| 18:20:37 | Settled state: all 5 jobs still `RESTARTING` |
| 18:20:46 | `reconcile`: exit 1 |
| 18:20:46 to 18:54 | `reconcile-gold` submitted and **hung** |

**Your question: was it a deadlock?** No. Nothing was waiting on anything else in a cycle. The TaskManager container was simply not running (`Exited (137)`, 52 minutes before I checked), and the cluster had no worker:
- `GET /overview`: `taskmanagers=0, slots-total=0, slots-available=0`.
- The job exception was `NoResourceAvailableException: Could not acquire the minimum required resources`, repeating once a minute.
- The batch `reconcile-gold` job asked for slots that did not exist and waited indefinitely. This is the same silent hang as in Session 7, with a different cause (no slots at all, not too few).

**Root cause: Docker does not apply the restart policy after `docker kill`.** The compose file has `restart: unless-stopped` on the taskmanager, and I had assumed the container would come back after the kill. The measurement says it does not: `exited (137)`, `restarts=0`, for 97 seconds and then for the 52 minutes after. Docker treats a kill sent through its API as a manual stop, and `unless-stopped` means "restart unless a human stopped it". My reading of Docker's behaviour rests on this measurement; I did not look up the daemon's source.

**This also explains the original case 1 failure from Session 9.** There, `docker compose ps taskmanager` showed no container right after the kill, and the 16:01 `reconcile` failed with pings at 27% and bronze at 269 of 924. The TaskManager had probably never restarted then either, so the "FAIL" was the harness, not the pipeline. I have not re-checked that run (its stack is gone), so this is the best explanation, not a proven one.

**What the case 1 `reconcile` shows** (`evidence/E1b_taskmanager_kill_reconcile.txt`), with the new checks:

| Check | Result |
|-------|--------|
| lake is not empty | **FAIL**: `silver.trips`, `bronze.cdc_raw (trips)` empty |
| pings no loss | FAIL: 29,012 of 216,000 |
| pings no duplicates | PASS: 29,012 rows = 29,012 distinct |
| trips: all four trip checks | FAIL / empty (0 of 231) |
| bronze, no unexplained extras | FAIL: 0 of 924 |
| gold supply | INFO: "missing" is negative (-221 of 3,252): gold has more rows than silver, because silver stopped earlier than gold |

- **Why trips and bronze are exactly 0:** the TaskManager log at 18:02:31, just before the kill, says `Skip commit for checkpoint 9 due to no data files or delete files` for `bronze.cdc_raw`. At minute 2 of an 18-minute replay no trip events had been emitted yet. So the failure was injected before the CDC part of the data began. Injecting at a fixed time hits a different part of the data depending on the speedup. For a fair test the failure should be injected after at least one commit has landed in every table.
- **No duplicates among the 29,012 pings** is the one real signal: whatever was committed before the kill is clean.

**Why it took 55 minutes:** the script ran `reconcile-gold` with no time limit. It waited on a job that could never start. I noticed because you did.

### Part 3. Stopping, as you asked

| Time (UTC) | Action |
|------------|--------|
| 18:54 | Checked: log, jobs, containers, `/overview`, exceptions |
| about 18:55 | Stopped the experiment script (PID 25464) and its child processes with `taskkill /T /F` (5 processes, including the `tasks` wrapper and the Flink SQL client session) |
| about 18:55 | Cancelled the hung batch job `reconcile-gold-2025-01-15` (`fa8cd074abd0055285ae6701d482acc6`) through the Flink REST API |
| about 18:55 | Saved a final state file (`evidence/flink_state_E1b-taskmanager_kill-final_185518.txt`) |
| about 18:56 | `docker compose down` (stopped and removed the containers; **volumes kept**, so no data was deleted) |

The Docker stack is now down. Two unrelated Python processes that have been running since the morning were left alone.

### Part 4. One more fix, made after stopping (not tested)
`chaos taskmanager-kill` now waits 5 s after the kill and runs `docker compose start taskmanager`, logging why. This plays the part a real orchestrator (Kubernetes, ECS) would play after a crash. It is untested because the stack is down.

### Not done
- **Cases 2 (MinIO) and 3 (catalog) were never rerun.** So the Postgres catalog has not been tested against a catalog outage, and the E1 result for those cases is still Session 9's.
- **The checkpoint ids for case 1** were saved to `evidence/flink_state_taskmanager-kill-*` files, but they show an incomplete run.
- `02_maintenance.sql` is still untested; the E2 sweep has not been started.

### Blog notes
- **Check the restart policy by measuring, not assuming.** The compose file said `restart: unless-stopped`. The test showed it did not apply. One line in the config, an unverified belief, and an hour lost.
- **A harness can fail in the same way as the thing under test.** Case 1 "failed" twice and both times the cause was outside the pipeline. The first time I did not have the evidence to say so; this time the harness logged the container state every 5 s, which turned a guess into a measurement.
- **Every wait needs a limit.** The experiment script had no timeout on a call that could not finish. The recovery watcher had one and worked; the batch call did not and hung for 34 minutes.
- **Inject failures after the system has data in every table**, not at a fixed time.
- **A stricter check paid for itself immediately.** The old `>=` and `0 == 0` checks would have hidden the empty tables.
- **Stalled is not deadlocked.** A deadlock is a cycle of waits; this was one missing resource. The diagnosis (`taskmanagers=0`) took one request.

### Next steps
1. Bring the stack up fresh (`.\tasks fresh`), since the old data belongs to a stopped experiment.
2. Rerun case 1 with the new harness, injecting the failure after the first trip commit (about minute 4 at 10x), and confirm the TaskManager comes back and the jobs recover.
3. Run case 3 (catalog outage) on the Postgres catalog. This is the test that matters for Fix 1.
4. Run case 2 (MinIO outage).
5. Add a time limit to every blocking call in the experiment script.
6. Remove the unused `restdata:` volume declaration.
7. Loose ends: `02_maintenance.sql`, the E2 sweep.

---

## Session 11 — 2026-11-10 — Closing E1 without the rerun, and the learnings so far

### Decision
You chose to skip the rerun of the three E1 cases and move on. Nothing was run in this session; this entry is the write-up. The Docker stack is down (`docker compose down`, volumes kept). When you want it back: `.\tasks fresh`.

### Where E1 stands

| Case | What was run | Result | Confidence |
|------|--------------|--------|------------|
| 1. TaskManager kill (F5) | Session 9 (original) and Session 10 (rerun) | Both runs left the cluster with no TaskManager. Docker did not apply the restart policy after `docker compose kill`. So neither run tested Flink's recovery from a lost worker | The cause in the rerun is measured (`exited 137`, `restarts=0`, `taskmanagers=0`). For the original run it is the best explanation, not proven |
| 2. MinIO outage (F7) | Session 9 only | No result: the stack was wiped before `reconcile` was saved | None. Untested |
| 3. Catalog outage (F8) | Session 9 only, on the SQLite catalog | All 5 jobs looped in `RESTARTING` for 40+ minutes after a 49 s outage. After manual restores from retained checkpoints every check passed: no loss, no duplicates, gold repaired | High for "data safe, no self-recovery on SQLite". Nothing is known yet about the Postgres catalog |

- **E1's hypothesis** ("after each failure, `reconcile` still passes") is therefore **partly tested**: it held for the one case that produced a result (case 3, after manual recovery), and the other two cases are open.
- **Open follow-ups, not done:** rerun cases 1 to 3 on the Postgres catalog with the fixed harness; inject after every table has data; time limits on every blocking call; remove the unused `restdata:` volume declaration; `02_maintenance.sql` untested; E2 sweep not started.

### Learnings, grouped

**A. How the pipeline behaves (technical)**
1. **Exactly-once on an Iceberg sink depends on two things: checkpoints and a catalog that accepts the commit.** Files are written first and become visible only when the catalog accepts one commit call per checkpoint. A restore replays that pending commit once, which is what prevents loss and duplicates (Session 9: 216,000 pings once each after 40 minutes of failures).
2. **`CommitStateUnknownException` means "I do not know whether it landed".** Flink's answer is to restart and retry. If the dependency keeps failing, the job loops forever with no data visible. Nothing alerts you; only the Flink UI shows it.
3. **A single-writer catalog (SQLite) plus several committers is a trap.** SQLite takes a shared lock for reads and an exclusive lock for writes. Without a busy timeout a collision fails at once with `SQLITE_BUSY`, which becomes HTTP 500. Five jobs committing about every 30 s collide often, and a failed commit restarts the job, which then commits again at the same time as the others. One job alone never collided; four or five did.
4. **A short outage can become a long one.** A 49 s catalog stop turned into about 74 minutes before all jobs were healthy again. Retries from the jobs themselves kept the recovered service overloaded (a retry storm).
5. **Restarting the failing service is a test, not a cure.** Restarting the catalog changed nothing, which showed the problem was not a leftover lock but the load pattern. A failed fix is evidence.
6. **Cancelling a Flink job is not pausing it.** A resubmission is a new job with empty state and, for append-only tables, rewrites the data. Retained checkpoints survive a cancel and can be restored with `execution.savepoint.path`; Flink reports such a restore as `is_savepoint: true`. Only the newest checkpoint per job is kept.
7. **Which tables forgive duplicates?** The upsert table (`silver.trips`) did not change when two job copies wrote it (Session 7). Append-only tables (bronze, pings, gold) doubled. Idempotent sinks are what make retries safe.
8. **Replay length is not the configured length.** `wall_seconds` came out at 180.6 (60x) and 1083.3 (10x). Both equal about 10,835 simulated seconds, so the seeded stream spans about 180.6 simulated minutes, not 120. (Inferred from the arithmetic; the replayer code was not read.)
9. **Late-ping counts vary with timing.** The same seed gave 4,219 / 4,212 / 4,206 late rows in three runs, and 2,263 after a stall and catch-up. The classification depends on the watermark at the moment a ping is processed.

**B. How the platform behaves (operations)**
10. **Docker's restart policy ignores `docker kill`.** `restart: unless-stopped` did not restart a container killed through the Docker API. A test that "kills the worker" must also bring it back, or it tests nothing about recovery.
11. **A batch job with no slots waits silently.** In Session 7 there were too few slots (10 streaming jobs); in Session 10 there were none (no TaskManager). Both times `reconcile-gold` printed its first statement and stopped. When a Flink statement hangs, check `GET /overview` for `slots-available` first.
12. **The cluster is sized exactly.** 8 slots for 5 streaming jobs plus one batch job. Anything extra starves the batch job.
13. **Registries, images and volumes have their own failure modes** (Session 1): removed images return 401, distroless images have no shell, fresh named volumes are root-owned.
14. **`fresh` deletes evidence.** Checkpoint ids live in the MinIO volume. Anything you want to keep (reconcile output, checkpoint ids, logs) has to be saved before the next `fresh`. The `chaos` command now saves Flink state automatically.
15. **Timezones:** the Flink containers run in UTC, the host in UTC+5:30. Write times in UTC and say so.

**C. How the tests and checks behave (method)**
16. **A passing check can hide a fault.** `bronze >= emitted` passed with `extra=924`; `gold == expected` passed as `0 == 0`; an empty lake passed some checks. Each was tightened (Session 10) and the new "lake is not empty" check caught an empty table on its first run.
17. **A harness can fail like the thing under test.** Case 1 "failed" twice because the worker never restarted. Log the state of the harness itself (container status every few seconds), so a guess becomes a measurement.
18. **Inject failures after the system has data in every table.** A kill at minute 2 landed before any trip events existed, so trips and bronze were empty and the result said little.
19. **Every wait needs a limit.** The experiment script had no timeout on `reconcile-gold`; it waited 34 minutes on a job that could not start. The recovery watcher had a 600 s limit and worked.
20. **Order matters between repair and verify.** Never `fresh` or `nuke` between `reconcile-gold` and `reconcile-after` (Session 3).
21. **Do not trust a "healthy" state taken right after a failure.** Right after the kill the jobs still said `RUNNING` because the JobManager had not noticed yet. The recovery watcher requires a checkpoint completed *after* the failure, held for 60 s.
22. **State side effects before running an action.** I cancelled four jobs without saying that cancelling discards state; the checkpoints happened to survive. Say what an action destroys before doing it.
23. **Make the safe thing the default.** The double-submission guard (Session 8) removed a footgun instead of documenting it, and `.\tasks restore` does the same for resuming from a checkpoint.

### What the project now has that it did not have at the start of E1
- A catalog on Postgres (running, untested under failure).
- A stricter `reconcile` (3 new tests; 21 tests pass).
- `flink-state`, `cancel`, `restore`, and `chaos` that records state and the time to recovery.
- A duplicate-submission guard (Session 8).
- Raw evidence for E0, case 1 (twice), and case 3 in `evidence/`.

### Blog notes
- **Suggested spine for a post:** "I killed things on purpose and learned that my safety nets were not what I thought." Three acts: (1) the baseline looks perfect; (2) a 49-second catalog outage leaves the system down for over an hour while the data stays correct; (3) the next test fails because Docker ignores the restart policy.
- The most reusable lessons are 3, 4, 10, 16 and 17.
- Be honest in the post about what was not tested: the Postgres catalog under failure, the MinIO outage, and Flink's recovery from a lost worker.

### Next steps
Your choice. The open follow-ups are listed above under "Where E1 stands". The runbook's next experiments are E2 (watermark bound), E3 (dedup TTL) and E5 (Debezium restart, where bronze extras first become legitimate and need `--allow-bronze-extras`).

---
