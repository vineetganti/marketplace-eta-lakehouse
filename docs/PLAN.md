# Project Plan: Real-time Marketplace State & ETA Lakehouse

## 1. Why this project

H-1B sponsorship data shows the product companies sponsoring the most data/ML talent (Amazon,
Meta, Microsoft, Google, Apple, Walmart) run on the same problems this project forces:
logistics state machines, event-time correctness, CDC into a lakehouse, experimentation-grade
data quality, and agentic/AI features that depend on trustworthy serving data. Your Uber and
Dunzo background supplies the domain stories (dispatch, ETA, surge, merchant dimensions); this
project supplies the systems depth.

**You should be able to defend, with measurements from your own system:**

1. Why event time, what a watermark is, how you chose 30 s, and what it cost you.
2. What "exactly-once" means hop by hop, and where it silently degrades to at-least-once.
3. Why your upsert table's partition column must be immutable and part of the PK.
4. When you'd choose Kappa, Lambda, or Kappa + batch repair, and what you chose and why.
5. What breaks when the broker, a worker, the catalog, or the object store disappears.
6. How schema changes propagate (or silently vanish) end to end.

## 2. Scope

In scope: one OLTP source, one stream source, 5 streaming jobs, 1 batch job, Iceberg medallion
tables, Trino serving, fault injection, written design doc with evidence.

Out of scope (deliberately): ML model training, exactly-once into external systems (Redis,
OpenSearch), multi-node HA, auth/security hardening, UI. Several appear as stretch goals.

## 3. Milestones

Estimates assume ~10 hrs/week; total about 4 weeks. Each milestone ends with **Evidence**: an
artifact you commit (a screenshot, a query result, a number in `DESIGN.md`).

| # | Milestone | Build | You learn | Done when | Evidence |
|---|---|---|---|---|---|
| M0 | Bring-up (2-3 d) | `.\tasks up`, `.\tasks init`, fix first-run issues | Container wiring, Kafka listeners, REST catalog, S3 path-style | All services healthy; Flink UI reachable; tables created | `docker compose ps` output; list of fixes you had to make |
| M1 | CDC + bronze (2 d) | Debezium to bronze `cdc_raw` | WAL, replication slots, REPLICA IDENTITY, Debezium envelope, snapshot vs streaming | Row changes in Postgres appear in bronze within a checkpoint | Raw envelope for insert/update/delete pasted into DESIGN section 4 |
| M2 | Silver trips upsert (3 d) | `20_silver_trips.sql` | Changelog semantics (+I/-U/+U/-D), Iceberg v2 equality deletes, PK+partition rule | `reconcile` trips checks PASS; you can *break* it by partitioning on `updated_at` and explain the duplicates | ADR-0002 reproduction notes |
| M3 | Event-time pings (3 d) | `21_silver_pings.sql`, dedup, late path | Event vs processing time, watermark, idle partitions, state TTL | No dup pings in silver; late table populated; you can predict how many pings land late | E2, E3 results |
| M4 | Gold windows + serving (2 d) | `30_gold...`, Trino views | Tumbling windows, idempotent aggregates, FULL OUTER JOIN serving view | Surge ratio query works; hot-cell skew visible | `v_surge_1m` top cells |
| M5 | SCD2 + as-of join (2 d) | `23_silver_dim_versions.sql`, Trino SCD2 views | Version logs vs materialised SCD2, point-in-time joins, LEAD() | `v_trips_enriched` shows the driver tier *at accept time*, not today's | Query + result with a driver whose attribute changed mid-run |
| M6 | Failure injection (4 d) | `.\tasks chaos`, fill failure table | Recovery semantics, checkpoint restore, duplicate sources, silent drops | Every row in DESIGN section 9 has an *Observed* entry | The completed table (the centrepiece) |
| M7 | Maintenance (2 d) | Compaction, snapshot expiry, orphan cleanup | Small-file math, metadata growth, equality-delete cost | Before/after file counts and read latency recorded | E7 table |
| M8 | Backfill + reconcile (3 d) | Batch overwrite; bronze replay; shadow-and-swap | Kappa vs Lambda in practice, atomic partition replace | Gold rebuilt from silver equals streaming gold + late data; replay from bronze equals live silver (`EXCEPT` = 0 rows) | E8, E9 results |
| M9 | ETA + write-up (2 d) | Baseline ETA, accuracy query, finish DESIGN.md | Leakage-free eval, as-of features | DESIGN.md complete; 10-minute walkthrough rehearsed | Final doc + recorded walkthrough |

### Weekly rhythm (suggested)

- **Week 1:** M0, M1, M2. End of week: bronze and silver trips reconcile.
- **Week 2:** M3, M4, M5. End of week: pings, gold, SCD2 working.
- **Week 3:** M6, M7. End of week: failure table half to fully observed.
- **Week 4:** M8, M9. End of week: finished design doc and walkthrough.

## 4. Working method

1. **Hypothesis first.** Before each experiment write what you expect in the *Expected*
   column of the failure table. The learning is in the gap between expectation and observation.
2. **One variable at a time.** Change one knob (watermark bound, checkpoint interval, TTL, H3
   resolution) and re-run the same seeded replay (`--seed` makes it deterministic).
3. **Ground truth over eyeballing.** Always finish with `.\tasks reconcile`. If the manifest and
   the lake disagree, you have found something worth writing down.
4. **Write the ADR when you decide**, not afterwards.

## 5. Risks

| Risk | Mitigation |
|---|---|
| Laptop memory (about 10 GB for the stack) | Stop `trino` when idle; lower TaskManager memory; run fewer jobs at once |
| Version drift on pinned images and jars | Bump pins, record the working set in README |
| Flink SQL cannot express something (e.g. true side outputs) | Use the documented workaround; for the real thing, port that job to the DataStream API (stretch) |
| Spending weeks on tooling friction | Time-box M0 to 3 days; the interesting work starts at M2 |

## 6. Stretch goals (pick by interest)

- **Agentic layer on top (links to your agentic AI goals):** an MCP server over the `serving`
  views with read-only roles, row limits and query-cost guards; an agent that answers "why did
  demand/supply ratio spike in cell X at 18:00?" by querying surge, late-data, and SCD2 views,
  with an eval harness of golden questions.
- **DataStream API rewrite** of the pings job with a real late-data side output and custom
  dedup state; compare against the SQL version.
- **Spark or Flink-Java compaction** of the upsert table (rewrite data files + delete files).
- **Iceberg branches/tags (write-audit-publish):** write to an audit branch, validate, fast-forward.
- **Schema Registry + Avro/Protobuf** between Debezium and Flink; compatibility checks in CI.
- **Lineage:** OpenLineage events from Flink/Trino into Marquez.
- **Serve to your own Redis clone:** push the latest per-cell surge/ETA to the Redis you build,
  and measure staleness end to end.

## 7. Interview narrative (fill in at M9)

*Problem:* a marketplace needs fresh supply/demand and trustworthy ETA features from messy,
late, duplicated events.
*Architecture:* CDC for state, direct stream for telemetry, Iceberg medallion, Kappa with batch repair.
*Hard parts you can speak to:* the three or four failure modes that surprised you, with numbers.
*What you would change at 100x scale:* partitioning, state backend sizing, HA Flink, schema
registry, ownership of compaction.
