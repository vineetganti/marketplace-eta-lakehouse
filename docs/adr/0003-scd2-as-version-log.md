# ADR-0003: SCD2 as an append-only version log plus a derived view

Status: Accepted (compare with a materialised MERGE in M5)

## Context
Driver and merchant attributes (vehicle type, rating, status, tier, commission) change over
time. Trips and ETA features must use the attribute value valid at the time of the event.
Duplicate CDC deliveries and late dimension updates occur.

## Decision
Flink appends one row per CDC change to `silver.*_versions` (never updates or deletes). Trino
views derive `valid_from` / `valid_to` / `is_current` with `LEAD(version_ts)`, collapsing
duplicates with `GROUP BY (id, version_ts)` and `max_by(..., source_lsn)`. Facts join dimensions
with an as-of predicate (`ts >= valid_from AND ts < valid_to`).

## Consequences
- (+) The streaming job is simple and idempotent in effect; no "close previous row" write race.
- (+) Late or reordered dimension changes are handled correctly at read time.
- (-) Every read pays for window functions; large dims will need a materialised table.
- (-) Deletes are not modelled; soft-delete via `status`.
- (-) Consecutive identical versions (an update that only touched `updated_at`) are not collapsed.

## Alternatives considered
- **Materialised SCD2 via Trino `MERGE`** on a schedule: faster reads, but a batch dependency and
  conflict risk with streaming writers.
- **Flink temporal table join:** needs a versioned table with event-time semantics; good for
  enrichment in-stream, but silver wants history for analytics too.
