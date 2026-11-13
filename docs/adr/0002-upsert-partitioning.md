# ADR-0002: Partition `silver.trips` by an immutable column included in the PK

Status: Accepted (reproduce the failure in M2)

## Context
`silver.trips` is an Iceberg v2 upsert table written by Flink. Each update is written as an
equality delete for the old row plus an insert. In Iceberg, an equality delete applies only to
data files within the same partition (for partitioned tables), and Flink requires the PK to
include all partition source columns.

## Decision
Partition by `requested_date` (derived from the immutable `requested_at`) and use
`PRIMARY KEY (trip_id, requested_date)`.

## Consequences
- (+) Every version of a trip lives in one partition, so equality deletes find and remove older rows.
- (+) Date partitions prune well for time-bounded analytics.
- (-) Late queries by `trip_id` alone scan all partitions (add `requested_date` predicates, or
  sort/cluster by trip_id).
- (-) Equality deletes accumulate and hurt reads until compacted (E7).

## How to prove it (M2)
Temporarily partition by `CAST(updated_at AS DATE)` (or `status`), replay, and run
`sql/trino/04_verify.sql` query 2. Expect multiple "latest" rows per `trip_id`. Explain why in
your own words, then revert.

## Alternatives considered
- Unpartitioned table: correct, but no pruning and large rewrite scope.
- Partition by `trip_id` bucket: also derived from the key (valid), more compaction-friendly;
  consider for 100x scale.
