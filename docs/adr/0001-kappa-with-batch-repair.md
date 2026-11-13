# ADR-0001: Kappa with batch repair for gold

Status: Accepted (revisit after E2, E8, E9)

## Context
Gold aggregates (1-minute supply/demand per H3 cell) need low latency, but late events beyond the
watermark bound are dropped from streaming windows. We need correct history without running two
full pipelines.

## Decision
One streaming path produces bronze, silver and gold. Silver is the source of truth. A daily
batch job recomputes closed gold partitions from silver using `INSERT OVERWRITE ... PARTITION`,
an atomic replace-partitions commit.

## Consequences
- (+) One streaming code path for state and telemetry; no separate Lambda batch layer.
- (+) Late data is eventually reflected; the late table quantifies the gap.
- (+) Atomic swap: readers never see half a day.
- (-) Gold is briefly "wrong" (missing late data) until repaired; consumers needing exact numbers
  must read repaired partitions only.
- (-) Reconcile must not run on partitions the stream still writes (E8).
- (-) Silver rebuild beyond Kafka retention depends on bronze replay (E9).

## Alternatives considered
- **Pure Kappa:** simplest, but cannot incorporate data later than the watermark.
- **Lambda:** independent batch recompute; two implementations drift apart.
- **Very large watermark bound:** fixes lateness by sacrificing freshness (quantify in E2).
