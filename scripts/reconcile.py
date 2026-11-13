#!/usr/bin/env python
"""Compare what the replayer SAYS it emitted (manifest) against what the lakehouse HOLDS.

    python scripts/reconcile.py --manifest manifest.json
    python scripts/reconcile.py --manifest manifest.json --after-reconcile

PASS/FAIL = correctness invariants (loss, duplicates, upsert integrity).
INFO      = expected, quantifiable imperfection (late data dropped from streaming gold).
Run after the replayer finished AND Flink had >= 2 checkpoints to flush (~60 s).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable

PG_DSN = "postgresql://lakehouse:lakehouse@localhost:5432/marketplace"


def run_checks(
    m: dict,
    scalar: Callable[[str], int],
    rows: Callable[[str], list[tuple]],
    pg_rows: Callable[[str], list[tuple]],
    after_reconcile: bool = False,
    allow_bronze_extras: bool = False,
) -> list[tuple[str, str, str]]:
    """Return (status, name, detail). Pure function over query callables => unit-testable."""
    out: list[tuple[str, str, str]] = []

    def check(name: str, ok: bool, detail: str, kind: str = "FAIL") -> None:
        out.append(("PASS" if ok else kind, name, detail))

    total = scalar("SELECT count(*) FROM iceberg.silver.driver_pings")
    distinct = scalar(
        "SELECT count(*) FROM (SELECT DISTINCT driver_id, seq FROM iceberg.silver.driver_pings)"
    )
    check("pings: no loss", distinct == m["pings_unique"],
          f"silver distinct={distinct} manifest unique={m['pings_unique']}")
    check("pings: no duplicates (dedup works)", total == distinct,
          f"rows={total} distinct={distinct} (injected dups={m['pings_dup_emitted']})")

    t_rows = scalar("SELECT count(*) FROM iceberg.silver.trips")
    t_keys = scalar("SELECT count(DISTINCT trip_id) FROM iceberg.silver.trips")
    check("trips: one latest row per trip (upsert integrity)", t_rows == t_keys,
          f"rows={t_rows} distinct trip_id={t_keys}")
    check("trips: none lost", t_keys == m["trips_distinct"],
          f"silver={t_keys} manifest={m['trips_distinct']}")

    silver_status = dict(rows("SELECT status, count(*) FROM iceberg.silver.trips GROUP BY 1"))
    check("trips: final status counts match manifest",
          {k: int(v) for k, v in silver_status.items()} == m["trips_final_status"],
          f"silver={silver_status} manifest={m['trips_final_status']}")

    pg_status = dict(pg_rows("SELECT status, count(*) FROM trips GROUP BY 1"))
    check("trips: silver == Postgres (CDC fidelity)",
          {k: int(v) for k, v in silver_status.items()} == {k: int(v) for k, v in pg_status.items()},
          f"silver={silver_status} postgres={pg_status}")

    bronze_trips = scalar("SELECT count(*) FROM iceberg.bronze.cdc_raw WHERE topic = 'pg.public.trips'")
    extra = bronze_trips - m["trip_events"]
    if allow_bronze_extras:
        # Only for runs that legitimately re-emit CDC events (a Debezium restart, E5).
        check("bronze: every trip CDC event landed (extras allowed: connector restart)",
              extra >= 0, f"bronze={bronze_trips} emitted={m['trip_events']} extra={extra}")
    else:
        # Strict by default. The old check was ">=", which let a duplicated Flink job pass with
        # extra=924 (BUILD_LOG Session 7). Extra rows without a Debezium restart are a fault.
        check("bronze: every trip CDC event landed, no unexplained extras",
              extra == 0, f"bronze={bronze_trips} emitted={m['trip_events']} extra={extra}")

    # An empty lake must not look healthy (BUILD_LOG Session 3: a wiped lake passed some checks).
    empty = [name for name, n, expected in (
        ("silver.driver_pings", total, m["pings_unique"]),
        ("silver.trips", t_rows, m["trips_distinct"]),
        ("bronze.cdc_raw (trips)", bronze_trips, m["trip_events"]),
    ) if expected > 0 and n == 0]
    out.insert(0, ("FAIL" if empty else "PASS", "lake is not empty",
                   ("EMPTY but manifest expects data: " + ", ".join(empty)) if empty
                   else "all core tables have rows"))

    late_rows = scalar("SELECT count(*) FROM iceberg.silver.driver_pings_late")
    late_detail = (
        f"late rows={late_rows}; replayer injected {m['pings_late_injected']} "
        f"({m['pings_late_beyond_watermark_bound']} beyond bound)"
    )
    out.append(("INFO", "late audit table (approximate; see E2)", late_detail))

    expected_cells = scalar(
        "SELECT count(*) FROM (SELECT DISTINCT date_trunc('minute', event_time), h3_r8 "
        "FROM iceberg.silver.driver_pings)"
    )
    gold_cells = scalar("SELECT count(*) FROM iceberg.gold.h3_supply_1m")
    if after_reconcile:
        # expected_cells > 0 stops "0 == 0" from passing on an empty lake.
        check("gold supply == recomputed-from-silver (after batch reconcile)",
              expected_cells > 0 and gold_cells == expected_cells,
              f"gold={gold_cells} expected={expected_cells}")
    else:
        missing = expected_cells - gold_cells
        gold_detail = (
            f"missing={missing} of {expected_cells} ({missing / max(expected_cells, 1):.2%}) "
            "-> late data dropped by streaming windows; run batch/40_reconcile_gold.sql "
            "then re-run with --after-reconcile"
        )
        out.append(("INFO", "gold supply: (minute,cell) rows missing vs silver", gold_detail))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--trino-host", default="localhost")
    ap.add_argument("--trino-port", type=int, default=8080)
    ap.add_argument("--pg-dsn", default=PG_DSN)
    ap.add_argument("--after-reconcile", action="store_true")
    ap.add_argument("--allow-bronze-extras", action="store_true",
                    help="accept extra bronze CDC rows (only for runs with a Debezium restart)")
    a = ap.parse_args()

    import psycopg
    import trino

    with open(a.manifest) as f:
        m = json.load(f)
    if m.get("truncated_by_limit"):
        print("manifest is from a --limit run; counts will not match. Re-run without --limit.",
              file=sys.stderr)
        return 2

    tconn = trino.dbapi.connect(host=a.trino_host, port=a.trino_port, user="dev", catalog="iceberg")
    tcur = tconn.cursor()

    def trino_rows(sql: str) -> list[tuple]:
        tcur.execute(sql)
        return tcur.fetchall()

    def scalar(sql: str) -> int:
        return int(trino_rows(sql)[0][0])

    with psycopg.connect(a.pg_dsn) as pg, pg.cursor() as cur:
        def pg_rows(sql: str) -> list[tuple]:
            cur.execute(sql)
            return cur.fetchall()

        results = run_checks(m, scalar, trino_rows, pg_rows, a.after_reconcile,
                             a.allow_bronze_extras)

    width = max(len(n) for _, n, _ in results)
    for status, name, detail in results:
        print(f"[{status:4}] {name:<{width}}  {detail}")
    return 1 if any(s == "FAIL" for s, _, _ in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
