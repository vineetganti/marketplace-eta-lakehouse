"""replayer CLI.

  python -m replayer run --source synth --speedup 60 --duration-min 120 --manifest manifest.json
  python -m replayer run --dry-run --limit 20            # print events, touch nothing
  python -m replayer run --source tlc --tlc-path data/yellow_tripdata_2024-01.parquet \
        --zones-csv data/zone_centroids.csv --sample 0.05

--speedup N: 1 wall second = N simulated seconds. 0 = as fast as possible (no sleeping).
The manifest is the ground truth that scripts/reconcile.py compares the lakehouse against.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

from .inject import Injection
from .model import SINK_DRIVERS, SINK_MERCHANTS, SINK_PINGS, SINK_TRIPS, Event
from .synth import City, synth_plans

DEFAULT_PG = "postgresql://lakehouse:lakehouse@localhost:5432/marketplace"
DEFAULT_KAFKA = "localhost:19092"


class Stats:
    """Ground truth about what was emitted."""

    def __init__(self, watermark_bound_s: float) -> None:
        self.bound = watermark_bound_s
        self.pings_unique = 0
        self.pings_dup_emitted = 0
        self.pings_late_injected = 0
        self.pings_late_beyond_bound = 0
        self.max_late_by_s = 0.0
        self.trip_events = 0
        self.driver_events = 0
        self.merchant_events = 0
        self.trip_final: dict[int, str] = {}
        self.first_event_time: datetime | None = None
        self.last_event_time: datetime | None = None

    def observe(self, ev: Event) -> None:
        if self.first_event_time is None or ev.event_time < self.first_event_time:
            self.first_event_time = ev.event_time
        if self.last_event_time is None or ev.event_time > self.last_event_time:
            self.last_event_time = ev.event_time
        if ev.sink == SINK_PINGS:
            if ev.is_dup:
                self.pings_dup_emitted += 1
            else:
                self.pings_unique += 1
                if ev.late_by_s > 0:
                    self.pings_late_injected += 1
                    self.max_late_by_s = max(self.max_late_by_s, ev.late_by_s)
                    if ev.late_by_s > self.bound:
                        self.pings_late_beyond_bound += 1
        elif ev.sink == SINK_TRIPS:
            self.trip_events += 1
            self.trip_final[ev.payload["trip_id"]] = ev.payload["status"]
        elif ev.sink == SINK_DRIVERS:
            self.driver_events += 1
        elif ev.sink == SINK_MERCHANTS:
            self.merchant_events += 1

    def manifest(self, extra: dict) -> dict:
        by_status: dict[str, int] = {}
        for s in self.trip_final.values():
            by_status[s] = by_status.get(s, 0) + 1
        return {
            **extra,
            "pings_unique": self.pings_unique,
            "pings_dup_emitted": self.pings_dup_emitted,
            "pings_late_injected": self.pings_late_injected,
            "pings_late_beyond_watermark_bound": self.pings_late_beyond_bound,
            "max_late_by_s": round(self.max_late_by_s, 1),
            "trips_distinct": len(self.trip_final),
            "trips_final_status": by_status,
            "trip_events": self.trip_events,
            "driver_events": self.driver_events,
            "merchant_events": self.merchant_events,
            "first_event_time": self.first_event_time.isoformat() if self.first_event_time else None,
            "last_event_time": self.last_event_time.isoformat() if self.last_event_time else None,
        }


def stream(
    events: Iterable[Event],
    sinks: dict,
    *,
    sim_start: datetime,
    speedup: float,
    stats: Stats,
    flush_interval_s: float = 0.2,
    limit: int | None = None,
) -> int:
    uniq = list({id(s): s for s in sinks.values()}.values())

    def flush_all() -> None:
        for s in uniq:
            s.flush()

    wall0 = time.monotonic()
    last_flush = wall0
    n = 0
    for ev in events:
        if speedup > 0:
            target = wall0 + (ev.emit_time - sim_start).total_seconds() / speedup
            now = time.monotonic()
            if target > now:
                flush_all()
                last_flush = time.monotonic()
                time.sleep(target - now)
        sinks[ev.sink].write(ev)
        stats.observe(ev)
        n += 1
        if time.monotonic() - last_flush >= flush_interval_s:
            flush_all()
            last_flush = time.monotonic()
        if limit and n >= limit:
            break
    flush_all()
    return n


def _parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="replayer")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="stream trips (Postgres) and pings (Kafka)")
    r.add_argument("--source", choices=["synth", "tlc"], default="synth")
    r.add_argument("--sim-start", default=None, help="ISO time; default 2025-01-15T00:00Z (synth)")
    r.add_argument("--duration-min", type=float, default=120.0, help="simulated minutes")
    r.add_argument("--speedup", type=float, default=60.0)
    r.add_argument("--drivers", type=int, default=300)
    r.add_argument("--merchants", type=int, default=120)
    r.add_argument("--trips-per-min", type=float, default=8.0, help="synth baseline arrival rate")
    r.add_argument("--p-delivery", type=float, default=0.25)
    r.add_argument("--ping-interval-s", type=float, default=10.0)
    r.add_argument("--seed", type=int, default=42)
    r.add_argument("--late-prob", type=float, default=0.02)
    r.add_argument("--late-min-s", type=float, default=45.0)
    r.add_argument("--late-max-s", type=float, default=600.0)
    r.add_argument("--dup-prob", type=float, default=0.01)
    r.add_argument("--dup-max-delay-s", type=float, default=30.0)
    r.add_argument("--watermark-bound-s", type=float, default=30.0,
                   help="only used to label 'late beyond bound' in the manifest")
    r.add_argument("--tlc-path")
    r.add_argument("--zones-csv")
    r.add_argument("--sample", type=float, default=0.05)
    r.add_argument("--pg-dsn", default=DEFAULT_PG)
    r.add_argument("--kafka", default=DEFAULT_KAFKA)
    r.add_argument("--manifest", default=None)
    r.add_argument("--dry-run", action="store_true", help="print JSON lines; touch nothing")
    r.add_argument("--limit", type=int, default=None, help="stop after N events")
    return p


def cmd_run(a: argparse.Namespace) -> int:
    inj = Injection(a.late_prob, a.late_min_s, a.late_max_s, a.dup_prob, a.dup_max_delay_s)
    sim_start = _parse_dt(a.sim_start) if a.sim_start else None

    tlc_list = None
    if a.source == "tlc":
        if not (a.tlc_path and a.zones_csv):
            print("--source tlc needs --tlc-path and --zones-csv", file=sys.stderr)
            return 2
        from .tlc import tlc_plans

        tlc_list = tlc_plans(a.tlc_path, a.zones_csv, sample=a.sample, seed=a.seed,
                             sim_start=sim_start)
        if not tlc_list:
            print("no TLC trips after filtering", file=sys.stderr)
            return 2
        if sim_start is None:
            first = tlc_list[0].requested_at
            sim_start = first.replace(hour=0, minute=0, second=0, microsecond=0)
    elif sim_start is None:
        sim_start = datetime(2025, 1, 15, tzinfo=timezone.utc)

    sim_end = sim_start + timedelta(minutes=a.duration_min)
    city = City(
        seed=a.seed, n_drivers=a.drivers, n_merchants=a.merchants, sim_start=sim_start,
        ping_interval_s=a.ping_interval_s, injection=inj,
    )
    if tlc_list is not None:
        plans = iter(tlc_list)
    else:
        plans = synth_plans(random.Random(a.seed + 1), sim_start, sim_end, a.trips_per_min,
                            city.merchant_rows(), a.p_delivery)

    if a.dry_run:
        from .sinks import StdoutSink

        out = StdoutSink()
        sinks = {s: out for s in (SINK_TRIPS, SINK_DRIVERS, SINK_MERCHANTS, SINK_PINGS)}
    else:
        from .sinks import KafkaSink, PostgresSink

        pg = PostgresSink(a.pg_dsn)
        kafka = KafkaSink(a.kafka)
        sinks = {SINK_TRIPS: pg, SINK_DRIVERS: pg, SINK_MERCHANTS: pg, SINK_PINGS: kafka}

    stats = Stats(a.watermark_bound_s)
    t_wall = time.time()
    n = stream(city.run(plans, sim_end), sinks, sim_start=sim_start, speedup=a.speedup,
               stats=stats, limit=a.limit)
    for s in {id(s): s for s in sinks.values()}.values():
        s.close()

    manifest = stats.manifest(
        {
            "source": a.source,
            "seed": a.seed,
            "sim_start": sim_start.isoformat(),
            "sim_end": sim_end.isoformat(),
            "speedup": a.speedup,
            "drivers": a.drivers,
            "merchants": a.merchants,
            "injection": {
                "late_prob": a.late_prob, "late_min_s": a.late_min_s, "late_max_s": a.late_max_s,
                "dup_prob": a.dup_prob, "dup_max_delay_s": a.dup_max_delay_s,
            },
            "events_emitted": n,
            "wall_seconds": round(time.time() - t_wall, 1),
            "truncated_by_limit": bool(a.limit and n >= a.limit),
        }
    )
    if a.manifest:
        with open(a.manifest, "w") as f:
            json.dump(manifest, f, indent=2)
    print(json.dumps({k: manifest[k] for k in (
        "events_emitted", "pings_unique", "pings_dup_emitted", "pings_late_injected",
        "trips_distinct", "trips_final_status", "wall_seconds")}), file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "run":
        return cmd_run(args)
    return 2
