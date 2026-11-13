import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from replayer.inject import Injection
from replayer.model import SINK_DRIVERS, SINK_MERCHANTS, SINK_PINGS, SINK_TRIPS
from replayer.sinks import PG_TABLES, upsert_sql
from replayer.synth import City, synth_plans

START = datetime(2025, 1, 15, 7, tzinfo=timezone.utc)


def make(seed=1, minutes=20, inj=None, drivers=40, tpm=6.0):
    end = START + timedelta(minutes=minutes)
    city = City(seed=seed, n_drivers=drivers, n_merchants=10, sim_start=START,
                injection=inj or Injection(0.0, dup_prob=0.0))
    plans = synth_plans(random.Random(seed + 1), START, end, tpm, city.merchant_rows())
    return list(city.run(plans, end))


def test_stream_is_globally_ordered_by_emit_time():
    evs = make(inj=Injection(late_prob=0.1, dup_prob=0.05))
    emits = [e.emit_time for e in evs]
    assert emits == sorted(emits)


def test_trip_lifecycle_is_ordered_and_terminal():
    evs = make(minutes=40)
    by_trip = defaultdict(list)
    for e in evs:
        if e.sink == SINK_TRIPS:
            by_trip[e.payload["trip_id"]].append(e)
    assert by_trip
    for rows in by_trip.values():
        statuses = [r.payload["status"] for r in rows]
        assert statuses[0] == "requested"
        assert statuses[-1] in ("completed", "cancelled")
        times = [r.event_time for r in rows]
        assert times == sorted(times)
        for r in rows:  # updated_at IS the event time
            assert r.payload["updated_at"] == r.event_time
        if statuses[-1] == "completed":
            assert statuses == ["requested", "accepted", "in_progress", "completed"]
            last = rows[-1].payload
            assert last["fare_amount"] is not None and last["completed_at"] > last["picked_up_at"]


def test_ping_sequences_are_unique_and_gapless_per_driver():
    evs = [e for e in make() if e.sink == SINK_PINGS]
    seqs = defaultdict(list)
    for e in evs:
        seqs[e.payload["driver_id"]].append(e.payload["seq"])
    for s in seqs.values():
        assert sorted(s) == list(range(1, len(s) + 1))


def test_injection_creates_late_and_duplicate_pings():
    evs = [e for e in make(inj=Injection(late_prob=0.2, dup_prob=0.1)) if e.sink == SINK_PINGS]
    assert any(e.late_by_s > 0 for e in evs)
    dups = [e for e in evs if e.is_dup]
    assert dups
    keys = {(e.payload["driver_id"], e.payload["seq"]) for e in evs}
    uniq = sum(1 for e in evs if not e.is_dup)
    assert len(keys) == uniq  # a duplicate never invents a new key


def test_same_seed_same_stream():
    a = make(seed=9, inj=Injection(0.1, dup_prob=0.1))
    b = make(seed=9, inj=Injection(0.1, dup_prob=0.1))
    assert [(e.sink, e.emit_time, e.payload) for e in a] == [(e.sink, e.emit_time, e.payload) for e in b]


def test_dimension_churn_emits_new_versions():
    city = City(seed=3, n_drivers=50, n_merchants=20, sim_start=START,
                injection=Injection(0.0, dup_prob=0.0), driver_churn_per_hour=2.0,
                merchant_churn_per_hour=1.0)
    end = START + timedelta(hours=2)
    evs = list(city.run(iter(()), end))
    drivers = [e for e in evs if e.sink == SINK_DRIVERS]
    merchants = [e for e in evs if e.sink == SINK_MERCHANTS]
    assert len(drivers) > 50 and len(merchants) > 20   # initial rows + churn versions


def test_payload_matches_postgres_columns():
    evs = make(minutes=30)
    seen = set()
    for e in evs:
        if e.sink in PG_TABLES and e.sink not in seen:
            seen.add(e.sink)
            assert set(e.payload) == set(PG_TABLES[e.sink][1])
    assert seen == set(PG_TABLES)


def test_upsert_sql_shape():
    sql = upsert_sql("drivers", ["driver_id", "city"], "driver_id")
    assert sql == ("INSERT INTO drivers (driver_id, city) VALUES (%s, %s) "
                   "ON CONFLICT (driver_id) DO UPDATE SET city = EXCLUDED.city")
