import random
from datetime import datetime, timedelta, timezone

from replayer.inject import Injection
from replayer.model import SINK_PINGS, SINK_TRIPS, Event

T0 = datetime(2025, 1, 15, tzinfo=timezone.utc)


def ping(i: int) -> Event:
    t = T0 + timedelta(seconds=i)
    return Event(t, t, SINK_PINGS, {"driver_id": 1, "seq": i})


def test_non_ping_events_untouched():
    ev = Event(T0, T0, SINK_TRIPS, {"trip_id": 1})
    inj = Injection(late_prob=1.0, dup_prob=1.0)
    assert inj.apply(ev, random.Random(1)) == [ev]


def test_late_events_are_delayed_within_bounds():
    inj = Injection(late_prob=1.0, late_min_s=45, late_max_s=600, dup_prob=0.0)
    rng = random.Random(1)
    for i in range(200):
        (out,) = inj.apply(ping(i), rng)
        assert 45 <= out.late_by_s <= 600
        assert out.emit_time - out.event_time == timedelta(seconds=out.late_by_s)
        assert out.event_time == ping(i).event_time  # event time is never rewritten


def test_duplicates_share_identity_and_arrive_later():
    inj = Injection(late_prob=0.0, dup_prob=1.0, dup_max_delay_s=30)
    out = inj.apply(ping(5), random.Random(2))
    assert len(out) == 2
    first, dup = out
    assert not first.is_dup and dup.is_dup
    assert first.payload == dup.payload
    assert 0 <= (dup.emit_time - first.emit_time).total_seconds() <= 30


def test_rates_are_roughly_respected_and_deterministic():
    inj = Injection(late_prob=0.1, dup_prob=0.05)
    def run(seed):
        rng = random.Random(seed)
        evs = [e for i in range(20000) for e in inj.apply(ping(i), rng)]
        return evs
    a, b = run(3), run(3)
    assert [(e.emit_time, e.is_dup) for e in a] == [(e.emit_time, e.is_dup) for e in b]
    late = sum(1 for e in a if e.late_by_s > 0 and not e.is_dup)
    dups = sum(1 for e in a if e.is_dup)
    assert 0.08 < late / 20000 < 0.12
    assert 0.035 < dups / 20000 < 0.065
