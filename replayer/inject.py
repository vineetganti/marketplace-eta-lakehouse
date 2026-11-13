"""Deterministic fault injection for the ping stream: lateness and duplicate delivery.

Only SINK_PINGS events are touched. Trip/dimension rows go through Postgres in order, because
Postgres' WAL gives them a real total order; pings go straight to Kafka, where the real world
delivers them late, out of order and more than once.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import timedelta

from .model import SINK_PINGS, Event


@dataclass(frozen=True)
class Injection:
    late_prob: float = 0.02        # fraction of pings delayed
    late_min_s: float = 45.0       # must exceed the 30 s watermark bound to actually be "late"
    late_max_s: float = 600.0
    dup_prob: float = 0.01         # fraction of pings delivered twice
    dup_max_delay_s: float = 30.0  # second delivery lands within this many seconds

    def apply(self, ev: Event, rng: random.Random) -> list[Event]:
        if ev.sink != SINK_PINGS:
            return [ev]
        out: list[Event] = []
        base = ev
        if rng.random() < self.late_prob:
            delay = rng.uniform(self.late_min_s, self.late_max_s)
            base = replace(ev, emit_time=ev.event_time + timedelta(seconds=delay), late_by_s=delay)
        out.append(base)
        if rng.random() < self.dup_prob:
            delay = rng.uniform(0.0, self.dup_max_delay_s)
            out.append(
                replace(base, emit_time=base.emit_time + timedelta(seconds=delay), is_dup=True)
            )
        return out
