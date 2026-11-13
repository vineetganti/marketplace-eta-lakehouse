"""Core types shared by sources, injection and sinks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

SINK_TRIPS = "pg.trips"
SINK_DRIVERS = "pg.drivers"
SINK_MERCHANTS = "pg.merchants"
SINK_PINGS = "kafka.pings"


@dataclass(frozen=True)
class Event:
    """One thing to write somewhere.

    event_time: when it happened in the simulated world (goes INTO the data).
    emit_time:  when the replayer hands it to the sink (simulated clock). emit_time >=
                event_time; the gap is injected lateness. The pacer converts emit_time to
                wall-clock using --speedup.
    """

    event_time: datetime
    emit_time: datetime
    sink: str
    payload: dict[str, Any]
    late_by_s: float = 0.0   # > 0 when this ping was deliberately delayed
    is_dup: bool = False     # True for an injected duplicate delivery


@dataclass(frozen=True)
class TripPlan:
    """Full ground-truth lifecycle of one trip, before driver assignment."""

    trip_id: int
    rider_id: int
    service_type: str                # ride | delivery
    merchant_id: int | None
    pickup: tuple[float, float]      # lat, lon
    dropoff: tuple[float, float]
    distance_km: float
    fare: float
    quoted_eta_sec: int              # what we told the rider (noisy vs. actual ride time)
    requested_at: datetime
    accepted_at: datetime
    picked_up_at: datetime
    completed_at: datetime
