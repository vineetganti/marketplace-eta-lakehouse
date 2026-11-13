"""Synthetic NYC-ish marketplace: drivers, merchants, trips, GPS pings, dimension churn.

Design goals (so the pipeline has real things to get wrong):
  * Spatial skew     - a handful of hotspots => hot H3 cells and key skew.
  * Temporal pattern - rush-hour demand and congestion => ETA baselines by hour-of-week are learnable.
  * Noisy ETA quotes - quoted_eta_sec = truth * lognormal noise.
  * Slowly changing dimensions - drivers/merchants mutate => SCD2 has something to track.
  * Pings are paced by an event-time clock and then perturbed by Injection (late / duplicate).
"""

from __future__ import annotations

import heapq
import itertools
import math
import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import ClassVar

from .inject import Injection
from .model import (
    SINK_DRIVERS,
    SINK_MERCHANTS,
    SINK_PINGS,
    SINK_TRIPS,
    Event,
    TripPlan,
)

# lat, lon, weight
HOTSPOTS = [
    (40.7580, -73.9855, 3.0),   # Midtown
    (40.7128, -74.0060, 2.0),   # FiDi
    (40.7794, -73.9632, 1.5),   # UES
    (40.6413, -73.7781, 1.2),   # JFK
    (40.7527, -73.9772, 2.0),   # Grand Central
    (40.6782, -73.9442, 1.0),   # Brooklyn
]
BBOX = (40.55, 40.92, -74.05, -73.70)  # lat_min, lat_max, lon_min, lon_max

_DEMAND = {
    0: .3, 1: .2, 2: .15, 3: .15, 4: .2, 5: .4, 6: .8, 7: 1.4, 8: 1.8, 9: 1.4, 10: 1.0,
    11: 1.0, 12: 1.2, 13: 1.1, 14: 1.0, 15: 1.1, 16: 1.3, 17: 1.8, 18: 1.9, 19: 1.6,
    20: 1.3, 21: 1.1, 22: .8, 23: .5,
}


def demand_multiplier(hour: int) -> float:
    return _DEMAND[hour % 24]


def congestion(hour: int) -> float:
    """Speed multiplier: <1 slower than free flow."""
    if 7 <= hour < 10 or 16 <= hour < 20:
        return 0.55
    if 10 <= hour < 16:
        return 0.80
    if 0 <= hour < 5:
        return 1.20
    return 0.95


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def sample_point(rng: random.Random) -> tuple[float, float]:
    lat0, lon0, _ = rng.choices(HOTSPOTS, weights=[h[2] for h in HOTSPOTS])[0]
    lat = _clamp(rng.gauss(lat0, 0.02), BBOX[0], BBOX[1])
    lon = _clamp(rng.gauss(lon0, 0.02), BBOX[2], BBOX[3])
    return round(lat, 6), round(lon, 6)


def make_plan(
    rng: random.Random,
    trip_id: int,
    requested_at: datetime,
    merchants: list[dict],
    p_delivery: float,
) -> TripPlan:
    is_delivery = bool(merchants) and rng.random() < p_delivery
    merchant_id = None
    if is_delivery:
        m = rng.choice(merchants)
        merchant_id = m["merchant_id"]
        pickup = (m["lat"], m["lon"])
    else:
        pickup = sample_point(rng)
    dropoff = sample_point(rng)
    straight = haversine_km(*pickup, *dropoff)
    distance_km = max(0.4, straight * 1.3)  # road factor
    speed_kph = rng.uniform(18.0, 32.0) * congestion(requested_at.hour)
    ride_s = max(120.0, distance_km / speed_kph * 3600.0)
    accepted_at = requested_at + timedelta(seconds=rng.uniform(5, 45))
    picked_up_at = accepted_at + timedelta(seconds=rng.uniform(60, 420))
    completed_at = picked_up_at + timedelta(seconds=ride_s)
    quoted = max(60, int(ride_s * rng.lognormvariate(0.0, 0.18)))
    fare = round(3.0 + 1.5 * distance_km + 0.35 * ride_s / 60.0, 2)
    return TripPlan(
        trip_id=trip_id,
        rider_id=rng.randint(1, 500_000),
        service_type="delivery" if is_delivery else "ride",
        merchant_id=merchant_id,
        pickup=pickup,
        dropoff=dropoff,
        distance_km=round(distance_km, 3),
        fare=fare,
        quoted_eta_sec=quoted,
        requested_at=requested_at,
        accepted_at=accepted_at,
        picked_up_at=picked_up_at,
        completed_at=completed_at,
    )


def synth_plans(
    rng: random.Random,
    sim_start: datetime,
    sim_end: datetime,
    trips_per_min: float,
    merchants: list[dict],
    p_delivery: float = 0.25,
) -> Iterator[TripPlan]:
    """Non-homogeneous Poisson arrivals (hourly rate multiplier), in requested_at order."""
    t = sim_start
    trip_id = 1
    while True:
        rate_per_s = trips_per_min / 60.0 * demand_multiplier(t.hour)
        t = t + timedelta(seconds=rng.expovariate(rate_per_s))
        if t >= sim_end:
            return
        yield make_plan(rng, trip_id, t, merchants, p_delivery)
        trip_id += 1


@dataclass
class _Leg:
    t0: datetime
    t1: datetime
    frm: tuple[float, float]
    to: tuple[float, float]
    speed_kph: float


@dataclass
class _Driver:
    driver_id: int
    vehicle_type: str
    city: str
    rating: float
    status: str
    lat: float
    lon: float
    phase: float                      # seconds offset of this driver's ping clock
    seq: int = 0
    busy_until: datetime | None = None
    legs: list[_Leg] = field(default_factory=list)


class City:
    """Turns TripPlans into a globally emit-time-ordered stream of Events."""

    CITIES: ClassVar[list[str]] = ["New York", "Brooklyn", "Queens"]
    VEHICLES: ClassVar[list[str]] = ["sedan", "suv", "bike", "ev"]
    CATEGORIES: ClassVar[list[str]] = ["grocery", "food", "pharmacy", "electronics"]
    TIERS: ClassVar[list[str]] = ["bronze", "silver", "gold"]

    def __init__(
        self,
        *,
        seed: int,
        n_drivers: int,
        n_merchants: int,
        sim_start: datetime,
        ping_interval_s: float = 10.0,
        injection: Injection | None = None,
        driver_churn_per_hour: float = 0.05,
        merchant_churn_per_hour: float = 0.02,
    ) -> None:
        self.rng = random.Random(seed)
        self.sim_start = sim_start
        self.ping_interval_s = ping_interval_s
        self.injection = injection or Injection()
        self.driver_churn = driver_churn_per_hour
        self.merchant_churn = merchant_churn_per_hour
        self._counter = itertools.count()
        self.drivers: list[_Driver] = []
        for i in range(1, n_drivers + 1):
            lat, lon = sample_point(self.rng)
            self.drivers.append(
                _Driver(
                    driver_id=i,
                    vehicle_type=self.rng.choice(self.VEHICLES),
                    city=self.rng.choices(self.CITIES, weights=[6, 3, 1])[0],
                    rating=round(self.rng.uniform(4.0, 5.0), 2),
                    status="active",
                    lat=lat,
                    lon=lon,
                    phase=self.rng.uniform(0, ping_interval_s),
                )
            )
        self.merchants: list[dict] = []
        for i in range(1, n_merchants + 1):
            lat, lon = sample_point(self.rng)
            self.merchants.append(
                {
                    "merchant_id": i,
                    "category": self.rng.choice(self.CATEGORIES),
                    "tier": self.rng.choices(self.TIERS, weights=[6, 3, 1])[0],
                    "city": self.rng.choices(self.CITIES, weights=[6, 3, 1])[0],
                    "lat": lat,
                    "lon": lon,
                    "commission_pct": round(self.rng.uniform(10, 25), 1),
                    "updated_at": sim_start,
                }
            )

    # ------------------------------------------------------------------ rows
    def merchant_rows(self) -> list[dict]:
        return self.merchants

    @staticmethod
    def _driver_row(d: _Driver, ts: datetime) -> dict:
        return {
            "driver_id": d.driver_id,
            "vehicle_type": d.vehicle_type,
            "city": d.city,
            "rating": d.rating,
            "status": d.status,
            "updated_at": ts,
        }

    def initial_events(self) -> list[Event]:
        ts = self.sim_start
        evs = [Event(ts, ts, SINK_DRIVERS, self._driver_row(d, ts)) for d in self.drivers]
        evs += [Event(ts, ts, SINK_MERCHANTS, dict(m)) for m in self.merchants]
        return evs

    # ------------------------------------------------------------------ main loop
    def run(
        self,
        plans: Iterator[TripPlan],
        sim_end: datetime,
        chunk_s: float = 60.0,
    ) -> Iterator[Event]:
        pending: list[tuple[datetime, int, Event]] = []

        def push(ev: Event) -> None:
            heapq.heappush(pending, (ev.emit_time, next(self._counter), ev))

        for ev in self.initial_events():
            push(ev)

        plans = iter(plans)
        nxt = next(plans, None)
        t0 = self.sim_start
        chunk = timedelta(seconds=chunk_s)
        while t0 < sim_end:
            t1 = min(t0 + chunk, sim_end)
            while nxt is not None and nxt.requested_at < t1:
                if nxt.requested_at >= t0:
                    self._schedule_trip(nxt, push)
                nxt = next(plans, None)
            self._emit_pings(t0, t1, push)
            self._emit_churn(t0, t1, push)
            while pending and pending[0][0] < t1:
                yield heapq.heappop(pending)[2]
            t0 = t1
        while pending:  # drain lifecycle events + delayed pings scheduled past sim_end
            yield heapq.heappop(pending)[2]

    # ------------------------------------------------------------------ trips
    @staticmethod
    def _trip_row(plan: TripPlan, status: str, driver_id: int | None, **kw) -> dict:
        row = {
            "trip_id": plan.trip_id,
            "rider_id": plan.rider_id,
            "driver_id": driver_id,
            "merchant_id": plan.merchant_id,
            "service_type": plan.service_type,
            "status": status,
            "pickup_lat": plan.pickup[0],
            "pickup_lon": plan.pickup[1],
            "dropoff_lat": plan.dropoff[0],
            "dropoff_lon": plan.dropoff[1],
            "requested_at": plan.requested_at,
            "accepted_at": None,
            "picked_up_at": None,
            "completed_at": None,
            "quoted_eta_sec": plan.quoted_eta_sec,
            "fare_amount": None,
            "distance_km": plan.distance_km,
            "updated_at": plan.requested_at,
        }
        row.update(kw)
        return row

    def _schedule_trip(self, plan: TripPlan, push) -> None:
        def emit(ts: datetime, row: dict) -> None:
            row["updated_at"] = ts
            push(Event(ts, ts, SINK_TRIPS, row))

        emit(plan.requested_at, self._trip_row(plan, "requested", None))

        idle = [
            d for d in self.drivers
            if d.status == "active" and (d.busy_until is None or d.busy_until <= plan.accepted_at)
        ]
        if not idle:
            cancel_at = plan.requested_at + timedelta(seconds=self.rng.uniform(60, 180))
            emit(cancel_at, self._trip_row(plan, "cancelled", None))
            return

        d = self.rng.choice(idle)
        d.busy_until = plan.completed_at + timedelta(seconds=30)
        pickup_s = (plan.picked_up_at - plan.accepted_at).total_seconds()
        pickup_km = haversine_km(d.lat, d.lon, *plan.pickup)
        d.legs.append(
            _Leg(plan.accepted_at, plan.picked_up_at, (d.lat, d.lon), plan.pickup,
                 min(60.0, pickup_km / max(pickup_s, 1) * 3600))
        )
        ride_s = (plan.completed_at - plan.picked_up_at).total_seconds()
        d.legs.append(
            _Leg(plan.picked_up_at, plan.completed_at, plan.pickup, plan.dropoff,
                 plan.distance_km / max(ride_s, 1) * 3600)
        )
        emit(plan.accepted_at,
             self._trip_row(plan, "accepted", d.driver_id, accepted_at=plan.accepted_at))
        emit(plan.picked_up_at,
             self._trip_row(plan, "in_progress", d.driver_id,
                            accepted_at=plan.accepted_at, picked_up_at=plan.picked_up_at))
        emit(plan.completed_at,
             self._trip_row(plan, "completed", d.driver_id,
                            accepted_at=plan.accepted_at, picked_up_at=plan.picked_up_at,
                            completed_at=plan.completed_at, fare_amount=plan.fare))

    # ------------------------------------------------------------------ pings
    def _position(self, d: _Driver, t: datetime) -> tuple[float, float, float]:
        for leg in d.legs:
            if leg.t0 <= t < leg.t1:
                span = (leg.t1 - leg.t0).total_seconds()
                f = (t - leg.t0).total_seconds() / span if span > 0 else 1.0
                d.lat = leg.frm[0] + (leg.to[0] - leg.frm[0]) * f
                d.lon = leg.frm[1] + (leg.to[1] - leg.frm[1]) * f
                return d.lat, d.lon, leg.speed_kph
        d.lat = _clamp(d.lat + self.rng.gauss(0, 0.0004), BBOX[0], BBOX[1])
        d.lon = _clamp(d.lon + self.rng.gauss(0, 0.0004), BBOX[2], BBOX[3])
        return d.lat, d.lon, self.rng.uniform(0, 12)

    def _emit_pings(self, t0: datetime, t1: datetime, push) -> None:
        interval = self.ping_interval_s
        off0 = (t0 - self.sim_start).total_seconds()
        off1 = (t1 - self.sim_start).total_seconds()
        for d in self.drivers:
            d.legs = [leg for leg in d.legs if leg.t1 > t0]
            n = math.ceil((off0 - d.phase) / interval)
            while True:
                off = d.phase + n * interval
                if off >= off1:
                    break
                n += 1
                if off < off0:
                    continue
                t = self.sim_start + timedelta(seconds=off)
                lat, lon, speed = self._position(d, t)
                d.seq += 1
                payload = {
                    "driver_id": d.driver_id,
                    "seq": d.seq,
                    "event_ts_ms": int(t.timestamp() * 1000),
                    "lat": round(lat, 6),
                    "lon": round(lon, 6),
                    "speed_kph": round(speed, 1),
                }
                base = Event(t, t, SINK_PINGS, payload)
                for ev in self.injection.apply(base, self.rng):
                    push(ev)

    # ------------------------------------------------------------------ churn (SCD2 fodder)
    def _emit_churn(self, t0: datetime, t1: datetime, push) -> None:
        span_h = (t1 - t0).total_seconds() / 3600.0
        span_s = (t1 - t0).total_seconds()
        for d in self.drivers:
            if self.rng.random() < self.driver_churn * span_h:
                ts = t0 + timedelta(seconds=self.rng.uniform(0, span_s))
                d.rating = round(_clamp(d.rating + self.rng.gauss(0, 0.05), 3.0, 5.0), 2)
                if self.rng.random() < 0.15:
                    d.vehicle_type = self.rng.choice(self.VEHICLES)
                if self.rng.random() < 0.10:
                    d.status = "on_break" if d.status == "active" else "active"
                if self.rng.random() < 0.02:
                    d.city = self.rng.choice(self.CITIES)
                push(Event(ts, ts, SINK_DRIVERS, self._driver_row(d, ts)))
        for m in self.merchants:
            if self.rng.random() < self.merchant_churn * span_h:
                ts = t0 + timedelta(seconds=self.rng.uniform(0, span_s))
                if self.rng.random() < 0.5:
                    m["tier"] = self.rng.choice(self.TIERS)
                m["commission_pct"] = round(
                    _clamp(m["commission_pct"] + self.rng.gauss(0, 1.0), 5.0, 30.0), 1)
                m["updated_at"] = ts
                push(Event(ts, ts, SINK_MERCHANTS, dict(m)))
