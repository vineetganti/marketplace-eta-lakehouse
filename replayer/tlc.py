"""NYC TLC yellow-taxi trips -> TripPlans.

TLC data has pickup/dropoff TIMES and ZONE ids (no GPS since 2016, no driver ids, no ETA quote).
So: location = zone centroid + ~200 m jitter, drivers come from the synthetic pool (City assigns
them), and quoted_eta_sec is synthetic (truth * lognormal noise). Timestamps in the files are
local NYC wall-clock; we treat them as UTC for simplicity - consistent, just not "real" UTC.
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .model import TripPlan

COLS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
]


def load_zone_centroids(path: str | Path) -> dict[int, tuple[float, float]]:
    """CSV with header LocationID,lat,lon (produced by scripts/fetch_tlc.py)."""
    out: dict[int, tuple[float, float]] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            out[int(row["LocationID"])] = (float(row["lat"]), float(row["lon"]))
    return out


def tlc_plans(
    parquet_path: str | Path,
    zones_csv: str | Path,
    *,
    sample: float = 0.05,
    seed: int = 7,
    quote_sigma: float = 0.18,
    sim_start: datetime | None = None,
) -> list[TripPlan]:
    """Materialised list (sampled, so small) sorted by requested_at."""
    import pandas as pd  # local import: keep the synthetic path dependency-light

    zones = load_zone_centroids(zones_csv)
    rng = random.Random(seed)

    df = pd.read_parquet(parquet_path, columns=COLS).dropna()
    dur = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
    keep = (
        (dur >= 120)
        & (dur <= 3 * 3600)
        & (df["trip_distance"] > 0.1)
        & (df["fare_amount"] > 0)
        & df["PULocationID"].isin(zones.keys())
        & df["DOLocationID"].isin(zones.keys())
    )
    df = df[keep]
    if sample < 1.0:
        df = df.sample(frac=sample, random_state=seed)

    plans: list[TripPlan] = []
    for i, r in enumerate(df.itertuples(index=False), start=1):
        pickup_at = r.tpep_pickup_datetime.to_pydatetime().replace(tzinfo=timezone.utc)
        drop_at = r.tpep_dropoff_datetime.to_pydatetime().replace(tzinfo=timezone.utc)
        ride_s = (drop_at - pickup_at).total_seconds()
        requested_at = pickup_at - timedelta(seconds=rng.uniform(60, 400))
        if sim_start is not None and requested_at < sim_start:
            continue
        accepted_at = requested_at + timedelta(seconds=rng.uniform(5, 45))
        if accepted_at >= pickup_at:  # keep lifecycle strictly ordered
            accepted_at = requested_at + (pickup_at - requested_at) / 2
        plo, pla = zones[int(r.PULocationID)]
        dlo, dla = zones[int(r.DOLocationID)]
        plans.append(
            TripPlan(
                trip_id=i,
                rider_id=rng.randint(1, 500_000),
                service_type="ride",
                merchant_id=None,
                pickup=(round(plo + rng.gauss(0, 0.002), 6), round(pla + rng.gauss(0, 0.002), 6)),
                dropoff=(round(dlo + rng.gauss(0, 0.002), 6), round(dla + rng.gauss(0, 0.002), 6)),
                distance_km=round(float(r.trip_distance) * 1.609344, 3),
                fare=float(r.fare_amount),
                quoted_eta_sec=max(60, int(ride_s * rng.lognormvariate(0.0, quote_sigma))),
                requested_at=requested_at,
                accepted_at=accepted_at,
                picked_up_at=pickup_at,
                completed_at=drop_at,
            )
        )
    plans.sort(key=lambda p: p.requested_at)
    # re-number in requested order so trip_id is monotone with time
    return [
        TripPlan(**{**p.__dict__, "trip_id": i}) for i, p in enumerate(plans, start=1)
    ]
