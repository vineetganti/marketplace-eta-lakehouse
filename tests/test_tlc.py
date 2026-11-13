import csv
from datetime import datetime, timedelta, timezone

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from replayer.tlc import load_zone_centroids, tlc_plans


def _fixture(tmp_path):
    zones = tmp_path / "zones.csv"
    with open(zones, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["LocationID", "lat", "lon"])
        w.writerow([1, 40.75, -73.98])
        w.writerow([2, 40.70, -74.00])
    base = datetime(2024, 1, 15, 8, 0)
    rows = []
    for i in range(50):
        pick = base + timedelta(minutes=3 * i)
        rows.append(
            {
                "tpep_pickup_datetime": pick,
                "tpep_dropoff_datetime": pick + timedelta(minutes=12),
                "PULocationID": 1 if i % 2 else 2,
                "DOLocationID": 2 if i % 2 else 1,
                "trip_distance": 2.5,
                "fare_amount": 14.0,
            }
        )
    rows.append({  # too short -> filtered
        "tpep_pickup_datetime": base, "tpep_dropoff_datetime": base + timedelta(seconds=30),
        "PULocationID": 1, "DOLocationID": 2, "trip_distance": 0.2, "fare_amount": 3.0})
    rows.append({  # unknown zone -> filtered
        "tpep_pickup_datetime": base, "tpep_dropoff_datetime": base + timedelta(minutes=9),
        "PULocationID": 999, "DOLocationID": 2, "trip_distance": 1.0, "fare_amount": 8.0})
    path = tmp_path / "yellow.parquet"
    pd.DataFrame(rows).to_parquet(path)
    return path, zones


def test_tlc_plans_filter_order_and_lifecycle(tmp_path):
    path, zones = _fixture(tmp_path)
    assert load_zone_centroids(zones)[1] == (40.75, -73.98)
    plans = tlc_plans(path, zones, sample=1.0, seed=1)
    assert len(plans) == 50
    reqs = [p.requested_at for p in plans]
    assert reqs == sorted(reqs)
    assert [p.trip_id for p in plans] == list(range(1, 51))
    for p in plans:
        assert p.requested_at < p.accepted_at < p.picked_up_at < p.completed_at
        assert p.requested_at.tzinfo == timezone.utc
        assert p.picked_up_at.minute % 3 == 0  # pickup time preserved from the file
        assert p.distance_km == pytest.approx(2.5 * 1.609344, rel=1e-3)
