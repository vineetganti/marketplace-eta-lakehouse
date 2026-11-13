import csv
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "fetch_tlc", Path(__file__).resolve().parents[1] / "scripts" / "fetch_tlc.py"
)
fetch_tlc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_tlc)

SQUARE = [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]


def test_ring_centroid_of_square():
    cx, cy, area = fetch_tlc.ring_centroid(SQUARE)
    assert (round(cx, 6), round(cy, 6)) == (1.0, 1.0)
    assert abs(area) == 4.0


def test_multipolygon_is_area_weighted():
    big = [[[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]]]        # area 16, centre (2, 2)
    small = [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]]  # area 1, centre (10.5, 10.5)
    lon, lat = fetch_tlc.geometry_centroid({"type": "MultiPolygon", "coordinates": [big, small]})
    assert 2.0 < lon < 3.0 and 2.0 < lat < 3.0  # pulled toward the big polygon


def test_build_centroids_writes_csv(tmp_path):
    gj = {
        "features": [
            {"properties": {"location_id": "7"},
             "geometry": {"type": "Polygon", "coordinates": [SQUARE]}},
            {"properties": {"location_id": "8"}, "geometry": None},  # skipped
        ]
    }
    src, dst = tmp_path / "z.geojson", tmp_path / "z.csv"
    src.write_text(json.dumps(gj))
    assert fetch_tlc.build_centroids(src, dst) == 1
    with open(dst, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [{"LocationID": "7", "lat": "1.0", "lon": "1.0"}]
