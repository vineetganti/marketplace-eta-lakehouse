#!/usr/bin/env python
"""Download a month of NYC TLC yellow-taxi trips + build zone centroids (pure stdlib).

    python scripts/fetch_tlc.py --month 2024-01 --out data/

Produces:
    data/yellow_tripdata_2024-01.parquet
    data/zone_centroids.csv        (LocationID,lat,lon)

UNTESTED against the live endpoints (written without network access to them). If a URL or a
GeoJSON property name has changed, the two constants below are the only places to touch.
The synthetic source (`--source synth`) needs none of this.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import urllib.request
from pathlib import Path

TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
# NYC Open Data "NYC Taxi Zones" (EPSG:4326 GeoJSON). Property key for the id varies by export.
ZONES_URL = "https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON"
ID_KEYS = ("location_id", "LocationID", "locationid", "OBJECTID", "objectid")


def download(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"exists, skipping: {dest}")
        return
    print(f"GET {url}")
    with urllib.request.urlopen(url) as r, open(dest, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)


def ring_centroid(ring: list[list[float]]) -> tuple[float, float, float]:
    """Area-weighted centroid of one polygon ring (shoelace). Returns (cx, cy, signed_area)."""
    a = cx = cy = 0.0
    for (x0, y0), (x1, y1) in itertools.pairwise(ring):
        cross = x0 * y1 - x1 * y0
        a += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross
    a *= 0.5
    if a == 0:
        xs = [p[0] for p in ring]
        ys = [p[1] for p in ring]
        return sum(xs) / len(xs), sum(ys) / len(ys), 0.0
    return cx / (6 * a), cy / (6 * a), a


def geometry_centroid(geom: dict) -> tuple[float, float]:
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    tot = sx = sy = 0.0
    for poly in polys:
        cx, cy, a = ring_centroid(poly[0])  # outer ring only
        w = abs(a) or 1e-12
        tot += w
        sx += cx * w
        sy += cy * w
    return sx / tot, sy / tot  # lon, lat


def build_centroids(geojson_path: Path, out_csv: Path) -> int:
    with open(geojson_path) as f:
        gj = json.load(f)
    n = 0
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["LocationID", "lat", "lon"])
        for feat in gj["features"]:
            props = feat["properties"]
            key = next((k for k in ID_KEYS if k in props), None)
            if key is None or feat.get("geometry") is None:
                continue
            lon, lat = geometry_centroid(feat["geometry"])
            w.writerow([int(props[key]), round(lat, 6), round(lon, 6)])
            n += 1
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default="2024-01")
    ap.add_argument("--out", default="data")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    download(TRIPS_URL.format(month=a.month), out / f"yellow_tripdata_{a.month}.parquet")
    gj = out / "taxi_zones.geojson"
    download(ZONES_URL, gj)
    n = build_centroids(gj, out / "zone_centroids.csv")
    print(f"wrote {n} zone centroids -> {out / 'zone_centroids.csv'}")
    if n < 200:
        print("WARNING: expected ~260 zones; check ID_KEYS against the GeoJSON properties.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
