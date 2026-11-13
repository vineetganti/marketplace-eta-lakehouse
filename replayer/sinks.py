"""Sinks: where events go. All share write()/flush()/close()."""

from __future__ import annotations

import itertools
import json
import sys
from datetime import datetime

from .model import SINK_DRIVERS, SINK_MERCHANTS, SINK_TRIPS, Event

PG_TABLES: dict[str, tuple[str, list[str], str]] = {
    SINK_TRIPS: (
        "trips",
        ["trip_id", "rider_id", "driver_id", "merchant_id", "service_type", "status",
         "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "requested_at",
         "accepted_at", "picked_up_at", "completed_at", "quoted_eta_sec", "fare_amount",
         "distance_km", "updated_at"],
        "trip_id",
    ),
    SINK_DRIVERS: (
        "drivers",
        ["driver_id", "vehicle_type", "city", "rating", "status", "updated_at"],
        "driver_id",
    ),
    SINK_MERCHANTS: (
        "merchants",
        ["merchant_id", "category", "tier", "city", "lat", "lon", "commission_pct", "updated_at"],
        "merchant_id",
    ),
}


def upsert_sql(table: str, cols: list[str], pk: str) -> str:
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    )


def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(type(o))


class StdoutSink:
    """--dry-run: one JSON line per event."""

    def __init__(self, stream=None) -> None:
        self.stream = stream or sys.stdout

    def write(self, ev: Event) -> None:
        rec = {
            "sink": ev.sink,
            "event_time": ev.event_time.isoformat(),
            "emit_time": ev.emit_time.isoformat(),
            "late_by_s": round(ev.late_by_s, 1) if ev.late_by_s else 0,
            "dup": ev.is_dup,
            "payload": ev.payload,
        }
        self.stream.write(json.dumps(rec, default=_json_default) + "\n")

    def flush(self) -> None:
        self.stream.flush()

    def close(self) -> None:
        self.flush()


class PostgresSink:
    """Upserts rows (idempotent), one transaction per flush tick, preserving event order."""

    def __init__(self, dsn: str) -> None:
        import psycopg  # lazy: not needed for --dry-run / tests

        self._psycopg = psycopg
        self.conn = psycopg.connect(dsn)
        self.buf: list[tuple[str, dict]] = []
        self.sql = {k: upsert_sql(*v) for k, v in PG_TABLES.items()}

    def write(self, ev: Event) -> None:
        self.buf.append((ev.sink, ev.payload))

    def flush(self) -> None:
        if not self.buf:
            return
        with self.conn.transaction(), self.conn.cursor() as cur:
            for sink, group in itertools.groupby(self.buf, key=lambda x: x[0]):
                cols = PG_TABLES[sink][1]
                cur.executemany(self.sql[sink], [[p[c] for c in cols] for _, p in group])
        self.buf.clear()

    def close(self) -> None:
        self.flush()
        self.conn.close()


class KafkaSink:
    """Idempotent producer; keyed by driver_id so a driver's pings stay in one partition."""

    def __init__(self, bootstrap: str, topic: str = "driver.pings") -> None:
        from confluent_kafka import Producer  # lazy

        self.topic = topic
        self._err: Exception | None = None
        self.p = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )

    def _on_delivery(self, err, _msg) -> None:
        if err is not None and self._err is None:
            self._err = RuntimeError(f"kafka delivery failed: {err}")

    def write(self, ev: Event) -> None:
        value = json.dumps(ev.payload, separators=(",", ":"))
        while True:
            try:
                self.p.produce(
                    self.topic,
                    key=str(ev.payload["driver_id"]),
                    value=value,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self.p.poll(0.1)
        self.p.poll(0)

    def flush(self) -> None:
        self.p.flush(10)
        if self._err:
            raise self._err

    def close(self) -> None:
        self.flush()
