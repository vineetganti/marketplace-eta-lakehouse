import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "reconcile", Path(__file__).resolve().parents[1] / "scripts" / "reconcile.py"
)
reconcile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reconcile)

MANIFEST = {
    "pings_unique": 100, "pings_dup_emitted": 3, "pings_late_injected": 5,
    "pings_late_beyond_watermark_bound": 5, "trips_distinct": 10,
    "trips_final_status": {"completed": 9, "cancelled": 1}, "trip_events": 38,
}


def fake(answers):
    """Map a SQL substring -> result; first match wins."""
    def lookup(sql):
        for needle, res in answers:
            if needle in sql:
                return res
        raise AssertionError(f"unexpected query: {sql}")
    return lookup


def results(**over):
    ans = {
        "SELECT count(*) FROM iceberg.silver.driver_pings_late": 7,
        "SELECT count(*) FROM (SELECT DISTINCT driver_id, seq": 100,
        "SELECT count(*) FROM iceberg.silver.driver_pings": 100,
        "SELECT count(*) FROM iceberg.silver.trips": 10,
        "SELECT count(DISTINCT trip_id)": 10,
        "FROM iceberg.bronze.cdc_raw": 38,
        "SELECT count(*) FROM (SELECT DISTINCT date_trunc": 50,
        "SELECT count(*) FROM iceberg.gold.h3_supply_1m": 48,
    }
    ans.update(over)
    lk = fake(list(ans.items()))
    status_rows = [("completed", 9), ("cancelled", 1)]
    return reconcile.run_checks(
        MANIFEST,
        scalar=lambda q: lk(q),
        rows=lambda q: status_rows,
        pg_rows=lambda q: status_rows,
    )


def test_all_invariants_pass_on_healthy_lake():
    res = results()
    assert not [r for r in res if r[0] == "FAIL"]
    assert any(r[0] == "INFO" and "missing=2" in r[2] for r in res)


def test_unexplained_bronze_extras_fail_unless_allowed():
    res = results(**{"FROM iceberg.bronze.cdc_raw": 76})  # a duplicated job doubles bronze
    assert "bronze: every trip CDC event landed, no unexplained extras" in {
        r[1] for r in res if r[0] == "FAIL"}


def test_empty_lake_fails():
    res = results(**{
        "SELECT count(*) FROM (SELECT DISTINCT driver_id, seq": 0,
        "SELECT count(*) FROM iceberg.silver.driver_pings": 0,
        "SELECT count(*) FROM iceberg.silver.trips": 0,
        "SELECT count(DISTINCT trip_id)": 0,
        "FROM iceberg.bronze.cdc_raw": 0,
    })
    assert ("FAIL", "lake is not empty") in {(r[0], r[1]) for r in res}


def test_after_reconcile_does_not_pass_on_zero_equals_zero():
    ans = {
        "SELECT count(*) FROM (SELECT DISTINCT date_trunc": 0,
        "SELECT count(*) FROM iceberg.gold.h3_supply_1m": 0,
    }
    lk = fake(list({
        "SELECT count(*) FROM iceberg.silver.driver_pings_late": 7,
        "SELECT count(*) FROM (SELECT DISTINCT driver_id, seq": 100,
        "SELECT count(*) FROM iceberg.silver.driver_pings": 100,
        "SELECT count(*) FROM iceberg.silver.trips": 10,
        "SELECT count(DISTINCT trip_id)": 10,
        "FROM iceberg.bronze.cdc_raw": 38, **ans}.items()))
    status_rows = [("completed", 9), ("cancelled", 1)]
    res = reconcile.run_checks(MANIFEST, lambda q: lk(q), lambda q: status_rows,
                               lambda q: status_rows, after_reconcile=True)
    assert any(r[0] == "FAIL" and r[1].startswith("gold supply") for r in res)


def test_detects_loss_and_duplicates():
    res = results(**{
        "SELECT count(*) FROM (SELECT DISTINCT driver_id, seq": 97,
        "SELECT count(*) FROM iceberg.silver.driver_pings": 105,
    })
    failed = {r[1] for r in res if r[0] == "FAIL"}
    assert "pings: no loss" in failed
    assert "pings: no duplicates (dedup works)" in failed
