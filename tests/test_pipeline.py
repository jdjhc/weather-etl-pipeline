"""
Offline unit tests — verify the Transform and Load/Quality logic WITHOUT
hitting the network, by feeding a deliberately messy synthetic payload.

Run:  python -m pytest -q     (or: python tests/test_pipeline.py)
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime as dt

import duckdb

from etl import transform, load, quality, faults, pipeline


# a raw payload that mimics Open-Meteo but is full of real-world problems
MESSY_RAW = {
    "_city": "testville",
    "hourly": {
        "time": [
            "2026-07-01T00:00", "2026-07-01T01:00", "2026-07-01T02:00",
            "2026-07-01T02:00",                       # duplicate timestamp
            "2026-07-01T03:00", "2026-07-01T04:00",
        ],
        "temperature_2m":      [12.3, 11.8, -999.0, 11.5, None, 10.9],  # sentinel + gap
        "relative_humidity_2m": [80,  82,   85,     85,   150,  88],    # 150 impossible
        "precipitation_2m":    None,  # wrong key on purpose -> handled as missing
        "precipitation":       [0.0, 0.0, -2.0, 0.0, 0.1, 0.0],         # negative rain
    },
}


def test_transform_cleans_everything():
    df, report = transform.clean(MESSY_RAW)
    assert report["temp_outliers"] == 1          # the -999
    assert report["humidity_outliers"] == 1      # the 150
    assert report["negative_rain_fixed"] == 1    # the -2.0
    assert report["duplicates_dropped"] == 1     # the repeated 02:00
    # no impossible values survive
    assert df["temperature_c"].between(-50, 60).all()
    assert df["humidity_pct"].between(0, 100).all()
    assert (df["precipitation_mm"] >= 0).all()
    # duplicate timestamp gone
    assert not df.duplicated(subset=["city", "ts"]).any()
    print("transform OK:", report)


def test_load_is_idempotent_and_quality_passes():
    df, _ = transform.clean(MESSY_RAW)
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "test.duckdb")
        first = load.upsert(df, db)
        second = load.upsert(df, db)              # same data again
        assert first == len(df)
        assert second == 0                         # incremental: nothing new
        checks = quality.run_checks(db, max_staleness_hours=10**9)
        assert all(c["passed"] for c in checks), checks
        print("load+quality OK: inserted", first, "then", second)


def _clean_payload(hours=48):
    start = dt.datetime(2026, 7, 1)
    times = [(start + dt.timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M")
             for i in range(hours)]
    return {
        "_city": "testville",
        "hourly": {
            "time": times,
            "temperature_2m": [10.0 + (i % 12) * 0.5 for i in range(hours)],
            "relative_humidity_2m": [70 + (i % 20) for i in range(hours)],
            "precipitation": [0.0] * hours,
        },
    }


def test_fault_injection_is_reproducible_and_cleaned():
    raw = _clean_payload()
    corrupted, freport = faults.inject(raw, seed=42)
    corrupted2, freport2 = faults.inject(raw, seed=42)
    assert freport == freport2                     # same seed -> same faults
    assert freport["injected"] > 0
    assert raw["hourly"]["temperature_2m"][0] == 10.0   # original untouched

    df, report = transform.clean(corrupted)
    fixed = (report["temp_outliers"] + report["humidity_outliers"]
             + report["negative_rain_fixed"] + report["duplicates_dropped"])
    assert fixed > 0                               # transform caught faults
    # nothing corrupted survives into the clean table
    assert df["temperature_c"].dropna().between(-50, 60).all()
    assert df["humidity_pct"].dropna().between(0, 100).all()
    assert (df["precipitation_mm"] >= 0).all()
    assert not df.duplicated(subset=["city", "ts"]).any()
    print("faults OK:", freport, "->", report)


def test_transform_reports_are_persisted():
    df, report = transform.clean(MESSY_RAW)
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "test.duckdb")
        report["rows_inserted"] = load.upsert(df, db)
        summary = {
            "run_at": dt.datetime.now(), "cities": ["testville"],
            "rows_inserted": report["rows_inserted"], "checks_passed": 6,
            "checks_total": 6, "ok": True, "simulated": True,
            "reports": [report],
        }
        pipeline._record_run(db, summary)
        con = duckdb.connect(db, read_only=True)
        rows = con.execute("SELECT city, simulated, temp_outliers, "
                           "duplicates_dropped FROM transform_reports").fetchall()
        run_row = con.execute("SELECT simulated FROM runs").fetchone()
        q = con.execute("SELECT field, original_value, issue "
                        "FROM quarantine ORDER BY field").fetchall()
        con.close()
        assert rows == [("testville", True, 1, 1)]
        assert run_row == (True,)
        # quarantine preserves the ORIGINAL bad values, e.g. the -999 sentinel
        assert any("-999" in orig for _, orig, _ in q)
        assert len(q) == len(report["quarantine_records"])
        print("persistence OK:", rows, "| quarantine rows:", len(q))


def test_migrations_do_not_reset_existing_flags():
    # regression: DuckDB's ADD COLUMN IF NOT EXISTS silently resets an
    # existing column to its default, wiping simulated/interpolated flags
    # on every later run — migrations must check the schema first
    df, report = transform.clean(MESSY_RAW)
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "test.duckdb")
        report["rows_inserted"] = load.upsert(df, db)
        summary = {
            "run_at": dt.datetime.now(), "cities": ["testville"],
            "rows_inserted": report["rows_inserted"], "checks_passed": 6,
            "checks_total": 6, "ok": True, "simulated": True,
            "reports": [report],
        }
        pipeline._record_run(db, summary)
        # a second, non-simulated run re-executes every migration path
        summary2 = {**summary, "run_at": dt.datetime.now(), "simulated": False}
        load.upsert(df, db)
        pipeline._record_run(db, summary2)
        con = duckdb.connect(db, read_only=True)
        flags = con.execute(
            "SELECT simulated FROM runs ORDER BY run_at").fetchall()
        interp = con.execute(
            "SELECT COUNT(*) FROM weather_hourly WHERE interpolated").fetchone()[0]
        con.close()
        assert flags == [(True,), (False,)]      # first run's flag survives
        assert interp == int(df["interpolated"].sum())
        print("migration-safety OK:", flags)


def test_cross_field_conflicts_are_flagged_not_fixed():
    raw = _clean_payload(hours=6)
    h = raw["hourly"]
    h["precipitation"][2] = 12.0          # heavy rain...
    h["relative_humidity_2m"][2] = 20     # ...in bone-dry air: contradiction
    df, report = transform.clean(raw)
    assert report["cross_field_flags"] == 1
    # flagged but NOT modified — the pipeline never guesses which field is wrong
    row = df[df["precipitation_mm"] == 12.0]
    assert len(row) == 1 and row["humidity_pct"].iloc[0] == 20
    q = [r for r in report["quarantine_records"] if "cross-field" in r["issue"]]
    assert len(q) == 1 and "kept" in q[0]["action"]
    print("cross-field OK:", q[0])


def test_interpolated_values_are_lineage_flagged():
    raw = _clean_payload(hours=6)
    raw["hourly"]["temperature_2m"][3] = None      # one gap
    df, report = transform.clean(raw)
    assert report["gaps_interpolated"] == 1
    assert df["interpolated"].sum() == 1           # exactly that row is flagged
    assert not df.loc[~df["interpolated"], "temperature_c"].isna().any()
    # the lineage flag survives the load into the warehouse
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "test.duckdb")
        load.upsert(df, db)
        con = duckdb.connect(db, read_only=True)
        n = con.execute("SELECT COUNT(*) FROM weather_hourly "
                        "WHERE interpolated").fetchone()[0]
        con.close()
        assert n == 1
    print("lineage OK: 1 interpolated row flagged end-to-end")


if __name__ == "__main__":
    test_transform_cleans_everything()
    test_load_is_idempotent_and_quality_passes()
    test_fault_injection_is_reproducible_and_cleaned()
    test_transform_reports_are_persisted()
    test_migrations_do_not_reset_existing_flags()
    test_cross_field_conflicts_are_flagged_not_fixed()
    test_interpolated_values_are_lineage_flagged()
    print("\nAll tests passed ✅")
