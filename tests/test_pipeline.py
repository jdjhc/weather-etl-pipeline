"""
Offline unit tests — verify the Transform and Load/Quality logic WITHOUT
hitting the network, by feeding a deliberately messy synthetic payload.

Run:  python -m pytest -q     (or: python tests/test_pipeline.py)
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl import transform, load, quality


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


if __name__ == "__main__":
    test_transform_cleans_everything()
    test_load_is_idempotent_and_quality_passes()
    print("\nAll tests passed ✅")
