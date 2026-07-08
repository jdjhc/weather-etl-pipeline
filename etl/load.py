"""
Load — write the clean table into a DuckDB warehouse, incrementally.

DuckDB stands in for a real analytical warehouse. The load is idempotent:
re-running the pipeline only appends genuinely new (city, ts) rows, so daily
runs don't create duplicates — the core of an incremental pipeline.
"""
import logging
import duckdb
import pandas as pd

log = logging.getLogger("etl.load")

TABLE = "weather_hourly"


def _connect(db_path: str):
    con = duckdb.connect(db_path)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            city              VARCHAR,
            ts                TIMESTAMP,
            temperature_c     DOUBLE,
            humidity_pct      DOUBLE,
            precipitation_mm  DOUBLE,
            interpolated      BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (city, ts)
        )
    """)
    # migrate warehouses created before the lineage column existed.
    # NB: DuckDB's ADD COLUMN IF NOT EXISTS silently RESETS an existing
    # column to its default (observed in 1.5.4), so check first.
    cols = {r[1] for r in con.execute(f"PRAGMA table_info('{TABLE}')").fetchall()}
    if "interpolated" not in cols:
        con.execute(f"ALTER TABLE {TABLE} "
                    "ADD COLUMN interpolated BOOLEAN DEFAULT FALSE")
    return con


def upsert(df: pd.DataFrame, db_path: str = "warehouse.duckdb") -> int:
    """Insert only new (city, ts) rows. Returns number of rows inserted."""
    if df.empty:
        return 0
    if "interpolated" not in df.columns:
        df = df.assign(interpolated=False)
    con = _connect(db_path)
    before = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]

    con.register("incoming", df[["city", "ts", "temperature_c",
                                 "humidity_pct", "precipitation_mm",
                                 "interpolated"]])
    # ON CONFLICT DO NOTHING => idempotent incremental load
    con.execute(f"""
        INSERT INTO {TABLE}
        SELECT * FROM incoming
        ON CONFLICT (city, ts) DO NOTHING
    """)

    after = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    con.unregister("incoming")
    con.close()
    inserted = after - before
    log.info("Loaded %d new rows (warehouse now %d)", inserted, after)
    return inserted
