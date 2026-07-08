"""
Data quality — automated checks run against the warehouse after each load.

If a check fails the pipeline flags it (and returns a non-zero status), the
same way a real pipeline would alert on a broken feed. Mirrors the "data
quality, testing and monitoring" part of a data-engineering role.
"""
import logging
import duckdb

log = logging.getLogger("etl.quality")

TABLE = "weather_hourly"


def run_checks(db_path: str = "warehouse.duckdb", max_staleness_hours: int = 48):
    con = duckdb.connect(db_path)
    checks = []

    def add(name, passed, detail):
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    # 1. warehouse is not empty
    n = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    add("row_count > 0", n > 0, f"{n} rows")

    if n:
        # 2. null rate on key columns
        nulls = con.execute(f"""
            SELECT
              AVG(CASE WHEN temperature_c IS NULL THEN 1 ELSE 0 END) AS t,
              AVG(CASE WHEN humidity_pct  IS NULL THEN 1 ELSE 0 END) AS h
            FROM {TABLE}
        """).fetchone()
        add("temperature null rate < 5%", nulls[0] < 0.05, f"{nulls[0]:.1%}")
        add("humidity null rate < 5%", nulls[1] < 0.05, f"{nulls[1]:.1%}")

        # 3. values within physical range
        bad = con.execute(f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE temperature_c NOT BETWEEN -50 AND 60
               OR humidity_pct  NOT BETWEEN 0 AND 100
               OR precipitation_mm < 0
        """).fetchone()[0]
        add("all values in valid range", bad == 0, f"{bad} out-of-range rows")

        # 4. no duplicate (city, ts)
        dup = con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT city, ts, COUNT(*) c FROM {TABLE}
              GROUP BY city, ts HAVING c > 1
            )
        """).fetchone()[0]
        add("no duplicate keys", dup == 0, f"{dup} duplicate keys")

        # 5. freshness — most recent reading is recent enough
        stale = con.execute(f"""
            SELECT date_diff('hour', MAX(ts), now()) FROM {TABLE}
        """).fetchone()[0]
        add(f"data fresh (< {max_staleness_hours}h)",
            stale is not None and stale <= max_staleness_hours,
            f"{stale}h old")

    con.close()
    passed = sum(c["passed"] for c in checks)
    log.info("Data quality: %d/%d checks passed", passed, len(checks))
    return checks
