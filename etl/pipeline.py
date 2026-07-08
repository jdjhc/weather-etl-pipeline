"""
Pipeline — orchestrate Extract -> Transform -> Load -> Data Quality,
with structured logging and a per-run summary written to a `runs` log table.
"""
import time
import logging
import datetime as dt

import duckdb

from . import extract, transform, load, quality, faults

log = logging.getLogger("etl.pipeline")


def _record_run(db_path, summary):
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_at TIMESTAMP, cities VARCHAR, rows_inserted INTEGER,
            checks_passed INTEGER, checks_total INTEGER, ok BOOLEAN,
            simulated BOOLEAN DEFAULT FALSE
        )
    """)
    # migrate warehouses created before the `simulated` column existed
    con.execute(
        "ALTER TABLE runs ADD COLUMN IF NOT EXISTS simulated BOOLEAN DEFAULT FALSE")
    con.execute("""
        CREATE TABLE IF NOT EXISTS transform_reports (
            run_at TIMESTAMP, city VARCHAR, simulated BOOLEAN,
            rows_in INTEGER, temp_outliers INTEGER, humidity_outliers INTEGER,
            negative_rain_fixed INTEGER, duplicates_dropped INTEGER,
            gaps_interpolated INTEGER, rows_out INTEGER, rows_inserted INTEGER
        )
    """)
    con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?)", [
        summary["run_at"], ",".join(summary["cities"]),
        summary["rows_inserted"], summary["checks_passed"],
        summary["checks_total"], summary["ok"], summary["simulated"],
    ])
    for r in summary["reports"]:
        con.execute("INSERT INTO transform_reports VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
            summary["run_at"], r["city"], summary["simulated"],
            r["rows_in"], r["temp_outliers"], r["humidity_outliers"],
            r["negative_rain_fixed"], r["duplicates_dropped"],
            r["gaps_interpolated"], r["rows_out"], r["rows_inserted"],
        ])
    con.close()


def run(cities=("auckland",), past_days=7, db_path="warehouse.duckdb",
        simulate_faults=False):
    t0 = time.time()
    total_inserted = 0
    reports = []

    for city in cities:
        raw = extract.fetch_weather(city, past_days=past_days)   # E
        if simulate_faults:                                     # (demo) chaos
            raw, fault_report = faults.inject(raw)
            log.info("Injected faults for %s: %s", city, fault_report)
        clean_df, report = transform.clean(raw)                  # T
        report["rows_inserted"] = load.upsert(clean_df, db_path)  # L
        reports.append(report)
        total_inserted += report["rows_inserted"]

    checks = quality.run_checks(db_path)                         # DQ
    passed = sum(c["passed"] for c in checks)

    summary = {
        "run_at": dt.datetime.now(),
        "cities": list(cities),
        "rows_inserted": total_inserted,
        "checks_passed": passed,
        "checks_total": len(checks),
        "ok": passed == len(checks),
        "simulated": simulate_faults,
        "reports": reports,
        "checks": checks,
        "seconds": round(time.time() - t0, 1),
    }
    _record_run(db_path, summary)

    log.info("Run done in %ss: +%d rows, DQ %d/%d %s",
             summary["seconds"], total_inserted, passed, len(checks),
             "OK" if summary["ok"] else "FAILED")
    return summary
