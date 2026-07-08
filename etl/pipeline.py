"""
Pipeline — orchestrate Extract -> Transform -> Load -> Data Quality,
with structured logging and a per-run summary written to a `runs` log table.
"""
import time
import logging
import datetime as dt

import duckdb

from . import extract, transform, load, quality

log = logging.getLogger("etl.pipeline")


def _record_run(db_path, summary):
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_at TIMESTAMP, cities VARCHAR, rows_inserted INTEGER,
            checks_passed INTEGER, checks_total INTEGER, ok BOOLEAN
        )
    """)
    con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?)", [
        summary["run_at"], ",".join(summary["cities"]),
        summary["rows_inserted"], summary["checks_passed"],
        summary["checks_total"], summary["ok"],
    ])
    con.close()


def run(cities=("auckland",), past_days=7, db_path="warehouse.duckdb"):
    t0 = time.time()
    total_inserted = 0
    reports = []

    for city in cities:
        raw = extract.fetch_weather(city, past_days=past_days)   # E
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
        "reports": reports,
        "checks": checks,
        "seconds": round(time.time() - t0, 1),
    }
    _record_run(db_path, summary)

    log.info("Run done in %ss: +%d rows, DQ %d/%d %s",
             summary["seconds"], total_inserted, passed, len(checks),
             "OK" if summary["ok"] else "FAILED")
    return summary
