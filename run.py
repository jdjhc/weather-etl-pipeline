"""
CLI entrypoint.

    python run.py                        # Auckland, last 7 days
    python run.py --cities auckland wellington christchurch
    python run.py --past-days 3 --db warehouse.duckdb
"""
import sys
import argparse
import logging

from etl import pipeline, extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    ap = argparse.ArgumentParser(description="Weather ETL pipeline")
    ap.add_argument("--cities", nargs="+", default=["auckland"],
                    choices=list(extract.CITIES))
    ap.add_argument("--past-days", type=int, default=7)
    ap.add_argument("--db", default="warehouse.duckdb")
    args = ap.parse_args()

    summary = pipeline.run(tuple(args.cities), args.past_days, args.db)

    print("\n=== RUN SUMMARY ===")
    print(f"Cities        : {', '.join(summary['cities'])}")
    print(f"Rows inserted : {summary['rows_inserted']}")
    print(f"Data quality  : {summary['checks_passed']}/{summary['checks_total']} passed")
    for c in summary["checks"]:
        print(f"  {'✅' if c['passed'] else '❌'} {c['check']:32} {c['detail']}")
    print(f"Status        : {'OK' if summary['ok'] else 'FAILED'}")

    sys.exit(0 if summary["ok"] else 1)   # non-zero exit alerts a scheduler


if __name__ == "__main__":
    main()
