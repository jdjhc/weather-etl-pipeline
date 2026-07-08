"""
Tiny scheduler — run the pipeline once a day.

For a demo this simple loop is enough. In production you'd use Airflow /
Dagster / cron; see the README for the equivalent cron line.
"""
import time
import logging
import datetime as dt

from etl import pipeline

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

RUN_AT_HOUR = 6          # 06:00 local


def main():
    print("Scheduler started — will run daily at 06:00. Ctrl+C to stop.")
    last_run_date = None
    while True:
        now = dt.datetime.now()
        if now.hour == RUN_AT_HOUR and now.date() != last_run_date:
            pipeline.run(("auckland", "wellington", "christchurch"))
            last_run_date = now.date()
        time.sleep(60)


if __name__ == "__main__":
    main()
