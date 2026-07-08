"""
Transform — turn a raw Open-Meteo payload into a clean, tidy table.

Real-world messiness handled here:
  * column-oriented JSON arrays  -> tidy one-row-per-observation table
  * sentinel / out-of-range values (e.g. -999 temperature) -> NaN
  * impossible humidity (<0 or >100) / negative rainfall     -> corrected
  * small gaps                                               -> interpolated
  * duplicate timestamps                                     -> de-duplicated
  * timezone-aware, sorted timestamps

Returns (clean_df, report) so the pipeline can log exactly what was fixed.
"""
import logging
import numpy as np
import pandas as pd

log = logging.getLogger("etl.transform")

# plausible physical ranges — anything outside is treated as bad data
TEMP_RANGE = (-50.0, 60.0)     # °C
HUMIDITY_RANGE = (0.0, 100.0)  # %


def clean(raw: dict) -> tuple[pd.DataFrame, dict]:
    city = raw.get("_city", "unknown")
    hourly = raw.get("hourly", {})
    if not hourly or "time" not in hourly:
        raise ValueError("Payload has no 'hourly' data")

    df = pd.DataFrame({
        "ts": hourly["time"],
        "temperature_c": hourly.get("temperature_2m"),
        "humidity_pct": hourly.get("relative_humidity_2m"),
        "precipitation_mm": hourly.get("precipitation"),
    })
    df["city"] = city
    rows_in = len(df)

    # timestamps -> tz-aware datetime
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"])

    # numeric coercion
    for col in ["temperature_c", "humidity_pct", "precipitation_mm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    report = {"city": city, "rows_in": rows_in}

    # out-of-range / sentinel values -> NaN (ignore already-missing values)
    bad_temp = df["temperature_c"].notna() & ~df["temperature_c"].between(*TEMP_RANGE)
    report["temp_outliers"] = int(bad_temp.sum())
    df.loc[bad_temp, "temperature_c"] = np.nan

    bad_hum = df["humidity_pct"].notna() & ~df["humidity_pct"].between(*HUMIDITY_RANGE)
    report["humidity_outliers"] = int(bad_hum.sum())
    df.loc[bad_hum, "humidity_pct"] = np.nan

    neg_rain = df["precipitation_mm"] < 0
    report["negative_rain_fixed"] = int(neg_rain.sum())
    df.loc[neg_rain, "precipitation_mm"] = 0.0

    # de-duplicate on (city, ts), keep last
    dupes = df.duplicated(subset=["city", "ts"]).sum()
    report["duplicates_dropped"] = int(dupes)
    df = df.drop_duplicates(subset=["city", "ts"], keep="last")

    # sort, then interpolate small gaps in the continuous signals
    df = df.sort_values("ts").reset_index(drop=True)
    missing_before = int(df[["temperature_c", "humidity_pct"]].isna().sum().sum())
    df["temperature_c"] = df["temperature_c"].interpolate(limit=3)
    df["humidity_pct"] = df["humidity_pct"].interpolate(limit=3)
    df["precipitation_mm"] = df["precipitation_mm"].fillna(0.0)
    missing_after = int(df[["temperature_c", "humidity_pct"]].isna().sum().sum())
    report["gaps_interpolated"] = missing_before - missing_after

    # round for tidiness
    df["temperature_c"] = df["temperature_c"].round(1)
    df["humidity_pct"] = df["humidity_pct"].round(0)
    df["precipitation_mm"] = df["precipitation_mm"].round(2)

    report["rows_out"] = len(df)
    log.info("Transformed %s: %s", city, report)
    return df, report
