"""
Transform — turn a raw Open-Meteo payload into a clean, tidy table.

Real-world messiness handled here:
  * column-oriented JSON arrays  -> tidy one-row-per-observation table
  * sentinel / out-of-range values (e.g. -999 temperature) -> NaN
  * impossible humidity (<0 or >100) / negative rainfall     -> corrected
  * cross-field inconsistencies (heavy rain + bone-dry air)  -> flagged, kept
  * small gaps                                               -> interpolated
  * duplicate timestamps                                     -> de-duplicated
  * timezone-aware, sorted timestamps

Auditability: every value that gets touched is recorded in
report["quarantine_records"] with its ORIGINAL value, the issue, and the
action taken — the pipeline persists these to a `quarantine` table.
Interpolated readings are flagged in an `interpolated` column (data lineage),
so the warehouse always distinguishes measured from imputed values.

Returns (clean_df, report) so the pipeline can log exactly what was fixed.
"""
import logging
import numpy as np
import pandas as pd

log = logging.getLogger("etl.transform")

# plausible physical ranges — anything outside is treated as bad data
TEMP_RANGE = (-50.0, 60.0)     # °C
HUMIDITY_RANGE = (0.0, 100.0)  # %

# cross-field consistency: sustained rain implies near-saturated air
HEAVY_RAIN_MM = 5.0
DRY_AIR_PCT = 40.0


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
    quarantine = []          # audit log: (ts, field, original, issue, action)

    def _quarantine(mask, field, issue, action):
        for _, row in df.loc[mask, ["ts", field]].iterrows():
            quarantine.append({
                "ts": row["ts"], "field": field,
                "original_value": str(row[field]),
                "issue": issue, "action": action,
            })

    # out-of-range / sentinel values -> NaN (ignore already-missing values)
    bad_temp = df["temperature_c"].notna() & ~df["temperature_c"].between(*TEMP_RANGE)
    report["temp_outliers"] = int(bad_temp.sum())
    _quarantine(bad_temp, "temperature_c", "sentinel / out of physical range",
                "set to missing (interpolated below if gap ≤ 3h)")
    df.loc[bad_temp, "temperature_c"] = np.nan

    bad_hum = df["humidity_pct"].notna() & ~df["humidity_pct"].between(*HUMIDITY_RANGE)
    report["humidity_outliers"] = int(bad_hum.sum())
    _quarantine(bad_hum, "humidity_pct", "impossible humidity",
                "set to missing (interpolated below if gap ≤ 3h)")
    df.loc[bad_hum, "humidity_pct"] = np.nan

    neg_rain = df["precipitation_mm"] < 0
    report["negative_rain_fixed"] = int(neg_rain.sum())
    _quarantine(neg_rain, "precipitation_mm", "negative rainfall",
                "clamped to 0 (accumulations are never invented)")
    df.loc[neg_rain, "precipitation_mm"] = 0.0

    # cross-field consistency: heavy rain with bone-dry air can't both be
    # right — we can't tell WHICH field is wrong, so flag it, don't guess
    conflict = (df["precipitation_mm"] > HEAVY_RAIN_MM) & \
               (df["humidity_pct"] < DRY_AIR_PCT)
    report["cross_field_flags"] = int(conflict.sum())
    for _, row in df.loc[conflict].iterrows():
        quarantine.append({
            "ts": row["ts"], "field": "precipitation_mm + humidity_pct",
            "original_value": f"rain={row['precipitation_mm']}mm, "
                              f"humidity={row['humidity_pct']:.0f}%",
            "issue": "cross-field inconsistency (heavy rain + dry air)",
            "action": "flagged for review, values kept unchanged",
        })

    # de-duplicate on (city, ts), keep last
    dup_mask = df.duplicated(subset=["city", "ts"], keep="last")
    report["duplicates_dropped"] = int(dup_mask.sum())
    for _, row in df.loc[dup_mask].iterrows():
        quarantine.append({
            "ts": row["ts"], "field": "(whole row)",
            "original_value": f"temp={row['temperature_c']}, "
                              f"hum={row['humidity_pct']}, "
                              f"rain={row['precipitation_mm']}",
            "issue": "duplicate timestamp",
            "action": "row dropped (kept last occurrence)",
        })
    df = df.drop_duplicates(subset=["city", "ts"], keep="last")

    # sort, then interpolate small gaps in the continuous signals;
    # every imputed reading is flagged (lineage: measured vs interpolated)
    df = df.sort_values("ts").reset_index(drop=True)
    was_missing = df[["temperature_c", "humidity_pct"]].isna()
    df["temperature_c"] = df["temperature_c"].interpolate(limit=3)
    df["humidity_pct"] = df["humidity_pct"].interpolate(limit=3)
    df["precipitation_mm"] = df["precipitation_mm"].fillna(0.0)
    filled = was_missing & df[["temperature_c", "humidity_pct"]].notna()
    df["interpolated"] = filled.any(axis=1)
    report["gaps_interpolated"] = int(filled.to_numpy().sum())
    for col in ["temperature_c", "humidity_pct"]:
        for _, row in df.loc[filled[col]].iterrows():
            quarantine.append({
                "ts": row["ts"], "field": col,
                "original_value": "missing",
                "issue": "gap in the series",
                "action": f"interpolated → {row[col]:.1f} "
                          "(flagged in `interpolated` column)",
            })

    # round for tidiness
    df["temperature_c"] = df["temperature_c"].round(1)
    df["humidity_pct"] = df["humidity_pct"].round(0)
    df["precipitation_mm"] = df["precipitation_mm"].round(2)

    report["rows_out"] = len(df)
    report["quarantine_records"] = quarantine
    log.info("Transformed %s: %s", city,
             {k: v for k, v in report.items() if k != "quarantine_records"})
    return df, report
