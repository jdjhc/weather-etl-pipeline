"""
Streamlit dashboard — visualize the DuckDB warehouse the pipeline maintains.

    streamlit run dashboard.py

Read-only: it never writes to the warehouse, so it is safe to keep open
while the pipeline runs.
"""
import datetime as dt

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

DB_PATH = "warehouse.duckdb"

# One fixed color per city (never reassigned when filters change)
CITY_COLORS = {
    "auckland": "#2a78d6",
    "wellington": "#1baf7a",
    "christchurch": "#eda100",
}

RANGE_PRESETS = {
    "Last 24 h": dt.timedelta(hours=24),
    "Last 3 days": dt.timedelta(days=3),
    "Last 7 days": dt.timedelta(days=7),
    "All data": None,
}

st.set_page_config(page_title="Weather ETL Dashboard", page_icon="🌦️", layout="wide")


@st.cache_data(ttl=60)
def load_weather() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute(
            "SELECT city, ts, temperature_c, humidity_pct, precipitation_mm "
            "FROM weather_hourly ORDER BY ts"
        ).fetchdf()
    finally:
        con.close()


@st.cache_data(ttl=60)
def load_runs() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute("SELECT * FROM runs ORDER BY run_at DESC").fetchdf()
    finally:
        con.close()


def city_color_scale(cities: list[str]) -> alt.Scale:
    return alt.Scale(domain=cities, range=[CITY_COLORS[c] for c in cities])


def time_axis() -> alt.X:
    return alt.X("ts:T", title=None, axis=alt.Axis(format="%d %b %H:%M", grid=False))


st.title("🌦️ Weather ETL Dashboard")

try:
    weather = load_weather()
except (duckdb.IOException, duckdb.CatalogException):
    st.error(
        f"Warehouse `{DB_PATH}` not found or empty — run the pipeline first: "
        "`python run.py`"
    )
    st.stop()

if weather.empty:
    st.warning("The warehouse has no rows yet — run `python run.py` first.")
    st.stop()

runs = load_runs()

# ---- filters (one row above the charts) ------------------------------------
available = [c for c in CITY_COLORS if c in set(weather["city"])]
fcol1, fcol2 = st.columns([3, 2])
with fcol1:
    cities = st.multiselect("Cities", available, default=available)
with fcol2:
    preset = st.radio("Time range", list(RANGE_PRESETS), index=2, horizontal=True)

if not cities:
    st.info("Select at least one city.")
    st.stop()

df = weather[weather["city"].isin(cities)]
window = RANGE_PRESETS[preset]
if window is not None:
    df = df[df["ts"] >= df["ts"].max() - window]

# ---- KPI row ----------------------------------------------------------------
latest_ts = weather["ts"].max()
age_h = (dt.datetime.now() - latest_ts).total_seconds() / 3600
last_run = runs.iloc[0] if not runs.empty else None

k1, k2, k3, k4 = st.columns(4)
k1.metric("Rows in warehouse", f"{len(weather):,}")
k2.metric("Cities tracked", f"{weather['city'].nunique()}")
k3.metric("Latest observation", latest_ts.strftime("%d %b %H:%M"),
          f"{abs(age_h):.0f} h {'ahead (forecast)' if age_h < 0 else 'old'}",
          delta_color="off")
if last_run is not None:
    k4.metric("Last run data quality",
              f"{last_run['checks_passed']}/{last_run['checks_total']} checks",
              "OK" if last_run["ok"] else "FAILED",
              delta_color="normal" if last_run["ok"] else "inverse")

st.divider()

# ---- temperature ------------------------------------------------------------
st.subheader("Temperature (°C)")
temp_chart = (
    alt.Chart(df)
    .mark_line(strokeWidth=2)
    .encode(
        x=time_axis(),
        y=alt.Y("temperature_c:Q", title="°C", scale=alt.Scale(zero=False)),
        color=alt.Color("city:N", scale=city_color_scale(cities), title=None),
        tooltip=[
            alt.Tooltip("city:N", title="City"),
            alt.Tooltip("ts:T", title="Time", format="%d %b %H:%M"),
            alt.Tooltip("temperature_c:Q", title="Temp °C", format=".1f"),
        ],
    )
    .properties(height=280)
)
st.altair_chart(temp_chart, width="stretch")

# ---- humidity + precipitation (two measures -> two charts, one axis each) ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("Relative humidity (%)")
    hum_chart = (
        alt.Chart(df)
        .mark_line(strokeWidth=2)
        .encode(
            x=time_axis(),
            y=alt.Y("humidity_pct:Q", title="%", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color("city:N", scale=city_color_scale(cities), title=None),
            tooltip=[
                alt.Tooltip("city:N", title="City"),
                alt.Tooltip("ts:T", title="Time", format="%d %b %H:%M"),
                alt.Tooltip("humidity_pct:Q", title="Humidity %", format=".0f"),
            ],
        )
        .properties(height=240)
    )
    st.altair_chart(hum_chart, width="stretch")

with c2:
    st.subheader("Daily precipitation (mm)")
    daily = (
        df.assign(date=df["ts"].dt.floor("D"))
        .groupby(["date", "city"], as_index=False)["precipitation_mm"].sum()
    )
    rain_chart = (
        alt.Chart(daily)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("date:T", title=None, axis=alt.Axis(format="%d %b", grid=False)),
            xOffset=alt.XOffset("city:N"),
            y=alt.Y("precipitation_mm:Q", title="mm"),
            color=alt.Color("city:N", scale=city_color_scale(cities), title=None),
            tooltip=[
                alt.Tooltip("city:N", title="City"),
                alt.Tooltip("date:T", title="Date", format="%d %b"),
                alt.Tooltip("precipitation_mm:Q", title="Rain mm", format=".1f"),
            ],
        )
        .properties(height=240)
    )
    st.altair_chart(rain_chart, width="stretch")

# ---- pipeline runs (audit trail) ---------------------------------------------
st.divider()
st.subheader("Pipeline run history")
if runs.empty:
    st.info("No runs recorded yet.")
else:
    runs_view = runs.copy()
    runs_view["data quality"] = runs_view.apply(
        lambda r: f"{'✅' if r['ok'] else '❌'} {r['checks_passed']}/{r['checks_total']}",
        axis=1,
    )
    st.dataframe(
        runs_view[["run_at", "cities", "rows_inserted", "data quality"]],
        width="stretch",
        hide_index=True,
    )

# ---- accessible table view (relief for low-contrast series colors) ----------
with st.expander("View filtered data as a table"):
    st.dataframe(df.sort_values("ts", ascending=False),
                 width="stretch", hide_index=True)
