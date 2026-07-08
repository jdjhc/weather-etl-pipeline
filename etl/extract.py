"""
Extract — pull raw weather data from the Open-Meteo API.

Open-Meteo is free and needs no API key. We request an hourly series with a
few `past_days` so the pipeline can be run daily and back-fill recent history.
Includes basic retry so a flaky network doesn't kill the run.
"""
import time
import logging
import requests

log = logging.getLogger("etl.extract")

API_URL = "https://api.open-meteo.com/v1/forecast"

# a few NZ cities so the demo isn't single-source
CITIES = {
    "auckland":    {"latitude": -36.8485, "longitude": 174.7633},
    "wellington":  {"latitude": -41.2866, "longitude": 174.7756},
    "christchurch": {"latitude": -43.5321, "longitude": 172.6362},
}


def fetch_weather(city: str, past_days: int = 7, retries: int = 3) -> dict:
    """Return the raw JSON payload for one city (hourly temp/humidity/rain)."""
    if city not in CITIES:
        raise ValueError(f"Unknown city '{city}'. Options: {list(CITIES)}")

    params = {
        **CITIES[city],
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "Pacific/Auckland",
    }

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            log.info("Extracting %s (attempt %d/%d)", city, attempt, retries)
            r = requests.get(API_URL, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
            payload["_city"] = city                 # tag the source
            return payload
        except Exception as e:                       # network / http / json
            last_err = e
            log.warning("Extract failed (%s); retrying in %ds", e, 2 * attempt)
            time.sleep(2 * attempt)
    raise RuntimeError(f"Extract failed for {city} after {retries} tries: {last_err}")
