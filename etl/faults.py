"""
Fault injection (DEMO / TESTING ONLY).

Real weather-API data is already clean, so the cleaning stats would read all
zeros — which hides the whole point of the pipeline. This module deliberately
corrupts a small fraction of an incoming payload with realistic sensor faults
(sentinels, impossible values, negative rain, duplicate rows) so the Transform
and Data-Quality stages have something to catch and you can SEE them work.

This is standard "chaos testing" for a data pipeline. It is OFF by default and
only enabled with --simulate-faults, so production runs stay honest.
"""
import copy
import random
import logging

log = logging.getLogger("etl.faults")


def inject(raw: dict, rate: float = 0.06, seed: int | None = None) -> tuple[dict, dict]:
    """Corrupt ~`rate` of the readings. Returns (corrupted_raw, report)."""
    rng = random.Random(seed)
    raw = copy.deepcopy(raw)
    h = raw.get("hourly", {})
    n = len(h.get("time", []))
    if not n:
        return raw, {"injected": 0}

    temp = list(h.get("temperature_2m") or [None] * n)
    hum = list(h.get("relative_humidity_2m") or [None] * n)
    rain = list(h.get("precipitation") or [0.0] * n)

    report = {"sentinels": 0, "bad_humidity": 0, "neg_rain": 0,
              "conflicts": 0, "dupes": 0}
    k = max(1, int(n * rate))

    for _ in range(k):
        i = rng.randrange(n)
        kind = rng.choice(["sentinel", "humidity", "rain", "missing", "conflict"])
        if kind == "sentinel":
            temp[i] = -999.0; report["sentinels"] += 1
        elif kind == "humidity":
            hum[i] = rng.choice([150, -5, 999]); report["bad_humidity"] += 1
        elif kind == "rain":
            rain[i] = -rng.uniform(1, 5); report["neg_rain"] += 1
        elif kind == "conflict":  # heavy rain + bone-dry air: fields disagree
            rain[i] = round(rng.uniform(8, 20), 1)
            hum[i] = round(rng.uniform(10, 35))
            report["conflicts"] += 1
        else:  # missing
            temp[i] = None

    h["temperature_2m"] = temp
    h["relative_humidity_2m"] = hum
    h["precipitation"] = rain

    # inject a couple of duplicate timestamps
    times = list(h["time"])
    for _ in range(2):
        if len(times) > 3:
            i = rng.randrange(len(times) - 1)
            times.insert(i, times[i])
            temp.insert(i, temp[i]); hum.insert(i, hum[i]); rain.insert(i, rain[i])
            report["dupes"] += 1
    h["time"] = times

    report["injected"] = sum(v for k, v in report.items() if k != "injected")
    log.info("Fault injection: %s", report)
    return raw, report
