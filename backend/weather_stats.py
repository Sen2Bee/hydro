from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


def _quantile(sorted_vals: list[float], q: float) -> float | None:
    if not sorted_vals:
        return None
    if not (0.0 < q < 1.0):
        return None
    pos = (len(sorted_vals) - 1) * q
    base = int(pos)
    rest = pos - base
    if base + 1 >= len(sorted_vals):
        return float(sorted_vals[base])
    return float(sorted_vals[base] + rest * (sorted_vals[base + 1] - sorted_vals[base]))


def _daily_sums(series: list[dict]) -> list[float]:
    # series item: {"t": "...Z", "precip_mm": float}
    by_day: dict[str, float] = {}
    for row in series or []:
        t = row.get("t")
        if not t:
            continue
        day = str(t)[:10]  # YYYY-MM-DD
        p = row.get("precip_mm", 0.0)
        try:
            v = float(p)
        except Exception:
            v = 0.0
        by_day[day] = by_day.get(day, 0.0) + (v if v == v else 0.0)  # NaN guard
    return [by_day[k] for k in sorted(by_day.keys())]


def compute_api14(daily_mm: list[float]) -> float:
    """
    Simple antecedent precipitation index proxy over up to the last 14 days.
    Weighted: newest day weight ~1.0, oldest ~0.1 (linear).
    """
    days = (daily_mm or [])[-14:]
    n = len(days)
    if n == 0:
        return 0.0
    api = 0.0
    denom = max(1, n - 1)
    for i in range(n):
        w = 1.0 - (i / denom) * 0.9  # 1.0 -> 0.1
        api += float(days[n - 1 - i]) * w
    return float(api)


def classify_moisture(api14: float) -> str:
    # MVP thresholds; calibrate later.
    if api14 < 10.0:
        return "trocken"
    if api14 < 25.0:
        return "normal"
    return "nass"


def build_weather_stats(
    bundle: list[dict],
    *,
    quantiles: list[float] | None = None,
) -> dict:
    qs = quantiles or [0.5, 0.9, 0.95, 0.99]
    out: list[dict] = []
    for item in bundle or []:
        point = item.get("point")
        series = item.get("series") or []
        vals: list[float] = []
        for s in series:
            try:
                v = float(s.get("precip_mm", 0.0))
            except Exception:
                continue
            if v == v:
                vals.append(v)
        vals.sort()

        qmap: dict[str, float | None] = {}
        for q in qs:
            qmap[str(q)] = _quantile(vals, float(q))

        max_v = vals[-1] if vals else None
        sum_v = float(sum(vals)) if vals else 0.0

        daily = _daily_sums(series)
        api14 = compute_api14(daily)
        moisture = classify_moisture(api14)

        out.append(
            {
                "point": point,
                "station": item.get("station"),
                "precip_hourly": {
                    "count": len(vals),
                    "sum_mm": sum_v,
                    "max_mm": max_v,
                    "quantiles_mm": qmap,
                },
                "antecedent_moisture": {"api14": api14, "class": moisture},
            }
        )

    return {"perPoint": out}

