from __future__ import annotations

import datetime as dt
import math
import os
import threading

import requests

from weather_dwd import find_nearest_station, load_hourly_series


TEN_MIN_S = 10 * 60
_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_LOCK = threading.Lock()
ICON2D_TIMEOUT_S = int(os.getenv("ICON2D_TIMEOUT_S", "45") or 45)
HIST_HOST = "https://historical-forecast-api.open-meteo.com/v1/forecast"
LIVE_HOST = "https://api.open-meteo.com/v1/forecast"
MODEL = "icon_d2"
MIXED_CUTOFF_DAYS = int(os.getenv("ICON2D_CUTOFF_DAYS", "16") or 16)


def _now_s() -> float:
    return dt.datetime.now(tz=dt.timezone.utc).timestamp()


def parse_points(points_str: str) -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    for raw in (points_str or "").split(";"):
        raw = raw.strip()
        if not raw:
            continue
        parts = raw.split(",")
        if len(parts) != 2:
            raise ValueError(f"Invalid point '{raw}'. Expected 'lat,lon'")
        lat = float(parts[0])
        lon = float(parts[1])
        if not (math.isfinite(lat) and math.isfinite(lon)):
            raise ValueError(f"Invalid point '{raw}'. Expected finite lat/lon")
        pts.append((lat, lon))
    if not pts:
        raise ValueError("Missing points")
    return pts


def _cache_key(points: list[tuple[float, float]], start_iso: str, end_iso: str, agg: str) -> str:
    # stable key: round to 3 decimals + hour-rounded window + count
    def r(v: float) -> str:
        return f"{round(v, 3):.3f}"

    p0 = points[0]
    rounded_start = dt.datetime.fromisoformat(start_iso.replace("Z", "+00:00")).replace(minute=0, second=0, microsecond=0)
    rounded_end = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00")).replace(minute=0, second=0, microsecond=0)
    return f"{r(p0[0])},{r(p0[1])}_{rounded_start.isoformat()}_{rounded_end.isoformat()}_{agg}_{len(points)}"


def _cache_get(key: str) -> object | None:
    now = _now_s()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if not hit:
            return None
        ts, obj = hit
        if now - ts > TEN_MIN_S:
            _CACHE.pop(key, None)
            return None
        return obj


def _cache_set(key: str, obj: object) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (_now_s(), obj)


def _provider_mode() -> str:
    mode = (os.getenv("WEATHER_PROVIDER", "icon2d") or "icon2d").strip().lower()
    if mode not in ("auto", "icon2d", "dwd"):
        return "icon2d"
    return mode


def _icon2d_base_url() -> str | None:
    base = (os.getenv("ICON2D_BASE_URL", "") or "").strip()
    if not base:
        return None
    return base.rstrip("/")


def _icon2d_transport() -> str:
    mode = (os.getenv("ICON2D_TRANSPORT", "auto") or "auto").strip().lower()
    if mode not in ("auto", "direct", "proxy"):
        return "auto"
    return mode


def _normalize_point_key(lat: float, lon: float) -> str:
    return f"{float(lat):.5f},{float(lon):.5f}"


def _as_iso_z(val: object) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("Z"):
        return s
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
        return d.isoformat().replace("+00:00", "Z")
    except Exception:
        return s


def _as_precip_mm(row: dict) -> float | None:
    for key in ("precip_mm", "precip", "rain_mm", "rr", "rain"):
        if key not in row:
            continue
        try:
            v = float(row.get(key))
        except Exception:
            continue
        if math.isfinite(v):
            return v
    return None


def _normalize_series(rows: object) -> list[dict]:
    out: list[dict] = []
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, dict):
            continue
        t = _as_iso_z(r.get("t") or r.get("time") or r.get("timestamp") or r.get("ts"))
        p = _as_precip_mm(r)
        if not t or p is None:
            continue
        out.append({"t": t, "precip_mm": float(p)})
    return out


def _normalize_icon2d_response(raw: object, points: list[tuple[float, float]]) -> list[dict]:
    """
    Normalize different possible icon2d response shapes to:
      [{"point":"lat,lon","station":{...optional...},"series":[{"t","precip_mm"}]}]
    """
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        # Most common wrappers.
        if isinstance(raw.get("data"), list):
            items = raw.get("data")
        elif isinstance(raw.get("points"), list):
            items = raw.get("points")
        elif isinstance(raw.get("series"), list):
            items = raw.get("series")
        elif isinstance(raw.get("data"), dict):
            # map-like: {"lat,lon":[...]}
            items = [{"point": k, "series": v} for k, v in raw["data"].items()]
        elif isinstance(raw.get("seriesByPoint"), dict):
            items = [{"point": k, "series": v} for k, v in raw["seriesByPoint"].items()]
        else:
            items = []
    else:
        items = []

    out: list[dict] = []
    fallback_points = [_normalize_point_key(lat, lon) for lat, lon in points]
    idx = 0

    for it in items:
        if not isinstance(it, dict):
            continue
        p = it.get("point")
        if isinstance(p, str) and "," in p:
            pkey = p
        else:
            try:
                plat = float(it.get("lat"))
                plon = float(it.get("lon"))
                pkey = _normalize_point_key(plat, plon)
            except Exception:
                pkey = fallback_points[min(idx, len(fallback_points) - 1)] if fallback_points else "0,0"
        idx += 1

        series = _normalize_series(it.get("series") or it.get("rows") or it.get("values"))
        if not series:
            continue

        out.append(
            {
                "point": pkey,
                "station": it.get("station")
                or {
                    "source": "ICON2D grid/model",
                },
                "series": series,
            }
        )

    # Ensure one item per requested point (when provider returns map/list with missing keys).
    if out:
        by_point = {str(item.get("point")): item for item in out}
        filled: list[dict] = []
        for lat, lon in points:
            pkey = _normalize_point_key(lat, lon)
            if pkey in by_point:
                filled.append(by_point[pkey])
            else:
                # fallback: nearest available item
                filled.append(next(iter(by_point.values())))
        return filled

    raise RuntimeError("icon2d response konnte nicht normalisiert werden (keine gueltigen Reihen).")


def _fetch_batch_icon2d_proxy(points: list[tuple[float, float]], start_iso: str, end_iso: str, agg: str = "hourly") -> list[dict]:
    base = _icon2d_base_url()
    if not base:
        raise RuntimeError("ICON2D_BASE_URL ist nicht gesetzt.")
    path = (os.getenv("ICON2D_BATCH_PATH", "/weather/batch") or "/weather/batch").strip()
    if not path.startswith("/"):
        path = "/" + path
    url = base + path

    payload = {
        "points": [{"lat": float(lat), "lon": float(lon)} for lat, lon in points],
        "startISO": start_iso,
        "endISO": end_iso,
        "agg": agg,
    }
    resp = requests.post(url, json=payload, timeout=ICON2D_TIMEOUT_S)
    resp.raise_for_status()
    return _normalize_icon2d_response(resp.json(), points)


def _iso_from_open_meteo_time(t: object) -> str | None:
    if t is None:
        return None
    s = str(t).strip()
    if not s:
        return None
    if s.endswith("Z"):
        return s
    if len(s) == 16 and "T" in s:
        s = s + ":00"
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _to_yyyy_mm_dd(iso: str) -> str:
    d = dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    return d.date().isoformat()


def _icon2d_fetch_batch_all_open_meteo(
    points: list[tuple[float, float]],
    start_iso: str,
    end_iso: str,
    *,
    live: bool,
) -> list[dict]:
    if not points:
        return []

    host = LIVE_HOST if live else HIST_HOST
    lats = ",".join(f"{float(lat):.6f}" for lat, _ in points)
    lons = ",".join(f"{float(lon):.6f}" for _, lon in points)
    params = {
        "latitude": lats,
        "longitude": lons,
        "hourly": "precipitation",
        "start_date": _to_yyyy_mm_dd(start_iso),
        "end_date": _to_yyyy_mm_dd(end_iso),
        "models": MODEL,
        "timezone": "UTC",
    }

    resp = requests.get(host, params=params, timeout=ICON2D_TIMEOUT_S)
    resp.raise_for_status()
    raw = resp.json()
    locations = raw if isinstance(raw, list) else [raw]

    out: list[dict] = []
    for idx, (lat, lon) in enumerate(points):
        loc = locations[idx] if idx < len(locations) else {}
        hourly = loc.get("hourly") if isinstance(loc, dict) else {}
        times = hourly.get("time") if isinstance(hourly, dict) else None
        vals = hourly.get("precipitation") if isinstance(hourly, dict) else None
        if not isinstance(times, list) or not isinstance(vals, list):
            out.append(
                {
                    "point": _normalize_point_key(lat, lon),
                    "station": {"source": "Open-Meteo ICON-D2", "model": MODEL, "host": host},
                    "series": [],
                }
            )
            continue

        series: list[dict] = []
        for i, t in enumerate(times):
            tt = _iso_from_open_meteo_time(t)
            if not tt:
                continue
            if i >= len(vals):
                continue
            try:
                p = float(vals[i])
            except Exception:
                continue
            if not math.isfinite(p):
                continue
            series.append({"t": tt, "precip_mm": p})

        out.append(
            {
                "point": _normalize_point_key(lat, lon),
                "station": {"source": "Open-Meteo ICON-D2", "model": MODEL, "host": host},
                "series": series,
            }
        )
    return out


def _merge_point_series(a: list[dict], b: list[dict]) -> list[dict]:
    by_t: dict[str, float] = {}
    for src in (a, b):
        for row in src:
            t = row.get("t")
            if not isinstance(t, str):
                continue
            try:
                p = float(row.get("precip_mm", 0.0))
            except Exception:
                continue
            if math.isfinite(p):
                by_t[t] = p
    return [{"t": t, "precip_mm": by_t[t]} for t in sorted(by_t.keys())]


def _choose_icon2d_host_mode(start_iso: str, end_iso: str) -> str:
    start_dt = dt.datetime.fromisoformat(start_iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    end_dt = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    cutoff = (dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=MIXED_CUTOFF_DAYS)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    if end_dt <= cutoff:
        return "historical"
    if start_dt >= cutoff:
        return "live"
    return "mixed"


def _icon2d_smart_fetch_open_meteo(points: list[tuple[float, float]], start_iso: str, end_iso: str) -> list[dict]:
    mode = _choose_icon2d_host_mode(start_iso, end_iso)
    if mode == "historical":
        return _icon2d_fetch_batch_all_open_meteo(points, start_iso, end_iso, live=False)
    if mode == "live":
        return _icon2d_fetch_batch_all_open_meteo(points, start_iso, end_iso, live=True)

    cutoff = (dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=MIXED_CUTOFF_DAYS)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    cutoff_iso = cutoff.isoformat().replace("+00:00", "Z")
    hist = _icon2d_fetch_batch_all_open_meteo(points, start_iso, cutoff_iso, live=False)
    live = _icon2d_fetch_batch_all_open_meteo(points, cutoff_iso, end_iso, live=True)

    out: list[dict] = []
    n = max(len(hist), len(live), len(points))
    for idx in range(n):
        lat, lon = points[idx] if idx < len(points) else (0.0, 0.0)
        hp = hist[idx] if idx < len(hist) else {"series": [], "station": {}}
        lp = live[idx] if idx < len(live) else {"series": [], "station": {}}
        out.append(
            {
                "point": _normalize_point_key(lat, lon),
                "station": lp.get("station") or hp.get("station") or {"source": "Open-Meteo ICON-D2", "model": MODEL},
                "series": _merge_point_series(hp.get("series") or [], lp.get("series") or []),
            }
        )
    return out


def _fetch_batch_icon2d(points: list[tuple[float, float]], start_iso: str, end_iso: str, agg: str = "hourly") -> list[dict]:
    transport = _icon2d_transport()
    base = _icon2d_base_url()
    if transport == "proxy":
        return _fetch_batch_icon2d_proxy(points, start_iso, end_iso, agg)
    if transport == "direct":
        return _icon2d_smart_fetch_open_meteo(points, start_iso, end_iso)

    # auto: prefer local proxy when configured, fallback to direct Open-Meteo.
    if base:
        try:
            return _fetch_batch_icon2d_proxy(points, start_iso, end_iso, agg)
        except Exception:
            pass
    return _icon2d_smart_fetch_open_meteo(points, start_iso, end_iso)


def _fetch_batch_dwd(points: list[tuple[float, float]], start_iso: str, end_iso: str) -> list[dict]:
    # coverage validation uses dates; treat window as inclusive in that day range
    start_dt = dt.datetime.fromisoformat(start_iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    end_dt = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    start_d = start_dt.date()
    end_d = end_dt.date()

    out: list[dict] = []
    for lat, lon in points:
        st, dist_km = find_nearest_station(lat, lon, start_d, end_d)
        series = load_hourly_series(st, start_iso, end_iso)
        out.append(
            {
                "point": f"{lat},{lon}",
                "station": {
                    "id": st.station_id,
                    "name": st.name,
                    "state": st.state,
                    "lat": st.lat,
                    "lon": st.lon,
                    "height_m": st.height_m,
                    "distance_km": round(float(dist_km), 2),
                    "source": "DWD CDC hourly precipitation (RR)",
                },
                "series": series,
            }
        )
    return out


def fetch_batch(points: list[tuple[float, float]], start_iso: str, end_iso: str, agg: str = "hourly") -> list[dict]:
    """
    Batch fetch a compact hourly precipitation series for each point.

    Provider strategy:
      - WEATHER_PROVIDER=icon2d: ICON-D2 (Open-Meteo / proxy)
      - WEATHER_PROVIDER=dwd: station-based DWD (legacy/explicit)
      - WEATHER_PROVIDER=auto: behaves like icon2d (no silent DWD fallback)
    """
    mode = _provider_mode()
    key = f"{mode}:{_cache_key(points, start_iso, end_iso, agg)}"
    cached = _cache_get(key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    data: list[dict]
    if mode == "dwd":
        data = _fetch_batch_dwd(points, start_iso, end_iso)
    else:
        data = _fetch_batch_icon2d(points, start_iso, end_iso, agg)

    _cache_set(key, data)
    return data
