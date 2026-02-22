from __future__ import annotations

import datetime as dt
import gzip
import io
import math
import os
import tarfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

try:
    from pyproj import Transformer
except Exception:
    Transformer = None


RADAR_TIMEOUT_S = int(os.getenv("RADAR_TIMEOUT_S", "30") or 30)
RADAR_PROVIDER = (os.getenv("RADAR_PROVIDER", "dwd_radolan") or "dwd_radolan").strip().lower()
RADAR_EVENTS_URL = (os.getenv("RADAR_EVENTS_URL", "") or "").strip()
RADAR_MAX_HOURS = int(os.getenv("RADAR_MAX_HOURS", str(24 * 180)) or (24 * 180))
RADAR_CACHE_DIR = Path(os.getenv("RADAR_CACHE_DIR", ".cache/radolan") or ".cache/radolan")
RADOLAN_BASE = (
    os.getenv(
        "RADOLAN_BASE_URL",
        "https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/historical/asc",
    )
    or ""
).strip().rstrip("/")
RADOLAN_SCALE = float(os.getenv("RADOLAN_SCALE_MM", "0.1") or 0.1)
RADOLAN_CRS = "+proj=stere +lat_0=90 +lat_ts=60 +lon_0=10 +a=6370040 +b=6370040 +units=m +no_defs"


def _event_level(max_1h: float, max_6h: float) -> tuple[str, int]:
    if max_1h > 40.0 or max_6h > 60.0:
        return "extrem", 3
    if max_1h > 25.0 or max_6h > 35.0:
        return "unwetter", 2
    if max_1h >= 15.0 or max_6h >= 20.0:
        return "starkregen", 1
    return "none", 0


def _rolling_6h_max(mm: list[float], end_idx: int) -> float:
    s = max(0, end_idx - 5)
    return float(sum(mm[s : end_idx + 1]))


def _detect_events(series: list[dict], source: str = "radar") -> list[dict]:
    rows: list[tuple[dt.datetime, float]] = []
    for r in series or []:
        t = r.get("t")
        if not t:
            continue
        try:
            ts = dt.datetime.fromisoformat(str(t).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            p = float(r.get("precip_mm", 0.0))
        except Exception:
            continue
        if not math.isfinite(p):
            p = 0.0
        rows.append((ts, max(0.0, p)))

    rows.sort(key=lambda x: x[0])
    if not rows:
        return []

    ts = [x[0] for x in rows]
    mm = [x[1] for x in rows]
    trig = []
    for i in range(len(rows)):
        trig.append((mm[i] >= 15.0) or (_rolling_6h_max(mm, i) >= 20.0))

    events: list[dict] = []
    i = 0
    while i < len(rows):
        if not trig[i]:
            i += 1
            continue
        s = i
        e = i
        while e + 1 < len(rows):
            if trig[e + 1] or mm[e + 1] >= 0.2:
                e += 1
                continue
            break

        max_1h = max(mm[s : e + 1]) if e >= s else 0.0
        peak_idx = s + max(range(e - s + 1), key=lambda k: mm[s + k])
        max_6h = max(_rolling_6h_max(mm, j) for j in range(s, e + 1)) if e >= s else 0.0
        total_mm = float(sum(mm[s : e + 1]))
        lvl, sev = _event_level(max_1h, max_6h)
        if lvl != "none":
            events.append(
                {
                    "start": ts[s].isoformat().replace("+00:00", "Z"),
                    "end": ts[e].isoformat().replace("+00:00", "Z"),
                    "peak_ts": ts[peak_idx].isoformat().replace("+00:00", "Z"),
                    "max_1h_mm": round(float(max_1h), 2),
                    "max_6h_mm": round(float(max_6h), 2),
                    "sum_mm": round(total_mm, 2),
                    "warnstufe": lvl,
                    "severity": sev,
                    "source": source,
                }
            )
        i = e + 1

    events.sort(key=lambda ev: (ev.get("severity", 0), ev.get("max_1h_mm", 0.0), ev.get("max_6h_mm", 0.0)), reverse=True)
    return events


def _point_key(lat: float, lon: float) -> str:
    return f"{float(lat):.5f},{float(lon):.5f}"


def _parse_iso(s: str) -> dt.datetime:
    t = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc)


def _month_iter(start: dt.datetime, end: dt.datetime) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    y, m = start.year, start.month
    while True:
        out.append((y, m))
        if y == end.year and m == end.month:
            break
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _ensure_month_tar(year: int, month: int) -> Path:
    RADAR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"RW-{year:04d}{month:02d}.tar"
    local = RADAR_CACHE_DIR / fname
    if local.exists() and local.stat().st_size > 0:
        return local
    url = f"{RADOLAN_BASE}/{year:04d}/{fname}"
    with requests.get(url, timeout=RADAR_TIMEOUT_S, stream=True) as r:
        r.raise_for_status()
        with local.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
    return local


def _build_hour_requests(start: dt.datetime, end: dt.datetime) -> dict[str, list[int]]:
    cur = start.replace(minute=0, second=0, microsecond=0)
    out: dict[str, list[int]] = defaultdict(list)
    while cur <= end:
        dkey = cur.strftime("%Y%m%d")
        out[dkey].append(cur.hour)
        cur += dt.timedelta(hours=1)
    return out


def _transform_points(points: list[tuple[float, float]]) -> dict[str, tuple[float, float]]:
    if Transformer is None:
        raise RuntimeError("pyproj fehlt fuer RADOLAN-Transformation.")
    tr = Transformer.from_crs("EPSG:4326", RADOLAN_CRS, always_xy=True)
    out: dict[str, tuple[float, float]] = {}
    for lat, lon in points:
        x, y = tr.transform(float(lon), float(lat))
        out[_point_key(lat, lon)] = (float(x), float(y))
    return out


def _parse_asc_point_values(
    asc_bytes: bytes,
    point_xy: dict[str, tuple[float, float]],
) -> dict[str, float | None]:
    f = io.StringIO(asc_bytes.decode("ascii", errors="replace"))
    header: dict[str, float] = {}
    for _ in range(6):
        line = f.readline()
        if not line:
            raise RuntimeError("RADOLAN ASC Header unvollstaendig.")
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        header[parts[0].lower()] = float(parts[-1])
    ncols = int(header.get("ncols", 0))
    nrows = int(header.get("nrows", 0))
    xll = float(header.get("xllcorner", 0.0))
    yll = float(header.get("yllcorner", 0.0))
    cellsize = float(header.get("cellsize", 1000.0))
    nodata = float(header.get("nodata_value", -1))
    if ncols <= 0 or nrows <= 0:
        raise RuntimeError("RADOLAN ASC Rasterheader ungueltig.")

    row_to_points: dict[int, list[tuple[str, int]]] = defaultdict(list)
    out: dict[str, float | None] = {k: None for k in point_xy.keys()}
    for pkey, (x, y) in point_xy.items():
        col = int((x - xll) // cellsize)
        row_from_bottom = int((y - yll) // cellsize)
        row = nrows - 1 - row_from_bottom
        if row < 0 or row >= nrows or col < 0 or col >= ncols:
            continue
        row_to_points[row].append((pkey, col))

    if not row_to_points:
        return out

    target_rows = set(row_to_points.keys())
    for row_idx in range(nrows):
        line = f.readline()
        if not line:
            break
        if row_idx not in target_rows:
            continue
        vals = line.strip().split()
        for pkey, col in row_to_points[row_idx]:
            if col >= len(vals):
                continue
            try:
                raw = float(vals[col])
            except Exception:
                continue
            if raw <= nodata:
                out[pkey] = None
            else:
                out[pkey] = float(raw) * RADOLAN_SCALE
    return out


def _fetch_radolan_series(points: list[tuple[float, float]], start_iso: str, end_iso: str) -> dict[str, list[dict]]:
    start = _parse_iso(start_iso)
    end = _parse_iso(end_iso)
    if end < start:
        raise RuntimeError("Radar-Zeitfenster ungueltig.")
    hours = int((end - start).total_seconds() // 3600) + 1
    if hours > RADAR_MAX_HOURS:
        raise RuntimeError(f"Radar-Zeitfenster zu gross ({hours}h). Limit: {RADAR_MAX_HOURS}h.")

    point_xy = _transform_points(points)
    per_point: dict[str, list[dict]] = {_point_key(lat, lon): [] for lat, lon in points}
    hours_by_day = _build_hour_requests(start, end)
    months = _month_iter(start, end)

    for year, month in months:
        month_tar_path = _ensure_month_tar(year, month)
        with tarfile.open(month_tar_path, "r") as month_tar:
            for day_key, hours_for_day in hours_by_day.items():
                if not day_key.startswith(f"{year:04d}{month:02d}"):
                    continue
                day_member = f"RW-{day_key}.tar.gz"
                try:
                    day_bytes = month_tar.extractfile(day_member).read()
                except Exception:
                    continue
                with tarfile.open(fileobj=gzip.GzipFile(fileobj=io.BytesIO(day_bytes)), mode="r:") as day_tar:
                    for hh in sorted(set(hours_for_day)):
                        asc_name = f"RW_{day_key}-{hh:02d}50.asc"
                        try:
                            asc_bytes = day_tar.extractfile(asc_name).read()
                        except Exception:
                            continue
                        vals = _parse_asc_point_values(asc_bytes, point_xy)
                        ts = dt.datetime(
                            int(day_key[0:4]),
                            int(day_key[4:6]),
                            int(day_key[6:8]),
                            int(hh),
                            0,
                            0,
                            tzinfo=dt.timezone.utc,
                        )
                        t_iso = ts.isoformat().replace("+00:00", "Z")
                        for pkey in per_point.keys():
                            v = vals.get(pkey)
                            per_point[pkey].append({"t": t_iso, "precip_mm": float(v) if v is not None else 0.0})

    for pkey in per_point.keys():
        per_point[pkey].sort(key=lambda r: r.get("t", ""))
    return per_point


def _fetch_from_connector(points: list[tuple[float, float]], start_iso: str, end_iso: str) -> dict:
    if not RADAR_EVENTS_URL:
        return {"available": False, "reason": "RADAR_EVENTS_URL nicht gesetzt", "per_point": {}}
    per_point: dict[str, list[dict]] = {}
    ok_any = False
    last_err: str | None = None
    for lat, lon in points:
        pkey = _point_key(lat, lon)
        try:
            resp = requests.get(
                RADAR_EVENTS_URL,
                params={"lat": lat, "lon": lon, "start": start_iso, "end": end_iso},
                timeout=RADAR_TIMEOUT_S,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("events") if isinstance(data, dict) else data
            per_point[pkey] = [ev for ev in (items or []) if isinstance(ev, dict)]
            ok_any = True
        except Exception as exc:
            last_err = str(exc)
            per_point[pkey] = []
    return {
        "available": bool(ok_any),
        "reason": None if ok_any else (last_err or "Radar-Connector nicht erreichbar"),
        "per_point": per_point,
    }


def fetch_radar_events(
    points: list[tuple[float, float]],
    start_iso: str,
    end_iso: str,
) -> dict:
    """
    Radar event source with two modes:
    - connector: external RADAR_EVENTS_URL service
    - dwd_radolan: built-in DWD RADOLAN CDC reader (hourly ASC archives)
    """
    provider = RADAR_PROVIDER if RADAR_PROVIDER in ("connector", "dwd_radolan", "auto") else "dwd_radolan"
    if provider == "auto":
        provider = "connector" if RADAR_EVENTS_URL else "dwd_radolan"

    if provider == "connector":
        res = _fetch_from_connector(points, start_iso, end_iso)
        if res.get("available"):
            return res
        if not RADAR_EVENTS_URL:
            return res
        # fallback to built-in RADOLAN if connector fails

    try:
        per_series = _fetch_radolan_series(points, start_iso, end_iso)
        out: dict[str, list[dict]] = {}
        ok_any = False
        for lat, lon in points:
            pkey = _point_key(lat, lon)
            evs = _detect_events(per_series.get(pkey) or [], source="radar")
            out[pkey] = evs
            if evs:
                ok_any = True
        # available=True means radar source reachable, not necessarily that events exist.
        if per_series:
            return {"available": True, "reason": None, "per_point": out}
        return {"available": False, "reason": "Keine RADOLAN-Daten im Zeitraum gefunden.", "per_point": out}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "per_point": {}}
