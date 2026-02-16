"""
DWD CDC hourly precipitation (RR) helper.

MVP goal:
  - pick nearest DWD station to an AOI (bbox centroid)
  - compute simple precipitation metrics for a date range (default: last 3 years)

Notes:
  - We use the "historical" station zips. For many stations these are updated up to today.
  - Parsing is intentionally lightweight (no pandas dependency).
"""

from __future__ import annotations

import datetime as _dt
import io
import math
import os
import re
import threading
import zipfile
from dataclasses import dataclass
from typing import Iterable

import requests


DWD_CDC_BASE = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation"
STATION_LIST_URL = f"{DWD_CDC_BASE}/historical/RR_Stundenwerte_Beschreibung_Stationen.txt"
HISTORICAL_DIR = f"{DWD_CDC_BASE}/historical"

DEFAULT_WEATHER_CACHE_DIR = os.path.join(os.path.dirname(__file__), ".weather_cache")
_FETCH_LOCK = threading.Lock()
STATION_LIST_MAX_AGE_S = 24 * 3600


@dataclass(frozen=True)
class DwdStation:
    station_id: str  # zero-padded
    from_date: str  # YYYYMMDD
    to_date: str  # YYYYMMDD
    height_m: int
    lat: float
    lon: float
    name: str
    state: str

    @property
    def zip_url(self) -> str:
        return f"{HISTORICAL_DIR}/stundenwerte_RR_{self.station_id}_{self.from_date}_{self.to_date}_hist.zip"


def _cache_dir() -> str:
    return os.getenv("WEATHER_CACHE_DIR", DEFAULT_WEATHER_CACHE_DIR)


def _cache_path(name: str) -> str:
    return os.path.join(_cache_dir(), name)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p = math.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin(dlon / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def _parse_station_list(text: str) -> list[DwdStation]:
    lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]
    out: list[DwdStation] = []
    # Header lines begin with "Stations_id"; data lines begin with digits.
    for ln in lines:
        if not ln[:1].isdigit():
            continue
        # Fixed-width file with multiple spaces between columns; station name contains spaces.
        # Split by 2+ spaces to preserve station name as one field.
        parts = re.split(r"\s{2,}", ln.strip())
        if len(parts) < 9:
            continue
        try:
            station_id = parts[0].zfill(5)
            from_date = parts[1]
            to_date = parts[2]
            height_m = int(parts[3])
            lat = float(parts[4])
            lon = float(parts[5])
            name = parts[6].strip()
            state = parts[7].strip()
            out.append(
                DwdStation(
                    station_id=station_id,
                    from_date=from_date,
                    to_date=to_date,
                    height_m=height_m,
                    lat=lat,
                    lon=lon,
                    name=name,
                    state=state,
                )
            )
        except Exception:
            continue
    return out


def _is_cache_fresh(path: str, max_age_s: int | None) -> bool:
    if not os.path.exists(path):
        return False
    if not max_age_s or max_age_s <= 0:
        return True
    try:
        age_s = max(0.0, _dt.datetime.now().timestamp() - os.path.getmtime(path))
        return age_s <= float(max_age_s)
    except Exception:
        return False


def _download_text_cached(
    url: str,
    cache_name: str,
    timeout_s: int = 60,
    max_age_s: int | None = None,
) -> str:
    os.makedirs(_cache_dir(), exist_ok=True)
    path = _cache_path(cache_name)
    if _is_cache_fresh(path, max_age_s):
        with open(path, "rb") as f:
            raw = f.read()
        return raw.decode("latin-1", errors="replace")

    with _FETCH_LOCK:
        if _is_cache_fresh(path, max_age_s):
            with open(path, "rb") as f:
                raw = f.read()
            return raw.decode("latin-1", errors="replace")

        try:
            resp = requests.get(url, timeout=timeout_s)
            resp.raise_for_status()
            raw = resp.content
            with open(path, "wb") as f:
                f.write(raw)
            return raw.decode("latin-1", errors="replace")
        except Exception:
            # Fallback: if refresh fails, continue with stale local cache instead of hard fail.
            if os.path.exists(path):
                with open(path, "rb") as f:
                    raw = f.read()
                return raw.decode("latin-1", errors="replace")
            raise


def list_stations() -> list[DwdStation]:
    text = _download_text_cached(
        STATION_LIST_URL,
        "RR_Stundenwerte_Beschreibung_Stationen.txt",
        max_age_s=STATION_LIST_MAX_AGE_S,
    )
    return _parse_station_list(text)


def _covers_range(st: DwdStation, start: _dt.date, end: _dt.date) -> bool:
    try:
        s0 = _dt.datetime.strptime(st.from_date, "%Y%m%d").date()
        s1 = _dt.datetime.strptime(st.to_date, "%Y%m%d").date()
    except Exception:
        return False
    # Station files include hours; treat range as inclusive by day.
    return s0 <= start and s1 >= end


def find_nearest_station(
    lat: float, lon: float, start: _dt.date, end: _dt.date, preferred_state: str | None = None
) -> tuple[DwdStation, float]:
    stations = list_stations()
    candidates: list[tuple[float, DwdStation]] = []
    for st in stations:
        if not _covers_range(st, start, end):
            continue
        if preferred_state and st.state != preferred_state:
            continue
        d = _haversine_km(lat, lon, st.lat, st.lon)
        candidates.append((d, st))
    if not candidates and preferred_state:
        # fallback: ignore preferred state
        for st in stations:
            if not _covers_range(st, start, end):
                continue
            d = _haversine_km(lat, lon, st.lat, st.lon)
            candidates.append((d, st))
    if not candidates:
        # Robust fallback: use nearest station even if listed coverage doesn't fully include the window.
        # This avoids false negatives when station list metadata is stale but ZIP already has newer values.
        for st in stations:
            if preferred_state and st.state != preferred_state:
                continue
            d = _haversine_km(lat, lon, st.lat, st.lon)
            candidates.append((d, st))
        if not candidates and preferred_state:
            for st in stations:
                d = _haversine_km(lat, lon, st.lat, st.lon)
                candidates.append((d, st))
    if not candidates:
        raise RuntimeError("Keine DWD-Station gefunden.")
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1], float(candidates[0][0])


def _download_zip_cached(url: str, cache_name: str, timeout_s: int = 180) -> bytes:
    os.makedirs(_cache_dir(), exist_ok=True)
    path = _cache_path(cache_name)
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()

    with _FETCH_LOCK:
        if os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()
        with requests.get(url, stream=True, timeout=timeout_s) as resp:
            resp.raise_for_status()
            buf = io.BytesIO()
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    buf.write(chunk)
            raw = buf.getvalue()
        with open(path, "wb") as f:
            f.write(raw)
        return raw


def _iter_hourly_rr_mm(zf: zipfile.ZipFile, station_id: str) -> Iterable[tuple[_dt.datetime, float]]:
    # Example file: produkt_rr_stunde_19950901_20110401_00003.txt
    product = None
    for name in zf.namelist():
        low = name.lower()
        if low.endswith(".txt") and "produkt_rr_stunde" in low and station_id in name:
            product = name
            break
    if not product:
        # fallback: first product_rr_stunde file
        for name in zf.namelist():
            low = name.lower()
            if low.endswith(".txt") and "produkt_rr_stunde" in low:
                product = name
                break
    if not product:
        raise RuntimeError("DWD ZIP: produkt_rr_stunde Datei nicht gefunden.")

    raw = zf.read(product)
    text = raw.decode("latin-1", errors="replace")
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if ln.startswith("STATIONS_ID;"):
            continue
        # Format: STATIONS_ID;MESS_DATUM;QN_8;R1;RS_IND;WRTR;eor
        parts = [p.strip() for p in ln.split(";")]
        if len(parts) < 4:
            continue
        try:
            ts = _dt.datetime.strptime(parts[1], "%Y%m%d%H")
            r1 = float(parts[3])
            if r1 <= -900:
                continue
            yield ts, r1
        except Exception:
            continue


def _rolling_max(values: list[float], window: int) -> float | None:
    if not values or window <= 0 or len(values) < window:
        return None
    s = sum(values[:window])
    best = s
    for i in range(window, len(values)):
        s += values[i] - values[i - window]
        if s > best:
            best = s
    return float(best)


def compute_precip_metrics(
    station: DwdStation, start: _dt.date, end: _dt.date
) -> dict:
    zip_bytes = _download_zip_cached(
        station.zip_url,
        f"stundenwerte_RR_{station.station_id}_{station.from_date}_{station.to_date}_hist.zip",
    )
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))

    start_dt = _dt.datetime.combine(start, _dt.time(0, 0, 0))
    end_dt = _dt.datetime.combine(end, _dt.time(23, 0, 0))

    rows: list[tuple[_dt.datetime, float]] = []
    for ts, rr in _iter_hourly_rr_mm(zf, station.station_id):
        if ts < start_dt or ts > end_dt:
            continue
        rows.append((ts, rr))
    rows.sort(key=lambda x: x[0])

    if not rows:
        raise RuntimeError("Keine Niederschlagsdaten im Zeitraum gefunden.")

    # Build dense hourly series where possible; gaps break rolling windows.
    values_dense: list[float] = []
    times_dense: list[_dt.datetime] = []
    last_ts = None
    for ts, rr in rows:
        if last_ts is not None:
            delta_h = int((ts - last_ts).total_seconds() // 3600)
            if delta_h != 1:
                # gap: stop dense accumulation here
                values_dense.append(math.nan)
                times_dense.append(ts)
        values_dense.append(rr)
        times_dense.append(ts)
        last_ts = ts

    # For rolling windows, split into contiguous blocks (no NaNs)
    blocks: list[list[float]] = []
    cur: list[float] = []
    for v in values_dense:
        if not math.isfinite(v):
            if cur:
                blocks.append(cur)
                cur = []
            continue
        cur.append(v)
    if cur:
        blocks.append(cur)

    max_1h = max(rr for _, rr in rows)
    max_6h = None
    max_24h = None
    for b in blocks:
        r6 = _rolling_max(b, 6)
        r24 = _rolling_max(b, 24)
        if r6 is not None:
            max_6h = r6 if max_6h is None else max(max_6h, r6)
        if r24 is not None:
            max_24h = r24 if max_24h is None else max(max_24h, r24)

    total_mm = float(sum(rr for _, rr in rows))
    count_hours = len(rows)
    count_ge_10 = sum(1 for _, rr in rows if rr >= 10.0)
    count_ge_25 = sum(1 for _, rr in rows if rr >= 25.0)
    count_ge_40 = sum(1 for _, rr in rows if rr >= 40.0)

    top_events = sorted(rows, key=lambda x: x[1], reverse=True)[:8]
    top = [{"ts": ts.isoformat(timespec="minutes"), "mm_1h": float(mm)} for ts, mm in top_events]

    return {
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "count_hours": int(count_hours),
        "total_mm": round(total_mm, 2),
        "max_1h_mm": round(float(max_1h), 2),
        "max_6h_mm": None if max_6h is None else round(float(max_6h), 2),
        "max_24h_mm": None if max_24h is None else round(float(max_24h), 2),
        "count_hours_ge_10mm": int(count_ge_10),
        "count_hours_ge_25mm": int(count_ge_25),
        "count_hours_ge_40mm": int(count_ge_40),
        "top_hours": top,
    }


def default_last_years_range(years: int = 3) -> tuple[_dt.date, _dt.date]:
    today = _dt.date.today()
    start = _dt.date(today.year - years, today.month, today.day)
    return start, today


def load_hourly_series(
    station: DwdStation,
    start_iso_utc: str,
    end_iso_utc: str,
) -> list[dict]:
    """
    Return a compact hourly precipitation series for a station in [start,end] (UTC).

    Output: [{"t": "...Z", "precip_mm": float}, ...]
    """
    try:
        start_dt = _dt.datetime.fromisoformat(start_iso_utc.replace("Z", "+00:00")).astimezone(_dt.timezone.utc)
        end_dt = _dt.datetime.fromisoformat(end_iso_utc.replace("Z", "+00:00")).astimezone(_dt.timezone.utc)
    except Exception as exc:
        raise RuntimeError(f"Ungueltiges Zeitfenster: {exc}")

    zip_bytes = _download_zip_cached(
        station.zip_url,
        f"stundenwerte_RR_{station.station_id}_{station.from_date}_{station.to_date}_hist.zip",
    )
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))

    out: list[dict] = []
    for ts, rr in _iter_hourly_rr_mm(zf, station.station_id):
        ts_utc = ts.replace(tzinfo=_dt.timezone.utc)
        if ts_utc < start_dt or ts_utc > end_dt:
            continue
        out.append({"t": ts_utc.isoformat().replace("+00:00", "Z"), "precip_mm": float(rr)})
    return out
