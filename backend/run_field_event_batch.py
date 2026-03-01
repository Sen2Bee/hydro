from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

FIELDNAMES = [
    "field_id",
    "event_id",
    "event_start_iso",
    "event_end_iso",
    "event_source",
    "event_peak_iso",
    "event_severity",
    "event_neighbor_cell_id",
    "event_neighbor_distance_km",
    "analysis_type",
    "metric_type",
    "risk_score_mean",
    "risk_score_max",
    "event_probability_mean",
    "event_probability_p90",
    "event_probability_max",
    "event_detected_share_percent",
    "abag_index_mean",
    "abag_index_p90",
    "abag_index_max",
    "network_length_km",
    "aoi_area_km2",
    "model_version",
    "ml_model_key_used",
    "ml_severity_model_key_used",
    "abag_c_factor_raster_path",
    "nodata_only",
    "dem_valid_cell_share",
    "dem_nodata_cell_share",
    "status",
    "error",
]

_EVENT_FETCH_LOCK = threading.Lock()
_LAST_EVENT_FETCH_TS = 0.0


@dataclass
class FieldAOI:
    field_id: str
    polygon_latlon: list[list[float]]
    south: float
    west: float
    north: float
    east: float


@dataclass
class FieldEvent:
    event_id: str
    event_start_iso: str
    event_end_iso: str
    event_source: str = "csv"
    event_peak_iso: str | None = None
    event_severity: int | None = None
    event_neighbor_cell_id: str | None = None
    event_neighbor_distance_km: float | None = None


def _largest_ring_from_geom(geom: dict[str, Any]) -> list[list[float]] | None:
    gtype = (geom or {}).get("type")
    coords = (geom or {}).get("coordinates")
    if not gtype or not coords:
        return None

    if gtype == "Polygon":
        rings = coords
        if not rings:
            return None
        return rings[0]

    if gtype == "MultiPolygon":
        best = None
        best_n = -1
        for poly in coords:
            if not poly:
                continue
            ring = poly[0]
            n = len(ring)
            if n > best_n:
                best_n = n
                best = ring
        return best
    return None


def _bbox_from_ring_lonlat(ring: list[list[float]]) -> tuple[float, float, float, float]:
    lons = [float(p[0]) for p in ring]
    lats = [float(p[1]) for p in ring]
    return min(lats), min(lons), max(lats), max(lons)


def _ring_lonlat_to_latlon(ring: list[list[float]]) -> list[list[float]]:
    out = []
    for p in ring:
        if len(p) < 2:
            continue
        out.append([float(p[1]), float(p[0])])
    if len(out) >= 3 and out[0] != out[-1]:
        out.append(out[0])
    return out


def _field_id_from_props(props: dict[str, Any], fallback_idx: int) -> str:
    for k in ("field_id", "schlag_id", "flik", "id", "ID"):
        v = props.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return f"field_{fallback_idx:05d}"


def _load_fields_geojson(path: Path) -> list[FieldAOI]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    feats = obj.get("features") or []
    out: list[FieldAOI] = []
    for i, f in enumerate(feats, start=1):
        geom = f.get("geometry") or {}
        ring = _largest_ring_from_geom(geom)
        if not ring or len(ring) < 3:
            continue
        poly_latlon = _ring_lonlat_to_latlon(ring)
        if len(poly_latlon) < 4:
            continue
        south, west, north, east = _bbox_from_ring_lonlat(ring)
        props = f.get("properties") or {}
        out.append(
            FieldAOI(
                field_id=_field_id_from_props(props, i),
                polygon_latlon=poly_latlon,
                south=south,
                west=west,
                north=north,
                east=east,
            )
        )
    if not out:
        raise RuntimeError("Keine gueltigen Polygon-Schlaege in GeoJSON gefunden.")
    return out


def _load_events_csv(path: Path) -> list[FieldEvent]:
    rows: list[FieldEvent] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for idx, r in enumerate(reader, start=1):
            event_id = str(r.get("event_id") or f"event_{idx:04d}").strip()
            start = str(r.get("event_start_iso") or "").strip()
            end = str(r.get("event_end_iso") or "").strip()
            if not start or not end:
                continue
            rows.append(
                FieldEvent(
                    event_id=event_id,
                    event_start_iso=start,
                    event_end_iso=end,
                    event_source="csv",
                )
            )
    if not rows:
        raise RuntimeError("Keine gueltigen Events in CSV gefunden (event_start_iso/event_end_iso).")
    return rows


def _field_centroid_latlon(aoi: FieldAOI) -> tuple[float, float]:
    # polygon is [lat,lon]
    pts = aoi.polygon_latlon or []
    if not pts:
        return ((aoi.south + aoi.north) / 2.0, (aoi.west + aoi.east) / 2.0)
    lats = [float(p[0]) for p in pts if len(p) >= 2]
    lons = [float(p[1]) for p in pts if len(p) >= 2]
    if not lats or not lons:
        return ((aoi.south + aoi.north) / 2.0, (aoi.west + aoi.east) / 2.0)
    return (sum(lats) / len(lats), sum(lons) / len(lons))


def _events_cache_path(cache_dir: Path, field_id: str, cache_key: str) -> Path:
    safe_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(field_id))
    return cache_dir / f"{safe_id}_{cache_key}.json"


def _events_cell_cache_path(cell_cache_dir: Path, cell_id: str, cache_key: str) -> Path:
    # IMPORTANT (Windows): ":" in filenames creates NTFS ADS and results in unusable cache files.
    safe_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(cell_id))
    return cell_cache_dir / f"cell_{safe_id}_{cache_key}.json"


def _weather_cell_id(lat: float, lon: float, cell_km: float) -> str:
    km = max(0.1, float(cell_km))
    dlat = km / 111.32
    cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
    dlon = km / (111.32 * cos_lat)
    i_lat = int(math.floor((lat + 90.0) / dlat))
    i_lon = int(math.floor((lon + 180.0) / dlon))
    return f"{i_lat}:{i_lon}"


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(max(0.0, min(1.0, a))))


def _weather_cell_center(cell_id: str, cell_km: float) -> tuple[float, float] | None:
    try:
        i_lat_s, i_lon_s = str(cell_id).split(":", 1)
        i_lat = int(i_lat_s)
        i_lon = int(i_lon_s)
    except Exception:
        return None
    km = max(0.1, float(cell_km))
    dlat = km / 111.32
    lat = -90.0 + (i_lat + 0.5) * dlat
    cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
    dlon = km / (111.32 * cos_lat)
    lon = -180.0 + (i_lon + 0.5) * dlon
    return (lat, lon)


def _load_events_from_cache_payload(data: dict[str, Any]) -> list[FieldEvent]:
    rows: list[FieldEvent] = []
    for it in (data.get("events") or []):
        if not isinstance(it, dict):
            continue
        s_iso, e_iso = _ensure_nonzero_event_window(
            str(it.get("event_start_iso") or ""),
            str(it.get("event_end_iso") or ""),
        )
        item = dict(it)
        item["event_start_iso"] = s_iso
        item["event_end_iso"] = e_iso
        rows.append(FieldEvent(**item))
    return rows


def _search_neighbor_cell_events(
    *,
    cell_cache_dir: Path,
    cache_key: str,
    lat: float,
    lon: float,
    source_cell_id: str,
    weather_cell_km: float,
    neighbor_max_km: float,
) -> tuple[list[FieldEvent], str | None, float | None]:
    max_km = max(0.0, float(neighbor_max_km))
    if max_km <= 0.0:
        return [], None, None
    try:
        src_i_lat, src_i_lon = [int(x) for x in str(source_cell_id).split(":", 1)]
    except Exception:
        return [], None, None
    step_limit = max(1, int(math.ceil(max_km / max(0.1, float(weather_cell_km)))))
    candidates: list[tuple[float, str]] = []
    for di in range(-step_limit, step_limit + 1):
        for dj in range(-step_limit, step_limit + 1):
            if di == 0 and dj == 0:
                continue
            cid = f"{src_i_lat + di}:{src_i_lon + dj}"
            center = _weather_cell_center(cid, weather_cell_km)
            if center is None:
                continue
            dist_km = _haversine_km(lat, lon, float(center[0]), float(center[1]))
            if dist_km <= max_km:
                candidates.append((dist_km, cid))
    candidates.sort(key=lambda x: x[0])
    for dist_km, cid in candidates:
        cpath = _events_cell_cache_path(cell_cache_dir, cid, cache_key)
        if not cpath.exists():
            continue
        try:
            data = json.loads(cpath.read_text(encoding="utf-8"))
            rows = _load_events_from_cache_payload(data)
            if rows:
                for ev in rows:
                    src = str(ev.event_source or "auto")
                    ev.event_source = f"{src}|neighbor_2km"
                    ev.event_neighbor_cell_id = cid
                    ev.event_neighbor_distance_km = round(float(dist_km), 3)
                return rows, cid, float(dist_km)
        except Exception:
            continue
    return [], None, None


def _events_cache_key(
    *,
    source: str,
    start: str | None,
    end: str | None,
    hours: int,
    days_ago: int,
    top_n: int,
    min_severity: int,
) -> str:
    raw = f"{source}|{start}|{end}|{hours}|{days_ago}|{top_n}|{min_severity}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _parse_iso_utc(value: str) -> dt.datetime | None:
    v = str(value or "").strip()
    if not v:
        return None
    try:
        if v.endswith("Z"):
            return dt.datetime.fromisoformat(v[:-1] + "+00:00")
        parsed = dt.datetime.fromisoformat(v)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except Exception:
        return None


def _ensure_nonzero_event_window(start_iso: str, end_iso: str) -> tuple[str, str]:
    start_dt = _parse_iso_utc(start_iso)
    end_dt = _parse_iso_utc(end_iso)
    if start_dt is None or end_dt is None:
        return start_iso, end_iso
    if end_dt <= start_dt:
        end_dt = start_dt + dt.timedelta(hours=1)
    start_out = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    end_out = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return start_out, end_out


def _split_time_window_utc(
    start_iso: str,
    end_iso: str,
    *,
    max_hours: int,
) -> list[tuple[str, str]]:
    start_dt = _parse_iso_utc(start_iso)
    end_dt = _parse_iso_utc(end_iso)
    if start_dt is None or end_dt is None:
        return [(start_iso, end_iso)]
    if end_dt <= start_dt:
        end_dt = start_dt + dt.timedelta(hours=1)
    step = dt.timedelta(hours=max(1, int(max_hours)))
    out: list[tuple[str, str]] = []
    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + step)
        s = cur.astimezone(dt.timezone.utc).date().isoformat()
        e = nxt.astimezone(dt.timezone.utc).date().isoformat()
        out.append((s, e))
        cur = nxt
    return out


def _parse_auto_events_payload(
    payload: dict[str, Any],
    *,
    top_n: int,
    min_severity: int,
) -> list[FieldEvent]:
    events = ((((payload or {}).get("events") or {}).get("mergedTop")) or [])
    out: list[FieldEvent] = []
    for idx, e in enumerate(events, start=1):
        if not isinstance(e, dict):
            continue
        try:
            sev = int(e.get("severity") or 0)
        except Exception:
            sev = 0
        if sev < int(min_severity):
            continue
        start_iso = str(e.get("start") or "").strip()
        end_iso = str(e.get("end") or "").strip()
        if not start_iso or not end_iso:
            continue
        start_iso, end_iso = _ensure_nonzero_event_window(start_iso, end_iso)
        peak_iso = str(e.get("peak_ts") or "").strip() or None
        tag = (peak_iso or start_iso).replace(":", "").replace("-", "").replace("T", "_").replace("Z", "")
        eid = f"auto_{tag}_{idx:02d}"
        out.append(
            FieldEvent(
                event_id=eid,
                event_start_iso=start_iso,
                event_end_iso=end_iso,
                event_source="auto",
                event_peak_iso=peak_iso,
                event_severity=sev,
            )
        )
        if len(out) >= int(top_n):
            break
    return out


def _load_or_fetch_auto_events_for_field(
    *,
    base_url: str,
    aoi: FieldAOI,
    source: str,
    start: str | None,
    end: str | None,
    hours: int,
    days_ago: int,
    top_n: int,
    min_severity: int,
    timeout_s: int,
    cache_dir: Path | None,
    cell_cache_dir: Path | None,
    weather_cell_km: float = 2.0,
    request_retries: int = 6,
    retry_backoff_initial_s: float = 5.0,
    retry_backoff_max_s: float = 90.0,
    min_interval_s: float = 1.5,
    neighbor_max_km: float = 2.0,
    cache_only: bool = False,
    use_cached_empty: bool = False,
) -> list[FieldEvent]:
    def _is_throttle_message(msg: str) -> bool:
        m = (msg or "").lower()
        if "too many requests" in m:
            return True
        if re.search(r"(?:^|\\D)429(?:\\D|$)", m):
            return True
        if "http 429" in m or "status 429" in m:
            return True
        return False

    def _throttle_requests() -> None:
        global _LAST_EVENT_FETCH_TS
        interval = max(0.0, float(min_interval_s))
        if interval <= 0.0:
            return
        with _EVENT_FETCH_LOCK:
            now = time.monotonic()
            wait_s = interval - (now - _LAST_EVENT_FETCH_TS)
            if wait_s > 0:
                time.sleep(wait_s)
            _LAST_EVENT_FETCH_TS = time.monotonic()

    cache_path: Path | None = None
    cache_key = _events_cache_key(
        source=source,
        start=start,
        end=end,
        hours=hours,
        days_ago=days_ago,
        top_n=top_n,
        min_severity=min_severity,
    )
    lat, lon = _field_centroid_latlon(aoi)
    cell_id = _weather_cell_id(lat, lon, weather_cell_km)
    cell_cache_path: Path | None = None
    if cell_cache_dir is not None:
        cell_cache_dir.mkdir(parents=True, exist_ok=True)
        cell_cache_path = _events_cell_cache_path(cell_cache_dir, cell_id, cache_key)
        if cell_cache_path.exists():
            try:
                data = json.loads(cell_cache_path.read_text(encoding="utf-8"))
                rows = _load_events_from_cache_payload(data)
                if rows:
                    return rows
                nrows, _, _ = _search_neighbor_cell_events(
                    cell_cache_dir=cell_cache_dir,
                    cache_key=cache_key,
                    lat=lat,
                    lon=lon,
                    source_cell_id=cell_id,
                    weather_cell_km=weather_cell_km,
                    neighbor_max_km=neighbor_max_km,
                )
                if nrows:
                    return nrows
                if bool(cache_only) or bool(use_cached_empty):
                    return []
            except Exception:
                pass

    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = _events_cache_path(cache_dir, aoi.field_id, cache_key)
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                rows = _load_events_from_cache_payload(data)
                if rows:
                    return rows
                if cell_cache_dir is not None:
                    nrows, _, _ = _search_neighbor_cell_events(
                        cell_cache_dir=cell_cache_dir,
                        cache_key=cache_key,
                        lat=lat,
                        lon=lon,
                        source_cell_id=cell_id,
                        weather_cell_km=weather_cell_km,
                        neighbor_max_km=neighbor_max_km,
                    )
                    if nrows:
                        return nrows
                if bool(cache_only) or bool(use_cached_empty):
                    return []
            except Exception:
                pass
            if bool(cache_only):
                return []

    if bool(cache_only):
        return []

    params: dict[str, Any] = {
        "points": f"{lat:.5f},{lon:.5f}",
        "agg": "hourly",
        "source": source,
    }
    if start and end:
        params["start"] = start
        params["end"] = end
    else:
        params["hours"] = int(hours)
        params["daysAgo"] = int(days_ago)

    url = f"{base_url.rstrip('/')}/abflussatlas/weather/events"
    retries = max(1, int(request_retries))
    backoff_s = max(0.1, float(retry_backoff_initial_s))
    backoff_cap_s = max(backoff_s, float(retry_backoff_max_s))
    payload: dict[str, Any] | None = None
    last_err: Exception | None = None
    def _fetch_payload_once(window_start: str | None, window_end: str | None) -> dict[str, Any]:
        nonlocal backoff_s, last_err
        q = dict(params)
        if window_start and window_end:
            q["start"] = window_start
            q["end"] = window_end
            q.pop("hours", None)
            q.pop("daysAgo", None)
        for attempt in range(1, retries + 1):
            try:
                _throttle_requests()
                resp = requests.get(url, params=q, timeout=timeout_s)
                if int(resp.status_code) == 429:
                    raise RuntimeError("auto-events provider throttled: HTTP 429")
                resp.raise_for_status()
                pl = resp.json() if resp.content else {}

                # Guard against silent upstream throttling:
                # Some providers return HTTP 200 plus a note containing "429 Too Many Requests".
                meta = pl.get("meta") or {}
                notes = meta.get("notes") or []
                if isinstance(notes, list):
                    note_txt = " | ".join(str(x) for x in notes)
                else:
                    note_txt = str(notes)
                if _is_throttle_message(note_txt):
                    raise RuntimeError(f"auto-events provider throttled: {note_txt}")
                return pl
            except Exception as exc:
                last_err = exc
                msg = str(exc)
                retryable = _is_throttle_message(msg) or any(
                    tok in msg.lower() for tok in ("timeout", "connection", "http", "gateway")
                )
                if (attempt >= retries) or (not retryable):
                    break
                sleep_s = min(backoff_s, backoff_cap_s)
                print(
                    f"  [auto-events retry {attempt}/{retries}] "
                    f"sleep={sleep_s:.1f}s reason={msg}"
                )
                time.sleep(sleep_s)
                backoff_s = min(backoff_s * 2.0, backoff_cap_s)
        raise RuntimeError(str(last_err) if last_err else "auto-events fetch failed")

    # Radar adapter exposes max 4320h per request. Split long windows automatically.
    source_l = str(source or "").strip().lower()
    if source_l in ("radar", "hybrid_radar") and start and end:
        slices = _split_time_window_utc(start, end, max_hours=4320)
        merged_top: list[dict[str, Any]] = []
        last_meta: dict[str, Any] = {}
        for s0, e0 in slices:
            pl = _fetch_payload_once(s0, e0)
            last_meta = pl.get("meta") or {}
            mt = (((pl.get("events") or {}).get("mergedTop")) or [])
            merged_top.extend([e for e in mt if isinstance(e, dict)])
        # De-duplicate by time window + source tag.
        dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
        for ev in merged_top:
            key = (
                str(ev.get("start") or ""),
                str(ev.get("end") or ""),
                str(ev.get("source") or ""),
            )
            old = dedup.get(key)
            if old is None:
                dedup[key] = ev
            else:
                sev_old = int(old.get("severity") or 0)
                sev_new = int(ev.get("severity") or 0)
                if sev_new > sev_old:
                    dedup[key] = ev
        payload = {
            "meta": last_meta,
            "events": {"mergedTop": list(dedup.values())},
        }
    else:
        payload = _fetch_payload_once(start if (start and end) else None, end if (start and end) else None)

    if payload is None:
        raise RuntimeError(str(last_err) if last_err else "auto-events fetch failed")

    rows = _parse_auto_events_payload(payload, top_n=top_n, min_severity=min_severity)
    if (not rows) and (cell_cache_dir is not None):
        nrows, _, _ = _search_neighbor_cell_events(
            cell_cache_dir=cell_cache_dir,
            cache_key=cache_key,
            lat=lat,
            lon=lon,
            source_cell_id=cell_id,
            weather_cell_km=weather_cell_km,
            neighbor_max_km=neighbor_max_km,
        )
        if nrows:
            rows = nrows

    if cache_path is not None:
        try:
            cache_path.write_text(
                json.dumps(
                    {
                        "field_id": aoi.field_id,
                        "params": params,
                        "events": [e.__dict__ for e in rows],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass
    if cell_cache_path is not None:
        try:
            cell_cache_path.write_text(
                json.dumps(
                    {
                        "cell_id": cell_id,
                        "weather_cell_km": float(weather_cell_km),
                        "params": params,
                        "events": [e.__dict__ for e in rows],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass
    return rows


def _call_analyze_bbox(
    *,
    base_url: str,
    aoi: FieldAOI,
    analysis_type: str,
    provider: str,
    dem_source: str,
    threshold: int,
    event_start_iso: str | None = None,
    event_end_iso: str | None = None,
    abag_p_factor: float | None = None,
    ml_model_key: str | None = None,
    ml_severity_model_key: str | None = None,
    ml_threshold: float | None = None,
    timeout_s: int = 1200,
    request_retries: int = 3,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "analysis_type": analysis_type,
        "provider": provider,
        "dem_source": dem_source,
        "threshold": int(threshold),
    }
    if event_start_iso:
        params["event_start_iso"] = event_start_iso
    if event_end_iso:
        params["event_end_iso"] = event_end_iso
    if abag_p_factor is not None:
        params["abag_p_factor"] = float(abag_p_factor)
    if ml_model_key:
        params["ml_model_key"] = ml_model_key
    if ml_severity_model_key:
        params["ml_severity_model_key"] = ml_severity_model_key
    if ml_threshold is not None:
        params["ml_threshold"] = float(ml_threshold)

    body = {
        "south": aoi.south,
        "west": aoi.west,
        "north": aoi.north,
        "east": aoi.east,
        "polygon": aoi.polygon_latlon,
    }
    url = f"{base_url.rstrip('/')}/analyze-bbox"
    last_err: Exception | None = None
    for attempt in range(1, max(1, int(request_retries)) + 1):
        try:
            with requests.post(url, params=params, json=body, stream=True, timeout=timeout_s) as resp:
                resp.raise_for_status()
                result = None
                for raw in resp.iter_lines(decode_unicode=True):
                    if not raw:
                        continue
                    msg = json.loads(raw)
                    mtype = msg.get("type")
                    if mtype == "error":
                        raise RuntimeError(str(msg.get("detail") or "Unbekannter Stream-Fehler"))
                    if mtype == "result":
                        result = msg.get("data")
                if result is None:
                    raise RuntimeError("Keine Result-Nachricht vom Backend erhalten.")
                return result
        except Exception as exc:
            last_err = exc
            if attempt >= max(1, int(request_retries)):
                break
            print(f"  [retry {attempt}/{request_retries}] {exc}")
    raise RuntimeError(str(last_err) if last_err else "Request fehlgeschlagen")


def _extract_metrics(result: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    analysis = (result or {}).get("analysis") or {}
    metrics = analysis.get("metrics") or {}
    assumptions = analysis.get("assumptions") or {}
    return metrics, assumptions


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _extract_diagnostics(metrics: dict[str, Any]) -> tuple[bool, float | None, float | None]:
    nodata_only = _coerce_bool(metrics.get("nodata_only"))
    try:
        valid_share = float(metrics.get("dem_valid_cell_share"))
    except Exception:
        valid_share = None
    try:
        nodata_share = float(metrics.get("dem_nodata_cell_share"))
    except Exception:
        nodata_share = None
    return nodata_only, valid_share, nodata_share


def _write_checkpoint_csv(out_csv: Path, rows_out: list[dict[str, Any]]) -> None:
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        if rows_out:
            w.writerows(rows_out)


def run(args: argparse.Namespace) -> int:
    fields = _load_fields_geojson(Path(args.fields_geojson).resolve())
    events_source = str(args.events_source).strip().lower()
    if events_source not in ("csv", "auto"):
        raise RuntimeError(f"Unbekannte events_source '{args.events_source}' (erlaubt: csv|auto).")
    events: list[FieldEvent] = []
    if events_source == "csv":
        if not args.events_csv:
            raise RuntimeError("--events-csv ist erforderlich bei events_source=csv.")
        events = _load_events_csv(Path(args.events_csv).resolve())

    out_csv = Path(args.out_csv).resolve()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_meta = out_csv.with_suffix(".meta.json")
    checkpoint_every = max(1, int(args.checkpoint_every))
    auto_events_cache_dir = (
        Path(args.events_auto_cache_dir).resolve()
        if str(args.events_auto_cache_dir or "").strip()
        else None
    )
    auto_events_cell_cache_dir = (
        Path(args.events_auto_cell_cache_dir).resolve()
        if str(args.events_auto_cell_cache_dir or "").strip()
        else None
    )

    modes = [m.strip() for m in str(args.analysis_modes).split(",") if m.strip()]
    if not modes:
        modes = ["erosion_events_ml", "abag"]

    rows_out: list[dict[str, Any]] = []
    _write_checkpoint_csv(out_csv, rows_out)
    n_total = (len(fields) * len(events) * len(modes)) if events_source == "csv" else 0
    n_done = 0
    since_checkpoint = 0
    started = dt.datetime.now(tz=dt.timezone.utc)

    for fld in fields:
        field_events: list[FieldEvent]
        if events_source == "csv":
            field_events = events
        else:
            try:
                field_events = _load_or_fetch_auto_events_for_field(
                    base_url=args.api_base_url,
                    aoi=fld,
                    source=args.events_auto_source,
                    start=args.events_auto_start,
                    end=args.events_auto_end,
                    hours=int(args.events_auto_hours),
                    days_ago=int(args.events_auto_days_ago),
                    top_n=int(args.events_auto_top_n),
                    min_severity=int(args.events_auto_min_severity),
                    timeout_s=int(args.timeout_s),
                    cache_dir=auto_events_cache_dir,
                    cell_cache_dir=auto_events_cell_cache_dir,
                    weather_cell_km=float(args.events_auto_weather_cell_km),
                    request_retries=int(args.events_auto_request_retries),
                    retry_backoff_initial_s=float(args.events_auto_retry_backoff_initial_s),
                    retry_backoff_max_s=float(args.events_auto_retry_backoff_max_s),
                    min_interval_s=float(args.events_auto_min_interval_s),
                    neighbor_max_km=float(args.events_auto_neighbor_max_km),
                    cache_only=bool(args.events_auto_cache_only),
                    use_cached_empty=bool(args.events_auto_use_cached_empty),
                )
            except Exception as exc:
                if not args.continue_on_error:
                    raise
                print(f"[field={fld.field_id}] auto-events fetch failed: {exc}")
                field_events = []
        if not field_events:
            print(f"[field={fld.field_id}] no events ({events_source}), skip field.")
            continue

        for ev in field_events:
            for mode in modes:
                n_done += 1
                pct = (float(n_done) / float(n_total) * 100.0) if n_total > 0 else 0.0
                elapsed_s = max(0.001, (dt.datetime.now(tz=dt.timezone.utc) - started).total_seconds())
                rate = float(n_done) / float(elapsed_s)
                eta_s = int(max(0.0, (float(n_total - n_done) / rate))) if (rate > 0 and n_total > 0) else -1
                eta_txt = f"{eta_s//3600:02d}:{(eta_s%3600)//60:02d}:{eta_s%60:02d}" if eta_s >= 0 else "--:--:--"
                if n_total > 0:
                    print(
                        f"[{n_done}/{n_total} | {pct:5.1f}% | ETA {eta_txt}] "
                        f"field={fld.field_id} event={ev.event_id} mode={mode}"
                    )
                else:
                    print(
                        f"[{n_done} | ETA {eta_txt}] "
                        f"field={fld.field_id} event={ev.event_id} mode={mode}"
                    )
                if mode == "erosion_events_ml":
                    try:
                        res = _call_analyze_bbox(
                            base_url=args.api_base_url,
                            aoi=fld,
                            analysis_type="erosion_events_ml",
                            provider=args.provider,
                            dem_source=args.dem_source,
                            threshold=args.threshold,
                            event_start_iso=ev.event_start_iso,
                            event_end_iso=ev.event_end_iso,
                            ml_model_key=args.ml_model_key,
                            ml_severity_model_key=args.ml_severity_model_key,
                            ml_threshold=args.ml_threshold,
                            timeout_s=args.timeout_s,
                            request_retries=args.request_retries,
                        )
                    except Exception as exc:
                        if not args.continue_on_error:
                            raise
                        rows_out.append(
                            {
                                "field_id": fld.field_id,
                                "event_id": ev.event_id,
                                "event_start_iso": ev.event_start_iso,
                                "event_end_iso": ev.event_end_iso,
                                "event_source": ev.event_source,
                                "event_peak_iso": ev.event_peak_iso,
                                "event_severity": ev.event_severity,
                                "event_neighbor_cell_id": ev.event_neighbor_cell_id,
                                "event_neighbor_distance_km": ev.event_neighbor_distance_km,
                                "analysis_type": mode,
                                "metric_type": None,
                                "risk_score_mean": None,
                                "risk_score_max": None,
                                "event_probability_mean": None,
                                "event_probability_p90": None,
                                "event_probability_max": None,
                                "event_detected_share_percent": None,
                                "abag_index_mean": None,
                                "abag_index_p90": None,
                                "abag_index_max": None,
                                "network_length_km": None,
                                "aoi_area_km2": None,
                                "model_version": None,
                                "ml_model_key_used": None,
                                "ml_severity_model_key_used": None,
                                "abag_c_factor_raster_path": None,
                                "nodata_only": None,
                                "dem_valid_cell_share": None,
                                "dem_nodata_cell_share": None,
                                "status": "error",
                                "error": str(exc),
                            }
                        )
                        print(f"  [skip] {exc}")
                        since_checkpoint += 1
                        if since_checkpoint >= checkpoint_every:
                            _write_checkpoint_csv(out_csv, rows_out)
                            print(f"  [checkpoint] {len(rows_out)} rows -> {out_csv}")
                            since_checkpoint = 0
                        continue
                elif mode == "abag":
                    try:
                        res = _call_analyze_bbox(
                            base_url=args.api_base_url,
                            aoi=fld,
                            analysis_type="abag",
                            provider=args.provider,
                            dem_source=args.dem_source,
                            threshold=args.threshold,
                            abag_p_factor=args.abag_p_factor,
                            timeout_s=args.timeout_s,
                            request_retries=args.request_retries,
                        )
                    except Exception as exc:
                        if not args.continue_on_error:
                            raise
                        rows_out.append(
                            {
                                "field_id": fld.field_id,
                                "event_id": ev.event_id,
                                "event_start_iso": ev.event_start_iso,
                                "event_end_iso": ev.event_end_iso,
                                "event_source": ev.event_source,
                                "event_peak_iso": ev.event_peak_iso,
                                "event_severity": ev.event_severity,
                                "event_neighbor_cell_id": ev.event_neighbor_cell_id,
                                "event_neighbor_distance_km": ev.event_neighbor_distance_km,
                                "analysis_type": mode,
                                "metric_type": None,
                                "risk_score_mean": None,
                                "risk_score_max": None,
                                "event_probability_mean": None,
                                "event_probability_p90": None,
                                "event_probability_max": None,
                                "event_detected_share_percent": None,
                                "abag_index_mean": None,
                                "abag_index_p90": None,
                                "abag_index_max": None,
                                "network_length_km": None,
                                "aoi_area_km2": None,
                                "model_version": None,
                                "ml_model_key_used": None,
                                "ml_severity_model_key_used": None,
                                "abag_c_factor_raster_path": None,
                                "nodata_only": None,
                                "dem_valid_cell_share": None,
                                "dem_nodata_cell_share": None,
                                "status": "error",
                                "error": str(exc),
                            }
                        )
                        print(f"  [skip] {exc}")
                        since_checkpoint += 1
                        if since_checkpoint >= checkpoint_every:
                            _write_checkpoint_csv(out_csv, rows_out)
                            print(f"  [checkpoint] {len(rows_out)} rows -> {out_csv}")
                            since_checkpoint = 0
                        continue
                else:
                    raise RuntimeError(f"Unbekannter analysis mode: {mode}")

                metrics, assumptions = _extract_metrics(res)
                nodata_only, dem_valid_cell_share, dem_nodata_cell_share = _extract_diagnostics(metrics)
                rows_out.append(
                    {
                        "field_id": fld.field_id,
                        "event_id": ev.event_id,
                        "event_start_iso": ev.event_start_iso,
                        "event_end_iso": ev.event_end_iso,
                        "event_source": ev.event_source,
                        "event_peak_iso": ev.event_peak_iso,
                        "event_severity": ev.event_severity,
                        "event_neighbor_cell_id": ev.event_neighbor_cell_id,
                        "event_neighbor_distance_km": ev.event_neighbor_distance_km,
                        "analysis_type": mode,
                        "metric_type": metrics.get("metric_type"),
                        "risk_score_mean": metrics.get("risk_score_mean"),
                        "risk_score_max": metrics.get("risk_score_max"),
                        "event_probability_mean": metrics.get("event_probability_mean"),
                        "event_probability_p90": metrics.get("event_probability_p90"),
                        "event_probability_max": metrics.get("event_probability_max"),
                        "event_detected_share_percent": metrics.get("event_detected_share_percent"),
                        "abag_index_mean": metrics.get("abag_index_mean"),
                        "abag_index_p90": metrics.get("abag_index_p90"),
                        "abag_index_max": metrics.get("abag_index_max"),
                        "network_length_km": metrics.get("network_length_km"),
                        "aoi_area_km2": metrics.get("aoi_area_km2"),
                        "model_version": metrics.get("model_version"),
                        "ml_model_key_used": assumptions.get("ml_model_key"),
                        "ml_severity_model_key_used": assumptions.get("ml_severity_model_key"),
                        "abag_c_factor_raster_path": assumptions.get("abag_c_factor_raster_path"),
                        "nodata_only": nodata_only,
                        "dem_valid_cell_share": dem_valid_cell_share,
                        "dem_nodata_cell_share": dem_nodata_cell_share,
                        "status": "ok",
                        "error": None,
                    }
                )
                since_checkpoint += 1
                if since_checkpoint >= checkpoint_every:
                    _write_checkpoint_csv(out_csv, rows_out)
                    print(f"  [checkpoint] {len(rows_out)} rows -> {out_csv}")
                    since_checkpoint = 0

    if not rows_out:
        print("[WARN] Keine Ergebniszeilen erzeugt.")
        _write_checkpoint_csv(out_csv, rows_out)
        meta = {
            "api_base_url": args.api_base_url,
            "fields_geojson": str(Path(args.fields_geojson).resolve()),
            "events_source": events_source,
            "events_csv": (str(Path(args.events_csv).resolve()) if args.events_csv else None),
            "events_auto_source": args.events_auto_source,
            "events_auto_start": args.events_auto_start,
            "events_auto_end": args.events_auto_end,
            "events_auto_hours": int(args.events_auto_hours),
            "events_auto_days_ago": int(args.events_auto_days_ago),
            "events_auto_top_n": int(args.events_auto_top_n),
            "events_auto_min_severity": int(args.events_auto_min_severity),
            "events_auto_cache_dir": (str(auto_events_cache_dir) if auto_events_cache_dir else None),
            "events_auto_cell_cache_dir": (str(auto_events_cell_cache_dir) if auto_events_cell_cache_dir else None),
            "events_auto_weather_cell_km": float(args.events_auto_weather_cell_km),
            "events_auto_request_retries": int(args.events_auto_request_retries),
            "events_auto_retry_backoff_initial_s": float(args.events_auto_retry_backoff_initial_s),
            "events_auto_retry_backoff_max_s": float(args.events_auto_retry_backoff_max_s),
            "events_auto_min_interval_s": float(args.events_auto_min_interval_s),
            "events_auto_neighbor_max_km": float(args.events_auto_neighbor_max_km),
            "events_auto_cache_only": bool(args.events_auto_cache_only),
            "events_auto_use_cached_empty": bool(args.events_auto_use_cached_empty),
            "analysis_modes": modes,
            "provider": args.provider,
            "dem_source": args.dem_source,
            "threshold": int(args.threshold),
            "abag_p_factor": float(args.abag_p_factor),
            "ml_model_key": args.ml_model_key,
            "ml_severity_model_key": args.ml_severity_model_key,
            "ml_threshold": float(args.ml_threshold),
            "row_count": 0,
            "out_csv": str(out_csv),
        }
        out_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(f"[OK] CSV:  {out_csv}")
        print(f"[OK] META: {out_meta}")
        return 0

    _write_checkpoint_csv(out_csv, rows_out)

    meta = {
        "api_base_url": args.api_base_url,
        "fields_geojson": str(Path(args.fields_geojson).resolve()),
        "events_source": events_source,
        "events_csv": (str(Path(args.events_csv).resolve()) if args.events_csv else None),
        "events_auto_source": args.events_auto_source,
        "events_auto_start": args.events_auto_start,
        "events_auto_end": args.events_auto_end,
        "events_auto_hours": int(args.events_auto_hours),
        "events_auto_days_ago": int(args.events_auto_days_ago),
        "events_auto_top_n": int(args.events_auto_top_n),
        "events_auto_min_severity": int(args.events_auto_min_severity),
        "events_auto_cache_dir": (str(auto_events_cache_dir) if auto_events_cache_dir else None),
        "events_auto_cell_cache_dir": (str(auto_events_cell_cache_dir) if auto_events_cell_cache_dir else None),
        "events_auto_weather_cell_km": float(args.events_auto_weather_cell_km),
        "events_auto_request_retries": int(args.events_auto_request_retries),
        "events_auto_retry_backoff_initial_s": float(args.events_auto_retry_backoff_initial_s),
        "events_auto_retry_backoff_max_s": float(args.events_auto_retry_backoff_max_s),
        "events_auto_min_interval_s": float(args.events_auto_min_interval_s),
        "events_auto_neighbor_max_km": float(args.events_auto_neighbor_max_km),
        "events_auto_cache_only": bool(args.events_auto_cache_only),
        "events_auto_use_cached_empty": bool(args.events_auto_use_cached_empty),
        "analysis_modes": modes,
        "provider": args.provider,
        "dem_source": args.dem_source,
        "threshold": int(args.threshold),
        "abag_p_factor": float(args.abag_p_factor),
        "ml_model_key": args.ml_model_key,
        "ml_severity_model_key": args.ml_severity_model_key,
        "ml_threshold": float(args.ml_threshold),
        "row_count": len(rows_out),
        "out_csv": str(out_csv),
    }
    out_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[OK] CSV:  {out_csv}")
    print(f"[OK] META: {out_meta}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Batch export for field x event analyses via /analyze-bbox.")
    p.add_argument("--fields-geojson", required=True, help="GeoJSON with Polygon/MultiPolygon fields.")
    p.add_argument("--events-source", default="csv", help="csv|auto")
    p.add_argument("--events-csv", required=False, help="CSV with event_id,event_start_iso,event_end_iso.")
    p.add_argument("--events-auto-source", default="hybrid_radar", help="icon2d|dwd|radar|hybrid|hybrid_radar")
    p.add_argument("--events-auto-start", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--events-auto-end", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--events-auto-hours", type=int, default=24 * 120)
    p.add_argument("--events-auto-days-ago", type=int, default=0)
    p.add_argument("--events-auto-top-n", type=int, default=2)
    p.add_argument("--events-auto-min-severity", type=int, default=1)
    p.add_argument("--events-auto-cache-dir", default=str(Path("paper") / "cache" / "auto_events"))
    p.add_argument("--events-auto-cell-cache-dir", default="", help="Optional shared weather-cell cache dir.")
    p.add_argument("--events-auto-weather-cell-km", type=float, default=2.0, help="Weather cell size in km for shared cell-cache.")
    p.add_argument("--events-auto-request-retries", type=int, default=6, help="Retries for auto-event endpoint requests.")
    p.add_argument("--events-auto-retry-backoff-initial-s", type=float, default=5.0, help="Initial backoff after retryable auto-event fetch errors.")
    p.add_argument("--events-auto-retry-backoff-max-s", type=float, default=90.0, help="Maximum backoff for auto-event fetch retries.")
    p.add_argument("--events-auto-min-interval-s", type=float, default=1.5, help="Minimum delay between auto-event requests (seconds).")
    p.add_argument("--events-auto-neighbor-max-km", type=float, default=2.0, help="If no events exist at point, use nearest cached weather cell within radius (km).")
    p.add_argument("--events-auto-cache-only", action=argparse.BooleanOptionalAction, default=False, help="Use only local auto-event cache, do not call weather/events.")
    p.add_argument("--events-auto-use-cached-empty", action=argparse.BooleanOptionalAction, default=False, help="Treat cached empty event lists as valid and skip remote fetch.")
    p.add_argument("--out-csv", default=str(Path("paper") / "exports" / "field_event_results.csv"))
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--analysis-modes", default="erosion_events_ml,abag", help="Comma list, e.g. erosion_events_ml,abag")
    p.add_argument("--provider", default="auto")
    p.add_argument("--dem-source", default="wcs")
    p.add_argument("--threshold", type=int, default=200)
    p.add_argument("--abag-p-factor", type=float, default=1.0)
    p.add_argument("--ml-model-key", default="event-ml-rf-v1")
    p.add_argument("--ml-severity-model-key", default="event-ml-rf-severity-v1")
    p.add_argument("--ml-threshold", type=float, default=0.50)
    p.add_argument("--timeout-s", type=int, default=1200)
    p.add_argument("--request-retries", type=int, default=3)
    p.add_argument("--checkpoint-every", type=int, default=20, help="Write partial CSV every N rows.")
    p.add_argument("--continue-on-error", action=argparse.BooleanOptionalAction, default=True)
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
