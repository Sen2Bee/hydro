import json
import math
import os
import queue
import shutil
import tempfile
import threading

import datetime as dt

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from processing import analyze_dem, delineate_catchment_dem
from weather_dwd import compute_precip_metrics, default_last_years_range, find_nearest_station, load_hourly_series
from weather_window import compute_window_safe
from abflussatlas_weather import fetch_batch, parse_points
from weather_stats import build_weather_stats
from weather_radar import fetch_radar_events
from wcs_client import detect_provider, fetch_dem_from_wcs
from wms_utils import list_wms_layers
from geocode import geocode
from wcs_selftest import run_wcs_selftest
from st_public_dem import fetch_dem_from_st_public_download
from st_cog_dem import fetch_dem_from_st_cog_dir

app = FastAPI(title="Hydrowatch Berlin API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class BboxRequest(BaseModel):
    south: float
    west: float
    north: float
    east: float
    # Optional AOI polygon (lat,lon points) to clip displayed/evaluated results.
    polygon: list[list[float]] | None = None


class CatchmentPoint(BaseModel):
    lat: float
    lon: float


class CatchmentRequest(BboxRequest):
    point: CatchmentPoint


class WeatherRequest(BaseModel):
    south: float
    west: float
    north: float
    east: float
    start: str | None = None  # YYYY-MM-DD
    end: str | None = None  # YYYY-MM-DD


def _bbox_area_km2(south: float, west: float, north: float, east: float) -> float:
    d_lat = abs(float(north) - float(south))
    d_lon = abs(float(east) - float(west))
    lat_mid_rad = ((float(north) + float(south)) / 2.0) * (3.141592653589793 / 180.0)
    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * max(0.01, abs(math.cos(lat_mid_rad)))
    return float(d_lat * km_per_deg_lat * d_lon * km_per_deg_lon)


def _sample_points_from_bbox(
    south: float,
    west: float,
    north: float,
    east: float,
    *,
    mode: str,
    inset_frac: float = 0.10,
) -> list[tuple[float, float]]:
    s = float(south)
    w = float(west)
    n = float(north)
    e = float(east)
    lat_c = (s + n) / 2.0
    lon_c = (w + e) / 2.0
    if mode == "standard":
        return [(lat_c, lon_c)]

    lat_span = max(0.0, n - s)
    lon_span = max(0.0, e - w)
    lat_inset = min(lat_span * inset_frac, lat_span * 0.45)
    lon_inset = min(lon_span * inset_frac, lon_span * 0.45)
    ss = s + lat_inset
    nn = n - lat_inset
    ww = w + lon_inset
    ee = e - lon_inset
    return [
        (lat_c, lon_c),
        (ss, ww),
        (ss, ee),
        (nn, ww),
        (nn, ee),
    ]


def _median(vals: list[float]) -> float | None:
    xs = [float(v) for v in vals if v is not None]
    if not xs:
        return None
    xs.sort()
    mid = len(xs) // 2
    if len(xs) % 2:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0


def _minmax(vals: list[float]) -> tuple[float | None, float | None]:
    xs = [float(v) for v in vals if v is not None]
    if not xs:
        return None, None
    return min(xs), max(xs)


def _today_utc_date() -> dt.date:
    return dt.datetime.now(tz=dt.timezone.utc).date()


def _compute_weather_window(
    *,
    start: str | None,
    end: str | None,
    hours: int,
    days_ago: int,
) -> tuple[str, str, bool]:
    """
    Returns (startISO, endISO, end_clamped_to_today).
    If start/end are provided (YYYY-MM-DD), they take precedence.
    """
    if not start and not end:
        s, e = compute_window_safe(hours=hours, days_ago=days_ago)
        return s, e, False

    today = _today_utc_date()
    end_clamped = False
    try:
        end_d = dt.date.fromisoformat(end) if end else today
    except Exception:
        raise HTTPException(status_code=400, detail="Ungueltiges End-Datum. Erwartet: YYYY-MM-DD")
    if end_d > today:
        end_d = today
        end_clamped = True

    try:
        start_d = dt.date.fromisoformat(start) if start else (end_d - dt.timedelta(days=13))
    except Exception:
        raise HTTPException(status_code=400, detail="Ungueltiges Start-Datum. Erwartet: YYYY-MM-DD")

    if end_d < start_d:
        raise HTTPException(status_code=400, detail="Ende muss nach Start liegen.")

    start_iso = dt.datetime.combine(start_d, dt.time(0, 0, 0, tzinfo=dt.timezone.utc)).isoformat().replace("+00:00", "Z")
    end_iso = dt.datetime.combine(end_d, dt.time(23, 0, 0, tzinfo=dt.timezone.utc)).isoformat().replace("+00:00", "Z")
    return start_iso, end_iso, end_clamped


def _event_level(max_1h: float, max_6h: float) -> tuple[str, int]:
    # DWD-style thresholds (screening-oriented, simplified):
    # Starkregen: >=15 mm/1h or >=20 mm/6h
    # Unwetter: >25 mm/1h or >35 mm/6h
    # Extrem: >40 mm/1h or >60 mm/6h
    if max_1h > 40.0 or max_6h > 60.0:
        return "extrem", 3
    if max_1h > 25.0 or max_6h > 35.0:
        return "unwetter", 2
    if max_1h >= 15.0 or max_6h >= 20.0:
        return "starkregen", 1
    return "none", 0


def _rolling_6h_max(mm: list[float], end_idx: int) -> float:
    if end_idx < 0:
        return 0.0
    s = max(0, end_idx - 5)
    return float(sum(mm[s : end_idx + 1]))


def _point_key(lat: float, lon: float) -> str:
    return f"{float(lat):.5f},{float(lon):.5f}"


def _parse_iso_z(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return None


def _merge_events_clustered(events: list[dict], *, point: str) -> list[dict]:
    xs = []
    for ev in events or []:
        t = _parse_iso_z(ev.get("peak_ts"))
        if t is None:
            continue
        xs.append((t, ev))
    xs.sort(key=lambda x: x[0])
    if not xs:
        return []

    out: list[dict] = []
    cur = None
    for t, ev in xs:
        if cur is None:
            cur = {
                "start": ev.get("start") or ev.get("peak_ts"),
                "end": ev.get("end") or ev.get("peak_ts"),
                "peak_ts": ev.get("peak_ts"),
                "max_1h_mm": float(ev.get("max_1h_mm") or 0.0),
                "max_6h_mm": float(ev.get("max_6h_mm") or 0.0),
                "sum_mm": float(ev.get("sum_mm") or 0.0),
                "warnstufe": ev.get("warnstufe") or "none",
                "severity": int(ev.get("severity") or 0),
                "sources": {str(ev.get("source") or "unknown")},
                "_peak_dt": t,
            }
            continue

        if abs((t - cur["_peak_dt"]).total_seconds()) <= 12 * 3600:
            s0 = _parse_iso_z(cur.get("start")) or cur["_peak_dt"]
            e0 = _parse_iso_z(cur.get("end")) or cur["_peak_dt"]
            cur["start"] = min(s0, _parse_iso_z(ev.get("start")) or t).isoformat().replace("+00:00", "Z")
            cur["end"] = max(e0, _parse_iso_z(ev.get("end")) or t).isoformat().replace("+00:00", "Z")
            cur["max_1h_mm"] = max(float(cur["max_1h_mm"]), float(ev.get("max_1h_mm") or 0.0))
            cur["max_6h_mm"] = max(float(cur["max_6h_mm"]), float(ev.get("max_6h_mm") or 0.0))
            cur["sum_mm"] = max(float(cur["sum_mm"]), float(ev.get("sum_mm") or 0.0))
            sev = int(ev.get("severity") or 0)
            if sev > int(cur["severity"]):
                cur["severity"] = sev
                cur["warnstufe"] = ev.get("warnstufe") or cur["warnstufe"]
            # prefer the strongest peak timestamp as representative
            cur_peak = float(cur["max_1h_mm"]) + 0.25 * float(cur["max_6h_mm"])
            ev_peak = float(ev.get("max_1h_mm") or 0.0) + 0.25 * float(ev.get("max_6h_mm") or 0.0)
            if ev_peak >= cur_peak:
                cur["peak_ts"] = ev.get("peak_ts") or cur["peak_ts"]
                cur["_peak_dt"] = t
            cur["sources"].add(str(ev.get("source") or "unknown"))
        else:
            out.append(
                {
                    "start": cur["start"],
                    "end": cur["end"],
                    "peak_ts": cur["peak_ts"],
                    "max_1h_mm": round(float(cur["max_1h_mm"]), 2),
                    "max_6h_mm": round(float(cur["max_6h_mm"]), 2),
                    "sum_mm": round(float(cur["sum_mm"]), 2),
                    "warnstufe": cur["warnstufe"],
                    "severity": int(cur["severity"]),
                    "source": "+".join(sorted(cur["sources"])),
                    "point": point,
                }
            )
            cur = {
                "start": ev.get("start") or ev.get("peak_ts"),
                "end": ev.get("end") or ev.get("peak_ts"),
                "peak_ts": ev.get("peak_ts"),
                "max_1h_mm": float(ev.get("max_1h_mm") or 0.0),
                "max_6h_mm": float(ev.get("max_6h_mm") or 0.0),
                "sum_mm": float(ev.get("sum_mm") or 0.0),
                "warnstufe": ev.get("warnstufe") or "none",
                "severity": int(ev.get("severity") or 0),
                "sources": {str(ev.get("source") or "unknown")},
                "_peak_dt": t,
            }

    if cur is not None:
        out.append(
            {
                "start": cur["start"],
                "end": cur["end"],
                "peak_ts": cur["peak_ts"],
                "max_1h_mm": round(float(cur["max_1h_mm"]), 2),
                "max_6h_mm": round(float(cur["max_6h_mm"]), 2),
                "sum_mm": round(float(cur["sum_mm"]), 2),
                "warnstufe": cur["warnstufe"],
                "severity": int(cur["severity"]),
                "source": "+".join(sorted(cur["sources"])),
                "point": point,
            }
        )

    out.sort(key=lambda ev: (ev.get("severity", 0), ev.get("max_1h_mm", 0.0), ev.get("max_6h_mm", 0.0)), reverse=True)
    return out


def _detect_starkregen_events_for_series(series: list[dict], *, source: str = "icon2d") -> list[dict]:
    rows: list[tuple[dt.datetime, float]] = []
    for r in series or []:
        t = r.get("t")
        if not t:
            continue
        try:
            ts = dt.datetime.fromisoformat(str(t).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
        except Exception:
            continue
        try:
            p = float(r.get("precip_mm", 0.0))
        except Exception:
            p = 0.0
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
        m1 = mm[i]
        m6 = _rolling_6h_max(mm, i)
        trig.append((m1 >= 15.0) or (m6 >= 20.0))

    events: list[dict] = []
    i = 0
    while i < len(rows):
        if not trig[i]:
            i += 1
            continue
        s = i
        e = i
        while e + 1 < len(rows):
            # keep event contiguous, allow one weak hour if still rainy.
            if trig[e + 1] or mm[e + 1] >= 0.2:
                e += 1
                continue
            break

        max_1h = max(mm[s : e + 1]) if e >= s else 0.0
        peak_idx = s + max(range(e - s + 1), key=lambda k: mm[s + k])
        max_6h = 0.0
        for j in range(s, e + 1):
            m6 = _rolling_6h_max(mm, j)
            if m6 > max_6h:
                max_6h = m6
        total_mm = float(sum(mm[s : e + 1]))
        level, severity = _event_level(max_1h, max_6h)
        if level != "none":
            events.append(
                {
                    "start": ts[s].isoformat().replace("+00:00", "Z"),
                    "end": ts[e].isoformat().replace("+00:00", "Z"),
                    "peak_ts": ts[peak_idx].isoformat().replace("+00:00", "Z"),
                    "max_1h_mm": round(float(max_1h), 2),
                    "max_6h_mm": round(float(max_6h), 2),
                    "sum_mm": round(total_mm, 2),
                    "warnstufe": level,
                    "severity": severity,
                    "source": source,
                }
            )
        i = e + 1

    events.sort(key=lambda ev: (ev.get("severity", 0), ev.get("max_1h_mm", 0.0), ev.get("max_6h_mm", 0.0)), reverse=True)
    return events


def _local_radar_events_for_point(lat: float, lon: float, start_iso: str, end_iso: str) -> list[dict]:
    """
    Local radar adapter (MVP):
    - uses local weather provider series as proxy source
    - exposes normalized event objects for RADAR_EVENTS_URL integration
    """
    series_bundle = fetch_batch([(float(lat), float(lon))], start_iso, end_iso, "hourly")
    if not series_bundle:
        return []
    series = (series_bundle[0] or {}).get("series") or []
    return _detect_starkregen_events_for_series(series, source="radar")


def _coerce_time_to_iso_window(start: str, end: str) -> tuple[str, str]:
    def parse_any(s: str) -> dt.datetime:
        # Accept YYYY-MM-DD or full ISO.
        txt = str(s).strip()
        if len(txt) == 10 and txt[4] == "-" and txt[7] == "-":
            d = dt.date.fromisoformat(txt)
            return dt.datetime.combine(d, dt.time(0, 0, 0, tzinfo=dt.timezone.utc))
        t = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return t.astimezone(dt.timezone.utc)

    sdt = parse_any(start)
    edt = parse_any(end)
    if edt < sdt:
        raise HTTPException(status_code=400, detail="Ende muss nach Start liegen.")
    # If dates were provided, normalize to whole-day window.
    if len(str(start).strip()) == 10:
        sdt = sdt.replace(hour=0, minute=0, second=0, microsecond=0)
    if len(str(end).strip()) == 10:
        edt = edt.replace(hour=23, minute=0, second=0, microsecond=0)
    return (
        sdt.isoformat().replace("+00:00", "Z"),
        edt.isoformat().replace("+00:00", "Z"),
    )


async def _compute_weather_context_for_bbox(
    *,
    south: float,
    west: float,
    north: float,
    east: float,
    mode: str = "auto",
    days_ago: int = 14,
    hours: int = 24 * 14,
    large_aoi_km2: float = 3.0,
) -> dict:
    mode_in = (mode or "auto").strip().lower()
    if mode_in not in ("auto", "standard", "genauer"):
        mode_in = "auto"

    area_km2 = _bbox_area_km2(south, west, north, east)
    inferred_mode = "genauer" if (mode_in == "auto" and area_km2 >= float(large_aoi_km2)) else mode_in
    if inferred_mode == "auto":
        inferred_mode = "standard"

    sampling_mode = "standard" if inferred_mode == "standard" else "genauer"
    points = _sample_points_from_bbox(south, west, north, east, mode=sampling_mode, inset_frac=0.10)

    startISO, endISO = compute_window_safe(hours=hours, days_ago=days_ago)
    bundle = await run_in_threadpool(fetch_batch, points, startISO, endISO, "hourly")
    stats = await run_in_threadpool(build_weather_stats, bundle, quantiles=[0.9, 0.95, 0.99])
    per = (stats or {}).get("perPoint") or []
    if not isinstance(per, list) or not per:
        raise RuntimeError("Keine Wetterdaten im Zeitfenster gefunden.")

    order = {"trocken": 0, "normal": 1, "nass": 2}
    classes = []
    for it in per:
        c = ((it or {}).get("antecedent_moisture") or {}).get("class")
        if isinstance(c, str) and c in order:
            classes.append(c)

    majority = None
    if classes:
        counts: dict[str, int] = {}
        for c in classes:
            counts[c] = counts.get(c, 0) + 1
        majority = max(counts.keys(), key=lambda k: counts[k])
    cmin = min(classes, key=lambda c: order[c]) if classes else None
    cmax = max(classes, key=lambda c: order[c]) if classes else None

    def qvals(qkey: str) -> list[float]:
        out: list[float] = []
        for it in per:
            qmap = (((it or {}).get("precip_hourly") or {}).get("quantiles_mm")) or {}
            v = qmap.get(qkey)
            try:
                fv = float(v)
            except Exception:
                continue
            if math.isfinite(fv):
                out.append(fv)
        return out

    q90 = qvals("0.9")
    q95 = qvals("0.95")
    q99 = qvals("0.99")
    q90_min, q90_max = _minmax(q90)
    q95_min, q95_max = _minmax(q95)
    q99_min, q99_max = _minmax(q99)
    q90_med = _median(q90)
    q95_med = _median(q95)
    q99_med = _median(q99)

    # Rain proxy for current risk-v2 Rain term (0..1), replacing fixed baseline 0.60 for Starkregen.
    moisture_base = {"trocken": 0.45, "normal": 0.60, "nass": 0.75}.get(majority or "normal", 0.60)
    intensity_base = 0.60
    if q95_med is not None and math.isfinite(float(q95_med)):
        # Normalize typical hourly intensity into 0..1 proxy.
        intensity_base = float(max(0.35, min(1.0, float(q95_med) / 20.0)))
    rain_proxy = float(max(0.35, min(0.95, 0.5 * moisture_base + 0.5 * intensity_base)))

    s_mod = int(round(float(q90_med))) if q90_med is not None else 30
    s_str = int(round(float(q95_med))) if q95_med is not None else 50
    s_ext = int(round(float(q99_med))) if q99_med is not None else 100
    scenario_mm_per_h = sorted({max(1, s_mod), max(1, s_str), max(1, s_ext)})

    dists: list[float] = []
    for it in per:
        try:
            d = float((((it or {}).get("station") or {}).get("distance_km")))
        except Exception:
            continue
        if math.isfinite(d):
            dists.append(d)
    dmin, dmax = _minmax(dists)

    return {
        "mode": {
            "requested": mode_in,
            "used": inferred_mode,
            "large_aoi_threshold_km2": float(large_aoi_km2),
        },
        "sampling": {
            "points": [{"lat": float(lat), "lon": float(lon)} for lat, lon in points],
            "point_count": len(points),
            "bbox_area_km2": round(float(area_km2), 3),
            "corner_inset_frac": 0.10 if len(points) > 1 else 0.0,
        },
        "moisture": {
            "class": majority,
            "range": {"min": cmin, "max": cmax},
        },
        "rainPreset": {
            "moderat": {"mm_per_h": q90_med, "range": {"min": q90_min, "max": q90_max}},
            "stark": {"mm_per_h": q95_med, "range": {"min": q95_min, "max": q95_max}},
            "extrem": {"mm_per_h": q99_med, "range": {"min": q99_min, "max": q99_max}},
        },
        "analysis_integration": {
            "rain_proxy": round(rain_proxy, 3),
            "scenario_mm_per_h": scenario_mm_per_h,
        },
        "meta": {
            "startISO": startISO,
            "endISO": endISO,
            "source": "weather provider (icon2d preferred, dwd fallback)",
            "distance_km_range": {"min": dmin, "max": dmax},
        },
    }


@app.post("/detect-provider")
async def detect_provider_endpoint(bbox: BboxRequest):
    try:
        p = detect_provider(bbox.south, bbox.west, bbox.north, bbox.east)
        return {"provider": {"key": p.key, "name": p.name}}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/")
def root():
    return {"status": "ok", "message": "Hydrowatch Berlin API"}


def _stream_threaded(run_fn):
    """Run blocking work in a background thread and stream NDJSON events live."""

    q: queue.Queue[dict | object] = queue.Queue()
    sentinel = object()

    def worker():
        try:
            result = run_fn(lambda event: q.put(event))
            q.put({"type": "result", "data": result})
        except Exception as exc:
            q.put({"type": "error", "detail": str(exc)})
        finally:
            q.put(sentinel)

    threading.Thread(target=worker, daemon=True).start()

    while True:
        item = q.get()
        if item is sentinel:
            break
        yield json.dumps(item) + "\n"


@app.post("/analyze")
async def analyze_endpoint(
    file: UploadFile = File(...),
    threshold: int = Query(200, ge=10, le=5000),
    analysis_type: str = Query("starkregen"),
):
    """Accept a GeoTIFF DEM, return streamed progress + GeoJSON."""

    suffix = os.path.splitext(file.filename or ".tif")[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    total_steps = 7

    def run(emit):
        def on_progress(step, _total, msg):
            emit({
                "type": "progress",
                "step": step,
                "total": total_steps,
                "message": msg,
            })

        try:
            return analyze_dem(
                tmp_path,
                threshold=threshold,
                progress_callback=on_progress,
                analysis_type=analysis_type,
            )
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    return StreamingResponse(
        _stream_threaded(run),
        media_type="application/x-ndjson",
    )


@app.post("/analyze-bbox")
async def analyze_bbox_endpoint(
    bbox: BboxRequest,
    threshold: int = Query(200, ge=10, le=5000),
    provider: str = Query("auto"),
    dem_source: str = Query("wcs"),
    analysis_type: str = Query("starkregen"),
    weather_auto: bool = Query(True),
    weather_mode: str = Query("auto"),
    weather_days_ago: int = Query(14, ge=0, le=60),
    weather_hours: int = Query(24 * 14, ge=24, le=24 * 90),
    weather_event_mm_h: float | None = Query(None, ge=1.0, le=300.0),
    weather_large_aoi_km2: float = Query(3.0, ge=0.1, le=5000.0),
    st_parts: str | None = Query(None),
    public_confirm: bool = Query(False),
    dem_cache_dir: str | None = Query(None),
    st_cog_dir: str | None = Query(None),
):
    """Fetch DEM from WCS (or public download fallback) and return streamed progress + GeoJSON."""

    dem_source = (dem_source or "wcs").strip().lower()
    # Progress mapping:
    # - public/cog acquisition uses steps 1..4
    # - analysis_dem uses 7 steps and is offset by +4 => max step 11
    # - wcs acquisition uses step 1, analysis offset by +1 => max step 8
    is_starkregen = (analysis_type or "starkregen").strip().lower() == "starkregen"
    weather_enabled = (is_starkregen and weather_auto) or (is_starkregen and weather_event_mm_h is not None)
    total_steps = (11 if dem_source in ("public", "cog") else 8) + (1 if weather_enabled else 0)

    def run(emit):
        tmp_path = None
        weather_ctx_for_analysis = None

        def emit_step(step: int, msg: str):
            emit({
                "type": "progress",
                "step": step,
                "total": total_steps,
                "message": msg,
            })

        def emit_wcs(msg: str):
            emit_step(1, msg)

        def emit_public(phase: str, msg: str):
            phase = (phase or "").strip().lower()
            if phase == "download":
                emit_step(1, f"Public DGM1: {msg}")
            elif phase == "extract":
                emit_step(2, f"Public DGM1: {msg}")
            elif phase == "vrt":
                emit_step(3, f"Public DGM1: {msg}")
            elif phase == "clip":
                emit_step(4, f"Public DGM1: {msg}")
            else:
                emit_step(1, f"Public DGM1: {msg}")

        try:
            if is_starkregen and weather_event_mm_h is not None:
                emit_step(1, "Wetterkontext (aus gewaehltem Ereignis) wird gesetzt...")
                mm_h = float(weather_event_mm_h)
                rain_proxy = float(max(0.35, min(0.95, mm_h / 20.0)))
                weather_ctx_for_analysis = {
                    "source": "weather_event_selected",
                    "mode_used": "event",
                    "moisture_class": "normal",
                    "rain_proxy": rain_proxy,
                    "scenario_mm_per_h": [max(1, int(round(mm_h)))],
                }
            elif weather_enabled:
                emit_step(1, "Wetterkontext wird berechnet...")
                try:
                    # Keep robust fallback: if any weather error occurs, analysis continues with baseline.
                    startISO, endISO = compute_window_safe(hours=weather_hours, days_ago=weather_days_ago)
                    area_km2 = _bbox_area_km2(bbox.south, bbox.west, bbox.north, bbox.east)
                    mode_in = (weather_mode or "auto").strip().lower()
                    if mode_in not in ("auto", "standard", "genauer"):
                        mode_in = "auto"
                    inferred_mode = "genauer" if (mode_in == "auto" and area_km2 >= float(weather_large_aoi_km2)) else mode_in
                    if inferred_mode == "auto":
                        inferred_mode = "standard"
                    sampling_mode = "standard" if inferred_mode == "standard" else "genauer"
                    points = _sample_points_from_bbox(
                        bbox.south,
                        bbox.west,
                        bbox.north,
                        bbox.east,
                        mode=sampling_mode,
                        inset_frac=0.10,
                    )
                    bundle = fetch_batch(points, startISO, endISO, "hourly")
                    stats = build_weather_stats(bundle, quantiles=[0.9, 0.95, 0.99])
                    per = (stats or {}).get("perPoint") or []
                    qmap = []
                    for qkey in ("0.9", "0.95", "0.99"):
                        vals = []
                        for it in per:
                            qm = (((it or {}).get("precip_hourly") or {}).get("quantiles_mm")) or {}
                            try:
                                v = float(qm.get(qkey))
                            except Exception:
                                continue
                            if math.isfinite(v):
                                vals.append(v)
                        qmap.append(_median(vals) if vals else None)
                    classes = []
                    for it in per:
                        c = ((it or {}).get("antecedent_moisture") or {}).get("class")
                        if isinstance(c, str):
                            classes.append(c)
                    majority = None
                    if classes:
                        counts = {}
                        for c in classes:
                            counts[c] = counts.get(c, 0) + 1
                        majority = max(counts.keys(), key=lambda k: counts[k])
                    moisture_base = {"trocken": 0.45, "normal": 0.60, "nass": 0.75}.get(majority or "normal", 0.60)
                    q95 = qmap[1]
                    intensity_base = 0.60 if q95 is None else max(0.35, min(1.0, float(q95) / 20.0))
                    rain_proxy = float(max(0.35, min(0.95, 0.5 * moisture_base + 0.5 * intensity_base)))
                    scen = sorted(
                        {
                            max(1, int(round(float(qmap[0])))) if qmap[0] is not None else 30,
                            max(1, int(round(float(qmap[1])))) if qmap[1] is not None else 50,
                            max(1, int(round(float(qmap[2])))) if qmap[2] is not None else 100,
                        }
                    )
                    weather_ctx_for_analysis = {
                        "source": "weather_preset_auto",
                        "mode_used": inferred_mode,
                        "moisture_class": majority or "normal",
                        "rain_proxy": rain_proxy,
                        "scenario_mm_per_h": scen,
                    }
                except Exception:
                    weather_ctx_for_analysis = None

            if dem_source == "public":
                if not public_confirm:
                    raise HTTPException(
                        status_code=400,
                        detail="Public DEM Download erfordert public_confirm=true (grosses Download-Volumen).",
                    )
                # Public download option is currently only wired for Sachsen-Anhalt DGM1.
                # Use provider=auto to detect first, but validate.
                p = detect_provider(bbox.south, bbox.west, bbox.north, bbox.east) if provider == "auto" else None
                key = p.key if p else provider.strip().lower()
                if key != "sachsen-anhalt":
                    raise HTTPException(
                        status_code=400,
                        detail="dem_source=public ist aktuell nur fuer Sachsen-Anhalt verfuegbar.",
                    )
                parts = [1]
                if st_parts:
                    parts = [int(x) for x in st_parts.split(",") if x.strip()]
                emit_step(1, "Public DGM1: Download/Cache wird vorbereitet...")
                tmp_path = fetch_dem_from_st_public_download(
                    south=bbox.south,
                    west=bbox.west,
                    north=bbox.north,
                    east=bbox.east,
                    parts=parts,
                    progress_callback=emit_public,
                    cache_dir=dem_cache_dir,
                )
                emit_step(4, "Public DGM1: DEM-Ausschnitt geladen")

                def on_progress(step, _total, msg):
                    emit({
                        "type": "progress",
                        "step": step + 4,
                        "total": total_steps,
                        "message": msg,
                    })
            elif dem_source == "cog":
                # Sachsen-Anhalt local COG folder clip (fast local fallback).
                cog_dir = st_cog_dir or os.getenv("ST_COG_DIR")
                if not cog_dir:
                    raise HTTPException(
                        status_code=400,
                        detail="dem_source=cog braucht st_cog_dir oder ST_COG_DIR.",
                    )
                emit_step(1, "COG: VRT/Cache wird vorbereitet...")
                tmp_path = fetch_dem_from_st_cog_dir(
                    south=bbox.south,
                    west=bbox.west,
                    north=bbox.north,
                    east=bbox.east,
                    cog_dir=cog_dir,
                    progress_callback=emit_public,  # reuse phase mapping
                    cache_dir=dem_cache_dir,
                )
                emit_step(4, "COG: DEM-Ausschnitt geladen")

                def on_progress(step, _total, msg):
                    emit({
                        "type": "progress",
                        "step": step + 4,
                        "total": total_steps,
                        "message": msg,
                    })
            else:
                emit_wcs("WCS-Abruf gestartet")
                tmp_path = fetch_dem_from_wcs(
                    bbox.south,
                    bbox.west,
                    bbox.north,
                    bbox.east,
                    progress_callback=emit_wcs,
                    provider_key=provider,
                )
                emit_wcs("WCS-DGM geladen")

                def on_progress(step, _total, msg):
                    emit({
                        "type": "progress",
                        "step": step + 1,
                        "total": total_steps,
                        "message": msg,
                    })
            return analyze_dem(
                tmp_path,
                threshold=threshold,
                progress_callback=on_progress,
                analysis_type=analysis_type,
                aoi_polygon=bbox.polygon,
                weather_context=weather_ctx_for_analysis,
            )
        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    return StreamingResponse(
        _stream_threaded(run),
        media_type="application/x-ndjson",
    )


@app.post("/catchment-bbox")
async def catchment_bbox_endpoint(
    req: CatchmentRequest,
    provider: str = Query("auto"),
    dem_source: str = Query("wcs"),
    analysis_type: str = Query("starkregen"),
    st_parts: str | None = Query(None),
    public_confirm: bool = Query(False),
    dem_cache_dir: str | None = Query(None),
    st_cog_dir: str | None = Query(None),
):
    """
    Delineate upstream catchment polygon for a single point.
    Note: DEM is still fetched by bbox; the catchment can be optionally clipped to the AOI polygon.
    """

    dem_source = (dem_source or "wcs").strip().lower()

    def run():
        tmp_path = None
        try:
            if dem_source == "public":
                if not public_confirm:
                    raise HTTPException(
                        status_code=400,
                        detail="Public DEM Download erfordert public_confirm=true (grosses Download-Volumen).",
                    )
                p = detect_provider(req.south, req.west, req.north, req.east) if provider == "auto" else None
                key = p.key if p else provider.strip().lower()
                if key != "sachsen-anhalt":
                    raise HTTPException(
                        status_code=400,
                        detail="dem_source=public ist aktuell nur fuer Sachsen-Anhalt verfuegbar.",
                    )
                parts = [1]
                if st_parts:
                    parts = [int(x) for x in st_parts.split(",") if x.strip()]
                tmp_path = fetch_dem_from_st_public_download(
                    south=req.south,
                    west=req.west,
                    north=req.north,
                    east=req.east,
                    parts=parts,
                    progress_callback=None,
                    cache_dir=dem_cache_dir,
                )
            elif dem_source == "cog":
                cog_dir = st_cog_dir or os.getenv("ST_COG_DIR")
                if not cog_dir:
                    raise HTTPException(
                        status_code=400,
                        detail="dem_source=cog braucht st_cog_dir oder ST_COG_DIR.",
                    )
                tmp_path = fetch_dem_from_st_cog_dir(
                    south=req.south,
                    west=req.west,
                    north=req.north,
                    east=req.east,
                    cog_dir=cog_dir,
                    progress_callback=None,
                    cache_dir=dem_cache_dir,
                )
            else:
                tmp_path = fetch_dem_from_wcs(
                    req.south,
                    req.west,
                    req.north,
                    req.east,
                    progress_callback=None,
                    provider_key=provider,
                )

            return delineate_catchment_dem(
                tmp_path,
                lat=req.point.lat,
                lon=req.point.lon,
                progress_callback=None,
                aoi_polygon=req.polygon,
            )
        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    return run()


@app.post("/weather-metrics")
async def weather_metrics(req: WeatherRequest):
    """
    DWD station-based weather summary for the AOI bbox.

    Returns: nearest station + precipitation metrics for the requested date range.
    """
    try:
        if req.start and req.end:
            start = dt.date.fromisoformat(req.start)
            end = dt.date.fromisoformat(req.end)
        else:
            start, end = default_last_years_range(3)
    except Exception:
        raise HTTPException(status_code=400, detail="Ungueltiges Datum. Erwartet: YYYY-MM-DD")

    if end < start:
        raise HTTPException(status_code=400, detail="Ende muss nach Start liegen.")

    lat = (req.south + req.north) / 2.0
    lon = (req.west + req.east) / 2.0

    try:
        station, dist_km = find_nearest_station(lat, lon, start, end)
        metrics = compute_precip_metrics(station, start, end)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    return {
        "station": {
            "id": station.station_id,
            "name": station.name,
            "state": station.state,
            "lat": station.lat,
            "lon": station.lon,
            "height_m": station.height_m,
            "distance_km": round(float(dist_km), 2),
            "source": "DWD CDC hourly precipitation (RR)",
        },
        "metrics": metrics,
    }


@app.get("/radar/events-local")
async def radar_events_local(
    lat: float = Query(...),
    lon: float = Query(...),
    start: str = Query(...),
    end: str = Query(...),
):
    """
    Local radar adapter endpoint for hybrid mode.
    Returns event list in a connector-friendly shape: {"events":[...]}.
    """
    try:
        start_iso, end_iso = _coerce_time_to_iso_window(start, end)
        events = await run_in_threadpool(_local_radar_events_for_point, float(lat), float(lon), start_iso, end_iso)
        out = []
        for ev in events:
            out.append(
                {
                    "start": ev.get("start"),
                    "end": ev.get("end"),
                    "peak_ts": ev.get("peak_ts"),
                    "max_1h_mm": ev.get("max_1h_mm"),
                    "max_6h_mm": ev.get("max_6h_mm"),
                    "sum_mm": ev.get("sum_mm"),
                    "warnstufe": ev.get("warnstufe"),
                    "severity": ev.get("severity"),
                }
            )
        return {"events": out, "meta": {"source": "local_radar_adapter"}}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/abflussatlas/weather")
async def abflussatlas_weather(
    points: str = Query(..., min_length=3),
    hours: int = Query(552, ge=1, le=24 * 90),
    daysAgo: int = Query(3, ge=0, le=60),
    agg: str = Query("hourly"),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    try:
        pts = parse_points(points)
        startISO, endISO, end_clamped = _compute_weather_window(start=start, end=end, hours=hours, days_ago=daysAgo)
        data = await run_in_threadpool(fetch_batch, pts, startISO, endISO, agg)
        return {
            "meta": {
                "startISO": startISO,
                "endISO": endISO,
                "agg": agg,
                "points": len(pts),
                "endClampedToToday": bool(end_clamped),
            },
            "data": data,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/abflussatlas/weather/stats")
async def abflussatlas_weather_stats(
    points: str = Query(..., min_length=3),
    hours: int = Query(552, ge=1, le=24 * 90),
    daysAgo: int = Query(14, ge=0, le=60),
    agg: str = Query("hourly"),
    quantiles: str = Query("0.5,0.9,0.95,0.99"),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    try:
        pts = parse_points(points)
        qs: list[float] = []
        for raw in (quantiles or "").split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                q = float(raw)
            except Exception:
                continue
            if 0.0 < q < 1.0:
                qs.append(q)
        if not qs:
            qs = [0.5, 0.9, 0.95, 0.99]

        startISO, endISO, end_clamped = _compute_weather_window(start=start, end=end, hours=hours, days_ago=daysAgo)
        bundle = await run_in_threadpool(fetch_batch, pts, startISO, endISO, agg)
        stats = await run_in_threadpool(build_weather_stats, bundle, quantiles=qs)
        return {
            "meta": {
                "startISO": startISO,
                "endISO": endISO,
                "agg": agg,
                "points": len(pts),
                "quantiles": qs,
                "endClampedToToday": bool(end_clamped),
            },
            "stats": stats,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/abflussatlas/weather/events")
async def abflussatlas_weather_events(
    points: str = Query(..., min_length=3),
    hours: int = Query(24 * 30, ge=24, le=24 * 365),
    daysAgo: int = Query(0, ge=0, le=60),
    agg: str = Query("hourly"),
    source: str = Query("hybrid_radar"),  # icon2d | dwd | radar | hybrid | hybrid_radar
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """
    Detect Starkregen events directly from hourly precipitation series.
    """
    try:
        pts = parse_points(points)
        startISO, endISO, end_clamped = _compute_weather_window(start=start, end=end, hours=hours, days_ago=daysAgo)
        src = (source or "hybrid_radar").strip().lower()
        if src not in ("icon2d", "dwd", "radar", "hybrid", "hybrid_radar"):
            src = "hybrid_radar"

        use_icon = src in ("icon2d", "hybrid", "hybrid_radar")
        use_dwd = src in ("dwd", "hybrid", "hybrid_radar")
        use_radar = src in ("radar", "hybrid_radar")

        # Collect per-point events by source, then cluster/fuse.
        by_point: dict[str, list[dict]] = {_point_key(lat, lon): [] for lat, lon in pts}
        sources_used: list[str] = []
        notes: list[str] = []

        if use_icon:
            bundle = await run_in_threadpool(fetch_batch, pts, startISO, endISO, agg)
            for item in bundle or []:
                p = str(item.get("point") or "")
                pkey = p if p in by_point else None
                if not pkey:
                    # normalize fallback for providers returning slightly different precision
                    try:
                        lat, lon = [float(x) for x in p.split(",")]
                        pkey = _point_key(lat, lon)
                    except Exception:
                        pkey = None
                if not pkey:
                    continue
                evs = _detect_starkregen_events_for_series(item.get("series") or [], source="icon2d")
                by_point[pkey].extend(evs)
            sources_used.append("icon2d")

        if use_dwd:
            start_dt = dt.datetime.fromisoformat(startISO.replace("Z", "+00:00")).astimezone(dt.timezone.utc).date()
            end_dt = dt.datetime.fromisoformat(endISO.replace("Z", "+00:00")).astimezone(dt.timezone.utc).date()
            dwd_ok = 0
            for lat, lon in pts:
                pkey = _point_key(lat, lon)
                try:
                    st, _dist_km = find_nearest_station(float(lat), float(lon), start_dt, end_dt)
                    series = load_hourly_series(st, startISO, endISO)
                    evs = _detect_starkregen_events_for_series(series, source="dwd")
                    by_point[pkey].extend(evs)
                    dwd_ok += 1
                except Exception as exc:
                    notes.append(f"DWD fuer {pkey}: {exc}")
            if dwd_ok > 0:
                sources_used.append("dwd")

        radar_meta = {"available": False, "reason": None}
        if use_radar:
            radar = await run_in_threadpool(fetch_radar_events, pts, startISO, endISO)
            radar_meta["available"] = bool(radar.get("available"))
            radar_meta["reason"] = radar.get("reason")
            per = radar.get("per_point") or {}
            if isinstance(per, dict):
                for pkey, evs in per.items():
                    if pkey not in by_point:
                        continue
                    if isinstance(evs, list):
                        by_point[pkey].extend([e for e in evs if isinstance(e, dict)])
            if radar_meta["available"]:
                sources_used.append("radar")
            else:
                notes.append(str(radar_meta.get("reason") or "Radar nicht verfuegbar"))

        per_point: list[dict] = []
        merged: list[dict] = []
        for pkey, evs in by_point.items():
            fused = _merge_events_clustered(evs, point=pkey)
            per_point.append({"point": pkey, "events": fused, "count": len(fused)})
            merged.extend(fused)

        merged.sort(key=lambda ev: (ev.get("severity", 0), ev.get("max_1h_mm", 0.0), ev.get("max_6h_mm", 0.0)), reverse=True)

        return {
            "meta": {
                "startISO": startISO,
                "endISO": endISO,
                "agg": agg,
                "points": len(pts),
                "source": src,
                "sourcesUsed": sources_used,
                "endClampedToToday": bool(end_clamped),
                "radar": radar_meta if use_radar else None,
                "notes": notes[:8],
            },
            "thresholds": {
                "starkregen": {"max_1h_mm_ge": 15, "max_6h_mm_ge": 20},
                "unwetter": {"max_1h_mm_gt": 25, "max_6h_mm_gt": 35},
                "extrem": {"max_1h_mm_gt": 40, "max_6h_mm_gt": 60},
            },
            "events": {
                "perPoint": per_point,
                "mergedTop": merged[:200],
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/abflussatlas/weather/preset")
async def abflussatlas_weather_preset(
    south: float = Query(...),
    west: float = Query(...),
    north: float = Query(...),
    east: float = Query(...),
    mode: str = Query("auto"),  # auto | standard | genauer
    daysAgo: int = Query(14, ge=0, le=60),
    hours: int = Query(24 * 14, ge=24, le=24 * 90),
    largeAoiKm2: float = Query(3.0, ge=0.1, le=5000.0),
):
    """
    Frontend-friendly weather preset endpoint.

    Returns compact weather context:
      - moisture class (trocken/normal/nass) with spread
      - rain presets (moderat/stark/extrem) from 90/95/99% quantiles
      - auto mode: 1 point for small AOI, 5 points (10% inset) for larger AOIs
    """
    try:
        return await _compute_weather_context_for_bbox(
            south=south,
            west=west,
            north=north,
            east=east,
            mode=mode,
            days_ago=daysAgo,
            hours=hours,
            large_aoi_km2=largeAoiKm2,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/wms/layers")
async def wms_layers(url: str = Query(..., min_length=8)):
    try:
        return {"layers": list_wms_layers(url)}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"WMS GetCapabilities fehlgeschlagen: {exc}")


@app.get("/geocode")
async def geocode_endpoint(
    q: str = Query(..., min_length=2),
    limit: int = Query(6, ge=1, le=12),
    south: float | None = None,
    west: float | None = None,
    north: float | None = None,
    east: float | None = None,
):
    try:
        viewbox = None
        if None not in (south, west, north, east):
            viewbox = (float(west), float(south), float(east), float(north))
        results = await run_in_threadpool(
            geocode,
            q,
            limit,
            "de",
            viewbox=viewbox,
        )
        return {"results": results}
    except Exception as exc:
        msg = str(exc)
        if "Rate limit" in msg:
            raise HTTPException(status_code=429, detail=msg)
        raise HTTPException(status_code=502, detail=f"Geocoding fehlgeschlagen: {msg}")


@app.get("/wcs/selftest")
async def wcs_selftest(
    provider: str = Query("auto"),
    south: float | None = None,
    west: float | None = None,
    north: float | None = None,
    east: float | None = None,
):
    """
    Provider health check:
      - GetCapabilities
      - DescribeCoverage
      - GetCoverage (tiny bbox)

    If provider=auto, you must pass bbox (south/west/north/east).
    """
    if provider == "auto":
        if None in (south, west, north, east):
            raise HTTPException(status_code=400, detail="provider=auto braucht south/west/north/east.")
        try:
            p = detect_provider(float(south), float(west), float(north), float(east))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    else:
        from wcs_client import PROVIDERS

        key = provider.strip().lower()
        if key not in PROVIDERS:
            raise HTTPException(status_code=400, detail=f"Unbekannter Provider '{provider}'.")
        p = PROVIDERS[key]

    # Decide endpoint + coverage for selftest.
    wcs_base = p.wcs_base
    coverage_id = p.coverage_id

    # Sachsen-Anhalt: allow testing the known OpenData service even if disabled in env.
    if p.key == "sachsen-anhalt" and (not wcs_base or not coverage_id):
        wcs_base = "https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_DGM1_WCS_OpenData/guest"
        coverage_id = "Coverage1"

    if not wcs_base or not coverage_id:
        raise HTTPException(
            status_code=400,
            detail=f"Provider '{p.name}' hat keinen WCS-Endpunkt konfiguriert (wcs_base/coverage_id fehlen).",
        )

    # Tiny test bboxes inside known extents (EPSG:25832).
    # NRW: use DescribeCoverage-based bounds already in code; SA: use DescribeCoverage envelope observed in caps.xml.
    if p.key == "nrw":
        test_bbox = (350000.0, 5650000.0, 350200.0, 5650200.0)
    elif p.key == "sachsen-anhalt":
        test_bbox = (670000.0, 5770000.0, 670200.0, 5770200.0)
    else:
        # generic UTM32 bbox; may be outside for Sachsen -> caller should upload or configure endpoint later.
        test_bbox = (400000.0, 5650000.0, 400200.0, 5650200.0)

    try:
        result = run_wcs_selftest(
            provider_key=p.key,
            provider_name=p.name,
            wcs_base=wcs_base,
            coverage_id=coverage_id,
            test_utm32_bbox=test_bbox,
            timeout_s=25,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    return result
