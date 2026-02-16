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
from weather_dwd import compute_precip_metrics, default_last_years_range, find_nearest_station
from weather_window import compute_window_safe
from abflussatlas_weather import fetch_batch, parse_points
from weather_stats import build_weather_stats
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
    total_steps = 11 if dem_source in ("public", "cog") else 8

    def run(emit):
        tmp_path = None

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


@app.get("/abflussatlas/weather")
async def abflussatlas_weather(
    points: str = Query(..., min_length=3),
    hours: int = Query(552, ge=1, le=24 * 90),
    daysAgo: int = Query(3, ge=0, le=60),
    agg: str = Query("hourly"),
):
    try:
        pts = parse_points(points)
        startISO, endISO = compute_window_safe(hours=hours, days_ago=daysAgo)
        data = await run_in_threadpool(fetch_batch, pts, startISO, endISO, agg)
        return {"meta": {"startISO": startISO, "endISO": endISO, "agg": agg, "points": len(pts)}, "data": data}
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

        startISO, endISO = compute_window_safe(hours=hours, days_ago=daysAgo)
        bundle = await run_in_threadpool(fetch_batch, pts, startISO, endISO, agg)
        stats = await run_in_threadpool(build_weather_stats, bundle, quantiles=qs)
        return {"meta": {"startISO": startISO, "endISO": endISO, "agg": agg, "points": len(pts), "quantiles": qs}, "stats": stats}
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
        mode_in = (mode or "auto").strip().lower()
        if mode_in not in ("auto", "standard", "genauer"):
            raise HTTPException(status_code=400, detail="mode muss auto|standard|genauer sein.")

        area_km2 = _bbox_area_km2(south, west, north, east)
        inferred_mode = "genauer" if (mode_in == "auto" and area_km2 >= float(largeAoiKm2)) else mode_in
        if inferred_mode == "auto":
            inferred_mode = "standard"

        sampling_mode = "standard" if inferred_mode == "standard" else "genauer"
        points = _sample_points_from_bbox(south, west, north, east, mode=sampling_mode, inset_frac=0.10)

        startISO, endISO = compute_window_safe(hours=hours, days_ago=daysAgo)
        bundle = await run_in_threadpool(fetch_batch, points, startISO, endISO, "hourly")
        stats = await run_in_threadpool(build_weather_stats, bundle, quantiles=[0.9, 0.95, 0.99])
        per = (stats or {}).get("perPoint") or []
        if not isinstance(per, list) or not per:
            raise HTTPException(status_code=404, detail="Keine Wetterdaten im Zeitfenster gefunden.")

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
                "large_aoi_threshold_km2": float(largeAoiKm2),
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
                "moderat": {"mm_per_h": _median(q90), "range": {"min": q90_min, "max": q90_max}},
                "stark": {"mm_per_h": _median(q95), "range": {"min": q95_min, "max": q95_max}},
                "extrem": {"mm_per_h": _median(q99), "range": {"min": q99_min, "max": q99_max}},
            },
            "meta": {
                "startISO": startISO,
                "endISO": endISO,
                "source": "DWD CDC hourly precipitation (RR), nearest station per sample point",
                "distance_km_range": {"min": dmin, "max": dmax},
            },
        }
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
