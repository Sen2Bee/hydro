"""
Hydrology processing engine using pysheds.

Workflow:
  1. Load DEM raster
  2. Fill depressions (pit filling)
  3. Resolve flats
  4. Compute flow direction (D8)
  5. Compute flow accumulation
  6. Extract river/stream network as GeoJSON
  7. Reproject coordinates to WGS84 (EPSG:4326)

Release-1 additions:
  - Risk score (0-100) and risk classes on stream features
  - Hotspot list with coordinates + reasons
  - Scenario summaries (30/50/100 mm in 1h)
  - Metrics block for UI/reporting
"""

from __future__ import annotations

import math
import os
import tempfile
import threading
import zipfile
from urllib.parse import unquote, urlparse
from typing import Any

import numpy as np
import rasterio
import requests
from pyproj import CRS, Transformer
from pysheds.grid import Grid
from rasterio import features as rio_features
from rasterio.enums import Resampling
from rasterio.transform import rowcol, xy
from rasterio.warp import reproject, transform_bounds
from rasterio.windows import from_bounds


MAX_ANALYSIS_CELLS = 4_000_000
MAX_OUTPUT_FEATURES = 4_000
MAX_LINE_POINTS = 80
DEFAULT_LAYER_CACHE_DIR = os.path.join(os.path.dirname(__file__), ".layer_cache")
_LAYER_FETCH_LOCK = threading.Lock()
DEFAULT_SOIL_LAYER_PATH = os.path.join(os.path.dirname(__file__), "data", "layers", "nrw_soil_kf_10m.tif")
DEFAULT_IMPERVIOUS_LAYER_PATH = os.path.join(os.path.dirname(__file__), "data", "layers", "nrw_impervious_10m.tif")
DEFAULT_LAYER_AOI_BUFFER_M = 100.0


def _to_float_array(arr) -> np.ndarray:
    """Convert ndarray/masked array to float ndarray with NaN for nodata."""
    if np.ma.isMaskedArray(arr):
        return np.asarray(arr.filled(np.nan), dtype=float)
    out = np.asarray(arr, dtype=float)
    return out


def _normalize(values: np.ndarray) -> np.ndarray:
    """Normalize finite values to [0,1], keep NaNs."""
    out = np.full(values.shape, np.nan, dtype=float)
    mask = np.isfinite(values)
    if not np.any(mask):
        return out
    vmin = float(np.nanmin(values[mask]))
    vmax = float(np.nanmax(values[mask]))
    if math.isclose(vmax, vmin):
        out[mask] = 0.0
        return out
    out[mask] = (values[mask] - vmin) / (vmax - vmin)
    return np.clip(out, 0.0, 1.0)


def _load_layer_to_dem_grid(
    layer_path: str | None,
    dem_shape: tuple[int, int],
    dem_transform,
    dem_crs: str | None,
    aoi_buffer_m: float = DEFAULT_LAYER_AOI_BUFFER_M,
) -> np.ndarray | None:
    """Load external raster (windowed by DEM AOI) and reproject it to DEM grid."""
    if not layer_path:
        return None
    if not os.path.exists(layer_path):
        return None
    if not dem_crs:
        return None

    try:
        with rasterio.open(layer_path) as src:
            dem_h, dem_w = dem_shape
            dem_left, dem_bottom, dem_right, dem_top = rasterio.transform.array_bounds(
                dem_h, dem_w, dem_transform
            )

            # Transform DEM extent to source CRS and only read intersecting window.
            src_left, src_bottom, src_right, src_top = transform_bounds(
                dem_crs, src.crs, dem_left, dem_bottom, dem_right, dem_top, densify_pts=21
            )
            src_left -= aoi_buffer_m
            src_bottom -= aoi_buffer_m
            src_right += aoi_buffer_m
            src_top += aoi_buffer_m
            src_bounds = src.bounds
            ix_left = max(src_left, src_bounds.left)
            ix_bottom = max(src_bottom, src_bounds.bottom)
            ix_right = min(src_right, src_bounds.right)
            ix_top = min(src_top, src_bounds.top)

            if ix_left >= ix_right or ix_bottom >= ix_top:
                return None

            window = from_bounds(ix_left, ix_bottom, ix_right, ix_top, transform=src.transform)
            src_data = src.read(1, window=window).astype(np.float32)
            src_transform = src.window_transform(window)

            src_nodata = src.nodata
            if src_nodata is not None:
                src_data[src_data == src_nodata] = np.nan

            dst = np.full(dem_shape, np.nan, dtype=np.float32)
            reproject(
                source=src_data,
                destination=dst,
                src_transform=src_transform,
                src_crs=src.crs,
                src_nodata=np.nan,
                dst_transform=dem_transform,
                dst_crs=dem_crs,
                dst_nodata=np.nan,
                resampling=Resampling.bilinear,
            )
            return dst
    except Exception:
        return None


def _looks_like_http_url(value: str | None) -> bool:
    return bool(value and value.lower().startswith(("http://", "https://")))


def _cache_path_from_url(url: str, layer_key: str) -> str:
    parsed = urlparse(url)
    base = os.path.basename(unquote(parsed.path)) or f"{layer_key}.tif"
    if "." not in base:
        base = f"{base}.tif"
    cache_dir = os.getenv("LAYER_CACHE_DIR", DEFAULT_LAYER_CACHE_DIR)
    return os.path.join(cache_dir, base)


def _download_layer_if_missing(local_path: str | None, url: str | None, layer_label: str) -> str | None:
    """
    Ensure layer exists locally.

    If local path is missing and URL is configured, download once and cache.
    """
    auto_fetch = os.getenv("AUTO_FETCH_LAYERS", "1").strip().lower() not in ("0", "false", "no")
    target_path = local_path

    if target_path and os.path.exists(target_path):
        return target_path

    if not auto_fetch or not _looks_like_http_url(url):
        return target_path if target_path and os.path.exists(target_path) else None

    if not target_path:
        target_path = _cache_path_from_url(url, layer_label)

    with _LAYER_FETCH_LOCK:
        if os.path.exists(target_path):
            return target_path

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        tmp_path = f"{target_path}.download"
        try:
            print(f"[LAYER] Download {layer_label} from {url}")
            with requests.get(url, stream=True, timeout=180) as resp:
                resp.raise_for_status()
                with open(tmp_path, "wb") as dst:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            dst.write(chunk)

            is_zip = url.lower().endswith(".zip") or target_path.lower().endswith(".zip")
            if is_zip:
                zip_cache = target_path if target_path.lower().endswith(".zip") else f"{target_path}.zip"
                os.replace(tmp_path, zip_cache)
                extract_dir = os.path.splitext(zip_cache)[0]
                os.makedirs(extract_dir, exist_ok=True)
                with zipfile.ZipFile(zip_cache, "r") as zf:
                    zf.extractall(extract_dir)
                candidates = []
                for root, _, files in os.walk(extract_dir):
                    for name in files:
                        low = name.lower()
                        if low.endswith((".tif", ".tiff")):
                            candidates.append(os.path.join(root, name))
                if not candidates:
                    raise RuntimeError("zip contains no GeoTIFF")
                chosen = sorted(candidates, key=lambda p: os.path.getsize(p), reverse=True)[0]
                final_path = target_path if target_path.lower().endswith((".tif", ".tiff")) else f"{extract_dir}_{layer_label}.tif"
                os.replace(chosen, final_path)
                print(f"[LAYER] Cached {layer_label} -> {final_path} (from zip)")
                return final_path

            os.replace(tmp_path, target_path)
            print(f"[LAYER] Cached {layer_label} -> {target_path}")
            return target_path
        except Exception as exc:
            print(f"[LAYER] Failed to fetch {layer_label}: {exc}")
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            return None


def _risk_class(score: float) -> str:
    if score >= 85:
        return "sehr_hoch"
    if score >= 70:
        return "hoch"
    if score >= 45:
        return "mittel"
    return "niedrig"


def _feature_midpoint_xy(feature: dict) -> tuple[float, float] | None:
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates")
    gtype = geom.get("type")
    if not coords:
        return None

    if gtype == "LineString" and len(coords) > 0:
        return float(coords[len(coords) // 2][0]), float(coords[len(coords) // 2][1])

    if gtype == "MultiLineString":
        for line in coords:
            if line:
                return float(line[len(line) // 2][0]), float(line[len(line) // 2][1])

    return None


def _sample_value(arr: np.ndarray, transform, x_coord: float, y_coord: float) -> float | None:
    try:
        row, col = rowcol(transform, x_coord, y_coord)
    except Exception:
        return None

    if row < 0 or col < 0 or row >= arr.shape[0] or col >= arr.shape[1]:
        return None

    value = arr[row, col]
    if not np.isfinite(value):
        return None
    return float(value)


def _segment_length_m(line_coords: list[list[float]]) -> float:
    if not line_coords or len(line_coords) < 2:
        return 0.0
    total = 0.0
    for i in range(1, len(line_coords)):
        x0, y0 = line_coords[i - 1]
        x1, y1 = line_coords[i]
        total += math.hypot(float(x1) - float(x0), float(y1) - float(y0))
    return total


def _network_length_km(features: list[dict]) -> float:
    total_m = 0.0
    for feature in features:
        geom = feature.get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if not coords:
            continue
        if gtype == "LineString":
            total_m += _segment_length_m(coords)
        elif gtype == "MultiLineString":
            for line in coords:
                total_m += _segment_length_m(line)
    return total_m / 1000.0


def _resolve_external_factors(
    dem_shape: tuple[int, int],
    dem_transform,
    dem_crs: str | None,
    slope_norm: np.ndarray,
    acc_norm: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """
    Build soil/impervious risk factors from external rasters if available.

    Expected optional env vars:
    - SOIL_RASTER_PATH: soil-related raster (higher value = better infiltration)
    - IMPERVIOUS_RASTER_PATH: imperviousness raster (higher value = more sealed)
    """
    soil_path_cfg = os.getenv("SOIL_RASTER_PATH") or os.path.abspath(DEFAULT_SOIL_LAYER_PATH)
    impervious_path_cfg = os.getenv("IMPERVIOUS_RASTER_PATH") or os.path.abspath(DEFAULT_IMPERVIOUS_LAYER_PATH)
    soil_url = os.getenv("SOIL_RASTER_URL")
    impervious_url = os.getenv("IMPERVIOUS_RASTER_URL")
    try:
        aoi_buffer_m = float(os.getenv("LAYER_AOI_BUFFER_M", str(DEFAULT_LAYER_AOI_BUFFER_M)))
    except ValueError:
        aoi_buffer_m = DEFAULT_LAYER_AOI_BUFFER_M

    soil_path = _download_layer_if_missing(soil_path_cfg, soil_url, "soil")
    impervious_path = _download_layer_if_missing(impervious_path_cfg, impervious_url, "impervious")

    soil_raw = _load_layer_to_dem_grid(
        soil_path, dem_shape, dem_transform, dem_crs, aoi_buffer_m=aoi_buffer_m
    )
    impervious_raw = _load_layer_to_dem_grid(
        impervious_path, dem_shape, dem_transform, dem_crs, aoi_buffer_m=aoi_buffer_m
    )

    if soil_raw is not None:
        # Higher infiltration -> lower risk (invert normalized value).
        soil_infiltration = _normalize(soil_raw)
        soil_risk = 1.0 - np.nan_to_num(soil_infiltration, nan=0.5)
        soil_source = "external"
    else:
        # Proxy fallback: flatter terrain tends to better infiltration.
        soil_risk = np.clip(0.45 + 0.25 * np.nan_to_num(slope_norm, nan=0.5), 0.0, 1.0)
        soil_source = "proxy"

    if impervious_raw is not None:
        impervious_risk = np.nan_to_num(_normalize(impervious_raw), nan=0.35)
        impervious_source = "external"
    else:
        # Proxy fallback: stronger flow corridors are often more urbanized downstream.
        impervious_risk = np.clip(0.35 + 0.50 * np.nan_to_num(acc_norm, nan=0.0), 0.0, 1.0)
        impervious_source = "proxy"

    return soil_risk, impervious_risk, {
        "soil_source": soil_source,
        "impervious_source": impervious_source,
        "soil_path": soil_path if soil_source == "external" else None,
        "impervious_path": impervious_path if impervious_source == "external" else None,
        "layer_aoi_buffer_m": aoi_buffer_m,
    }


def _prepare_analysis_dem(file_path: str) -> tuple[str, dict[str, Any]]:
    """Downsample very large rasters to keep runtime and memory bounded."""
    input_width = 0
    input_height = 0
    scale = 1.0
    new_width = 0
    new_height = 0
    with rasterio.open(file_path) as src:
        input_width = int(src.width)
        input_height = int(src.height)
        total_cells = int(src.width * src.height)
        if total_cells <= MAX_ANALYSIS_CELLS:
            return file_path, {
                "downsample_applied": False,
                "input_width": input_width,
                "input_height": input_height,
                "work_width": input_width,
                "work_height": input_height,
                "scale_factor": 1.0,
            }

        scale = math.sqrt(total_cells / MAX_ANALYSIS_CELLS)
        new_width = max(256, int(src.width / scale))
        new_height = max(256, int(src.height / scale))

        data = src.read(
            1,
            out_shape=(new_height, new_width),
            resampling=Resampling.bilinear,
        )
        transform = src.transform * src.transform.scale(
            src.width / new_width,
            src.height / new_height,
        )
        profile = src.profile.copy()
        profile.update(
            width=new_width,
            height=new_height,
            transform=transform,
        )

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tif")
        tmp.close()
        with rasterio.open(tmp.name, "w", **profile) as dst:
            dst.write(data, 1)

    return tmp.name, {
        "downsample_applied": True,
        "input_width": input_width,
        "input_height": input_height,
        "work_width": int(new_width),
        "work_height": int(new_height),
        "scale_factor": round(float(scale), 3),
    }


def _downsample_line_points(coords: list[list[float]], max_points: int) -> list[list[float]]:
    if len(coords) <= max_points:
        return coords
    if max_points < 3:
        return [coords[0], coords[-1]]

    step = max(1, len(coords) // (max_points - 1))
    reduced = coords[::step]
    if reduced[-1] != coords[-1]:
        reduced.append(coords[-1])
    return reduced[:max_points - 1] + [coords[-1]] if len(reduced) > max_points else reduced


def _reduce_feature_geometry(feature: dict, max_points_per_line: int) -> dict:
    geom = feature.get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if not coords:
        return feature

    if gtype == "LineString":
        geom["coordinates"] = _downsample_line_points(coords, max_points_per_line)
    elif gtype == "MultiLineString":
        geom["coordinates"] = [
            _downsample_line_points(line, max_points_per_line) for line in coords
        ]
    return feature


def _limit_output_features(features: list[dict]) -> tuple[list[dict], bool]:
    if len(features) <= MAX_OUTPUT_FEATURES:
        return [_reduce_feature_geometry(f, MAX_LINE_POINTS) for f in features], False

    ranked = sorted(
        features,
        key=lambda f: float((f.get("properties") or {}).get("risk_score", 0)),
        reverse=True,
    )
    selected = ranked[:MAX_OUTPUT_FEATURES]
    return [_reduce_feature_geometry(f, MAX_LINE_POINTS) for f in selected], True


def _reproject_geojson(geojson: dict, src_crs_str: str) -> dict:
    """Reproject all GeoJSON coordinates to WGS84 in-place."""
    src_crs = CRS(src_crs_str)
    dst_crs = CRS("EPSG:4326")

    if src_crs == dst_crs:
        return geojson

    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)

    def transform_xy(pt):
        x, y = pt
        return list(transformer.transform(x, y))

    def transform_coords(coords):
        return [transform_xy(pt) for pt in coords]

    for feature in geojson.get("features", []):
        geom = feature.get("geometry", {})
        gtype = geom.get("type", "")
        coords = geom.get("coordinates")
        if not coords:
            continue

        if gtype == "Point":
            geom["coordinates"] = transform_xy(coords)
        elif gtype == "MultiPoint":
            geom["coordinates"] = transform_coords(coords)
        elif gtype == "LineString":
            geom["coordinates"] = transform_coords(coords)
        elif gtype == "MultiLineString":
            geom["coordinates"] = [transform_coords(line) for line in coords]
        elif gtype == "Polygon":
            # coords: [ring[pt[x,y], ...], ...]
            geom["coordinates"] = [transform_coords(ring) for ring in coords]
        elif gtype == "MultiPolygon":
            # coords: [poly[ring[pt], ...], ...]
            geom["coordinates"] = [[transform_coords(ring) for ring in poly] for poly in coords]
        elif gtype == "GeometryCollection":
            # Rare, but keep it robust.
            for g in geom.get("geometries", []) or []:
                _reproject_geojson({"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": g}]}, src_crs_str)

    return geojson


def _build_hotspots(
    risk_score: np.ndarray,
    acc: np.ndarray,
    slope_deg: np.ndarray,
    soil_risk: np.ndarray,
    impervious_risk: np.ndarray,
    transform,
    src_crs_str: str | None,
    pixel_area_m2: float,
    top_n: int = 8,
) -> list[dict[str, Any]]:
    mask = np.isfinite(risk_score)
    indices = np.argwhere(mask)
    if indices.size == 0:
        return []

    values = risk_score[mask]
    order = np.argsort(values)[::-1]

    to_wgs = None
    if src_crs_str:
        src_crs = CRS(src_crs_str)
        if src_crs != CRS("EPSG:4326"):
            to_wgs = Transformer.from_crs(src_crs, "EPSG:4326", always_xy=True)

    selected: list[tuple[int, int]] = []
    min_dist_px = 25
    hotspots: list[dict[str, Any]] = []

    for ord_idx in order:
        row_i, col_i = indices[ord_idx]

        too_close = False
        for prev_row, prev_col in selected:
            if (row_i - prev_row) ** 2 + (col_i - prev_col) ** 2 < min_dist_px ** 2:
                too_close = True
                break
        if too_close:
            continue

        score_val = float(risk_score[row_i, col_i])
        acc_val = float(acc[row_i, col_i]) if np.isfinite(acc[row_i, col_i]) else 0.0
        slope_val = float(slope_deg[row_i, col_i]) if np.isfinite(slope_deg[row_i, col_i]) else 0.0
        soil_val = float(soil_risk[row_i, col_i]) if np.isfinite(soil_risk[row_i, col_i]) else 0.0
        imp_val = (
            float(impervious_risk[row_i, col_i])
            if np.isfinite(impervious_risk[row_i, col_i])
            else 0.0
        )

        upstream_area_m2 = 0.0
        upstream_area_km2 = 0.0
        if acc_val > 0 and pixel_area_m2 > 0:
            upstream_area_m2 = float(acc_val) * float(pixel_area_m2)
            upstream_area_km2 = upstream_area_m2 / 1_000_000.0

        reason_parts = []
        if acc_val >= np.nanpercentile(acc[np.isfinite(acc)], 90):
            reason_parts.append("starke Fliessakkumulation")
        if slope_val >= np.nanpercentile(slope_deg[np.isfinite(slope_deg)], 75):
            reason_parts.append("hohe Hangneigung")
        if soil_val >= 0.65:
            reason_parts.append("geringe Infiltration")
        if imp_val >= 0.65:
            reason_parts.append("hoher Versiegelungsgrad")
        if not reason_parts:
            reason_parts.append("kombinierter Terrain-Risikoindikator")

        x_coord, y_coord = xy(transform, int(row_i), int(col_i), offset="center")
        lon, lat = (float(x_coord), float(y_coord))
        if to_wgs is not None:
            lon, lat = to_wgs.transform(lon, lat)

        hotspots.append(
            {
                "rank": len(hotspots) + 1,
                "lat": float(lat),
                "lon": float(lon),
                "risk_score": int(round(score_val)),
                "risk_class": _risk_class(score_val),
                "reason": " + ".join(reason_parts),
                # Keep precise upstream area for UI filters (net density) and avoid rounding to 0 on small AOIs.
                "upstream_area_m2": int(round(upstream_area_m2)),
                "upstream_area_km2": round(upstream_area_km2, 6),
            }
        )

        selected.append((int(row_i), int(col_i)))
        if len(hotspots) >= top_n:
            break

    return hotspots


def _build_ponding_hotspots(
    ponding_depth_m: np.ndarray,
    acc: np.ndarray,
    transform,
    src_crs_str: str | None,
    pixel_area_m2: float,
    top_n: int = 4,
) -> list[dict[str, Any]]:
    """
    Identify potential ponding/sink hotspots from depression fill depth.

    This is a screening indicator: where the DEM suggests local sinks where water could collect.
    """
    mask = np.isfinite(ponding_depth_m) & (ponding_depth_m > 0.0)
    indices = np.argwhere(mask)
    if indices.size == 0:
        return []

    depths = ponding_depth_m[mask]
    order = np.argsort(depths)[::-1]

    p95 = float(np.nanpercentile(depths, 95)) if depths.size else 0.0
    if not np.isfinite(p95) or p95 <= 0.0:
        p95 = float(np.nanmax(depths)) if depths.size else 0.0
    if not np.isfinite(p95) or p95 <= 0.0:
        return []

    to_wgs = None
    if src_crs_str:
        src_crs = CRS(src_crs_str)
        if src_crs != CRS("EPSG:4326"):
            to_wgs = Transformer.from_crs(src_crs, "EPSG:4326", always_xy=True)

    selected: list[tuple[int, int]] = []
    min_dist_px = 28
    hotspots: list[dict[str, Any]] = []

    for ord_idx in order:
        row_i, col_i = indices[ord_idx]

        too_close = False
        for prev_row, prev_col in selected:
            if (row_i - prev_row) ** 2 + (col_i - prev_col) ** 2 < min_dist_px ** 2:
                too_close = True
                break
        if too_close:
            continue

        depth_m = float(ponding_depth_m[row_i, col_i])
        score_val = float(np.clip((depth_m / p95) * 100.0, 0.0, 100.0))
        acc_val = float(acc[row_i, col_i]) if np.isfinite(acc[row_i, col_i]) else 0.0
        upstream_area_m2 = 0.0
        upstream_area_km2 = 0.0
        if acc_val > 0 and pixel_area_m2 > 0:
            upstream_area_m2 = float(acc_val) * float(pixel_area_m2)
            upstream_area_km2 = upstream_area_m2 / 1_000_000.0

        x_coord, y_coord = xy(transform, int(row_i), int(col_i), offset="center")
        lon, lat = (float(x_coord), float(y_coord))
        if to_wgs is not None:
            lon, lat = to_wgs.transform(lon, lat)

        hotspots.append(
            {
                "rank": len(hotspots) + 1,
                "lat": float(lat),
                "lon": float(lon),
                "risk_score": int(round(score_val)),
                "risk_class": _risk_class(score_val),
                "reason": f"Senke / pot. Stauwasser (Tiefe ~{int(round(depth_m * 100.0))} cm)",
                "ponding_depth_m": round(depth_m, 3),
                "upstream_area_m2": int(round(upstream_area_m2)),
                "upstream_area_km2": round(upstream_area_km2, 6),
                "hotspot_type": "ponding",
            }
        )

        selected.append((int(row_i), int(col_i)))
        if len(hotspots) >= top_n:
            break

    return hotspots


def _measures_for_hotspot(h: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Simple rule-based measure suggestions for end users (v1).
    Output is intentionally practical and non-technical.
    """
    reasons = (h.get("reason") or "").lower()
    risk_score = int(h.get("risk_score") or 0)

    def prio(base: int) -> int:
        if risk_score >= 85:
            return max(1, base - 1)
        if risk_score >= 70:
            return base
        return base + 1

    measures: list[dict[str, Any]] = []

    if "versiegel" in reasons:
        measures.append(
            {
                "id": "de-seal",
                "title": "Entsiegeln und wasserfreundliche Flaechen",
                "why": "Weniger Direktabfluss, mehr Versickerung.",
                "what": "Pflaster mit Fugen, Rasengitter, Drainflaechen, begruente Streifen.",
                "effort": "mittel",
                "time": "Wochen",
                "priority": prio(2),
            }
        )
        measures.append(
            {
                "id": "rain-garden",
                "title": "Mulde / Regenbeet (Retention vor Ort)",
                "why": "Spitzenabfluss wird gepuffert.",
                "what": "Mulde mit Notueberlauf, ggf. kombiniert mit Rigole.",
                "effort": "mittel",
                "time": "Tage",
                "priority": prio(2),
            }
        )

    if "infiltration" in reasons or "infil" in reasons or "infiltration" in h.get("reason", "").lower():
        measures.append(
            {
                "id": "surface-roughness",
                "title": "Oberflaeche rauer machen (Abfluss bremsen)",
                "why": "Reduziert Erosion und verzögert Abfluss.",
                "what": "Zwischenfrucht, Mulch, quer zur Hangrichtung bearbeiten.",
                "effort": "gering",
                "time": "Tage",
                "priority": prio(1),
            }
        )

    if "hang" in reasons or "neigung" in reasons:
        measures.append(
            {
                "id": "contour",
                "title": "Hang: Bewirtschaftung quer zum Gefaelle / Terrassierung",
                "why": "Verringert Abflussgeschwindigkeit und Erosionsenergie.",
                "what": "Konturpfluegen, kleine Querriegel, Hecken-/Saumstreifen.",
                "effort": "mittel",
                "time": "Wochen",
                "priority": prio(1),
            }
        )

    if "akkumulation" in reasons or "fliess" in reasons:
        measures.append(
            {
                "id": "drainage-path",
                "title": "Abflusswege sichern (Graben, Durchlass, Rueckhalt)",
                "why": "Verhindert unkontrollierte Umwege des Wassers.",
                "what": "Einlauf freihalten, kleine Rueckhalte, definierter Notabfluss.",
                "effort": "gering",
                "time": "Tage",
                "priority": prio(1),
            }
        )

    if "stauwasser" in reasons or "senke" in reasons:
        measures.append(
            {
                "id": "micro-retention",
                "title": "Stauwasser entschärfen (Notablauf / Rueckhalt)",
                "why": "Senken koennen Wasser sammeln und bei Ueberlauf Schaden verlagern.",
                "what": "Kontrollierter Notablauf, kleine Rueckhalte, Einlauf freihalten.",
                "effort": "mittel",
                "time": "Tage",
                "priority": prio(1),
            }
        )

    # Generic measure always present:
    measures.append(
        {
            "id": "site-check",
            "title": "Vor-Ort Pruefung",
            "why": "Modelle erkennen nicht alle lokalen Details (Kanten, Einlaeufe, Hindernisse).",
            "what": "Hotspot begehen, Fotos/Notizen, Abflussrichtung bei Starkregen beobachten.",
            "effort": "gering",
            "time": "Stunden",
            "priority": prio(1),
        }
    )

    # Sort by priority then effort:
    effort_rank = {"gering": 1, "mittel": 2, "hoch": 3}
    measures.sort(key=lambda m: (int(m.get("priority", 9)), effort_rank.get(m.get("effort"), 9)))
    return measures[:6]


def _scenario_summary(risk_norm: np.ndarray, valid_mask: np.ndarray, rain_mm_per_h: int) -> dict[str, Any]:
    scale = rain_mm_per_h / 50.0
    scenario_score = np.clip(risk_norm * scale, 0.0, 1.0) * 100.0

    if not np.any(valid_mask):
        return {
            "rain_mm_per_h": rain_mm_per_h,
            "mean_score": 0,
            "high_share_percent": 0,
            "very_high_share_percent": 0,
        }

    vals = scenario_score[valid_mask]
    high_share = float(np.mean(vals >= 70.0) * 100.0)
    very_high_share = float(np.mean(vals >= 85.0) * 100.0)

    return {
        "rain_mm_per_h": rain_mm_per_h,
        "mean_score": int(round(float(np.mean(vals)))),
        "high_share_percent": round(high_share, 1),
        "very_high_share_percent": round(very_high_share, 1),
    }


def analyze_dem(
    file_path: str,
    threshold: int = 200,
    progress_callback=None,
    analysis_type: str = "starkregen",
    aoi_polygon: list[list[float]] | None = None,
    weather_context: dict[str, Any] | None = None,
) -> dict:
    """Run full flow accumulation analysis and return enriched GeoJSON."""

    def progress(step, total, msg):
        print(f"  [{step}/{total}] {msg}")
        if progress_callback:
            progress_callback(step, total, msg)

    work_path, prep_info = _prepare_analysis_dem(file_path)
    temp_created = work_path != file_path

    if prep_info.get("downsample_applied"):
        print(
            "[PERF] Downsample applied: "
            f"{prep_info['input_width']}x{prep_info['input_height']} -> "
            f"{prep_info['work_width']}x{prep_info['work_height']} "
            f"(scale ~{prep_info['scale_factor']})"
        )

    progress(1, 7, "CRS wird erkannt...")
    with rasterio.open(work_path) as src:
        src_crs = str(src.crs) if src.crs else None
        transform = src.transform
        pixel_area_m2 = abs(float(src.transform.a * src.transform.e))

    print(f"  Source CRS: {src_crs}")

    progress(2, 7, "DEM wird geladen...")
    grid = Grid.from_raster(work_path)
    dem = grid.read_raster(work_path)
    dem_arr = _to_float_array(dem)

    print(f"  DEM shape: {dem_arr.shape}")
    if np.any(np.isfinite(dem_arr)):
        print(f"  DEM range: {float(np.nanmin(dem_arr)):.1f} - {float(np.nanmax(dem_arr)):.1f}")

    progress(3, 7, "Senken werden gefuellt...")
    pit_filled = grid.fill_depressions(dem)
    flats_resolved = grid.resolve_flats(pit_filled)
    pit_arr = _to_float_array(pit_filled)
    ponding_depth_m = np.clip(pit_arr - dem_arr, 0.0, None)
    ponding_depth_m[~np.isfinite(ponding_depth_m)] = np.nan

    progress(4, 7, "Fliessrichtung wird berechnet (D8)...")
    dirmap = (64, 128, 1, 2, 4, 8, 16, 32)
    fdir = grid.flowdir(flats_resolved, dirmap=dirmap)

    progress(5, 7, "Fliessakkumulation wird berechnet...")
    acc = grid.accumulation(fdir, dirmap=dirmap)
    acc_arr = _to_float_array(acc)

    if np.any(np.isfinite(acc_arr)):
        print(
            f"  Accumulation range: {float(np.nanmin(acc_arr)):.0f} - "
            f"{float(np.nanmax(acc_arr)):.0f}"
        )

    progress(6, 7, "Fliessnetzwerk wird extrahiert...")
    branches = grid.extract_river_network(fdir, acc > threshold, dirmap=dirmap)

    # Risk model v2: terrain + external layers (soil/impervious) with fallback.
    res_x = abs(float(transform.a)) if transform else 1.0
    res_y = abs(float(transform.e)) if transform else 1.0
    grad_y, grad_x = np.gradient(dem_arr, res_y, res_x)
    slope_deg = np.degrees(np.arctan(np.hypot(grad_x, grad_y)))

    acc_log = np.log1p(np.clip(acc_arr, 0.0, None))
    acc_norm = _normalize(acc_log)
    slope_norm = _normalize(np.clip(slope_deg, 0.0, 60.0))

    soil_risk, impervious_risk, layer_info = _resolve_external_factors(
        dem_shape=dem_arr.shape,
        dem_transform=transform,
        dem_crs=src_crs,
        slope_norm=slope_norm,
        acc_norm=acc_norm,
    )
    print(
        "[RISK] Sources: "
        f"soil={layer_info['soil_source']}, "
        f"impervious={layer_info['impervious_source']}"
    )
    analysis_type = (analysis_type or "starkregen").strip().lower()
    rain_proxy_value = 0.60
    weather_source = "constant_baseline"
    weather_mode_used = "n/a"
    weather_moisture_class = "n/a"
    weather_scenarios_mm_h = [30, 50, 100]
    if isinstance(weather_context, dict):
        try:
            rp = float(weather_context.get("rain_proxy"))
            if np.isfinite(rp):
                rain_proxy_value = float(np.clip(rp, 0.05, 1.0))
        except Exception:
            pass
        try:
            s = weather_context.get("scenario_mm_per_h")
            if isinstance(s, list):
                vals = sorted({int(round(float(v))) for v in s if np.isfinite(float(v)) and float(v) > 0})
                if vals:
                    weather_scenarios_mm_h = vals[:3]
        except Exception:
            pass
        weather_source = str(weather_context.get("source") or weather_source)
        weather_mode_used = str(weather_context.get("mode_used") or weather_mode_used)
        weather_moisture_class = str(weather_context.get("moisture_class") or weather_moisture_class)

    rain_hist_proxy = np.full(acc_norm.shape, rain_proxy_value, dtype=float)

    # Default: Starkregen-Screening (Score v2).
    # Erosion MVP: topographischer Treiber (LS-Proxy) ohne Anspruch auf Gutachten.
    scenarios = []
    if analysis_type == "erosion":
        drv = np.nan_to_num(acc_norm, nan=0.0) * np.nan_to_num(slope_norm, nan=0.0)
        drv_norm = _normalize(drv)
        risk_norm = drv_norm
        risk_score = np.clip(np.round(drv_norm * 100.0), 0.0, 100.0)
        model_version = "erosion-v1-topo"
        rain_history_assumption = "n/a"
    else:
        risk_norm = (
            0.35 * np.nan_to_num(acc_norm, nan=0.0)
            + 0.25 * np.nan_to_num(slope_norm, nan=0.0)
            + 0.15 * np.nan_to_num(soil_risk, nan=0.5)
            + 0.15 * np.nan_to_num(impervious_risk, nan=0.35)
            + 0.10 * rain_hist_proxy
        )
        risk_score = np.clip(np.round(risk_norm * 100.0), 0.0, 100.0)
        model_version = "risk-v2-soil-impervious"
        rain_history_assumption = (
            "weather_driven_proxy"
            if weather_source != "constant_baseline"
            else "constant_nrw_baseline"
        )

    valid_mask = np.isfinite(dem_arr) & np.isfinite(acc_arr)
    risk_score[~valid_mask] = np.nan

    features = branches.get("features", [])
    full_feature_count = len(features)
    for feature in features:
        midpoint = _feature_midpoint_xy(feature)
        if not midpoint:
            continue
        sampled = _sample_value(risk_score, transform, midpoint[0], midpoint[1])
        if sampled is None:
            continue
        acc_mid = _sample_value(acc_arr, transform, midpoint[0], midpoint[1])
        slope_mid = _sample_value(slope_deg, transform, midpoint[0], midpoint[1])
        props = feature.setdefault("properties", {})
        props["risk_score"] = int(round(sampled))
        props["risk_class"] = _risk_class(sampled)
        if acc_mid is not None:
            props["acc_cells"] = int(round(float(acc_mid)))
            upstream_area_m2 = float(acc_mid) * float(pixel_area_m2)
            props["upstream_area_m2"] = int(round(upstream_area_m2))
            props["upstream_area_km2"] = round(upstream_area_m2 / 1_000_000.0, 6)
        if slope_mid is not None:
            props["slope_deg"] = round(float(slope_mid), 1)

    if src_crs:
        progress(7, 7, "Koordinaten werden transformiert...")
        branches = _reproject_geojson(branches, src_crs)
        features = branches.get("features", [])

    def _point_in_poly(lon: float, lat: float, poly_lonlat: list[tuple[float, float]]) -> bool:
        # Ray casting algorithm. poly is [(lon,lat), ...] (closed or open).
        if len(poly_lonlat) < 3:
            return False
        inside = False
        n = len(poly_lonlat)
        x, y = lon, lat
        for i in range(n):
            x1, y1 = poly_lonlat[i]
            x2, y2 = poly_lonlat[(i + 1) % n]
            # Check if edge intersects horizontal ray at y.
            if (y1 > y) != (y2 > y):
                xinters = (x2 - x1) * (y - y1) / ((y2 - y1) if (y2 - y1) != 0 else 1e-12) + x1
                if x < xinters:
                    inside = not inside
        return inside

    def _feature_any_point_inside(feature: dict, poly_lonlat: list[tuple[float, float]]) -> bool:
        g = (feature or {}).get("geometry") or {}
        coords = g.get("coordinates")
        if not coords:
            return False
        if g.get("type") == "LineString":
            lines = [coords]
        elif g.get("type") == "MultiLineString":
            lines = coords
        else:
            return False
        for line in lines:
            for pt in line:
                try:
                    lon, lat = float(pt[0]), float(pt[1])
                except Exception:
                    continue
                if _point_in_poly(lon, lat, poly_lonlat):
                    return True
        return False

    clip_poly_lonlat: list[tuple[float, float]] | None = None

    # If a polygon AOI was provided, clip displayed/evaluated outputs to that polygon.
    # Note: DEM/accumulation are still computed on the bbox window; this is a presentation/evaluation clip (MVP).
    if aoi_polygon and isinstance(aoi_polygon, list) and len(aoi_polygon) >= 3:
        try:
            poly_lonlat: list[tuple[float, float]] = []
            for p in aoi_polygon:
                if not isinstance(p, (list, tuple)) or len(p) < 2:
                    continue
                lat = float(p[0])
                lon = float(p[1])
                poly_lonlat.append((lon, lat))
            if len(poly_lonlat) >= 3:
                clip_poly_lonlat = poly_lonlat
                clipped = [f for f in (branches.get("features") or []) if _feature_any_point_inside(f, poly_lonlat)]
                branches["features"] = clipped
                features = clipped
        except Exception:
            # Fail open: better show bbox result than crash.
            pass

    reduced_features, truncated = _limit_output_features(features)
    branches["features"] = reduced_features

    hotspots = _build_hotspots(
        risk_score=risk_score,
        acc=acc_arr,
        slope_deg=slope_deg,
        soil_risk=soil_risk,
        impervious_risk=impervious_risk,
        transform=transform,
        src_crs_str=src_crs,
        pixel_area_m2=pixel_area_m2,
    )

    # Starkregen: add a few dedicated ponding/sink hotspots (from depression fill depth).
    if analysis_type != "erosion":
        pond_hotspots = _build_ponding_hotspots(
            ponding_depth_m=ponding_depth_m,
            acc=acc_arr,
            transform=transform,
            src_crs_str=src_crs,
            pixel_area_m2=pixel_area_m2,
            top_n=4,
        )
        for h in pond_hotspots:
            h["rank"] = len(hotspots) + 1
            hotspots.append(h)

    for h in hotspots:
        h["measures"] = _measures_for_hotspot(h)

    if clip_poly_lonlat:
        clipped_hotspots = []
        for h in hotspots:
            try:
                lat = float(h.get("lat"))
                lon = float(h.get("lon"))
            except Exception:
                continue
            if _point_in_poly(lon, lat, clip_poly_lonlat):
                clipped_hotspots.append(h)
        # Re-rank after clipping to keep Hotspot #1..N stable.
        for idx, h in enumerate(clipped_hotspots, start=1):
            h["rank"] = idx
        hotspots = clipped_hotspots

    class_counts = {"niedrig": 0, "mittel": 0, "hoch": 0, "sehr_hoch": 0}
    for score_val in risk_score[valid_mask]:
        class_counts[_risk_class(float(score_val))] += 1

    metrics = {
        "feature_count": int(full_feature_count),
        "feature_count_output": int(len(reduced_features)),
        "network_length_km": round(_network_length_km(features), 2),
        "aoi_area_km2": round(float(np.sum(valid_mask) * pixel_area_m2 / 1_000_000.0), 3),
        "risk_score_mean": int(round(float(np.nanmean(risk_score[valid_mask])))) if np.any(valid_mask) else 0,
        "risk_score_max": int(round(float(np.nanmax(risk_score[valid_mask])))) if np.any(valid_mask) else 0,
        "threshold": int(threshold),
        "model_version": model_version,
    }
    if analysis_type != "erosion":
        pond_mask = np.isfinite(ponding_depth_m) & (ponding_depth_m > 0.0) & valid_mask
        if np.any(pond_mask):
            metrics["ponding_area_km2"] = round(float(np.sum(pond_mask) * pixel_area_m2 / 1_000_000.0), 3)
            metrics["ponding_volume_m3"] = int(round(float(np.nansum(ponding_depth_m[pond_mask]) * pixel_area_m2)))
            metrics["ponding_max_depth_m"] = round(float(np.nanmax(ponding_depth_m[pond_mask])), 3)
        else:
            metrics["ponding_area_km2"] = 0.0
            metrics["ponding_volume_m3"] = 0
            metrics["ponding_max_depth_m"] = 0.0

    if analysis_type != "erosion":
        scenarios = [_scenario_summary(risk_norm, valid_mask, int(mm)) for mm in weather_scenarios_mm_h]

    branches["analysis"] = {
        "kind": analysis_type,
        "metrics": metrics,
        "class_distribution": class_counts,
        "hotspots": hotspots,
        "scenarios": scenarios,
        "assumptions": {
            "soil": layer_info["soil_source"],
            "impervious": layer_info["impervious_source"],
            "rain_history": rain_history_assumption,
            "rain_proxy": round(float(rain_proxy_value), 3),
            "weather_source": weather_source,
            "weather_mode": weather_mode_used,
            "weather_moisture_class": weather_moisture_class,
            "soil_path": layer_info["soil_path"],
            "impervious_path": layer_info["impervious_path"],
            "layer_aoi_buffer_m": layer_info["layer_aoi_buffer_m"],
        },
        "performance": {
            **prep_info,
            "output_truncated": bool(truncated),
            "max_output_features": MAX_OUTPUT_FEATURES,
            "max_line_points": MAX_LINE_POINTS,
        },
    }

    print(f"  Features: {full_feature_count} (output: {len(reduced_features)})")
    if temp_created:
        try:
            os.remove(work_path)
        except OSError:
            pass
    return branches


def delineate_catchment_dem(
    file_path: str,
    lat: float,
    lon: float,
    progress_callback=None,
    aoi_polygon: list[list[float]] | None = None,
) -> dict:
    """
    Delineate upstream catchment polygon for a single pour point.

    Inputs:
      - file_path: DEM clipped to AOI bbox
      - lat/lon: pour point (WGS84)
      - aoi_polygon: optional AOI polygon (lat,lon points) to clip the catchment mask before polygonization

    Returns:
      - GeoJSON FeatureCollection (WGS84)
      - meta: area metrics
    """

    def progress(msg: str):
        if progress_callback:
            try:
                progress_callback(msg)
            except Exception:
                pass

    progress("DEM wird geladen...")
    grid = Grid.from_raster(file_path)
    dem = grid.read_raster(file_path)
    dem_arr = _to_float_array(dem)

    with rasterio.open(file_path) as src:
        src_crs = src.crs
        transform = src.transform
        pixel_area_m2 = abs(float(src.transform.a * src.transform.e))

    if not src_crs:
        raise ValueError("DEM hat kein CRS.")

    # Project pour point to DEM CRS
    tr = Transformer.from_crs("EPSG:4326", src_crs, always_xy=True)
    x, y = tr.transform(float(lon), float(lat))

    progress("Senken werden gefuellt...")
    pit_filled = grid.fill_depressions(dem)
    flats_resolved = grid.resolve_flats(pit_filled)

    progress("Fliessrichtung wird berechnet (D8)...")
    dirmap = (64, 128, 1, 2, 4, 8, 16, 32)
    fdir = grid.flowdir(flats_resolved, dirmap=dirmap)

    progress("Einzugsgebiet wird abgegrenzt...")
    catch = grid.catchment(x=x, y=y, fdir=fdir, dirmap=dirmap, xytype="coordinate")
    catch_arr = _to_float_array(catch)
    catch_mask = np.isfinite(catch_arr) & (catch_arr > 0)

    # Optional clip to AOI polygon before polygonization (mask-level, robust, no shapely).
    if aoi_polygon and isinstance(aoi_polygon, list) and len(aoi_polygon) >= 3:
        try:
            ring_xy = []
            for p in aoi_polygon:
                if not isinstance(p, (list, tuple)) or len(p) < 2:
                    continue
                plat = float(p[0])
                plon = float(p[1])
                px, py = tr.transform(plon, plat)
                ring_xy.append((px, py))
            if len(ring_xy) >= 3:
                if ring_xy[0] != ring_xy[-1]:
                    ring_xy.append(ring_xy[0])
                geom = {"type": "Polygon", "coordinates": [[list(pt) for pt in ring_xy]]}
                aoi_mask = rio_features.rasterize(
                    [(geom, 1)],
                    out_shape=dem_arr.shape,
                    transform=transform,
                    fill=0,
                    dtype="uint8",
                    all_touched=False,
                )
                catch_mask = catch_mask & (aoi_mask == 1)
        except Exception:
            pass

    if not np.any(catch_mask):
        raise ValueError("Kein Einzugsgebiet gefunden (Punkt evtl. ausserhalb der Auswahl oder auf NoData).")

    area_m2 = float(np.sum(catch_mask) * pixel_area_m2)

    # Polygonize mask -> GeoJSON (in DEM CRS)
    shapes = rio_features.shapes(
        catch_mask.astype("uint8"),
        mask=catch_mask,
        transform=transform,
    )
    polys = []
    for geom, val in shapes:
        if val != 1:
            continue
        polys.append({"type": "Feature", "properties": {}, "geometry": geom})

    fc = {"type": "FeatureCollection", "features": polys}
    # Reproject to WGS84
    fc_wgs = _reproject_geojson(fc, str(src_crs))

    # Keep only the largest polygon (visual clarity).
    try:
        feats = fc_wgs.get("features") or []
        if len(feats) > 1:
            feats_sorted = sorted(
                feats,
                key=lambda f: abs(_signedRingAreaLonLat((f.get("geometry") or {}).get("coordinates", [[]])[0])),
                reverse=True,
            )
            fc_wgs["features"] = feats_sorted[:1]
    except Exception:
        pass

    return {
        "geojson": fc_wgs,
        "meta": {
            "area_m2": int(round(area_m2)),
            "area_ha": round(area_m2 / 10_000.0, 3),
            "area_km2": round(area_m2 / 1_000_000.0, 3),
        },
    }
