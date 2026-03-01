from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np
import rasterio
import requests
from pyproj import Transformer
from rasterio import features as rio_features
from rasterio.fill import fillnodata
from rasterio.warp import Resampling, reproject

FELDBLOCK_URL = "https://www.geodatenportal.sachsen-anhalt.de/arcgisportal/rest/services/Hosted/Feldbloecke/FeatureServer/0/query"

# Simple, explicit baseline values by HBN group proxy.
HBN_TO_C_BASE = {
    "GL": 0.03,  # Gruenland
    "A": 0.20,   # Acker (generic)
}

DEFAULT_C = 0.20
DEFAULT_NDVI_A = 1.2
DEFAULT_NDVI_MIN = 0.5
DEFAULT_NDVI_MAX = 1.25
DEFAULT_C_MIN = 0.01
DEFAULT_C_MAX = 0.60


def _load_method_config(path: str | None) -> dict:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"C config not found: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _load_crop_history_map(path: str | None, crop_year: int | None) -> dict[str, str]:
    """
    Load optional crop history CSV with at least columns:
      - flik
      - crop_code (or crop)
      - year (optional if crop_year not used)
    Returns mapping flik -> crop_code for selected year (if provided).
    """
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"crop history CSV not found: {p}")
    out: dict[str, str] = {}
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        flik_col = cols.get("flik")
        crop_col = cols.get("crop_code") or cols.get("crop")
        year_col = cols.get("year")
        if not flik_col or not crop_col:
            raise RuntimeError("crop history CSV needs columns: flik and crop_code (or crop)")
        for row in r:
            flik = str(row.get(flik_col) or "").strip()
            crop = str(row.get(crop_col) or "").strip()
            if not flik or not crop:
                continue
            if crop_year is not None and year_col:
                y_raw = str(row.get(year_col) or "").strip()
                if not y_raw:
                    continue
                try:
                    y = int(y_raw)
                except ValueError:
                    continue
                if y != int(crop_year):
                    continue
            out[flik] = crop.upper()
    return out


def _hbn_to_c(hbn: str | None, hbn_map: dict[str, float], default_c: float) -> float:
    if not hbn:
        return default_c
    x = str(hbn).strip().upper()
    if x in hbn_map:
        return float(hbn_map[x])
    if x.startswith("GL"):
        return float(hbn_map.get("GL", 0.03))
    if x.startswith("A"):
        return float(hbn_map.get("A", 0.20))
    return default_c


def _crop_to_c(crop_code: str | None, crop_map: dict[str, float], default_c: float) -> float:
    if not crop_code:
        return default_c
    c = str(crop_code).strip().upper()
    if not c:
        return default_c
    return float(crop_map.get(c, default_c))


def _fetch_points_utm32(west: float, south: float, east: float, north: float, page_size: int = 2000) -> list[dict]:
    tr = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
    minx, miny = tr.transform(west, south)
    maxx, maxy = tr.transform(east, north)
    xmin, xmax = min(minx, maxx), max(minx, maxx)
    ymin, ymax = min(miny, maxy), max(miny, maxy)

    all_feats: list[dict] = []
    offset = 0
    while True:
        params = {
            "where": "1=1",
            "outFields": "flik,hbn_kurz,fl_netto",
            "returnGeometry": "true",
            "geometryType": "esriGeometryEnvelope",
            "geometry": f"{xmin},{ymin},{xmax},{ymax}",
            "inSR": "25832",
            "outSR": "25832",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
        }
        r = requests.get(FELDBLOCK_URL, params=params, timeout=60)
        r.raise_for_status()
        obj = r.json()
        feats = obj.get("features") or []
        all_feats.extend(feats)
        if len(feats) < page_size:
            break
        offset += page_size
        if offset > 200000:
            break
    return all_feats


def _load_template(path: str) -> tuple[tuple[int, int], rasterio.Affine, str]:
    with rasterio.open(path) as src:
        return (int(src.height), int(src.width)), src.transform, str(src.crs)


def _resample_ndvi_to_template(ndvi_path: str | None, dst_shape: tuple[int, int], dst_transform, dst_crs: str) -> np.ndarray | None:
    if not ndvi_path:
        return None
    p = Path(ndvi_path)
    if not p.exists():
        return None
    out = np.full(dst_shape, np.nan, dtype=np.float32)
    with rasterio.open(p) as src:
        src_arr = src.read(1).astype(np.float32)
        nodata = src.nodata
        if nodata is not None:
            src_arr[src_arr == nodata] = np.nan
        reproject(
            source=src_arr,
            destination=out,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=np.nan,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            dst_nodata=np.nan,
            resampling=Resampling.bilinear,
        )
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Build C-factor proxy raster from Feldblock points + optional NDVI.")
    p.add_argument("--west", type=float, required=True)
    p.add_argument("--south", type=float, required=True)
    p.add_argument("--east", type=float, required=True)
    p.add_argument("--north", type=float, required=True)
    p.add_argument("--template-raster", default=str(Path("data") / "layers" / "st_mwl_erosion" / "K_Faktor.tif"))
    p.add_argument("--ndvi-raster", default=str(Path("data") / "layers" / "st_mwl_erosion" / "NDVI_latest.tif"))
    p.add_argument("--out-tif", default=str(Path("data") / "layers" / "st_mwl_erosion" / "C_Faktor_proxy.tif"))
    p.add_argument("--c-config", default=str(Path("data") / "config" / "c_factor_method_v1.json"))
    p.add_argument("--season-label", default=None, help="Optional season/window label for metadata.")
    p.add_argument("--crop-history-csv", default=None, help="Optional CSV (flik,crop_code[,year]) for crop-based C.")
    p.add_argument("--crop-year", type=int, default=None, help="Optional year filter for crop history.")
    args = p.parse_args()

    cfg = _load_method_config(args.c_config)
    hbn_map = dict(HBN_TO_C_BASE)
    hbn_map.update({str(k).upper(): float(v) for k, v in (cfg.get("hbn_to_c_base") or {}).items()})
    default_c = float(cfg.get("default_c", DEFAULT_C))
    ndvi_cfg = cfg.get("ndvi_modifier") or {}
    ndvi_a = float(ndvi_cfg.get("a", DEFAULT_NDVI_A))
    ndvi_min = float(ndvi_cfg.get("min", DEFAULT_NDVI_MIN))
    ndvi_max = float(ndvi_cfg.get("max", DEFAULT_NDVI_MAX))
    c_clip_cfg = cfg.get("c_clip") or {}
    c_min = float(c_clip_cfg.get("min", DEFAULT_C_MIN))
    c_max = float(c_clip_cfg.get("max", DEFAULT_C_MAX))
    crop_map = {str(k).upper(): float(v) for k, v in (cfg.get("crop_to_c_base") or {}).items()}
    crop_hist = _load_crop_history_map(args.crop_history_csv, args.crop_year)
    crop_hits = 0

    dst_shape, dst_transform, dst_crs = _load_template(args.template_raster)
    feats = _fetch_points_utm32(args.west, args.south, args.east, args.north)
    if not feats:
        raise RuntimeError("Keine Feldblockpunkte fuer AOI erhalten.")

    seeds = []
    for f in feats:
        geom = f.get("geometry") or {}
        props = f.get("properties") or {}
        coords = geom.get("coordinates") or []
        if len(coords) < 2:
            continue
        x, y = float(coords[0]), float(coords[1])
        flik = str(props.get("flik") or "").strip()
        if flik and flik in crop_hist and crop_map:
            c0 = _crop_to_c(crop_hist.get(flik), crop_map=crop_map, default_c=default_c)
            crop_hits += 1
        else:
            c0 = _hbn_to_c(props.get("hbn_kurz"), hbn_map=hbn_map, default_c=default_c)
        seeds.append(({"type": "Point", "coordinates": [x, y]}, float(c0)))
    if not seeds:
        raise RuntimeError("Keine gueltigen Punktgeometrien fuer C-Seed.")

    c_seed = rio_features.rasterize(
        seeds,
        out_shape=dst_shape,
        transform=dst_transform,
        fill=np.nan,
        dtype="float32",
    )
    # Interpolate sparse point seeds across AOI.
    c_base = fillnodata(c_seed, mask=np.isfinite(c_seed), max_search_distance=4096, smoothing_iterations=0).astype(np.float32)
    c_base = np.clip(c_base, c_min, min(c_max, 0.45))

    ndvi = _resample_ndvi_to_template(args.ndvi_raster, dst_shape, dst_transform, dst_crs)
    if ndvi is not None:
        # Higher NDVI -> lower C.
        modifier = np.clip(ndvi_a - ndvi, ndvi_min, ndvi_max)
        c_final = np.clip(c_base * modifier, c_min, c_max)
        c_mode = "hbn_points_plus_ndvi"
    else:
        c_final = c_base
        c_mode = "hbn_points_only"

    out = Path(args.out_tif)
    out.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": dst_shape[0],
        "width": dst_shape[1],
        "count": 1,
        "dtype": "float32",
        "crs": dst_crs,
        "transform": dst_transform,
        "nodata": np.nan,
        "compress": "DEFLATE",
        "predictor": 2,
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
    }
    with rasterio.open(out, "w", **profile) as dst:
        dst.write(c_final.astype(np.float32), 1)

    meta = {
        "mode": c_mode,
        "season_label": args.season_label,
        "method_version": str(cfg.get("method_version", "c_proxy_v1")),
        "method_source": str(args.c_config) if Path(args.c_config).exists() else None,
        "method_params": {
            "hbn_to_c_base": hbn_map,
            "crop_to_c_base": crop_map if crop_map else None,
            "default_c": default_c,
            "ndvi_modifier": {"a": ndvi_a, "min": ndvi_min, "max": ndvi_max},
            "c_clip": {"min": c_min, "max": c_max},
        },
        "query": {
            "west": float(args.west),
            "south": float(args.south),
            "east": float(args.east),
            "north": float(args.north),
        },
        "source_feldblock": FELDBLOCK_URL,
        "point_count": len(seeds),
        "crop_history_csv": args.crop_history_csv,
        "crop_year": args.crop_year,
        "crop_history_matches": int(crop_hits),
        "template_raster": args.template_raster,
        "ndvi_raster": args.ndvi_raster if ndvi is not None else None,
        "out_tif": str(out),
    }
    (out.with_suffix(".json")).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[OK] C factor -> {out} ({c_mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
