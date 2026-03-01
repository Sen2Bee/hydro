from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import requests
from pyproj import Transformer
from rasterio.transform import from_origin
from rasterio.warp import Resampling, reproject

STAC_URL = "https://earth-search.aws.element84.com/v1/search"


def _pick_asset_href(assets: dict[str, Any], keys: list[str]) -> str | None:
    for k in keys:
        a = assets.get(k) or {}
        href = a.get("href")
        if isinstance(href, str) and href:
            return href
    return None


def _stac_best_item(west: float, south: float, east: float, north: float, start: str, end: str, max_cloud: float) -> dict[str, Any]:
    payload = {
        "collections": ["sentinel-2-l2a"],
        "bbox": [west, south, east, north],
        "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
        "query": {"eo:cloud_cover": {"lt": float(max_cloud)}},
        "limit": 5,
    }
    r = requests.post(STAC_URL, json=payload, timeout=60)
    r.raise_for_status()
    obj = r.json()
    feats = obj.get("features") or []
    if not feats:
        raise RuntimeError("Kein Sentinel-2 Item fuer AOI/Zeitraum gefunden.")
    # Some STAC deployments ignore/deny sort parameters. Select best cloud coverage locally.
    feats.sort(key=lambda f: float((f.get("properties") or {}).get("eo:cloud_cover", 1000.0)))
    return feats[0]


def _target_grid_utm32(west: float, south: float, east: float, north: float, res_m: float) -> tuple[tuple[float, float, float, float], rasterio.Affine, int, int]:
    tr = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
    minx, miny = tr.transform(west, south)
    maxx, maxy = tr.transform(east, north)
    left, right = min(minx, maxx), max(minx, maxx)
    bottom, top = min(miny, maxy), max(miny, maxy)
    width = max(1, int(np.ceil((right - left) / float(res_m))))
    height = max(1, int(np.ceil((top - bottom) / float(res_m))))
    transform = from_origin(left, top, float(res_m), float(res_m))
    return (left, bottom, right, top), transform, width, height


def _reproject_band_to_grid(src_href: str, dst_shape: tuple[int, int], dst_transform, dst_crs: str) -> np.ndarray:
    out = np.full(dst_shape, np.nan, dtype=np.float32)
    with rasterio.Env(AWS_NO_SIGN_REQUEST="YES"):
        with rasterio.open(src_href) as src:
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
    p = argparse.ArgumentParser(description="Fetch Sentinel-2 NDVI raster (EPSG:25832) for AOI.")
    p.add_argument("--west", type=float, required=True)
    p.add_argument("--south", type=float, required=True)
    p.add_argument("--east", type=float, required=True)
    p.add_argument("--north", type=float, required=True)
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--max-cloud", type=float, default=40.0)
    p.add_argument("--resolution-m", type=float, default=10.0)
    p.add_argument("--out-tif", default=str(Path("data") / "layers" / "st_mwl_erosion" / "NDVI_latest.tif"))
    args = p.parse_args()

    item = _stac_best_item(args.west, args.south, args.east, args.north, args.start, args.end, args.max_cloud)
    assets = item.get("assets") or {}
    red = _pick_asset_href(assets, ["red", "B04", "b04"])
    nir = _pick_asset_href(assets, ["nir", "B08", "b08"])
    if not red or not nir:
        raise RuntimeError("Sentinel-Item ohne B04/B08 Assets.")

    _, dst_transform, w, h = _target_grid_utm32(args.west, args.south, args.east, args.north, args.resolution_m)
    dst_shape = (h, w)
    red_arr = _reproject_band_to_grid(red, dst_shape, dst_transform, "EPSG:25832")
    nir_arr = _reproject_band_to_grid(nir, dst_shape, dst_transform, "EPSG:25832")

    denom = nir_arr + red_arr
    ndvi = np.where(np.isfinite(denom) & (np.abs(denom) > 1e-6), (nir_arr - red_arr) / denom, np.nan).astype(np.float32)
    ndvi = np.clip(ndvi, -1.0, 1.0)

    out = Path(args.out_tif)
    out.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": h,
        "width": w,
        "count": 1,
        "dtype": "float32",
        "crs": "EPSG:25832",
        "transform": dst_transform,
        "nodata": np.nan,
        "compress": "DEFLATE",
        "predictor": 2,
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
    }
    with rasterio.open(out, "w", **profile) as dst:
        dst.write(ndvi, 1)

    meta = {
        "query": {
            "west": float(args.west),
            "south": float(args.south),
            "east": float(args.east),
            "north": float(args.north),
            "start": str(args.start),
            "end": str(args.end),
            "max_cloud": float(args.max_cloud),
            "resolution_m": float(args.resolution_m),
        },
        "item_id": item.get("id"),
        "datetime": item.get("properties", {}).get("datetime"),
        "cloud_cover": item.get("properties", {}).get("eo:cloud_cover"),
        "red": red,
        "nir": nir,
        "out_tif": str(out),
    }
    (out.with_suffix(".json")).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[OK] NDVI -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
