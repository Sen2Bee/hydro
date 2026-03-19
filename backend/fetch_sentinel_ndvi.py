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
from rasterio.windows import transform as window_transform
from rasterio.warp import Resampling, reproject

STAC_URL = "https://earth-search.aws.element84.com/v1/search"


def _pick_asset_href(assets: dict[str, Any], keys: list[str]) -> str | None:
    for k in keys:
        a = assets.get(k) or {}
        href = a.get("href")
        if isinstance(href, str) and href:
            return href
    return None


def _stac_items(
    west: float,
    south: float,
    east: float,
    north: float,
    start: str,
    end: str,
    max_cloud: float,
    limit: int,
) -> list[dict[str, Any]]:
    payload = {
        "collections": ["sentinel-2-l2a"],
        "bbox": [west, south, east, north],
        "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
        "query": {"eo:cloud_cover": {"lt": float(max_cloud)}},
        "limit": int(limit),
    }
    r = requests.post(STAC_URL, json=payload, timeout=60)
    r.raise_for_status()
    obj = r.json()
    feats = obj.get("features") or []
    if not feats:
        raise RuntimeError("Kein Sentinel-2 Item fuer AOI/Zeitraum gefunden.")
    # Some STAC deployments ignore/deny sort parameters. Select best cloud coverage locally.
    feats.sort(key=lambda f: float((f.get("properties") or {}).get("eo:cloud_cover", 1000.0)))
    out: list[dict[str, Any]] = []
    for f in feats:
        assets = f.get("assets") or {}
        red = _pick_asset_href(assets, ["red", "B04", "b04"])
        nir = _pick_asset_href(assets, ["nir", "B08", "b08"])
        if red and nir:
            out.append(
                {
                    "id": f.get("id"),
                    "datetime": (f.get("properties") or {}).get("datetime"),
                    "cloud_cover": (f.get("properties") or {}).get("eo:cloud_cover"),
                    "red": red,
                    "nir": nir,
                }
            )
    if not out:
        raise RuntimeError("Sentinel-Items ohne B04/B08 Assets.")
    return out


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


def _write_ndvi_tiled(
    scenes: list[dict[str, Any]],
    out_tif: Path,
    width: int,
    height: int,
    dst_transform,
    dst_crs: str,
) -> None:
    profile = {
        "driver": "GTiff",
        "height": int(height),
        "width": int(width),
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
    with rasterio.Env(AWS_NO_SIGN_REQUEST="YES"):
        sources = []
        try:
            for s in scenes:
                red_src = rasterio.open(str(s["red"]))
                nir_src = rasterio.open(str(s["nir"]))
                sources.append((red_src, nir_src))
            with rasterio.open(out_tif, "w", **profile) as dst:
                for _, win in dst.block_windows(1):
                    tile_h = int(win.height)
                    tile_w = int(win.width)
                    win_transform = window_transform(win, dst_transform)
                    red_stack = []
                    nir_stack = []
                    for red_src, nir_src in sources:
                        red_tile = np.full((tile_h, tile_w), np.nan, dtype=np.float32)
                        nir_tile = np.full((tile_h, tile_w), np.nan, dtype=np.float32)
                        reproject(
                            source=rasterio.band(red_src, 1),
                            destination=red_tile,
                            src_transform=red_src.transform,
                            src_crs=red_src.crs,
                            src_nodata=red_src.nodata,
                            dst_transform=win_transform,
                            dst_crs=dst_crs,
                            dst_nodata=np.nan,
                            resampling=Resampling.bilinear,
                        )
                        reproject(
                            source=rasterio.band(nir_src, 1),
                            destination=nir_tile,
                            src_transform=nir_src.transform,
                            src_crs=nir_src.crs,
                            src_nodata=nir_src.nodata,
                            dst_transform=win_transform,
                            dst_crs=dst_crs,
                            dst_nodata=np.nan,
                            resampling=Resampling.bilinear,
                        )
                        red_stack.append(red_tile)
                        nir_stack.append(nir_tile)
                    red_comp = np.nanmedian(np.stack(red_stack, axis=0), axis=0).astype(np.float32)
                    nir_comp = np.nanmedian(np.stack(nir_stack, axis=0), axis=0).astype(np.float32)
                    denom = nir_comp + red_comp
                    ndvi_tile = np.where(
                        np.isfinite(denom) & (np.abs(denom) > 1e-6),
                        (nir_comp - red_comp) / denom,
                        np.nan,
                    ).astype(np.float32)
                    ndvi_tile = np.clip(ndvi_tile, -1.0, 1.0)
                    dst.write(ndvi_tile, 1, window=win)
        finally:
            for red_src, nir_src in sources:
                try:
                    red_src.close()
                    nir_src.close()
                except Exception:
                    pass


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch Sentinel-2 NDVI raster (EPSG:25832) for AOI.")
    p.add_argument("--west", type=float, required=True)
    p.add_argument("--south", type=float, required=True)
    p.add_argument("--east", type=float, required=True)
    p.add_argument("--north", type=float, required=True)
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--max-cloud", type=float, default=40.0)
    p.add_argument("--stac-limit", type=int, default=12)
    p.add_argument("--resolution-m", type=float, default=10.0)
    p.add_argument("--out-tif", default=str(Path("data") / "layers" / "st_mwl_erosion" / "NDVI_latest.tif"))
    args = p.parse_args()

    scenes = _stac_items(
        args.west,
        args.south,
        args.east,
        args.north,
        args.start,
        args.end,
        args.max_cloud,
        args.stac_limit,
    )

    _, dst_transform, w, h = _target_grid_utm32(args.west, args.south, args.east, args.north, args.resolution_m)
    out = Path(args.out_tif)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_ndvi_tiled(
        scenes=scenes,
        out_tif=out,
        width=w,
        height=h,
        dst_transform=dst_transform,
        dst_crs="EPSG:25832",
    )

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
        "stac_limit": int(args.stac_limit),
        "scene_count": len(scenes),
        "scenes": [
            {
                "item_id": s.get("id"),
                "datetime": s.get("datetime"),
                "cloud_cover": s.get("cloud_cover"),
                "red": s.get("red"),
                "nir": s.get("nir"),
            }
            for s in scenes
        ],
        "out_tif": str(out),
    }
    (out.with_suffix(".json")).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[OK] NDVI -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
