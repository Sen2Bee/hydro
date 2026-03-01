from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio
from pyproj import Transformer
from rasterio import features as rio_features
from rasterio import windows as rio_windows
from rasterio.warp import transform_geom
import requests


FELDBLOCK_URL = "https://www.geodatenportal.sachsen-anhalt.de/arcgisportal/rest/services/Hosted/Feldbloecke/FeatureServer/0/query"


def _fetch_feldblock_features(page_size: int = 2000) -> list[dict]:
    out: list[dict] = []
    offset = 0
    while True:
        params = {
            "where": "1=1",
            "outFields": "flik",
            "returnGeometry": "true",
            "outSR": "25832",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
        }
        r = requests.get(FELDBLOCK_URL, params=params, timeout=120)
        r.raise_for_status()
        obj = r.json()
        feats = obj.get("features") or []
        out.extend(feats)
        print(f"[feldblock] offset={offset} fetched={len(feats)} total={len(out)}")
        if len(feats) < page_size:
            break
        offset += page_size
        if offset > 500000:
            break
    return out


def _majority_crop_code(ds: rasterio.io.DatasetReader, geom_utm32: dict) -> int | None:
    geom = transform_geom("EPSG:25832", ds.crs, geom_utm32, precision=6)
    try:
        win = rio_features.geometry_window(ds, [geom], pad_x=0, pad_y=0)
    except Exception:
        return None
    if win.width <= 0 or win.height <= 0:
        return None
    arr = ds.read(1, window=win, masked=False)
    if arr.size == 0:
        return None

    wtransform = ds.window_transform(win)
    mask = rio_features.rasterize(
        [(geom, 1)],
        out_shape=(int(win.height), int(win.width)),
        transform=wtransform,
        fill=0,
        dtype="uint8",
        all_touched=False,
    )
    inside = arr[mask == 1]
    if inside.size == 0:
        return None
    nodata = ds.nodata
    if nodata is not None:
        inside = inside[inside != nodata]
    inside = inside[np.isfinite(inside)]
    inside = inside[inside > 0]
    if inside.size == 0:
        return None
    vals, cnt = np.unique(inside.astype(np.int64), return_counts=True)
    idx = int(np.argmax(cnt))
    return int(vals[idx])


def main() -> int:
    p = argparse.ArgumentParser(description="Build crop_history.csv (flik,crop_code,year) from open crop rasters.")
    p.add_argument(
        "--rasters",
        default=(
            "2024:data/raw/crop_history_open/downloads/17197830/CTM_GER_2024_rst_v302_COG.tif,"
            "2025:data/raw/crop_history_open/downloads/17182293/CTM_GER_2025_rst_v302_COG_2025_08.tif"
        ),
        help="Comma-separated year:path pairs.",
    )
    p.add_argument("--out-csv", default="data/derived/crop_history/crop_history.csv")
    p.add_argument("--out-meta", default="data/derived/crop_history/crop_history.meta.json")
    p.add_argument("--page-size", type=int, default=2000)
    args = p.parse_args()

    raster_pairs: list[tuple[int, Path]] = []
    for part in (args.rasters or "").split(","):
        s = part.strip()
        if not s:
            continue
        if ":" not in s:
            raise RuntimeError(f"invalid raster pair: {s}")
        y, rp = s.split(":", 1)
        year = int(y.strip())
        path = Path(rp.strip())
        if not path.exists():
            raise RuntimeError(f"raster missing for year {year}: {path}")
        raster_pairs.append((year, path))
    if not raster_pairs:
        raise RuntimeError("no rasters configured")

    features = _fetch_feldblock_features(page_size=args.page_size)
    if not features:
        raise RuntimeError("no feldblock features fetched")

    rows: list[tuple[str, int, int]] = []
    per_year_stats: dict[int, dict[str, int]] = {}

    for year, rpath in raster_pairs:
        print(f"[year={year}] raster={rpath}")
        with rasterio.open(rpath) as ds:
            hit = 0
            miss = 0
            for i, feat in enumerate(features, start=1):
                props = feat.get("properties") or {}
                geom = feat.get("geometry")
                flik = str(props.get("flik") or "").strip()
                if not flik or not geom:
                    miss += 1
                    continue
                code = _majority_crop_code(ds, geom)
                if code is None:
                    miss += 1
                    continue
                rows.append((flik, int(code), int(year)))
                hit += 1
                if i % 5000 == 0:
                    print(f"  [year={year}] processed={i}/{len(features)} hits={hit} miss={miss}")
            per_year_stats[year] = {"hits": hit, "miss": miss}

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["flik", "crop_code", "year"])
        for flik, code, year in rows:
            w.writerow([flik, code, year])

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "feldblock_count": len(features),
        "rasters": [{"year": y, "path": str(p)} for y, p in raster_pairs],
        "rows_written": len(rows),
        "per_year_stats": per_year_stats,
        "out_csv": str(out_csv),
    }
    out_meta = Path(args.out_meta)
    out_meta.parent.mkdir(parents=True, exist_ok=True)
    out_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[OK] csv={out_csv}")
    print(f"[OK] meta={out_meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

