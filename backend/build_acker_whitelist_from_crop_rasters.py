from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any

import rasterio
from pyproj import Transformer


def _now_utc() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_rasters(raw: str) -> list[Path]:
    out: list[Path] = []
    for part in (raw or "").split(","):
        s = part.strip()
        if not s:
            continue
        if ":" in s:
            _, p = s.split(":", 1)
            s = p.strip()
        out.append(Path(s).resolve())
    return out


def _largest_ring_lonlat(geom: dict[str, Any]) -> list[list[float]] | None:
    gtype = str((geom or {}).get("type") or "")
    coords = (geom or {}).get("coordinates")
    if not gtype or not coords:
        return None
    if gtype == "Polygon":
        return coords[0] if coords else None
    if gtype == "MultiPolygon":
        best = None
        best_n = -1
        for poly in coords:
            if not poly:
                continue
            ring = poly[0]
            n = len(ring or [])
            if n > best_n:
                best_n = n
                best = ring
        return best
    return None


def _centroid_lonlat_from_ring(ring: list[list[float]]) -> tuple[float, float] | None:
    if not ring:
        return None
    xs = []
    ys = []
    for p in ring:
        if not isinstance(p, (list, tuple)) or len(p) < 2:
            continue
        xs.append(float(p[0]))
        ys.append(float(p[1]))
    if not xs or not ys:
        return None
    return ((min(xs) + max(xs)) * 0.5, (min(ys) + max(ys)) * 0.5)


def _field_id(props: dict[str, Any], idx: int) -> str:
    for k in ("schlag_id", "field_id", "flik", "FLURSTUECKSKENNZEICHEN", "id", "ID", "OBJECT_ID"):
        v = props.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return f"field_{idx:08d}"


def main() -> int:
    p = argparse.ArgumentParser(description="Build agrar/acker whitelist IDs from open crop rasters (centroid sampling).")
    p.add_argument(
        "--source-sqlite",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"),
    )
    p.add_argument(
        "--rasters",
        default=(
            "2024:data/raw/crop_history_open/downloads/17197830/CTM_GER_2024_rst_v302_COG.tif,"
            "2025:data/raw/crop_history_open/downloads/17182293/CTM_GER_2025_rst_v302_COG_2025_08.tif"
        ),
        help="Comma-separated list, optional year:path pairs.",
    )
    p.add_argument("--out-txt", default=str(Path("data") / "derived" / "whitelists" / "acker_ids.txt"))
    p.add_argument(
        "--out-meta",
        default=str(Path("data") / "derived" / "whitelists" / "acker_ids.meta.json"),
    )
    p.add_argument("--checkpoint-every", type=int, default=100000)
    args = p.parse_args()

    src = Path(args.source_sqlite).resolve()
    if not src.exists():
        raise SystemExit(f"sqlite missing: {src}")
    raster_paths = _parse_rasters(args.rasters)
    if not raster_paths:
        raise SystemExit("no rasters configured")
    for rp in raster_paths:
        if not rp.exists():
            raise SystemExit(f"raster missing: {rp}")

    out_txt = Path(args.out_txt).resolve()
    out_meta = Path(args.out_meta).resolve()
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_meta.parent.mkdir(parents=True, exist_ok=True)

    datasets = [rasterio.open(str(pth)) for pth in raster_paths]
    try:
        raster_crs = datasets[0].crs
        nodatas = [ds.nodata for ds in datasets]
        for ds in datasets[1:]:
            if str(ds.crs) != str(raster_crs):
                raise SystemExit("raster CRS mismatch")

        tr = Transformer.from_crs("EPSG:4326", raster_crs, always_xy=True)

        total = 0
        geom_missing = 0
        outside_raster = 0
        crop_positive = 0
        whitelist_ids: set[str] = set()

        with sqlite3.connect(str(src)) as conn:
            cur = conn.execute("SELECT feature_json FROM flurstuecke")
            for idx, (fj,) in enumerate(cur, start=1):
                total += 1
                try:
                    obj = json.loads(str(fj))
                    props = obj.get("properties") or {}
                    geom = obj.get("geometry") or {}
                except Exception:
                    geom_missing += 1
                    continue

                fid = _field_id(props, idx)
                ring = _largest_ring_lonlat(geom)
                c = _centroid_lonlat_from_ring(ring or [])
                if c is None:
                    geom_missing += 1
                    continue
                lon, lat = c
                x, y = tr.transform(lon, lat)

                has_crop = False
                in_any = False
                for ds, nd in zip(datasets, nodatas):
                    b = ds.bounds
                    if not (b.left <= x <= b.right and b.bottom <= y <= b.top):
                        continue
                    in_any = True
                    val = float(next(ds.sample([(x, y)]))[0])
                    if nd is not None and val == float(nd):
                        continue
                    if val > 0:
                        has_crop = True
                        break

                if not in_any:
                    outside_raster += 1
                    continue
                if has_crop:
                    whitelist_ids.add(fid)
                    crop_positive += 1

                if args.checkpoint_every > 0 and (idx % int(args.checkpoint_every) == 0):
                    print(
                        f"[{idx}] total={total} whitelist={len(whitelist_ids)} "
                        f"geom_missing={geom_missing} outside={outside_raster} crop_positive={crop_positive}"
                    )

        out_txt.write_text("\n".join(sorted(whitelist_ids)) + "\n", encoding="utf-8")
        meta = {
            "created_at_utc": _now_utc(),
            "source_sqlite": str(src),
            "rasters": [str(p) for p in raster_paths],
            "total_features": int(total),
            "whitelist_count": int(len(whitelist_ids)),
            "geom_missing": int(geom_missing),
            "outside_raster": int(outside_raster),
            "crop_positive_hits": int(crop_positive),
            "method": "centroid_sampling_open_crop_rasters",
        }
        out_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(f"[OK] whitelist={out_txt}")
        print(f"[OK] meta={out_meta}")
        print(f"[OK] count={len(whitelist_ids)}")
        return 0
    finally:
        for ds in datasets:
            ds.close()


if __name__ == "__main__":
    raise SystemExit(main())

