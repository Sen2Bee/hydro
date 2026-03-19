from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

from pyproj import Transformer


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build a spatially stratified sample from a filtered SA field pool."
    )
    p.add_argument(
        "--source-sqlite",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"),
    )
    p.add_argument(
        "--tile-plan",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "tile_plan.json"),
    )
    p.add_argument("--target-count", type=int, default=50000)
    p.add_argument("--grid-rows", type=int, default=20)
    p.add_argument("--grid-cols", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--min-field-area-ha", type=float, default=0.05)
    p.add_argument(
        "--field-id-whitelist-file",
        default=str(Path("data") / "derived" / "whitelists" / "acker_ids.txt"),
    )
    p.add_argument(
        "--out-geojson",
        default=str(Path("paper") / "input" / "schlaege_sa_spatial_filtered_50k.geojson"),
    )
    return p.parse_args()


def _load_bbox_from_tile_plan(path: Path) -> tuple[float, float, float, float]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    tiles = obj.get("tiles") or []
    if not tiles:
        raise RuntimeError(f"No tiles found in: {path}")
    west = min(float(t["west"]) for t in tiles)
    south = min(float(t["south"]) for t in tiles)
    east = max(float(t["east"]) for t in tiles)
    north = max(float(t["north"]) for t in tiles)
    return west, south, east, north


def _flatten_numbers(x: Any) -> list[float]:
    out: list[float] = []
    stack: list[Any] = [x]
    while stack:
        cur = stack.pop()
        if isinstance(cur, (int, float)):
            out.append(float(cur))
        elif isinstance(cur, list):
            stack.extend(cur)
    return out


def _centroid_from_geometry_bbox(geom: dict[str, Any]) -> tuple[float, float] | None:
    coords = geom.get("coordinates")
    if coords is None:
        return None
    nums = _flatten_numbers(coords)
    if len(nums) < 2:
        return None
    xs = nums[0::2]
    ys = nums[1::2]
    if not xs or not ys:
        return None
    return ((min(xs) + max(xs)) * 0.5, (min(ys) + max(ys)) * 0.5)


def _cell_idx(
    x: float,
    y: float,
    west: float,
    south: float,
    east: float,
    north: float,
    rows: int,
    cols: int,
) -> int:
    fx = (x - west) / max(1e-12, east - west)
    fy = (y - south) / max(1e-12, north - south)
    c = int(math.floor(fx * cols))
    r = int(math.floor(fy * rows))
    c = max(0, min(cols - 1, c))
    r = max(0, min(rows - 1, r))
    return r * cols + c


def _rank(seed: int, schlag_id: str) -> int:
    h = hashlib.blake2b(f"{seed}|{schlag_id}".encode("utf-8"), digest_size=8)
    return int.from_bytes(h.digest(), byteorder="big", signed=False)


def _largest_ring_lonlat(feature: dict[str, Any]) -> list[list[float]] | None:
    geom = (feature or {}).get("geometry") or {}
    gtype = str(geom.get("type") or "")
    coords = geom.get("coordinates")
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
                best = ring
                best_n = n
        return best
    return None


def _shoelace_area_m2_xy(xy: list[tuple[float, float]]) -> float:
    if len(xy) < 3:
        return 0.0
    area2 = 0.0
    for i in range(len(xy)):
        x1, y1 = xy[i]
        x2, y2 = xy[(i + 1) % len(xy)]
        area2 += (x1 * y2) - (x2 * y1)
    return abs(area2) * 0.5


def _field_area_ha(feature: dict[str, Any], tx: Transformer) -> float:
    ring = _largest_ring_lonlat(feature)
    if not ring or len(ring) < 3:
        return 0.0
    xy: list[tuple[float, float]] = []
    for p in ring:
        if not isinstance(p, (list, tuple)) or len(p) < 2:
            continue
        x, y = tx.transform(float(p[0]), float(p[1]))
        xy.append((float(x), float(y)))
    return _shoelace_area_m2_xy(xy) / 10000.0 if len(xy) >= 3 else 0.0


def _field_id(props: dict[str, Any]) -> str:
    for key in ("schlag_id", "field_id", "flik", "FLURSTUECKSKENNZEICHEN", "id", "ID", "OBJECT_ID"):
        val = props.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def _load_whitelist(path: Path) -> set[str]:
    return {line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()}


def main() -> int:
    args = _parse_args()
    src = Path(args.source_sqlite).resolve()
    tile_plan = Path(args.tile_plan).resolve()
    whitelist_path = Path(args.field_id_whitelist_file).resolve()
    out_geojson = Path(args.out_geojson).resolve()
    out_geojson.parent.mkdir(parents=True, exist_ok=True)
    out_meta = out_geojson.with_suffix(".meta.json")

    whitelist = _load_whitelist(whitelist_path)
    rows = max(1, int(args.grid_rows))
    cols = max(1, int(args.grid_cols))
    target = max(1, int(args.target_count))
    cell_count = rows * cols
    cap_per_cell = max(10, int(math.ceil((target / float(cell_count)) * 2.2)))

    tile_west, tile_south, tile_east, tile_north = _load_bbox_from_tile_plan(tile_plan)
    tx = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
    by_cell: list[list[tuple[int, dict[str, Any]]]] = [[] for _ in range(cell_count)]

    data_min_x: float | None = None
    data_min_y: float | None = None
    data_max_x: float | None = None
    data_max_y: float | None = None
    kept_rows = 0
    seen_rows = 0
    dropped_whitelist = 0
    dropped_area = 0
    invalid_geom = 0

    # Pass 1: bbox on filtered pool.
    with sqlite3.connect(str(src)) as conn:
        cur = conn.execute("SELECT feature_json FROM flurstuecke ORDER BY rowid")
        for (fj,) in cur:
            seen_rows += 1
            try:
                feat = json.loads(str(fj))
            except Exception:
                continue
            props = feat.get("properties") or {}
            fid = _field_id(props)
            if not fid or fid not in whitelist:
                dropped_whitelist += 1
                continue
            area_ha = _field_area_ha(feat, tx)
            if area_ha < float(args.min_field_area_ha):
                dropped_area += 1
                continue
            cxy = _centroid_from_geometry_bbox(feat.get("geometry") or {})
            if cxy is None:
                invalid_geom += 1
                continue
            cx, cy = cxy
            if data_min_x is None:
                data_min_x = data_max_x = cx
                data_min_y = data_max_y = cy
            else:
                data_min_x = min(data_min_x, cx)
                data_max_x = max(data_max_x, cx)
                data_min_y = min(data_min_y, cy)
                data_max_y = max(data_max_y, cy)
            kept_rows += 1

    if None in (data_min_x, data_min_y, data_max_x, data_max_y):
        raise SystemExit("No valid filtered features found.")

    west, south, east, north = data_min_x, data_min_y, data_max_x, data_max_y

    # Pass 2: filtered spatial selection.
    with sqlite3.connect(str(src)) as conn:
        cur = conn.execute("SELECT feature_json FROM flurstuecke ORDER BY rowid")
        for (fj,) in cur:
            try:
                feat = json.loads(str(fj))
            except Exception:
                continue
            props = feat.get("properties") or {}
            fid = _field_id(props)
            if not fid or fid not in whitelist:
                continue
            area_ha = _field_area_ha(feat, tx)
            if area_ha < float(args.min_field_area_ha):
                continue
            cxy = _centroid_from_geometry_bbox(feat.get("geometry") or {})
            if cxy is None:
                continue
            props["field_id"] = fid
            cell = _cell_idx(cxy[0], cxy[1], west, south, east, north, rows, cols)
            rv = _rank(int(args.seed), fid)
            cell_items = by_cell[cell]
            cell_items.append((rv, feat))
            if len(cell_items) > cap_per_cell:
                cell_items.sort(key=lambda t: t[0])
                del cell_items[cap_per_cell:]

    base = target // cell_count
    rem = target % cell_count
    selected: list[tuple[int, dict[str, Any], int]] = []
    leftovers: list[tuple[int, dict[str, Any], int]] = []

    for idx, cell in enumerate(by_cell):
        if not cell:
            continue
        cell.sort(key=lambda t: t[0])
        want = base + (1 if idx < rem else 0)
        take = min(want, len(cell))
        for i in range(take):
            rv, feat = cell[i]
            selected.append((rv, feat, idx))
        for i in range(take, len(cell)):
            rv, feat = cell[i]
            leftovers.append((rv, feat, idx))

    if len(selected) < target and leftovers:
        leftovers.sort(key=lambda t: t[0])
        selected.extend(leftovers[: target - len(selected)])

    selected.sort(key=lambda t: t[0])
    selected = selected[:target]
    features = [feat for _, feat, _ in selected]

    out_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False),
        encoding="utf-8",
    )

    meta = {
        "strategy": "spatial_grid_stratified_hash_filtered",
        "source_sqlite": str(src),
        "tile_plan": str(tile_plan),
        "whitelist_file": str(whitelist_path),
        "whitelist_count": len(whitelist),
        "min_field_area_ha": float(args.min_field_area_ha),
        "target_count": target,
        "selected_count": len(features),
        "grid_rows": rows,
        "grid_cols": cols,
        "grid_cells": cell_count,
        "tile_plan_bbox_wgs84": {
            "west": tile_west,
            "south": tile_south,
            "east": tile_east,
            "north": tile_north,
        },
        "filtered_centroid_bbox": {"west": west, "south": south, "east": east, "north": north},
        "seed": int(args.seed),
        "candidates_cap_per_cell": cap_per_cell,
        "scan_seen_rows": seen_rows,
        "filtered_rows_kept": kept_rows,
        "drop_whitelist": dropped_whitelist,
        "drop_area_min": dropped_area,
        "drop_geom": invalid_geom,
        "out_geojson": str(out_geojson),
    }
    out_meta.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[OK] GEOJSON: {out_geojson}")
    print(f"[OK] META:    {out_meta}")
    print(f"[INFO] selected={len(features)} target={target} kept_pool={kept_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
