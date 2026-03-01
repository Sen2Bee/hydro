from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Build a spatially stratified sample from SA flurstuecke SQLite. "
            "This avoids rowid-based sampling bias for paper maps/evaluation."
        )
    )
    p.add_argument(
        "--source-sqlite",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"),
        help="SQLite source with table flurstuecke(schlag_id, feature_json).",
    )
    p.add_argument(
        "--tile-plan",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "tile_plan.json"),
        help="Tile plan JSON to derive SA bbox.",
    )
    p.add_argument(
        "--target-count",
        type=int,
        default=50000,
        help="Target number of sampled fields.",
    )
    p.add_argument("--grid-rows", type=int, default=20, help="Grid rows for stratification.")
    p.add_argument("--grid-cols", type=int, default=20, help="Grid cols for stratification.")
    p.add_argument("--seed", type=int, default=42, help="Deterministic seed for hash ranking.")
    p.add_argument(
        "--out-geojson",
        default=str(Path("paper") / "input" / "schlaege_sa_spatial_50k.geojson"),
        help="Output sample GeoJSON path.",
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
    if east <= west or north <= south:
        return 0
    fx = (x - west) / (east - west)
    fy = (y - south) / (north - south)
    c = int(math.floor(fx * cols))
    r = int(math.floor(fy * rows))
    c = max(0, min(cols - 1, c))
    r = max(0, min(rows - 1, r))
    return r * cols + c


def _rank(seed: int, schlag_id: str) -> int:
    h = hashlib.blake2b(f"{seed}|{schlag_id}".encode("utf-8"), digest_size=8)
    return int.from_bytes(h.digest(), byteorder="big", signed=False)


def main() -> int:
    args = _parse_args()
    src = Path(args.source_sqlite).resolve()
    if not src.exists():
        raise SystemExit(f"SQLite not found: {src}")
    tile_plan = Path(args.tile_plan).resolve()
    if not tile_plan.exists():
        raise SystemExit(f"Tile plan not found: {tile_plan}")

    out_geojson = Path(args.out_geojson).resolve()
    out_geojson.parent.mkdir(parents=True, exist_ok=True)
    out_meta = out_geojson.with_suffix(".meta.json")

    tile_west, tile_south, tile_east, tile_north = _load_bbox_from_tile_plan(tile_plan)
    rows = max(1, int(args.grid_rows))
    cols = max(1, int(args.grid_cols))
    target = max(1, int(args.target_count))
    cell_count = rows * cols

    # Keep only top-N deterministic candidates per grid cell to bound memory.
    cap_per_cell = max(10, int(math.ceil((target / float(cell_count)) * 2.2)))
    by_cell: list[list[tuple[int, dict[str, Any]]]] = [[] for _ in range(cell_count)]
    seen = 0
    invalid_geom = 0

    # Pass 1: derive bbox from actual feature centroids (robust to CRS mismatch).
    data_min_x: float | None = None
    data_min_y: float | None = None
    data_max_x: float | None = None
    data_max_y: float | None = None
    pass1_valid = 0

    with sqlite3.connect(str(src)) as conn:
        cur = conn.execute("SELECT feature_json FROM flurstuecke ORDER BY rowid")
        for (fj,) in cur:
            try:
                feat = json.loads(str(fj))
            except Exception:
                continue
            geom = feat.get("geometry") or {}
            cxy = _centroid_from_geometry_bbox(geom)
            if cxy is None:
                continue
            cx, cy = cxy
            if data_min_x is None:
                data_min_x = cx
                data_max_x = cx
                data_min_y = cy
                data_max_y = cy
            else:
                data_min_x = min(data_min_x, cx)
                data_max_x = max(data_max_x, cx)
                data_min_y = min(data_min_y, cy)
                data_max_y = max(data_max_y, cy)
            pass1_valid += 1

    if (
        data_min_x is None
        or data_min_y is None
        or data_max_x is None
        or data_max_y is None
        or pass1_valid <= 0
    ):
        raise SystemExit("Could not derive centroid bbox from source data.")

    west, south, east, north = data_min_x, data_min_y, data_max_x, data_max_y

    # Pass 2: stratified selection.
    with sqlite3.connect(str(src)) as conn:
        cur = conn.execute("SELECT feature_json FROM flurstuecke ORDER BY rowid")
        for (fj,) in cur:
            seen += 1
            try:
                feat = json.loads(str(fj))
            except Exception:
                continue
            props = feat.get("properties") or {}
            schlag_id = str(props.get("schlag_id") or props.get("OBJECT_ID") or "")
            if not schlag_id:
                continue
            geom = feat.get("geometry") or {}
            cxy = _centroid_from_geometry_bbox(geom)
            if cxy is None:
                invalid_geom += 1
                continue
            cx, cy = cxy
            idx = _cell_idx(cx, cy, west, south, east, north, rows, cols)
            rv = _rank(int(args.seed), schlag_id)
            cell = by_cell[idx]
            cell.append((rv, feat))
            if len(cell) > cap_per_cell:
                cell.sort(key=lambda t: t[0])
                del cell[cap_per_cell:]

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
        need = target - len(selected)
        selected.extend(leftovers[:need])

    selected.sort(key=lambda t: t[0])
    selected = selected[:target]
    features = [f for _, f, _ in selected]

    fc = {"type": "FeatureCollection", "features": features}
    out_geojson.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")

    chosen_per_cell: dict[str, int] = {}
    for _, _, idx in selected:
        k = str(idx)
        chosen_per_cell[k] = chosen_per_cell.get(k, 0) + 1

    meta = {
        "strategy": "spatial_grid_stratified_hash",
        "source_sqlite": str(src),
        "tile_plan": str(tile_plan),
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
        "data_centroid_bbox": {"west": west, "south": south, "east": east, "north": north},
        "pass1_valid_centroids": pass1_valid,
        "seed": int(args.seed),
        "candidates_cap_per_cell": cap_per_cell,
        "scan_seen_rows": seen,
        "scan_invalid_geometry": invalid_geom,
        "non_empty_cells_selected": len(chosen_per_cell),
        "out_geojson": str(out_geojson),
    }
    out_meta.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[OK] GEOJSON: {out_geojson}")
    print(f"[OK] META:    {out_meta}")
    print(
        f"[INFO] selected={len(features)} target={target} "
        f"cells={len(chosen_per_cell)}/{cell_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
