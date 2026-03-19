from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Split a spatially stratified sample GeoJSON into balanced chunk GeoJSONs."
    )
    p.add_argument("--sample-geojson", required=True)
    p.add_argument("--tile-plan", required=True)
    p.add_argument("--chunks-dir", required=True)
    p.add_argument("--chunk-size", type=int, default=1000)
    p.add_argument("--grid-rows", type=int, default=20)
    p.add_argument("--grid-cols", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def _load_bbox_from_tile_plan(path: Path) -> tuple[float, float, float, float]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    bbox = obj.get("bbox_wgs84") or {}
    return (
        float(bbox["west"]),
        float(bbox["south"]),
        float(bbox["east"]),
        float(bbox["north"]),
    )


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
    fx = (x - west) / max(1e-12, (east - west))
    fy = (y - south) / max(1e-12, (north - south))
    c = int(math.floor(fx * cols))
    r = int(math.floor(fy * rows))
    c = max(0, min(cols - 1, c))
    r = max(0, min(rows - 1, r))
    return r * cols + c


def _field_id(props: dict[str, Any], fallback_idx: int) -> str:
    for key in ("field_id", "schlag_id", "flik", "FLURSTUECKSKENNZEICHEN", "id", "ID", "OBJECT_ID"):
        val = props.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return f"field_{fallback_idx:06d}"


def _rank(seed: int, value: str) -> int:
    h = hashlib.blake2b(f"{seed}|{value}".encode("utf-8"), digest_size=8)
    return int.from_bytes(h.digest(), byteorder="big", signed=False)


def main() -> int:
    args = _parse_args()
    sample_geojson = Path(args.sample_geojson).resolve()
    tile_plan = Path(args.tile_plan).resolve()
    out_dir = Path(args.chunks_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    obj = json.loads(sample_geojson.read_text(encoding="utf-8"))
    features = list(obj.get("features", []))
    if not features:
        raise SystemExit("No features in sample GeoJSON.")

    west, south, east, north = _load_bbox_from_tile_plan(tile_plan)
    rows = max(1, int(args.grid_rows))
    cols = max(1, int(args.grid_cols))
    chunk_size = max(1, int(args.chunk_size))

    by_cell: dict[int, list[tuple[int, dict[str, Any]]]] = {}
    for idx, feat in enumerate(features, start=1):
        geom = feat.get("geometry") or {}
        cxy = _centroid_from_geometry_bbox(geom)
        if cxy is None:
            continue
        props = feat.get("properties") or {}
        fid = _field_id(props, idx)
        cell = _cell_idx(cxy[0], cxy[1], west, south, east, north, rows, cols)
        props["field_id"] = fid
        props["sample_cell_id"] = cell
        item = (_rank(int(args.seed), fid), feat)
        by_cell.setdefault(cell, []).append(item)

    for cell_items in by_cell.values():
        cell_items.sort(key=lambda t: t[0])

    cell_order = sorted(by_cell.keys(), key=lambda c: _rank(int(args.seed), f"cell:{c}"))
    ordered: list[dict[str, Any]] = []
    while True:
        progressed = False
        for cell in cell_order:
            cell_items = by_cell.get(cell) or []
            if not cell_items:
                continue
            _, feat = cell_items.pop(0)
            ordered.append(feat)
            progressed = True
        if not progressed:
            break

    total_chunks = int(math.ceil(len(ordered) / float(chunk_size)))
    for chunk_idx in range(1, total_chunks + 1):
        start = (chunk_idx - 1) * chunk_size
        end = min(len(ordered), start + chunk_size)
        chunk_features = ordered[start:end]
        out_geo = out_dir / f"schlaege_chunk_{chunk_idx:05d}.geojson"
        out_geo.write_text(
            json.dumps({"type": "FeatureCollection", "features": chunk_features}, ensure_ascii=False),
            encoding="utf-8",
        )

    manifest = {
        "strategy": "spatial_sample_round_robin_chunks",
        "sample_geojson": str(sample_geojson),
        "tile_plan": str(tile_plan),
        "grid_rows": rows,
        "grid_cols": cols,
        "chunk_size": chunk_size,
        "selected_features": len(ordered),
        "total_chunks": total_chunks,
        "chunks_dir": str(out_dir),
    }
    (out_dir / "chunk_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] chunks_dir={out_dir} chunks={total_chunks} features={len(ordered)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
