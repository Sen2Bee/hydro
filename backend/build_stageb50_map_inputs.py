from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from shapely.geometry import shape


def _field_id(props: dict, fallback_idx: int) -> str:
    for key in ("field_id", "schlag_id", "flik", "id", "ID", "FLURSTUECKSKENNZEICHEN"):
        val = props.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return f"field_{fallback_idx:06d}"


def _load_top10_ids(path: Path) -> set[str]:
    out: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            fid = str(row.get("field_id") or "").strip()
            if fid:
                out.add(fid)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Build merged field GeoJSON and matching Top-10 GeoJSON for Stage-B final figures.")
    p.add_argument("--chunks-dir", required=True)
    p.add_argument("--top10-csv", required=True)
    p.add_argument("--out-fields-geojson", required=True)
    p.add_argument("--out-top10-geojson", required=True)
    args = p.parse_args()

    chunks_dir = Path(args.chunks_dir).resolve()
    top10_csv = Path(args.top10_csv).resolve()
    out_fields = Path(args.out_fields_geojson).resolve()
    out_top10 = Path(args.out_top10_geojson).resolve()

    top10_ids = _load_top10_ids(top10_csv)
    merged_features = []
    top10_features = []

    chunk_files = sorted(chunks_dir.glob("schlaege_chunk_*.geojson"))
    for chunk in chunk_files:
        match = re.search(r"schlaege_chunk_(\d+)\.geojson$", chunk.name)
        chunk_id = int(match.group(1)) if match else None
        obj = json.loads(chunk.read_text(encoding="utf-8"))
        for idx, feat in enumerate(obj.get("features", []), start=1):
            props = feat.get("properties") or {}
            fid = _field_id(props, idx)
            props["field_id"] = fid
            if chunk_id is not None:
                props["chunk_id"] = chunk_id
            centroid = shape(feat.get("geometry")).centroid
            point_geom = {"type": "Point", "coordinates": [centroid.x, centroid.y]}
            merged_features.append({"type": "Feature", "geometry": point_geom, "properties": props})
            if fid in top10_ids:
                top10_features.append({"type": "Feature", "geometry": point_geom, "properties": props})

    out_fields.parent.mkdir(parents=True, exist_ok=True)
    out_top10.parent.mkdir(parents=True, exist_ok=True)

    out_fields.write_text(json.dumps({"type": "FeatureCollection", "features": merged_features}, ensure_ascii=False), encoding="utf-8")
    out_top10.write_text(json.dumps({"type": "FeatureCollection", "features": top10_features}, ensure_ascii=False), encoding="utf-8")

    print(f"[OK] fields_geojson: {out_fields} features={len(merged_features)}")
    print(f"[OK] top10_geojson: {out_top10} features={len(top10_features)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
