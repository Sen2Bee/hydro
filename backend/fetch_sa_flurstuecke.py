from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import requests

ALKIS_FLST_QUERY_URL = (
    "https://www.geodatenportal.sachsen-anhalt.de/arcgis/rest/services/"
    "Geobasisdaten/alkis_xtra_fme/FeatureServer/0/query"
)


def _fetch_page(
    *,
    west: float,
    south: float,
    east: float,
    north: float,
    offset: int,
    page_size: int,
) -> dict[str, Any]:
    params = {
        "where": "1=1",
        "outFields": "OBJECT_ID,GEMARKUNG,GEMARKUNGSNUMMER,FLURNUMMER,FLURSTUECKSNUMMER_ZAEHLER,FLURSTUECKSNUMMER_NENNER,FLURSTUECKSKENNZEICHEN",
        "returnGeometry": "true",
        "geometryType": "esriGeometryEnvelope",
        "geometry": f"{west},{south},{east},{north}",
        "inSR": "4326",
        "outSR": "4326",
        "f": "geojson",
        "resultOffset": str(offset),
        "resultRecordCount": str(page_size),
    }
    r = requests.get(ALKIS_FLST_QUERY_URL, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def _norm_id(props: dict[str, Any]) -> str:
    gnr = str(props.get("GEMARKUNGSNUMMER") or "").strip()
    flur = str(props.get("FLURNUMMER") or "").strip()
    z = str(props.get("FLURSTUECKSNUMMER_ZAEHLER") or "").strip()
    n = str(props.get("FLURSTUECKSNUMMER_NENNER") or "").strip()
    if gnr and flur and z:
        return f"{gnr}-{flur}-{z}{('/' + n) if n else ''}"
    fid = props.get("FLURSTUECKSKENNZEICHEN")
    if fid is not None and str(fid).strip():
        return str(fid).strip()
    oid = props.get("OBJECT_ID")
    return str(oid if oid is not None else "unknown")


def run(args: argparse.Namespace) -> int:
    out = Path(args.out_geojson).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    all_features: list[dict[str, Any]] = []
    offset = 0
    page = 0
    while True:
        page += 1
        data = _fetch_page(
            west=args.west,
            south=args.south,
            east=args.east,
            north=args.north,
            offset=offset,
            page_size=args.page_size,
        )
        feats = data.get("features") or []
        if not feats:
            break

        for f in feats:
            props = f.get("properties") or {}
            props["schlag_id"] = _norm_id(props)
            f["properties"] = props
        all_features.extend(feats)

        print(f"[page {page}] fetched {len(feats)} (total={len(all_features)})")
        exceeded = bool(data.get("exceededTransferLimit"))
        if not exceeded:
            break
        offset += len(feats)
        if len(all_features) >= int(args.max_records):
            print(f"[WARN] max_records reached ({args.max_records}), stop paging.")
            break

    if not all_features:
        raise RuntimeError("Keine Flurstueck-Polygone fuer AOI erhalten.")

    fc = {"type": "FeatureCollection", "features": all_features}
    out.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
    meta = {
        "source": ALKIS_FLST_QUERY_URL,
        "bbox_wgs84": {
            "west": float(args.west),
            "south": float(args.south),
            "east": float(args.east),
            "north": float(args.north),
        },
        "feature_count": len(all_features),
    }
    out.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[OK] Flurstuecke GeoJSON: {out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch SA ALKIS Flurstueck polygons for AOI as GeoJSON.")
    p.add_argument("--west", type=float, required=True)
    p.add_argument("--south", type=float, required=True)
    p.add_argument("--east", type=float, required=True)
    p.add_argument("--north", type=float, required=True)
    p.add_argument("--out-geojson", default=str(Path("paper") / "input" / "schlaege.geojson"))
    p.add_argument("--page-size", type=int, default=2000)
    p.add_argument("--max-records", type=int, default=100000)
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
