from __future__ import annotations

import argparse
import gzip
import json
import math
import sqlite3
import time
from pathlib import Path
from typing import Any

import requests
from pyproj import Transformer

ALKIS_FLST_QUERY_URL = (
    "https://www.geodatenportal.sachsen-anhalt.de/arcgis/rest/services/"
    "Geobasisdaten/alkis_xtra_fme/FeatureServer/0/query"
)

# Sachsen-Anhalt extent from hosted Feldbloecke service (EPSG:25832), converted to WGS84 at runtime.
SA_EXTENT_25832 = {
    "xmin": 607711.1,
    "ymin": 5647977.1,
    "xmax": 787428.0,
    "ymax": 5879739.9,
}


def _extent_25832_to_wgs84(ext: dict[str, float]) -> tuple[float, float, float, float]:
    tr = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    w, s = tr.transform(ext["xmin"], ext["ymin"])
    e, n = tr.transform(ext["xmax"], ext["ymax"])
    return (float(w), float(s), float(e), float(n))


def _norm_id(props: dict[str, Any]) -> str:
    gnr = str(props.get("GEMARKUNGSNUMMER") or "").strip()
    flur = str(props.get("FLURNUMMER") or "").strip()
    z = str(props.get("FLURSTUECKSNUMMER_ZAEHLER") or "").strip()
    n = str(props.get("FLURSTUECKSNUMMER_NENNER") or "").strip()
    if gnr and flur and z:
        return f"{gnr}-{flur}-{z}{('/' + n) if n else ''}"
    flst = props.get("FLURSTUECKSKENNZEICHEN")
    if flst is not None and str(flst).strip():
        return str(flst).strip()
    oid = props.get("OBJECT_ID")
    return str(oid if oid is not None else "unknown")


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
        "outFields": (
            "OBJECT_ID,GEMARKUNG,GEMARKUNGSNUMMER,FLURNUMMER,"
            "FLURSTUECKSNUMMER_ZAEHLER,FLURSTUECKSNUMMER_NENNER,FLURSTUECKSKENNZEICHEN"
        ),
        "returnGeometry": "true",
        "geometryType": "esriGeometryEnvelope",
        "geometry": f"{west},{south},{east},{north}",
        "inSR": "4326",
        "outSR": "4326",
        "f": "geojson",
        "resultOffset": str(offset),
        "resultRecordCount": str(page_size),
    }
    r = requests.get(ALKIS_FLST_QUERY_URL, params=params, timeout=180)
    r.raise_for_status()
    return r.json()


def _fetch_page_retry(
    *,
    west: float,
    south: float,
    east: float,
    north: float,
    offset: int,
    page_size: int,
    retries: int,
) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return _fetch_page(
                west=west,
                south=south,
                east=east,
                north=north,
                offset=offset,
                page_size=page_size,
            )
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = min(30.0, 1.5 * attempt)
            print(f"  [retry {attempt}/{retries}] request failed: {exc}; sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"request failed after {retries} retries: {last_exc}")


def _tile_ranges(west: float, south: float, east: float, north: float, tile_size_deg: float) -> list[tuple[float, float, float, float]]:
    x_count = max(1, int(math.ceil((east - west) / tile_size_deg)))
    y_count = max(1, int(math.ceil((north - south) / tile_size_deg)))
    tiles: list[tuple[float, float, float, float]] = []
    for yi in range(y_count):
        ys = south + yi * tile_size_deg
        yn = min(north, ys + tile_size_deg)
        for xi in range(x_count):
            xw = west + xi * tile_size_deg
            xe = min(east, xw + tile_size_deg)
            tiles.append((xw, ys, xe, yn))
    return tiles


def _open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS flurstuecke (
            schlag_id TEXT PRIMARY KEY,
            feature_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tile_log (
            tile_idx INTEGER PRIMARY KEY,
            west REAL NOT NULL,
            south REAL NOT NULL,
            east REAL NOT NULL,
            north REAL NOT NULL,
            fetched_features INTEGER NOT NULL,
            fetched_pages INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _tile_already_done(conn: sqlite3.Connection, tile_idx: int) -> bool:
    cur = conn.execute(
        "SELECT fetched_pages FROM tile_log WHERE tile_idx = ? LIMIT 1",
        (tile_idx,),
    )
    row = cur.fetchone()
    if not row:
        return False
    try:
        return int(row[0]) > 0
    except Exception:
        return False


def _insert_feature(conn: sqlite3.Connection, schlag_id: str, feature: dict[str, Any]) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO flurstuecke (schlag_id, feature_json) VALUES (?, ?)",
        (schlag_id, json.dumps(feature, ensure_ascii=False)),
    )


def _export_geojson_from_db(conn: sqlite3.Connection, out_geojson: Path) -> int:
    out_geojson.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_geojson.open("w", encoding="utf-8") as f:
        f.write('{"type":"FeatureCollection","features":[')
        cur = conn.execute("SELECT feature_json FROM flurstuecke ORDER BY schlag_id")
        first = True
        for (feat_json,) in cur:
            if not first:
                f.write(",")
            f.write(feat_json)
            first = False
            count += 1
        f.write("]}")
    return count


def _gzip_copy(src: Path, dst_gz: Path) -> None:
    with src.open("rb") as fi, gzip.open(dst_gz, "wb", compresslevel=6) as fo:
        while True:
            chunk = fi.read(1024 * 1024)
            if not chunk:
                break
            fo.write(chunk)


def run(args: argparse.Namespace) -> int:
    if args.sa_default_extent:
        west, south, east, north = _extent_25832_to_wgs84(SA_EXTENT_25832)
    else:
        west, south, east, north = args.west, args.south, args.east, args.north
        if None in (west, south, east, north):
            raise RuntimeError("BBox fehlt. Entweder --use-sa-default-extent oder --west/--south/--east/--north setzen.")

    work_dir = Path(args.work_dir).resolve()
    out_geojson = Path(args.out_geojson).resolve()
    db_path = work_dir / "cache" / "flurstuecke.sqlite"
    meta_path = out_geojson.with_suffix(".meta.json")
    out_gz = out_geojson.with_suffix(".geojson.gz")
    tiles_file = work_dir / "tile_plan.json"
    tile_logs_dir = work_dir / "tiles"
    tile_logs_dir.mkdir(parents=True, exist_ok=True)

    tiles = _tile_ranges(west, south, east, north, args.tile_size_deg)
    tiles_file.write_text(
        json.dumps(
            {
                "bbox_wgs84": {"west": west, "south": south, "east": east, "north": north},
                "tile_size_deg": args.tile_size_deg,
                "tile_count": len(tiles),
                "tiles": [
                    {"tile_idx": i + 1, "west": t[0], "south": t[1], "east": t[2], "north": t[3]}
                    for i, t in enumerate(tiles)
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    conn = _open_db(db_path)
    total_pages = 0
    total_fetched = 0
    try:
        for i, (tw, ts, te, tn) in enumerate(tiles, start=1):
            if args.resume and _tile_already_done(conn, i):
                print(f"[tile {i}/{len(tiles)}] skip (already done)")
                continue
            print(f"[tile {i}/{len(tiles)}] bbox=({tw:.5f},{ts:.5f},{te:.5f},{tn:.5f})")
            offset = 0
            fetched_tile = 0
            pages_tile = 0
            tile_error: str | None = None
            while True:
                try:
                    data = _fetch_page_retry(
                        west=tw,
                        south=ts,
                        east=te,
                        north=tn,
                        offset=offset,
                        page_size=args.page_size,
                        retries=args.request_retries,
                    )
                except Exception as exc:
                    tile_error = str(exc)
                    print(f"  [WARN] tile failed and will be skipped: {tile_error}")
                    break
                feats = data.get("features") or []
                pages_tile += 1
                if not feats:
                    break

                for f in feats:
                    props = f.get("properties") or {}
                    props["schlag_id"] = _norm_id(props)
                    f["properties"] = props
                    _insert_feature(conn, str(props["schlag_id"]), f)
                conn.commit()

                fetched_tile += len(feats)
                total_fetched += len(feats)
                print(f"  page {pages_tile}: fetched {len(feats)} (tile_total={fetched_tile})")

                exceeded = bool(data.get("exceededTransferLimit"))
                if not exceeded:
                    break
                offset += len(feats)
                if fetched_tile >= args.max_records_per_tile:
                    print(f"  [WARN] tile max_records reached ({args.max_records_per_tile}), stop tile.")
                    break

            total_pages += pages_tile
            conn.execute(
                """
                INSERT OR REPLACE INTO tile_log (tile_idx, west, south, east, north, fetched_features, fetched_pages)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (i, tw, ts, te, tn, fetched_tile, pages_tile),
            )
            conn.commit()
            (tile_logs_dir / f"tile_{i:04d}.json").write_text(
                json.dumps(
                    {
                        "tile_idx": i,
                        "west": tw,
                        "south": ts,
                        "east": te,
                        "north": tn,
                        "fetched_features": fetched_tile,
                        "fetched_pages": pages_tile,
                        "error": tile_error,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

        unique_count = _export_geojson_from_db(conn, out_geojson)
        _gzip_copy(out_geojson, out_gz)
        meta = {
            "source": ALKIS_FLST_QUERY_URL,
            "bbox_wgs84": {"west": west, "south": south, "east": east, "north": north},
            "tile_size_deg": args.tile_size_deg,
            "tile_count": len(tiles),
            "page_size": args.page_size,
            "max_records_per_tile": args.max_records_per_tile,
            "fetched_feature_total_raw": total_fetched,
            "fetched_pages_total": total_pages,
            "unique_feature_count": unique_count,
            "sqlite_cache": str(db_path),
            "merged_geojson": str(out_geojson),
            "merged_geojson_gz": str(out_gz),
        }
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(f"[OK] unique features: {unique_count}")
        print(f"[OK] merged: {out_geojson}")
        print(f"[OK] gzip:   {out_gz}")
        print(f"[OK] meta:   {meta_path}")
        return 0
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Tiled fetch of SA ALKIS flurstueck polygons + dedup + merge.")
    p.add_argument(
        "--sa-default-extent",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use Sachsen-Anhalt default extent (disable with --no-sa-default-extent).",
    )
    p.add_argument("--west", type=float, default=None)
    p.add_argument("--south", type=float, default=None)
    p.add_argument("--east", type=float, default=None)
    p.add_argument("--north", type=float, default=None)
    p.add_argument("--tile-size-deg", type=float, default=0.12, help="Tile width/height in degrees.")
    p.add_argument("--page-size", type=int, default=2000)
    p.add_argument("--max-records-per-tile", type=int, default=200000)
    p.add_argument("--request-retries", type=int, default=3)
    p.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume by skipping tiles already logged in SQLite.",
    )
    p.add_argument("--work-dir", default=str(Path("data") / "raw" / "sa_flurstuecke"))
    p.add_argument("--out-geojson", default=str(Path("data") / "raw" / "sa_flurstuecke" / "sa_flurstuecke.geojson"))
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
