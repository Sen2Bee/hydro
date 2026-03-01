from __future__ import annotations

import argparse
import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from pyproj import Transformer
import rasterio

DEFAULT_WMS_URL = "https://www.geodatenportal.sachsen-anhalt.de/wss-org1/service/ST_MWL_Erosion/guest"
DEFAULT_LAYERS = ["K-Faktor", "R-Faktor", "S-Faktor", "Wasser_Erosion", "Wind_Erosion"]


def _parse_layers_and_formats(wms_url: str, timeout_s: int = 45) -> tuple[set[str], set[str]]:
    sep = "&" if "?" in wms_url else "?"
    cap_url = f"{wms_url}{sep}SERVICE=WMS&REQUEST=GetCapabilities"
    resp = requests.get(cap_url, timeout=timeout_s)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    ns = {"wms": "http://www.opengis.net/wms"}
    names = set()
    for el in root.findall(".//wms:Layer/wms:Name", ns):
        if el.text and el.text.strip():
            names.add(el.text.strip())

    fmts = set()
    for el in root.findall(".//wms:GetMap//wms:Format", ns):
        if el.text and el.text.strip():
            fmts.add(el.text.strip())
    return names, fmts


def _choose_format(formats: set[str]) -> str:
    prefs = ["image/tiff", "image/geotiff", "image/geotiff8", "image/png"]
    lower_map = {f.lower(): f for f in formats}
    for p in prefs:
        if p in lower_map:
            return lower_map[p]
    if not formats:
        raise RuntimeError("WMS liefert keine GetMap-Formate.")
    return sorted(formats)[0]


def _to_utm32_bbox(west: float, south: float, east: float, north: float) -> tuple[float, float, float, float]:
    tr = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
    minx, miny = tr.transform(float(west), float(south))
    maxx, maxy = tr.transform(float(east), float(north))
    return min(minx, maxx), min(miny, maxy), max(minx, maxx), max(miny, maxy)


def _fetch_layer(
    *,
    wms_url: str,
    layer_name: str,
    out_path: Path,
    bbox_utm32: tuple[float, float, float, float],
    width: int,
    height: int,
    fmt: str,
    timeout_s: int = 120,
) -> None:
    minx, miny, maxx, maxy = bbox_utm32
    params = {
        "SERVICE": "WMS",
        "REQUEST": "GetMap",
        "VERSION": "1.3.0",
        "LAYERS": layer_name,
        "STYLES": "",
        "CRS": "EPSG:25832",
        "BBOX": f"{minx},{miny},{maxx},{maxy}",
        "WIDTH": str(int(width)),
        "HEIGHT": str(int(height)),
        "FORMAT": fmt,
        "TRANSPARENT": "FALSE",
    }
    resp = requests.get(wms_url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)


def _sanitize_layer_name(name: str) -> str:
    return (
        name.replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("-", "_")
    )


def _cache_key(*, wms_url: str, bbox_utm32: tuple[float, float, float, float], width: int, height: int, fmt: str, layers: list[str]) -> str:
    payload = {
        "wms_url": wms_url,
        "bbox_utm32": [round(float(x), 3) for x in bbox_utm32],
        "width": int(width),
        "height": int(height),
        "format": fmt,
        "layers": list(layers),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _optimize_geotiff(src_path: Path, dst_path: Path) -> None:
    with rasterio.open(src_path) as src:
        profile = src.profile.copy()
        # Compact + fast random-read profile for raster processing.
        profile.update(
            compress="DEFLATE",
            predictor=2 if src.dtypes[0] != "uint8" else 1,
            tiled=True,
            blockxsize=512,
            blockysize=512,
            BIGTIFF="IF_SAFER",
        )
        with rasterio.open(dst_path, "w", **profile) as dst:
            for b in range(1, src.count + 1):
                dst.write(src.read(b), b)
            if src.colorinterp:
                dst.colorinterp = src.colorinterp
            try:
                dst.build_overviews([2, 4, 8, 16], rasterio.enums.Resampling.average)
                dst.update_tags(ns="rio_overview", resampling="average")
            except Exception:
                # Overviews are optional for this workflow.
                pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch ABAG-relevant layers from ST_MWL_Erosion WMS into local TIFF files."
    )
    parser.add_argument("--wms-url", default=DEFAULT_WMS_URL)
    parser.add_argument("--west", type=float, required=True, help="BBox west (EPSG:4326)")
    parser.add_argument("--south", type=float, required=True, help="BBox south (EPSG:4326)")
    parser.add_argument("--east", type=float, required=True, help="BBox east (EPSG:4326)")
    parser.add_argument("--north", type=float, required=True, help="BBox north (EPSG:4326)")
    parser.add_argument("--width", type=int, default=2048)
    parser.add_argument("--height", type=int, default=2048)
    parser.add_argument("--layers", default=",".join(DEFAULT_LAYERS), help="Comma-separated WMS layer names")
    parser.add_argument("--out-dir", default=str(Path("data") / "layers" / "st_mwl_erosion"))
    parser.add_argument("--cache-dir", default=str(Path("data") / "layers" / "st_mwl_erosion" / "_cache"))
    parser.add_argument("--force", action="store_true", help="Force re-download even if cached files exist.")
    parser.add_argument(
        "--optimize",
        choices=["auto", "off"],
        default="auto",
        help="auto: write compressed/tiled GeoTIFF (default), off: keep raw response.",
    )
    args = parser.parse_args()

    names, formats = _parse_layers_and_formats(args.wms_url)
    fmt = _choose_format(formats)
    wanted = [x.strip() for x in str(args.layers).split(",") if x.strip()]
    missing = [x for x in wanted if x not in names]
    if missing:
        raise RuntimeError(f"Unbekannte Layer: {missing}. Verfuegbar sind z.B.: {sorted(names)[:20]}")

    bbox_utm = _to_utm32_bbox(args.west, args.south, args.east, args.north)
    out_dir = Path(args.out_dir)
    cache_dir = Path(args.cache_dir)
    key = _cache_key(
        wms_url=args.wms_url,
        bbox_utm32=bbox_utm,
        width=args.width,
        height=args.height,
        fmt=fmt,
        layers=wanted,
    )
    run_cache_dir = cache_dir / key
    print(f"[INFO] WMS: {args.wms_url}")
    print(f"[INFO] Format: {fmt}")
    print(f"[INFO] BBox EPSG:25832: {bbox_utm}")
    print(f"[INFO] Cache key: {key}")

    for layer in wanted:
        safe_name = _sanitize_layer_name(layer)
        ext = ".tif" if "tiff" in fmt.lower() or "geotiff" in fmt.lower() else ".img"
        out_path = out_dir / f"{safe_name}{ext}"
        cache_path = run_cache_dir / f"{safe_name}{ext}"
        if cache_path.exists() and not args.force:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if out_path.resolve() != cache_path.resolve():
                out_path.write_bytes(cache_path.read_bytes())
            print(f"[OK] {layer} -> {out_path} (cache hit)")
            continue

        run_cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_raw = run_cache_dir / f"{safe_name}.raw{ext}"
        _fetch_layer(
            wms_url=args.wms_url,
            layer_name=layer,
            out_path=tmp_raw,
            bbox_utm32=bbox_utm,
            width=args.width,
            height=args.height,
            fmt=fmt,
        )

        final_cache = cache_path
        if args.optimize == "auto" and ext == ".tif":
            _optimize_geotiff(tmp_raw, final_cache)
            try:
                tmp_raw.unlink()
            except OSError:
                pass
        else:
            final_cache.write_bytes(tmp_raw.read_bytes())
            try:
                tmp_raw.unlink()
            except OSError:
                pass

        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.resolve() != final_cache.resolve():
            out_path.write_bytes(final_cache.read_bytes())
        print(f"[OK] {layer} -> {out_path}")

    manifest = {
        "cache_key": key,
        "wms_url": args.wms_url,
        "bbox_utm32": [float(x) for x in bbox_utm],
        "width": int(args.width),
        "height": int(args.height),
        "format": fmt,
        "layers": wanted,
        "optimized": args.optimize == "auto",
        "cache_dir": str(run_cache_dir),
    }
    (run_cache_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("[DONE] Abruf abgeschlossen (nur interne Testnutzung bis Freigabe).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
