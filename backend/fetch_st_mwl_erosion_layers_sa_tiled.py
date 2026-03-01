from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

DEFAULT_WMS_URL = "https://www.geodatenportal.sachsen-anhalt.de/wss-org1/service/ST_MWL_Erosion/guest"
DEFAULT_LAYERS = ["K-Faktor", "R-Faktor", "S-Faktor", "Wasser_Erosion"]


def _sanitize_layer_name(name: str) -> str:
    return (
        name.replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("-", "_")
    )


def _parse_capabilities(
    wms_url: str, timeout_s: int
) -> tuple[set[str], set[str], dict[str, tuple[float, float, float, float] | None]]:
    sep = "&" if "?" in wms_url else "?"
    cap_url = f"{wms_url}{sep}SERVICE=WMS&REQUEST=GetCapabilities"
    resp = requests.get(cap_url, timeout=timeout_s)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    ns = {"wms": "http://www.opengis.net/wms"}

    formats = set()
    for el in root.findall(".//wms:GetMap//wms:Format", ns):
        if el.text and el.text.strip():
            formats.add(el.text.strip())

    layer_bbox: dict[str, tuple[float, float, float, float] | None] = {}
    for layer_el in root.findall(".//wms:Layer", ns):
        name_el = layer_el.find("wms:Name", ns)
        if name_el is None or not name_el.text:
            continue
        layer_name = name_el.text.strip()
        bbox_el = None
        for bb in layer_el.findall("wms:BoundingBox", ns):
            crs = (bb.attrib.get("CRS") or bb.attrib.get("SRS") or "").upper()
            if crs == "EPSG:25832":
                bbox_el = bb
                break
        if bbox_el is None:
            layer_bbox[layer_name] = None
            continue
        layer_bbox[layer_name] = (
            float(bbox_el.attrib["minx"]),
            float(bbox_el.attrib["miny"]),
            float(bbox_el.attrib["maxx"]),
            float(bbox_el.attrib["maxy"]),
        )

    return set(layer_bbox.keys()), formats, layer_bbox


def _choose_tiff_format(formats: set[str]) -> str:
    lower_map = {f.lower(): f for f in formats}
    for wanted in ("image/tiff", "image/geotiff", "image/geotiff8"):
        if wanted in lower_map:
            return lower_map[wanted]
    raise RuntimeError(f"Kein TIFF-Format verfuegbar. Formate: {sorted(formats)}")


def _build_tiles(bbox: tuple[float, float, float, float], span_m: float) -> list[tuple[int, int, float, float, float, float]]:
    minx, miny, maxx, maxy = bbox
    cols = max(1, int(math.ceil((maxx - minx) / span_m)))
    rows = max(1, int(math.ceil((maxy - miny) / span_m)))
    tiles: list[tuple[int, int, float, float, float, float]] = []
    for r in range(rows):
        y0 = miny + r * span_m
        y1 = min(maxy, y0 + span_m)
        for c in range(cols):
            x0 = minx + c * span_m
            x1 = min(maxx, x0 + span_m)
            tiles.append((r, c, x0, y0, x1, y1))
    return tiles


def _getmap(
    *,
    wms_url: str,
    layer_name: str,
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
    fmt: str,
    timeout_s: int,
) -> bytes:
    minx, miny, maxx, maxy = bbox
    params = {
        "SERVICE": "WMS",
        "REQUEST": "GetMap",
        "VERSION": "1.3.0",
        "LAYERS": layer_name,
        "STYLES": "",
        "CRS": "EPSG:25832",
        "BBOX": f"{minx},{miny},{maxx},{maxy}",
        "WIDTH": str(width),
        "HEIGHT": str(height),
        "FORMAT": fmt,
        "TRANSPARENT": "FALSE",
    }
    resp = requests.get(wms_url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    return resp.content


def _build_vrt(vrt_path: Path, tile_paths: list[Path]) -> tuple[bool, str]:
    gdalbuildvrt = shutil.which("gdalbuildvrt")
    if not gdalbuildvrt:
        return False, "gdalbuildvrt nicht gefunden"
    if not tile_paths:
        return False, "keine Tiles"

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".txt") as tmp:
        for p in tile_paths:
            tmp.write(str(p.resolve()) + "\n")
        file_list = tmp.name

    try:
        cmd = [gdalbuildvrt, "-overwrite", "-input_file_list", file_list, str(vrt_path)]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            return False, err[:500]
        return True, "ok"
    finally:
        try:
            Path(file_list).unlink(missing_ok=True)
        except OSError:
            pass


def main() -> int:
    p = argparse.ArgumentParser(
        description="SA-weiter, gekachelter Fetch fuer ST_MWL_Erosion-Layer (TIFF + optional VRT)."
    )
    p.add_argument("--wms-url", default=DEFAULT_WMS_URL)
    p.add_argument("--layers", default=",".join(DEFAULT_LAYERS))
    p.add_argument("--target-res-m", type=float, default=10.0, help="Zielauflosung in Metern (z. B. 10 oder 5).")
    p.add_argument("--tile-px", type=int, default=5000, help="Kachelgroesse in Pixel je Richtung.")
    p.add_argument("--out-dir", default=str(Path("data") / "layers" / "st_mwl_erosion_sa_tiled"))
    p.add_argument("--timeout-s", type=int, default=180)
    p.add_argument("--force", action="store_true")
    p.add_argument("--build-vrt", choices=["auto", "off"], default="auto")
    args = p.parse_args()

    if args.target_res_m <= 0:
        raise RuntimeError("--target-res-m muss > 0 sein")
    if args.tile_px < 256:
        raise RuntimeError("--tile-px muss >= 256 sein")

    wanted = [x.strip() for x in str(args.layers).split(",") if x.strip()]
    layer_names, formats, bboxes = _parse_capabilities(args.wms_url, timeout_s=args.timeout_s)
    missing = [w for w in wanted if w not in layer_names]
    if missing:
        raise RuntimeError(f"Unbekannte Layer: {missing}")

    fmt = _choose_tiff_format(formats)
    span_m = float(args.target_res_m) * int(args.tile_px)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    run_manifest: dict[str, object] = {
        "wms_url": args.wms_url,
        "target_res_m": float(args.target_res_m),
        "tile_px": int(args.tile_px),
        "tile_span_m": span_m,
        "format": fmt,
        "layers": {},
    }

    for layer_name in wanted:
        safe = _sanitize_layer_name(layer_name)
        bbox = bboxes.get(layer_name)
        if not bbox:
            print(f"[WARN] {layer_name}: keine EPSG:25832 BBox in Capabilities")
            continue

        layer_dir = out_dir / safe
        tiles_dir = layer_dir / "tiles"
        tiles_dir.mkdir(parents=True, exist_ok=True)

        tiles = _build_tiles(bbox, span_m=span_m)
        tile_entries: list[dict[str, object]] = []
        tile_paths: list[Path] = []
        print(f"[LAYER] {layer_name}: bbox={bbox}, tiles={len(tiles)}")

        for idx, (r, c, x0, y0, x1, y1) in enumerate(tiles, start=1):
            width = max(1, int(math.ceil((x1 - x0) / args.target_res_m)))
            height = max(1, int(math.ceil((y1 - y0) / args.target_res_m)))
            tile_path = tiles_dir / f"{safe}_r{r:03d}_c{c:03d}.tif"

            if tile_path.exists() and not args.force:
                tile_paths.append(tile_path)
                tile_entries.append(
                    {
                        "row": r,
                        "col": c,
                        "bbox_utm32": [x0, y0, x1, y1],
                        "width": width,
                        "height": height,
                        "path": str(tile_path),
                        "status": "cache_hit",
                    }
                )
                continue

            print(f"  [tile {idx}/{len(tiles)}] r={r} c={c} px={width}x{height}")
            content = _getmap(
                wms_url=args.wms_url,
                layer_name=layer_name,
                bbox=(x0, y0, x1, y1),
                width=width,
                height=height,
                fmt=fmt,
                timeout_s=args.timeout_s,
            )
            tile_path.write_bytes(content)
            tile_paths.append(tile_path)
            tile_entries.append(
                {
                    "row": r,
                    "col": c,
                    "bbox_utm32": [x0, y0, x1, y1],
                    "width": width,
                    "height": height,
                    "path": str(tile_path),
                    "status": "downloaded",
                }
            )

        vrt_path = layer_dir / f"{safe}.vrt"
        vrt_ok = False
        vrt_msg = "off"
        if args.build_vrt == "auto":
            vrt_ok, vrt_msg = _build_vrt(vrt_path, tile_paths)
            print(f"  [vrt] {safe}: {'OK' if vrt_ok else 'WARN'} ({vrt_msg})")

        layer_manifest = {
            "layer_name": layer_name,
            "safe_name": safe,
            "bbox_utm32": list(bbox),
            "tile_count": len(tiles),
            "tiles": tile_entries,
            "vrt_path": str(vrt_path) if vrt_ok else None,
            "vrt_status": vrt_msg,
        }
        (layer_dir / "manifest.json").write_text(json.dumps(layer_manifest, indent=2), encoding="utf-8")
        run_manifest["layers"][safe] = layer_manifest

    run_manifest_path = out_dir / "run_manifest.json"
    run_manifest_path.write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")
    print(f"[DONE] {run_manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
