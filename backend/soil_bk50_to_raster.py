"""
Convert NRW BK50 (GeoPackage) polygons to a raster for risk scoring.

The output can be used as SOIL_RASTER_PATH in the hydrology pipeline.

Notes:
- Requires GDAL CLI tools: `ogrinfo`, `gdal_rasterize`
- Input should be EPSG:25832 (or transformable by GDAL)
- You must provide a numeric attribute field (`--value-field`)
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
from pathlib import Path


NRW_EXTENT_25832 = (278000, 5560000, 536000, 5828000)  # minx miny maxx maxy


def _run(cmd: list[str]) -> str:
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        raise RuntimeError(
            f"Command failed ({res.returncode}): {' '.join(cmd)}\n{res.stderr.strip()}"
        )
    return res.stdout


def _require_tool(name: str) -> None:
    if shutil.which(name):
        return
    raise RuntimeError(f"Required tool not found in PATH: {name}")


def _list_layers(gpkg_path: str) -> list[str]:
    out = _run(["ogrinfo", gpkg_path])
    layers = []
    for line in out.splitlines():
        m = re.match(r"^\s*\d+:\s+(.+)$", line)
        if m:
            layers.append(m.group(1).strip())
    return layers


def _list_fields(gpkg_path: str, layer: str) -> list[str]:
    out = _run(["ogrinfo", "-so", gpkg_path, layer])
    fields = []
    for line in out.splitlines():
        m = re.match(r"^\s*([A-Za-z0-9_]+)\s+\(", line)
        if m:
            fields.append(m.group(1))
    return fields


def _choose_layer(gpkg_path: str, explicit_layer: str | None) -> str:
    if explicit_layer:
        return explicit_layer
    layers = _list_layers(gpkg_path)
    if not layers:
        raise RuntimeError("No layers found in GeoPackage.")
    if len(layers) > 1:
        raise RuntimeError(
            "GeoPackage has multiple layers. Please pass --layer.\n"
            f"Available layers: {', '.join(layers)}"
        )
    return layers[0]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert BK50 GeoPackage polygons to a GeoTIFF raster."
    )
    parser.add_argument("--input-gpkg", required=True, help="Path to BK50 .gpkg")
    parser.add_argument("--output-tif", required=True, help="Output raster path (.tif)")
    parser.add_argument("--layer", help="Layer name inside the GeoPackage")
    parser.add_argument(
        "--value-field",
        help="Numeric attribute field to burn into raster (required for useful output)",
    )
    parser.add_argument("--pixel-size", type=float, default=10.0, help="Pixel size in meters (default: 10)")
    parser.add_argument("--nodata", type=float, default=-9999.0, help="NoData value (default: -9999)")
    parser.add_argument(
        "--extent",
        nargs=4,
        type=float,
        metavar=("MINX", "MINY", "MAXX", "MAXY"),
        default=NRW_EXTENT_25832,
        help="Target extent in EPSG:25832 (default: NRW bounds)",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only print layers/fields and exit",
    )
    args = parser.parse_args()

    _require_tool("ogrinfo")
    _require_tool("gdal_rasterize")

    gpkg = os.path.abspath(args.input_gpkg)
    if not os.path.exists(gpkg):
        raise RuntimeError(f"Input file not found: {gpkg}")

    layer = _choose_layer(gpkg, args.layer)
    fields = _list_fields(gpkg, layer)

    print(f"[BK50] input: {gpkg}")
    print(f"[BK50] layer: {layer}")
    print(f"[BK50] fields: {', '.join(fields) if fields else '(none detected)'}")

    if args.list_only:
        return 0

    if not args.value_field:
        raise RuntimeError(
            "Missing --value-field. Use --list-only first to inspect fields."
        )
    if args.value_field not in fields:
        raise RuntimeError(
            f"Field '{args.value_field}' not found in layer '{layer}'."
        )

    out_path = os.path.abspath(args.output_tif)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    minx, miny, maxx, maxy = args.extent
    tr = float(args.pixel_size)

    cmd = [
        "gdal_rasterize",
        "-of", "GTiff",
        "-ot", "Float32",
        "-a", args.value_field,
        "-a_nodata", str(args.nodata),
        "-a_srs", "EPSG:25832",
        "-te", str(minx), str(miny), str(maxx), str(maxy),
        "-tr", str(tr), str(tr),
        "-tap",
        "-l", layer,
        gpkg,
        out_path,
    ]

    print(f"[BK50] rasterize -> {out_path}")
    _run(cmd)
    print("[BK50] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

