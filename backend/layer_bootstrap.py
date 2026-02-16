"""
Bootstrap external risk layers (soil + imperviousness).

Behavior:
- Loads env vars from ../.env if present.
- Verifies existing local layer files.
- If missing, downloads from configured URLs and caches locally.

Env vars:
- SOIL_RASTER_PATH
- IMPERVIOUS_RASTER_PATH
- SOIL_RASTER_URL
- IMPERVIOUS_RASTER_URL
- AUTO_FETCH_LAYERS (default: 1)
- LAYER_CACHE_DIR (default: backend/.layer_cache)
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import rasterio

from processing import _download_layer_if_missing


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _validate_raster(path: str) -> dict:
    with rasterio.open(path) as src:
        return {
            "path": path,
            "width": int(src.width),
            "height": int(src.height),
            "crs": str(src.crs) if src.crs else None,
            "dtype": str(src.dtypes[0]),
        }


def _resolve_layer(path_key: str, url_key: str, label: str) -> tuple[str | None, str]:
    path_val = os.getenv(path_key)
    url_val = os.getenv(url_key)
    resolved = _download_layer_if_missing(path_val, url_val, label)
    if resolved and os.path.exists(resolved):
        return resolved, "ok"
    if path_val and not os.path.exists(path_val):
        return None, f"missing: {path_val}"
    if url_val:
        return None, "download failed"
    return None, "not configured"


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap soil/impervious raster layers.")
    parser.add_argument("--strict", action="store_true", help="Exit with code 1 if a layer is missing.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    _load_dotenv(repo_root / ".env")

    print("[BOOTSTRAP] layer bootstrap started")

    results = []
    for label, path_key, url_key in [
        ("soil", "SOIL_RASTER_PATH", "SOIL_RASTER_URL"),
        ("impervious", "IMPERVIOUS_RASTER_PATH", "IMPERVIOUS_RASTER_URL"),
    ]:
        resolved, status = _resolve_layer(path_key, url_key, label)
        if resolved:
            try:
                info = _validate_raster(resolved)
                print(
                    f"[OK] {label}: {info['path']} | {info['width']}x{info['height']} "
                    f"| {info['crs']} | {info['dtype']}"
                )
                results.append((label, True))
            except Exception as exc:
                print(f"[ERR] {label}: invalid raster ({exc})")
                results.append((label, False))
        else:
            print(f"[WARN] {label}: {status}")
            results.append((label, False))

    ok_count = sum(1 for _, ok in results if ok)
    print(f"[BOOTSTRAP] ready layers: {ok_count}/{len(results)}")

    if args.strict and ok_count < len(results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

