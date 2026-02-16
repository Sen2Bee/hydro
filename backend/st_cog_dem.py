from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path

from pyproj import Transformer


_to_utm = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
_VRT_LOCK = threading.Lock()


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _default_cache_dir() -> Path:
    raw = os.getenv("DEM_CACHE_DIR")
    if raw:
        return Path(raw).expanduser()
    return _repo_root() / "data" / "dem_cache"


def _cache_root(override: str | None) -> Path:
    if override:
        p = Path(override).expanduser()
        if not p.is_absolute():
            p = _repo_root() / p
        return p
    return _default_cache_dir()


def _emit(progress_callback, phase: str, message: str):
    if progress_callback:
        progress_callback(phase, message)


def _which_gdalbuildvrt() -> str | None:
    return (
        os.getenv("GDALBUILDVRT_BIN")
        or shutil.which("gdalbuildvrt")
        or shutil.which("gdalbuildvrt.exe")
    )


def _list_cog_tiles(cog_dir: Path) -> list[str]:
    # Avoid deep heuristics; assume all tiles end with *_cog.tif.
    return [str(p) for p in cog_dir.rglob("*_cog.tif") if p.is_file()]


def build_vrt_for_cog_dir(
    *,
    cog_dir: str,
    progress_callback=None,
    cache_dir: str | None = None,
) -> Path:
    """
    Build (and cache) a VRT mosaic over all *_cog.tif in cog_dir.

    The VRT is written to DEM_CACHE_DIR to keep the source directory read-only safe.
    """
    cog_root = Path(cog_dir).expanduser()
    if not cog_root.exists():
        raise RuntimeError(f"COG dir not found: {cog_root}")

    cache_root = _cache_root(cache_dir) / "st_dgm1_cog"
    cache_root.mkdir(parents=True, exist_ok=True)
    vrt_path = cache_root / "st_dgm1_cog.vrt"
    list_path = cache_root / "st_dgm1_cog_files.txt"

    if vrt_path.exists() and list_path.exists() and list_path.stat().st_size > 0:
        return vrt_path

    gdalbuildvrt = _which_gdalbuildvrt()
    if not gdalbuildvrt:
        raise RuntimeError(
            "gdalbuildvrt nicht gefunden. In Docker: gdal-bin installieren; lokal: OSGeo4W nutzen "
            "oder GDALBUILDVRT_BIN setzen."
        )

    with _VRT_LOCK:
        if vrt_path.exists() and list_path.exists() and list_path.stat().st_size > 0:
            return vrt_path

        _emit(progress_callback, "vrt", "Suche COG-Tiles...")
        tiles = _list_cog_tiles(cog_root)
        if not tiles:
            raise RuntimeError(f"Keine '*_cog.tif' Dateien gefunden unter: {cog_root}")

        _emit(progress_callback, "vrt", f"Schreibe Tile-Liste ({len(tiles)})...")
        list_path.write_text("\n".join(tiles) + "\n", encoding="utf-8")

        _emit(progress_callback, "vrt", f"Baue VRT ({len(tiles)} Tiles)...")
        proc = subprocess.run(
            [gdalbuildvrt, "-overwrite", "-input_file_list", str(list_path), str(vrt_path)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeError(f"gdalbuildvrt fehlgeschlagen: {detail[:400]}")

    return vrt_path


def fetch_dem_from_st_cog_dir(
    *,
    south: float,
    west: float,
    north: float,
    east: float,
    cog_dir: str,
    progress_callback=None,
    cache_dir: str | None = None,
) -> str:
    """
    Clip a DEM from a local folder of ST DGM1 COG tiles.

    - input bbox: WGS84
    - expected DEM CRS: EPSG:25832 (ST DGM1)
    """
    vrt = build_vrt_for_cog_dir(cog_dir=cog_dir, progress_callback=progress_callback, cache_dir=cache_dir)

    try:
        import rasterio
        from rasterio.windows import from_bounds
    except Exception as exc:
        raise RuntimeError(
            "Clipping benoetigt 'rasterio'. Bitte OSGeo4W-Umgebung nutzen oder rasterio installieren."
        ) from exc

    min_x, min_y = _to_utm.transform(west, south)
    max_x, max_y = _to_utm.transform(east, north)
    if min_x > max_x:
        min_x, max_x = max_x, min_x
    if min_y > max_y:
        min_y, max_y = max_y, min_y

    _emit(progress_callback, "clip", "Schneide DEM auf Auswahl zu...")
    with rasterio.open(vrt) as src:
        src_crs = str(src.crs) if src.crs else ""
        if "25832" not in src_crs:
            raise RuntimeError(f"COG-VRT muss EPSG:25832 sein (gefunden: {src_crs or 'unknown'}).")

        w = from_bounds(min_x, min_y, max_x, max_y, transform=src.transform)
        w = w.round_offsets().round_lengths()
        if w.width <= 0 or w.height <= 0:
            raise RuntimeError("Ausschnitt ist leer (BBox ausserhalb des Rasters?).")

        data = src.read(1, window=w, boundless=False)
        profile = src.profile.copy()
        profile.update(
            driver="GTiff",
            height=int(w.height),
            width=int(w.width),
            transform=rasterio.windows.transform(w, src.transform),
        )

        out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tif")
        out_tmp.close()
        with rasterio.open(out_tmp.name, "w", **profile) as dst:
            dst.write(data, 1)

    _emit(progress_callback, "clip", "DEM-Ausschnitt fertig.")
    return out_tmp.name
