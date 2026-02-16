from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path

import requests
from pyproj import Transformer


# Official LVermGeo Sachsen-Anhalt webshare download base (DGM1 ZIP parts).
ST_DGM1_BASE_URL = (
    "https://www.geodatenportal.sachsen-anhalt.de/gfds_webshare/download/"
    "LVermGeo/Geodatenportal/Online-Bereitstellung-LVermGeo/DGM"
)

_to_utm = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)


def _repo_root() -> Path:
    # backend/.. is repo root in this project layout.
    return Path(__file__).resolve().parent.parent


def _cache_dir() -> Path:
    # Stable default independent of current working directory.
    raw = os.getenv("DEM_CACHE_DIR")
    if raw:
        return Path(raw).expanduser()
    return _repo_root() / "data" / "dem_cache"


def _cache_root(override: str | None) -> Path:
    """
    Resolve an optional user-provided cache folder.
    - Absolute paths are used as-is.
    - Relative paths are resolved relative to repo root (predictable).
    """
    if override:
        p = Path(override).expanduser()
        if not p.is_absolute():
            p = _repo_root() / p
        return p
    return _cache_dir()


def _st_dir(cache_root: Path) -> Path:
    return cache_root / "st_dgm1"


def _zip_path(cache_root: Path, part: int) -> Path:
    return _st_dir(cache_root) / f"DGM1_{part}.zip"


def _extract_dir(cache_root: Path, part: int) -> Path:
    return _st_dir(cache_root) / f"DGM1_{part}"


def _vrt_path(cache_root: Path, parts: list[int]) -> Path:
    key = "-".join(str(p) for p in parts)
    return _st_dir(cache_root) / f"DGM1_{key}.vrt"


def _list_path(cache_root: Path, parts: list[int]) -> Path:
    key = "-".join(str(p) for p in parts)
    return _st_dir(cache_root) / f"DGM1_{key}_files.txt"


def _which_gdalbuildvrt() -> str | None:
    return os.getenv("GDALBUILDVRT_BIN") or shutil.which("gdalbuildvrt") or shutil.which("gdalbuildvrt.exe")


def _emit(progress_callback, phase: str, message: str):
    if progress_callback:
        progress_callback(phase, message)


def _download_file(url: str, out_path: Path, progress_callback=None):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    if tmp.exists():
        try:
            tmp.unlink()
        except OSError:
            pass

    _emit(progress_callback, "download", f"Download startet: {out_path.name}")
    t0 = time.time()
    got = 0
    last_emit = 0.0

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length") or 0)
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                got += len(chunk)
                now = time.time()
                if now - last_emit > 1.5:
                    if total > 0:
                        pct = (got / total) * 100.0
                        _emit(progress_callback, "download", f"{out_path.name}: {pct:.1f}% ({got/1e6:.0f}/{total/1e6:.0f} MB)")
                    else:
                        _emit(progress_callback, "download", f"{out_path.name}: {got/1e6:.0f} MB")
                    last_emit = now

    tmp.replace(out_path)
    dt = time.time() - t0
    _emit(progress_callback, "download", f"Download fertig: {out_path.name} ({got/1e6:.0f} MB in {dt:.0f}s)")


def _extract_zip(zip_path: Path, out_dir: Path, progress_callback=None):
    marker = out_dir / ".extracted.ok"
    if marker.exists():
        _emit(progress_callback, "extract", f"Entpacken uebersprungen (cache): {out_dir.name}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    _emit(progress_callback, "extract", f"Entpacke: {zip_path.name}")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    marker.write_text("ok\n", encoding="utf-8")
    _emit(progress_callback, "extract", f"Entpackt: {zip_path.name}")


def _find_tifs(root: Path) -> list[str]:
    return [str(p) for p in root.rglob("*.tif")]


def _build_vrt(*, cache_root: Path, parts: list[int], progress_callback=None) -> Path:
    vrt = _vrt_path(cache_root, parts)
    if vrt.exists():
        _emit(progress_callback, "vrt", f"VRT uebersprungen (cache): {vrt.name}")
        return vrt

    gdalbuildvrt = _which_gdalbuildvrt()
    if not gdalbuildvrt:
        raise RuntimeError(
            "gdalbuildvrt nicht gefunden. Starte Backend ueber OSGeo4W (run_backend.bat) "
            "oder setze GDALBUILDVRT_BIN."
        )

    files = []
    for p in parts:
        files.extend(_find_tifs(_extract_dir(cache_root, p)))
    if not files:
        raise RuntimeError("Keine .tif Kacheln gefunden (Entpacken fehlgeschlagen oder falscher ZIP-Inhalt).")

    list_path = _list_path(cache_root, parts)
    list_path.write_text("\n".join(files) + "\n", encoding="utf-8")

    _emit(progress_callback, "vrt", f"Baue VRT ({len(files)} Tiles): {vrt.name}")
    # Build a VRT mosaic (fast, no huge merged GeoTIFF).
    proc = subprocess.run(
        [gdalbuildvrt, "-overwrite", "-input_file_list", str(list_path), str(vrt)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"gdalbuildvrt fehlgeschlagen: {detail[:400]}")

    _emit(progress_callback, "vrt", f"VRT fertig: {vrt.name}")
    return vrt


def prepare_st_dgm1(parts: list[int], progress_callback=None, cache_dir: str | None = None) -> Path:
    parts = sorted(set(int(p) for p in parts))
    if not parts:
        raise ValueError("parts darf nicht leer sein.")
    for p in parts:
        if p not in (1, 2, 3, 4):
            raise ValueError(f"Ungueltiger DGM1-Part: {p} (erwartet 1-4)")

    cache_root = _cache_root(cache_dir)
    _st_dir(cache_root).mkdir(parents=True, exist_ok=True)

    for p in parts:
        url = f"{ST_DGM1_BASE_URL}/DGM1_{p}.zip"
        zp = _zip_path(cache_root, p)
        if zp.exists() and zp.stat().st_size > 1024 * 1024:
            _emit(progress_callback, "download", f"Download uebersprungen (cache): {zp.name}")
        else:
            _download_file(url, zp, progress_callback=progress_callback)
        _extract_zip(zp, _extract_dir(cache_root, p), progress_callback=progress_callback)

    return _build_vrt(cache_root=cache_root, parts=parts, progress_callback=progress_callback)


def fetch_dem_from_st_public_download(
    *,
    south: float,
    west: float,
    north: float,
    east: float,
    parts: list[int],
    progress_callback=None,
    cache_dir: str | None = None,
) -> str:
    """
    Download+prepare official ST DGM1 data (ZIP -> extracted -> VRT) and return a clipped GeoTIFF for the bbox.
    bbox is WGS84; clipping happens in EPSG:25832.
    """
    vrt = prepare_st_dgm1(parts, progress_callback=progress_callback, cache_dir=cache_dir)

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
            raise RuntimeError(f"ST DGM1 VRT muss EPSG:25832 sein (gefunden: {src_crs or 'unknown'}).")

        w = from_bounds(min_x, min_y, max_x, max_y, transform=src.transform)
        w = w.round_offsets().round_lengths()
        if w.width <= 0 or w.height <= 0:
            raise RuntimeError("Ausschnitt ist leer (BBox ausserhalb des Rasters?).")

        data = src.read(1, window=w, boundless=False)
        profile = src.profile.copy()
        profile.update(
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
