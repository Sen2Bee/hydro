"""
WCS client for fetching DEM data from Geobasis NRW.

Fetches DGM1 (1m resolution) via OGC WCS 2.0.1 from the
Nordrhein-Westfalen Open Data geodata portal.
"""

import os
import tempfile

import requests
from pyproj import Transformer

# ── WCS Configuration ─────────────────────────────────────────────────
WCS_BASE = "https://www.wcs.nrw.de/geobasis/wcs_nw_dgm"
WCS_VERSION = "2.0.1"
COVERAGE_ID = "nw_dgm"

# Maximum bbox side length in metres (≈ 5 km)
MAX_SIDE_M = 5_000

# Transformers: WGS84 ↔ ETRS89 / UTM 32N
_to_utm = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
_to_wgs = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


class WCSError(Exception):
    """Raised when the WCS request fails."""
    pass


class BboxTooLargeError(Exception):
    """Raised when the requested area is too large."""
    pass


def _validate_and_transform(south: float, west: float,
                            north: float, east: float):
    """
    Validate input bbox (WGS84) and transform to EPSG:25832.

    Returns (min_x, min_y, max_x, max_y) in UTM 32N metres.
    """
    # Transform corners to UTM
    min_x, min_y = _to_utm.transform(west, south)
    max_x, max_y = _to_utm.transform(east, north)

    # Ensure correct order
    if min_x > max_x:
        min_x, max_x = max_x, min_x
    if min_y > max_y:
        min_y, max_y = max_y, min_y

    dx = max_x - min_x
    dy = max_y - min_y

    if dx > MAX_SIDE_M or dy > MAX_SIDE_M:
        raise BboxTooLargeError(
            f"Auswahl zu groß: {dx:.0f}m × {dy:.0f}m "
            f"(max {MAX_SIDE_M}m × {MAX_SIDE_M}m). "
            f"Bitte einen kleineren Bereich wählen."
        )

    return min_x, min_y, max_x, max_y


def _build_wcs_url(min_x, min_y, max_x, max_y):
    """
    Build WCS GetCoverage URL manually.

    We must NOT URL-encode the SUBSET parentheses/commas –
    many WCS servers (including LVermGeo) reject encoded values.
    """
    return (
        f"{WCS_BASE}"
        f"?SERVICE=WCS"
        f"&VERSION={WCS_VERSION}"
        f"&REQUEST=GetCoverage"
        f"&COVERAGEID={COVERAGE_ID}"
        f"&FORMAT=image/tiff"
        f"&SUBSET=x({min_x:.2f},{max_x:.2f})"
        f"&SUBSET=y({min_y:.2f},{max_y:.2f})"
        f"&SUBSETTINGCRS=http://www.opengis.net/def/crs/EPSG/0/25832"
    )


def fetch_dem_from_wcs(south: float, west: float,
                       north: float, east: float) -> str:
    """
    Download a DGM1 GeoTIFF for the given WGS84 bounding box.

    Parameters
    ----------
    south, west, north, east : float
        Bounding box in WGS84 decimal degrees.

    Returns
    -------
    str
        Path to the downloaded temporary GeoTIFF.

    Raises
    ------
    BboxTooLargeError
        If the area exceeds ~5 km × 5 km.
    WCSError
        If the WCS service is unavailable or returns an error.
    """
    min_x, min_y, max_x, max_y = _validate_and_transform(
        south, west, north, east
    )

    url = _build_wcs_url(min_x, min_y, max_x, max_y)

    print(f"[WCS] Fetching DGM1: x=[{min_x:.0f},{max_x:.0f}] "
          f"y=[{min_y:.0f},{max_y:.0f}] (EPSG:25832)")
    print(f"[WCS] URL: {url}")

    try:
        resp = requests.get(url, timeout=120)
    except requests.ConnectionError:
        raise WCSError(
            "WCS-Dienst nicht erreichbar. "
            "Bitte versuchen Sie den manuellen Upload."
        )
    except requests.Timeout:
        raise WCSError(
            "WCS-Zeitüberschreitung. "
            "Bitte einen kleineren Bereich wählen oder manuell hochladen."
        )

    if resp.status_code == 503:
        raise WCSError(
            "WCS-Dienst vorübergehend nicht verfügbar (Wartung). "
            "Bitte versuchen Sie den manuellen GeoTIFF-Upload."
        )

    if resp.status_code != 200:
        raise WCSError(
            f"WCS-Fehler (HTTP {resp.status_code}): {resp.text[:300]}"
        )

    # Check that response is actually a TIFF
    ct = resp.headers.get("Content-Type", "")
    if "tiff" not in ct.lower() and "octet" not in ct.lower():
        raise WCSError(
            f"WCS hat kein GeoTIFF zurückgegeben (Content-Type: {ct}). "
            f"Antwort: {resp.text[:300]}"
        )

    # Write to temp file
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tif")
    tmp.write(resp.content)
    tmp.close()

    size_kb = len(resp.content) / 1024
    print(f"[WCS] Downloaded {size_kb:.0f} KB → {tmp.name}")

    return tmp.name

