"""
WCS client for fetching DEM data with provider auto-detection.

Current productive provider:
- Nordrhein-Westfalen (nrw)

Prepared (not yet configured with endpoint/coverage):
- Sachsen
- Sachsen-Anhalt
"""

from __future__ import annotations

import math
import os
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import requests
from pyproj import Transformer

# WCS configuration
WCS_VERSION = "2.0.1"
WCS_REQUEST_TIMEOUT_S = int(os.getenv("WCS_REQUEST_TIMEOUT_S", "60"))

# Maximum requested side length per WCS tile in meters
MAX_TILE_SIDE_M = 5_000
# If proxy still rejects a tile, recursively split until this size
MIN_TILE_SIDE_M = 500
# Safety cap to avoid accidental huge fan-out requests
MAX_TILE_COUNT = 400

# Coverage bounds from DescribeCoverage(nw_dgm), EPSG:25832
NRW_MIN_X = 278_000.0
NRW_MIN_Y = 5_560_000.0
NRW_MAX_X = 536_000.0
NRW_MAX_Y = 5_828_000.0


@dataclass(frozen=True)
class WCSProvider:
    key: str
    name: str
    wgs84_bounds: tuple[float, float, float, float]  # south, west, north, east
    wcs_base: str | None
    coverage_id: str | None
    utm32_bounds: tuple[float, float, float, float] | None = None
    # Optional: local DEM GeoTIFF path for offline fallback (must be in EPSG:25832 or readable with CRS).
    local_dem_path: str | None = None

# Transformers: WGS84 <-> ETRS89 / UTM 32N
_to_utm = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
_to_wgs = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


PROVIDERS: dict[str, WCSProvider] = {
    "nrw": WCSProvider(
        key="nrw",
        name="Nordrhein-Westfalen",
        # rough admin bbox for auto-detection in WGS84
        wgs84_bounds=(50.20, 5.85, 52.65, 9.65),
        wcs_base="https://www.wcs.nrw.de/geobasis/wcs_nw_dgm",
        coverage_id="nw_dgm",
        utm32_bounds=(NRW_MIN_X, NRW_MIN_Y, NRW_MAX_X, NRW_MAX_Y),
    ),
    "sachsen": WCSProvider(
        key="sachsen",
        name="Sachsen",
        wgs84_bounds=(50.10, 11.90, 51.70, 15.10),
        wcs_base=None,
        coverage_id=None,
    ),
    "sachsen-anhalt": WCSProvider(
        key="sachsen-anhalt",
        name="Sachsen-Anhalt",
        wgs84_bounds=(50.90, 10.45, 53.10, 13.25),
        # LVermGeo Sachsen-Anhalt DGM1 WCS (OpenData)
        wcs_base="https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_DGM1_WCS_OpenData/guest",
        coverage_id="Coverage1",
        local_dem_path=os.getenv("ST_DEM_LOCAL_PATH"),
    ),
}


def detect_provider(south: float, west: float, north: float, east: float) -> WCSProvider:
    """Public helper for UI/provider selection."""
    return _detect_provider(south, west, north, east)


class WCSError(Exception):
    """Raised when the WCS request fails."""


class BboxTooLargeError(Exception):
    """Raised when the requested area is too large."""


class BboxOutOfCoverageError(Exception):
    """Raised when bbox is outside currently supported provider coverage."""


def _rect_overlap_area(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> float:
    """Return overlap area for rectangles (south, west, north, east) in degrees."""
    south = max(a[0], b[0])
    west = max(a[1], b[1])
    north = min(a[2], b[2])
    east = min(a[3], b[3])
    if south >= north or west >= east:
        return 0.0
    return (north - south) * (east - west)


def _detect_provider(south: float, west: float, north: float, east: float) -> WCSProvider:
    bbox = (south, west, north, east)
    ranked = sorted(
        (
            (
                (
                    # Primary signal: bbox overlap with rough provider admin bounds.
                    _rect_overlap_area(bbox, provider.wgs84_bounds),
                    # Tie-breaker: prefer providers that are actually configured.
                    1 if (provider.wcs_base and provider.coverage_id) else 0,
                ),
                provider,
            )
            for provider in PROVIDERS.values()
        ),
        key=lambda item: item[0],
        reverse=True,
    )
    if not ranked or ranked[0][0][0] <= 0:
        raise BboxOutOfCoverageError(
            "Auswahl liegt ausserhalb der aktuell unterstuetzten Regionen "
            "(NRW, Sachsen, Sachsen-Anhalt)."
        )
    return ranked[0][1]


def _resolve_provider(south: float, west: float, north: float, east: float, provider_key: str | None) -> WCSProvider:
    if not provider_key or provider_key.lower() == "auto":
        return _detect_provider(south, west, north, east)

    key = provider_key.strip().lower()
    if key not in PROVIDERS:
        known = ", ".join(sorted(PROVIDERS.keys()))
        raise WCSError(f"Unbekannter Provider '{provider_key}'. Erlaubt: auto, {known}")
    return PROVIDERS[key]


def _extract_ows_exception_text(xml_text: str) -> str:
    """Extract OWS ExceptionText if present; return fallback otherwise."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return xml_text[:300]

    # Match by local-name to handle varying OWS namespaces
    for elem in root.iter():
        tag = elem.tag
        if tag.endswith("ExceptionText") and elem.text:
            return elem.text.strip()

    return xml_text[:300]


def _validate_and_transform(
    south: float, west: float, north: float, east: float, provider: WCSProvider
):
    """Validate input bbox (WGS84) and transform to EPSG:25832."""
    min_x, min_y = _to_utm.transform(west, south)
    max_x, max_y = _to_utm.transform(east, north)

    if min_x > max_x:
        min_x, max_x = max_x, min_x
    if min_y > max_y:
        min_y, max_y = max_y, min_y

    if provider.utm32_bounds:
        p_min_x, p_min_y, p_max_x, p_max_y = provider.utm32_bounds
        if (
            min_x < p_min_x
            or max_x > p_max_x
            or min_y < p_min_y
            or max_y > p_max_y
        ):
            raise BboxOutOfCoverageError(
                f"Der WCS-Dienst fuer {provider.name} deckt diese Auswahl nicht komplett ab. "
                "Bitte Bereich anpassen oder GeoTIFF manuell hochladen."
            )

    return min_x, min_y, max_x, max_y


def _clip_local_dem_utm32(
    *,
    local_path: str,
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
) -> str:
    """
    Clip a local DEM (GeoTIFF) by UTM32 bounds and return a temporary GeoTIFF path.

    This is used as a fallback when a provider's WCS GetCoverage is unavailable.
    """
    try:
        import rasterio
        from rasterio.windows import from_bounds
    except Exception as exc:
        raise WCSError(
            "Lokaler DEM-Ausschnitt benoetigt 'rasterio'. Bitte OSGeo4W-Umgebung nutzen "
            "oder rasterio in die Python-Umgebung installieren."
        ) from exc

    if not os.path.exists(local_path):
        raise WCSError(f"Lokales DEM nicht gefunden: {local_path}")

    with rasterio.open(local_path) as src:
        # Expect local DEM to be in EPSG:25832 for now (keeps this fast and predictable).
        # If needed later: add reproject-on-the-fly.
        src_crs = str(src.crs) if src.crs else ""
        if "25832" not in src_crs:
            raise WCSError(
                f"Lokales DEM muss in EPSG:25832 vorliegen (gefunden: {src_crs or 'unknown'})."
            )

        w = from_bounds(min_x, min_y, max_x, max_y, transform=src.transform)
        w = w.round_offsets().round_lengths()
        if w.width <= 0 or w.height <= 0:
            raise WCSError("Lokales DEM: Ausschnitt ist leer (BBox ausserhalb des Rasters?).")

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
        return out_tmp.name


def _iter_tiles(min_x: float, min_y: float, max_x: float, max_y: float):
    """Yield UTM tile extents (min_x, min_y, max_x, max_y) within MAX_TILE_SIDE_M."""
    width = max_x - min_x
    height = max_y - min_y
    nx = max(1, math.ceil(width / MAX_TILE_SIDE_M))
    ny = max(1, math.ceil(height / MAX_TILE_SIDE_M))

    if nx * ny > MAX_TILE_COUNT:
        raise BboxTooLargeError(
            f"Auswahl erzeugt zu viele Kacheln ({nx * ny}). "
            f"Bitte Bereich verkleinern (max {MAX_TILE_COUNT} Kacheln)."
        )

    step_x = width / nx
    step_y = height / ny

    for ix in range(nx):
        tx0 = min_x + ix * step_x
        tx1 = max_x if ix == nx - 1 else min_x + (ix + 1) * step_x
        for iy in range(ny):
            ty0 = min_y + iy * step_y
            ty1 = max_y if iy == ny - 1 else min_y + (iy + 1) * step_y
            yield tx0, ty0, tx1, ty1


def _split_tile(tile: tuple[float, float, float, float]):
    """Split one tile into four quadrants."""
    min_x, min_y, max_x, max_y = tile
    mid_x = (min_x + max_x) / 2
    mid_y = (min_y + max_y) / 2
    return [
        (min_x, min_y, mid_x, mid_y),
        (mid_x, min_y, max_x, mid_y),
        (min_x, mid_y, mid_x, max_y),
        (mid_x, mid_y, max_x, max_y),
    ]


def _build_wcs_url(wcs_base: str, coverage_id: str, min_x, min_y, max_x, max_y):
    """Build WCS GetCoverage URL (KVP)."""
    return (
        f"{wcs_base}"
        f"?SERVICE=WCS"
        f"&VERSION={WCS_VERSION}"
        f"&REQUEST=GetCoverage"
        f"&COVERAGEID={coverage_id}"
        f"&FORMAT=image/tiff"
        f"&SUBSET=x({min_x:.2f},{max_x:.2f})"
        f"&SUBSET=y({min_y:.2f},{max_y:.2f})"
        f"&SUBSETTINGCRS=http://www.opengis.net/def/crs/EPSG/0/25832"
    )


def _build_wcs_url_variant(
    wcs_base: str,
    coverage_id: str,
    min_x,
    min_y,
    max_x,
    max_y,
    subsetting_crs: str,
    output_crs: str | None = None,
):
    """Build alternative WCS KVP URL variants for proxy compatibility."""
    url = (
        f"{wcs_base}"
        f"?SERVICE=WCS"
        f"&VERSION={WCS_VERSION}"
        f"&REQUEST=GetCoverage"
        f"&COVERAGEID={coverage_id}"
        f"&FORMAT=image/tiff"
        f"&SUBSET=x({min_x:.2f},{max_x:.2f})"
        f"&SUBSET=y({min_y:.2f},{max_y:.2f})"
        f"&SUBSETTINGCRS={subsetting_crs}"
    )
    if output_crs:
        url += f"&OUTPUTCRS={output_crs}"
    return url


def _build_wcs_url_axis_variant(
    wcs_base: str,
    coverage_id: str,
    min_x,
    min_y,
    max_x,
    max_y,
    axis_x: str,
    axis_y: str,
    subsetting_crs: str,
    output_crs: str | None = None,
):
    """Build KVP URL with alternative axis labels (e.g., E/N)."""
    url = (
        f"{wcs_base}"
        f"?SERVICE=WCS"
        f"&VERSION={WCS_VERSION}"
        f"&REQUEST=GetCoverage"
        f"&COVERAGEID={coverage_id}"
        f"&FORMAT=image/tiff"
        f"&SUBSET={axis_x}({min_x:.2f},{max_x:.2f})"
        f"&SUBSET={axis_y}({min_y:.2f},{max_y:.2f})"
        f"&SUBSETTINGCRS={subsetting_crs}"
    )
    if output_crs:
        url += f"&OUTPUTCRS={output_crs}"
    return url


def _build_wcs_url_wgs84_variant(
    wcs_base: str,
    coverage_id: str,
    west: float,
    south: float,
    east: float,
    north: float,
    axis_lon: str,
    axis_lat: str,
):
    """Build KVP URL in EPSG:4326 with configurable axis labels."""
    return (
        f"{wcs_base}"
        f"?SERVICE=WCS"
        f"&VERSION={WCS_VERSION}"
        f"&REQUEST=GetCoverage"
        f"&COVERAGEID={coverage_id}"
        f"&FORMAT=image/tiff"
        f"&SUBSET={axis_lon}({west:.8f},{east:.8f})"
        f"&SUBSET={axis_lat}({south:.8f},{north:.8f})"
        f"&SUBSETTINGCRS=EPSG:4326"
        f"&OUTPUTCRS=EPSG:4326"
    )


def _fetch_single_tile(
    provider: WCSProvider,
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    notify=None,
) -> str:
    west, south = _to_wgs.transform(min_x, min_y)
    east, north = _to_wgs.transform(max_x, max_y)

    print(
        f"[WCS] [{provider.key}] Fetching tile: x=[{min_x:.0f},{max_x:.0f}] "
        f"y=[{min_y:.0f},{max_y:.0f}] (EPSG:25832)"
    )

    if not provider.wcs_base or not provider.coverage_id:
        raise WCSError(
            f"Fuer {provider.name} ist noch kein WCS-Endpunkt konfiguriert. "
            "Bitte vorerst GeoTIFF manuell hochladen."
        )

    base = provider.wcs_base
    cov = provider.coverage_id
    # Provider-specific behavior:
    # - NRW is known to work with x/y + OGC CRS URI.
    # - Sachsen-Anhalt often behaves differently (axis labels and OUTPUTCRS quirks),
    #   so we prioritize E/N + OUTPUTCRS first to reduce retries.
    if provider.key == "sachsen-anhalt":
        candidate_urls = [
            _build_wcs_url_axis_variant(base, cov, min_x, min_y, max_x, max_y, "E", "N", "EPSG:25832", "EPSG:25832"),
            _build_wcs_url_variant(base, cov, min_x, min_y, max_x, max_y, "EPSG:25832", "EPSG:25832"),
            _build_wcs_url_axis_variant(
                base,
                cov,
                min_x,
                min_y,
                max_x,
                max_y,
                "E",
                "N",
                "http://www.opengis.net/def/crs/EPSG/0/25832",
                "http://www.opengis.net/def/crs/EPSG/0/25832",
            ),
            _build_wcs_url_variant(
                base,
                cov,
                min_x,
                min_y,
                max_x,
                max_y,
                "http://www.opengis.net/def/crs/EPSG/0/25832",
                "http://www.opengis.net/def/crs/EPSG/0/25832",
            ),
            _build_wcs_url_axis_variant(base, cov, min_x, min_y, max_x, max_y, "E", "N", "EPSG:25832"),
            _build_wcs_url_variant(base, cov, min_x, min_y, max_x, max_y, "EPSG:25832"),
            _build_wcs_url(base, cov, min_x, min_y, max_x, max_y),
            _build_wcs_url_wgs84_variant(base, cov, west, south, east, north, "Long", "Lat"),
            _build_wcs_url_wgs84_variant(base, cov, west, south, east, north, "lon", "lat"),
        ]
    else:
        candidate_urls = [
            _build_wcs_url(base, cov, min_x, min_y, max_x, max_y),
            _build_wcs_url_variant(
                base,
                cov,
                min_x,
                min_y,
                max_x,
                max_y,
                "http://www.opengis.net/def/crs/EPSG/0/25832",
                "http://www.opengis.net/def/crs/EPSG/0/25832",
            ),
            _build_wcs_url_variant(base, cov, min_x, min_y, max_x, max_y, "EPSG:25832", "EPSG:25832"),
            _build_wcs_url_variant(base, cov, min_x, min_y, max_x, max_y, "EPSG:25832"),
            _build_wcs_url_axis_variant(base, cov, min_x, min_y, max_x, max_y, "E", "N", "EPSG:25832", "EPSG:25832"),
            _build_wcs_url_axis_variant(base, cov, min_x, min_y, max_x, max_y, "E", "N", "EPSG:25832"),
            _build_wcs_url_wgs84_variant(base, cov, west, south, east, north, "Long", "Lat"),
            _build_wcs_url_wgs84_variant(base, cov, west, south, east, north, "lon", "lat"),
        ]

    last_status = None
    last_detail = ""
    resp = None

    for idx, url in enumerate(candidate_urls, start=1):
        print(f"[WCS] Try {idx}/{len(candidate_urls)}")
        if notify:
            notify(f"WCS: Anfrage {idx}/{len(candidate_urls)}")
        try:
            resp = requests.get(url, timeout=WCS_REQUEST_TIMEOUT_S)
        except requests.ConnectionError as exc:
            raise WCSError(
                "WCS-Dienst nicht erreichbar. Bitte manuellen Upload versuchen."
            ) from exc
        except requests.Timeout as exc:
            raise WCSError(
                "WCS-Zeitueberschreitung. Bitte kleineren Bereich waehlen oder manuell hochladen."
            ) from exc

        if resp.status_code == 503:
            raise WCSError(
                "WCS-Dienst voruebergehend nicht verfuegbar. "
                "Bitte manuellen GeoTIFF-Upload versuchen."
            )

        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "")
            if "tiff" in content_type.lower() or "octet" in content_type.lower():
                break
            last_status = 200
            last_detail = (
                f"WCS hat kein GeoTIFF zurueckgegeben (Content-Type: {content_type}). "
                f"Antwort: {_extract_ows_exception_text(resp.text)}"
            )
            resp = None
            continue

        # Sachsen-Anhalt WCS currently tends to respond with a generic 500 for *any* GetCoverage.
        # Avoid hammering the service with many variants in that case; fall back to upload mode.
        if provider.key == "sachsen-anhalt" and resp.status_code == 500:
            last_status = resp.status_code
            # Some servers return plain text; keep it as-is to preserve the "Internal Server Error" signal.
            last_detail = (resp.text or "").strip() or "Internal Server Error"
            resp = None
            break

        last_status = resp.status_code
        last_detail = _extract_ows_exception_text(resp.text)
        resp = None

    if resp is None:
        if "InvalidParameterValue" in last_detail or "OGC Proxy" in last_detail:
            raise WCSError(
                "WCS-Proxy lehnt die Anfrageparameter ab (InvalidParameterValue). "
                "Bitte Auswahl etwas verschieben/verkleinern oder GeoTIFF manuell hochladen."
            )
        if provider.key == "sachsen-anhalt" and last_status == 500:
            raise WCSError(
                "Sachsen-Anhalt WCS liefert aktuell 'Internal Server Error'. "
                "Bitte vorerst DEM als GeoTIFF hochladen (Upload-Modus) und spaeter erneut testen."
            )
        raise WCSError(f"WCS-Fehler (HTTP {last_status}): {last_detail}")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tif")
    tmp.write(resp.content)
    tmp.close()

    size_kb = len(resp.content) / 1024
    print(f"[WCS] Tile downloaded {size_kb:.0f} KB -> {tmp.name}")
    return tmp.name


def _merge_tiles(tile_paths: list[str]) -> str:
    try:
        import rasterio
        from rasterio.merge import merge as rio_merge
    except Exception as exc:
        raise WCSError(
            "GeoTIFF-Kachelmerge benoetigt 'rasterio'. Bitte OSGeo4W-Umgebung nutzen "
            "oder rasterio in die Python-Umgebung installieren."
        ) from exc

    srcs = [rasterio.open(p) for p in tile_paths]
    try:
        mosaic, out_transform = rio_merge(srcs)
        profile = srcs[0].profile.copy()
        profile.update(
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            transform=out_transform,
        )

        out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tif")
        out_tmp.close()
        with rasterio.open(out_tmp.name, "w", **profile) as dst:
            dst.write(mosaic)
        return out_tmp.name
    finally:
        for src in srcs:
            src.close()
        for p in tile_paths:
            try:
                os.remove(p)
            except OSError:
                pass


def fetch_dem_from_wcs(
    south: float,
    west: float,
    north: float,
    east: float,
    progress_callback=None,
    provider_key: str | None = "auto",
) -> str:
    """Download a DGM1 GeoTIFF for the given WGS84 bbox (tiling+merge for large AOIs)."""
    def notify(msg: str):
        if progress_callback:
            progress_callback(msg)

    provider = _resolve_provider(south, west, north, east, provider_key)
    notify(f"Region erkannt: {provider.name}")
    min_x, min_y, max_x, max_y = _validate_and_transform(south, west, north, east, provider)

    def fetch_via_wcs() -> str:
        pending = list(_iter_tiles(min_x, min_y, max_x, max_y))
        print(f"[WCS] [{provider.key}] AOI split into {len(pending)} tile(s)")
        notify(f"WCS: Bereich in {len(pending)} Kachel(n) aufgeteilt")

        tile_paths = []
        processed = 0
        while pending:
            if processed > MAX_TILE_COUNT:
                raise BboxTooLargeError(
                    f"Auswahl erzeugt zu viele WCS-Kacheln ({processed}). "
                    "Bitte Bereich verkleinern oder GeoTIFF manuell hochladen."
                )

            tx0, ty0, tx1, ty1 = pending.pop(0)
            processed += 1
            print(f"[WCS] Tile {processed} (queue={len(pending)})")
            notify(f"WCS: Kachel {processed} (verbleibend: {len(pending)})")

            try:
                tile_paths.append(_fetch_single_tile(provider, tx0, ty0, tx1, ty1, notify=notify))
            except WCSError as exc:
                msg = str(exc)
                dx = tx1 - tx0
                dy = ty1 - ty0
                can_split = dx > MIN_TILE_SIDE_M or dy > MIN_TILE_SIDE_M
                is_param_issue = "InvalidParameterValue" in msg or "Proxy lehnt" in msg
                if can_split and is_param_issue:
                    print(
                        f"[WCS] Splitting rejected tile ({dx:.0f}m x {dy:.0f}m) "
                        f"due to proxy parameter error."
                    )
                    notify(
                        f"WCS: Kachel abgelehnt, splitte weiter ({dx:.0f}m x {dy:.0f}m)"
                    )
                    pending = _split_tile((tx0, ty0, tx1, ty1)) + pending
                    continue
                raise

        if len(tile_paths) == 1:
            notify("WCS: Einzelkachel geladen")
            return tile_paths[0]

        merged = _merge_tiles(tile_paths)
        print(f"[WCS] Merged {len(tile_paths)} tiles -> {merged}")
        notify(f"WCS: {len(tile_paths)} Kacheln zusammengefuehrt")
        return merged

    # Sachsen-Anhalt fallback: If the official WCS GetCoverage is down, allow a local DEM.
    if provider.key == "sachsen-anhalt" and provider.local_dem_path:
        try:
            return fetch_via_wcs()
        except WCSError as exc:
            msg = str(exc)
            looks_like_service_down = (
                "Internal Server Error" in msg
                or "nicht erreichbar" in msg
                or "voruebergehend nicht verfuegbar" in msg
                or "WCS-Fehler (HTTP 500)" in msg
            )
            if not looks_like_service_down:
                raise
            notify("WCS: Sachsen-Anhalt WCS down, nutze lokales DEM (ST_DEM_LOCAL_PATH)")
            return _clip_local_dem_utm32(
                local_path=provider.local_dem_path,
                min_x=min_x,
                min_y=min_y,
                max_x=max_x,
                max_y=max_y,
            )

    return fetch_via_wcs()
