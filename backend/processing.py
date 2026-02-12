"""
Hydrology processing engine using pysheds.

Workflow:
  1. Load DEM raster
  2. Fill depressions (pit filling)
  3. Resolve flats
  4. Compute flow direction (D8)
  5. Compute flow accumulation
  6. Extract river/stream network as GeoJSON
  7. Reproject coordinates to WGS84 (EPSG:4326)
"""

from pysheds.grid import Grid
import numpy as np
import rasterio
from pyproj import Transformer, CRS


def _reproject_geojson(geojson: dict, src_crs_str: str) -> dict:
    """
    Reproject all coordinates in a GeoJSON FeatureCollection
    from src_crs to EPSG:4326 (WGS84) in-place.
    """
    src_crs = CRS(src_crs_str)
    dst_crs = CRS("EPSG:4326")

    if src_crs == dst_crs:
        return geojson  # already in WGS84

    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)

    def transform_coords(coords):
        """Transform a list of [x, y] coordinate pairs."""
        return [list(transformer.transform(x, y)) for x, y in coords]

    for feature in geojson.get("features", []):
        geom = feature.get("geometry", {})
        gtype = geom.get("type", "")
        if gtype == "LineString":
            geom["coordinates"] = transform_coords(geom["coordinates"])
        elif gtype == "MultiLineString":
            geom["coordinates"] = [
                transform_coords(line) for line in geom["coordinates"]
            ]

    return geojson


def analyze_dem(file_path: str, threshold: int = 200,
                progress_callback=None) -> dict:
    """
    Run full flow-accumulation analysis on a GeoTIFF DEM.

    Parameters
    ----------
    file_path : str
        Path to a single-band GeoTIFF elevation raster.
    threshold : int
        Minimum accumulation value to include a cell in the
        stream network.
    progress_callback : callable, optional
        Called with (step: int, total: int, message: str) at
        each major processing step.

    Returns
    -------
    dict
        GeoJSON FeatureCollection of stream-line geometries
        in WGS84 (EPSG:4326).
    """

    def progress(step, total, msg):
        print(f"  [{step}/{total}] {msg}")
        if progress_callback:
            progress_callback(step, total, msg)

    # ---- Detect source CRS -------------------------------------------
    progress(1, 7, "CRS wird erkannt…")
    with rasterio.open(file_path) as src:
        src_crs = str(src.crs) if src.crs else None
    print(f"  Source CRS: {src_crs}")

    # ---- Load --------------------------------------------------------
    progress(2, 7, "DEM wird geladen…")
    grid = Grid.from_raster(file_path)
    dem  = grid.read_raster(file_path)

    print(f"  DEM shape : {dem.shape}")
    print(f"  DEM range : {float(np.nanmin(dem)):.1f} – {float(np.nanmax(dem)):.1f}")

    # ---- Condition ----------------------------------------------------
    progress(3, 7, "Senken werden gefüllt…")
    pit_filled   = grid.fill_depressions(dem)
    flats_resolved = grid.resolve_flats(pit_filled)

    # ---- Flow direction (D8) -----------------------------------------
    progress(4, 7, "Fließrichtung wird berechnet (D8)…")
    dirmap = (64, 128, 1, 2, 4, 8, 16, 32)
    fdir   = grid.flowdir(flats_resolved, dirmap=dirmap)

    # ---- Flow accumulation -------------------------------------------
    progress(5, 7, "Fließakkumulation wird berechnet…")
    acc = grid.accumulation(fdir, dirmap=dirmap)

    print(f"  Accumulation range : {float(np.nanmin(acc)):.0f} – {float(np.nanmax(acc)):.0f}")

    # ---- Extract network ---------------------------------------------
    progress(6, 7, "Fließnetzwerk wird extrahiert…")
    branches = grid.extract_river_network(fdir, acc > threshold, dirmap=dirmap)

    # ---- Reproject to WGS84 ------------------------------------------
    if src_crs:
        progress(7, 7, "Koordinaten werden transformiert…")
        branches = _reproject_geojson(branches, src_crs)

    n = len(branches.get('features', []))
    print(f"  Features: {n}")
    return branches
