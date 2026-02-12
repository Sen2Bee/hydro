"""
Generate a synthetic DEM (GeoTIFF) for the Halle-Mansfeld region.

The terrain slopes from west (Harz foothills, ~250 m) to east
(Saale lowlands, ~80 m) with rolling hills and a central valley.
"""

import numpy as np
import rasterio
from rasterio.transform import from_origin


def create_mock_dem(filename: str = "mock_dem_halle.tif"):
    # --- Geographic extent -------------------------------------------
    west, north = 11.40, 51.60          # top-left corner
    pixel_size  = 0.001                  # ~100 m
    cols, rows  = 300, 150               # ~30 km × 15 km

    # --- Build elevation surface ------------------------------------
    x = np.linspace(0, 1, cols)          # 0 = west, 1 = east
    y = np.linspace(0, 1, rows)          # 0 = north, 1 = south
    X, Y = np.meshgrid(x, y)

    # General west→east slope (Harz foothills → Saale plain)
    base = 250 - 170 * X                 # 250 m → 80 m

    # Rolling hills
    hills = (20 * np.sin(8 * np.pi * X) * np.cos(6 * np.pi * Y)
           + 10 * np.sin(12 * np.pi * X + 1.3))

    # Central river valley trending SW → NE
    valley_center = 0.5 + 0.15 * np.sin(3 * np.pi * X)
    dist_to_valley = np.abs(Y - valley_center)
    valley = -40 * np.exp(-(dist_to_valley ** 2) / 0.005)

    elevation = (base + hills + valley).astype(np.float32)

    # --- Write GeoTIFF -----------------------------------------------
    transform = from_origin(west, north, pixel_size, pixel_size)
    with rasterio.open(
        filename,
        "w",
        driver="GTiff",
        height=rows,
        width=cols,
        count=1,
        dtype=elevation.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(elevation, 1)

    print(f"Created {filename}  ({cols}×{rows}, "
          f"{float(elevation.min()):.0f}–{float(elevation.max()):.0f} m)")


if __name__ == "__main__":
    create_mock_dem()
