"""Quick smoke test for the processing pipeline."""

import os
import json
from create_mock_dem import create_mock_dem
from processing import analyze_dem


def main():
    dem_file = "mock_dem_halle.tif"

    # Generate mock DEM if not present
    if not os.path.exists(dem_file):
        print("Generating mock DEM...")
        create_mock_dem(dem_file)

    print(f"\nRunning flow-accumulation analysis on {dem_file}...")
    result = analyze_dem(dem_file)

    n_features = len(result.get("features", []))
    print(f"\n✓ Analysis complete – {n_features} stream segments found.")

    # Dump a small preview
    if n_features > 0:
        preview = json.dumps(result["features"][0], indent=2)[:500]
        print(f"\nFirst feature (preview):\n{preview}")
    else:
        print("\n⚠ No features returned. Try lowering the threshold.")


if __name__ == "__main__":
    main()
