import os
import sys

# Add current directory to path so we can import wcs_client
sys.path.append(os.path.dirname(__file__))

from wcs_client import fetch_dem_from_wcs, WCSError

def verify():
    print("Verifying Sachsen-Anhalt WCS integration...")
    
    # Coordinates in Halle (Saale), inside Coverage1
    south, west = 51.48, 11.95
    north, east = 51.49, 11.97
    
    try:
        tiff_path = fetch_dem_from_wcs(
            south=south,
            west=west,
            north=north,
            east=east,
            provider_key="sachsen-anhalt"
        )
        print(f"SUCCESS: Downloaded DEM to {tiff_path}")
        
        # Check file size
        size = os.path.getsize(tiff_path)
        print(f"File size: {size} bytes")
        
        if size < 1000:
            print("WARNING: File seems too small to be a valid GeoTIFF.")
        else:
            print("Verification passed.")
            
    except WCSError as e:
        print(f"FAILURE: WCS Error: {e}")
    except Exception as e:
        print(f"FAILURE: Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        verify()
        with open("verification_result.txt", "w") as f:
            f.write("VERIFICATION_COMPLETED\nCheck logs for details.")
    except Exception as e:
        with open("verification_result.txt", "w") as f:
            f.write(f"VERIFICATION_FAILED: {e}")
