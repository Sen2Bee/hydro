"""
Quick test to determine which GetCoverage URL variant works for Sachsen-Anhalt WCS.
"""
import requests
import sys

BASE = "https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_DGM1_WCS_OpenData/guest"
COV = "Coverage1"

# Small bbox in Halle area, EPSG:25832 (500m x 500m)
MIN_X, MIN_Y = 700000, 5750000
MAX_X, MAX_Y = 700500, 5750500

variants = [
    (
        "OGC CRS URI, axes x/y",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"x({MIN_X},{MAX_X})", f"y({MIN_Y},{MAX_Y})"],
            "SUBSETTINGCRS": "http://www.opengis.net/def/crs/EPSG/0/25832",
        },
    ),
    (
        "EPSG shorthand, axes x/y",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"x({MIN_X},{MAX_X})", f"y({MIN_Y},{MAX_Y})"],
            "SUBSETTINGCRS": "EPSG:25832",
        },
    ),
    (
        "EPSG shorthand + OUTPUTCRS, axes x/y",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"x({MIN_X},{MAX_X})", f"y({MIN_Y},{MAX_Y})"],
            "SUBSETTINGCRS": "EPSG:25832", "OUTPUTCRS": "EPSG:25832",
        },
    ),
    (
        "OGC CRS URI + OUTPUTCRS, axes x/y",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"x({MIN_X},{MAX_X})", f"y({MIN_Y},{MAX_Y})"],
            "SUBSETTINGCRS": "http://www.opengis.net/def/crs/EPSG/0/25832",
            "OUTPUTCRS": "http://www.opengis.net/def/crs/EPSG/0/25832",
        },
    ),
    (
        "Axes E/N, EPSG shorthand",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"E({MIN_X},{MAX_X})", f"N({MIN_Y},{MAX_Y})"],
            "SUBSETTINGCRS": "EPSG:25832", "OUTPUTCRS": "EPSG:25832",
        },
    ),
    (
        "No SUBSETTINGCRS (native), axes x/y",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"x({MIN_X},{MAX_X})", f"y({MIN_Y},{MAX_Y})"],
        },
    ),
    (
        "WGS84, axes Long/Lat",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"Long(11.95,11.96)", f"Lat(51.85,51.86)"],
            "SUBSETTINGCRS": "EPSG:4326", "OUTPUTCRS": "EPSG:4326",
        },
    ),
    (
        "WGS84, axes lon/lat",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"lon(11.95,11.96)", f"lat(51.85,51.86)"],
            "SUBSETTINGCRS": "EPSG:4326", "OUTPUTCRS": "EPSG:4326",
        },
    ),
    (
        "WGS84 OGC URI, axes Long/Lat",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"Long(11.95,11.96)", f"Lat(51.85,51.86)"],
            "SUBSETTINGCRS": "http://www.opengis.net/def/crs/EPSG/0/4326",
            "OUTPUTCRS": "http://www.opengis.net/def/crs/EPSG/0/4326",
        },
    ),
    (
        "Smaller tile 100m, OGC CRS URI",
        {
            "SERVICE": "WCS", "VERSION": "2.0.1", "REQUEST": "GetCoverage",
            "COVERAGEID": COV, "FORMAT": "image/tiff",
            "SUBSET": [f"x({MIN_X},{MIN_X+100})", f"y({MIN_Y},{MIN_Y+100})"],
            "SUBSETTINGCRS": "http://www.opengis.net/def/crs/EPSG/0/25832",
        },
    ),
]


def build_url(params: dict) -> str:
    """Build URL manually to handle duplicate SUBSET keys."""
    parts = [f"{BASE}?"]
    for k, v in params.items():
        if k == "SUBSET":
            for sv in v:
                parts.append(f"SUBSET={sv}&")
        else:
            parts.append(f"{k}={v}&")
    return "".join(parts).rstrip("&")


if __name__ == "__main__":
    print(f"Testing {len(variants)} WCS URL variants against Sachsen-Anhalt DGM1...\n")
    for i, (label, params) in enumerate(variants, 1):
        url = build_url(params)
        print(f"[{i}/{len(variants)}] {label}")
        print(f"  URL: {url[:120]}...")
        try:
            resp = requests.get(url, timeout=30)
            ct = resp.headers.get("Content-Type", "?")
            is_tiff = "tiff" in ct.lower() or "octet" in ct.lower()
            status = "SUCCESS" if resp.status_code == 200 and is_tiff else "FAIL"
            print(f"  {status}  HTTP {resp.status_code}  CT: {ct}  Size: {len(resp.content)} bytes")
            if resp.status_code != 200 or not is_tiff:
                body = resp.text[:200] if resp.text else "(empty)"
                print(f"  Body: {body}")
            if status == "SUCCESS":
                # Save a sample tile
                fname = f"test_st_tile_{i}.tif"
                with open(fname, "wb") as f:
                    f.write(resp.content)
                print(f"  -> Saved as {fname}")
        except Exception as exc:
            print(f"  ERROR: {exc}")
        print()
