from __future__ import annotations

import time
import xml.etree.ElementTree as ET

import requests


def _ms(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)


def _safe_snip(text: str, n: int = 240) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ").strip()
    return text[:n]


def _extract_exception_text(xml_text: str) -> str:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return _safe_snip(xml_text)
    for elem in root.iter():
        if str(elem.tag).endswith("ExceptionText") and elem.text:
            return _safe_snip(elem.text)
    return _safe_snip(xml_text)


def run_wcs_selftest(
    *,
    provider_key: str,
    provider_name: str,
    wcs_base: str,
    coverage_id: str,
    test_utm32_bbox: tuple[float, float, float, float],
    timeout_s: int = 25,
) -> dict:
    """
    Minimal WCS health test:
      1) GetCapabilities
      2) DescribeCoverage
      3) GetCoverage (small bbox)

    test_utm32_bbox is (min_x, min_y, max_x, max_y) in EPSG:25832.
    """
    min_x, min_y, max_x, max_y = test_utm32_bbox
    out: dict = {
        "provider": {"key": provider_key, "name": provider_name},
        "wcs": {"base": wcs_base, "coverage_id": coverage_id},
        "steps": [],
    }

    # 1) Capabilities
    t0 = time.perf_counter()
    cap_url = f"{wcs_base}?SERVICE=WCS&REQUEST=GetCapabilities&VERSION=2.0.1"
    try:
        r = requests.get(cap_url, timeout=timeout_s)
        ok = r.status_code == 200 and "xml" in (r.headers.get("content-type", "").lower())
        out["steps"].append(
            {
                "name": "GetCapabilities",
                "ok": bool(ok),
                "status": int(r.status_code),
                "elapsed_ms": _ms(t0),
                "content_type": r.headers.get("content-type"),
                "detail": "" if ok else _extract_exception_text(r.text),
            }
        )
    except Exception as exc:
        out["steps"].append(
            {
                "name": "GetCapabilities",
                "ok": False,
                "status": None,
                "elapsed_ms": _ms(t0),
                "detail": str(exc),
            }
        )
        return out

    # 2) DescribeCoverage
    t0 = time.perf_counter()
    desc_url = f"{wcs_base}?SERVICE=WCS&REQUEST=DescribeCoverage&VERSION=2.0.1&COVERAGEID={coverage_id}"
    try:
        r = requests.get(desc_url, timeout=timeout_s)
        ok = r.status_code == 200 and "xml" in (r.headers.get("content-type", "").lower())
        out["steps"].append(
            {
                "name": "DescribeCoverage",
                "ok": bool(ok),
                "status": int(r.status_code),
                "elapsed_ms": _ms(t0),
                "content_type": r.headers.get("content-type"),
                "detail": "" if ok else _extract_exception_text(r.text),
            }
        )
    except Exception as exc:
        out["steps"].append(
            {
                "name": "DescribeCoverage",
                "ok": False,
                "status": None,
                "elapsed_ms": _ms(t0),
                "detail": str(exc),
            }
        )
        return out

    # 3) GetCoverage (variants)
    variants = [
        (
            "subset_url_crs",
            (
                f"{wcs_base}?SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage"
                f"&COVERAGEID={coverage_id}&FORMAT=image/tiff"
                f"&SUBSET=x({min_x:.2f},{max_x:.2f})&SUBSET=y({min_y:.2f},{max_y:.2f})"
                f"&SUBSETTINGCRS=http://www.opengis.net/def/crs/EPSG/0/25832"
            ),
        ),
        (
            "subset_epsg",
            (
                f"{wcs_base}?SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage"
                f"&COVERAGEID={coverage_id}&FORMAT=image/tiff"
                f"&SUBSET=x({min_x:.2f},{max_x:.2f})&SUBSET=y({min_y:.2f},{max_y:.2f})"
                f"&SUBSETTINGCRS=EPSG:25832&OUTPUTCRS=EPSG:25832"
            ),
        ),
        (
            "axis_EN",
            (
                f"{wcs_base}?SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage"
                f"&COVERAGEID={coverage_id}&FORMAT=image/tiff"
                f"&SUBSET=E({min_x:.2f},{max_x:.2f})&SUBSET=N({min_y:.2f},{max_y:.2f})"
                f"&SUBSETTINGCRS=EPSG:25832&OUTPUTCRS=EPSG:25832"
            ),
        ),
    ]

    for label, url in variants:
        t0 = time.perf_counter()
        try:
            r = requests.get(url, timeout=timeout_s)
            ct = (r.headers.get("content-type") or "").lower()
            ok = r.status_code == 200 and ("tiff" in ct or "octet" in ct)
            out["steps"].append(
                {
                    "name": f"GetCoverage:{label}",
                    "ok": bool(ok),
                    "status": int(r.status_code),
                    "elapsed_ms": _ms(t0),
                    "content_type": r.headers.get("content-type"),
                    "bytes": int(len(r.content or b"")),
                    "detail": "" if ok else _extract_exception_text(r.text),
                }
            )
            if ok:
                break
        except Exception as exc:
            out["steps"].append(
                {
                    "name": f"GetCoverage:{label}",
                    "ok": False,
                    "status": None,
                    "elapsed_ms": _ms(t0),
                    "detail": str(exc),
                }
            )

    return out

