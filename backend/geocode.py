from __future__ import annotations

import os
import threading
import time
from typing import Any

import requests


NOMINATIM_URL = os.getenv("NOMINATIM_URL", "https://nominatim.openstreetmap.org/search")
NOMINATIM_USER_AGENT = os.getenv(
    "NOMINATIM_USER_AGENT",
    "hydrowatch (local dev; set NOMINATIM_USER_AGENT for production)",
)

# Nominatim usage policy: keep request rate low. We enforce a simple 1 req/s gate.
_LOCK = threading.Lock()
_LAST_CALL = 0.0
_CACHE: dict[tuple, tuple[float, list[dict[str, Any]]]] = {}
_CACHE_TTL_S = float(os.getenv("GEOCODE_CACHE_TTL_S", str(24 * 3600)))


def _cache_get(key: tuple) -> list[dict[str, Any]] | None:
    now = time.time()
    item = _CACHE.get(key)
    if not item:
        return None
    ts, data = item
    if now - ts > _CACHE_TTL_S:
        _CACHE.pop(key, None)
        return None
    return data


def _cache_set(key: tuple, data: list[dict[str, Any]]) -> None:
    _CACHE[key] = (time.time(), data)


def geocode(
    q: str,
    limit: int = 6,
    countrycodes: str = "de",
    *,
    viewbox: tuple[float, float, float, float] | None = None,  # west,south,east,north
) -> list[dict[str, Any]]:
    q = (q or "").strip()
    if not q:
        return []

    cache_key = (q.lower(), int(limit), countrycodes or "", tuple(viewbox) if viewbox else None)
    with _LOCK:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    global _LAST_CALL
    wait_s = 0.0
    with _LOCK:
        now = time.time()
        wait_s = max(0.0, 1.0 - (now - _LAST_CALL))
        # Reserve the next slot to keep global rate <= 1 req/s across threads.
        _LAST_CALL = now + wait_s
    if wait_s > 0:
        time.sleep(wait_s)

    params = {
        "q": q,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": max(1, min(int(limit), 12)),
    }
    if countrycodes:
        params["countrycodes"] = countrycodes
    if viewbox and len(viewbox) == 4:
        west, south, east, north = viewbox
        params["viewbox"] = f"{west:.8f},{south:.8f},{east:.8f},{north:.8f}"
        # Important UX choice:
        # Do NOT set "bounded=1" here. With bounded=1, Nominatim will return *only* results inside the AOI bbox,
        # which often yields "Keine Treffer" for small AOIs (even when the query is correct).
        # Keeping only "viewbox" biases results towards the AOI without excluding global matches.

    headers = {
        "User-Agent": NOMINATIM_USER_AGENT,
        "Accept-Language": "de,en;q=0.7",
    }

    r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    out = []
    for item in data or []:
        try:
            out.append(
                {
                    "display_name": item.get("display_name"),
                    "lat": float(item.get("lat")),
                    "lon": float(item.get("lon")),
                    "boundingbox": item.get("boundingbox"),  # [south, north, west, east] strings
                    "class": item.get("class"),
                    "type": item.get("type"),
                    "importance": item.get("importance"),
                    "address": item.get("address"),
                }
            )
        except Exception:
            continue
    with _LOCK:
        _cache_set(cache_key, out)
    return out
