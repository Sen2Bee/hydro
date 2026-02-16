from __future__ import annotations

import xml.etree.ElementTree as ET

import requests


def list_wms_layers(url: str, timeout_s: int = 30) -> list[dict]:
    """
    Fetch and parse a WMS GetCapabilities document and return a flat list of layers.

    Returns items: {name, title}
    """
    if "request=getcapabilities" not in url.lower():
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}SERVICE=WMS&REQUEST=GetCapabilities"

    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    ns_wms = "http://www.opengis.net/wms"
    L = f"{{{ns_wms}}}Layer"
    N = f"{{{ns_wms}}}Name"
    T = f"{{{ns_wms}}}Title"

    out: list[dict] = []
    for layer in root.findall(f".//{L}"):
        name_el = layer.find(N)
        title_el = layer.find(T)
        if name_el is None or title_el is None:
            continue
        name = (name_el.text or "").strip()
        title = (title_el.text or "").strip()
        if not name:
            continue
        out.append({"name": name, "title": title})

    # stable output
    out.sort(key=lambda x: (x["title"].lower(), x["name"].lower()))
    return out

