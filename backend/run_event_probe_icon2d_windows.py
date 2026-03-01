from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any

import requests


def _ts() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append_log(log_path: Path, msg: str) -> None:
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _parse_windows(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        s, e = part.split(":", 1)
        s = s.strip()
        e = e.strip()
        if s and e:
            out.append((s, e))
    if not out:
        raise RuntimeError("Keine gueltigen Fenster gefunden. Format: YYYY-MM-DD:YYYY-MM-DD,...")
    return out


def _centroid_latlon(feature: dict[str, Any]) -> tuple[float, float]:
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or []
    if geom.get("type") == "MultiPolygon":
        ring = coords[0][0]
    else:
        ring = coords[0]
    lat = sum(float(p[1]) for p in ring) / len(ring)
    lon = sum(float(p[0]) for p in ring) / len(ring)
    return lat, lon


def _field_id(feature: dict[str, Any], idx: int) -> str:
    props = feature.get("properties") or {}
    for key in ("schlag_id", "field_id", "id", "ID", "flik"):
        v = props.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return f"field_{idx:03d}"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "field_id",
        "lat",
        "lon",
        "source",
        "window_start",
        "window_end",
        "http_status",
        "events_count",
        "sources_used",
        "note",
        "top_event_start",
        "top_event_end",
        "top_event_peak",
        "top_event_severity",
        "top_event_max1h_mm",
        "top_event_max6h_mm",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        if rows:
            w.writerows(rows)


def main() -> int:
    p = argparse.ArgumentParser(description="Probe run for icon2d event windows with progress + logfile.")
    p.add_argument("--fields-geojson", default=str(Path("paper") / "input" / "schlaege_sa_spatial_10.geojson"))
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--source", default="icon2d")
    p.add_argument(
        "--windows",
        default="2023-04-01:2023-10-31,2024-04-01:2024-10-31,2025-04-01:2025-10-31",
        help="Comma list: start:end,start:end",
    )
    p.add_argument("--timeout-s", type=int, default=240)
    p.add_argument("--out-csv", default=str(Path("paper") / "exports" / "event_probe_icon2d_windows.csv"))
    p.add_argument("--log-file", default="")
    args = p.parse_args()

    run_id = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_csv = Path(args.out_csv).resolve()
    log_file = Path(args.log_file).resolve() if str(args.log_file).strip() else out_csv.with_suffix(f".{run_id}.log")

    geo = Path(args.fields_geojson).resolve()
    if not geo.exists():
        raise SystemExit(f"GeoJSON fehlt: {geo}")

    windows = _parse_windows(args.windows)
    obj = json.loads(geo.read_text(encoding="utf-8"))
    features = obj.get("features") or []
    if not features:
        raise SystemExit("Keine Features im GeoJSON gefunden.")

    _append_log(log_file, f"start run_id={run_id}")
    _append_log(log_file, f"geojson={geo}")
    _append_log(log_file, f"out_csv={out_csv}")
    _append_log(log_file, f"source={args.source} windows={windows}")

    rows: list[dict[str, Any]] = []
    total = len(features) * len(windows)
    done = 0
    ok = 0
    err = 0

    for i, f in enumerate(features, start=1):
        fid = _field_id(f, i)
        lat, lon = _centroid_latlon(f)
        for ws, we in windows:
            done += 1
            _append_log(log_file, f"[{done}/{total}] field={fid} window={ws}..{we}")
            params = {
                "points": f"{lat:.5f},{lon:.5f}",
                "start": ws,
                "end": we,
                "source": args.source,
            }
            try:
                r = requests.get(f"{args.api_base_url.rstrip('/')}/abflussatlas/weather/events", params=params, timeout=int(args.timeout_s))
                body = r.json() if r.content else {}
                meta = body.get("meta") or {}
                merged = ((body.get("events") or {}).get("mergedTop") or [])
                first = merged[0] if merged else {}
                rows.append(
                    {
                        "field_id": fid,
                        "lat": round(lat, 5),
                        "lon": round(lon, 5),
                        "source": args.source,
                        "window_start": ws,
                        "window_end": we,
                        "http_status": r.status_code,
                        "events_count": len(merged),
                        "sources_used": "|".join(meta.get("sourcesUsed") or []),
                        "note": " | ".join(meta.get("notes") or []),
                        "top_event_start": first.get("start"),
                        "top_event_end": first.get("end"),
                        "top_event_peak": first.get("peak_ts"),
                        "top_event_severity": first.get("severity"),
                        "top_event_max1h_mm": first.get("max_1h_mm"),
                        "top_event_max6h_mm": first.get("max_6h_mm"),
                    }
                )
                if int(r.status_code) == 200:
                    ok += 1
                else:
                    err += 1
                    _append_log(log_file, f"non-200 status={r.status_code} field={fid} window={ws}..{we}")
            except Exception as exc:
                err += 1
                rows.append(
                    {
                        "field_id": fid,
                        "lat": round(lat, 5),
                        "lon": round(lon, 5),
                        "source": args.source,
                        "window_start": ws,
                        "window_end": we,
                        "http_status": "ERR",
                        "events_count": 0,
                        "sources_used": "",
                        "note": str(exc),
                        "top_event_start": "",
                        "top_event_end": "",
                        "top_event_peak": "",
                        "top_event_severity": "",
                        "top_event_max1h_mm": "",
                        "top_event_max6h_mm": "",
                    }
                )
                _append_log(log_file, f"error field={fid} window={ws}..{we}: {exc}")
            _write_csv(out_csv, rows)

    # summary by window
    by_window: dict[str, dict[str, int]] = {}
    for r in rows:
        k = f"{r.get('window_start')}..{r.get('window_end')}"
        if k not in by_window:
            by_window[k] = {"ok": 0, "err": 0, "events": 0}
        if str(r.get("http_status")) == "200":
            by_window[k]["ok"] += 1
        else:
            by_window[k]["err"] += 1
        try:
            by_window[k]["events"] += int(r.get("events_count") or 0)
        except Exception:
            pass

    _append_log(log_file, f"done rows={len(rows)} ok={ok} err={err}")
    _append_log(log_file, f"summary={by_window}")
    _append_log(log_file, f"csv={out_csv}")
    _append_log(log_file, f"log={log_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

