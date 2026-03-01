from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path


def _ts() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _weather_cell_id(lat: float, lon: float, cell_km: float) -> str:
    km = max(0.1, float(cell_km))
    dlat = km / 111.32
    cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
    dlon = km / (111.32 * cos_lat)
    i_lat = int(math.floor((lat + 90.0) / dlat))
    i_lon = int(math.floor((lon + 180.0) / dlon))
    return f"{i_lat}:{i_lon}"


def _events_cell_cache_path(cell_cache_dir: Path, cell_id: str, cache_key: str) -> Path:
    safe_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(cell_id))
    return cell_cache_dir / f"cell_{safe_id}_{cache_key}.json"


@dataclass
class CellCandidate:
    cell_id: str
    cache_key: str
    events: list[dict]
    params: dict
    meta: dict
    precomputed_at_utc: str


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Repair/backfill cell_cache from existing field_cache JSON files.")
    p.add_argument("--cache-root", required=True, help="Root like data/events/sa_2km/icon2d_20230401_20231031")
    p.add_argument("--weather-cell-km", type=float, default=2.0)
    p.add_argument("--remove-empty-legacy", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=False)
    return p.parse_args()


def _is_better(new: CellCandidate, old: CellCandidate) -> bool:
    # Prefer non-empty and larger event sets, then newer timestamp.
    n_new = len(new.events or [])
    n_old = len(old.events or [])
    if n_new != n_old:
        return n_new > n_old
    return str(new.precomputed_at_utc or "") > str(old.precomputed_at_utc or "")


def _load_field_json(path: Path) -> tuple[str, CellCandidate] | None:
    # field_cache name pattern: <safe_field_id>_<cachekey>.json
    m = re.match(r"^(.+)_([0-9a-f]{12})\.json$", path.name, flags=re.IGNORECASE)
    if not m:
        return None
    cache_key = m.group(2).lower()
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        params = obj.get("params") or {}
        point = str(params.get("points") or "").strip()
        if "," not in point:
            return None
        lat_s, lon_s = point.split(",", 1)
        lat = float(lat_s)
        lon = float(lon_s)
        cell_id = _weather_cell_id(lat, lon, 2.0)
        events = obj.get("events") or []
        meta = obj.get("meta") or {}
        precomputed = str(obj.get("precomputed_at_utc") or "")
        cand = CellCandidate(
            cell_id=cell_id,
            cache_key=cache_key,
            events=[e for e in events if isinstance(e, dict)],
            params={
                "source": params.get("source"),
                "start": params.get("start"),
                "end": params.get("end"),
            },
            meta=meta if isinstance(meta, dict) else {},
            precomputed_at_utc=precomputed,
        )
        return f"{cell_id}|{cache_key}", cand
    except Exception:
        return None


def main() -> int:
    args = _parse_args()
    root = Path(args.cache_root).resolve()
    field_cache = root / "field_cache"
    cell_cache = root / "cell_cache"
    if not field_cache.exists():
        raise SystemExit(f"field_cache not found: {field_cache}")
    cell_cache.mkdir(parents=True, exist_ok=True)

    removed_legacy = 0
    if bool(args.remove_empty_legacy):
        for p in cell_cache.glob("cell_*"):
            if not p.is_file():
                continue
            # Old Windows ADS side-effect files had no extension and size 0.
            if p.suffix == "" and p.stat().st_size == 0:
                if not bool(args.dry_run):
                    p.unlink(missing_ok=True)
                removed_legacy += 1

    grouped: dict[str, CellCandidate] = {}
    scanned = 0
    for fp in field_cache.glob("*.json"):
        scanned += 1
        item = _load_field_json(fp)
        if item is None:
            continue
        key, cand = item
        old = grouped.get(key)
        if old is None or _is_better(cand, old):
            grouped[key] = cand

    written = 0
    for key, cand in grouped.items():
        out = _events_cell_cache_path(cell_cache, cand.cell_id, cand.cache_key)
        payload = {
            "cell_id": cand.cell_id,
            "weather_cell_km": float(args.weather_cell_km),
            "params": cand.params,
            "events": cand.events,
            "meta": cand.meta,
            "precomputed_at_utc": cand.precomputed_at_utc or _ts(),
            "repaired_at_utc": _ts(),
            "repaired_from": "field_cache",
        }
        if not bool(args.dry_run):
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        written += 1

    summary = {
        "finished_at_utc": _ts(),
        "cache_root": str(root),
        "field_cache": str(field_cache),
        "cell_cache": str(cell_cache),
        "field_files_scanned": scanned,
        "cell_groups": len(grouped),
        "cell_files_written": written,
        "legacy_empty_removed": removed_legacy,
        "dry_run": bool(args.dry_run),
    }
    out_meta = root / "repair_cell_cache_from_field_cache.meta.json"
    if not bool(args.dry_run):
        out_meta.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
