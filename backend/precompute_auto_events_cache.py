from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import time
from pathlib import Path
from typing import Any

import requests

from run_field_event_batch import (
    _events_cache_key,
    _events_cache_path,
    _field_centroid_latlon,
    _load_fields_geojson,
    _parse_auto_events_payload,
)


def _ts() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    cols = [
        "field_id",
        "status",
        "events_count",
        "source",
        "window_start",
        "window_end",
        "http_status",
        "note",
        "cache_path",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def _is_throttle(note: str) -> bool:
    t = (note or "").lower()
    if "too many requests" in t:
        return True
    if re.search(r"(?:^|\\D)429(?:\\D|$)", t):
        return True
    if "http 429" in t or "status 429" in t:
        return True
    return False


def _looks_backend_down(note: str) -> bool:
    t = (note or "").lower()
    if not t:
        return False
    markers = (
        "failed to establish a new connection",
        "connection refused",
        "max retries exceeded",
        "winerror 10061",
        "target machine actively refused",
        "newconnectionerror",
        "connection aborted",
    )
    return any(m in t for m in markers)


def _backend_health_ok(api_base_url: str, health_path: str, timeout_s: float, retries: int) -> bool:
    base = str(api_base_url or "").rstrip("/")
    hp = str(health_path or "/openapi.json")
    url = f"{base}{hp if hp.startswith('/') else '/' + hp}"
    tries = max(1, int(retries))
    for _ in range(tries):
        try:
            r = requests.get(url, timeout=max(0.5, float(timeout_s)))
            if int(r.status_code) == 200:
                return True
        except Exception:
            pass
    return False


def _parse_iso_utc(value: str) -> dt.datetime | None:
    v = str(value or "").strip()
    if not v:
        return None
    try:
        if v.endswith("Z"):
            return dt.datetime.fromisoformat(v[:-1] + "+00:00")
        x = dt.datetime.fromisoformat(v)
        if x.tzinfo is None:
            return x.replace(tzinfo=dt.timezone.utc)
        return x
    except Exception:
        return None


def _split_time_window_utc(start_iso: str, end_iso: str, max_hours: int = 4320) -> list[tuple[str, str]]:
    s = _parse_iso_utc(start_iso)
    e = _parse_iso_utc(end_iso)
    if s is None or e is None:
        return [(start_iso, end_iso)]
    if e <= s:
        e = s + dt.timedelta(hours=1)
    step = dt.timedelta(hours=max(1, int(max_hours)))
    out: list[tuple[str, str]] = []
    cur = s
    while cur < e:
        nxt = min(e, cur + step)
        out.append(
            (
                cur.astimezone(dt.timezone.utc).date().isoformat(),
                nxt.astimezone(dt.timezone.utc).date().isoformat(),
            )
        )
        cur = nxt
    return out


def _weather_cell_id(lat: float, lon: float, cell_km: float) -> str:
    km = max(0.1, float(cell_km))
    dlat = km / 111.32
    cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
    dlon = km / (111.32 * cos_lat)
    i_lat = int(math.floor((lat + 90.0) / dlat))
    i_lon = int(math.floor((lon + 180.0) / dlon))
    return f"{i_lat}:{i_lon}"


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(max(0.0, min(1.0, a))))


def _weather_cell_center(cell_id: str, cell_km: float) -> tuple[float, float] | None:
    try:
        i_lat_s, i_lon_s = str(cell_id).split(":", 1)
        i_lat = int(i_lat_s)
        i_lon = int(i_lon_s)
    except Exception:
        return None
    km = max(0.1, float(cell_km))
    dlat = km / 111.32
    lat = -90.0 + (i_lat + 0.5) * dlat
    cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
    dlon = km / (111.32 * cos_lat)
    lon = -180.0 + (i_lon + 0.5) * dlon
    return (lat, lon)


def _events_cell_cache_path(cell_cache_dir: Path, cell_id: str, cache_key: str) -> Path:
    # IMPORTANT (Windows): ":" in filenames creates NTFS ADS and results in unusable cache files.
    safe_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(cell_id))
    return cell_cache_dir / f"cell_{safe_id}_{cache_key}.json"


def _event_to_dict(ev: Any) -> dict[str, Any]:
    if isinstance(ev, dict):
        return dict(ev)
    if hasattr(ev, "__dict__"):
        return dict(getattr(ev, "__dict__", {}) or {})
    return {}


def _load_neighbor_cached_events(
    *,
    cell_cache_dir: Path,
    cache_key: str,
    lat: float,
    lon: float,
    source_cell_id: str,
    weather_cell_km: float,
    neighbor_max_km: float,
) -> tuple[list[dict[str, Any]], str | None, float | None]:
    max_km = max(0.0, float(neighbor_max_km))
    if max_km <= 0.0:
        return [], None, None
    try:
        src_i_lat, src_i_lon = [int(x) for x in str(source_cell_id).split(":", 1)]
    except Exception:
        return [], None, None
    step_limit = max(1, int(math.ceil(max_km / max(0.1, float(weather_cell_km)))))
    candidates: list[tuple[float, str]] = []
    for di in range(-step_limit, step_limit + 1):
        for dj in range(-step_limit, step_limit + 1):
            if di == 0 and dj == 0:
                continue
            cid = f"{src_i_lat + di}:{src_i_lon + dj}"
            ctr = _weather_cell_center(cid, weather_cell_km)
            if ctr is None:
                continue
            dist_km = _haversine_km(lat, lon, float(ctr[0]), float(ctr[1]))
            if dist_km <= max_km:
                candidates.append((dist_km, cid))
    candidates.sort(key=lambda x: x[0])
    for dist_km, cid in candidates:
        cpath = _events_cell_cache_path(cell_cache_dir, cid, cache_key)
        if not cpath.exists():
            continue
        try:
            data = json.loads(cpath.read_text(encoding="utf-8"))
            evs = []
            for it in (data.get("events") or []):
                if not isinstance(it, dict):
                    continue
                item = dict(it)
                src = str(item.get("event_source") or "auto")
                item["event_source"] = f"{src}|neighbor_2km"
                item["event_neighbor_cell_id"] = cid
                item["event_neighbor_distance_km"] = round(float(dist_km), 3)
                evs.append(item)
            if evs:
                return evs, cid, float(dist_km)
        except Exception:
            continue
    return [], None, None


def main() -> int:
    p = argparse.ArgumentParser(description="Precompute local auto-event cache for fields/windows.")
    p.add_argument("--fields-geojson", required=True)
    p.add_argument("--cache-dir", required=True)
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--source", default="hybrid_radar")
    p.add_argument("--fallback-source", default="")
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--hours", type=int, default=24 * 120)
    p.add_argument("--days-ago", type=int, default=0)
    p.add_argument("--top-n", type=int, default=3)
    p.add_argument("--min-severity", type=int, default=1)
    p.add_argument("--request-retries", type=int, default=6)
    p.add_argument("--request-timeout-s", type=float, default=120.0)
    p.add_argument("--retry-backoff-initial-s", type=float, default=5.0)
    p.add_argument("--retry-backoff-max-s", type=float, default=90.0)
    p.add_argument("--min-interval-s", type=float, default=1.5)
    p.add_argument("--throttle-cooldown-s", type=float, default=900.0)
    p.add_argument("--throttle-max-cooldowns", type=int, default=4)
    p.add_argument("--checkpoint-every", type=int, default=100)
    p.add_argument("--weather-cell-km", type=float, default=2.0)
    p.add_argument("--neighbor-max-km", type=float, default=2.0)
    p.add_argument("--cell-cache-dir", default="")
    p.add_argument("--backend-down-consecutive-stop", type=int, default=12)
    p.add_argument("--backend-health-path", default="/openapi.json")
    p.add_argument("--backend-health-timeout-s", type=float, default=2.0)
    p.add_argument("--backend-health-retries", type=int, default=1)
    p.add_argument("--out-csv", required=True)
    p.add_argument("--log-file", default="")
    args = p.parse_args()

    fields = _load_fields_geojson(Path(args.fields_geojson).resolve())
    cache_dir = Path(args.cache_dir).resolve()
    cell_cache_dir = (
        Path(args.cell_cache_dir).resolve()
        if str(args.cell_cache_dir or "").strip()
        else None
    )
    out_csv = Path(args.out_csv).resolve()
    log_file = Path(args.log_file).resolve() if str(args.log_file).strip() else out_csv.with_suffix(".log")
    cache_dir.mkdir(parents=True, exist_ok=True)
    if cell_cache_dir is not None:
        cell_cache_dir.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    cache_key = _events_cache_key(
        source=str(args.source),
        start=args.start,
        end=args.end,
        hours=int(args.hours),
        days_ago=int(args.days_ago),
        top_n=int(args.top_n),
        min_severity=int(args.min_severity),
    )

    rows: list[dict[str, Any]] = []
    n = len(fields)
    ok = 0
    err = 0
    empty = 0
    last_call_ts = 0.0
    cell_fetch_cache: dict[str, dict[str, Any]] = {}
    backend_down_streak = 0

    def log(msg: str) -> None:
        line = f"[{_ts()}] {msg}"
        print(line, flush=True)
        with log_file.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    log(
        f"start fields={n} source={args.source} "
        f"window={args.start or '-'}..{args.end or '-'} cache={cache_dir} "
        f"weather_cell_km={float(args.weather_cell_km):.2f}"
    )

    for i, fld in enumerate(fields, start=1):
        lat, lon = _field_centroid_latlon(fld)
        params: dict[str, Any] = {
            "points": f"{lat:.5f},{lon:.5f}",
            "agg": "hourly",
            "source": str(args.source),
        }
        if args.start and args.end:
            params["start"] = args.start
            params["end"] = args.end
        else:
            params["hours"] = int(args.hours)
            params["daysAgo"] = int(args.days_ago)

        url = f"{args.api_base_url.rstrip('/')}/abflussatlas/weather/events"
        retries = max(1, int(args.request_retries))
        backoff = max(0.1, float(args.retry_backoff_initial_s))
        backoff_cap = max(backoff, float(args.retry_backoff_max_s))
        payload: dict[str, Any] | None = None
        last_error = ""
        http_status: int | None = None

        def _fetch_one(p: dict[str, Any]) -> dict[str, Any]:
            nonlocal backoff, last_call_ts, http_status, last_error
            cooldowns = 0
            while True:
                exhausted_throttle = False
                for attempt in range(1, retries + 1):
                    now = time.monotonic()
                    wait_s = max(0.0, float(args.min_interval_s)) - (now - last_call_ts)
                    if wait_s > 0:
                        time.sleep(wait_s)
                    last_call_ts = time.monotonic()

                    try:
                        resp = requests.get(url, params=p, timeout=max(1.0, float(args.request_timeout_s)))
                        http_status = int(resp.status_code)
                        if http_status == 429:
                            raise RuntimeError("HTTP 429")
                        resp.raise_for_status()
                        pl = resp.json() if resp.content else {}
                        notes = ((pl.get("meta") or {}).get("notes") or [])
                        note_text = " | ".join(str(x) for x in notes) if isinstance(notes, list) else str(notes)
                        if _is_throttle(note_text):
                            raise RuntimeError(note_text)
                        return pl
                    except Exception as exc:
                        last_error = str(exc)
                        if attempt >= retries:
                            if _is_throttle(last_error) and cooldowns < max(0, int(args.throttle_max_cooldowns)):
                                cooldowns += 1
                                cooldown_s = max(1.0, float(args.throttle_cooldown_s))
                                log(
                                    f"[{i}/{n}] field={fld.field_id} throttle-cooldown "
                                    f"{cooldowns}/{int(args.throttle_max_cooldowns)} sleep={cooldown_s:.1f}s "
                                    f"reason={last_error}"
                                )
                                time.sleep(cooldown_s)
                                backoff = max(0.1, float(args.retry_backoff_initial_s))
                                exhausted_throttle = True
                            break
                        sleep_s = min(backoff, backoff_cap)
                        log(
                            f"[{i}/{n}] field={fld.field_id} retry {attempt}/{retries} "
                            f"sleep={sleep_s:.1f}s reason={last_error}"
                        )
                        time.sleep(sleep_s)
                        backoff = min(backoff * 2.0, backoff_cap)
                if exhausted_throttle:
                    continue
                break
            raise RuntimeError(last_error or "event fetch failed")

        source_l = str(args.source or "").strip().lower()
        def _fetch_for_source(source_name: str) -> dict[str, Any]:
            p0 = dict(params)
            p0["source"] = source_name
            src = str(source_name or "").strip().lower()
            if args.start and args.end and src in ("radar", "hybrid_radar"):
                slices = _split_time_window_utc(str(args.start), str(args.end), max_hours=4320)
                merged_top: list[dict[str, Any]] = []
                last_meta: dict[str, Any] = {}
                for s0, e0 in slices:
                    p2 = dict(p0)
                    p2["start"] = s0
                    p2["end"] = e0
                    p2.pop("hours", None)
                    p2.pop("daysAgo", None)
                    pl = _fetch_one(p2)
                    last_meta = pl.get("meta") or {}
                    mt = (((pl.get("events") or {}).get("mergedTop")) or [])
                    merged_top.extend([e for e in mt if isinstance(e, dict)])
                dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
                for ev in merged_top:
                    key = (str(ev.get("start") or ""), str(ev.get("end") or ""), str(ev.get("source") or ""))
                    old = dedup.get(key)
                    if old is None:
                        dedup[key] = ev
                    else:
                        if int(ev.get("severity") or 0) > int(old.get("severity") or 0):
                            dedup[key] = ev
                return {"meta": last_meta, "events": {"mergedTop": list(dedup.values())}}
            return _fetch_one(p0)

        cell_id = _weather_cell_id(lat, lon, float(args.weather_cell_km))
        fetch_error = ""
        if cell_id in cell_fetch_cache:
            cell_state = cell_fetch_cache[cell_id]
            payload = cell_state.get("payload")
            fetch_error = str(cell_state.get("error") or "")
            cached_http = cell_state.get("http_status")
            http_status = int(cached_http) if cached_http not in (None, "") else http_status
        else:
            try:
                payload = _fetch_for_source(str(args.source))
            except Exception as exc:
                fetch_error = str(exc)
                payload = None
                fb_src = str(args.fallback_source or "").strip()
                if fb_src:
                    log(f"[{i}/{n}] field={fld.field_id} primary source failed, trying fallback source={fb_src}")
                    try:
                        payload = _fetch_for_source(fb_src)
                        meta = payload.get("meta") or {}
                        notes = list(meta.get("notes") or [])
                        notes.append(f"fallback_source_used:{fb_src}")
                        meta["notes"] = notes
                        payload["meta"] = meta
                        fetch_error = ""
                    except Exception as exc_fb:
                        fetch_error = f"primary={fetch_error} | fallback={exc_fb}"
                        payload = None
            cell_fetch_cache[cell_id] = {
                "payload": payload,
                "error": fetch_error,
                "http_status": http_status if http_status is not None else "",
            }
            if payload is not None and cell_cache_dir is not None:
                try:
                    cpath = _events_cell_cache_path(cell_cache_dir, cell_id, cache_key)
                    evs_cell = _parse_auto_events_payload(
                        payload,
                        top_n=int(args.top_n),
                        min_severity=int(args.min_severity),
                    )
                    cpath.write_text(
                        json.dumps(
                            {
                                "cell_id": cell_id,
                                "weather_cell_km": float(args.weather_cell_km),
                                "params": {"source": str(args.source), "start": args.start, "end": args.end},
                                "events": [e.__dict__ for e in evs_cell],
                                "meta": payload.get("meta") or {},
                                "precomputed_at_utc": _ts(),
                            },
                            ensure_ascii=False,
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                except Exception:
                    pass

        cache_path = _events_cache_path(cache_dir, fld.field_id, cache_key)
        if payload is None:
            down_like = _looks_backend_down(fetch_error or last_error)
            if down_like:
                backend_down_streak += 1
            else:
                backend_down_streak = 0
            err += 1
            rows.append(
                {
                    "field_id": fld.field_id,
                    "status": "error",
                    "events_count": 0,
                    "source": args.source,
                    "window_start": args.start or "",
                    "window_end": args.end or "",
                    "http_status": http_status if http_status is not None else "",
                    "note": (fetch_error or last_error) + f" | weather_cell={cell_id}",
                    "cache_path": str(cache_path),
                }
            )
            if backend_down_streak >= max(1, int(args.backend_down_consecutive_stop)):
                healthy = _backend_health_ok(
                    api_base_url=str(args.api_base_url),
                    health_path=str(args.backend_health_path),
                    timeout_s=float(args.backend_health_timeout_s),
                    retries=int(args.backend_health_retries),
                )
                if not healthy:
                    _write_csv(out_csv, rows)
                    log(
                        "abort backend unreachable: "
                        f"consecutive_failures={backend_down_streak} api_base={args.api_base_url}"
                    )
                    return 86
        else:
            backend_down_streak = 0
            evs = _parse_auto_events_payload(
                payload,
                top_n=int(args.top_n),
                min_severity=int(args.min_severity),
            )
            evs_out: list[dict[str, Any]] = [_event_to_dict(e) for e in evs]
            neighbor_note = ""
            if (not evs_out) and (cell_cache_dir is not None):
                n_evs, n_cell, n_dist = _load_neighbor_cached_events(
                    cell_cache_dir=cell_cache_dir,
                    cache_key=cache_key,
                    lat=lat,
                    lon=lon,
                    source_cell_id=cell_id,
                    weather_cell_km=float(args.weather_cell_km),
                    neighbor_max_km=float(args.neighbor_max_km),
                )
                if n_evs:
                    evs_out = n_evs
                    neighbor_note = f"neighbor_2km_from={n_cell} dist_km={n_dist:.3f}"
            cache_path.write_text(
                json.dumps(
                    {
                        "field_id": fld.field_id,
                        "params": params,
                        "events": evs_out,
                        "meta": payload.get("meta") or {},
                        "precomputed_at_utc": _ts(),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            ok += 1
            if not evs_out:
                empty += 1
            note_text = " | ".join(str(x) for x in ((payload.get("meta") or {}).get("notes") or []))
            if cell_id in cell_fetch_cache and i > 1:
                note_text = (note_text + " | " if note_text else "") + f"weather_cell={cell_id}"
            if neighbor_note:
                note_text = (note_text + " | " if note_text else "") + neighbor_note
            rows.append(
                {
                    "field_id": fld.field_id,
                    "status": "ok",
                    "events_count": len(evs_out),
                    "source": args.source,
                    "window_start": args.start or "",
                    "window_end": args.end or "",
                    "http_status": http_status if http_status is not None else 200,
                    "note": note_text,
                    "cache_path": str(cache_path),
                }
            )

        if (i % max(1, int(args.checkpoint_every)) == 0) or (i == n):
            _write_csv(out_csv, rows)
            log(f"[{i}/{n}] checkpoint rows={len(rows)} ok={ok} err={err} empty={empty}")

    summary = {
        "finished_at_utc": _ts(),
        "fields_total": n,
        "ok": ok,
        "error": err,
        "ok_empty_events": empty,
        "source": args.source,
        "window_start": args.start,
        "window_end": args.end,
        "weather_cell_km": float(args.weather_cell_km),
        "neighbor_max_km": float(args.neighbor_max_km),
        "weather_cells_total": len(cell_fetch_cache),
        "cell_cache_dir": (str(cell_cache_dir) if cell_cache_dir else None),
        "cache_dir": str(cache_dir),
        "out_csv": str(out_csv),
    }
    out_meta = out_csv.with_suffix(".meta.json")
    out_meta.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"done ok={ok} err={err} empty={empty} csv={out_csv} meta={out_meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
