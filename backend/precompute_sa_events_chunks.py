from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests


def _iso_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _run_id() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SA-wide weather event precompute by chunks (cache only).")
    p.add_argument("--source-sqlite", default=str(Path("data") / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"))
    p.add_argument("--chunk-size", type=int, default=1000)
    p.add_argument("--start-chunk", type=int, default=1)
    p.add_argument("--max-chunks", type=int, default=0)
    p.add_argument("--chunks-dir", default=str(Path("paper") / "input" / "sa_precompute_chunks"))
    p.add_argument("--exports-dir", default=str(Path("paper") / "exports" / "sa_precompute"))
    p.add_argument("--cache-dir", required=True)
    p.add_argument("--cell-cache-dir", default="")
    p.add_argument("--source", default="icon2d")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--top-n", type=int, default=5)
    p.add_argument("--min-severity", type=int, default=0)
    p.add_argument("--weather-cell-km", type=float, default=2.0)
    p.add_argument("--request-retries", type=int, default=2)
    p.add_argument("--retry-backoff-initial-s", type=float, default=5.0)
    p.add_argument("--retry-backoff-max-s", type=float, default=20.0)
    p.add_argument("--min-interval-s", type=float, default=4.0)
    p.add_argument("--throttle-cooldown-s", type=float, default=30.0)
    p.add_argument("--throttle-max-cooldowns", type=int, default=1)
    p.add_argument("--checkpoint-every", type=int, default=50)
    p.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--health-path", default="/openapi.json")
    p.add_argument("--health-retries", type=int, default=20)
    p.add_argument("--health-sleep-s", type=float, default=3.0)
    p.add_argument("--fail-on-chunk-error", action=argparse.BooleanOptionalAction, default=True)
    return p.parse_args()


def _count_and_span(conn: sqlite3.Connection) -> tuple[int, int, int]:
    cur = conn.execute("SELECT COUNT(*), MIN(rowid), MAX(rowid) FROM flurstuecke")
    total, min_rowid, max_rowid = cur.fetchone()
    return int(total or 0), int(min_rowid or 0), int(max_rowid or 0)


def _chunk_bounds(min_rowid: int, max_rowid: int, chunk_size: int, chunk_idx_1b: int) -> tuple[int, int]:
    start = int(min_rowid + (chunk_idx_1b - 1) * chunk_size)
    end = min(int(max_rowid), start + chunk_size - 1)
    return start, end


def _fetch_chunk_features(conn: sqlite3.Connection, rowid_start: int, rowid_end: int) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT feature_json FROM flurstuecke WHERE rowid BETWEEN ? AND ? ORDER BY rowid",
        (int(rowid_start), int(rowid_end)),
    )
    out: list[dict[str, Any]] = []
    for (fj,) in cur:
        try:
            out.append(json.loads(str(fj)))
        except Exception:
            continue
    return out


def _write_geojson(path: Path, features: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False), encoding="utf-8")


def _chunk_meta_path(out_csv: Path) -> Path:
    return out_csv.with_suffix(".meta.json")


def _read_chunk_meta(meta_path: Path) -> dict[str, Any] | None:
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _meta_has_zero_error(meta: dict[str, Any] | None) -> bool:
    if not meta:
        return False
    err = int(meta.get("error", meta.get("err", 0)) or 0)
    return err == 0


def _is_valid_done(done_flag: Path, out_csv: Path) -> bool:
    if not done_flag.exists():
        return False
    meta = _read_chunk_meta(_chunk_meta_path(out_csv))
    return _meta_has_zero_error(meta)


def _wait_backend_health(api_base_url: str, health_path: str, retries: int, sleep_s: float) -> bool:
    base = str(api_base_url or "").rstrip("/")
    hp = str(health_path or "/openapi.json")
    url = f"{base}{hp if hp.startswith('/') else '/' + hp}"
    for _ in range(max(1, int(retries))):
        try:
            r = requests.get(url, timeout=10)
            if int(r.status_code) == 200:
                return True
        except Exception:
            pass
        time.sleep(max(0.5, float(sleep_s)))
    return False


def main() -> int:
    args = _parse_args()
    src = Path(args.source_sqlite).resolve()
    if not src.exists():
        raise SystemExit(f"sqlite missing: {src}")

    chunks_dir = Path(args.chunks_dir).resolve()
    exports_dir = Path(args.exports_dir).resolve()
    cache_dir = Path(args.cache_dir).resolve()
    cell_cache_dir = Path(args.cell_cache_dir).resolve() if str(args.cell_cache_dir or "").strip() else None
    chunks_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    if cell_cache_dir is not None:
        cell_cache_dir.mkdir(parents=True, exist_ok=True)

    run_id = _run_id()
    manifest = exports_dir / "runs" / f"precompute_run_{run_id}.json"
    state_file = exports_dir / "precompute_state.json"
    meta: dict[str, Any] = {
        "run_id": run_id,
        "started_at_utc": _iso_now(),
        "status": "running",
        "args": vars(args),
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    with sqlite3.connect(str(src)) as conn:
        total_rows, min_rowid, max_rowid = _count_and_span(conn)
        chunk_size = max(1, int(args.chunk_size))
        total_chunks = int(math.ceil(total_rows / float(chunk_size)))
        start_chunk = max(1, int(args.start_chunk))
        end_chunk = total_chunks if int(args.max_chunks) <= 0 else min(total_chunks, start_chunk + int(args.max_chunks) - 1)

        state: dict[str, Any] = {
            "run_id": run_id,
            "started_at_utc": _iso_now(),
            "source_sqlite": str(src),
            "total_rows": total_rows,
            "total_chunks": total_chunks,
            "run_start_chunk": start_chunk,
            "run_end_chunk": end_chunk,
            "completed": [],
            "failed": [],
        }
        state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
        print(
            f"[PRECOMPUTE] total_rows={total_rows} chunk_size={chunk_size} "
            f"total_chunks={total_chunks} run={start_chunk}..{end_chunk}"
        )

        script = Path(__file__).resolve().parent / "precompute_auto_events_cache.py"
        for idx in range(start_chunk, end_chunk + 1):
            rowid_start, rowid_end = _chunk_bounds(min_rowid, max_rowid, chunk_size, idx)
            chunk_geojson = chunks_dir / f"schlaege_chunk_{idx:05d}.geojson"
            out_csv = exports_dir / f"precompute_chunk_{idx:05d}.csv"
            out_log = exports_dir / f"precompute_chunk_{idx:05d}.log"
            done_flag = exports_dir / f"chunk_{idx:05d}.done"
            if bool(args.resume) and _is_valid_done(done_flag, out_csv):
                print(f"[PRECOMPUTE] chunk {idx}/{total_chunks}: skip(valid done)")
                state["completed"].append({"chunk_idx": idx, "skipped": True})
                state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
                continue
            if done_flag.exists() and not _is_valid_done(done_flag, out_csv):
                print(f"[PRECOMPUTE] chunk {idx}/{total_chunks}: stale done detected -> recompute")
                try:
                    done_flag.unlink(missing_ok=True)
                except Exception:
                    pass

            feats = _fetch_chunk_features(conn, rowid_start, rowid_end)
            if not feats:
                done_flag.write_text("empty\n", encoding="utf-8")
                state["completed"].append({"chunk_idx": idx, "rows": 0, "empty": True})
                state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
                continue

            _write_geojson(chunk_geojson, feats)
            if not _wait_backend_health(
                api_base_url=str(args.api_base_url),
                health_path=str(args.health_path),
                retries=int(args.health_retries),
                sleep_s=float(args.health_sleep_s),
            ):
                msg = {
                    "chunk_idx": idx,
                    "exit_code": 9001,
                    "reason": "backend_unreachable",
                    "api_base_url": str(args.api_base_url),
                }
                state["failed"].append(msg)
                state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
                print(f"[PRECOMPUTE] chunk {idx}/{total_chunks}: backend healthcheck failed -> stop")
                raise SystemExit(9001)

            cmd = [
                sys.executable,
                "-u",
                str(script),
                "--fields-geojson",
                str(chunk_geojson.resolve()),
                "--cache-dir",
                str(cache_dir.resolve()),
                "--api-base-url",
                str(args.api_base_url),
                "--source",
                str(args.source),
                "--start",
                str(args.start),
                "--end",
                str(args.end),
                "--top-n",
                str(int(args.top_n)),
                "--min-severity",
                str(int(args.min_severity)),
                "--weather-cell-km",
                str(float(args.weather_cell_km)),
                "--request-retries",
                str(int(args.request_retries)),
                "--retry-backoff-initial-s",
                str(float(args.retry_backoff_initial_s)),
                "--retry-backoff-max-s",
                str(float(args.retry_backoff_max_s)),
                "--min-interval-s",
                str(float(args.min_interval_s)),
                "--throttle-cooldown-s",
                str(float(args.throttle_cooldown_s)),
                "--throttle-max-cooldowns",
                str(int(args.throttle_max_cooldowns)),
                "--checkpoint-every",
                str(int(args.checkpoint_every)),
                "--out-csv",
                str(out_csv.resolve()),
                "--log-file",
                str(out_log.resolve()),
            ]
            if cell_cache_dir is not None:
                cmd.extend(["--cell-cache-dir", str(cell_cache_dir.resolve())])
            print("[PRECOMPUTE-CMD] " + " ".join(cmd))
            rc = int(subprocess.run(cmd, check=False).returncode)
            if rc != 0:
                state["failed"].append({"chunk_idx": idx, "exit_code": rc})
                state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
                raise SystemExit(rc)
            meta = _read_chunk_meta(_chunk_meta_path(out_csv))
            if _meta_has_zero_error(meta):
                done_flag.write_text("ok\n", encoding="utf-8")
                state["completed"].append({"chunk_idx": idx, "rows": len(feats), "out_csv": str(out_csv)})
                state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
            else:
                err_val = int((meta or {}).get("error", (meta or {}).get("err", -1)) if meta is not None else -1)
                state["failed"].append({"chunk_idx": idx, "exit_code": 9002, "reason": "chunk_meta_error", "error": err_val})
                state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
                print(f"[PRECOMPUTE] chunk {idx}/{total_chunks}: meta reports error={err_val}")
                if bool(args.fail_on_chunk_error):
                    raise SystemExit(9002)

    meta["status"] = "completed"
    meta["finished_at_utc"] = _iso_now()
    meta["state_file"] = str(state_file)
    manifest.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[PRECOMPUTE] done state={state_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
