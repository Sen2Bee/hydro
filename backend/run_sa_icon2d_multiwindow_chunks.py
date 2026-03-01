from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import threading
from pathlib import Path


def _now_tag() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d_%H%M%S")


def _parse_windows(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        s, e = part.split(":", 1)
        s = s.strip()
        e = e.strip()
        if s and e:
            out.append((s, e))
    if not out:
        raise RuntimeError("No valid windows. Use: YYYY-MM-DD:YYYY-MM-DD,...")
    return out


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Run SA chunk batch for multiple weather windows (reproducible 50/100 chunk scaling)."
    )
    p.add_argument("--chunk-size", type=int, default=1000)
    p.add_argument("--start-chunk", type=int, default=1)
    p.add_argument("--max-chunks", type=int, default=50, help="Use 50 now, later 100 with same setup.")
    p.add_argument("--windows", default="2023-04-01:2023-10-31,2024-04-01:2024-10-31,2025-04-01:2025-10-31")
    p.add_argument("--events-auto-source", default="hybrid_radar")
    p.add_argument("--events-auto-top-n", type=int, default=3)
    p.add_argument("--events-auto-min-severity", type=int, default=1)
    p.add_argument("--events-auto-request-retries", type=int, default=6)
    p.add_argument("--events-auto-retry-backoff-initial-s", type=float, default=5.0)
    p.add_argument("--events-auto-retry-backoff-max-s", type=float, default=90.0)
    p.add_argument("--events-auto-min-interval-s", type=float, default=1.5)
    p.add_argument("--events-auto-cache-only", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--events-auto-use-cached-empty", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--checkpoint-every", type=int, default=100)
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--dem-source", default="cog")
    p.add_argument("--provider", default="auto")
    p.add_argument("--threshold", type=int, default=200)
    p.add_argument("--analysis-modes", default="erosion_events_ml,abag")
    p.add_argument("--ml-threshold", type=float, default=0.05, help="Dataset run: use minimal allowed threshold (0.05) to keep negatives.")
    p.add_argument("--min-field-area-ha", type=float, default=0.0)
    p.add_argument("--max-field-area-ha", type=float, default=0.0)
    p.add_argument("--field-id-whitelist-file", default="")
    p.add_argument("--field-id-whitelist-column", default="")
    p.add_argument("--require-whitelist", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--exports-root", default=str(Path("paper") / "exports" / "sa_chunks_icon2d"))
    p.add_argument("--cache-root", default=str(Path("paper") / "cache" / "auto_events_icon2d"))
    p.add_argument("--chunks-root", default=str(Path("paper") / "input" / "sa_chunks_icon2d"))
    p.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--validate-chunk", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--fail-on-qa-error", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--continue-on-error", action=argparse.BooleanOptionalAction, default=True)
    args = p.parse_args()

    run_tag = _now_tag()
    windows = _parse_windows(args.windows)
    root = Path.cwd().resolve()
    script = root / "backend" / "run_field_event_batch_sa_chunks.py"
    if not script.exists():
        raise SystemExit(f"Missing script: {script}")

    exports_root = (root / args.exports_root).resolve()
    cache_root = (root / args.cache_root).resolve()
    chunks_root = (root / args.chunks_root).resolve()
    log_path = exports_root / "automation" / f"icon2d_multiwindow_{run_tag}.log"
    manifest_path = exports_root / "automation" / f"icon2d_multiwindow_{run_tag}.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    manifest: dict = {
        "run_tag": run_tag,
        "started_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "args": vars(args),
        "windows": windows,
        "steps": [],
        "status": "running",
    }
    _write_json(manifest_path, manifest)

    def log(msg: str) -> None:
        line = f"[{dt.datetime.now(tz=dt.timezone.utc).isoformat().replace('+00:00', 'Z')}] {msg}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    try:
        for ws, we in windows:
            win_key = f"{ws.replace('-','')}_{we.replace('-','')}"
            exports_dir = exports_root / win_key
            cache_dir = cache_root / win_key
            chunks_dir = chunks_root / win_key
            cmd = [
                sys.executable,
                "-u",
                str(script),
                "--chunk-size",
                str(args.chunk_size),
                "--start-chunk",
                str(args.start_chunk),
                "--max-chunks",
                str(args.max_chunks),
                "--events-source",
                "auto",
                "--events-auto-source",
                args.events_auto_source,
                "--events-auto-start",
                ws,
                "--events-auto-end",
                we,
                "--events-auto-top-n",
                str(args.events_auto_top_n),
                "--events-auto-min-severity",
                str(args.events_auto_min_severity),
                "--events-auto-request-retries",
                str(args.events_auto_request_retries),
                "--events-auto-retry-backoff-initial-s",
                str(args.events_auto_retry_backoff_initial_s),
                "--events-auto-retry-backoff-max-s",
                str(args.events_auto_retry_backoff_max_s),
                "--events-auto-min-interval-s",
                str(args.events_auto_min_interval_s),
                "--events-auto-cache-only" if bool(args.events_auto_cache_only) else "--no-events-auto-cache-only",
                "--events-auto-use-cached-empty" if bool(args.events_auto_use_cached_empty) else "--no-events-auto-use-cached-empty",
                "--events-auto-cache-dir",
                str(cache_dir),
                "--chunks-dir",
                str(chunks_dir),
                "--exports-dir",
                str(exports_dir),
                "--api-base-url",
                args.api_base_url,
                "--analysis-modes",
                args.analysis_modes,
                "--provider",
                args.provider,
                "--dem-source",
                args.dem_source,
                "--threshold",
                str(args.threshold),
                "--ml-threshold",
                str(args.ml_threshold),
                "--checkpoint-every",
                str(args.checkpoint_every),
                "--resume" if args.resume else "--no-resume",
                "--validate-chunk" if args.validate_chunk else "--no-validate-chunk",
                "--fail-on-qa-error" if args.fail_on_qa_error else "--no-fail-on-qa-error",
                "--continue-on-error" if args.continue_on_error else "--no-continue-on-error",
                "--min-field-area-ha",
                str(args.min_field_area_ha),
                "--max-field-area-ha",
                str(args.max_field_area_ha),
            ]
            if str(args.field_id_whitelist_file or "").strip():
                cmd.extend(["--field-id-whitelist-file", str(args.field_id_whitelist_file)])
            if str(args.field_id_whitelist_column or "").strip():
                cmd.extend(["--field-id-whitelist-column", str(args.field_id_whitelist_column)])
            cmd.append("--require-whitelist" if bool(args.require_whitelist) else "--no-require-whitelist")
            log(f"start window={ws}..{we}")
            log("cmd: " + " ".join(cmd))
            # Stream child process output into the same orchestration log so the
            # run can be followed from a single file in VSCode.
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            chunk_tag = {"value": "chunk=?"}

            def _pump() -> None:
                assert proc.stdout is not None
                for line in proc.stdout:
                    txt = line.rstrip("\r\n")
                    if txt:
                        m = re.search(r"\[SA-CHUNKS\]\s+chunk\s+(\d+)/(\d+).*run\s+(\d+)/(\d+)", txt)
                        if m:
                            chunk_tag["value"] = f"chunk={m.group(1)}/{m.group(2)} run={m.group(3)}/{m.group(4)}"
                        log(f"[chunk {ws}..{we} {chunk_tag['value']}] {txt}")

            t = threading.Thread(target=_pump, daemon=True)
            t.start()
            rc = proc.wait()
            t.join(timeout=2.0)
            manifest["steps"].append(
                {
                    "window_start": ws,
                    "window_end": we,
                    "exports_dir": str(exports_dir),
                    "cache_dir": str(cache_dir),
                    "chunks_dir": str(chunks_dir),
                    "return_code": rc,
                }
            )
            _write_json(manifest_path, manifest)
            if rc != 0:
                log(f"failed window={ws}..{we} rc={rc}")
                manifest["status"] = "failed"
                manifest["finished_at_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
                _write_json(manifest_path, manifest)
                return rc
            log(f"done window={ws}..{we}")

        manifest["status"] = "completed"
        manifest["finished_at_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
        _write_json(manifest_path, manifest)
        log("all windows completed")
        log(f"manifest={manifest_path}")
        return 0
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        manifest["finished_at_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
        _write_json(manifest_path, manifest)
        log(f"fatal: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
