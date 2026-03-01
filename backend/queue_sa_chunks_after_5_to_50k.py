from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def _iso_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _read_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _completed_chunk_ids(state: dict) -> set[int]:
    out: set[int] = set()
    for item in (state.get("completed") or []):
        try:
            out.add(int(item.get("chunk_idx")))
        except Exception:
            continue
    return out


def _has_failures(state: dict) -> bool:
    return bool(state.get("failed"))


def _running_chunk_runner() -> bool:
    # Windows only (project runs on Windows in this workspace).
    ps = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'run_field_event_batch_sa_chunks.py' } | "
        "Measure-Object | Select-Object -ExpandProperty Count"
    )
    proc = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps],
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        return int((proc.stdout or "0").strip()) > 0
    except Exception:
        return False


def main() -> int:
    p = argparse.ArgumentParser(
        description="Wait for first chunk block (1..5) to finish, then auto-start chunk block to reach ~50k fields."
    )
    p.add_argument(
        "--state-file",
        default=str(Path("paper") / "exports" / "sa_chunks" / "sa_chunk_run_state.json"),
    )
    p.add_argument("--wait-completed-chunks", type=int, default=5)
    p.add_argument("--poll-seconds", type=int, default=30)
    p.add_argument("--chunk-size", type=int, default=1000)
    p.add_argument("--start-chunk", type=int, default=6)
    p.add_argument("--max-chunks", type=int, default=45)  # 6..50 => 50k fields at size 1000
    p.add_argument("--checkpoint-every", type=int, default=100)
    p.add_argument("--dem-source", default="cog")
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--events-source", default="csv", help="csv|auto")
    p.add_argument("--events-auto-source", default="hybrid_radar")
    p.add_argument("--events-auto-start", default=None)
    p.add_argument("--events-auto-end", default=None)
    p.add_argument("--events-auto-hours", type=int, default=24 * 120)
    p.add_argument("--events-auto-days-ago", type=int, default=0)
    p.add_argument("--events-auto-top-n", type=int, default=2)
    p.add_argument("--events-auto-min-severity", type=int, default=1)
    p.add_argument("--events-auto-cache-dir", default=str(Path("paper") / "cache" / "auto_events"))
    p.add_argument("--post-merge", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--merge-exports-dir", default=str(Path("paper") / "exports" / "sa_chunks"))
    p.add_argument("--out-log", default=str(Path("paper") / "exports" / "sa_chunks_50k_queue.log"))
    args = p.parse_args()

    out_log = Path(args.out_log).resolve()
    out_log.parent.mkdir(parents=True, exist_ok=True)

    def log(msg: str) -> None:
        line = f"[{_iso_now()}] {msg}"
        print(line, flush=True)
        with out_log.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    state_file = Path(args.state_file).resolve()
    log(f"Queue start. Waiting for >= {args.wait_completed_chunks} completed chunks in {state_file}")

    # Wait until first run has produced enough completed chunks and no active failure.
    while True:
        st = _read_state(state_file)
        done = _completed_chunk_ids(st)
        failed = _has_failures(st)
        running = _running_chunk_runner()
        log(f"status: completed={len(done)} failed={failed} running={running}")

        if failed:
            log("Abort: current chunk run has failures in state file.")
            return 2

        if len(done) >= int(args.wait_completed_chunks) and not running:
            break

        time.sleep(max(5, int(args.poll_seconds)))

    cmd = [
        sys.executable,
        "-u",
        str((Path(__file__).resolve().parent / "run_field_event_batch_sa_chunks.py")),
        "--chunk-size",
        str(int(args.chunk_size)),
        "--start-chunk",
        str(int(args.start_chunk)),
        "--max-chunks",
        str(int(args.max_chunks)),
        "--checkpoint-every",
        str(int(args.checkpoint_every)),
        "--dem-source",
        str(args.dem_source),
        "--api-base-url",
        str(args.api_base_url),
        "--events-source",
        str(args.events_source),
        "--events-auto-source",
        str(args.events_auto_source),
        "--events-auto-hours",
        str(int(args.events_auto_hours)),
        "--events-auto-days-ago",
        str(int(args.events_auto_days_ago)),
        "--events-auto-top-n",
        str(int(args.events_auto_top_n)),
        "--events-auto-min-severity",
        str(int(args.events_auto_min_severity)),
        "--events-auto-cache-dir",
        str(Path(args.events_auto_cache_dir)),
    ]
    if args.events_auto_start:
        cmd.extend(["--events-auto-start", str(args.events_auto_start)])
    if args.events_auto_end:
        cmd.extend(["--events-auto-end", str(args.events_auto_end)])
    log("Starting follow-up chunk run: " + " ".join(cmd))
    rc = subprocess.run(cmd, check=False).returncode
    log(f"Follow-up chunk run finished with exit_code={rc}")
    if rc == 0 and bool(args.post_merge):
        merge_cmd = [
            sys.executable,
            "-u",
            str((Path(__file__).resolve().parent / "merge_sa_chunk_results.py")),
            "--exports-dir",
            str(Path(args.merge_exports_dir)),
            "--run-quickcheck",
        ]
        log("Starting post-merge: " + " ".join(merge_cmd))
        merge_rc = subprocess.run(merge_cmd, check=False).returncode
        log(f"Post-merge finished with exit_code={merge_rc}")
        if merge_rc != 0:
            return int(merge_rc)
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
