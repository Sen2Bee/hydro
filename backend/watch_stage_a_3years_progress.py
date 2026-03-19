from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import time
from pathlib import Path


def ts_local() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simple human-readable progress log for Stage-A 3-year run.")
    p.add_argument("--manifest", required=True, help="run_manifest.json from stage_a_sawide_3years run")
    p.add_argument("--out-log", default="", help="Output progress log file path")
    p.add_argument("--interval-s", type=int, default=60, help="Polling interval seconds")
    p.add_argument("--stop-on-finish", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument(
        "--stall-max-intervals",
        type=int,
        default=20,
        help="Abort watcher when no overall progress for N intervals",
    )
    p.add_argument(
        "--on-finish-cmd",
        default="",
        help="Optional command to start once when all Stage-A chunks are complete",
    )
    p.add_argument(
        "--on-finish-log",
        default="",
        help="Optional log file for on-finish command stdout/stderr",
    )
    p.add_argument(
        "--on-finish-cwd",
        default="",
        help="Optional working directory for on-finish command (defaults to manifest repo_root if present)",
    )
    p.add_argument(
        "--trigger-state-file",
        default="",
        help="Optional JSON file used to ensure on-finish command runs only once",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest).resolve()
    manifest = load_json(manifest_path)
    if not manifest:
        raise SystemExit(f"manifest not readable: {manifest_path}")

    workers = manifest.get("workers", []) or []
    if not workers:
        raise SystemExit("no workers in manifest")

    out_log = Path(args.out_log).resolve() if args.out_log else manifest_path.with_name("overall_progress.log")
    out_log.parent.mkdir(parents=True, exist_ok=True)

    trigger_state_path = (
        Path(args.trigger_state_file).resolve()
        if args.trigger_state_file
        else manifest_path.with_name("stage_b_autostart_state.json")
    )
    trigger_state = load_json(trigger_state_path) if trigger_state_path.exists() else {}

    with out_log.open("a", encoding="utf-8") as out:
        out.write(f"[{ts_local()}] START overall watcher manifest={manifest_path}\n")
        out.flush()

        prev_total_done: int | None = None
        prev_ts: dt.datetime | None = None
        prev_done_by_year: dict[str, int] = {}
        max_done_by_year: dict[str, int] = {}
        no_progress_intervals = 0

        while True:
            total_done = 0
            total_all = 0
            total_open_issues = 0
            per_year = []

            for w in workers:
                year_key = str(w.get("year_window", "unknown"))
                exports_dir = Path(str(w.get("exports_dir", "")))
                state_path = exports_dir / "precompute_state.json"
                st = load_json(state_path) if state_path.exists() else {}

                completed_entries = st.get("completed", []) or []
                completed_ids = {
                    int(x.get("chunk_idx"))
                    for x in completed_entries
                    if isinstance(x, dict) and x.get("chunk_idx") is not None
                }
                raw_done = len(completed_ids) if completed_ids else len(completed_entries)
                # Keep displayed counters monotonic across worker restarts/state rewrites.
                prev_max = int(max_done_by_year.get(year_key, 0))
                done = raw_done if raw_done >= prev_max else prev_max
                max_done_by_year[year_key] = done
                failed_entries = st.get("failed", []) or []
                failed_ids = {
                    int(x.get("chunk_idx"))
                    for x in failed_entries
                    if isinstance(x, dict) and x.get("chunk_idx") is not None
                }
                open_issues = len(failed_ids - completed_ids) if failed_ids else 0
                all_chunks = int(st.get("total_chunks", 0) or 0)
                pct = (100.0 * done / all_chunks) if all_chunks > 0 else 0.0

                total_done += done
                total_all += all_chunks
                total_open_issues += open_issues
                prev_done = int(prev_done_by_year.get(year_key, done))
                delta_done = int(done - prev_done)
                per_year.append((year_key, done, all_chunks, pct, delta_done))

            overall_pct = (100.0 * total_done / total_all) if total_all > 0 else 0.0
            now_ts = dt.datetime.now()
            if prev_total_done is None or prev_ts is None:
                delta_total_done = 0
                mins = max(1.0 / 60.0, float(args.interval_s) / 60.0)
            else:
                delta_total_done = int(total_done - prev_total_done)
                mins = max(1.0 / 60.0, (now_ts - prev_ts).total_seconds() / 60.0)
            rate_per_min = float(delta_total_done) / mins
            status = "OK" if delta_total_done > 0 else ("WARN" if total_open_issues > 0 else "IDLE")
            line = (
                f"[{ts_local()}] Gesamt: {total_done}/{total_all} Chunks "
                f"({overall_pct:5.2f}%), +{delta_total_done} ({rate_per_min:.2f}/min), "
                f"open_issues={total_open_issues}, status={status}"
            )
            out.write(line + "\n")
            out.flush()

            prev_total_done = int(total_done)
            prev_ts = now_ts
            for year_key, done, *_rest in per_year:
                prev_done_by_year[year_key] = int(done)

            all_finished = total_all > 0 and total_done >= total_all
            if delta_total_done > 0:
                no_progress_intervals = 0
            else:
                no_progress_intervals += 1

            stall_limit = max(1, int(args.stall_max_intervals))
            if (not all_finished) and (no_progress_intervals >= stall_limit):
                out.write(
                    f"[{ts_local()}] STALLED no progress for {no_progress_intervals} intervals "
                    f"(limit={stall_limit}), open_issues={total_open_issues}. Stop watcher.\n"
                )
                out.flush()
                return 2

            if bool(args.stop_on_finish) and all_finished:
                if args.on_finish_cmd.strip():
                    already_started = bool(trigger_state.get("started", False))
                    if already_started:
                        out.write(
                            f"[{ts_local()}] AUTOSTART skipped (already started): {trigger_state_path}\n"
                        )
                        out.flush()
                    else:
                        cmd = args.on_finish_cmd.strip()
                        cwd = args.on_finish_cwd.strip() or str(manifest.get("repo_root", "")).strip() or str(
                            manifest_path.parent
                        )
                        on_finish_log = (
                            Path(args.on_finish_log).resolve()
                            if args.on_finish_log
                            else manifest_path.with_name("stage_b_autostart.log")
                        )
                        on_finish_log.parent.mkdir(parents=True, exist_ok=True)
                        with on_finish_log.open("a", encoding="utf-8") as lf:
                            lf.write(f"[{ts_local()}] AUTOSTART cmd={cmd}\n")
                            lf.flush()
                            proc = subprocess.Popen(
                                ["cmd.exe", "/c", cmd],
                                cwd=cwd,
                                stdout=lf,
                                stderr=subprocess.STDOUT,
                            )
                        trigger_state = {
                            "started": True,
                            "started_at_local": ts_local(),
                            "pid": int(proc.pid),
                            "cwd": cwd,
                            "cmd": cmd,
                            "log": str(on_finish_log),
                        }
                        trigger_state_path.write_text(
                            json.dumps(trigger_state, indent=2), encoding="utf-8"
                        )
                        out.write(
                            f"[{ts_local()}] AUTOSTART started pid={proc.pid} log={on_finish_log}\n"
                        )
                        out.flush()
                out.write(f"[{ts_local()}] FINISHED all chunks completed.\n")
                out.flush()
                return 0

            time.sleep(max(10, int(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
