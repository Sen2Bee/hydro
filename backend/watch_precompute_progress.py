#!/usr/bin/env python
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f)


def _tail(path: Path, n: int = 3) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return lines[-n:]


def _count_cache(cache_dir: Path) -> int:
    if not cache_dir.exists():
        return 0
    return sum(1 for _ in cache_dir.glob("*.json"))


def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            import ctypes  # lazy import

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid
            )
            if not handle:
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watch precompute run and write structured progress log.")
    p.add_argument("--csv", required=True, help="Precompute CSV path")
    p.add_argument("--log", required=True, help="Precompute log path")
    p.add_argument("--cache-dir", required=True, help="Cache directory")
    p.add_argument("--out-log", required=True, help="Watcher output log")
    p.add_argument("--pid", type=int, default=0, help="Optional PID of precompute process")
    p.add_argument("--interval-s", type=int, default=60, help="Polling interval in seconds")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv).resolve()
    pre_log = Path(args.log).resolve()
    cache_dir = Path(args.cache_dir).resolve()
    out_log = Path(args.out_log).resolve()
    out_log.parent.mkdir(parents=True, exist_ok=True)

    start_lines = _count_lines(csv_path)
    start_cache = _count_cache(cache_dir)
    started_at = _utc_now()

    with out_log.open("a", encoding="utf-8") as out:
        out.write(
            json.dumps(
                {
                    "ts_utc": started_at,
                    "type": "watcher_start",
                    "csv": str(csv_path),
                    "precompute_log": str(pre_log),
                    "cache_dir": str(cache_dir),
                    "pid": int(args.pid),
                    "interval_s": int(args.interval_s),
                    "start_csv_lines": start_lines,
                    "start_cache_files": start_cache,
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        out.flush()

        while True:
            now = _utc_now()
            csv_lines = _count_lines(csv_path)
            cache_files = _count_cache(cache_dir)
            pid_alive = _is_pid_alive(int(args.pid)) if int(args.pid) > 0 else None
            payload = {
                "ts_utc": now,
                "type": "progress",
                "csv_lines_total": csv_lines,
                "csv_rows_est": max(0, csv_lines - 1),
                "cache_files": cache_files,
                "delta_csv_lines": csv_lines - start_lines,
                "delta_cache_files": cache_files - start_cache,
                "pid_alive": pid_alive,
                "precompute_tail": _tail(pre_log, 3),
            }
            out.write(json.dumps(payload, ensure_ascii=False) + "\n")
            out.flush()

            if int(args.pid) > 0 and pid_alive is False:
                out.write(
                    json.dumps(
                        {
                            "ts_utc": _utc_now(),
                            "type": "watcher_end",
                            "reason": "pid_not_alive",
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                out.flush()
                return 0

            time.sleep(max(5, int(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
