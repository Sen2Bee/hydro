import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path


PROGRESS_RE = re.compile(r"\[(\d+)(?:/(\d+))?\s*\|?.*?\]\s*field=")
ERROR_502_RE = re.compile(r"auto-events fetch failed:\s*502", re.IGNORECASE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watch SA chunk batch progress and write periodic status log.")
    p.add_argument("--run-log", required=True, help="Path to active run log (sa_chunks_*run*.log)")
    p.add_argument(
        "--state-file",
        default=str(Path("paper") / "exports" / "sa_chunks" / "sa_chunk_run_state.json"),
        help="Path to chunk run state JSON",
    )
    p.add_argument(
        "--exports-dir",
        default=str(Path("paper") / "exports" / "sa_chunks"),
        help="SA chunk exports directory",
    )
    p.add_argument(
        "--out-log",
        default=str(Path("paper") / "exports" / "automation" / f"sa_progress_watch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        help="Status output log",
    )
    p.add_argument("--interval-sec", type=int, default=60, help="Polling interval in seconds")
    p.add_argument("--max-lines-scan", type=int, default=1200, help="Max recent lines to scan in run log")
    return p.parse_args()


def read_state(state_file: Path) -> dict:
    if not state_file.exists():
        return {}
    try:
        return json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return {}


def tail_lines(path: Path, max_lines: int) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        if len(lines) <= max_lines:
            return lines
        return lines[-max_lines:]
    except Exception:
        return []


def latest_progress(lines: list[str]) -> tuple[int | None, int | None]:
    last_n = None
    last_total = None
    for ln in lines:
        m = PROGRESS_RE.search(ln)
        if not m:
            continue
        try:
            last_n = int(m.group(1))
        except Exception:
            last_n = None
        g2 = m.group(2)
        if g2:
            try:
                last_total = int(g2)
            except Exception:
                pass
    return last_n, last_total


def append_line(out_log: Path, text: str) -> None:
    out_log.parent.mkdir(parents=True, exist_ok=True)
    with out_log.open("a", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")


def latest_chunk_csv(exports_dir: Path) -> tuple[Path | None, int | None]:
    files = sorted(exports_dir.glob("field_event_results_chunk_*.csv"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    if not files:
        return None, None
    p = files[0]
    m = re.search(r"chunk_(\d+)\.csv$", p.name)
    idx = int(m.group(1)) if m else None
    return p, idx


def main() -> int:
    args = parse_args()
    run_log = Path(args.run_log)
    state_file = Path(args.state_file)
    exports_dir = Path(args.exports_dir)
    out_log = Path(args.out_log)

    append_line(out_log, f"[{utc_now()}] watcher start run_log={run_log}")

    prev_t = time.time()
    prev_n = None
    prev_csv_size = None
    while True:
        now_t = time.time()
        dt_s = max(1e-6, now_t - prev_t)

        lines = tail_lines(run_log, args.max_lines_scan)
        n, n_total = latest_progress(lines)
        err_502_tail = sum(1 for ln in lines if ERROR_502_RE.search(ln))

        rate_per_min = None
        if n is not None and prev_n is not None:
            dn = n - prev_n
            if dn >= 0:
                rate_per_min = (dn / dt_s) * 60.0

        state = read_state(state_file)
        completed = state.get("completed") or []
        failed = state.get("failed") or []
        run_end_chunk = state.get("run_end_chunk")
        total_chunks = state.get("total_chunks")

        done_flags = sorted(exports_dir.glob("*.done"))
        chunk_csvs = sorted(exports_dir.glob("field_event_results_chunk_*.csv"))
        active_csv, active_idx = latest_chunk_csv(exports_dir)
        active_csv_size = None
        active_csv_mtime = None
        csv_rate_mb_min = None
        if active_csv and active_csv.exists():
            st = active_csv.stat()
            active_csv_size = int(st.st_size)
            active_csv_mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
            if prev_csv_size is not None:
                dsz = active_csv_size - prev_csv_size
                if dsz >= 0:
                    csv_rate_mb_min = (dsz / dt_s) * 60.0 / (1024.0 * 1024.0)

        eta_txt = "n/a"
        if n_total and n and rate_per_min and rate_per_min > 0:
            rem = max(0, n_total - n)
            eta_min = rem / rate_per_min
            eta_txt = f"{eta_min:.1f} min"

        line = (
            f"[{utc_now()}] "
            f"n={n if n is not None else '-'} "
            f"total={n_total if n_total is not None else '-'} "
            f"rate={f'{rate_per_min:.1f}/min' if rate_per_min is not None else '-'} "
            f"eta={eta_txt} "
            f"tail502={err_502_tail} "
            f"chunk_csv={len(chunk_csvs)} done={len(done_flags)} "
            f"active_chunk={active_idx if active_idx is not None else '-'} "
            f"active_csv_mb={f'{(active_csv_size or 0)/(1024.0*1024.0):.2f}'} "
            f"csv_rate={f'{csv_rate_mb_min:.2f}MB/min' if csv_rate_mb_min is not None else '-'} "
            f"active_csv_mtime={active_csv_mtime if active_csv_mtime else '-'} "
            f"state_completed={len(completed)} state_failed={len(failed)} "
            f"run_end_chunk={run_end_chunk if run_end_chunk is not None else '-'} "
            f"total_chunks={total_chunks if total_chunks is not None else '-'}"
        )
        append_line(out_log, line)

        prev_t = now_t
        if n is not None:
            prev_n = n
        if active_csv_size is not None:
            prev_csv_size = active_csv_size
        time.sleep(max(5, int(args.interval_sec)))


if __name__ == "__main__":
    raise SystemExit(main())
