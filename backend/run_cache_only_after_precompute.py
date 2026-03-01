from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path


def _ts() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _append(log_path: Path, msg: str) -> None:
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> int:
    p = argparse.ArgumentParser(description="Wait for precompute meta, then run cache-only batch and append master log.")
    p.add_argument("--precompute-meta", required=True)
    p.add_argument("--master-log", required=True)
    p.add_argument("--fields-geojson", required=True)
    p.add_argument("--cache-dir", required=True)
    p.add_argument("--out-csv", required=True)
    p.add_argument("--events-auto-source", default="radar")
    p.add_argument("--events-auto-start", required=True)
    p.add_argument("--events-auto-end", required=True)
    p.add_argument("--analysis-modes", default="erosion_events_ml,abag")
    p.add_argument("--dem-source", default="cog")
    p.add_argument("--provider", default="auto")
    p.add_argument("--threshold", type=int, default=200)
    p.add_argument("--ml-threshold", type=float, default=0.05)
    p.add_argument("--poll-seconds", type=int, default=20)
    p.add_argument("--wait-timeout-min", type=int, default=360)
    args = p.parse_args()

    pre_meta = Path(args.precompute_meta).resolve()
    master = Path(args.master_log).resolve()

    _append(master, f"wait_precompute_meta={pre_meta}")
    t0 = time.time()
    max_wait = max(60, int(args.wait_timeout_min) * 60)
    while True:
        if pre_meta.exists():
            break
        if (time.time() - t0) > max_wait:
            _append(master, "ERROR timeout waiting for precompute meta")
            return 1
        time.sleep(max(1, int(args.poll_seconds)))

    try:
        meta = json.loads(pre_meta.read_text(encoding="utf-8"))
        _append(master, f"precompute_meta_loaded ok={meta.get('ok')} err={meta.get('error')} empty={meta.get('ok_empty_events')}")
    except Exception as exc:
        _append(master, f"WARN could not parse precompute meta: {exc}")

    script = Path(__file__).resolve().parent / "run_field_event_batch.py"
    cmd = [
        sys.executable,
        "-u",
        str(script),
        "--fields-geojson",
        str(Path(args.fields_geojson).resolve()),
        "--events-source",
        "auto",
        "--events-auto-source",
        str(args.events_auto_source),
        "--events-auto-start",
        str(args.events_auto_start),
        "--events-auto-end",
        str(args.events_auto_end),
        "--events-auto-top-n",
        "3",
        "--events-auto-min-severity",
        "1",
        "--events-auto-cache-dir",
        str(Path(args.cache_dir).resolve()),
        "--events-auto-cache-only",
        "--events-auto-use-cached-empty",
        "--analysis-modes",
        str(args.analysis_modes),
        "--provider",
        str(args.provider),
        "--dem-source",
        str(args.dem_source),
        "--threshold",
        str(int(args.threshold)),
        "--ml-threshold",
        str(float(args.ml_threshold)),
        "--out-csv",
        str(Path(args.out_csv).resolve()),
        "--continue-on-error",
    ]
    _append(master, "cache_only_cmd: " + " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        txt = line.rstrip("\r\n")
        if txt:
            _append(master, "[cache-only] " + txt)
    rc = int(proc.wait())
    _append(master, f"cache_only_exit_code={rc}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
