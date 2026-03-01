from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import requests


WINDOWS = {
    "2023": ("20230401_20231031", "2023-04-01", "2023-10-31"),
    "2024": ("20240401_20241031", "2024-04-01", "2024-10-31"),
    "2025": ("20250401_20251031", "2025-04-01", "2025-10-31"),
}


def _wait_health(api_base_url: str, retries: int, sleep_s: float) -> bool:
    url = f"{api_base_url.rstrip('/')}/openapi.json"
    for _ in range(max(1, int(retries))):
        try:
            r = requests.get(url, timeout=10)
            if int(r.status_code) == 200:
                return True
        except Exception:
            pass
        time.sleep(max(0.5, float(sleep_s)))
    return False


def _meta_ok(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return int(d.get("error", d.get("err", 0)) or 0) == 0


def main() -> int:
    p = argparse.ArgumentParser(description="Repair failed Stage-A chunks from audit JSON.")
    p.add_argument("--repo-root", default=r"D:\__GeoFlux\hydrowatch")
    p.add_argument("--audit-json", required=True)
    p.add_argument("--selection", choices=["common", "union"], default="common")
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--health-retries", type=int, default=20)
    p.add_argument("--health-sleep-s", type=float, default=3.0)
    p.add_argument("--request-retries", type=int, default=3)
    p.add_argument("--retry-backoff-initial-s", type=float, default=2.0)
    p.add_argument("--retry-backoff-max-s", type=float, default=20.0)
    p.add_argument("--min-interval-s", type=float, default=1.5)
    p.add_argument("--checkpoint-every", type=int, default=100)
    p.add_argument("--top-n", type=int, default=5)
    p.add_argument("--min-severity", type=int, default=0)
    p.add_argument("--weather-cell-km", type=float, default=2.0)
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    audit = json.loads(Path(args.audit_json).resolve().read_text(encoding="utf-8"))
    chunk_ids = audit.get("common_bad_chunks" if args.selection == "common" else "union_bad_chunks", []) or []
    chunk_ids = [int(x) for x in chunk_ids]
    if not chunk_ids:
        print("[REPAIR] no chunks to repair")
        return 0

    print(f"[REPAIR] selection={args.selection} chunks={len(chunk_ids)}")
    script = repo / "backend" / "precompute_auto_events_cache.py"
    py = sys.executable
    failures: list[dict] = []
    repaired = 0

    for cid in chunk_ids:
        cid_s = f"{cid:05d}"
        print(f"[REPAIR] chunk={cid_s}")
        for y, (wkey, ws, we) in WINDOWS.items():
            chunks_geojson = repo / "paper" / "input" / "sa_precompute_chunks" / wkey / f"schlaege_chunk_{cid_s}.geojson"
            out_csv = repo / "paper" / "exports" / "sa_precompute" / wkey / f"precompute_chunk_{cid_s}.csv"
            out_log = repo / "paper" / "exports" / "sa_precompute" / wkey / f"precompute_chunk_{cid_s}.repair.log"
            field_cache = repo / "data" / "events" / "sa_2km" / f"icon2d_{wkey}" / "field_cache"
            cell_cache = repo / "data" / "events" / "sa_2km" / f"icon2d_{wkey}" / "cell_cache"
            done_flag = repo / "paper" / "exports" / "sa_precompute" / wkey / f"chunk_{cid_s}.done"
            meta_path = out_csv.with_suffix(".meta.json")

            if not chunks_geojson.exists():
                failures.append({"chunk": cid, "year": y, "reason": "missing_geojson"})
                continue
            if not _wait_health(args.api_base_url, args.health_retries, args.health_sleep_s):
                failures.append({"chunk": cid, "year": y, "reason": "backend_unreachable"})
                continue

            cmd = [
                py, "-u", str(script),
                "--fields-geojson", str(chunks_geojson),
                "--cache-dir", str(field_cache),
                "--cell-cache-dir", str(cell_cache),
                "--api-base-url", str(args.api_base_url),
                "--source", "icon2d",
                "--start", ws,
                "--end", we,
                "--top-n", str(int(args.top_n)),
                "--min-severity", str(int(args.min_severity)),
                "--weather-cell-km", str(float(args.weather_cell_km)),
                "--request-retries", str(int(args.request_retries)),
                "--retry-backoff-initial-s", str(float(args.retry_backoff_initial_s)),
                "--retry-backoff-max-s", str(float(args.retry_backoff_max_s)),
                "--min-interval-s", str(float(args.min_interval_s)),
                "--checkpoint-every", str(int(args.checkpoint_every)),
                "--out-csv", str(out_csv),
                "--log-file", str(out_log),
            ]
            rc = int(subprocess.run(cmd, check=False).returncode)
            if rc != 0 or not _meta_ok(meta_path):
                failures.append({"chunk": cid, "year": y, "reason": "repair_failed", "exit_code": rc})
            else:
                done_flag.write_text("ok\n", encoding="utf-8")
                repaired += 1

    rep = {
        "selection": args.selection,
        "chunks_total": len(chunk_ids),
        "repairs_ok": repaired,
        "failures": failures,
        "failures_count": len(failures),
    }
    out = repo / "paper" / "exports" / "automation" / "stage_a_repair_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(f"[REPAIR] report={out}")
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())

