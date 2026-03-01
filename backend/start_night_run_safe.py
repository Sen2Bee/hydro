from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def _utc_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _tag() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d_%H%M%S")


def _http_ok(url: str, timeout_s: float = 2.0) -> bool:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            code = int(getattr(resp, "status", 0) or 0)
            return 200 <= code < 500
    except urllib.error.HTTPError as e:
        return 200 <= int(e.code) < 500
    except Exception:
        return False


def _wait_http(url: str, wait_s: int) -> bool:
    t_end = time.time() + max(1, int(wait_s))
    while time.time() < t_end:
        if _http_ok(url, timeout_s=2.0):
            return True
        time.sleep(2)
    return False


def _free_gb(path: Path) -> float:
    usage = shutil.disk_usage(str(path))
    return float(usage.free) / float(1024**3)


def _tile_layer_ready(layer_dir: Path) -> bool:
    if not layer_dir.exists():
        return False
    manifest = layer_dir / "manifest.json"
    tiles_dir = layer_dir / "tiles"
    if not manifest.exists() or not tiles_dir.exists():
        return False
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except Exception:
        return False
    tile_count = int(data.get("tile_count", 0) or 0)
    tif_count = len(list(tiles_dir.glob("*.tif")))
    return tile_count > 0 and tif_count >= tile_count


def _ensure_tiles_ready(tiles_root: Path, require: bool) -> tuple[bool, dict]:
    layers = ["K_Faktor", "R_Faktor", "S_Faktor", "Wasser_Erosion"]
    status: dict[str, bool] = {}
    for layer in layers:
        status[layer] = _tile_layer_ready(tiles_root / layer)
    ok = all(status.values())
    if not require:
        return True, status
    return ok, status


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Safe overnight launcher for SA ICON2D chunk runs (checks + backend + resume)."
    )
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--chunk-size", type=int, default=1000)
    p.add_argument("--start-chunk", type=int, default=1)
    p.add_argument("--max-chunks", type=int, default=100)
    p.add_argument("--checkpoint-every", type=int, default=100)
    p.add_argument("--events-auto-top-n", type=int, default=3)
    p.add_argument("--events-auto-min-severity", type=int, default=1)
    p.add_argument("--analysis-modes", default="erosion_events_ml,abag")
    p.add_argument("--provider", default="auto")
    p.add_argument("--dem-source", default="cog")
    p.add_argument("--threshold", type=int, default=200)
    p.add_argument("--ml-threshold", type=float, default=0.05)
    p.add_argument("--min-field-area-ha", type=float, default=0.05)
    p.add_argument("--max-field-area-ha", type=float, default=0.0)
    p.add_argument("--field-id-whitelist-file", default="")
    p.add_argument("--field-id-whitelist-column", default="")
    p.add_argument("--min-free-gb", type=float, default=20.0)
    p.add_argument("--require-tiles-ready", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--backend-autostart", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--backend-wait-sec", type=int, default=180)
    p.add_argument(
        "--tiles-root",
        default=str(Path("data") / "layers" / "st_mwl_erosion_sa_tiled"),
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)

    night_dir = root / "paper" / "exports" / "automation" / "night_runs"
    night_dir.mkdir(parents=True, exist_ok=True)
    run_tag = _tag()
    launch_path = night_dir / f"night_launch_{run_tag}.json"
    backend_log = night_dir / f"night_backend_{run_tag}.log"
    runner_log = night_dir / f"night_runner_{run_tag}.log"

    launch: dict = {
        "run_tag": run_tag,
        "started_at_utc": _utc_now(),
        "cwd": str(root),
        "args": vars(args),
        "status": "starting",
    }

    source_sqlite = root / "data" / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"
    if not source_sqlite.exists():
        launch["status"] = "failed"
        launch["error"] = f"missing source sqlite: {source_sqlite}"
        launch_path.write_text(json.dumps(launch, indent=2), encoding="utf-8")
        print(f"[NIGHT] FAIL: {launch['error']}")
        return 1

    free_gb = _free_gb(root)
    launch["disk_free_gb"] = round(free_gb, 2)
    if free_gb < float(args.min_free_gb):
        launch["status"] = "failed"
        launch["error"] = f"free space {free_gb:.2f} GB < min {args.min_free_gb:.2f} GB"
        launch_path.write_text(json.dumps(launch, indent=2), encoding="utf-8")
        print(f"[NIGHT] FAIL: {launch['error']}")
        return 1

    tiles_root = Path(args.tiles_root).resolve()
    tiles_ok, tiles_status = _ensure_tiles_ready(tiles_root, require=bool(args.require_tiles_ready))
    launch["tiles_root"] = str(tiles_root)
    launch["tiles_status"] = tiles_status
    if not tiles_ok:
        launch["status"] = "failed"
        launch["error"] = "required SA tile layers not complete"
        launch_path.write_text(json.dumps(launch, indent=2), encoding="utf-8")
        print("[NIGHT] FAIL: required SA tile layers not complete")
        for k, v in tiles_status.items():
            print(f"  - {k}: {'OK' if v else 'MISSING'}")
        return 1

    api_root = args.api_base_url.rstrip("/") + "/"
    backend_pid = None
    if not _http_ok(api_root, timeout_s=2.0):
        if not bool(args.backend_autostart):
            launch["status"] = "failed"
            launch["error"] = f"backend not reachable at {api_root}"
            launch_path.write_text(json.dumps(launch, indent=2), encoding="utf-8")
            print(f"[NIGHT] FAIL: backend not reachable at {api_root}")
            return 1

        backend_cmd = f'cd /d "{root}" && call run_backend.bat > "{backend_log}" 2>&1'
        p_backend = subprocess.Popen(["cmd.exe", "/c", backend_cmd], creationflags=subprocess.CREATE_NEW_CONSOLE)
        backend_pid = int(p_backend.pid)
        print(f"[NIGHT] backend start pid={backend_pid}")
        if not _wait_http(api_root, wait_s=int(args.backend_wait_sec)):
            launch["status"] = "failed"
            launch["error"] = f"backend did not become reachable in {args.backend_wait_sec}s"
            launch["backend_pid"] = backend_pid
            launch["backend_log"] = str(backend_log)
            launch_path.write_text(json.dumps(launch, indent=2), encoding="utf-8")
            print(f"[NIGHT] FAIL: {launch['error']}")
            return 1

    py_exe = sys.executable
    runner_cmd_list = [
        f'"{py_exe}"',
        "-u",
        "backend/run_sa_icon2d_multiwindow_chunks.py",
        "--chunk-size",
        str(args.chunk_size),
        "--start-chunk",
        str(args.start_chunk),
        "--max-chunks",
        str(args.max_chunks),
        "--checkpoint-every",
        str(args.checkpoint_every),
        "--events-auto-top-n",
        str(args.events_auto_top_n),
        "--events-auto-min-severity",
        str(args.events_auto_min_severity),
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
        "--resume",
        "--validate-chunk",
        "--fail-on-qa-error",
        "--continue-on-error",
        "--min-field-area-ha",
        str(args.min_field_area_ha),
        "--max-field-area-ha",
        str(args.max_field_area_ha),
    ]
    if str(args.field_id_whitelist_file or "").strip():
        runner_cmd_list.extend(["--field-id-whitelist-file", str(args.field_id_whitelist_file)])
    if str(args.field_id_whitelist_column or "").strip():
        runner_cmd_list.extend(["--field-id-whitelist-column", str(args.field_id_whitelist_column)])
    runner_cmd = " ".join(runner_cmd_list)
    runner_shell = f'cd /d "{root}" && {runner_cmd} > "{runner_log}" 2>&1'
    p_runner = subprocess.Popen(["cmd.exe", "/c", runner_shell], creationflags=subprocess.CREATE_NEW_CONSOLE)

    launch["status"] = "running"
    launch["backend_pid"] = backend_pid
    launch["backend_log"] = str(backend_log) if backend_pid else None
    launch["runner_pid"] = int(p_runner.pid)
    launch["runner_log"] = str(runner_log)
    launch["runner_cmd"] = runner_cmd
    launch["started_runner_at_utc"] = _utc_now()
    launch_path.write_text(json.dumps(launch, indent=2), encoding="utf-8")

    print("[NIGHT] OK")
    print(f"[NIGHT] launch={launch_path}")
    print(f"[NIGHT] runner_pid={p_runner.pid}")
    print(f"[NIGHT] runner_log={runner_log}")
    if backend_pid:
        print(f"[NIGHT] backend_pid={backend_pid}")
        print(f"[NIGHT] backend_log={backend_log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
