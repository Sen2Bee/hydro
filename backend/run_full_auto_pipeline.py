from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import time
from pathlib import Path


def _utc_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _count_tiles(layer_dir: Path) -> tuple[int, int]:
    tiles_dir = layer_dir / "tiles"
    manifest = layer_dir / "manifest.json"
    have = len(list(tiles_dir.glob("*.tif"))) if tiles_dir.exists() else 0
    total = 20
    if manifest.exists():
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            total = int(data.get("tile_count", total) or total)
        except Exception:
            pass
    return have, total


def _all_tiles_ready(tiles_root: Path) -> tuple[bool, dict[str, str]]:
    layers = ["K_Faktor", "R_Faktor", "S_Faktor", "Wasser_Erosion"]
    out: dict[str, str] = {}
    ok = True
    for layer in layers:
        have, total = _count_tiles(tiles_root / layer)
        out[layer] = f"{have}/{total}"
        if have < total:
            ok = False
    return ok, out


def _run_in_new_cmd(root: Path, command: str, log_path: Path) -> int:
    shell = f'cd /d "{root}" && {command} > "{log_path}" 2>&1'
    p = subprocess.Popen(["cmd.exe", "/c", shell], creationflags=subprocess.CREATE_NEW_CONSOLE)
    return int(p.pid)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Auto pipeline: ensure SA tiles complete, then launch safe night run."
    )
    p.add_argument("--tiles-root", default=str(Path("data") / "layers" / "st_mwl_erosion_sa_tiled"))
    p.add_argument("--poll-sec", type=int, default=60)
    p.add_argument("--target-res-m", type=int, default=10)
    p.add_argument("--tile-px", type=int, default=5000)
    args = p.parse_args()

    root = Path(__file__).resolve().parents[1]
    auto_dir = root / "paper" / "exports" / "automation" / "full_auto"
    auto_dir.mkdir(parents=True, exist_ok=True)
    run_tag = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = auto_dir / f"full_auto_{run_tag}.log"
    state_file = auto_dir / f"full_auto_{run_tag}.json"

    def log(msg: str) -> None:
        line = f"[{_utc_now()}] {msg}"
        print(line, flush=True)
        with log_file.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    tiles_root = Path(args.tiles_root).resolve()
    state: dict = {
        "run_tag": run_tag,
        "started_at_utc": _utc_now(),
        "tiles_root": str(tiles_root),
        "status": "running",
    }

    # Start tile fetch if not already fully ready.
    ready, detail = _all_tiles_ready(tiles_root)
    state["tiles_start"] = detail
    tile_pid = None
    if not ready:
        tile_log = auto_dir / f"tile_fetch_{run_tag}.log"
        tile_cmd = (
            f'run_st_mwl_erosion_fetch_sa_tiled.bat --target-res-m {int(args.target_res_m)} '
            f'--tile-px {int(args.tile_px)} --out-dir "{tiles_root}"'
        )
        tile_pid = _run_in_new_cmd(root, tile_cmd, tile_log)
        log(f"tile fetch started pid={tile_pid}")
        log(f"tile log={tile_log}")
    else:
        log("tiles already complete")

    # Wait until all tile layers complete.
    while True:
        ready, detail = _all_tiles_ready(tiles_root)
        log("tiles: " + ", ".join([f"{k}={v}" for k, v in detail.items()]))
        state["tiles_last"] = detail
        state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
        if ready:
            break
        time.sleep(max(10, int(args.poll_sec)))

    # Launch safe night run.
    night_log = auto_dir / f"night_start_{run_tag}.log"
    night_pid = _run_in_new_cmd(root, "run_night_sa_safe.bat", night_log)
    log(f"night runner launcher started pid={night_pid}")
    log(f"night launcher log={night_log}")

    state["status"] = "launched_night_run"
    state["finished_at_utc"] = _utc_now()
    state["tile_pid"] = tile_pid
    state["night_pid"] = night_pid
    state["log_file"] = str(log_file)
    state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
    log(f"state={state_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
