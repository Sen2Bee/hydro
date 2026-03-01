from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def _is_running(pattern: str) -> bool:
    cmd = [
        "powershell",
        "-NoProfile",
        "-Command",
        f"Get-CimInstance Win32_Process | Where-Object {{ $_.Name -match 'python|cmd' -and $_.CommandLine -match '{pattern}' }} | Measure-Object | Select-Object -ExpandProperty Count",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if p.returncode != 0:
        return False
    try:
        return int((p.stdout or "0").strip()) > 0
    except Exception:
        return False


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    log_dir = root / "data" / "layers" / "c_dynamic_sa" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log = log_dir / f"queue_dynamic_c_after_crop_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    crop_csv = root / "data" / "derived" / "crop_history" / "crop_history.csv"

    def w(msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
        print(line)
        with log.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    w("watcher started")
    while True:
        crop_ready = crop_csv.exists() and crop_csv.stat().st_size > 0
        crop_builder_running = _is_running("build_crop_history_from_open_data.py|run_build_crop_history.bat")
        dyn_running = _is_running("build_dynamic_c_windows.py|run_build_dynamic_c_windows")
        if crop_ready and not crop_builder_running and not dyn_running:
            break
        w(
            f"wait crop_ready={crop_ready} crop_builder_running={crop_builder_running} dynamic_c_running={dyn_running}; sleep 60s"
        )
        time.sleep(60)

    w("starting run_build_dynamic_c_windows_with_crop.bat")
    p = subprocess.run(["cmd.exe", "/c", "run_build_dynamic_c_windows_with_crop.bat"], cwd=str(root), check=False)
    w(f"dynamic_c_with_crop exit_code={p.returncode}")
    return int(p.returncode)


if __name__ == "__main__":
    raise SystemExit(main())

