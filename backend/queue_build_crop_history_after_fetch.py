from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

def _fetch_running() -> bool:
    cmd = [
        "powershell",
        "-NoProfile",
        "-Command",
        "Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python' -and $_.CommandLine -match 'fetch_open_crop_history.py' } | Measure-Object | Select-Object -ExpandProperty Count",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if p.returncode != 0:
        return False
    txt = (p.stdout or "").strip()
    try:
        return int(txt) > 0
    except Exception:
        return False


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    log_dir = root / "data" / "derived" / "crop_history" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log = log_dir / f"queue_build_crop_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def w(msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
        print(line)
        with log.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    w("watcher started")
    while _fetch_running():
        w("fetch_open_crop_history still running; sleep 30s")
        time.sleep(30)

    w("fetch_open_crop_history finished; starting run_build_crop_history.bat")
    cmd = ["cmd.exe", "/c", "run_build_crop_history.bat"]
    p = subprocess.run(cmd, cwd=str(root), check=False)
    w(f"run_build_crop_history exit_code={p.returncode}")
    return int(p.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
