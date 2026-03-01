from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio


def _stats(path: Path) -> dict:
    with rasterio.open(path) as ds:
        arr = ds.read(1).astype(np.float32)
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        return {"count": 0}
    return {
        "count": int(finite.size),
        "mean": float(np.mean(finite)),
        "std": float(np.std(finite)),
        "p10": float(np.percentile(finite, 10)),
        "p50": float(np.percentile(finite, 50)),
        "p90": float(np.percentile(finite, 90)),
        "min": float(np.min(finite)),
        "max": float(np.max(finite)),
        "share_gt_0_30": float(np.mean(finite > 0.30)),
        "share_gt_0_40": float(np.mean(finite > 0.40)),
    }


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, check=False)
    if p.returncode != 0:
        raise RuntimeError(f"command failed ({p.returncode}): {' '.join(cmd)}")


def main() -> int:
    p = argparse.ArgumentParser(description="Run C-factor sensitivity across multiple method configs.")
    p.add_argument("--west", type=float, required=True)
    p.add_argument("--south", type=float, required=True)
    p.add_argument("--east", type=float, required=True)
    p.add_argument("--north", type=float, required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--template-raster", required=True)
    p.add_argument("--configs", required=True, help="Comma-separated config JSON paths.")
    p.add_argument("--out-dir", default=str(Path("paper") / "exports" / "c_sensitivity"))
    p.add_argument("--max-cloud", type=float, default=40.0)
    p.add_argument("--resolution-m", type=float, default=10.0)
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = f"{args.start.replace('-', '')}_{args.end.replace('-', '')}"
    ndvi_tif = out_dir / f"NDVI_{tag}.tif"

    fetch_cmd = [
        sys.executable,
        str(Path("backend") / "fetch_sentinel_ndvi.py"),
        "--west",
        str(args.west),
        "--south",
        str(args.south),
        "--east",
        str(args.east),
        "--north",
        str(args.north),
        "--start",
        str(args.start),
        "--end",
        str(args.end),
        "--max-cloud",
        str(args.max_cloud),
        "--resolution-m",
        str(args.resolution_m),
        "--out-tif",
        str(ndvi_tif),
    ]
    _run(fetch_cmd)

    configs = [Path(x.strip()) for x in args.configs.split(",") if x.strip()]
    if not configs:
        raise RuntimeError("no configs given")

    runs = []
    for cfg in configs:
        cfg_name = cfg.stem
        out_tif = out_dir / f"C_{cfg_name}_{tag}.tif"
        cmd = [
            sys.executable,
            str(Path("backend") / "build_c_factor_proxy.py"),
            "--west",
            str(args.west),
            "--south",
            str(args.south),
            "--east",
            str(args.east),
            "--north",
            str(args.north),
            "--template-raster",
            str(args.template_raster),
            "--ndvi-raster",
            str(ndvi_tif),
            "--out-tif",
            str(out_tif),
            "--c-config",
            str(cfg),
            "--season-label",
            f"{args.start}..{args.end}",
        ]
        _run(cmd)
        runs.append(
            {
                "config": str(cfg),
                "config_name": cfg_name,
                "out_tif": str(out_tif),
                "stats": _stats(out_tif),
            }
        )

    baseline = runs[0]["stats"]
    for r in runs[1:]:
        s = r["stats"]
        r["delta_vs_baseline"] = {
            "mean": float(s.get("mean", 0.0) - baseline.get("mean", 0.0)),
            "p90": float(s.get("p90", 0.0) - baseline.get("p90", 0.0)),
            "share_gt_0_30": float(s.get("share_gt_0_30", 0.0) - baseline.get("share_gt_0_30", 0.0)),
        }

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "window": {"start": args.start, "end": args.end},
        "bbox_wgs84": {
            "west": args.west,
            "south": args.south,
            "east": args.east,
            "north": args.north,
        },
        "ndvi_tif": str(ndvi_tif),
        "runs": runs,
    }
    out_json = out_dir / f"c_sensitivity_{tag}.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[OK] report={out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

