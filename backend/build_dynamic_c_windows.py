from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_WINDOWS = [
    ("2023-04-01", "2023-10-31"),
    ("2024-04-01", "2024-10-31"),
    ("2025-04-01", "2025-10-31"),
]

# Sachsen-Anhalt extent (WGS84), derived from Feldbloecke service extent.
DEFAULT_SA_BBOX = (10.534168657927681, 50.97330866288862, 13.283020232424267, 52.98996620841052)


def _parse_windows(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for part in (text or "").split(","):
        s = part.strip()
        if not s:
            continue
        if ":" not in s:
            raise ValueError(f"invalid window '{s}', expected start:end")
        a, b = s.split(":", 1)
        out.append((a.strip(), b.strip()))
    if not out:
        raise ValueError("no windows specified")
    return out


def _run_cmd(cmd: list[str]) -> None:
    proc = subprocess.run(cmd, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Build dynamic C-factor windows (NDVI + C proxy) for SA or custom AOI."
    )
    p.add_argument("--west", type=float, default=DEFAULT_SA_BBOX[0])
    p.add_argument("--south", type=float, default=DEFAULT_SA_BBOX[1])
    p.add_argument("--east", type=float, default=DEFAULT_SA_BBOX[2])
    p.add_argument("--north", type=float, default=DEFAULT_SA_BBOX[3])
    p.add_argument(
        "--windows",
        default=",".join([f"{a}:{b}" for a, b in DEFAULT_WINDOWS]),
        help="Comma-separated windows: YYYY-MM-DD:YYYY-MM-DD,...",
    )
    p.add_argument("--max-cloud", type=float, default=40.0)
    p.add_argument("--resolution-m", type=float, default=10.0)
    p.add_argument(
        "--template-raster",
        default=str(Path("data") / "layers" / "st_mwl_erosion_sa_tiled" / "K_Faktor" / "K_Faktor.vrt"),
    )
    p.add_argument(
        "--out-root",
        default=str(Path("data") / "layers" / "c_dynamic_sa"),
        help="Root output dir with subfolders ndvi/ and c_factor/",
    )
    p.add_argument(
        "--c-config",
        default=str(Path("data") / "config" / "c_factor_method_v1.json"),
        help="Optional C method config JSON for build_c_factor_proxy.py",
    )
    p.add_argument(
        "--crop-history-csv",
        default=None,
        help="Optional crop history CSV (flik,crop_code,year) passed to C builder.",
    )
    p.add_argument(
        "--crop-year-mode",
        choices=["none", "start_year", "end_year"],
        default="start_year",
        help="How to derive crop-year per window when crop history is used.",
    )
    args = p.parse_args()

    windows = _parse_windows(args.windows)
    out_root = Path(args.out_root)
    ndvi_root = out_root / "ndvi"
    c_root = out_root / "c_factor"
    ndvi_root.mkdir(parents=True, exist_ok=True)
    c_root.mkdir(parents=True, exist_ok=True)

    run_manifest: dict = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "bbox_wgs84": {
            "west": args.west,
            "south": args.south,
            "east": args.east,
            "north": args.north,
        },
        "windows": [],
        "settings": {
            "max_cloud": args.max_cloud,
            "resolution_m": args.resolution_m,
            "template_raster": args.template_raster,
            "python": sys.executable,
        },
    }

    fetch_script = str(Path("backend") / "fetch_sentinel_ndvi.py")
    c_script = str(Path("backend") / "build_c_factor_proxy.py")

    for i, (start, end) in enumerate(windows, start=1):
        tag = f"{start.replace('-', '')}_{end.replace('-', '')}"
        ndvi_out = ndvi_root / f"NDVI_{tag}.tif"
        c_out = c_root / f"C_Faktor_{tag}.tif"
        print(f"[{i}/{len(windows)}] window={start}..{end}")

        fetch_cmd = [
            sys.executable,
            fetch_script,
            "--west",
            str(args.west),
            "--south",
            str(args.south),
            "--east",
            str(args.east),
            "--north",
            str(args.north),
            "--start",
            start,
            "--end",
            end,
            "--max-cloud",
            str(args.max_cloud),
            "--resolution-m",
            str(args.resolution_m),
            "--out-tif",
            str(ndvi_out),
        ]
        _run_cmd(fetch_cmd)

        crop_year = None
        if args.crop_history_csv:
            if args.crop_year_mode == "start_year":
                crop_year = int(start.split("-", 1)[0])
            elif args.crop_year_mode == "end_year":
                crop_year = int(end.split("-", 1)[0])

        c_cmd = [
            sys.executable,
            c_script,
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
            str(ndvi_out),
            "--out-tif",
            str(c_out),
            "--season-label",
            f"{start}..{end}",
        ]
        if args.c_config:
            c_cmd.extend(["--c-config", str(args.c_config)])
        if args.crop_history_csv:
            c_cmd.extend(["--crop-history-csv", str(args.crop_history_csv)])
            if crop_year is not None:
                c_cmd.extend(["--crop-year", str(crop_year)])
        _run_cmd(c_cmd)

        run_manifest["windows"].append(
            {
                "start": start,
                "end": end,
                "ndvi_tif": str(ndvi_out),
                "ndvi_meta": str(ndvi_out.with_suffix(".json")),
                "c_tif": str(c_out),
                "c_meta": str(c_out.with_suffix(".json")),
            }
        )

    run_manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    run_manifest["out_root"] = str(out_root)
    mpath = out_root / "run_manifest.json"
    mpath.write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")
    print(f"[OK] dynamic C windows finished -> {mpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
