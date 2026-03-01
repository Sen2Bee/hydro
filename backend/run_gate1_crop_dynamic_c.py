from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


REQUIRED_WINDOWS = [
    ("2023-04-01", "2023-10-31"),
    ("2024-04-01", "2024-10-31"),
    ("2025-04-01", "2025-10-31"),
]


def _win_tag(s: str, e: str) -> str:
    return f"{s.replace('-', '')}_{e.replace('-', '')}"


def _count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        n = -1  # header
        for _ in r:
            n += 1
    return max(0, n)


def _safe_json(path: Path) -> dict | None:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def main() -> int:
    p = argparse.ArgumentParser(description="Gate-1 check: crop history + dynamic C readiness.")
    p.add_argument("--root", default=".")
    p.add_argument("--out-dir", default="paper/exports/qa")
    p.add_argument("--strict", action="store_true", help="Fail if any required window is missing.")
    args = p.parse_args()

    root = Path(args.root).resolve()
    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    crop_csv = root / "data" / "derived" / "crop_history" / "crop_history.csv"
    crop_meta = root / "data" / "derived" / "crop_history" / "crop_history.meta.json"
    c_run_manifest = root / "data" / "layers" / "c_dynamic_sa" / "run_manifest.json"

    crop_rows = _count_rows(crop_csv)
    crop_meta_obj = _safe_json(crop_meta) or {}
    c_manifest_obj = _safe_json(c_run_manifest) or {}

    windows_status = []
    total_crop_matches = 0
    for s, e in REQUIRED_WINDOWS:
        tag = _win_tag(s, e)
        c_tif = root / "data" / "layers" / "c_dynamic_sa" / "c_factor" / f"C_Faktor_{tag}.tif"
        c_meta_path = c_tif.with_suffix(".json")
        c_meta_obj = _safe_json(c_meta_path) or {}
        matches = int(c_meta_obj.get("crop_history_matches") or 0)
        total_crop_matches += matches
        ok = c_tif.exists() and c_meta_path.exists()
        windows_status.append(
            {
                "start": s,
                "end": e,
                "tag": tag,
                "c_tif": str(c_tif),
                "c_meta": str(c_meta_path),
                "exists": bool(ok),
                "crop_history_matches": matches,
                "method_version": c_meta_obj.get("method_version"),
            }
        )

    checks = {
        "crop_history_csv_exists": crop_csv.exists(),
        "crop_history_meta_exists": crop_meta.exists(),
        "crop_history_rows_gt_0": crop_rows > 0,
        "dynamic_c_manifest_exists": c_run_manifest.exists(),
        "dynamic_c_windows_present": all(w["exists"] for w in windows_status),
        "crop_matches_gt_0": total_crop_matches > 0,
    }

    failed = [k for k, v in checks.items() if not bool(v)]
    status = "pass"
    if failed:
        status = "fail" if args.strict else "warn"

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "strict": bool(args.strict),
        "checks": checks,
        "failed_checks": failed,
        "crop_history": {
            "csv": str(crop_csv),
            "meta": str(crop_meta),
            "rows": crop_rows,
            "meta_summary": {
                "rows_written": crop_meta_obj.get("rows_written"),
                "per_year_stats": crop_meta_obj.get("per_year_stats"),
            },
        },
        "dynamic_c": {
            "run_manifest": str(c_run_manifest),
            "run_manifest_status": c_manifest_obj.get("finished_at"),
            "total_crop_matches": total_crop_matches,
            "windows": windows_status,
        },
    }

    out_json = out_dir / f"gate1_crop_dynamic_c_{ts}.json"
    out_md = out_dir / f"gate1_crop_dynamic_c_{ts}.md"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Gate-1 Crop + Dynamic-C",
        "",
        f"- Status: **{status}**",
        f"- Strict: `{args.strict}`",
        f"- Report JSON: `{out_json}`",
        "",
        "## Checks",
    ]
    for k, v in checks.items():
        lines.append(f"- `{k}`: `{v}`")
    lines += ["", "## Crop History", f"- rows: `{crop_rows}`", "", "## Windows"]
    for w in windows_status:
        lines.append(
            f"- `{w['tag']}` exists=`{w['exists']}` crop_matches=`{w['crop_history_matches']}` method=`{w['method_version']}`"
        )
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[GATE1] status={status}")
    print(f"[GATE1] json={out_json}")
    print(f"[GATE1] md={out_md}")
    if status == "fail":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

