from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from validate_field_event_results import validate


def _chunk_index_from_name(name: str) -> int:
    m = re.search(r"chunk_(\d+)\.csv$", name)
    if not m:
        return 0
    return int(m.group(1))


def _discover_chunk_csvs(exports_dir: Path) -> list[Path]:
    files = [p for p in exports_dir.glob("field_event_results_chunk_*.csv") if p.is_file()]
    files.sort(key=lambda p: _chunk_index_from_name(p.name))
    return files


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _metric(rows: list[dict[str, str]], key: str) -> tuple[float | None, int]:
    vals: list[float] = []
    for r in rows:
        s = str(r.get(key) or "").strip()
        if not s:
            continue
        try:
            vals.append(float(s))
        except Exception:
            continue
    if not vals:
        return None, 0
    return sum(vals) / len(vals), len(vals)


def _render_report(
    *,
    generated_at_utc: str,
    exports_dir: Path,
    merged_csv: Path,
    qa_json: Path,
    chunk_files: list[Path],
    rows: list[dict[str, str]],
    qa: dict[str, Any],
) -> str:
    by_mode: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for r in rows:
        mode = str(r.get("analysis_type") or "").strip() or "unknown"
        by_mode[mode] = by_mode.get(mode, 0) + 1
        st = str(r.get("status") or "").strip() or "unknown"
        by_status[st] = by_status.get(st, 0) + 1

    p_mean, p_n = _metric(rows, "event_probability_mean")
    a_mean, a_n = _metric(rows, "abag_index_mean")
    r_mean, r_n = _metric(rows, "risk_score_mean")

    lines = [
        "# SA Chunk Merge Report",
        "",
        f"- Generated (UTC): `{generated_at_utc}`",
        f"- Exports dir: `{exports_dir}`",
        f"- Source chunks: `{len(chunk_files)}`",
        f"- Merged CSV: `{merged_csv}`",
        f"- QA JSON: `{qa_json}`",
        "",
        "## Coverage",
        "",
        f"- rows_total: `{len(rows)}`",
        f"- rows_by_mode: `{json.dumps(by_mode, ensure_ascii=True)}`",
        f"- rows_by_status: `{json.dumps(by_status, ensure_ascii=True)}`",
        "",
        "## QA Summary",
        "",
        f"- ok: `{qa.get('ok')}`",
        f"- error_rate_percent: `{qa.get('error_rate_percent')}`",
        f"- nodata_rate_percent: `{qa.get('nodata_rate_percent')}`",
        f"- rows_error: `{qa.get('rows_error')}`",
        f"- warnings_count: `{len(qa.get('warnings') or [])}`",
        f"- errors_count: `{len(qa.get('errors') or [])}`",
        "",
        "## Mean Metrics",
        "",
        f"- event_probability_mean: `{round(p_mean, 4) if p_mean is not None else None}` (n={p_n})",
        f"- abag_index_mean: `{round(a_mean, 4) if a_mean is not None else None}` (n={a_n})",
        f"- risk_score_mean: `{round(r_mean, 4) if r_mean is not None else None}` (n={r_n})",
        "",
        "## Chunk Files",
        "",
    ]
    for p in chunk_files:
        lines.append(f"- `{p.name}`")
    return "\n".join(lines) + "\n"


def _run_quickcheck(merged_csv: Path, out_dir: Path, label: str) -> int:
    script = Path(__file__).resolve().parent / "export_quickcheck_package.py"
    cmd = [
        sys.executable,
        "-u",
        str(script),
        "--results-csv",
        str(merged_csv.resolve()),
        "--out-dir",
        str(out_dir.resolve()),
        "--label",
        str(label),
    ]
    print("[QUICKCHECK] " + " ".join(cmd))
    return int(subprocess.run(cmd, check=False).returncode)


def main() -> int:
    p = argparse.ArgumentParser(description="Merge SA chunk CSVs, run overall QA, and create report.")
    p.add_argument("--exports-dir", default=str(Path("paper") / "exports" / "sa_chunks"))
    p.add_argument("--out-csv", default="")
    p.add_argument("--out-qa-json", default="")
    p.add_argument("--out-report-md", default="")
    p.add_argument("--run-quickcheck", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--quickcheck-out-dir", default=str(Path("paper") / "exports" / "quickcheck"))
    p.add_argument("--quickcheck-label", default="SA Chunk Merged Quick-Check")
    args = p.parse_args()

    exports_dir = Path(args.exports_dir).resolve()
    if not exports_dir.exists():
        raise SystemExit(f"exports dir not found: {exports_dir}")

    chunks = _discover_chunk_csvs(exports_dir)
    if not chunks:
        raise SystemExit(f"No chunk CSVs found in {exports_dir}")

    out_csv = Path(args.out_csv).resolve() if str(args.out_csv).strip() else (exports_dir / "field_event_results_merged.csv")
    out_qa = Path(args.out_qa_json).resolve() if str(args.out_qa_json).strip() else (exports_dir / "field_event_results_merged.qa.json")
    out_report = (
        Path(args.out_report_md).resolve()
        if str(args.out_report_md).strip()
        else (exports_dir / "field_event_results_merged.report.md")
    )
    out_manifest = exports_dir / "field_event_results_merged.manifest.json"

    all_rows: list[dict[str, str]] = []
    fieldnames: list[str] | None = None
    for idx, chunk in enumerate(chunks, start=1):
        rows = _load_rows(chunk)
        if not rows:
            continue
        if fieldnames is None:
            fieldnames = list(rows[0].keys())
        else:
            if list(rows[0].keys()) != fieldnames:
                raise SystemExit(f"Column mismatch in chunk {chunk.name}")
        all_rows.extend(rows)
        print(f"[MERGE] {idx}/{len(chunks)} {chunk.name}: +{len(rows)} rows (total={len(all_rows)})")

    if not all_rows or fieldnames is None:
        raise SystemExit("No rows merged from chunk CSVs.")

    _write_rows(out_csv, all_rows, fieldnames)
    qa = validate(all_rows)
    out_qa.write_text(json.dumps(qa, ensure_ascii=False, indent=2), encoding="utf-8")

    now = dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
    report = _render_report(
        generated_at_utc=now,
        exports_dir=exports_dir,
        merged_csv=out_csv,
        qa_json=out_qa,
        chunk_files=chunks,
        rows=all_rows,
        qa=qa,
    )
    out_report.write_text(report, encoding="utf-8")

    quickcheck_code = None
    if bool(args.run_quickcheck):
        quickcheck_code = _run_quickcheck(out_csv, Path(args.quickcheck_out_dir), str(args.quickcheck_label))

    manifest = {
        "generated_at_utc": now,
        "exports_dir": str(exports_dir),
        "chunk_files_count": len(chunks),
        "rows_total": len(all_rows),
        "out_csv": str(out_csv),
        "out_qa_json": str(out_qa),
        "out_report_md": str(out_report),
        "qa_ok": bool(qa.get("ok")),
        "quickcheck_requested": bool(args.run_quickcheck),
        "quickcheck_exit_code": quickcheck_code,
    }
    out_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] merged_csv: {out_csv}")
    print(f"[OK] merged_qa: {out_qa}")
    print(f"[OK] merged_report: {out_report}")
    print(f"[OK] merged_manifest: {out_manifest}")
    return 0 if bool(qa.get("ok")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
