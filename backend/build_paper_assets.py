from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _hist(values: list[float], bins: int, vmin: float, vmax: float) -> list[dict[str, float | int]]:
    if bins <= 0:
        bins = 10
    width = (vmax - vmin) / float(bins)
    if width <= 0:
        width = 1.0

    counts = [0] * bins
    for v in values:
        if v < vmin:
            idx = 0
        elif v >= vmax:
            idx = bins - 1
        else:
            idx = int((v - vmin) / width)
            if idx >= bins:
                idx = bins - 1
            if idx < 0:
                idx = 0
        counts[idx] += 1

    rows: list[dict[str, float | int]] = []
    for i in range(bins):
        b0 = vmin + i * width
        b1 = vmin + (i + 1) * width
        rows.append(
            {
                "bin_idx": i,
                "bin_min": round(b0, 6),
                "bin_max": round(b1, 6),
                "count": int(counts[i]),
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def _event_year(event_id: str) -> str:
    # expected patterns: auto_YYYYMMDD_HHMMSS_xx or evt_YYYY_MM_DD
    s = str(event_id or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 4:
        return digits[:4]
    return "unknown"


def main() -> int:
    p = argparse.ArgumentParser(description="Build paper-ready summary assets (CSV/JSON) from merged run CSV.")
    p.add_argument("--input-csv", required=True, help="Merged field-event CSV path")
    p.add_argument("--out-dir", default=str(Path("paper") / "exports" / "paper_assets"), help="Output directory")
    p.add_argument("--hist-bins", type=int, default=20)
    args = p.parse_args()

    in_csv = Path(args.input_csv).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    if not in_csv.exists():
        raise SystemExit(f"[ERROR] input CSV not found: {in_csv}")

    rows_total = 0
    rows_ok = 0
    rows_err = 0
    fields = set()
    events = set()
    modes = Counter()
    years = Counter()
    event_p = []
    abag_i = []
    risk_s = []
    status_counts = Counter()

    with in_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_total += 1
            status = str(row.get("status") or "").strip().lower()
            analysis = str(row.get("analysis_type") or "").strip().lower()
            field_id = str(row.get("field_id") or "").strip()
            event_id = str(row.get("event_id") or "").strip()
            status_counts[status or "unknown"] += 1
            modes[analysis or "unknown"] += 1

            if field_id:
                fields.add(field_id)
            if event_id:
                events.add(event_id)
                years[_event_year(event_id)] += 1

            if status == "ok":
                rows_ok += 1
            elif status == "error":
                rows_err += 1

            v1 = _to_float(row.get("event_probability_max"))
            if v1 is not None:
                event_p.append(v1)
            v2 = _to_float(row.get("abag_index_mean"))
            if v2 is not None:
                abag_i.append(v2)
            v3 = _to_float(row.get("risk_score_max"))
            if v3 is not None:
                risk_s.append(v3)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_csv": str(in_csv),
        "rows_total": rows_total,
        "rows_ok": rows_ok,
        "rows_error": rows_err,
        "rows_status_counts": dict(status_counts),
        "unique_fields": len(fields),
        "unique_events": len(events),
        "analysis_mode_counts": dict(modes),
        "event_year_counts": dict(years),
        "value_counts": {
            "event_probability_max": len(event_p),
            "abag_index_mean": len(abag_i),
            "risk_score_max": len(risk_s),
        },
    }

    (out_dir / "paper_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    mode_rows = [{"analysis_type": k, "count": int(v)} for k, v in sorted(modes.items(), key=lambda x: x[0])]
    _write_csv(out_dir / "analysis_mode_counts.csv", mode_rows, ["analysis_type", "count"])

    year_rows = [{"event_year": k, "count": int(v)} for k, v in sorted(years.items(), key=lambda x: x[0])]
    _write_csv(out_dir / "event_year_counts.csv", year_rows, ["event_year", "count"])

    status_rows = [{"status": k, "count": int(v)} for k, v in sorted(status_counts.items(), key=lambda x: x[0])]
    _write_csv(out_dir / "status_counts.csv", status_rows, ["status", "count"])

    ep_hist = _hist(event_p, bins=args.hist_bins, vmin=0.0, vmax=1.0)
    _write_csv(out_dir / "hist_event_probability_max.csv", ep_hist, ["bin_idx", "bin_min", "bin_max", "count"])

    ab_hist = _hist(abag_i, bins=args.hist_bins, vmin=0.0, vmax=1.0)
    _write_csv(out_dir / "hist_abag_index_mean.csv", ab_hist, ["bin_idx", "bin_min", "bin_max", "count"])

    rk_hist = _hist(risk_s, bins=args.hist_bins, vmin=0.0, vmax=100.0)
    _write_csv(out_dir / "hist_risk_score_max.csv", rk_hist, ["bin_idx", "bin_min", "bin_max", "count"])

    print(f"[OK] summary: {out_dir / 'paper_summary.json'}")
    print(f"[OK] tables:  {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
