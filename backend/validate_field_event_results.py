from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_bool(v: Any) -> bool | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def validate(rows: list[dict[str, str]]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    n = len(rows)
    if n == 0:
        errors.append("CSV enthaelt keine Datenzeilen.")
        return {
            "ok": False,
            "rows_total": 0,
            "rows_ok": 0,
            "rows_error": 0,
            "rows_nodata_only": 0,
            "error_rate_percent": 100.0,
            "errors": errors,
            "warnings": warnings,
        }

    rows_ok = 0
    rows_error = 0
    rows_nodata = 0
    rows_prob = 0
    rows_abag = 0

    for i, r in enumerate(rows, start=1):
        status = (r.get("status") or "").strip().lower()
        analysis_type = (r.get("analysis_type") or "").strip().lower()
        nodata_only = _to_bool(r.get("nodata_only"))

        if status == "ok":
            rows_ok += 1
        elif status == "error":
            rows_error += 1
        else:
            warnings.append(f"Zeile {i}: unbekannter status='{status}'.")

        if nodata_only is True:
            rows_nodata += 1

        risk_mean = _to_float(r.get("risk_score_mean"))
        risk_max = _to_float(r.get("risk_score_max"))
        if risk_mean is not None and not (0.0 <= risk_mean <= 100.0):
            errors.append(f"Zeile {i}: risk_score_mean ausserhalb [0,100]: {risk_mean}")
        if risk_max is not None and not (0.0 <= risk_max <= 100.0):
            errors.append(f"Zeile {i}: risk_score_max ausserhalb [0,100]: {risk_max}")
        if risk_mean is not None and risk_max is not None and risk_max < risk_mean:
            warnings.append(f"Zeile {i}: risk_score_max < risk_score_mean ({risk_max} < {risk_mean}).")

        valid_share = _to_float(r.get("dem_valid_cell_share"))
        nodata_share = _to_float(r.get("dem_nodata_cell_share"))
        if valid_share is not None and not (0.0 <= valid_share <= 1.0):
            errors.append(f"Zeile {i}: dem_valid_cell_share ausserhalb [0,1]: {valid_share}")
        if nodata_share is not None and not (0.0 <= nodata_share <= 1.0):
            errors.append(f"Zeile {i}: dem_nodata_cell_share ausserhalb [0,1]: {nodata_share}")
        if valid_share is not None and nodata_share is not None:
            if abs((valid_share + nodata_share) - 1.0) > 0.02:
                warnings.append(
                    f"Zeile {i}: dem_valid_cell_share + dem_nodata_cell_share != 1 ("
                    f"{valid_share + nodata_share:.3f})."
                )
            if nodata_only is True and valid_share > 0.01:
                warnings.append(f"Zeile {i}: nodata_only=true, aber valid_share={valid_share:.3f}.")

        if analysis_type == "erosion_events_ml":
            rows_prob += 1
            p_mean = _to_float(r.get("event_probability_mean"))
            p_p90 = _to_float(r.get("event_probability_p90"))
            p_max = _to_float(r.get("event_probability_max"))
            detected = _to_float(r.get("event_detected_share_percent"))
            for key, val in (
                ("event_probability_mean", p_mean),
                ("event_probability_p90", p_p90),
                ("event_probability_max", p_max),
            ):
                if val is not None and not (0.0 <= val <= 1.0):
                    errors.append(f"Zeile {i}: {key} ausserhalb [0,1]: {val}")
            if detected is not None and not (0.0 <= detected <= 100.0):
                errors.append(f"Zeile {i}: event_detected_share_percent ausserhalb [0,100]: {detected}")
            if p_mean is not None and p_p90 is not None and p_p90 < p_mean:
                warnings.append(f"Zeile {i}: event_probability_p90 < mean ({p_p90} < {p_mean}).")
            if p_p90 is not None and p_max is not None and p_max < p_p90:
                warnings.append(f"Zeile {i}: event_probability_max < p90 ({p_max} < {p_p90}).")

        if analysis_type == "abag":
            rows_abag += 1
            a_mean = _to_float(r.get("abag_index_mean"))
            a_p90 = _to_float(r.get("abag_index_p90"))
            a_max = _to_float(r.get("abag_index_max"))
            for key, val in (
                ("abag_index_mean", a_mean),
                ("abag_index_p90", a_p90),
                ("abag_index_max", a_max),
            ):
                if val is not None and val < 0.0:
                    errors.append(f"Zeile {i}: {key} ist negativ: {val}")
            if a_mean is not None and a_p90 is not None and a_p90 < a_mean:
                warnings.append(f"Zeile {i}: abag_index_p90 < mean ({a_p90} < {a_mean}).")
            if a_p90 is not None and a_max is not None and a_max < a_p90:
                warnings.append(f"Zeile {i}: abag_index_max < p90 ({a_max} < {a_p90}).")

        aoi_area = _to_float(r.get("aoi_area_km2"))
        if aoi_area is not None and aoi_area < 0.0:
            errors.append(f"Zeile {i}: aoi_area_km2 ist negativ: {aoi_area}")
        if nodata_only is False and aoi_area is not None and aoi_area == 0.0:
            warnings.append(f"Zeile {i}: nodata_only=false, aber aoi_area_km2=0.")

    error_rate = (rows_error / n) * 100.0
    nodata_rate = (rows_nodata / n) * 100.0
    if error_rate > 15.0:
        warnings.append(f"Hohe Fehlerquote: {error_rate:.1f}% (Ziel < 15%).")
    if nodata_rate > 25.0:
        warnings.append(f"Hoher NoData-Anteil: {nodata_rate:.1f}% (prüfe Feldgeometrien/DEM-Abdeckung).")

    return {
        "ok": len(errors) == 0,
        "rows_total": n,
        "rows_ok": rows_ok,
        "rows_error": rows_error,
        "rows_nodata_only": rows_nodata,
        "rows_erosion_events_ml": rows_prob,
        "rows_abag": rows_abag,
        "error_rate_percent": round(error_rate, 2),
        "nodata_rate_percent": round(nodata_rate, 2),
        "errors": errors,
        "warnings": warnings,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Validate plausibility and quality of field-event batch CSV.")
    p.add_argument("--csv", required=True, help="Path to field_event_results*.csv")
    p.add_argument("--out-json", default="", help="Optional JSON report output path.")
    args = p.parse_args()

    csv_path = Path(args.csv).resolve()
    if not csv_path.exists():
        raise SystemExit(f"CSV nicht gefunden: {csv_path}")

    report = validate(_load_rows(csv_path))
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if args.out_json:
        out_json = Path(args.out_json).resolve()
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] QA report: {out_json}")

    return 0 if bool(report.get("ok")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
