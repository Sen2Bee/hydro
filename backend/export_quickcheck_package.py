from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import os
import shutil
import subprocess
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


def _clip01(v: float | None) -> float:
    if v is None:
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return float(v)


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _field_id_from_props(props: dict[str, Any], fallback_idx: int) -> str:
    for key in ("field_id", "schlag_id", "flik", "id", "ID"):
        val = props.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return f"field_{fallback_idx:05d}"


def _load_field_geometry_map(fields_geojson: Path | None) -> dict[str, dict[str, Any]]:
    if fields_geojson is None:
        return {}
    if not fields_geojson.exists():
        return {}
    obj = json.loads(fields_geojson.read_text(encoding="utf-8"))
    out: dict[str, dict[str, Any]] = {}
    feats = obj.get("features") or []
    for idx, feat in enumerate(feats, start=1):
        props = feat.get("properties") or {}
        fid = _field_id_from_props(props, idx)
        geom = feat.get("geometry")
        if isinstance(geom, dict) and geom.get("type") and geom.get("coordinates"):
            out[fid] = geom
    return out


def _quantize_priority(score: float) -> str:
    if score >= 70.0:
        return "hoch"
    if score >= 45.0:
        return "mittel"
    return "beobachten"


def _recommendation(abag: float, pmax: float, risk_max: float) -> str:
    if pmax >= 0.60 and abag >= 0.40:
        return (
            "Kurzfristig Notabflusswege sichern; mittelfristig Erosionsschutzstreifen "
            "und quer zur Hangrichtung orientierte Bewirtschaftung pruefen."
        )
    if abag >= 0.50:
        return (
            "Topographisch-bedingtes Basisrisiko: dauerhafte Bodenbedeckung und "
            "strukturverbessernde Massnahmen priorisieren."
        )
    if pmax >= 0.60 or risk_max >= 70.0:
        return (
            "Ereignisgetriebenes Risiko: Starkregen-Alarmierung, Abflussbremsen und "
            "kritische Abflusslinien lokal absichern."
        )
    return "Regelmonitoring fortfuehren, Feldkontrollen nach Starkregen priorisieren."


def _aggregate(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_field: dict[str, dict[str, Any]] = {}
    for r in rows:
        fid = str(r.get("field_id") or "").strip()
        if not fid:
            continue
        status = str(r.get("status") or "").strip().lower()
        analysis_type = str(r.get("analysis_type") or "").strip().lower()
        node = by_field.setdefault(
            fid,
            {
                "field_id": fid,
                "rows_total": 0,
                "rows_ok": 0,
                "rows_error": 0,
                "events": set(),
                "abag_vals": [],
                "event_prob_vals": [],
                "risk_max_vals": [],
                "aoi_area_vals": [],
            },
        )
        node["rows_total"] += 1
        if status == "ok":
            node["rows_ok"] += 1
        elif status == "error":
            node["rows_error"] += 1

        ev = str(r.get("event_id") or "").strip()
        if ev:
            node["events"].add(ev)

        risk_max = _to_float(r.get("risk_score_max"))
        if risk_max is not None:
            node["risk_max_vals"].append(risk_max)

        aoi = _to_float(r.get("aoi_area_km2"))
        if aoi is not None:
            node["aoi_area_vals"].append(aoi)

        if status != "ok":
            continue
        if analysis_type == "abag":
            v = _to_float(r.get("abag_index_mean"))
            if v is not None:
                node["abag_vals"].append(v)
        if analysis_type == "erosion_events_ml":
            v = _to_float(r.get("event_probability_max"))
            if v is not None:
                node["event_prob_vals"].append(v)

    out: list[dict[str, Any]] = []
    for fid, node in by_field.items():
        abag_mean = (
            sum(node["abag_vals"]) / len(node["abag_vals"]) if node["abag_vals"] else None
        )
        event_pmax = max(node["event_prob_vals"]) if node["event_prob_vals"] else None
        risk_max = max(node["risk_max_vals"]) if node["risk_max_vals"] else None
        aoi_area = (
            sum(node["aoi_area_vals"]) / len(node["aoi_area_vals"]) if node["aoi_area_vals"] else None
        )

        score = 100.0 * (
            0.45 * _clip01(event_pmax)
            + 0.35 * _clip01(abag_mean)
            + 0.20 * _clip01((risk_max or 0.0) / 100.0)
        )
        out.append(
            {
                "field_id": fid,
                "score": round(score, 2),
                "priority": _quantize_priority(score),
                "rows_total": int(node["rows_total"]),
                "rows_ok": int(node["rows_ok"]),
                "rows_error": int(node["rows_error"]),
                "events_count": int(len(node["events"])),
                "abag_index_mean": round(float(abag_mean), 4) if abag_mean is not None else "",
                "event_probability_max": round(float(event_pmax), 4) if event_pmax is not None else "",
                "risk_score_max": round(float(risk_max), 2) if risk_max is not None else "",
                "aoi_area_km2": round(float(aoi_area), 6) if aoi_area is not None else "",
                "measure_recommendation": _recommendation(
                    float(abag_mean or 0.0),
                    float(event_pmax or 0.0),
                    float(risk_max or 0.0),
                ),
            }
        )
    out.sort(key=lambda x: float(x["score"]), reverse=True)
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _write_geojson(path: Path, rows: list[dict[str, Any]], geom_map: dict[str, dict[str, Any]]) -> int:
    features: list[dict[str, Any]] = []
    for r in rows:
        fid = str(r.get("field_id") or "")
        geom = geom_map.get(fid)
        if not geom:
            continue
        props = dict(r)
        features.append({"type": "Feature", "geometry": geom, "properties": props})
    fc = {"type": "FeatureCollection", "features": features}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
    return len(features)


def _render_report_markdown(
    *,
    label: str,
    csv_path: Path,
    generated_at: str,
    rows_total: int,
    fields_total: int,
    top_rows: list[dict[str, Any]],
) -> str:
    lines = [
        f"# Quick-Check Bericht: {label}",
        "",
        f"- Erstellt: `{generated_at}`",
        f"- Eingabe: `{csv_path}`",
        f"- Eingabezeilen: `{rows_total}`",
        f"- Bewertete Schlaege: `{fields_total}`",
        "",
        "## Top-10 Massnahmenraeume",
        "",
        "| Rang | field_id | score | Prioritaet | event_probability_max | abag_index_mean | risk_score_max |",
        "|---:|---|---:|---|---:|---:|---:|",
    ]
    for idx, r in enumerate(top_rows, start=1):
        lines.append(
            f"| {idx} | {r.get('field_id','')} | {r.get('score','')} | {r.get('priority','')} | "
            f"{r.get('event_probability_max','')} | {r.get('abag_index_mean','')} | {r.get('risk_score_max','')} |"
        )
    lines += [
        "",
        "## Massnahmenempfehlungen (Kurzfassung)",
        "",
    ]
    for idx, r in enumerate(top_rows, start=1):
        lines.append(f"{idx}. `{r.get('field_id','')}`: {r.get('measure_recommendation','')}")
    lines += [
        "",
        "## Methodik (Kurz)",
        "",
        "- Score = gewichtete Kombination aus Event-ML (`event_probability_max`), ABAG (`abag_index_mean`) und `risk_score_max`.",
        "- Prioritaet: `hoch` (>=70), `mittel` (>=45), sonst `beobachten`.",
        "- Ergebnis ist eine operative Priorisierung fuer Vorsorgeplanung, kein Ersatz fuer standortbezogene Detailgutachten.",
    ]
    return "\n".join(lines) + "\n"


def _markdown_to_simple_html(md_text: str, title: str) -> str:
    body = html.escape(md_text)
    body = body.replace("\r\n", "\n")
    body = body.replace("\n", "<br>\n")
    return (
        "<!doctype html>\n"
        "<html><head><meta charset='utf-8'>"
        f"<title>{html.escape(title)}</title>"
        "<style>body{font-family:Segoe UI,Arial,sans-serif;max-width:1000px;margin:24px auto;line-height:1.4}"
        "code{background:#f1f3f5;padding:1px 4px;border-radius:4px}"
        "h1,h2{margin-top:1.2em}</style></head><body>"
        f"{body}</body></html>"
    )


def _try_export_pdf(html_path: Path, pdf_path: Path) -> tuple[bool, str]:
    candidates: list[list[str]] = []
    wk = shutil.which("wkhtmltopdf")
    if wk:
        candidates.append([wk, str(html_path), str(pdf_path)])
    edge = shutil.which("msedge")
    if edge:
        candidates.append(
            [
                edge,
                "--headless",
                "--disable-gpu",
                f"--print-to-pdf={pdf_path}",
                html_path.resolve().as_uri(),
            ]
        )
    chrome = shutil.which("chrome")
    if chrome:
        candidates.append(
            [
                chrome,
                "--headless",
                "--disable-gpu",
                f"--print-to-pdf={pdf_path}",
                html_path.resolve().as_uri(),
            ]
        )
    for cmd in candidates:
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=120)
            if proc.returncode == 0 and pdf_path.exists():
                return True, f"ok via: {' '.join(cmd[:1])}"
        except Exception:
            continue
    return False, "Kein PDF-Renderer gefunden (wkhtmltopdf/msedge/chrome)."


def main() -> int:
    p = argparse.ArgumentParser(description="Build quick-check package: Top10, maps, measures, report.")
    p.add_argument("--results-csv", required=True, help="Input field_event_results*.csv")
    p.add_argument("--fields-geojson", default="", help="Optional field polygons for map layers.")
    p.add_argument("--out-dir", default=str(Path("paper") / "exports" / "quickcheck"))
    p.add_argument("--label", default="SA Quick-Check")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--export-pdf", action=argparse.BooleanOptionalAction, default=False)
    args = p.parse_args()

    csv_path = Path(args.results_csv).resolve()
    if not csv_path.exists():
        raise SystemExit(f"CSV nicht gefunden: {csv_path}")
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    fields_geojson = Path(args.fields_geojson).resolve() if str(args.fields_geojson).strip() else None
    rows = _load_csv_rows(csv_path)
    all_scores = _aggregate(rows)
    top_n = max(1, int(args.top_n))
    top_rows = all_scores[:top_n]
    geom_map = _load_field_geometry_map(fields_geojson)

    generated_at = dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
    base = out_dir / f"quickcheck_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    all_csv = base.with_name(base.name + "_field_scores.csv")
    top_csv = base.with_name(base.name + "_top10.csv")
    measures_csv = base.with_name(base.name + "_top10_measures.csv")
    geojson_path = base.with_name(base.name + "_top10.geojson")
    report_md = base.with_name(base.name + "_report.md")
    report_html = base.with_name(base.name + "_report.html")
    report_pdf = base.with_name(base.name + "_report.pdf")
    manifest = base.with_name(base.name + "_manifest.json")

    _write_csv(
        all_csv,
        all_scores,
        [
            "field_id",
            "score",
            "priority",
            "rows_total",
            "rows_ok",
            "rows_error",
            "events_count",
            "event_probability_max",
            "abag_index_mean",
            "risk_score_max",
            "aoi_area_km2",
            "measure_recommendation",
        ],
    )
    _write_csv(
        top_csv,
        top_rows,
        [
            "field_id",
            "score",
            "priority",
            "event_probability_max",
            "abag_index_mean",
            "risk_score_max",
            "aoi_area_km2",
            "rows_ok",
            "rows_error",
            "events_count",
        ],
    )
    _write_csv(
        measures_csv,
        top_rows,
        [
            "field_id",
            "priority",
            "score",
            "measure_recommendation",
            "event_probability_max",
            "abag_index_mean",
            "risk_score_max",
        ],
    )
    mapped_count = _write_geojson(geojson_path, top_rows, geom_map)

    md_text = _render_report_markdown(
        label=str(args.label),
        csv_path=csv_path,
        generated_at=generated_at,
        rows_total=len(rows),
        fields_total=len(all_scores),
        top_rows=top_rows,
    )
    report_md.write_text(md_text, encoding="utf-8")
    report_html.write_text(_markdown_to_simple_html(md_text, str(args.label)), encoding="utf-8")

    pdf_ok = False
    pdf_msg = "nicht angefordert"
    if bool(args.export_pdf):
        pdf_ok, pdf_msg = _try_export_pdf(report_html, report_pdf)

    meta = {
        "generated_at_utc": generated_at,
        "label": args.label,
        "input_csv": str(csv_path),
        "input_rows": len(rows),
        "fields_geojson": str(fields_geojson) if fields_geojson else None,
        "fields_total_scored": len(all_scores),
        "top_n": top_n,
        "top_geojson_features": mapped_count,
        "pdf_export_requested": bool(args.export_pdf),
        "pdf_export_ok": bool(pdf_ok),
        "pdf_export_info": pdf_msg,
        "outputs": {
            "all_field_scores_csv": str(all_csv),
            "top10_csv": str(top_csv),
            "top10_measures_csv": str(measures_csv),
            "top10_geojson": str(geojson_path),
            "report_md": str(report_md),
            "report_html": str(report_html),
            "report_pdf": str(report_pdf) if pdf_ok else None,
        },
    }
    manifest.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] field_scores: {all_csv}")
    print(f"[OK] top10: {top_csv}")
    print(f"[OK] measures: {measures_csv}")
    print(f"[OK] map_geojson: {geojson_path} (features={mapped_count})")
    print(f"[OK] report_md: {report_md}")
    print(f"[OK] report_html: {report_html}")
    if bool(args.export_pdf):
        if pdf_ok:
            print(f"[OK] report_pdf: {report_pdf}")
        else:
            print(f"[WARN] report_pdf: {pdf_msg}")
    print(f"[OK] manifest: {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
