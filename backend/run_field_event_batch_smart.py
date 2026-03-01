from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import sqlite3


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Smart field-event batch: sample SA-wide fields from SQLite, then run batch export."
    )
    p.add_argument(
        "--source-sqlite",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"),
        help="SQLite source with table 'flurstuecke(schlag_id, feature_json)'.",
    )
    p.add_argument(
        "--max-fields",
        type=int,
        default=500,
        help="Maximum number of fields for one run (safe default for first execution).",
    )
    p.add_argument(
        "--sample-strategy",
        choices=["spread", "first", "random"],
        default="spread",
        help="How to pick fields from SA-wide dataset.",
    )
    p.add_argument(
        "--sample-seed",
        type=int,
        default=42,
        help="Random seed for random strategy.",
    )
    p.add_argument(
        "--sample-geojson",
        default=str(Path("paper") / "input" / "schlaege_sample.geojson"),
        help="Temporary sample GeoJSON path.",
    )
    p.add_argument(
        "--events-csv",
        default=str(Path("paper") / "templates" / "events_template.csv"),
        help="CSV with event_id,event_start_iso,event_end_iso.",
    )
    p.add_argument("--events-source", default="csv", help="csv|auto")
    p.add_argument("--events-auto-source", default="hybrid_radar")
    p.add_argument("--events-auto-start", default=None)
    p.add_argument("--events-auto-end", default=None)
    p.add_argument("--events-auto-hours", type=int, default=24 * 120)
    p.add_argument("--events-auto-days-ago", type=int, default=0)
    p.add_argument("--events-auto-top-n", type=int, default=2)
    p.add_argument("--events-auto-min-severity", type=int, default=1)
    p.add_argument("--events-auto-cache-dir", default=str(Path("paper") / "cache" / "auto_events"))
    p.add_argument(
        "--out-csv",
        default=str(Path("paper") / "exports" / "field_event_results_sample.csv"),
        help="Output CSV for smart run.",
    )
    p.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    p.add_argument("--analysis-modes", default="erosion_events_ml,abag")
    p.add_argument("--provider", default="auto")
    p.add_argument("--dem-source", default="wcs")
    p.add_argument("--threshold", type=int, default=200)
    p.add_argument("--abag-p-factor", type=float, default=1.0)
    p.add_argument("--ml-model-key", default="event-ml-rf-v1")
    p.add_argument("--ml-severity-model-key", default="event-ml-rf-severity-v1")
    p.add_argument("--ml-threshold", type=float, default=0.50)
    p.add_argument("--timeout-s", type=int, default=1200)
    p.add_argument("--request-retries", type=int, default=3)
    p.add_argument("--checkpoint-every", type=int, default=20)
    p.add_argument("--continue-on-error", action=argparse.BooleanOptionalAction, default=True)
    return p.parse_args()


def _iso_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _run_id() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def _count_fields(conn: sqlite3.Connection) -> int:
    cur = conn.execute("SELECT COUNT(*) FROM flurstuecke")
    return int(cur.fetchone()[0])


def _sample_rowids_spread(conn: sqlite3.Connection, n: int) -> list[int]:
    cur = conn.execute("SELECT MIN(rowid), MAX(rowid) FROM flurstuecke")
    min_rowid, max_rowid = cur.fetchone()
    if min_rowid is None or max_rowid is None:
        return []
    min_rowid = int(min_rowid)
    max_rowid = int(max_rowid)
    if n <= 1:
        return [min_rowid]
    span = max(1, max_rowid - min_rowid)
    rowids: list[int] = []
    for i in range(n):
        target = min_rowid + int(round((span * i) / (n - 1)))
        rowids.append(target)
    return rowids


def _fetch_feature_by_rowid_floor(conn: sqlite3.Connection, rowid_floor: int) -> dict[str, Any] | None:
    cur = conn.execute(
        "SELECT feature_json FROM flurstuecke WHERE rowid >= ? ORDER BY rowid LIMIT 1",
        (int(rowid_floor),),
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        return json.loads(str(row[0]))
    except Exception:
        return None


def _build_sample_geojson(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    src = Path(args.source_sqlite).resolve()
    if not src.exists():
        raise RuntimeError(f"SQLite source not found: {src}")
    out = Path(args.sample_geojson).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(src)) as conn:
        total = _count_fields(conn)
        if total <= 0:
            raise RuntimeError("No fields in source SQLite.")
        n = max(1, min(int(args.max_fields), total))

        features: list[dict[str, Any]] = []
        if args.sample_strategy == "first":
            cur = conn.execute(
                "SELECT feature_json FROM flurstuecke ORDER BY rowid LIMIT ?",
                (n,),
            )
            for (fj,) in cur:
                try:
                    features.append(json.loads(str(fj)))
                except Exception:
                    continue
        elif args.sample_strategy == "random":
            cur = conn.execute(
                f"SELECT feature_json FROM flurstuecke ORDER BY RANDOM() LIMIT {n}"
            )
            for (fj,) in cur:
                try:
                    features.append(json.loads(str(fj)))
                except Exception:
                    continue
        else:
            rowids = _sample_rowids_spread(conn, n)
            seen_ids: set[str] = set()
            for rid in rowids:
                f = _fetch_feature_by_rowid_floor(conn, rid)
                if not f:
                    continue
                props = f.get("properties") or {}
                sid = str(props.get("schlag_id") or props.get("OBJECT_ID") or "")
                if sid and sid in seen_ids:
                    continue
                if sid:
                    seen_ids.add(sid)
                features.append(f)

        if not features:
            raise RuntimeError("Sampling returned no features.")

        fc = {"type": "FeatureCollection", "features": features}
        out.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")

        info = {
            "source_sqlite": str(src),
            "source_total_fields": total,
            "sample_count": len(features),
            "sample_strategy": args.sample_strategy,
            "sample_geojson": str(out),
        }
        out.with_suffix(".meta.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
        return out, info


def _run_batch(args: argparse.Namespace, sample_geojson: Path) -> list[str]:
    script = Path(__file__).resolve().parent / "run_field_event_batch.py"
    cmd = [
        sys.executable,
        "-u",
        str(script),
        "--fields-geojson",
        str(sample_geojson),
        "--events-source",
        args.events_source,
        "--events-csv",
        str(Path(args.events_csv).resolve()),
        "--events-auto-source",
        args.events_auto_source,
        "--events-auto-hours",
        str(args.events_auto_hours),
        "--events-auto-days-ago",
        str(args.events_auto_days_ago),
        "--events-auto-top-n",
        str(args.events_auto_top_n),
        "--events-auto-min-severity",
        str(args.events_auto_min_severity),
        "--events-auto-cache-dir",
        str(Path(args.events_auto_cache_dir).resolve()),
        "--out-csv",
        str(Path(args.out_csv).resolve()),
        "--api-base-url",
        args.api_base_url,
        "--analysis-modes",
        args.analysis_modes,
        "--provider",
        args.provider,
        "--dem-source",
        args.dem_source,
        "--threshold",
        str(args.threshold),
        "--abag-p-factor",
        str(args.abag_p_factor),
        "--ml-model-key",
        args.ml_model_key,
        "--ml-severity-model-key",
        args.ml_severity_model_key,
        "--ml-threshold",
        str(args.ml_threshold),
        "--timeout-s",
        str(args.timeout_s),
        "--request-retries",
        str(args.request_retries),
        "--checkpoint-every",
        str(args.checkpoint_every),
        "--continue-on-error" if bool(args.continue_on_error) else "--no-continue-on-error",
    ]
    if args.events_auto_start:
        cmd.extend(["--events-auto-start", str(args.events_auto_start)])
    if args.events_auto_end:
        cmd.extend(["--events-auto-end", str(args.events_auto_end)])
    print("[SMART] Running batch with sampled fields...")
    print("[SMART] " + " ".join(cmd))
    subprocess.run(cmd, check=True)
    return cmd


def main() -> int:
    args = _parse_args()
    started_at = _iso_now()
    rid = _run_id()
    out_csv = Path(args.out_csv).resolve()
    manifest = out_csv.parent / "runs" / f"smart_run_{rid}.json"
    run_meta: dict[str, Any] = {
        "run_id": rid,
        "runner": "run_field_event_batch_smart.py",
        "started_at_utc": started_at,
        "status": "running",
        "cwd": str(Path.cwd()),
        "python": sys.executable,
        "pid": os.getpid(),
        "args": vars(args),
        "outputs": {
            "out_csv": str(out_csv),
            "out_meta": str(out_csv.with_suffix(".meta.json")),
        },
    }
    _write_json(manifest, run_meta)

    try:
        sample_geojson, info = _build_sample_geojson(args)
        print(
            f"[SMART] Sample ready: {info['sample_count']} of {info['source_total_fields']} fields "
            f"({info['sample_strategy']})"
        )
        cmd = _run_batch(args, sample_geojson)
        run_meta["status"] = "completed"
        run_meta["sample"] = info
        run_meta["batch_command"] = cmd
        run_meta["outputs"]["sample_geojson"] = str(sample_geojson)
        run_meta["outputs"]["sample_meta"] = str(sample_geojson.with_suffix(".meta.json"))
        print("[SMART] Done.")
        return 0
    except Exception as exc:
        run_meta["status"] = "failed"
        run_meta["error"] = str(exc)
        raise
    finally:
        run_meta["finished_at_utc"] = _iso_now()
        _write_json(manifest, run_meta)
        print(f"[SMART] Run manifest: {manifest}")


if __name__ == "__main__":
    raise SystemExit(main())
