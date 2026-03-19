from __future__ import annotations

import argparse
import datetime as dt
import csv
import json
import math
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from pyproj import Transformer
except Exception:  # pragma: no cover
    Transformer = None  # type: ignore[assignment]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="SA-wide chunked field-event batch runner (resume-capable)."
    )
    p.add_argument(
        "--source-sqlite",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"),
        help="SQLite source with table flurstuecke(feature_json).",
    )
    p.add_argument(
        "--events-csv",
        default=str(Path("paper") / "templates" / "events_template.csv"),
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
    p.add_argument("--events-auto-cell-cache-dir", default="")
    p.add_argument("--events-auto-weather-cell-km", type=float, default=2.0)
    p.add_argument("--events-auto-request-retries", type=int, default=6)
    p.add_argument("--events-auto-retry-backoff-initial-s", type=float, default=5.0)
    p.add_argument("--events-auto-retry-backoff-max-s", type=float, default=90.0)
    p.add_argument("--events-auto-min-interval-s", type=float, default=1.5)
    p.add_argument("--events-auto-cache-only", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--events-auto-use-cached-empty", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument(
        "--chunks-dir",
        default=str(Path("paper") / "input" / "sa_chunks"),
        help="Temporary GeoJSON chunks.",
    )
    p.add_argument(
        "--use-existing-chunks",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use precomputed chunk GeoJSONs from --chunks-dir instead of rowid-based chunk export.",
    )
    p.add_argument(
        "--exports-dir",
        default=str(Path("paper") / "exports" / "sa_chunks"),
        help="Chunk CSV outputs.",
    )
    p.add_argument("--chunk-size", type=int, default=5000)
    p.add_argument("--start-chunk", type=int, default=1, help="1-based chunk index.")
    p.add_argument("--max-chunks", type=int, default=0, help="0=all remaining chunks.")
    p.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--validate-chunk", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--fail-on-qa-error", action=argparse.BooleanOptionalAction, default=True)

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
    p.add_argument("--checkpoint-every", type=int, default=50)
    p.add_argument("--continue-on-error", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument(
        "--min-field-area-ha",
        type=float,
        default=0.0,
        help="Skip fields smaller than this area in hectares (0=disabled).",
    )
    p.add_argument(
        "--max-field-area-ha",
        type=float,
        default=0.0,
        help="Skip fields larger than this area in hectares (0=disabled).",
    )
    p.add_argument(
        "--field-id-whitelist-file",
        default="",
        help="Optional TXT/CSV whitelist of allowed field IDs (schlag_id/flik/FLURSTUECKSKENNZEICHEN).",
    )
    p.add_argument(
        "--field-id-whitelist-column",
        default="",
        help="Optional column name when whitelist file is CSV.",
    )
    p.add_argument(
        "--require-whitelist",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fail fast when no --field-id-whitelist-file is provided.",
    )
    return p.parse_args()


def _iso_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _run_id() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _count_and_span(conn: sqlite3.Connection) -> tuple[int, int, int]:
    cur = conn.execute("SELECT COUNT(*), MIN(rowid), MAX(rowid) FROM flurstuecke")
    total, min_rowid, max_rowid = cur.fetchone()
    return int(total or 0), int(min_rowid or 0), int(max_rowid or 0)


def _chunk_bounds(min_rowid: int, max_rowid: int, chunk_size: int, chunk_idx_1b: int) -> tuple[int, int]:
    start = int(min_rowid + (chunk_idx_1b - 1) * chunk_size)
    end = min(int(max_rowid), start + chunk_size - 1)
    return start, end


def _fetch_chunk_features(conn: sqlite3.Connection, rowid_start: int, rowid_end: int) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT feature_json FROM flurstuecke WHERE rowid BETWEEN ? AND ? ORDER BY rowid",
        (int(rowid_start), int(rowid_end)),
    )
    out: list[dict[str, Any]] = []
    for (fj,) in cur:
        try:
            out.append(json.loads(str(fj)))
        except Exception:
            continue
    return out


def _load_geojson_features(path: Path) -> list[dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    return list(obj.get("features", []))


def _largest_ring_lonlat(feature: dict[str, Any]) -> list[list[float]] | None:
    geom = (feature or {}).get("geometry") or {}
    gtype = str(geom.get("type") or "")
    coords = geom.get("coordinates")
    if not gtype or not coords:
        return None

    if gtype == "Polygon":
        if not coords:
            return None
        return coords[0]
    if gtype == "MultiPolygon":
        best = None
        best_n = -1
        for poly in coords:
            if not poly:
                continue
            ring = poly[0]
            n = len(ring or [])
            if n > best_n:
                best_n = n
                best = ring
        return best
    return None


def _shoelace_area_m2_xy(xy: list[tuple[float, float]]) -> float:
    if len(xy) < 3:
        return 0.0
    area2 = 0.0
    for i in range(len(xy)):
        x1, y1 = xy[i]
        x2, y2 = xy[(i + 1) % len(xy)]
        area2 += (x1 * y2) - (x2 * y1)
    return abs(area2) * 0.5


def _field_area_ha(feature: dict[str, Any], tx: Any | None) -> float:
    ring = _largest_ring_lonlat(feature)
    if not ring or len(ring) < 3:
        return 0.0
    if tx is None:
        return 0.0
    xy: list[tuple[float, float]] = []
    for p in ring:
        if not isinstance(p, (list, tuple)) or len(p) < 2:
            continue
        lon = float(p[0])
        lat = float(p[1])
        x, y = tx.transform(lon, lat)
        xy.append((float(x), float(y)))
    if len(xy) < 3:
        return 0.0
    return _shoelace_area_m2_xy(xy) / 10000.0


def _field_id_from_feature(feature: dict[str, Any], fallback_idx: int) -> str:
    props = (feature or {}).get("properties") or {}
    for k in ("schlag_id", "field_id", "flik", "FLURSTUECKSKENNZEICHEN", "id", "ID", "OBJECT_ID"):
        v = props.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return f"field_{fallback_idx:06d}"


def _load_whitelist(path_raw: str, column_hint: str = "") -> set[str]:
    path = Path(path_raw).resolve()
    out: set[str] = set()
    if not path.exists():
        raise RuntimeError(f"whitelist file not found: {path}")

    if path.suffix.lower() in (".txt", ".lst"):
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            v = str(line).strip()
            if v:
                out.add(v)
        return out

    if path.suffix.lower() in (".csv", ".tsv"):
        delim = "\t" if path.suffix.lower() == ".tsv" else ","
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=delim)
            cols = [str(c) for c in (reader.fieldnames or [])]
            col = ""
            if column_hint and column_hint in cols:
                col = column_hint
            else:
                for c in ("schlag_id", "field_id", "flik", "FLURSTUECKSKENNZEICHEN", "id", "ID"):
                    if c in cols:
                        col = c
                        break
            if not col:
                raise RuntimeError(f"no usable id column in whitelist CSV: {path}")
            for row in reader:
                v = str(row.get(col) or "").strip()
                if v:
                    out.add(v)
        return out

    raise RuntimeError(f"unsupported whitelist file type: {path.suffix}")


def _apply_field_filters(
    features: list[dict[str, Any]],
    *,
    min_area_ha: float,
    max_area_ha: float,
    whitelist: set[str] | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not features:
        return features, {"input": 0, "kept": 0, "drop_whitelist": 0, "drop_area_min": 0, "drop_area_max": 0, "drop_geom": 0}

    tx = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True) if Transformer is not None else None
    kept: list[dict[str, Any]] = []
    stats = {"input": len(features), "kept": 0, "drop_whitelist": 0, "drop_area_min": 0, "drop_area_max": 0, "drop_geom": 0}

    for i, feat in enumerate(features, start=1):
        fid = _field_id_from_feature(feat, i)
        if whitelist is not None and fid not in whitelist:
            stats["drop_whitelist"] += 1
            continue

        ring = _largest_ring_lonlat(feat)
        if not ring or len(ring) < 3:
            stats["drop_geom"] += 1
            continue

        area_ha = _field_area_ha(feat, tx)
        if min_area_ha > 0.0 and area_ha < min_area_ha:
            stats["drop_area_min"] += 1
            continue
        if max_area_ha > 0.0 and area_ha > max_area_ha:
            stats["drop_area_max"] += 1
            continue
        kept.append(feat)

    stats["kept"] = len(kept)
    return kept, stats


def _write_geojson(path: Path, features: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fc = {"type": "FeatureCollection", "features": features}
    path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")


def _run_batch(args: argparse.Namespace, chunk_geojson: Path, out_csv: Path) -> None:
    script = Path(__file__).resolve().parent / "run_field_event_batch.py"
    cmd = [
        sys.executable,
        "-u",
        str(script),
        "--fields-geojson",
        str(chunk_geojson.resolve()),
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
        "--events-auto-weather-cell-km",
        str(args.events_auto_weather_cell_km),
        "--events-auto-request-retries",
        str(args.events_auto_request_retries),
        "--events-auto-retry-backoff-initial-s",
        str(args.events_auto_retry_backoff_initial_s),
        "--events-auto-retry-backoff-max-s",
        str(args.events_auto_retry_backoff_max_s),
        "--events-auto-min-interval-s",
        str(args.events_auto_min_interval_s),
        "--events-auto-cache-only" if bool(args.events_auto_cache_only) else "--no-events-auto-cache-only",
        "--events-auto-use-cached-empty" if bool(args.events_auto_use_cached_empty) else "--no-events-auto-use-cached-empty",
        "--out-csv",
        str(out_csv.resolve()),
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
    if str(args.events_auto_cell_cache_dir or "").strip():
        cmd.extend(["--events-auto-cell-cache-dir", str(Path(args.events_auto_cell_cache_dir).resolve())])
    print("[CHUNK] " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def _run_validate(out_csv: Path) -> int:
    script = Path(__file__).resolve().parent / "validate_field_event_results.py"
    qa_json = out_csv.with_suffix(".qa.json")
    cmd = [
        sys.executable,
        "-u",
        str(script),
        "--csv",
        str(out_csv.resolve()),
        "--out-json",
        str(qa_json.resolve()),
    ]
    print("[QA] " + " ".join(cmd))
    return int(subprocess.run(cmd, check=False).returncode)


def _state_path(exports_dir: Path) -> Path:
    return exports_dir / "sa_chunk_run_state.json"


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def main() -> int:
    args = _parse_args()
    run_id = _run_id()
    started_at = _iso_now()
    source = Path(args.source_sqlite).resolve()
    if not source.exists():
        raise SystemExit(f"SQLite not found: {source}")

    chunks_dir = Path(args.chunks_dir).resolve()
    exports_dir = Path(args.exports_dir).resolve()
    chunks_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    manifest = exports_dir / "runs" / f"sa_chunk_run_{run_id}.json"
    whitelist: set[str] | None = None
    if bool(args.require_whitelist) and not str(args.field_id_whitelist_file or "").strip():
        raise SystemExit(
            "require-whitelist active, but no --field-id-whitelist-file provided. "
            "Abort to avoid non-agricultural runs."
        )
    if str(args.field_id_whitelist_file or "").strip():
        whitelist = _load_whitelist(
            str(args.field_id_whitelist_file),
            column_hint=str(args.field_id_whitelist_column or "").strip(),
        )
        print(f"[FILTER] whitelist loaded: {len(whitelist)} IDs")
    run_meta: dict[str, Any] = {
        "run_id": run_id,
        "runner": "run_field_event_batch_sa_chunks.py",
        "started_at_utc": started_at,
        "status": "running",
        "cwd": str(Path.cwd()),
        "python": sys.executable,
        "pid": os.getpid(),
        "args": vars(args),
    }
    _write_state(manifest, run_meta)

    try:
        with sqlite3.connect(str(source)) as conn:
            total_rows, min_rowid, max_rowid = _count_and_span(conn)
            if total_rows <= 0:
                raise SystemExit("No rows in flurstuecke table.")
            chunk_size = max(1, int(args.chunk_size))
            if bool(args.use_existing_chunks):
                chunk_files = sorted(chunks_dir.glob("schlaege_chunk_*.geojson"))
                if not chunk_files:
                    raise SystemExit(f"No precomputed chunks found in {chunks_dir}")
                total_chunks = len(chunk_files)
            else:
                total_chunks = int(math.ceil(total_rows / float(chunk_size)))

            start_chunk = max(1, int(args.start_chunk))
            end_chunk = total_chunks if int(args.max_chunks) <= 0 else min(total_chunks, start_chunk + int(args.max_chunks) - 1)
            print(
                f"[SA-CHUNKS] total_rows={total_rows} rowid=[{min_rowid},{max_rowid}] "
                f"chunk_size={chunk_size} total_chunks={total_chunks} run={start_chunk}..{end_chunk}"
            )
            run_meta["dataset"] = {
                "source_sqlite": str(source),
                "total_rows": total_rows,
                "rowid_min": min_rowid,
                "rowid_max": max_rowid,
                "total_chunks": total_chunks,
                "run_start_chunk": start_chunk,
                "run_end_chunk": end_chunk,
                "use_existing_chunks": bool(args.use_existing_chunks),
            }
            _write_state(manifest, run_meta)

            state_file = _state_path(exports_dir)
            state: dict[str, Any] = {
                "run_id": run_id,
                "started_at_utc": started_at,
                "source_sqlite": str(source),
                "events_csv": str(Path(args.events_csv).resolve()),
                "chunk_size": chunk_size,
                "total_rows": total_rows,
                "total_chunks": total_chunks,
                "run_start_chunk": start_chunk,
                "run_end_chunk": end_chunk,
                "use_existing_chunks": bool(args.use_existing_chunks),
                "completed": [],
                "failed": [],
            }
            _write_state(state_file, state)

            for idx in range(start_chunk, end_chunk + 1):
                run_chunks_total = max(1, end_chunk - start_chunk + 1)
                run_chunks_done = idx - start_chunk + 1
                run_pct = (float(run_chunks_done) / float(run_chunks_total)) * 100.0
                chunk_geojson = chunks_dir / f"schlaege_chunk_{idx:05d}.geojson"
                out_csv = exports_dir / f"field_event_results_chunk_{idx:05d}.csv"
                done_flag = exports_dir / f"chunk_{idx:05d}.done"

                if bool(args.resume) and done_flag.exists() and out_csv.exists():
                    print(
                        f"[SA-CHUNKS] chunk {idx}/{total_chunks} "
                        f"(run {run_chunks_done}/{run_chunks_total}, {run_pct:5.1f}%): skip (done)"
                    )
                    state["completed"].append({"chunk_idx": idx, "skipped": True})
                    _write_state(state_file, state)
                    continue

                if bool(args.use_existing_chunks):
                    print(
                        f"[SA-CHUNKS] chunk {idx}/{total_chunks} "
                        f"(run {run_chunks_done}/{run_chunks_total}, {run_pct:5.1f}%): "
                        f"precomputed {chunk_geojson.name}"
                    )
                    if not chunk_geojson.exists():
                        raise RuntimeError(f"missing precomputed chunk: {chunk_geojson}")
                    features = _load_geojson_features(chunk_geojson)
                else:
                    rowid_start, rowid_end = _chunk_bounds(min_rowid, max_rowid, chunk_size, idx)
                    print(
                        f"[SA-CHUNKS] chunk {idx}/{total_chunks} "
                        f"(run {run_chunks_done}/{run_chunks_total}, {run_pct:5.1f}%): "
                        f"rowid {rowid_start}..{rowid_end}"
                    )
                    features = _fetch_chunk_features(conn, rowid_start, rowid_end)
                if not features:
                    print(f"[SA-CHUNKS] chunk {idx}: no features, mark done.")
                    done_flag.write_text("empty\n", encoding="utf-8")
                    state["completed"].append({"chunk_idx": idx, "rows": 0, "empty": True})
                    _write_state(state_file, state)
                    continue

                features, fstats = _apply_field_filters(
                    features,
                    min_area_ha=max(0.0, float(args.min_field_area_ha)),
                    max_area_ha=max(0.0, float(args.max_field_area_ha)),
                    whitelist=whitelist,
                )
                print(
                    f"[FILTER] chunk {idx}: in={fstats['input']} kept={fstats['kept']} "
                    f"drop_whitelist={fstats['drop_whitelist']} drop_area_min={fstats['drop_area_min']} "
                    f"drop_area_max={fstats['drop_area_max']} drop_geom={fstats['drop_geom']}"
                )
                if not features:
                    print(f"[SA-CHUNKS] chunk {idx}: all features filtered, mark done.")
                    done_flag.write_text("filtered\n", encoding="utf-8")
                    state["completed"].append(
                        {"chunk_idx": idx, "rows": 0, "empty": True, "filter_stats": fstats}
                    )
                    _write_state(state_file, state)
                    continue

                if not bool(args.use_existing_chunks):
                    _write_geojson(chunk_geojson, features)
                try:
                    _run_batch(args, chunk_geojson, out_csv)
                    qa_code = 0
                    if bool(args.validate_chunk):
                        qa_code = _run_validate(out_csv)
                        if qa_code != 0 and bool(args.fail_on_qa_error):
                            raise RuntimeError(f"QA failed for chunk {idx} with exit code {qa_code}")
                    done_flag.write_text("ok\n", encoding="utf-8")
                    state["completed"].append(
                        {
                            "chunk_idx": idx,
                            "rows": len(features),
                            "out_csv": str(out_csv),
                            "qa_exit_code": qa_code,
                        }
                    )
                    _write_state(state_file, state)
                except Exception as exc:
                    state["failed"].append({"chunk_idx": idx, "error": str(exc)})
                    _write_state(state_file, state)
                    raise

        run_meta["status"] = "completed"
        run_meta["state_file"] = str(_state_path(exports_dir))
        print(f"[SA-CHUNKS] done. state={_state_path(exports_dir)}")
        return 0
    except Exception as exc:
        run_meta["status"] = "failed"
        run_meta["error"] = str(exc)
        raise
    finally:
        run_meta["finished_at_utc"] = _iso_now()
        _write_state(manifest, run_meta)
        print(f"[SA-CHUNKS] Run manifest: {manifest}")


if __name__ == "__main__":
    raise SystemExit(main())
