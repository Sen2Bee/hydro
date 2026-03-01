from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _backend_dir() -> Path:
    return Path(__file__).resolve().parent


def _default_input_csv() -> Path:
    return (
        _repo_root()
        / "data"
        / "external"
        / "nowcastr"
        / "NowCastR-v1.0"
        / "JKI-GDM-NowCastR-ae3651e"
        / "input_maize.csv"
    )


def _default_out_dir() -> Path:
    return _backend_dir() / "models" / "event_ml"


def _run(cmd: list[str], cwd: Path) -> None:
    proc = subprocess.run(cmd, cwd=str(cwd), check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def train_bundle(args: argparse.Namespace) -> None:
    py = sys.executable
    backend = _backend_dir()
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    input_csv = Path(args.input_csv).resolve()

    det_model = out_dir / "event-ml-rf-v1.joblib"
    det_metrics = out_dir / "event-ml-rf-v1.metrics.json"
    sev_model = out_dir / "event-ml-rf-severity-v1.joblib"
    sev_metrics = out_dir / "event-ml-rf-severity-v1.metrics.json"

    _run(
        [
            py,
            "train_erosion_event_ml.py",
            "--input-csv",
            str(input_csv),
            "--output-model",
            str(det_model),
            "--output-metrics",
            str(det_metrics),
            "--random-state",
            str(args.random_state),
        ],
        cwd=backend,
    )
    _run(
        [
            py,
            "train_erosion_event_severity_ml.py",
            "--input-csv",
            str(input_csv),
            "--output-model",
            str(sev_model),
            "--output-metrics",
            str(sev_metrics),
            "--random-state",
            str(args.random_state),
        ],
        cwd=backend,
    )

    det = _load_json(det_metrics)
    sev = _load_json(sev_metrics)
    manifest = {
        "created_at_utc": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "input_csv": str(input_csv),
        "bundle": {
            "detection": {
                "model_key": "event-ml-rf-v1",
                "artifact_path": str(det_model),
                "metrics_path": str(det_metrics),
                "metrics_cv": det.get("metrics_cv"),
            },
            "severity": {
                "model_key": "event-ml-rf-severity-v1",
                "artifact_path": str(sev_model),
                "metrics_path": str(sev_metrics),
                "metrics_cv": sev.get("metrics_cv"),
            },
        },
        "runtime_parameters": {
            "analysis_type": "erosion_events_ml",
            "ml_model_key": "event-ml-rf-v1",
            "ml_severity_model_key": "event-ml-rf-severity-v1",
        },
    }
    manifest_path = out_dir / "event-ml-bundle.manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("Event-ML bundle complete")
    print(f"Manifest: {manifest_path}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Train detection + severity models and emit bundle manifest.")
    p.add_argument("--input-csv", default=str(_default_input_csv()))
    p.add_argument("--output-dir", default=str(_default_out_dir()))
    p.add_argument("--random-state", type=int, default=42)
    return p


if __name__ == "__main__":
    train_bundle(build_parser().parse_args())
