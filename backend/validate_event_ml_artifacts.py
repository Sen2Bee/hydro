from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from erosion_event_ml import infer_erosion_event_ml


def validate(args: argparse.Namespace) -> None:
    h = int(args.height)
    w = int(args.width)
    shape = (h, w)
    rng = np.random.default_rng(args.seed)
    out = infer_erosion_event_ml(
        acc_cells=rng.random(shape) * 250.0,
        slope_deg=rng.random(shape) * 35.0,
        soil_risk=rng.random(shape),
        impervious_risk=rng.random(shape),
        valid_mask=np.ones(shape, dtype=bool),
        weather_context={"rain_proxy": 0.7, "source": "validation_script"},
        event_start_iso="2025-06-01T00:00:00Z",
        event_end_iso="2025-06-02T00:00:00Z",
        ml_model_key=args.ml_model_key,
        ml_severity_model_key=args.ml_severity_model_key,
        ml_threshold=float(args.ml_threshold),
    )

    summary = {
        "inference_mode": out.get("meta", {}).get("assumptions", {}).get("inference_mode"),
        "artifact": out.get("meta", {}).get("artifact"),
        "severity": out.get("meta", {}).get("severity"),
        "mean_risk_score": float(np.nanmean(out.get("risk_score"))),
        "max_risk_score": float(np.nanmax(out.get("risk_score"))),
    }
    print(json.dumps(summary, indent=2))

    out_path = Path(args.output_json).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"Validation output: {out_path}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Smoke validate Event-ML artifacts with synthetic inputs.")
    p.add_argument("--ml-model-key", default="event-ml-rf-v1")
    p.add_argument("--ml-severity-model-key", default="event-ml-rf-severity-v1")
    p.add_argument("--ml-threshold", type=float, default=0.50)
    p.add_argument("--height", type=int, default=32)
    p.add_argument("--width", type=int, default=32)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--output-json",
        default=str(Path(__file__).resolve().parent / "models" / "event_ml" / "event-ml-validation.json"),
    )
    return p


if __name__ == "__main__":
    validate(build_parser().parse_args())
