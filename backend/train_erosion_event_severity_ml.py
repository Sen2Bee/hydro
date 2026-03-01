from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict


FEATURE_ORDER = [
    "RadolanGT10mm",
    "RadolanSum",
    "RadolanMax",
    "K_factor",
    "L_factor",
    "S_factor",
    "NDVI",
    "Phase",
]


def _default_input() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "data"
        / "external"
        / "nowcastr"
        / "NowCastR-v1.0"
        / "JKI-GDM-NowCastR-ae3651e"
        / "input_maize.csv"
    )


def _default_model_out() -> Path:
    return Path(__file__).resolve().parent / "models" / "event_ml" / "event-ml-rf-severity-v1.joblib"


def _default_metrics_out() -> Path:
    return Path(__file__).resolve().parent / "models" / "event_ml" / "event-ml-rf-severity-v1.metrics.json"


def _parse_target(row: dict[str, str]) -> int | None:
    raw = str(row.get("Erosion_class", "")).strip()
    if not raw:
        return None
    try:
        v = int(round(float(raw)))
    except Exception:
        return None
    if v < 1 or v > 3:
        return None
    return v


def _load_dataset(path: Path) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    rows = []
    skipped = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            y = _parse_target(row)
            if y is None:
                skipped += 1
                continue
            vals = []
            bad = False
            for feat in FEATURE_ORDER:
                raw = row.get(feat, "")
                try:
                    vals.append(float(raw))
                except Exception:
                    bad = True
                    break
            if bad:
                skipped += 1
                continue
            rows.append((vals, y))

    if not rows:
        raise RuntimeError(f"No valid rows parsed from {path}")

    X = np.asarray([r[0] for r in rows], dtype=float)
    y = np.asarray([r[1] for r in rows], dtype=int)
    meta = {
        "rows_total_valid": int(len(rows)),
        "rows_skipped": int(skipped),
        "class_counts": {str(k): int(np.sum(y == k)) for k in sorted(set(y.tolist()))},
    }
    return X, y, meta


def _build_model(args: argparse.Namespace) -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=int(args.n_estimators),
        random_state=int(args.random_state),
        n_jobs=-1,
        class_weight="balanced_subsample",
        min_samples_leaf=int(args.min_samples_leaf),
        max_depth=(int(args.max_depth) if args.max_depth and int(args.max_depth) > 0 else None),
    )


def _evaluate_cv(X: np.ndarray, y: np.ndarray, args: argparse.Namespace) -> dict[str, Any]:
    labels = sorted(set(y.tolist()))
    cv = StratifiedKFold(n_splits=int(args.cv_folds), shuffle=True, random_state=int(args.random_state))
    model = _build_model(args)
    y_hat = cross_val_predict(model, X, y, cv=cv, method="predict")
    cm = confusion_matrix(y, y_hat, labels=labels)
    return {
        "accuracy": float(accuracy_score(y, y_hat)),
        "precision_macro": float(precision_score(y, y_hat, average="macro", zero_division=0)),
        "recall_macro": float(recall_score(y, y_hat, average="macro", zero_division=0)),
        "f1_macro": float(f1_score(y, y_hat, average="macro", zero_division=0)),
        "labels": labels,
        "confusion_matrix": cm.tolist(),
    }


def _feature_importance(model: RandomForestClassifier) -> list[dict[str, Any]]:
    vals = list(getattr(model, "feature_importances_", np.zeros(len(FEATURE_ORDER))))
    ranked = sorted(zip(FEATURE_ORDER, vals), key=lambda t: t[1], reverse=True)
    return [{"feature": f, "importance": float(v)} for f, v in ranked]


def train(args: argparse.Namespace) -> None:
    in_path = Path(args.input_csv).resolve()
    out_model = Path(args.output_model).resolve()
    out_metrics = Path(args.output_metrics).resolve()
    out_model.parent.mkdir(parents=True, exist_ok=True)
    out_metrics.parent.mkdir(parents=True, exist_ok=True)

    X, y, parse_meta = _load_dataset(in_path)
    metrics = _evaluate_cv(X, y, args)

    model = _build_model(args)
    model.fit(X, y)
    joblib.dump(model, out_model)

    report = {
        "training": {
            "input_csv": str(in_path),
            "rows": parse_meta,
            "hyperparams": {
                "n_estimators": int(args.n_estimators),
                "max_depth": (int(args.max_depth) if args.max_depth and int(args.max_depth) > 0 else None),
                "min_samples_leaf": int(args.min_samples_leaf),
                "random_state": int(args.random_state),
                "cv_folds": int(args.cv_folds),
            },
        },
        "metrics_cv": metrics,
        "feature_importance": _feature_importance(model),
        "artifact": {
            "artifact_path": str(out_model),
            "artifact_type": "joblib",
            "model_key_suggestion": out_model.stem,
            "feature_order": FEATURE_ORDER,
            "target": "Erosion_class",
            "classes": sorted(set(y.tolist())),
        },
    }

    with out_metrics.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("Training complete (severity)")
    print(f"Model:   {out_model}")
    print(f"Metrics: {out_metrics}")
    print(f"Accuracy: {metrics['accuracy']:.3f}")
    print(f"F1 macro: {metrics['f1_macro']:.3f}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Train severity model (Erosion_class 1..3).")
    p.add_argument("--input-csv", default=str(_default_input()), help="CSV with features + Erosion_class.")
    p.add_argument("--output-model", default=str(_default_model_out()), help="Output .joblib model path.")
    p.add_argument("--output-metrics", default=str(_default_metrics_out()), help="Output .json metrics path.")
    p.add_argument("--n-estimators", type=int, default=500)
    p.add_argument("--max-depth", type=int, default=0, help="0 means None.")
    p.add_argument("--min-samples-leaf", type=int, default=2)
    p.add_argument("--cv-folds", type=int, default=4)
    p.add_argument("--random-state", type=int, default=42)
    return p


if __name__ == "__main__":
    parser = build_parser()
    train(parser.parse_args())
