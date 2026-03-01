from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
from typing import Any

import numpy as np

FEATURE_CONTRACT = [
    "RadolanGT10mm",
    "RadolanSum",
    "RadolanMax",
    "K_factor",
    "L_factor",
    "S_factor",
    "NDVI",
    "Phase",
]


def _parse_iso(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -40.0, 40.0)))


def _normalize(arr: np.ndarray) -> np.ndarray:
    out = np.zeros(arr.shape, dtype=float)
    m = np.isfinite(arr)
    if not np.any(m):
        return out
    vmin = float(np.nanmin(arr[m]))
    vmax = float(np.nanmax(arr[m]))
    if math.isclose(vmin, vmax):
        out[m] = 0.0
        return out
    out[m] = (arr[m] - vmin) / (vmax - vmin)
    return np.clip(out, 0.0, 1.0)


def _sanitize_key(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", str(key or "")).strip("_").upper()


def _resolve_model_artifact_path(ml_model_key: str | None) -> str | None:
    key = str(ml_model_key or "").strip()
    if key and os.path.exists(key):
        return key

    if key:
        env_key = f"EROSION_EVENT_ML_ARTIFACT_{_sanitize_key(key)}"
        p = os.getenv(env_key)
        if p and os.path.exists(p):
            return p

    p_default = os.getenv("EROSION_EVENT_ML_ARTIFACT")
    if p_default and os.path.exists(p_default):
        return p_default

    if key:
        base = os.path.join(os.path.dirname(__file__), "models", "event_ml", key)
        for cand in (f"{base}.joblib", f"{base}.pkl", f"{base}.pickle", f"{base}.json"):
            if os.path.exists(cand):
                return cand
    return None


def _build_feature_stack(
    *,
    acc_cells: np.ndarray,
    slope_deg: np.ndarray,
    soil_risk: np.ndarray,
    impervious_risk: np.ndarray,
    weather_context: dict[str, Any] | None,
    event_start_iso: str | None,
) -> dict[str, np.ndarray]:
    rain_proxy = 0.60
    if isinstance(weather_context, dict):
        try:
            rp = float(weather_context.get("rain_proxy"))
            if math.isfinite(rp):
                rain_proxy = float(np.clip(rp, 0.05, 1.0))
        except Exception:
            pass

    # Rain surrogates (replace with real RADOLAN/event features in next phase).
    radolan_max = np.full(acc_cells.shape, 5.0 + 55.0 * rain_proxy, dtype=float)
    radolan_sum = np.full(acc_cells.shape, 20.0 + 380.0 * rain_proxy, dtype=float)
    radolan_gt10 = np.full(acc_cells.shape, max(0.0, min(1.0, (radolan_max[0, 0] - 10.0) / 35.0)), dtype=float)

    slope = np.clip(np.nan_to_num(slope_deg, nan=0.0), 0.0, 65.0)
    slope_rad = np.radians(slope)
    s_factor = np.clip((np.sin(slope_rad) / 0.0896) ** 1.3, 0.0, 8.0)
    flow_length_proxy = np.sqrt(np.maximum(np.nan_to_num(acc_cells, nan=0.0), 1.0))
    l_factor = np.clip((flow_length_proxy / 22.13) ** 0.4, 0.0, 8.0)
    k_factor = 0.018 + 0.045 * np.clip(np.nan_to_num(soil_risk, nan=0.5), 0.0, 1.0)
    ndvi = np.clip(1.0 - np.nan_to_num(impervious_risk, nan=0.35), 0.0, 1.0)

    ts0 = _parse_iso(event_start_iso)
    month = (ts0.month if ts0 else dt.datetime.utcnow().month)
    phase = np.full(acc_cells.shape, float(month), dtype=float)

    return {
        "RadolanGT10mm": radolan_gt10,
        "RadolanSum": radolan_sum,
        "RadolanMax": radolan_max,
        "K_factor": k_factor,
        "L_factor": l_factor,
        "S_factor": s_factor,
        "NDVI": ndvi,
        "Phase": phase,
    }


def _predict_placeholder(features: dict[str, np.ndarray]) -> np.ndarray:
    radolan_gt10 = features["RadolanGT10mm"]
    radolan_sum = features["RadolanSum"]
    radolan_max = features["RadolanMax"]
    k_factor = features["K_factor"]
    l_factor = features["L_factor"]
    s_factor = features["S_factor"]
    ndvi = features["NDVI"]
    phase = features["Phase"]
    phase_norm = (phase - 1.0) / 11.0

    x = (
        -2.20
        + 1.45 * radolan_gt10
        + 0.60 * _normalize(radolan_sum)
        + 0.75 * _normalize(radolan_max)
        + 0.80 * _normalize(k_factor)
        + 0.70 * _normalize(l_factor)
        + 0.70 * _normalize(s_factor)
        - 0.50 * ndvi
        + 0.25 * phase_norm
    )
    return _sigmoid(x)


def _predict_linear_json(features: dict[str, np.ndarray], artifact: dict[str, Any]) -> np.ndarray:
    order = artifact.get("feature_order") or FEATURE_CONTRACT
    intercept = float(artifact.get("intercept", 0.0))
    weights = artifact.get("weights", {})
    z = np.full(next(iter(features.values())).shape, intercept, dtype=float)
    for i, name in enumerate(order):
        arr = features.get(name)
        if arr is None:
            continue
        if isinstance(weights, dict):
            w = float(weights.get(name, 0.0))
        elif isinstance(weights, list):
            w = float(weights[i]) if i < len(weights) else 0.0
        else:
            w = 0.0
        z = z + w * np.nan_to_num(arr, nan=0.0)
    return _sigmoid(z)


def _predict_joblib(features: dict[str, np.ndarray], valid_mask: np.ndarray, artifact_path: str) -> np.ndarray:
    try:
        import joblib  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"joblib artifact requires joblib package: {exc}")

    model = joblib.load(artifact_path)
    order = getattr(model, "feature_names_in_", None)
    if order is None:
        order = FEATURE_CONTRACT
    order = [str(x) for x in list(order)]

    h, w = valid_mask.shape
    out = np.full((h, w), np.nan, dtype=float)
    idx = np.argwhere(valid_mask)
    if idx.size == 0:
        return out

    # Build matrix by chunks to avoid large memory spikes on big AOIs.
    batch = 80_000
    for s in range(0, len(idx), batch):
        part = idx[s : s + batch]
        X = np.zeros((len(part), len(order)), dtype=float)
        for j, fname in enumerate(order):
            arr = features.get(fname)
            if arr is None:
                continue
            vals = arr[part[:, 0], part[:, 1]]
            X[:, j] = np.nan_to_num(vals, nan=0.0)

        if hasattr(model, "predict_proba"):
            P = model.predict_proba(X)
            pos_col = 1 if P.shape[1] > 1 else 0
            # Try to identify positive class robustly.
            classes = [str(c).lower() for c in getattr(model, "classes_", [])]
            if classes:
                for k, c in enumerate(classes):
                    if "erod" in c or c in ("1", "true", "yes", "y"):
                        pos_col = k
                        break
            p = np.asarray(P[:, pos_col], dtype=float)
        elif hasattr(model, "decision_function"):
            z = np.asarray(model.decision_function(X), dtype=float)
            p = _sigmoid(z)
        else:
            y = np.asarray(model.predict(X), dtype=float)
            p = np.clip(y, 0.0, 1.0)

        out[part[:, 0], part[:, 1]] = p
    return out


def _predict_with_artifact(
    *,
    features: dict[str, np.ndarray],
    valid_mask: np.ndarray,
    artifact_path: str,
) -> tuple[np.ndarray, dict[str, Any]]:
    low = artifact_path.lower()
    if low.endswith(".json"):
        with open(artifact_path, "r", encoding="utf-8") as f:
            artifact = json.load(f)
        atype = str(artifact.get("type") or "linear_logits").strip().lower()
        if atype != "linear_logits":
            raise RuntimeError(f"unsupported JSON model type '{atype}' in {artifact_path}")
        prob = _predict_linear_json(features, artifact)
        return prob, {
            "inference_mode": "linear_json_artifact",
            "artifact_type": atype,
            "artifact_path": artifact_path,
            "artifact_version": artifact.get("version"),
            "feature_contract": artifact.get("feature_order") or FEATURE_CONTRACT,
        }

    if low.endswith(".joblib") or low.endswith(".pkl") or low.endswith(".pickle"):
        prob = _predict_joblib(features, valid_mask, artifact_path)
        return prob, {
            "inference_mode": "joblib_artifact",
            "artifact_type": "joblib",
            "artifact_path": artifact_path,
            "artifact_version": None,
            "feature_contract": FEATURE_CONTRACT,
        }

    raise RuntimeError(f"unsupported artifact extension for '{artifact_path}'")


def infer_erosion_event_ml(
    *,
    acc_cells: np.ndarray,
    slope_deg: np.ndarray,
    soil_risk: np.ndarray,
    impervious_risk: np.ndarray,
    valid_mask: np.ndarray,
    weather_context: dict[str, Any] | None = None,
    event_start_iso: str | None = None,
    event_end_iso: str | None = None,
    ml_model_key: str = "event-ml-rf-v1-placeholder",
    ml_severity_model_key: str | None = None,
    ml_threshold: float = 0.50,
) -> dict[str, Any]:
    """
    Placeholder inference for event-based erosion detection.

    Feature contract mirrors the NowCastR script conceptually:
    - rain proxies: RadolanGT10mm, RadolanSum, RadolanMax
    - topo/soil: K_factor, L_factor, S_factor
    - cover: NDVI proxy
    - temporal: Phase (month)
    """
    features = _build_feature_stack(
        acc_cells=acc_cells,
        slope_deg=slope_deg,
        soil_risk=soil_risk,
        impervious_risk=impervious_risk,
        weather_context=weather_context,
        event_start_iso=event_start_iso,
    )

    artifact_path = _resolve_model_artifact_path(ml_model_key)
    mode_meta: dict[str, Any] = {}
    if artifact_path:
        try:
            prob, mode_meta = _predict_with_artifact(
                features=features,
                valid_mask=valid_mask,
                artifact_path=artifact_path,
            )
        except Exception as exc:
            prob = _predict_placeholder(features)
            mode_meta = {
                "inference_mode": "placeholder_after_artifact_error",
                "artifact_path": artifact_path,
                "artifact_error": str(exc),
                "feature_contract": FEATURE_CONTRACT,
            }
    else:
        prob = _predict_placeholder(features)
        mode_meta = {
            "inference_mode": "placeholder_heuristic",
            "artifact_path": None,
            "feature_contract": FEATURE_CONTRACT,
        }

    prob = np.where(valid_mask, prob, np.nan)
    risk_score = np.where(valid_mask, np.round(np.clip(prob, 0.0, 1.0) * 100.0), np.nan)
    event_detected = np.where(valid_mask, (prob >= float(np.clip(ml_threshold, 0.05, 0.95))), False)

    # Severity: artifact-based multiclass prediction if available, else probability bins.
    severity_mode = "probability_bins_fallback"
    severity_artifact_path = _resolve_model_artifact_path(ml_severity_model_key)
    severity_error = None
    if severity_artifact_path and severity_artifact_path.lower().endswith((".joblib", ".pkl", ".pickle")):
        try:
            import joblib  # type: ignore

            model_s = joblib.load(severity_artifact_path)
            order = getattr(model_s, "feature_names_in_", None)
            if order is None:
                order = FEATURE_CONTRACT
            order = [str(x) for x in list(order)]

            h, w = valid_mask.shape
            severity = np.zeros((h, w), dtype=int)
            idx = np.argwhere(valid_mask)
            batch = 80_000
            for s in range(0, len(idx), batch):
                part = idx[s : s + batch]
                X = np.zeros((len(part), len(order)), dtype=float)
                for j, fname in enumerate(order):
                    arr = features.get(fname)
                    if arr is None:
                        continue
                    vals = arr[part[:, 0], part[:, 1]]
                    X[:, j] = np.nan_to_num(vals, nan=0.0)
                pred = model_s.predict(X)
                out = []
                for v in pred:
                    try:
                        out.append(int(round(float(v))))
                    except Exception:
                        txt = str(v).strip().lower()
                        if txt in ("none", "0", "class0"):
                            out.append(0)
                        elif txt in ("1", "class1"):
                            out.append(1)
                        elif txt in ("2", "class2"):
                            out.append(2)
                        elif txt in ("3", "class3"):
                            out.append(3)
                        else:
                            out.append(0)
                out_arr = np.asarray(out, dtype=int)
                severity[part[:, 0], part[:, 1]] = np.clip(out_arr, 0, 3)
            severity_mode = "joblib_artifact"
        except Exception as exc:
            severity_error = str(exc)
            severity = np.full(prob.shape, 0, dtype=int)
            severity[(prob >= 0.25) & np.isfinite(prob)] = 1
            severity[(prob >= 0.50) & np.isfinite(prob)] = 2
            severity[(prob >= 0.75) & np.isfinite(prob)] = 3
    else:
        severity = np.full(prob.shape, 0, dtype=int)
        severity[(prob >= 0.25) & np.isfinite(prob)] = 1
        severity[(prob >= 0.50) & np.isfinite(prob)] = 2
        severity[(prob >= 0.75) & np.isfinite(prob)] = 3

    return {
        "risk_norm": np.nan_to_num(prob, nan=0.0),
        "risk_score": risk_score,
        "severity": severity,
        "event_detected": event_detected,
        "features": features,
        "meta": {
            "model_version": (
                "event-ml-v1-artifact"
                if str(mode_meta.get("inference_mode", "")).startswith(("linear_json", "joblib"))
                else "event-ml-v1-placeholder"
            ),
            "model_key": str(ml_model_key or "event-ml-rf-v1-placeholder"),
            "decision_threshold": float(np.clip(float(ml_threshold), 0.05, 0.95)),
            "event_window": {"start": event_start_iso, "end": event_end_iso},
            "feature_contract": mode_meta.get("feature_contract") or FEATURE_CONTRACT,
            "assumptions": {
                "metric_type": "event_probability",
                "inference_mode": mode_meta.get("inference_mode") or "placeholder_heuristic",
                "rain_source": str((weather_context or {}).get("source") or "proxy"),
            },
            "artifact": {
                "path": mode_meta.get("artifact_path"),
                "type": mode_meta.get("artifact_type"),
                "version": mode_meta.get("artifact_version"),
                "error": mode_meta.get("artifact_error"),
            },
            "severity": {
                "model_key": ml_severity_model_key,
                "artifact_path": severity_artifact_path,
                "mode": severity_mode,
                "error": severity_error,
            },
        },
    }
