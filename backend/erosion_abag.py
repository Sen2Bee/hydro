from __future__ import annotations

import math
import os
from typing import Any

import numpy as np


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _nan_minmax(arr: np.ndarray) -> tuple[float | None, float | None]:
    mask = np.isfinite(arr)
    if not np.any(mask):
        return None, None
    return float(np.nanmin(arr[mask])), float(np.nanmax(arr[mask]))


def compute_abag_index(
    *,
    acc_cells: np.ndarray,
    slope_deg: np.ndarray,
    soil_risk: np.ndarray,
    impervious_risk: np.ndarray,
    pixel_area_m2: float,
    valid_mask: np.ndarray,
    p_factor_override: float | None = None,
    r_factor_raster: np.ndarray | None = None,
    k_factor_raster: np.ndarray | None = None,
    s_factor_raster: np.ndarray | None = None,
    c_factor_raster: np.ndarray | None = None,
    p_factor_raster: np.ndarray | None = None,
) -> dict[str, Any]:
    """
    ABAG-like proxy index for MVP integration.

    Notes:
    - This is intentionally conservative and explicit as a screening implementation.
    - Factor ranges are configurable via ENV for calibration:
      ABAG_R_FACTOR, ABAG_P_FACTOR.
    """
    r_factor_env = _env_float("ABAG_R_FACTOR", 110.0)
    p_factor_env = _env_float("ABAG_P_FACTOR", 1.0)
    p_factor_const = float(p_factor_env if p_factor_override is None else p_factor_override)
    p_factor_mode = "constant_env" if p_factor_override is None else "request_override"

    slope = np.clip(np.nan_to_num(slope_deg, nan=0.0), 0.0, 65.0)
    slope_rad = np.radians(slope)
    slope_term = np.maximum(0.0, np.sin(slope_rad) / 0.0896)

    # Contributing area as coarse slope-length proxy.
    flow_length_m = np.sqrt(np.maximum(np.nan_to_num(acc_cells, nan=0.0), 1.0) * max(1e-6, float(pixel_area_m2)))
    l_factor = np.clip((flow_length_m / 22.13) ** 0.40, 0.0, 8.0)
    s_factor_topo = np.clip(slope_term**1.30, 0.0, 8.0)
    if isinstance(s_factor_raster, np.ndarray):
        s_factor = np.clip(np.nan_to_num(s_factor_raster, nan=np.nan), 0.0, 12.0)
        s_factor_mode = "raster"
    else:
        s_factor = s_factor_topo
        s_factor_mode = "topo_proxy"
    ls_factor = np.clip(l_factor * s_factor, 0.0, 20.0)

    # K from raster (preferred) or soil proxy fallback.
    if isinstance(k_factor_raster, np.ndarray):
        k_raw = np.nan_to_num(k_factor_raster, nan=np.nan)
        finite_k = k_raw[np.isfinite(k_raw)]
        if finite_k.size and float(np.nanpercentile(finite_k, 99)) <= 1.5:
            # Likely already a K-like factor scale.
            k_factor = np.clip(k_raw, 0.005, 1.2)
            k_factor_mode = "raster_direct"
        else:
            # Fallback mapping from unknown raster value scale.
            k01 = np.clip((k_raw - np.nanmin(finite_k)) / max(1e-9, (np.nanmax(finite_k) - np.nanmin(finite_k))), 0.0, 1.0) if finite_k.size else np.full(k_raw.shape, 0.5, dtype=float)
            k_factor = 0.018 + 0.045 * k01
            k_factor_mode = "raster_normalized"
    else:
        soil01 = np.clip(np.nan_to_num(soil_risk, nan=0.5), 0.0, 1.0)
        k_factor = 0.018 + 0.045 * soil01
        k_factor_mode = "soil_proxy"

    # C from raster (preferred) or cover proxy fallback.
    if isinstance(c_factor_raster, np.ndarray):
        c_factor = np.clip(np.nan_to_num(c_factor_raster, nan=0.30), 0.001, 1.0)
        c_factor_mode = "raster_direct"
    else:
        c01 = np.clip(np.nan_to_num(impervious_risk, nan=0.30), 0.0, 1.0)
        c_factor = 0.03 + 0.37 * c01
        c_factor_mode = "cover_proxy"

    if isinstance(r_factor_raster, np.ndarray):
        r_factor = np.clip(np.nan_to_num(r_factor_raster, nan=r_factor_env), 1.0, 1000.0)
        r_factor_mode = "raster_direct"
    else:
        r_factor = np.full(valid_mask.shape, float(r_factor_env), dtype=float)
        r_factor_mode = "constant_env"

    if isinstance(p_factor_raster, np.ndarray):
        p_factor = np.clip(np.nan_to_num(p_factor_raster, nan=p_factor_const), 0.1, 1.5)
        p_factor_mode = "raster_direct"
    else:
        p_factor = np.full(valid_mask.shape, float(p_factor_const), dtype=float)

    a_index = r_factor * k_factor * ls_factor * c_factor * p_factor
    a_index = np.where(valid_mask, a_index, np.nan)

    finite = np.isfinite(a_index)
    if np.any(finite):
        p95 = float(np.nanpercentile(a_index[finite], 95))
        denom = p95 if (math.isfinite(p95) and p95 > 0.0) else float(np.nanmax(a_index[finite]))
        if not math.isfinite(denom) or denom <= 0.0:
            denom = 1.0
    else:
        denom = 1.0

    risk_norm = np.clip(np.nan_to_num(a_index, nan=0.0) / float(denom), 0.0, 1.0)
    risk_score = np.clip(np.round(risk_norm * 100.0), 0.0, 100.0)
    risk_score = np.where(valid_mask, risk_score, np.nan)

    ls_min, ls_max = _nan_minmax(ls_factor)
    k_min, k_max = _nan_minmax(k_factor)
    c_min, c_max = _nan_minmax(c_factor)
    a_min, a_max = _nan_minmax(a_index)
    r_min, r_max = _nan_minmax(r_factor)
    p_min, p_max = _nan_minmax(p_factor)

    return {
        "risk_norm": risk_norm,
        "risk_score": risk_score,
        "factors": {
            "r_factor": r_factor,
            "p_factor": p_factor,
            "k_factor": k_factor,
            "ls_factor": ls_factor,
            "c_factor": c_factor,
            "a_index": a_index,
        },
        "meta": {
            "model_version": "abag-v1-hybrid",
            "factor_ranges": {
                "r_factor": {"min": r_min, "max": r_max},
                "ls_factor": {"min": ls_min, "max": ls_max},
                "k_factor": {"min": k_min, "max": k_max},
                "c_factor": {"min": c_min, "max": c_max},
                "p_factor": {"min": p_min, "max": p_max},
                "a_index": {"min": a_min, "max": a_max},
            },
            "assumptions": {
                "metric_type": "long_term_index_hybrid",
                "r_factor_mode": r_factor_mode,
                "k_factor_mode": k_factor_mode,
                "ls_factor_mode": f"dem_l_factor_plus_{s_factor_mode}",
                "c_factor_mode": c_factor_mode,
                "p_factor_mode": p_factor_mode,
            },
        },
    }
