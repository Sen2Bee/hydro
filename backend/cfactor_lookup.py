"""
C-Factor lookup table for CT-NOW 7-class crop type classification.

Values from DIN 19708:2017 (Bodenbeschaffenheit - Ermittlung der Erosionsgefährdung)
and Schwertmann et al. (1987) "Bodenerosion durch Wasser".

The C-factor represents the ratio of soil loss from a field with a specific
crop/management to soil loss from a continuously tilled bare fallow.
"""

# CT-NOW 7 classes → C-factor (annual average, conventional tillage)
# Source: DIN 19708 Tab. A.1, Schwertmann 1987 Tab. 6.2
CFACTOR_7CLASS = {
    "wintergetreide":  0.10,   # W-Weizen/Gerste/Roggen/Triticale
    "sommergetreide":  0.17,   # S-Gerste, Hafer
    "mais":            0.40,   # Silo-/Körnermais (wide rows, late canopy)
    "hackfruechte":    0.34,   # Kartoffel, Zuckerrübe (average)
    "winterraps":      0.10,   # early canopy closure, similar to W-Getreide
    "gruenland":       0.004,  # permanent grassland
    "brache":          0.02,   # fallow with residue cover
}

# C-factor proxy value used in the pipeline (static raster average for SA)
# Extracted from the C_Faktor_proxy.tif raster (median over cropland pixels)
CFACTOR_PROXY_DEFAULT = 0.15


def get_cfactor(crop_class: str) -> float | None:
    """Return C-factor for a CT-NOW 7-class label, or None if unknown."""
    return CFACTOR_7CLASS.get(crop_class)


def recalc_abag(old_abag: float, crop_class: str,
                c_proxy: float = CFACTOR_PROXY_DEFAULT) -> float | None:
    """
    Recalculate ABAG index using field-specific C-factor.

    ABAG = R × K × L × S × C × P
    Since all factors except C are unchanged:
        new_abag = old_abag × (C_new / C_old)

    Returns None if crop_class is unknown.
    """
    c_new = get_cfactor(crop_class)
    if c_new is None or c_proxy == 0:
        return None
    return old_abag * (c_new / c_proxy)
