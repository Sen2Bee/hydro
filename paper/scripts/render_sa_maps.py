"""
Render publication-quality SA maps from 50k fields_overview.geojson.

Figure 3: Block/Chunk-Zuordnung
Figure 4: Top-10 Karte mit drei Rankings (ML, ABAG, Kombiniert)
"""
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import matplotlib.ticker as mticker
import numpy as np

FIELDS_GEOJSON = Path("d:/__GeoFlux/erosion-monitor/public/data/fields_overview.geojson")
CHUNKS_DIR = Path("d:/__GeoFlux/hydrowatch/paper/input/sa_chunks_icon2d_3y_spatial_filtered_50k/20240401_20241031")
OUT_DIR = Path("d:/__GeoFlux/hydrowatch/paper/figures")

# Colors for three rankings
C_ML = "#d62828"       # red
C_ABAG = "#1d7874"     # teal
C_COMBO = "#e85d04"    # orange


def load_fields():
    """Load 50k field centroids with all properties."""
    with open(FIELDS_GEOJSON) as f:
        data = json.load(f)

    chunk_map = {}
    for cfile in sorted(CHUNKS_DIR.glob("schlaege_chunk_*.geojson")):
        chunk_id = int(cfile.stem.split("_")[-1])
        with open(cfile) as f:
            cdata = json.load(f)
        for feat in cdata["features"]:
            fid = feat["properties"].get("FELD_ID", feat["properties"].get("field_id", ""))
            chunk_map[str(fid)] = chunk_id

    fields = []
    for feat in data["features"]:
        lon, lat = feat["geometry"]["coordinates"]
        p = feat["properties"]
        fields.append({
            "x": lon, "y": lat,
            "field_id": p.get("field_id", ""),
            "gemarkung": p.get("gemarkung", ""),
            "chunk_id": chunk_map.get(p.get("field_id", ""), 0),
            "ml_risk_max": p.get("ml_risk_score_max") or 0,
            "ml_prob_max": p.get("ml_event_probability_max") or 0,
            "ml_detected": p.get("ml_event_detected_share_pct") or 0,
            "ml_events": p.get("ml_event_count") or 0,
            "abag_max": p.get("abag_index_max") or 0,
            "abag_mean": p.get("abag_index_mean") or 0,
        })
    return fields


def _geo_dims(xs, ys, target_h=12.0):
    """Compute figure width for geographic aspect ratio."""
    mean_lat = sum(ys) / len(ys)
    w_km = (max(xs) - min(xs)) * 111.32 * math.cos(math.radians(mean_lat))
    h_km = (max(ys) - min(ys)) * 111.32
    fig_w = target_h * (w_km / h_km)
    return fig_w, target_h, mean_lat


def compute_rankings(fields, n=10):
    """Compute three top-N rankings with tiebreakers."""
    # ML: composite = risk + prob*10 + events*0.5
    ml_ranked = sorted(fields, key=lambda f: (
        -f["ml_risk_max"], -f["ml_prob_max"], -f["ml_events"]))[:n]

    # ABAG: by abag_max
    abag_ranked = sorted(fields, key=lambda f: -f["abag_max"])[:n]

    # Combined: normalize both to 0-1, average
    ml_max = max(f["ml_risk_max"] for f in fields) or 1
    abag_max_val = max(f["abag_max"] for f in fields) or 1
    for f in fields:
        f["combo_score"] = (
            0.5 * (f["ml_risk_max"] / ml_max) +
            0.5 * (f["abag_max"] / abag_max_val)
        ) * 100
    combo_ranked = sorted(fields, key=lambda f: -f["combo_score"])[:n]

    return ml_ranked, abag_ranked, combo_ranked


def render_chunk_map(fields, out_path):
    """Figure 3: Block-Zuordnung."""
    xs = [f["x"] for f in fields]
    ys = [f["y"] for f in fields]
    fig_w, fig_h, mean_lat = _geo_dims(xs, ys, 12.0)

    fig, ax = plt.subplots(figsize=(fig_w + 3.5, fig_h))
    fig.subplots_adjust(right=0.78)

    cmap = plt.get_cmap("tab20")
    handles, seen = [], set()
    colors = []
    for f in fields:
        cid = f["chunk_id"]
        c = cmap((cid - 1) % 20) if cid > 0 else "#999999"
        colors.append(c)
        if cid > 0 and cid not in seen:
            handles.append(Patch(facecolor=c, edgecolor="none", label=f"Block {cid}"))
            seen.add(cid)

    ax.scatter(xs, ys, s=4, c=colors, alpha=0.8, linewidths=0)
    ax.set_aspect(1.0 / math.cos(math.radians(mean_lat)))
    ax.set_xlim(min(xs) - 0.05, max(xs) + 0.05)
    ax.set_ylim(min(ys) - 0.05, max(ys) + 0.05)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title("Räumliche Verteilung nach Block-Zuordnung\n"
                 "(50-Block-Finallauf, Vegetationsperiode 2024)",
                 fontsize=11, pad=10)

    ax.legend(
        handles=sorted(handles, key=lambda h: int(h.get_label().split()[-1])),
        loc="upper left", bbox_to_anchor=(1.02, 1.0),
        frameon=True, framealpha=0.95, facecolor="white", edgecolor="#cccccc",
        title="Block-Zuordnung", title_fontsize=9, fontsize=7, ncol=2,
    )

    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] {out_path.name}")


def render_top10_map(fields, out_path):
    """Figure 4: Drei Top-10-Rankings auf einer Karte mit Tabellen."""
    ml_top, abag_top, combo_top = compute_rankings(fields, n=10)

    xs = [f["x"] for f in fields]
    ys = [f["y"] for f in fields]
    fig_w, fig_h, mean_lat = _geo_dims(xs, ys, 13.0)

    # Layout: map left, three tables stacked right
    total_w = fig_w + 10.0
    fig = plt.figure(figsize=(total_w, fig_h))

    map_frac = fig_w / total_w * 0.95
    ax = fig.add_axes([0.02, 0.04, map_frac, 0.92])

    # Three table axes stacked vertically on the right
    tbl_x = map_frac + 0.06
    tbl_w = 1.0 - tbl_x - 0.02
    tbl_h = 0.28
    ax_ml = fig.add_axes([tbl_x, 0.68, tbl_w, tbl_h])
    ax_abag = fig.add_axes([tbl_x, 0.37, tbl_w, tbl_h])
    ax_combo = fig.add_axes([tbl_x, 0.06, tbl_w, tbl_h])

    # Background points
    ax.scatter(xs, ys, s=2, color="#e0e0e0", alpha=0.35, linewidths=0)

    # Plot all three sets of points
    def _plot_top(top_list, color, marker, size, zorder_base):
        for i, f in enumerate(top_list):
            ax.scatter(f["x"], f["y"], s=size, color=color,
                      edgecolors="white", linewidths=1.2, zorder=zorder_base,
                      marker=marker)
            ax.text(f["x"] + 0.04, f["y"] + 0.03, str(i + 1),
                   color=color, fontsize=10, fontweight="bold",
                   zorder=zorder_base + 1,
                   bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                            edgecolor=color, alpha=0.9, linewidth=0.8))

    _plot_top(combo_top, C_COMBO, "D", 120, 3)
    _plot_top(abag_top, C_ABAG, "s", 100, 4)
    _plot_top(ml_top, C_ML, "o", 120, 5)

    ax.set_aspect(1.0 / math.cos(math.radians(mean_lat)))
    ax.set_xlim(min(xs) - 0.05, max(xs) + 0.05)
    ax.set_ylim(min(ys) - 0.05, max(ys) + 0.05)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title("Priorisierte Felder nach drei Bewertungsmethoden\n"
                 "(50-Block-Finallauf, Vegetationsperiode 2024)",
                 fontsize=11, pad=10)

    ax.legend(
        handles=[
            Line2D([0], [0], marker="o", color="w", markerfacecolor=C_ML,
                   markersize=8, label="Erosionsrisiko (ML)"),
            Line2D([0], [0], marker="s", color="w", markerfacecolor=C_ABAG,
                   markersize=7, label="ABAG-Index"),
            Line2D([0], [0], marker="D", color="w", markerfacecolor=C_COMBO,
                   markersize=7, label="Kombiniert"),
            Patch(facecolor="#e0e0e0", edgecolor="none", label="Übrige Flächen"),
        ],
        loc="lower left", frameon=True, framealpha=0.95,
        facecolor="white", edgecolor="#cccccc", fontsize=8,
    )

    # Render tables
    def _render_table(ax_tbl, title, color, top_list, score_key, score_label, fmt=".1f"):
        ax_tbl.axis("off")
        ax_tbl.set_xlim(0, 1)
        ax_tbl.set_ylim(0, 1)

        # Title bar
        ax_tbl.fill_between([0, 1], [0.92, 0.92], [1.0, 1.0],
                           color=color, alpha=0.15)
        ax_tbl.text(0.03, 0.96, title, fontsize=12, fontweight="bold",
                   color=color, va="center")

        # Header
        y = 0.86
        ax_tbl.text(0.03, y, "#", fontsize=9.5, fontweight="bold", color="#666", va="center")
        ax_tbl.text(0.10, y, "Feld-ID", fontsize=9.5, fontweight="bold", color="#666", va="center")
        ax_tbl.text(0.55, y, "Gemarkung", fontsize=9.5, fontweight="bold", color="#666", va="center")
        ax_tbl.text(0.90, y, score_label, fontsize=9.5, fontweight="bold", color="#666",
                   va="center", ha="right")
        ax_tbl.axhline(y=0.83, xmin=0.02, xmax=0.98, color="#ccc", linewidth=0.5)

        # Rows
        for i, f in enumerate(top_list):
            y = 0.78 - i * 0.078
            bg = "#f8f8f8" if i % 2 == 0 else "white"
            ax_tbl.fill_between([0.01, 0.99], [y - 0.035, y - 0.035],
                               [y + 0.035, y + 0.035], color=bg)

            score = f.get(score_key, 0)
            gem = f.get("gemarkung", "")[:14]

            ax_tbl.text(0.03, y, f"{i+1}", fontsize=9.5, color=color,
                       fontweight="bold", va="center")
            ax_tbl.text(0.10, y, f["field_id"], fontsize=9, va="center",
                       family="monospace")
            ax_tbl.text(0.55, y, gem, fontsize=9, va="center", color="#555")
            ax_tbl.text(0.90, y, f"{score:{fmt}}", fontsize=9.5, va="center",
                       ha="right", fontweight="bold")

    _render_table(ax_ml, "Erosionsrisiko (ML)", C_ML,
                 ml_top, "ml_risk_max", "Score")
    _render_table(ax_abag, "ABAG-Index", C_ABAG,
                 abag_top, "abag_max", "Index", fmt=".2f")
    _render_table(ax_combo, "Kombiniert (ML + ABAG)", C_COMBO,
                 combo_top, "combo_score", "Score")

    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] {out_path.name}")


if __name__ == "__main__":
    fields = load_fields()
    print(f"Loaded {len(fields)} fields, "
          f"{sum(1 for f in fields if f['chunk_id'] > 0)} with chunk assignment")
    render_chunk_map(fields, OUT_DIR / "figure_03_chunk_map.png")
    render_top10_map(fields, OUT_DIR / "figure_04_top10_map.png")
