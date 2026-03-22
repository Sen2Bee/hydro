"""
Generate Figure 05: ABAG Proxy-C vs CT-NOW-C comparison figure for the Hydrowatch paper.
Three panels: (A) side-by-side histograms, (B) scatter plot, (C) box plots per crop class.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# --- Paths ---
CSV_ORIG = "d:/__GeoFlux/hydrowatch/paper/exports/sa_chunks_icon2d_3y_spatial_filtered_50k/field_event_results_ALL_50chunks.csv"
CSV_CTNOW = "d:/__GeoFlux/hydrowatch/paper/exports/sa_chunks_icon2d_3y_spatial_filtered_50k/field_event_results_ALL_50chunks_cfactor_updated.csv"
OUT_FIG = "d:/__GeoFlux/hydrowatch/paper/figures/figure_05_cfactor_comparison.png"

# --- Load data ---
df_orig = pd.read_csv(CSV_ORIG, low_memory=False)
df_ctnow = pd.read_csv(CSV_CTNOW, low_memory=False)

# Filter to ABAG rows only
df_orig = df_orig[df_orig["analysis_type"] == "abag"].copy()
df_ctnow = df_ctnow[df_ctnow["analysis_type"] == "abag"].copy()

# For CT-NOW: only rows where crop_class_ctnow is populated
df_ctnow = df_ctnow[df_ctnow["crop_class_ctnow"].notna() & (df_ctnow["crop_class_ctnow"] != "")].copy()

# --- Aggregate to field level (mean of abag_index_mean per field) ---
field_orig = df_orig.groupby("field_id").agg(abag_mean=("abag_index_mean", "mean")).reset_index()

field_ctnow = df_ctnow.groupby("field_id").agg(
    abag_mean_ctnow=("abag_index_mean", "mean"),
    abag_mean_orig=("abag_index_mean_orig", "mean"),
    crop_class=("crop_class_ctnow", "first"),
    c_factor_real=("c_factor_real", "first"),
).reset_index()

# --- Crop class labels (German) and colors ---
CROP_LABELS = {
    "wintergetreide": "Wintergetreide",
    "sommergetreide": "Sommergetreide",
    "winterraps": "Winterraps",
    "mais": "Mais",
    "hackfruechte": "Hackfrüchte",
    "gruenland": "Grünland",
    "brache": "Brache",
}

# Tableau-style categorical colors
CROP_COLORS = {
    "wintergetreide": "#4e79a7",
    "sommergetreide": "#f28e2b",
    "winterraps": "#e15759",
    "mais": "#76b7b2",
    "hackfruechte": "#59a14f",
    "gruenland": "#edc948",
    "brache": "#b07aa1",
}

# --- Figure setup ---
fig = plt.figure(figsize=(20 / 2.54, 28 / 2.54), dpi=300, facecolor="white")
gs = gridspec.GridSpec(2, 2, height_ratios=[1, 1.3], hspace=0.55, wspace=0.40,
                       left=0.12, right=0.96, top=0.89, bottom=0.09)

LABEL_SIZE = 10
TITLE_SIZE = 11

# ===== Panel A: Side-by-side histograms =====
ax_a1 = fig.add_subplot(gs[0, 0])
ax_a2 = fig.add_subplot(gs[0, 1])

# Common x-axis range — cap at P99 to use full plot area
xmax = max(field_orig["abag_mean"].quantile(0.99), field_ctnow["abag_mean_ctnow"].quantile(0.99))
bins = np.linspace(0, xmax, 40)

# Left: Proxy-C
ax_a1.hist(field_orig["abag_mean"], bins=bins, color="#4e79a7", edgecolor="white", linewidth=0.4, alpha=0.85)
med_proxy = field_orig["abag_mean"].median()
ax_a1.axvline(med_proxy, color="#c44e52", ls="--", lw=1.3, label=f"Median = {med_proxy:.2f}")
ax_a1.set_xlabel("ABAG-Index (Mittelwert)", fontsize=LABEL_SIZE)
ax_a1.set_ylabel("Anzahl Felder", fontsize=LABEL_SIZE)
ax_a1.set_title("Proxy-C", fontsize=TITLE_SIZE, fontweight="bold")
ax_a1.legend(fontsize=8, loc="upper right")
ax_a1.tick_params(labelsize=9)
ax_a1.set_xlim(0, xmax)

# Right: CT-NOW-C
ax_a2.hist(field_ctnow["abag_mean_ctnow"], bins=bins, color="#59a14f", edgecolor="white", linewidth=0.4, alpha=0.85)
med_ctnow = field_ctnow["abag_mean_ctnow"].median()
ax_a2.axvline(med_ctnow, color="#c44e52", ls="--", lw=1.3, label=f"Median = {med_ctnow:.2f}")
ax_a2.set_xlabel("ABAG-Index (Mittelwert)", fontsize=LABEL_SIZE)
ax_a2.set_ylabel("Anzahl Felder", fontsize=LABEL_SIZE)
ax_a2.set_title("CT-NOW-C", fontsize=TITLE_SIZE, fontweight="bold")
ax_a2.legend(fontsize=8, loc="upper right")
ax_a2.tick_params(labelsize=9)
ax_a2.set_xlim(0, xmax)

# Suptitle for Panel A — use fig.suptitle for proper spacing
fig.suptitle("(A) Verteilung ABAG-Index: Proxy-C vs. CT-NOW-C",
             fontsize=TITLE_SIZE, fontweight="bold", y=0.97)

# ===== Panel B: Scatter plot =====
ax_b = fig.add_subplot(gs[1, 0])

for crop_key, label in CROP_LABELS.items():
    mask = field_ctnow["crop_class"] == crop_key
    if mask.sum() == 0:
        continue
    ax_b.scatter(
        field_ctnow.loc[mask, "abag_mean_orig"],
        field_ctnow.loc[mask, "abag_mean_ctnow"],
        c=CROP_COLORS[crop_key],
        label=label,
        s=8, alpha=0.55, edgecolors="none", rasterized=True,
    )

# 1:1 line
lim = max(ax_b.get_xlim()[1], ax_b.get_ylim()[1])
ax_b.plot([0, lim], [0, lim], "k--", lw=0.8, alpha=0.5, label="1:1")
ax_b.set_xlim(0, lim)
ax_b.set_ylim(0, lim)
ax_b.set_aspect("equal", adjustable="box")
ax_b.set_xlabel("ABAG-Index Proxy-C", fontsize=LABEL_SIZE)
ax_b.set_ylabel("ABAG-Index CT-NOW-C", fontsize=LABEL_SIZE)
ax_b.set_title("(B) Feldspez. Veränderung\ndurch CT-NOW C-Faktor", fontsize=TITLE_SIZE, fontweight="bold")
ax_b.legend(fontsize=7, loc="upper left", markerscale=1.5, framealpha=0.9, ncol=2)
ax_b.tick_params(labelsize=9)

# ===== Panel C: Box plots per crop class =====
ax_c = fig.add_subplot(gs[1, 1])

# Order by median ascending
crop_medians = field_ctnow.groupby("crop_class")["abag_mean_ctnow"].median().sort_values()
ordered_crops = crop_medians.index.tolist()

box_data = [field_ctnow.loc[field_ctnow["crop_class"] == c, "abag_mean_ctnow"].dropna().values for c in ordered_crops]
box_labels = [CROP_LABELS.get(c, c) for c in ordered_crops]
box_colors = [CROP_COLORS.get(c, "#999999") for c in ordered_crops]

bp = ax_c.boxplot(
    box_data, vert=True, patch_artist=True, widths=0.55,
    showfliers=False, medianprops=dict(color="black", linewidth=1.2),
)
for patch, color in zip(bp["boxes"], box_colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.8)

ax_c.set_xticklabels(box_labels, rotation=40, ha="right", fontsize=8)
ax_c.set_ylabel("ABAG-Index (Mittelwert)", fontsize=LABEL_SIZE)
ax_c.set_title("(C) ABAG-Index nach Kulturart\n(CT-NOW)", fontsize=TITLE_SIZE, fontweight="bold")
ax_c.tick_params(labelsize=9)

# Annotate C-factor on each box (above upper whisker to avoid overlap)
c_factors_per_crop = field_ctnow.groupby("crop_class")["c_factor_real"].first()
for i, crop_key in enumerate(ordered_crops):
    cf = c_factors_per_crop.get(crop_key, np.nan)
    if not np.isnan(cf):
        q3 = np.percentile(box_data[i], 75) if len(box_data[i]) > 0 else 0
        iqr = np.percentile(box_data[i], 75) - np.percentile(box_data[i], 25) if len(box_data[i]) > 0 else 0
        whisker_top = min(q3 + 1.5 * iqr, np.max(box_data[i])) if len(box_data[i]) > 0 else 0
        ax_c.text(i + 1, whisker_top + 0.02, f"C={cf:.3f}", ha="center", va="bottom", fontsize=6.5,
                  fontstyle="italic", color="#333333", rotation=0)

plt.savefig(OUT_FIG, dpi=300, facecolor="white", bbox_inches="tight")
plt.close()
print(f"Saved: {OUT_FIG}")
print(f"  Proxy-C median: {med_proxy:.3f}  |  CT-NOW-C median: {med_ctnow:.3f}")
print(f"  Fields (orig): {len(field_orig)}  |  Fields (CT-NOW): {len(field_ctnow)}")
