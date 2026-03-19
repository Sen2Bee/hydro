from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
from matplotlib.patches import Patch, Polygon as MplPolygon
from shapely.geometry import shape


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _float(row: dict[str, str], key: str) -> float:
    value = row.get(key, "")
    try:
        return float(value)
    except Exception:
        return 0.0


def _draw_box(ax, xy, width, height, text, facecolor):
    x, y = xy
    box = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle="round,pad=0.02,rounding_size=0.03",
        linewidth=1.2,
        edgecolor="#284b63",
        facecolor=facecolor,
    )
    ax.add_patch(box)
    ax.text(x + width / 2.0, y + height / 2.0, text, ha="center", va="center", fontsize=11)


def _arrow(ax, start, end):
    ax.add_patch(
        FancyArrowPatch(
            start,
            end,
            arrowstyle="-|>",
            mutation_scale=18,
            linewidth=1.4,
            color="#284b63",
        )
    )


def render_pipeline(out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 4.8))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    _draw_box(ax, (0.03, 0.3), 0.18, 0.4, "Schlaggeometrien\nDGM / Boden\nC-Faktor-Logik", "#d9ed92")
    _draw_box(ax, (0.29, 0.3), 0.18, 0.4, "Stage A\nEventfenster 2023-2025\nICON2D-Cache", "#b5e48c")
    _draw_box(ax, (0.55, 0.3), 0.18, 0.4, "Stage B\nabag + erosion_events_ml\nChunk-Runner", "#99d98c")
    _draw_box(ax, (0.79, 0.3), 0.18, 0.4, "QA / Merge\nQuickcheck\nPaper-Artefakte", "#76c893")

    _arrow(ax, (0.21, 0.5), (0.29, 0.5))
    _arrow(ax, (0.47, 0.5), (0.55, 0.5))
    _arrow(ax, (0.73, 0.5), (0.79, 0.5))

    ax.text(
        0.5,
        0.9,
        "Reproduzierbarer Produktionspfad: statische Disposition, Ereignis-Cache und cache-basierte Analyse",
        ha="center",
        va="center",
        fontsize=13,
        fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def render_histograms(assets_dir: Path, out_path: Path, title_suffix: str) -> None:
    files = [
        ("hist_event_probability_max.csv", "event_probability_max", "Max. Ereigniswahrscheinlichkeit", "#577590"),
        ("hist_abag_index_mean.csv", "abag_index_mean", "ABAG-Index (Mittel)", "#90be6d"),
        ("hist_risk_score_max.csv", "risk_score_max", "Max. Risikoscore", "#f8961e"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2))
    for ax, (name, _, title, color) in zip(axes, files):
        rows = _read_csv(assets_dir / name)
        mids = [(_float(r, "bin_min") + _float(r, "bin_max")) / 2.0 for r in rows]
        counts = [_float(r, "count") for r in rows]
        widths = [max(_float(r, "bin_max") - _float(r, "bin_min"), 0.001) for r in rows]
        total = sum(counts) or 1.0
        shares = [(c / total) * 100.0 for c in counts]
        ax.bar(mids, shares, width=widths, color=color, edgecolor="white")
        ax.set_title(title)
        ax.set_ylabel("Anteil [%]")
        ax.grid(axis="y", alpha=0.25)
    fig.suptitle(f"Normalisierte Verteilungen im {title_suffix}", fontsize=13, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def _plot_geom(ax, geom, facecolor, edgecolor="none", linewidth=0.2, alpha=1.0):
    if geom.geom_type == "Polygon":
        x, y = geom.exterior.coords.xy
        patch = MplPolygon(list(zip(x, y)), closed=True, facecolor=facecolor, edgecolor=edgecolor, linewidth=linewidth, alpha=alpha)
        ax.add_patch(patch)
    elif geom.geom_type == "MultiPolygon":
        for g in geom.geoms:
            _plot_geom(ax, g, facecolor=facecolor, edgecolor=edgecolor, linewidth=linewidth, alpha=alpha)


def _feature_centroids(fields_geojson: Path) -> list[dict[str, object]]:
    obj = json.loads(fields_geojson.read_text(encoding="utf-8"))
    out: list[dict[str, object]] = []
    for idx, feat in enumerate(obj.get("features", []), start=1):
        geom = shape(feat["geometry"])
        centroid = geom.centroid
        props = feat.get("properties", {}) or {}
        field_id = (
            props.get("field_id")
            or props.get("schlag_id")
            or props.get("flik")
            or props.get("id")
            or props.get("ID")
            or f"field_{idx:05d}"
        )
        out.append({"field_id": str(field_id), "x": centroid.x, "y": centroid.y, "idx": idx})
    return out


def render_chunk_map(fields_geojson: Path, out_path: Path, title_suffix: str, chunk_size: int = 1000) -> None:
    pts = _feature_centroids(fields_geojson)
    colors = ["#f4a261", "#e63946", "#1d3557", "#6a4c93", "#2a9d8f"]
    fig, ax = plt.subplots(figsize=(8.6, 8.0))

    handles = []
    seen = set()
    for row in pts:
        idx = int(row["idx"])
        chunk_id = ((idx - 1) // chunk_size) + 1
        color = colors[(chunk_id - 1) % len(colors)]
        ax.scatter(row["x"], row["y"], s=6.5, color=color, alpha=0.8, linewidths=0)
        if chunk_id not in seen:
            handles.append(Patch(facecolor=color, edgecolor="none", label=f"Chunk {chunk_id}"))
            seen.add(chunk_id)

    xs = [float(r["x"]) for r in pts]
    ys = [float(r["y"]) for r in pts]
    ax.set_xlim(min(xs), max(xs))
    ax.set_ylim(min(ys), max(ys))
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(f"Räumliche Verteilung der Flächen nach Chunk-Zuordnung im {title_suffix}")
    ax.text(0.01, 0.99, "Darstellung über Flächenzentroide", transform=ax.transAxes, ha="left", va="top", fontsize=9)
    ax.legend(
        handles=handles,
        loc="lower left",
        frameon=True,
        framealpha=0.95,
        facecolor="white",
        edgecolor="#cccccc",
        title="Chunk-Zuordnung",
        title_fontsize=9,
        fontsize=9,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def render_top10_map(fields_geojson: Path, top10_geojson: Path, out_path: Path, title_suffix: str) -> None:
    all_pts = _feature_centroids(fields_geojson)
    top_obj = json.loads(top10_geojson.read_text(encoding="utf-8"))
    top_rows = []
    for rank, feat in enumerate(top_obj.get("features", []), start=1):
        geom = shape(feat["geometry"])
        centroid = geom.centroid
        props = feat.get("properties", {}) or {}
        top_rows.append(
            {
                "rank": rank,
                "field_id": str(props.get("field_id") or props.get("schlag_id") or f"rank_{rank}"),
                "score": props.get("score", ""),
                "x": centroid.x,
                "y": centroid.y,
            }
        )

    fig = plt.figure(figsize=(12.2, 8.1))
    ax = fig.add_axes([0.06, 0.1, 0.58, 0.82])
    ax_text = fig.add_axes([0.69, 0.1, 0.28, 0.82])

    ax.scatter([r["x"] for r in all_pts], [r["y"] for r in all_pts], s=5, color="#d9d9d9", alpha=0.45, linewidths=0)
    ax.scatter([r["x"] for r in top_rows], [r["y"] for r in top_rows], s=105, color="#d62828", edgecolors="white", linewidths=0.9, zorder=3)
    for r in top_rows:
        ax.text(r["x"], r["y"], str(r["rank"]), color="white", ha="center", va="center", fontsize=9.5, fontweight="bold", zorder=4)

    xs = [float(r["x"]) for r in all_pts]
    ys = [float(r["y"]) for r in all_pts]
    ax.set_xlim(min(xs), max(xs))
    ax.set_ylim(min(ys), max(ys))
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(f"Top-10 priorisierte Felder im räumlichen Kontext des {title_suffix}")
    ax.legend(
        handles=[
            Patch(facecolor="#d9d9d9", edgecolor="none", label="übrige Pilotflächen"),
            Patch(facecolor="#d62828", edgecolor="none", label="Top-10-Zentroide"),
        ],
        loc="upper left",
        bbox_to_anchor=(0.01, 0.985),
        frameon=True,
        framealpha=0.95,
        facecolor="white",
        edgecolor="#cccccc",
        fontsize=9,
        title="Legende",
        title_fontsize=9,
    )

    ax_text.axis("off")
    ax_text.set_title("Rangliste", loc="left", fontsize=12, pad=10)
    lines = []
    for r in top_rows:
        score = str(r["score"])
        lines.append(f"{r['rank']:>2}  {r['field_id']}  (Score {score})")
    ax_text.text(0.0, 0.98, "\n".join(lines), va="top", ha="left", fontsize=10, family="monospace")

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description="Render manuscript figures from paper assets and quickcheck outputs.")
    p.add_argument("--assets-dir", required=True)
    p.add_argument("--fields-geojson", required=True)
    p.add_argument("--top10-geojson", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--title-suffix", default="Pilotlauf")
    args = p.parse_args()

    assets_dir = Path(args.assets_dir).resolve()
    fields_geojson = Path(args.fields_geojson).resolve()
    top10_geojson = Path(args.top10_geojson).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    render_pipeline(out_dir / "figure_01_pipeline.png")
    render_histograms(assets_dir, out_dir / "figure_02_histograms.png", args.title_suffix)
    render_chunk_map(fields_geojson, out_dir / "figure_03_chunk_map.png", args.title_suffix)
    render_top10_map(fields_geojson, top10_geojson, out_dir / "figure_04_top10_map.png", args.title_suffix)

    print(f"[OK] figures: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
