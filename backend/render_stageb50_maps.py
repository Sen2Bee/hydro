from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


def _load_point_features(path: Path) -> list[dict]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    out = []
    for idx, feat in enumerate(obj.get("features", []), start=1):
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or [None, None]
        props = feat.get("properties") or {}
        out.append(
            {
                "idx": idx,
                "x": float(coords[0]),
                "y": float(coords[1]),
                "field_id": str(props.get("field_id") or props.get("schlag_id") or f"field_{idx:06d}"),
                "chunk_id": int(props["chunk_id"]) if props.get("chunk_id") is not None else None,
            }
        )
    return out


def _load_top10(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for idx, row in enumerate(csv.DictReader(f), start=1):
            rows.append(
                {
                    "rank": idx,
                    "field_id": str(row.get("field_id") or "").strip(),
                    "score": str(row.get("score") or "").strip(),
                }
            )
    return rows


def _load_bbox(path: Path | None) -> tuple[float, float, float, float] | None:
    if path is None:
        return None
    obj = json.loads(path.read_text(encoding="utf-8"))
    bbox = obj.get("bbox_wgs84") or {}
    if not bbox:
        return None
    return (
        float(bbox["west"]),
        float(bbox["south"]),
        float(bbox["east"]),
        float(bbox["north"]),
    )


def render_chunk_map(fields: list[dict], out_path: Path, title_suffix: str, state_bbox: tuple[float, float, float, float] | None) -> None:
    cmap = plt.get_cmap("tab20")
    fig, ax = plt.subplots(figsize=(10.5, 8.8))
    handles = []
    seen = set()
    xs = [r["x"] for r in fields]
    ys = [r["y"] for r in fields]
    point_colors = []
    for row in fields:
        chunk_id = int(row["chunk_id"]) if row.get("chunk_id") is not None else 0
        color = cmap((chunk_id - 1) % 20) if chunk_id > 0 else "#999999"
        point_colors.append(color)
        if chunk_id > 0 and chunk_id not in seen:
            handles.append(Patch(facecolor=color, edgecolor="none", label=f"Chunk {chunk_id}"))
            seen.add(chunk_id)
    ax.scatter(xs, ys, s=6.5, c=point_colors, alpha=0.8, linewidths=0)
    if state_bbox:
        ax.set_xlim(state_bbox[0], state_bbox[2])
        ax.set_ylim(state_bbox[1], state_bbox[3])
    else:
        ax.set_xlim(min(xs), max(xs))
        ax.set_ylim(min(ys), max(ys))
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(f"Erster 50-Chunk-Block im landesweiten Sachsen-Anhalt-Lauf ({title_suffix})")
    ax.text(0.01, 0.99, "Darstellung über Flächenzentroide im Landesrahmen", transform=ax.transAxes, ha="left", va="top", fontsize=9)
    ax.legend(
        handles=sorted(handles, key=lambda h: int(h.get_label().split()[-1])),
        loc="upper left",
        bbox_to_anchor=(1.01, 1.0),
        frameon=True,
        framealpha=0.95,
        facecolor="white",
        edgecolor="#cccccc",
        title="Chunk-Zuordnung",
        title_fontsize=9,
        fontsize=8,
        ncol=2,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def render_top10_map(fields: list[dict], top10_fields: list[dict], top10_rows: list[dict], out_path: Path, title_suffix: str, state_bbox: tuple[float, float, float, float] | None) -> None:
    lookup = {r["field_id"]: r for r in top10_rows}
    top_pts = []
    for row in top10_fields:
        if row["field_id"] in lookup:
            item = dict(row)
            item.update(lookup[row["field_id"]])
            top_pts.append(item)
    top_pts.sort(key=lambda r: int(r["rank"]))

    fig = plt.figure(figsize=(12.2, 8.1))
    ax = fig.add_axes([0.06, 0.1, 0.58, 0.82])
    ax_text = fig.add_axes([0.69, 0.1, 0.28, 0.82])

    ax.scatter([r["x"] for r in fields], [r["y"] for r in fields], s=5, color="#d9d9d9", alpha=0.45, linewidths=0)
    ax.scatter([r["x"] for r in top_pts], [r["y"] for r in top_pts], s=105, color="#d62828", edgecolors="white", linewidths=0.9, zorder=3)
    for r in top_pts:
        ax.text(r["x"], r["y"], str(r["rank"]), color="white", ha="center", va="center", fontsize=9.5, fontweight="bold", zorder=4)

    xs = [r["x"] for r in fields]
    ys = [r["y"] for r in fields]
    if state_bbox:
        ax.set_xlim(state_bbox[0], state_bbox[2])
        ax.set_ylim(state_bbox[1], state_bbox[3])
    else:
        ax.set_xlim(min(xs), max(xs))
        ax.set_ylim(min(ys), max(ys))
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(f"Top-10-Felder im Landesrahmen von Sachsen-Anhalt ({title_suffix})")
    ax.legend(
        handles=[
            Patch(facecolor="#d9d9d9", edgecolor="none", label="übrige Flächen"),
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
    lines = [f"{r['rank']:>2}  {r['field_id']}  (Score {r['score']})" for r in top_pts]
    ax_text.text(0.0, 0.98, "\n".join(lines), va="top", ha="left", fontsize=10, family="monospace")
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description="Render final Stage-B map figures from point GeoJSON inputs.")
    p.add_argument("--fields-geojson", required=True)
    p.add_argument("--top10-geojson", required=True)
    p.add_argument("--top10-csv", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--title-suffix", default="50-Chunk-Finallauf")
    p.add_argument("--bbox-json")
    args = p.parse_args()

    fields = _load_point_features(Path(args.fields_geojson).resolve())
    top10_fields = _load_point_features(Path(args.top10_geojson).resolve())
    top10_rows = _load_top10(Path(args.top10_csv).resolve())
    state_bbox = _load_bbox(Path(args.bbox_json).resolve()) if args.bbox_json else None
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    render_chunk_map(fields, out_dir / "figure_03_chunk_map.png", args.title_suffix, state_bbox)
    render_top10_map(fields, top10_fields, top10_rows, out_dir / "figure_04_top10_map.png", args.title_suffix, state_bbox)
    print(f"[OK] maps: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
