"""
Recalculate ABAG indices using CT-NOW field-specific C-factors.

Usage:
    python recalc_abag_with_ctnow.py \
        --erosion-csv paper/exports/.../field_event_results_ALL_50chunks.csv \
        --ctnow-csv data/derived/sa_ctnow_predictions_7class.csv \
        --output paper/exports/.../field_event_results_ALL_50chunks_cfactor_updated.csv \
        [--c-proxy 0.15] [--min-confidence 0.60]

Method:
    new_abag = old_abag × (C_real / C_proxy)

    Where C_real comes from CT-NOW crop type → DIN 19708 lookup,
    and C_proxy is the static raster value used in the original run.
"""

import argparse
import csv
import sys
from pathlib import Path
from cfactor_lookup import CFACTOR_7CLASS, CFACTOR_PROXY_DEFAULT


def main():
    parser = argparse.ArgumentParser(description="Recalculate ABAG with CT-NOW C-factors")
    parser.add_argument("--erosion-csv", required=True, help="Merged erosion results CSV")
    parser.add_argument("--ctnow-csv", required=True, help="CT-NOW predictions CSV")
    parser.add_argument("--output", required=True, help="Output CSV with updated ABAG")
    parser.add_argument("--c-proxy", type=float, default=CFACTOR_PROXY_DEFAULT,
                        help=f"C-factor proxy value used in original run (default: {CFACTOR_PROXY_DEFAULT})")
    parser.add_argument("--min-confidence", type=float, default=0.60,
                        help="Minimum CT-NOW confidence to use real C-factor (default: 0.60)")
    args = parser.parse_args()

    # Load CT-NOW predictions
    ctnow = {}
    with open(args.ctnow_csv) as f:
        for row in csv.DictReader(f):
            fid = row.get("field_id", row.get("id", ""))
            pred = row.get("prediction", "")
            conf = float(row.get("confidence", 0))
            if pred and conf >= args.min_confidence:
                ctnow[fid] = (pred, conf)

    print(f"CT-NOW: {len(ctnow)} fields with confidence >= {args.min_confidence}")

    # Class distribution
    from collections import Counter
    class_counts = Counter(v[0] for v in ctnow.values())
    for cls, n in class_counts.most_common():
        c = CFACTOR_7CLASS.get(cls, "?")
        print(f"  {cls}: {n} fields (C={c})")

    # Process erosion CSV
    updated = 0
    unchanged = 0
    abag_rows = 0
    total = 0

    with open(args.erosion_csv) as fin, open(args.output, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        fieldnames = list(reader.fieldnames) + [
            "crop_class_ctnow", "crop_confidence", "c_factor_real",
            "c_factor_proxy", "abag_index_mean_orig", "abag_index_max_orig"
        ]
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            fid = row["field_id"]
            is_abag = row.get("analysis_type") == "abag"

            # Add CT-NOW info
            if fid in ctnow:
                crop_class, conf = ctnow[fid]
                c_real = CFACTOR_7CLASS.get(crop_class)
                row["crop_class_ctnow"] = crop_class
                row["crop_confidence"] = f"{conf:.3f}"
                row["c_factor_real"] = f"{c_real:.4f}" if c_real else ""
                row["c_factor_proxy"] = f"{args.c_proxy:.4f}"

                if is_abag and c_real is not None:
                    abag_rows += 1
                    ratio = c_real / args.c_proxy

                    # Save originals
                    row["abag_index_mean_orig"] = row.get("abag_index_mean", "")
                    row["abag_index_max_orig"] = row.get("abag_index_max", "")

                    # Recalculate
                    for col in ["abag_index_mean", "abag_index_p90", "abag_index_max"]:
                        val = row.get(col, "")
                        if val:
                            try:
                                row[col] = f"{float(val) * ratio:.6f}"
                                updated += 1
                            except ValueError:
                                pass
            else:
                row["crop_class_ctnow"] = ""
                row["crop_confidence"] = ""
                row["c_factor_real"] = ""
                row["c_factor_proxy"] = f"{args.c_proxy:.4f}"
                row["abag_index_mean_orig"] = ""
                row["abag_index_max_orig"] = ""
                if is_abag:
                    unchanged += 1

            writer.writerow(row)

    print(f"\nResults:")
    print(f"  Total rows: {total}")
    print(f"  ABAG rows with CT-NOW C-factor: {abag_rows} ({updated} values updated)")
    print(f"  ABAG rows unchanged (no CT-NOW): {unchanged}")
    print(f"  Output: {args.output}")


if __name__ == "__main__":
    main()
