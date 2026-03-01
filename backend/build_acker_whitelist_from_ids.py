from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser(
        description=(
            "Build a strict field-id whitelist from source sqlite. "
            "This is an ID baseline file to be manually/externally refined to true agricultural IDs."
        )
    )
    p.add_argument(
        "--source-sqlite",
        default=str(Path("data") / "raw" / "sa_flurstuecke" / "cache" / "flurstuecke.sqlite"),
    )
    p.add_argument(
        "--out-txt",
        default=str(Path("data") / "derived" / "whitelists" / "acker_ids.txt"),
    )
    args = p.parse_args()

    src = Path(args.source_sqlite).resolve()
    out = Path(args.out_txt).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    if not src.exists():
        raise SystemExit(f"sqlite not found: {src}")

    ids: set[str] = set()
    with sqlite3.connect(str(src)) as conn:
        cur = conn.execute("SELECT feature_json FROM flurstuecke")
        for (fj,) in cur:
            try:
                obj = json.loads(str(fj))
                props = obj.get("properties") or {}
                for k in ("schlag_id", "field_id", "flik", "FLURSTUECKSKENNZEICHEN", "id", "ID"):
                    v = props.get(k)
                    if v is not None and str(v).strip():
                        ids.add(str(v).strip())
                        break
            except Exception:
                continue

    out.write_text("\n".join(sorted(ids)) + "\n", encoding="utf-8")
    print(f"[OK] whitelist IDs written: {len(ids)}")
    print(f"[OK] file: {out}")
    print(
        "[WARN] This is an ID baseline. For paper-grade agrar-only runs, replace/refine this file with true Acker-IDs."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

