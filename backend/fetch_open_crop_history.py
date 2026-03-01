from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests


ZENODO_API = "https://zenodo.org/api/records/{record_id}"

# WGS84 bbox from SA extent conversion already used in project.
SA_BBOX_WGS84 = (10.534168657927681, 50.97330866288862, 13.283020232424267, 52.98996620841052)

CORE_RECORDS = [13951344, 17197830, 17182293]
FULL_EXTRA_RECORDS = [10619782]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _download(url: str, out: Path, timeout_s: int = 120) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    part = out.with_suffix(out.suffix + ".part")
    got = 0
    next_log_mb = 100
    with requests.get(url, stream=True, timeout=timeout_s) as r:
        r.raise_for_status()
        with part.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    got += len(chunk)
                    mb = got / 1024 / 1024
                    if mb >= next_log_mb:
                        print(f"    ... {mb:.0f} MB")
                        next_log_mb += 100
    part.replace(out)


def _clip_sa(input_tif: Path, out_tif: Path) -> None:
    west, south, east, north = SA_BBOX_WGS84
    gdalwarp = r"C:\OSGeo4W\bin\gdalwarp.exe"
    cmd = [
        gdalwarp,
        "-overwrite",
        "-te",
        str(west),
        str(south),
        str(east),
        str(north),
        "-te_srs",
        "EPSG:4326",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "TILED=YES",
        str(input_tif),
        str(out_tif),
    ]
    p = subprocess.run(cmd, check=False)
    if p.returncode != 0:
        raise RuntimeError(f"gdalwarp failed ({p.returncode}) for {input_tif}")


def _include_file(profile: str, key: str) -> bool:
    k = key.lower()
    if profile == "core":
        if k.endswith(".tif"):
            return True
        if "legend" in k and (k.endswith(".clr") or k.endswith(".sld")):
            return True
        if "accuracy" in k and k.endswith(".pdf"):
            return True
        return False
    # full
    return True


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch open crop-history/c-factor datasets from Zenodo.")
    p.add_argument("--profile", choices=["core", "full"], default="core")
    p.add_argument(
        "--out-root",
        default=str(Path("data") / "raw" / "crop_history_open"),
        help="Output root for downloads and manifests.",
    )
    p.add_argument("--clip-sa", action="store_true", help="Create SA-clipped versions for downloaded TIFFs.")
    p.add_argument("--timeout-s", type=int, default=120)
    args = p.parse_args()

    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    downloads_root = out_root / "downloads"
    clips_root = out_root / "sa_clips"
    manifests_root = out_root / "manifests"
    manifests_root.mkdir(parents=True, exist_ok=True)
    if args.clip_sa:
        clips_root.mkdir(parents=True, exist_ok=True)

    record_ids = list(CORE_RECORDS)
    if args.profile == "full":
        record_ids.extend(FULL_EXTRA_RECORDS)

    run = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "profile": args.profile,
        "clip_sa": bool(args.clip_sa),
        "records": [],
    }

    for rid in record_ids:
        api_url = ZENODO_API.format(record_id=rid)
        r = requests.get(api_url, timeout=args.timeout_s)
        r.raise_for_status()
        rec = r.json()
        title = (rec.get("metadata") or {}).get("title")
        files = rec.get("files") or []
        rec_dir = downloads_root / str(rid)
        rec_dir.mkdir(parents=True, exist_ok=True)
        rec_meta = rec_dir / "record.json"
        rec_meta.write_text(json.dumps(rec, indent=2), encoding="utf-8")

        rec_run = {
            "record_id": rid,
            "title": title,
            "files_total": len(files),
            "downloaded": [],
            "skipped": [],
        }
        print(f"[record {rid}] {title}")

        for f in files:
            key = f.get("key")
            if not key:
                continue
            if not _include_file(args.profile, key):
                rec_run["skipped"].append({"key": key, "reason": "profile_filter"})
                continue
            links = f.get("links") or {}
            url = links.get("self")
            if not url:
                rec_run["skipped"].append({"key": key, "reason": "no_download_link"})
                continue
            out = rec_dir / key
            if out.exists() and out.stat().st_size > 0:
                print(f"  skip existing: {key}")
            else:
                print(f"  download: {key}")
                _download(url, out, timeout_s=args.timeout_s)
            entry = {
                "key": key,
                "path": str(out),
                "size_bytes": int(out.stat().st_size),
                "sha256": _sha256(out),
            }
            rec_run["downloaded"].append(entry)

            if args.clip_sa and key.lower().endswith(".tif"):
                clip_out = clips_root / f"{rid}_{Path(key).stem}_sa.tif"
                if clip_out.exists() and clip_out.stat().st_size > 0:
                    print(f"  clip exists: {clip_out.name}")
                else:
                    print(f"  clip SA: {clip_out.name}")
                    _clip_sa(out, clip_out)
                entry["sa_clip_path"] = str(clip_out)
                entry["sa_clip_size_bytes"] = int(clip_out.stat().st_size)

        run["records"].append(rec_run)

    run["finished_at"] = datetime.now(timezone.utc).isoformat()
    run["host"] = os.environ.get("COMPUTERNAME")
    run["python"] = sys.executable
    out_manifest = manifests_root / f"fetch_open_crop_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_manifest.write_text(json.dumps(run, indent=2), encoding="utf-8")
    print(f"[OK] manifest={out_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
