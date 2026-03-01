from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_output(args: list[str], cwd: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            return None
        return (proc.stdout or "").strip() or None
    except Exception:
        return None


def _collect_file_info(path: Path) -> dict[str, Any]:
    return {
        "path": str(path).replace("/", "\\"),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
        "sha256": _sha256(path) if path.exists() else None,
        "modified_utc": (
            datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if path.exists()
            else None
        ),
    }


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_manifest(repo_root: Path, out_path: Path, scenario_label: str) -> dict[str, Any]:
    tracked = [
        repo_root / "data" / "layers" / "st_mwl_erosion" / "NDVI_latest.tif",
        repo_root / "data" / "layers" / "st_mwl_erosion" / "NDVI_latest.json",
        repo_root / "data" / "layers" / "st_mwl_erosion" / "C_Faktor_proxy.tif",
        repo_root / "data" / "layers" / "st_mwl_erosion" / "C_Faktor_proxy.json",
        repo_root / "data" / "layers" / "st_mwl_erosion" / "K_Faktor.tif",
        repo_root / "data" / "layers" / "st_mwl_erosion" / "R_Faktor.tif",
        repo_root / "data" / "layers" / "st_mwl_erosion" / "S_Faktor.tif",
        repo_root / "run_fetch_sentinel_ndvi.bat",
        repo_root / "run_build_c_factor_proxy.bat",
        repo_root / "backend" / "fetch_sentinel_ndvi.py",
        repo_root / "backend" / "build_c_factor_proxy.py",
        repo_root / "backend" / "erosion_abag.py",
        repo_root / "backend" / "processing.py",
    ]

    ndvi_meta = _load_json_if_exists(repo_root / "data" / "layers" / "st_mwl_erosion" / "NDVI_latest.json")
    c_meta = _load_json_if_exists(repo_root / "data" / "layers" / "st_mwl_erosion" / "C_Faktor_proxy.json")

    git_commit = _git_output(["rev-parse", "HEAD"], repo_root)
    git_branch = _git_output(["rev-parse", "--abbrev-ref", "HEAD"], repo_root)
    git_status = _git_output(["status", "--short"], repo_root)

    manifest = {
        "schema_version": "paper-artifact-manifest-v1",
        "created_at_utc": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "scenario_label": scenario_label,
        "environment": {
            "python_version": sys.version,
            "platform": platform.platform(),
            "cwd": str(repo_root),
        },
        "git": {
            "commit": git_commit,
            "branch": git_branch,
            "status_short": git_status.splitlines() if git_status else [],
        },
        "data_lineage": {
            "ndvi_meta": ndvi_meta,
            "c_factor_meta": c_meta,
        },
        "artifacts": [_collect_file_info(p) for p in tracked],
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate paper-ready artifact manifest for reproducibility.")
    parser.add_argument(
        "--output",
        default=str(Path("paper") / "manifest" / "paper_artifact_manifest.json"),
        help="Output JSON path (repo-relative or absolute).",
    )
    parser.add_argument(
        "--scenario-label",
        default="sachsen-anhalt-abag-c-pipeline",
        help="Short label for the scenario/study run.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    out = Path(args.output)
    if not out.is_absolute():
        out = repo_root / out

    manifest = build_manifest(repo_root=repo_root, out_path=out, scenario_label=args.scenario_label)
    existing = sum(1 for a in manifest["artifacts"] if a["exists"])
    total = len(manifest["artifacts"])
    print(f"[OK] Manifest: {out}")
    print(f"[OK] Artifacts present: {existing}/{total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
