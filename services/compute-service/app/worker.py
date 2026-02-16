import json
import os
import shutil
import sys
import tempfile
from typing import Any
from uuid import UUID

import boto3
import psycopg
import redis
from botocore.exceptions import ClientError


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydrowatch:hydrowatch@localhost:5432/hydrowatch"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = os.getenv("QUEUE_KEY", "hydrowatch:jobs")
LEGACY_BACKEND_PATH = os.getenv("LEGACY_BACKEND_PATH", "/legacy_backend")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

if LEGACY_BACKEND_PATH not in sys.path:
    sys.path.insert(0, LEGACY_BACKEND_PATH)

try:
    from create_mock_dem import create_mock_dem
    from processing import analyze_dem
    from st_public_dem import fetch_dem_from_st_public_download
    from st_cog_dem import fetch_dem_from_st_cog_dir
    from wcs_client import fetch_dem_from_wcs
except Exception as exc:
    raise RuntimeError(
        f"Could not import legacy processing modules from {LEGACY_BACKEND_PATH}: {exc}"
    )


def set_status(job_id: UUID, status: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            if status == "running":
                cur.execute(
                    """
                    UPDATE model_runs
                    SET status = 'running', started_at = now()
                    WHERE id = %s
                    """,
                    (job_id,),
                )
            elif status == "succeeded":
                cur.execute(
                    """
                    UPDATE model_runs
                    SET status = 'succeeded', finished_at = now()
                    WHERE id = %s
                    """,
                    (job_id,),
                )
            elif status == "failed":
                cur.execute(
                    """
                    UPDATE model_runs
                    SET status = 'failed', finished_at = now()
                    WHERE id = %s
                    """,
                    (job_id,),
                )
        conn.commit()


def load_run_context(job_id: UUID) -> tuple[UUID, dict]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT model_id, parameters
                FROM model_runs
                WHERE id = %s
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"model_run not found: {job_id}")
            return row[0], (row[1] or {})


def _coerce_bbox(parameters: dict) -> dict[str, float] | None:
    bbox = parameters.get("bbox")
    if not isinstance(bbox, dict):
        return None
    try:
        south = float(bbox["south"])
        west = float(bbox["west"])
        north = float(bbox["north"])
        east = float(bbox["east"])
    except Exception:
        return None
    return {"south": south, "west": west, "north": north, "east": east}


def resolve_dem_path(parameters: dict) -> tuple[str, str | None]:
    dem_file_path = parameters.get("dem_file_path")
    if dem_file_path:
        if not os.path.exists(dem_file_path):
            raise FileNotFoundError(f"dem_file_path not found: {dem_file_path}")
        return dem_file_path, None

    bbox = _coerce_bbox(parameters)
    if bbox:
        dem_source = str(parameters.get("dem_source") or "wcs").strip().lower()
        provider = str(parameters.get("provider") or "auto").strip().lower()
        tmp_dir = tempfile.mkdtemp(prefix="hydrowatch-dem-fetch-")

        if dem_source == "public":
            parts_raw = parameters.get("st_parts") or parameters.get("parts") or [1]
            parts: list[int] = []
            if isinstance(parts_raw, str):
                parts = [int(x) for x in parts_raw.split(",") if x.strip()]
            elif isinstance(parts_raw, list):
                parts = [int(x) for x in parts_raw]
            else:
                parts = [int(parts_raw)]

            cache_dir = parameters.get("dem_cache_dir")
            dem_path = fetch_dem_from_st_public_download(
                south=bbox["south"],
                west=bbox["west"],
                north=bbox["north"],
                east=bbox["east"],
                parts=parts,
                cache_dir=cache_dir,
            )
            final_path = os.path.join(tmp_dir, "dem.tif")
            shutil.move(dem_path, final_path)
            return final_path, tmp_dir

        if dem_source == "cog":
            cog_dir = parameters.get("st_cog_dir") or os.getenv("ST_COG_DIR")
            if not cog_dir:
                raise RuntimeError("dem_source=cog braucht st_cog_dir oder ST_COG_DIR.")
            cache_dir = parameters.get("dem_cache_dir")
            dem_path = fetch_dem_from_st_cog_dir(
                south=bbox["south"],
                west=bbox["west"],
                north=bbox["north"],
                east=bbox["east"],
                cog_dir=str(cog_dir),
                cache_dir=cache_dir,
            )
            final_path = os.path.join(tmp_dir, "dem.tif")
            shutil.move(dem_path, final_path)
            return final_path, tmp_dir

        dem_path = fetch_dem_from_wcs(
            bbox["south"],
            bbox["west"],
            bbox["north"],
            bbox["east"],
            provider_key=provider,
        )
        final_path = os.path.join(tmp_dir, "dem.tif")
        shutil.move(dem_path, final_path)
        return final_path, tmp_dir

    # Default fallback for local smoke/dev runs.
    tmp_dir = tempfile.mkdtemp(prefix="hydrowatch-dem-")
    dem_path = os.path.join(tmp_dir, "mock_dem.tif")
    create_mock_dem(dem_path)
    return dem_path, tmp_dir


def run_pysheds_compute(job_id: UUID, model_id: UUID, parameters: dict) -> dict:
    threshold = int(parameters.get("threshold", 200))
    analysis_type = str(parameters.get("analysis_type") or "starkregen").strip().lower()

    dem_path, cleanup_dir = resolve_dem_path(parameters)
    try:
        geojson = analyze_dem(dem_path, threshold=threshold, analysis_type=analysis_type)
    finally:
        if cleanup_dir and os.path.isdir(cleanup_dir):
            shutil.rmtree(cleanup_dir, ignore_errors=True)

    return {
        "job_id": str(job_id),
        "model_id": str(model_id),
        "summary": {
            "threshold": threshold,
            "analysis_type": analysis_type,
            "feature_count": len(geojson.get("features", [])),
            "engine": "pysheds",
            "source": "legacy_backend.processing.analyze_dem",
        },
        "geojson": geojson,
    }


def _s3_client():
    if not (S3_ENDPOINT_URL and S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY and S3_BUCKET):
        return None
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=os.getenv("AWS_REGION") or "us-east-1",
    )


def _ensure_bucket(client) -> None:
    if not client or not S3_BUCKET:
        return
    try:
        client.head_bucket(Bucket=S3_BUCKET)
    except ClientError as exc:
        code = str((exc.response or {}).get("Error", {}).get("Code") or "")
        if code in ("404", "NoSuchBucket", "NotFound"):
            client.create_bucket(Bucket=S3_BUCKET)
            return
        raise


def save_output(job_id: UUID, result: dict) -> None:
    payload: dict[str, Any] = result
    s3_key: str | None = None

    client = _s3_client()
    if client and S3_BUCKET:
        try:
            _ensure_bucket(client)
            s3_key = f"model_runs/{job_id}/result.json"
            body = json.dumps(result).encode("utf-8")
            client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=body,
                ContentType="application/json",
            )
            # Keep DB small; store only an index + core summary.
            payload = {
                "job_id": result.get("job_id"),
                "model_id": result.get("model_id"),
                "summary": result.get("summary") or {},
                "analysis": (result.get("geojson") or {}).get("analysis") or {},
                "s3_key": s3_key,
            }
        except Exception as exc:
            print(f"[compute-service] S3 upload failed, falling back to DB metadata: {exc}")
            s3_key = None
            payload = result

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO model_run_outputs (model_run_id, output_type, s3_key, metadata)
                VALUES (%s, 'flow_network_geojson', %s, %s::jsonb)
                """,
                (job_id, s3_key, json.dumps(payload)),
            )
        conn.commit()


def main() -> None:
    client = redis.from_url(REDIS_URL, decode_responses=True)
    print(f"[compute-service] listening on {REDIS_URL}, queue={QUEUE_KEY}")

    while True:
        item = client.blpop(QUEUE_KEY, timeout=10)
        if not item:
            continue

        _, payload = item
        try:
            job = json.loads(payload)
            job_id = UUID(job["job_id"])
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            print(f"[compute-service] invalid payload: {payload} ({exc})")
            continue

        try:
            print(f"[compute-service] processing job {job_id}")
            set_status(job_id, "running")

            model_id, parameters = load_run_context(job_id)
            result = run_pysheds_compute(job_id, model_id, parameters)
            save_output(job_id, result)

            set_status(job_id, "succeeded")
            print(f"[compute-service] done job {job_id}")
        except Exception as exc:
            set_status(job_id, "failed")
            print(f"[compute-service] failed job {job_id}: {exc}")


if __name__ == "__main__":
    main()
