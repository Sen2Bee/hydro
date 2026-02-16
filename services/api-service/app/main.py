import json
import os
from datetime import datetime
from uuid import UUID

import boto3
import psycopg
import redis
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


app = FastAPI(title="Hydrowatch API Service", version="0.1.0")

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydrowatch:hydrowatch@localhost:5432/hydrowatch"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = os.getenv("QUEUE_KEY", "hydrowatch:jobs")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")


class Health(BaseModel):
    status: str
    service: str
    timestamp: str


class CreateJobRequest(BaseModel):
    project_id: UUID
    model_id: UUID
    parameters: dict = Field(default_factory=dict)


class TenantResponse(BaseModel):
    id: UUID
    name: str
    slug: str
    created_at: str


class ProjectCreateRequest(BaseModel):
    tenant_id: UUID
    name: str = Field(min_length=1, max_length=120)
    description: str | None = None


class ProjectResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    name: str
    description: str | None
    created_at: str


class ModelCreateRequest(BaseModel):
    key: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=120)
    category: str = Field(min_length=1, max_length=80)
    description: str | None = None


class ModelResponse(BaseModel):
    id: UUID
    key: str
    name: str
    category: str
    description: str | None
    created_at: str


class JobResponse(BaseModel):
    id: UUID
    project_id: UUID
    model_id: UUID
    status: str
    parameters: dict
    started_at: str | None
    finished_at: str | None
    created_at: str


class JobOutputResponse(BaseModel):
    id: UUID
    model_run_id: UUID
    output_type: str
    s3_key: str | None
    metadata: dict
    created_at: str


def _dt(value):
    return value.isoformat() if value else None


def _fetch_tenants() -> list[TenantResponse]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, slug, created_at
                FROM tenants
                ORDER BY created_at ASC
                """
            )
            rows = cur.fetchall()
            return [
                TenantResponse(
                    id=row[0],
                    name=row[1],
                    slug=row[2],
                    created_at=_dt(row[3]),
                )
                for row in rows
            ]


def _fetch_projects(tenant_id: UUID | None = None) -> list[ProjectResponse]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            if tenant_id:
                cur.execute(
                    """
                    SELECT id, tenant_id, name, description, created_at
                    FROM projects
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC
                    """,
                    (tenant_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, tenant_id, name, description, created_at
                    FROM projects
                    ORDER BY created_at DESC
                    """
                )
            rows = cur.fetchall()
            return [
                ProjectResponse(
                    id=row[0],
                    tenant_id=row[1],
                    name=row[2],
                    description=row[3],
                    created_at=_dt(row[4]),
                )
                for row in rows
            ]


def _create_project(payload: ProjectCreateRequest) -> ProjectResponse:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO projects (tenant_id, name, description)
                VALUES (%s, %s, %s)
                RETURNING id, tenant_id, name, description, created_at
                """,
                (payload.tenant_id, payload.name, payload.description),
            )
            row = cur.fetchone()
        conn.commit()
    return ProjectResponse(
        id=row[0],
        tenant_id=row[1],
        name=row[2],
        description=row[3],
        created_at=_dt(row[4]),
    )


def _fetch_models() -> list[ModelResponse]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, key, name, category, description, created_at
                FROM models
                ORDER BY created_at DESC
                """
            )
            rows = cur.fetchall()
            return [
                ModelResponse(
                    id=row[0],
                    key=row[1],
                    name=row[2],
                    category=row[3],
                    description=row[4],
                    created_at=_dt(row[5]),
                )
                for row in rows
            ]


def _create_model(payload: ModelCreateRequest) -> ModelResponse:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO models (key, name, category, description)
                VALUES (%s, %s, %s, %s)
                RETURNING id, key, name, category, description, created_at
                """,
                (payload.key, payload.name, payload.category, payload.description),
            )
            row = cur.fetchone()
        conn.commit()
    return ModelResponse(
        id=row[0],
        key=row[1],
        name=row[2],
        category=row[3],
        description=row[4],
        created_at=_dt(row[5]),
    )


def _fetch_job(job_id: UUID) -> JobResponse | None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, project_id, model_id, status, parameters,
                       started_at, finished_at, created_at
                FROM model_runs
                WHERE id = %s
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return JobResponse(
                id=row[0],
                project_id=row[1],
                model_id=row[2],
                status=row[3],
                parameters=row[4] or {},
                started_at=_dt(row[5]),
                finished_at=_dt(row[6]),
                created_at=_dt(row[7]),
            )


def _fetch_outputs(job_id: UUID) -> list[JobOutputResponse]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, model_run_id, output_type, s3_key, metadata, created_at
                FROM model_run_outputs
                WHERE model_run_id = %s
                ORDER BY created_at ASC
                """,
                (job_id,),
            )
            rows = cur.fetchall()
            return [
                JobOutputResponse(
                    id=row[0],
                    model_run_id=row[1],
                    output_type=row[2],
                    s3_key=row[3],
                    metadata=row[4] or {},
                    created_at=_dt(row[5]),
                )
                for row in rows
            ]


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


def _load_output_row(job_id: UUID, output_id: UUID) -> tuple[str, str | None, dict]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT output_type, s3_key, metadata
                FROM model_run_outputs
                WHERE id = %s AND model_run_id = %s
                """,
                (output_id, job_id),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Output not found")
            return row[0], row[1], row[2] or {}


def _load_latest_output_row(job_id: UUID, output_type: str) -> tuple[UUID, str | None, dict]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, s3_key, metadata
                FROM model_run_outputs
                WHERE model_run_id = %s AND output_type = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (job_id, output_type),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="No outputs for this job/output_type")
            return row[0], row[1], row[2] or {}


@app.get("/health", response_model=Health)
def health() -> Health:
    return Health(
        status="ok",
        service="api-service",
        timestamp=datetime.utcnow().isoformat() + "Z",
    )


@app.get("/v1/tenants", response_model=list[TenantResponse])
def list_tenants() -> list[TenantResponse]:
    return _fetch_tenants()


@app.get("/v1/projects", response_model=list[ProjectResponse])
def list_projects(tenant_id: UUID | None = None) -> list[ProjectResponse]:
    return _fetch_projects(tenant_id=tenant_id)


@app.post("/v1/projects", response_model=ProjectResponse, status_code=201)
def create_project(payload: ProjectCreateRequest) -> ProjectResponse:
    return _create_project(payload)


@app.get("/v1/models", response_model=list[ModelResponse])
def list_models() -> list[ModelResponse]:
    return _fetch_models()


@app.post("/v1/models", response_model=ModelResponse, status_code=201)
def create_model(payload: ModelCreateRequest) -> ModelResponse:
    try:
        return _create_model(payload)
    except Exception as exc:
        msg = str(exc)
        if "duplicate key" in msg.lower() or "unique" in msg.lower():
            raise HTTPException(status_code=409, detail="Model key already exists")
        raise


@app.post("/v1/jobs", response_model=JobResponse, status_code=201)
def create_job(payload: CreateJobRequest) -> JobResponse:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO model_runs (project_id, model_id, status, parameters)
                VALUES (%s, %s, 'queued', %s::jsonb)
                RETURNING id
                """,
                (payload.project_id, payload.model_id, json.dumps(payload.parameters)),
            )
            job_id = cur.fetchone()[0]
        conn.commit()

    queue_client = redis.from_url(REDIS_URL, decode_responses=True)
    queue_client.rpush(
        QUEUE_KEY,
        json.dumps(
            {
                "job_id": str(job_id),
                "project_id": str(payload.project_id),
                "model_id": str(payload.model_id),
            }
        ),
    )

    job = _fetch_job(job_id)
    if not job:
        raise HTTPException(status_code=500, detail="Job could not be loaded after create.")
    return job


@app.get("/v1/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: UUID) -> JobResponse:
    job = _fetch_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/v1/jobs/{job_id}/results", response_model=list[JobOutputResponse])
def get_job_results(job_id: UUID) -> list[JobOutputResponse]:
    job = _fetch_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return _fetch_outputs(job_id)


@app.get("/v1/jobs/{job_id}/outputs/{output_id}/content")
def get_job_output_content(job_id: UUID, output_id: UUID) -> dict:
    _ = _fetch_job(job_id)
    if not _:
        raise HTTPException(status_code=404, detail="Job not found")

    output_type, s3_key, metadata = _load_output_row(job_id, output_id)

    if s3_key:
        client = _s3_client()
        if not client or not S3_BUCKET:
            raise HTTPException(status_code=500, detail="S3 not configured on api-service")
        try:
            obj = client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            raw = obj["Body"].read()
            return json.loads(raw.decode("utf-8"))
        except ClientError as exc:
            raise HTTPException(status_code=502, detail=f"S3 read failed: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"S3 decode failed: {exc}")

    # Backwards-compatible fallback: payload stored in DB metadata.
    return {"output_type": output_type, "data": metadata}


@app.get("/v1/jobs/{job_id}/outputs/latest/content")
def get_latest_output_content(job_id: UUID, output_type: str) -> dict:
    _ = _fetch_job(job_id)
    if not _:
        raise HTTPException(status_code=404, detail="Job not found")

    output_id, s3_key, metadata = _load_latest_output_row(job_id, output_type)
    if s3_key:
        client = _s3_client()
        if not client or not S3_BUCKET:
            raise HTTPException(status_code=500, detail="S3 not configured on api-service")
        try:
            obj = client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            raw = obj["Body"].read()
            return json.loads(raw.decode("utf-8"))
        except ClientError as exc:
            raise HTTPException(status_code=502, detail=f"S3 read failed: {exc}")
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"S3 decode failed: {exc}")

    return {"output_id": str(output_id), "output_type": output_type, "data": metadata}
