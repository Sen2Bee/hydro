import json
import os
from datetime import datetime
from uuid import UUID

import psycopg
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


app = FastAPI(title="Hydrowatch API Service", version="0.1.0")

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydrowatch:hydrowatch@localhost:5432/hydrowatch"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = os.getenv("QUEUE_KEY", "hydrowatch:jobs")


class Health(BaseModel):
    status: str
    service: str
    timestamp: str


class CreateJobRequest(BaseModel):
    project_id: UUID
    model_id: UUID
    parameters: dict = Field(default_factory=dict)


class JobResponse(BaseModel):
    id: UUID
    project_id: UUID
    model_id: UUID
    status: str
    parameters: dict
    started_at: str | None
    finished_at: str | None
    created_at: str


def _dt(value):
    return value.isoformat() if value else None


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


@app.get("/health", response_model=Health)
def health() -> Health:
    return Health(
        status="ok",
        service="api-service",
        timestamp=datetime.utcnow().isoformat() + "Z",
    )


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
