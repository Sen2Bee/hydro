import json
import os
import time
from uuid import UUID

import psycopg
import redis


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydrowatch:hydrowatch@localhost:5432/hydrowatch"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = os.getenv("QUEUE_KEY", "hydrowatch:jobs")


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

            # Placeholder for actual hydrology/erosion run.
            time.sleep(2)

            set_status(job_id, "succeeded")
            print(f"[compute-service] done job {job_id}")
        except Exception as exc:
            set_status(job_id, "failed")
            print(f"[compute-service] failed job {job_id}: {exc}")


if __name__ == "__main__":
    main()
