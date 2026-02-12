# Hydrowatch Berlin MVP

A Micro-SaaS tool for flood risk analysis using flow accumulation on Digital Elevation Models (DEM).

## Current MVP Structure
- `backend/`: FastAPI application with PySheds for hydrological analysis.
- `frontend/`: React application with Leaflet for visualization.

## New v1 Service Scaffold
- `docker-compose.yml`: local multi-service runtime.
- `services/api-service/`: API gateway starter (health + job creation/status).
- `services/compute-service/`: worker starter (queue consumer + status updates).
- `infra/postgres/init/001_init.sql`: initial PostGIS schema for tenant/project/model-run data.

## Local Stack (v1)
1. PostGIS (`postgres`)
2. Redis (`redis`)
3. MinIO (`minio`)
4. API service (`api-service`, port `8001`)
5. Compute worker (`compute-service`)

Start command:
```bash
docker compose up -d
```

Stop command:
```bash
docker compose down
```

## v1 API Endpoints (Scaffold)
- `GET /health`
- `POST /v1/jobs`
  - Body:
    ```json
    {
      "project_id": "<uuid>",
      "model_id": "<uuid>",
      "parameters": {
        "threshold": 200
      }
    }
    ```
  - Creates a `model_runs` record with status `queued` and pushes a queue item to Redis.
- `GET /v1/jobs/{job_id}`
  - Returns current status (`queued`, `running`, `succeeded`, `failed`) from Postgres.

## Existing MVP Setup
### Backend
1. Initialize environment (Python 3.9+).
2. Install dependencies:
   ```bash
   pip install -r backend/requirements.txt
   ```
3. Run server:
   ```bash
   run_backend.bat
   ```

### Frontend
1. Install dependencies:
   ```bash
   cd frontend
   npm install
   ```
2. Run development server:
   ```bash
   ../run_frontend.bat
   ```

## Disclaimer
Dies ist eine indikative Analyse basierend auf Topographiedaten. Keine rechtsverbindliche Hochwasservorsorge.
