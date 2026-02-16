# Hydrowatch Berlin MVP

A Micro-SaaS starter for flood and erosion analysis on DEM data.

## Compose Split (Dev vs Prod)
- `docker-compose.dev.yml`: local UX/dev loop (mounted source, reload, quick iteration).
- `docker-compose.prod.yml`: deployment-oriented stack (built images, env-driven secrets, restart policies).

## Local UX Workflow (Dev)
Start dev stack:
```bash
docker compose -f docker-compose.dev.yml up -d
```

Dev ports:
- Legacy backend (non-docker): `http://127.0.0.1:8001` (used by `start_all.bat`)
- Job API (docker dev stack): `http://127.0.0.1:8002`

Run automated smoke test:
```bat
smoke_test.bat
```

Stop dev stack:
```bat
smoke_test_down.bat
```

Notes:
- First run can take 1-3 minutes because compute-service installs heavy geospatial dependencies.
- `smoke_test.bat` seeds demo rows, creates a job, and waits for `succeeded`/`failed`.

## Production Prep (IONOS later)
1. Copy `.env.example` to `.env` and set strong secrets.
2. Build and start:
```bash
docker compose --env-file .env -f docker-compose.prod.yml up -d --build
```
3. Check service status:
```bash
docker compose --env-file .env -f docker-compose.prod.yml ps
```

## Service Paths
- API app: `services/api-service/app/main.py`
- Worker app: `services/compute-service/app/worker.py`
- Legacy processing currently used by worker: `backend/processing.py`
- DB schema init: `infra/postgres/init/001_init.sql`

## Job Parameters (Classic "Klassiker")
The compute worker supports two DEM input modes via `model_runs.parameters`:
- Local file: `dem_file_path` (absolute path inside container/host)
- BBox fetch: `bbox: { south, west, north, east }` (WGS84)

Optional parameters:
- `threshold` (int, default `200`)
- `provider` (`auto`, `nrw`, `sachsen-anhalt`)
- `dem_source` (`wcs` or `public` for Sachsen-Anhalt DGM1 ZIP fallback)
- `st_parts` (list or comma string, e.g. `[1,2]` or `"1,2"`)
- `dem_cache_dir` (optional cache folder for public downloads)

Outputs:
- `model_run_outputs.s3_key` is used when S3/MinIO is configured; otherwise result JSON is stored in `metadata`.
- API helper endpoints exist to fetch output content from S3/DB:
  - `GET /v1/jobs/{job_id}/outputs/{output_id}/content`
  - `GET /v1/jobs/{job_id}/outputs/latest/content?output_type=flow_network_geojson`

## UX Checklist
Use `CHECKLIST_UX.md` before merges that affect frontend/API behavior.

## Frontend API Mode
Vite env vars (optional):
- `VITE_API_MODE=legacy` or `VITE_API_MODE=jobs`
- `VITE_LEGACY_API_URL` (default `http://127.0.0.1:8001`)
- `VITE_JOB_API_URL` (default `http://127.0.0.1:8002`)
- `VITE_DEMO_PROJECT_ID`, `VITE_DEMO_MODEL_ID` (defaults are the demo seed IDs)

## Weather Provider Mode
Backend env vars (optional):
- `WEATHER_PROVIDER=auto|icon2d|dwd` (default `auto`)
- `ICON2D_BASE_URL` (required for `icon2d`, optional for `auto`)
- `ICON2D_BATCH_PATH` (default `/weather/batch`)
- `ICON2D_TIMEOUT_S` (default `45`)

Behavior:
- `auto`: tries icon2d first (if configured), then falls back to DWD station-based data.
- `icon2d`: uses only icon2d backend (fails if unavailable).
- `dwd`: uses only DWD station-based data.

## Disclaimer
Dies ist eine indikative Analyse basierend auf Topographiedaten. Keine rechtsverbindliche Hochwasservorsorge.

## Hilfe & Methodik
- In-App Hilfe: Sidebar -> "Hilfe & Methodik"
- Ausfuehrliche Doku: `HILFE_WISSENSCHAFT.md`
- Roadmap (geordnet): `ROADMAP.md`

## WCS Large-Area Behavior
- Adaptive WCS tiling is enabled for large AOIs (<=5km requests per tile, auto-splitting on proxy parameter errors).
- Tiles are merged into one DEM before analysis.
- Very large AOIs may take longer due to multiple WCS requests.

## Sachsen-Anhalt (WCS Fallback)
The official Sachsen-Anhalt OpenData WCS can respond with HTTP 500 on `GetCoverage` even though
`GetCapabilities`/`DescribeCoverage` work. In that case, use the official DGM1 download (GeoTIFF ZIP)
and configure a local fallback DEM for BBox clipping:
- Download: `download_st_dgm1.bat 1` .. `download_st_dgm1.bat 4` (very large ZIPs)
- Build a mosaic VRT: `build_st_dgm1_vrt.bat 1` .. `build_st_dgm1_vrt.bat 4`
- Set `ST_DEM_LOCAL_PATH` to the resulting `.vrt` (see `.env.example`)

## Sachsen-Anhalt (Local COG Catalog)
If you already have Sachsen-Anhalt DGM1 tiles as Cloud Optimized GeoTIFFs in a folder (e.g. `*_cog.tif`),
you can clip DEMs per BBox without using the unstable WCS:

- Put COG tiles in a folder on the host, e.g. `D:\data\st_dgm1_cog`
- For Docker dev: `docker-compose.dev.yml` mounts this folder read-only into `compute-service` at `/data/st_dgm1_cog`
- Set `ST_COG_DIR` (container path) and run jobs with `parameters.dem_source="cog"`

The worker will build a cached VRT mosaic (under `DEM_CACHE_DIR`/`data/dem_cache`) and then window-read the BBox.

### In-App Public Download (Sachsen-Anhalt, Dev only)
There is a public DGM1 ZIP download fallback (very large), but it is intentionally **Dev-only** (`?dev=1` in the UI).

This will (server-side):
1. Download selected DGM1 ZIP part(s) into `data/dem_cache/st_dgm1`
2. Extract the ZIP(s)
3. Build a `.vrt` mosaic with `gdalbuildvrt`
4. Clip the mosaic to your AOI and run the analysis

You can override the cache folder via the UI ("Download-Ordner") or by setting `DEM_CACHE_DIR` in `.env`.

## Risk Layers (Score v2)
Optional external layers:
- `SOIL_RASTER_PATH` / `SOIL_RASTER_URL`
- `IMPERVIOUS_RASTER_PATH` / `IMPERVIOUS_RASTER_URL`

If local files are missing and URLs are configured, layers are auto-fetched and cached.

Bootstrap once manually:
```bat
run_layer_bootstrap.bat
```

### BK50 to Soil Raster
BK50 comes as GeoPackage and must be rasterized before use as `SOIL_RASTER_PATH`.

Inspect layers/fields:
```bat
run_soil_bk50_converter.bat --input-gpkg C:\data\ISBK50.gpkg --list-only
```

Convert with chosen numeric field:
```bat
run_soil_bk50_converter.bat --input-gpkg C:\data\ISBK50.gpkg --layer <LAYER> --value-field <NUMERIC_FIELD> --output-tif C:\data\nrw_soil.tif
```

