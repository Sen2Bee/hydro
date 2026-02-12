import json
import os
import shutil
import tempfile
import traceback

from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from processing import analyze_dem
from wcs_client import fetch_dem_from_wcs, WCSError, BboxTooLargeError

app = FastAPI(title="Hydrowatch Berlin API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class BboxRequest(BaseModel):
    south: float
    west: float
    north: float
    east: float


@app.get("/")
def root():
    return {"status": "ok", "message": "Hydrowatch Berlin API"}


def _stream_analysis(tmp_path: str, threshold: int, label: str,
                     pre_step: str | None = None, total_steps: int = 8):
    """
    Generator that yields NDJSON lines: progress events + final result.

    Each line is a JSON object:
      {"type":"progress","step":1,"total":8,"message":"..."}
      {"type":"result","data":{...geojson...}}
      {"type":"error","detail":"..."}
    """
    try:
        if pre_step:
            yield json.dumps({"type": "progress", "step": 0,
                              "total": total_steps,
                              "message": pre_step}) + "\n"

        def on_progress(step, total, msg):
            pass  # we collect steps differently

        # We need to stream progress from inside analyze_dem.
        # Use a list to collect emitted lines, then yield after each step.
        # Since analyze_dem is synchronous, we use a callback that appends.
        progress_lines = []

        def progress_cb(step, total, msg):
            # Offset step if we had a pre-step (like WCS download)
            actual_step = step + (1 if pre_step else 0)
            actual_total = total_steps
            progress_lines.append(
                json.dumps({"type": "progress", "step": actual_step,
                            "total": actual_total, "message": msg})
            )

        print(f"[INFO] Processing: {label} (threshold={threshold})")
        result = analyze_dem(tmp_path, threshold=threshold,
                             progress_callback=progress_cb)

        # Yield all collected progress lines
        for line in progress_lines:
            yield line + "\n"

        n = len(result.get("features", []))
        print(f"[INFO] Success – {n} features")

        yield json.dumps({"type": "result", "data": result}) + "\n"

    except Exception as exc:
        tb = traceback.format_exc()
        print(f"[ERROR] Processing failed:\n{tb}")
        yield json.dumps({"type": "error",
                           "detail": str(exc)}) + "\n"

    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


@app.post("/analyze")
async def analyze_endpoint(
    file: UploadFile = File(...),
    threshold: int = Query(200, ge=10, le=5000),
):
    """Accept a GeoTIFF DEM, return streamed progress + GeoJSON."""

    suffix = os.path.splitext(file.filename or ".tif")[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    return StreamingResponse(
        _stream_analysis(tmp_path, threshold,
                         label=file.filename or "upload",
                         total_steps=7),
        media_type="application/x-ndjson",
    )


@app.post("/analyze-bbox")
async def analyze_bbox_endpoint(
    bbox: BboxRequest,
    threshold: int = Query(200, ge=10, le=5000),
):
    """Fetch DEM from WCS and return streamed progress + GeoJSON."""

    try:
        # Step 0: WCS download (before analysis pipeline)
        print(f"[INFO] Bbox request: {bbox} (threshold={threshold})")

        # We can't easily stream the WCS download progress,
        # so we do it before starting the streaming response.
        tmp_path = fetch_dem_from_wcs(bbox.south, bbox.west,
                                      bbox.north, bbox.east)

    except BboxTooLargeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except WCSError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    except Exception as exc:
        tb = traceback.format_exc()
        print(f"[ERROR] WCS download failed:\n{tb}")
        raise HTTPException(status_code=500, detail=str(exc))

    return StreamingResponse(
        _stream_analysis(tmp_path, threshold,
                         label=f"bbox {bbox.south:.3f},{bbox.west:.3f}",
                         pre_step="DGM wurde vom WCS geladen ✓",
                         total_steps=8),
        media_type="application/x-ndjson",
    )


