@echo off
call C:\OSGeo4W\bin\o4w_env.bat
cd /d %~dp0backend
REM Convenience: auto-detect local ST COG directory for Sachsen-Anhalt if not configured.
if "%ST_COG_DIR%"=="" (
  if exist D:\data\st_dgm1_cog (
    set "ST_COG_DIR=D:\data\st_dgm1_cog"
  )
)
python -m pip install --quiet fastapi uvicorn python-multipart pysheds requests 2>nul
echo.
echo === Starting Hydrowatch Backend on http://127.0.0.1:8001 ===
echo.
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8001
