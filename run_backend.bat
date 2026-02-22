@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0backend
REM Convenience: auto-detect local ST COG directory for Sachsen-Anhalt if not configured.
if "%ST_COG_DIR%"=="" (
  if exist D:\data\st_dgm1_cog (
    set "ST_COG_DIR=D:\data\st_dgm1_cog"
  )
)
REM Weather provider defaults: use ICON2D first-class, not silent DWD fallback.
if "%WEATHER_PROVIDER%"=="" set "WEATHER_PROVIDER=icon2d"
if "%ICON2D_TRANSPORT%"=="" set "ICON2D_TRANSPORT=direct"
if "%ICON2D_BATCH_PATH%"=="" set "ICON2D_BATCH_PATH=/weather/batch"
if "%RADAR_PROVIDER%"=="" set "RADAR_PROVIDER=dwd_radolan"
if "%RADAR_MAX_HOURS%"=="" set "RADAR_MAX_HOURS=4320"
if "%BACKEND_PORT%"=="" set "BACKEND_PORT=8001"
"%PYTHON_EXE%" -m pip install --quiet fastapi uvicorn python-multipart pysheds requests pyproj 2>nul
echo.
echo === Starting Hydrowatch Backend on http://127.0.0.1:%BACKEND_PORT% ===
echo === Weather Provider: %WEATHER_PROVIDER% (transport=%ICON2D_TRANSPORT%) ===
echo === Radar Provider: %RADAR_PROVIDER% ===
if not "%RADAR_EVENTS_URL%"=="" echo === Radar Events URL (connector): %RADAR_EVENTS_URL% ===
echo === Python: %PYTHON_EXE% ===
echo.
"%PYTHON_EXE%" -m uvicorn main:app --reload --host 127.0.0.1 --port %BACKEND_PORT%
