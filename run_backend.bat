@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
set "REPO_ROOT=%~dp0"
REM Auto-detect ST local DEM/VRT and ST MWL layer defaults (if not explicitly configured).
if "%ST_DEM_LOCAL_PATH%"=="" (
  if exist "%REPO_ROOT%data\dem_cache\st_dgm1_cog\st_dgm1_cog.vrt" (
    set "ST_DEM_LOCAL_PATH=%REPO_ROOT%data\dem_cache\st_dgm1_cog\st_dgm1_cog.vrt"
  )
)
if "%SOIL_RASTER_PATH%"=="" (
  if exist "%REPO_ROOT%data\layers\st_mwl_erosion\K_Faktor.tif" (
    set "SOIL_RASTER_PATH=%REPO_ROOT%data\layers\st_mwl_erosion\K_Faktor.tif"
  )
)
if "%IMPERVIOUS_RASTER_PATH%"=="" (
  if exist "%REPO_ROOT%data\layers\st_mwl_erosion\Wasser_Erosion.tif" (
    set "IMPERVIOUS_RASTER_PATH=%REPO_ROOT%data\layers\st_mwl_erosion\Wasser_Erosion.tif"
  )
)
if "%ABAG_K_FACTOR_RASTER_PATH%"=="" (
  if exist "%REPO_ROOT%data\layers\st_mwl_erosion\K_Faktor.tif" (
    set "ABAG_K_FACTOR_RASTER_PATH=%REPO_ROOT%data\layers\st_mwl_erosion\K_Faktor.tif"
  )
)
if "%ABAG_R_FACTOR_RASTER_PATH%"=="" (
  if exist "%REPO_ROOT%data\layers\st_mwl_erosion\R_Faktor.tif" (
    set "ABAG_R_FACTOR_RASTER_PATH=%REPO_ROOT%data\layers\st_mwl_erosion\R_Faktor.tif"
  )
)
if "%ABAG_S_FACTOR_RASTER_PATH%"=="" (
  if exist "%REPO_ROOT%data\layers\st_mwl_erosion\S_Faktor.tif" (
    set "ABAG_S_FACTOR_RASTER_PATH=%REPO_ROOT%data\layers\st_mwl_erosion\S_Faktor.tif"
  )
)
if "%ABAG_C_FACTOR_RASTER_PATH%"=="" (
  if exist "%REPO_ROOT%data\layers\st_mwl_erosion\C_Faktor_proxy.tif" (
    set "ABAG_C_FACTOR_RASTER_PATH=%REPO_ROOT%data\layers\st_mwl_erosion\C_Faktor_proxy.tif"
  )
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
if "%BACKEND_RELOAD%"=="" set "BACKEND_RELOAD=0"
"%PYTHON_EXE%" -m pip install --quiet fastapi uvicorn python-multipart pysheds requests pyproj scikit-learn joblib 2>nul
echo.
echo === Starting Hydrowatch Backend on http://127.0.0.1:%BACKEND_PORT% ===
echo === Weather Provider: %WEATHER_PROVIDER% (transport=%ICON2D_TRANSPORT%) ===
echo === Radar Provider: %RADAR_PROVIDER% ===
echo === Backend Reload: %BACKEND_RELOAD% ===
if not "%RADAR_EVENTS_URL%"=="" echo === Radar Events URL (connector): %RADAR_EVENTS_URL% ===
if not "%ST_DEM_LOCAL_PATH%"=="" echo === ST_DEM_LOCAL_PATH: %ST_DEM_LOCAL_PATH% ===
if not "%SOIL_RASTER_PATH%"=="" echo === SOIL_RASTER_PATH: %SOIL_RASTER_PATH% ===
if not "%IMPERVIOUS_RASTER_PATH%"=="" echo === IMPERVIOUS_RASTER_PATH: %IMPERVIOUS_RASTER_PATH% ===
if not "%ABAG_K_FACTOR_RASTER_PATH%"=="" echo === ABAG_K_FACTOR_RASTER_PATH: %ABAG_K_FACTOR_RASTER_PATH% ===
if not "%ABAG_R_FACTOR_RASTER_PATH%"=="" echo === ABAG_R_FACTOR_RASTER_PATH: %ABAG_R_FACTOR_RASTER_PATH% ===
if not "%ABAG_S_FACTOR_RASTER_PATH%"=="" echo === ABAG_S_FACTOR_RASTER_PATH: %ABAG_S_FACTOR_RASTER_PATH% ===
if not "%ABAG_C_FACTOR_RASTER_PATH%"=="" echo === ABAG_C_FACTOR_RASTER_PATH: %ABAG_C_FACTOR_RASTER_PATH% ===
echo === Python: %PYTHON_EXE% ===
echo.
if "%BACKEND_WORKERS%"=="" set "BACKEND_WORKERS=3"
echo === Workers: %BACKEND_WORKERS% ===
if "%BACKEND_RELOAD%"=="1" (
  "%PYTHON_EXE%" -m uvicorn main:app --reload --host 127.0.0.1 --port %BACKEND_PORT%
) else (
  "%PYTHON_EXE%" -m uvicorn main:app --host 127.0.0.1 --port %BACKEND_PORT% --workers %BACKEND_WORKERS%
)
