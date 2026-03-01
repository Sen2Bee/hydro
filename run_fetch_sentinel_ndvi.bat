@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
set "LOG_DIR=%~dp0data\layers\st_mwl_erosion\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\c_pipeline.log"
echo.
echo === Sentinel-2 NDVI Fetch (Earth Search STAC) ===
echo.
echo Beispiel-BBox (Halle Umgebung):
echo   west=11.80 south=51.40 east=12.10 north=51.60
echo.
echo Beispiel-Zeitraum:
echo   start=2025-04-01 end=2025-09-30
echo.
echo [%date% %time%] run_fetch_sentinel_ndvi.bat %*>>"%LOG_FILE%"
"%PYTHON_EXE%" backend\fetch_sentinel_ndvi.py --west 11.80 --south 51.40 --east 12.10 --north 51.60 --start 2025-04-01 --end 2025-09-30 %*
set "ERR=%ERRORLEVEL%"
if "%ERR%"=="0" (
  echo [%date% %time%] NDVI_OK>>"%LOG_FILE%"
) else (
  echo [%date% %time%] NDVI_FAIL errorlevel=%ERR%>>"%LOG_FILE%"
)
exit /b %ERR%
