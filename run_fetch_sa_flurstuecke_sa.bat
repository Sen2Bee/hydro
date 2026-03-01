@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === SA-weiter Flurstueck Download (Tiles + Merge) ===
echo Python: %PYTHON_EXE%
echo.
echo Standard:
echo   SA-Default-Extent, tiled Download, SQLite-Deduplikation, Merge + .geojson.gz
echo Test mit eigener BBox:
echo   run_fetch_sa_flurstuecke_sa.bat --no-sa-default-extent --west 11.998 --south 51.458 --east 12.020 --north 51.470 --tile-size-deg 0.02
echo.
if "%~1"=="" (
  "%PYTHON_EXE%" -u backend\fetch_sa_flurstuecke_tiled.py
  exit /b %ERRORLEVEL%
)
"%PYTHON_EXE%" -u backend\fetch_sa_flurstuecke_tiled.py %*
endlocal
