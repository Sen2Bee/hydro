@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === SA Flurstueck-Polygone (ALKIS) holen ===
echo Python: %PYTHON_EXE%
echo Beispiel:
echo   run_fetch_sa_flurstuecke.bat --west 11.998 --south 51.458 --east 12.002 --north 51.460 --out-geojson paper\input\schlaege.geojson
echo.
if "%~1"=="" (
  "%PYTHON_EXE%" backend\fetch_sa_flurstuecke.py --help
  exit /b 0
)
"%PYTHON_EXE%" backend\fetch_sa_flurstuecke.py %*
endlocal
