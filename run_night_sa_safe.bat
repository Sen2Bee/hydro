@echo off
setlocal
cd /d "%~dp0"

call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)

echo.
echo === Safe Nachtlauf SA (ICON2D) ===
echo Python: %PYTHON_EXE%
echo Basis: D:\__GeoFlux\hydrowatch
echo Defaults: 100 Chunks, Resume aktiv, QA aktiv, Backend-Autostart aktiv
echo Filter: min-field-area-ha=0.05 (sehr kleine/unplausible Flaechen raus)
echo.

"%PYTHON_EXE%" -u backend\start_night_run_safe.py ^
  --chunk-size 1000 ^
  --start-chunk 1 ^
  --max-chunks 100 ^
  --checkpoint-every 100 ^
  --events-auto-top-n 3 ^
  --events-auto-min-severity 1 ^
  --analysis-modes erosion_events_ml,abag ^
  --provider auto ^
  --dem-source cog ^
  --threshold 200 ^
  --min-field-area-ha 0.05 ^
  --min-free-gb 20 ^
  --require-tiles-ready ^
  --backend-autostart ^
  --backend-wait-sec 180 ^
  %*

exit /b %ERRORLEVEL%
