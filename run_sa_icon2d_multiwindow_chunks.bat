@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0

if "%~1"=="" (
  echo.
  echo === SA Auto-Events Multi-Window Chunks ===
  echo Python: %PYTHON_EXE%
  echo Default Source: hybrid_radar
  echo Default: 50 Chunks x 1000 Felder, 3 Fenster (2023/2024/2025 Apr-Oct)
  echo Rate-limit-safe Defaults: retries=6, backoff=5..90s, min-interval=1.5s
  echo Upgrade auf 100 Chunks: --max-chunks 100
  echo.
  "%PYTHON_EXE%" -u backend\run_sa_icon2d_multiwindow_chunks.py --chunk-size 1000 --start-chunk 1 --max-chunks 50 --events-auto-source hybrid_radar --events-auto-request-retries 6 --events-auto-retry-backoff-initial-s 5 --events-auto-retry-backoff-max-s 90 --events-auto-min-interval-s 1.5
  exit /b %ERRORLEVEL%
)

echo.
echo === SA Auto-Events Multi-Window Chunks ===
echo Python: %PYTHON_EXE%
echo.
"%PYTHON_EXE%" -u backend\run_sa_icon2d_multiwindow_chunks.py %*
exit /b %ERRORLEVEL%
