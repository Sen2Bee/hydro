@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === Field x Event Batch Export ===
echo Python: %PYTHON_EXE%
echo Hinweis: Backend muss laufen (Standard: http://127.0.0.1:8001).
echo.
"%PYTHON_EXE%" backend\run_field_event_batch.py %*
endlocal
