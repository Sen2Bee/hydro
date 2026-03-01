@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === Precompute Auto-Events Cache ===
echo Python: %PYTHON_EXE%
echo.
"%PYTHON_EXE%" -u backend\precompute_auto_events_cache.py %*
endlocal
