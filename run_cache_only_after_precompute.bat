@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === Cache-only After Precompute ===
echo Python: %PYTHON_EXE%
echo.
"%PYTHON_EXE%" -u backend\run_cache_only_after_precompute.py %*
endlocal
