@echo off
setlocal
cd /d "%~dp0"

call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)

echo.
echo === Full Auto Pipeline ===
echo 1) SA tiles bis vollstaendig
echo 2) danach safe Nachtlauf automatisch starten
echo.

"%PYTHON_EXE%" -u backend\run_full_auto_pipeline.py %*
exit /b %ERRORLEVEL%
