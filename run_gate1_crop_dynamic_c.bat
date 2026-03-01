@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0

echo === Gate-1: Crop History + Dynamic C Readiness ===
"%PYTHON_EXE%" -u backend\run_gate1_crop_dynamic_c.py %*
exit /b %ERRORLEVEL%

