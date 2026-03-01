@echo off
setlocal
cd /d "%~dp0"

call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)

echo.
echo === Build Acker Whitelist aus Crop-Rastern ===
echo Python: %PYTHON_EXE%
echo.

"%PYTHON_EXE%" -u backend\build_acker_whitelist_from_crop_rasters.py %*
exit /b %ERRORLEVEL%

