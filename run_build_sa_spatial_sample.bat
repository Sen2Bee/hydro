@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === Build SA Spatial Sample ===
echo Python: %PYTHON_EXE%
echo.
if "%~1"=="" (
  "%PYTHON_EXE%" -u backend\build_sa_spatial_sample.py
  exit /b %ERRORLEVEL%
)
"%PYTHON_EXE%" -u backend\build_sa_spatial_sample.py %*
endlocal
