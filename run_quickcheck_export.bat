@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === Quick-Check Export (Top10/Karten/Bericht) ===
echo Python: %PYTHON_EXE%
echo.
"%PYTHON_EXE%" backend\export_quickcheck_package.py %*
endlocal
