@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === Build Paper Assets (Summary/Diagramm-Tabellen) ===
echo Python: %PYTHON_EXE%
echo.
"%PYTHON_EXE%" backend\build_paper_assets.py %*
endlocal
