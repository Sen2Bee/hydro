@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === Paper Prep: Artifact Manifest ===
echo Python: %PYTHON_EXE%
echo.
"%PYTHON_EXE%" backend\generate_paper_artifact_manifest.py %*
if errorlevel 1 exit /b 1
echo.
echo Fertig: paper\manifest\paper_artifact_manifest.json
endlocal
