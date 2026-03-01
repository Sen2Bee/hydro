@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0backend
echo.
echo === Event-ML Bundle Training ===
echo Python: %PYTHON_EXE%
echo.
"%PYTHON_EXE%" train_event_ml_bundle.py %*
if errorlevel 1 exit /b 1
echo.
echo === Event-ML Artifact Validation ===
"%PYTHON_EXE%" validate_event_ml_artifacts.py
if errorlevel 1 exit /b 1
echo.
echo Fertig. Siehe backend\models\event_ml\event-ml-bundle.manifest.json
endlocal
