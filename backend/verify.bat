@echo off
call "%~dp0..\resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
"%PYTHON_EXE%" verify_sachsen_integration.py > verification.log 2>&1
type verification.log
