@echo off
@echo off
call "%~dp0..\resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
"%PYTHON_EXE%" test_st_wcs.py > result.txt
