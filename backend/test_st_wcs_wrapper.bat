@echo off
echo Starting test... > test_output.txt
call "%~dp0..\resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden. >> test_output.txt
  exit /b 1
)
"%PYTHON_EXE%" -u test_st_wcs.py >> test_output.txt 2>&1
echo Done. >> test_output.txt
