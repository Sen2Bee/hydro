@echo off
setlocal
cd /d "%~dp0"

if "%~1"=="" (
  echo Usage:
  echo   run_watch_stage_a_3years_progress.bat ^<manifest.json^> [--interval-s 60]
  echo   run_watch_stage_a_3years_progress.bat ^<manifest.json^> --on-finish-cmd "run_stage_b.bat ..."
  echo.
  echo Optional:
  echo   --on-finish-log ^<path^>
  echo   --on-finish-cwd ^<repo-root^>
  echo   --trigger-state-file ^<path-to-json^>
  exit /b 1
)

call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)

"%PYTHON_EXE%" -u backend\watch_stage_a_3years_progress.py --manifest "%~1" %2 %3 %4 %5 %6 %7 %8 %9
exit /b %ERRORLEVEL%
