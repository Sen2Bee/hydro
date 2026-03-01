@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === SA Chunk Field x Event Batch ===
echo Python: %PYTHON_EXE%
echo Hinweis: Standard chunk-size=5000, resume=on.
echo.
if "%~1"=="" (
  "%PYTHON_EXE%" -u backend\run_field_event_batch_sa_chunks.py
  exit /b %ERRORLEVEL%
)
"%PYTHON_EXE%" -u backend\run_field_event_batch_sa_chunks.py %*
endlocal

