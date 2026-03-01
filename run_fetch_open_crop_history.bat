@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0

set "LOG_DIR=%~dp0data\raw\crop_history_open\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "TS=%DATE:~-4%%DATE:~3,2%%DATE:~0,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%"
set "TS=%TS: =0%"
set "LOG_FILE=%LOG_DIR%\fetch_open_crop_history_%TS%.log"

echo.
echo === Fetch Open Crop History Datasets ===
echo Profile default: core (RUSLE-C + 2024/2025 crop rasters)
echo Log: %LOG_FILE%
echo.

echo [%date% %time%] run_fetch_open_crop_history.bat %*>>"%LOG_FILE%"
"%PYTHON_EXE%" -u backend\fetch_open_crop_history.py --profile core --clip-sa %* >>"%LOG_FILE%" 2>&1
set "ERR=%ERRORLEVEL%"
if "%ERR%"=="0" (
  echo [%date% %time%] FETCH_OPEN_CROP_HISTORY_OK>>"%LOG_FILE%"
  echo [OK] Done. Log: %LOG_FILE%
) else (
  echo [%date% %time%] FETCH_OPEN_CROP_HISTORY_FAIL errorlevel=%ERR%>>"%LOG_FILE%"
  echo [FAIL] Fehler. Log: %LOG_FILE%
)
exit /b %ERR%

