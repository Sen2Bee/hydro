@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0

set "CROP_CSV=data\derived\crop_history\crop_history.csv"
if not exist "%CROP_CSV%" (
  echo [ERROR] Crop history fehlt: %CROP_CSV%
  exit /b 2
)

set "LOG_DIR=%~dp0data\layers\c_dynamic_sa\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "TS=%DATE:~-4%%DATE:~3,2%%DATE:~0,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%"
set "TS=%TS: =0%"
set "LOG_FILE=%LOG_DIR%\dynamic_c_windows_with_crop_%TS%.log"

echo.
echo === Build Dynamic C Windows (mit Crop-Historie) ===
echo Crop CSV: %CROP_CSV%
echo Log: %LOG_FILE%
echo.

echo [%date% %time%] run_build_dynamic_c_windows_with_crop.bat %*>>"%LOG_FILE%"
"%PYTHON_EXE%" -u backend\build_dynamic_c_windows.py ^
  --crop-history-csv "%CROP_CSV%" ^
  --crop-year-mode start_year ^
  %* >>"%LOG_FILE%" 2>&1

set "ERR=%ERRORLEVEL%"
if "%ERR%"=="0" (
  echo [%date% %time%] DYNAMIC_C_WITH_CROP_OK>>"%LOG_FILE%"
  echo [OK] Done. Log: %LOG_FILE%
) else (
  echo [%date% %time%] DYNAMIC_C_WITH_CROP_FAIL errorlevel=%ERR%>>"%LOG_FILE%"
  echo [FAIL] Fehler. Log: %LOG_FILE%
)
exit /b %ERR%

