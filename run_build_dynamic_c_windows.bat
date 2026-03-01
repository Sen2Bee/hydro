@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0

set "LOG_DIR=%~dp0data\layers\c_dynamic_sa\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "TS=%DATE:~-4%%DATE:~3,2%%DATE:~0,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%"
set "TS=%TS: =0%"
set "LOG_FILE=%LOG_DIR%\dynamic_c_windows_%TS%.log"

echo.
echo === Build Dynamic C Windows (SA) ===
echo Fenster default:
echo   2023-04-01:2023-10-31,2024-04-01:2024-10-31,2025-04-01:2025-10-31
echo Log: %LOG_FILE%
echo.

echo [%date% %time%] run_build_dynamic_c_windows.bat %*>>"%LOG_FILE%"
"%PYTHON_EXE%" -u backend\build_dynamic_c_windows.py %* >>"%LOG_FILE%" 2>&1
set "ERR=%ERRORLEVEL%"
if "%ERR%"=="0" (
  echo [%date% %time%] DYNAMIC_C_OK>>"%LOG_FILE%"
  echo [OK] Done. Log: %LOG_FILE%
) else (
  echo [%date% %time%] DYNAMIC_C_FAIL errorlevel=%ERR%>>"%LOG_FILE%"
  echo [FAIL] Fehler. Log: %LOG_FILE%
)
exit /b %ERR%

