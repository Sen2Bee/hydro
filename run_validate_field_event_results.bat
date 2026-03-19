@echo off
setlocal
set SCRIPT_DIR=%~dp0
call "%SCRIPT_DIR%resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Could not resolve Python interpreter.
  exit /b 1
)

if "%~1"=="" (
  set CSV_PATH=paper\exports\field_event_results_sample.csv
) else (
  set CSV_PATH=%~1
)

set OUT_JSON=%CSV_PATH:.csv=.qa.json%

echo === Validate Field x Event Results ===
echo Python: %PYTHON_EXE%
echo CSV:    %CSV_PATH%
echo Report: %OUT_JSON%
echo.

%PYTHON_EXE% -u backend\validate_field_event_results.py --csv "%CSV_PATH%" --out-json "%OUT_JSON%"
set EXIT_CODE=%ERRORLEVEL%

if %EXIT_CODE%==0 (
  echo.
  echo [OK] QA checks passed.
) else (
  echo.
  echo [WARN] QA checks found issues. See JSON report.
)

exit /b %EXIT_CODE%
