@echo off
setlocal
cd /d "%~dp0"

set "NIGHT_DIR=%CD%\paper\exports\automation\night_runs"
if not exist "%NIGHT_DIR%" (
  echo NO_NIGHT_RUNS_DIR
  exit /b 0
)

for /f "delims=" %%F in ('dir /b /o-d "%NIGHT_DIR%\night_launch_*.json" 2^>nul') do (
  set "LATEST=%NIGHT_DIR%\%%F"
  goto :found
)
echo NO_NIGHT_LAUNCH_MANIFEST
exit /b 0

:found
echo LATEST_MANIFEST=%LATEST%
echo.
echo --- MANIFEST (first lines) ---
for /f "usebackq delims=" %%L in ("%LATEST%") do (
  echo %%L
)

endlocal
