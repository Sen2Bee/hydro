@echo off
setlocal
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0

if not exist "paper\exports" mkdir "paper\exports"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set RUNTS=%%i
set "OUTCSV=paper\exports\event_probe_icon2d_windows_%RUNTS%.csv"
set "LOGFILE=paper\exports\event_probe_icon2d_windows_%RUNTS%.log"

echo.
echo === Event Probe ICON2D (3 windows) ===
echo Python: %PYTHON_EXE%
echo CSV:    %OUTCSV%
echo LOG:    %LOGFILE%
echo.

"%PYTHON_EXE%" -u backend\run_event_probe_icon2d_windows.py --out-csv "%OUTCSV%" --log-file "%LOGFILE%" %*
set RC=%ERRORLEVEL%

echo.
echo ExitCode: %RC%
echo CSV: %OUTCSV%
echo LOG: %LOGFILE%
exit /b %RC%

