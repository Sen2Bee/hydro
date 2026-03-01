@echo off
setlocal

set "AUTOMATION_DIR=D:\__GeoFlux\hydrowatch\paper\exports\automation"
set "BASE="
for /f "delims=" %%D in ('dir /b /ad /o-d "%AUTOMATION_DIR%\stage_a_sawide_3years_*" 2^>nul') do (
  set "BASE=%AUTOMATION_DIR%\%%D"
  goto :found
)

echo Kein stage_a_sawide_3years_* Ordner gefunden in:
echo %AUTOMATION_DIR%
exit /b 1

:found
set "L1=%BASE%\overall_progress.log"
set "L2=%BASE%\stage_a_20230401_20231031.log"
set "L3=%BASE%\stage_a_20240401_20241031.log"
set "L4=%BASE%\stage_a_20250401_20251031.log"

echo Verwende Run-Ordner:
echo %BASE%

where code >nul 2>nul
if %errorlevel%==0 (
  code "%L1%" "%L2%" "%L3%" "%L4%"
) else (
  start "" "%L1%"
  start "" "%L2%"
  start "" "%L3%"
  start "" "%L4%"
)

endlocal
