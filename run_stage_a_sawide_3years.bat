@echo off
setlocal
cd /d "%~dp0"

echo.
echo === Stage A SA-weit (3 Jahre parallel) ===
echo - 2023-04-01..2023-10-31
echo - 2024-04-01..2024-10-31
echo - 2025-04-01..2025-10-31
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0backend\start_stage_a_sawide_3years.ps1" %*
exit /b %ERRORLEVEL%

