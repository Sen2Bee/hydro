@echo off
setlocal
cd /d "%~dp0"

echo.
echo === Stage A: 3 Worker Precompute (2023/2024/2025) ===
echo - Worker 1: 2023-04-01..2023-10-31
echo - Worker 2: 2024-04-01..2024-10-31
echo - Worker 3: 2025-04-01..2025-10-31
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0backend\start_stage_a_3workers.ps1" %*
exit /b %ERRORLEVEL%

