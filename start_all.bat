@echo off
setlocal
set "BACKEND_PORT=8010"
set "FRONTEND_PORT=5180"
echo === Starting Hydrowatch Servers ===

echo [1/2] Starting Frontend (Vite) on %FRONTEND_PORT%...
cd /d %~dp0frontend
start "HW-Frontend" powershell -NoProfile -ExecutionPolicy Bypass -Command "$env:VITE_LEGACY_API_URL='http://127.0.0.1:%BACKEND_PORT%'; npx vite --host 127.0.0.1 --port %FRONTEND_PORT% 2>&1 | Tee-Object -FilePath frontend.log"

echo [2/2] Starting Backend (FastAPI) on %BACKEND_PORT%...
cd /d %~dp0backend
start "HW-Backend" powershell -NoProfile -ExecutionPolicy Bypass -Command "$env:BACKEND_PORT='%BACKEND_PORT%'; cmd /c \"..\\run_backend.bat\" 2>&1 | Tee-Object -FilePath backend.log"

echo.
echo Servers starting... check the new windows for details.
echo Frontend: http://127.0.0.1:%FRONTEND_PORT%
echo Backend:  http://127.0.0.1:%BACKEND_PORT%
endlocal
