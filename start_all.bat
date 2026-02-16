@echo off
echo === Starting Hydrowatch Servers ===

echo [1/2] Starting Frontend (Vite)...
cd /d %~dp0frontend
start "HW-Frontend" powershell -NoProfile -ExecutionPolicy Bypass -Command "$env:VITE_LEGACY_API_URL='http://127.0.0.1:8001'; npx vite --host 2>&1 | Tee-Object -FilePath frontend.log"

echo [2/2] Starting Backend (FastAPI)...
cd /d %~dp0backend
start "HW-Backend" powershell -NoProfile -ExecutionPolicy Bypass -Command "cmd /c \"..\\run_backend.bat\" 2>&1 | Tee-Object -FilePath backend.log"

echo.
echo Servers starting... check the new windows for details.
echo Frontend: http://localhost:5173
echo Backend:  http://127.0.0.1:8001
