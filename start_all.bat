@echo off
echo === Starting Hydrowatch Servers ===

echo [1/2] Starting Frontend (Vite)...
cd /d %~dp0frontend
start "HW-Frontend" cmd /c "npx vite --host 2>&1 | tee frontend.log"

echo [2/2] Starting Backend (FastAPI)...
cd /d %~dp0backend
start "HW-Backend" cmd /c "call C:\OSGeo4W\bin\o4w_env.bat && python -m uvicorn main:app --reload --host 127.0.0.1 --port 8001 2>&1 | tee backend.log"

echo.
echo Servers starting... check the new windows for details.
echo Frontend: http://localhost:5173
echo Backend:  http://127.0.0.1:8001
