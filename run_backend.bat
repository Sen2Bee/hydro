@echo off
call C:\OSGeo4W\bin\o4w_env.bat
cd /d %~dp0backend
python -m pip install --quiet fastapi uvicorn python-multipart pysheds 2>nul
echo.
echo === Starting Hydrowatch Backend on http://127.0.0.1:8001 ===
echo.
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8001
