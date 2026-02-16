@echo off
setlocal

echo === Starting Hydrowatch Dev Stack ===
echo [1/2] Starting Docker dev stack (PostGIS/Redis/MinIO + Job API/Worker)...
docker compose -f "%~dp0docker-compose.dev.yml" up -d
if errorlevel 1 (
  echo [ERROR] docker compose up failed
  exit /b 1
)

echo [2/2] Starting local frontend + legacy backend...
call "%~dp0start_all.bat"
exit /b 0

