@echo off
setlocal

set "ROOT=%~dp0"
set "DCMD=C:\Program Files\Docker\Docker\resources\bin\docker.exe"

if not exist "%DCMD%" set "DCMD=docker"

echo Check Docker daemon...
"%DCMD%" info >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Docker daemon not reachable. Is Docker Desktop running?
  exit /b 1
)

echo Stopping Hydrowatch stack...
"%DCMD%" compose -f "%ROOT%docker-compose.dev.yml" down
if errorlevel 1 (
  echo [ERROR] docker compose down failed
  exit /b 1
)

echo [OK] Stack stopped.
exit /b 0

