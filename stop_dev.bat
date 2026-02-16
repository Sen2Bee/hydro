@echo off
setlocal

echo === Stopping Hydrowatch Dev Stack ===
echo [1/2] Stopping local frontend + legacy backend...
call "%~dp0stop_all.bat"

echo [2/2] Stopping Docker dev stack...
docker compose -f "%~dp0docker-compose.dev.yml" down
exit /b %errorlevel%

