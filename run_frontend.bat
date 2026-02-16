@echo off
cd /d %~dp0frontend
echo === Starting Hydrowatch Frontend ===
set "VITE_LEGACY_API_URL=http://127.0.0.1:8001"
npx vite
