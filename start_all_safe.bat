@echo off
setlocal
set BACKEND_PORT=8010
set FRONTEND_PORT=5180

echo === Starting Hydrowatch Servers (safe ports) ===

echo [1/2] Starting Frontend (Vite) on %FRONTEND_PORT%...
cd /d %~dp0frontend
start "HW-Frontend-Safe" powershell -NoProfile -ExecutionPolicy Bypass -Command "$env:VITE_LEGACY_API_URL='http://127.0.0.1:%BACKEND_PORT%'; npx vite --host 127.0.0.1 --port %FRONTEND_PORT% 2>&1 | Tee-Object -FilePath frontend_safe.log"

echo [2/2] Starting Backend (FastAPI) on %BACKEND_PORT%...
cd /d %~dp0backend
start "HW-Backend-Safe" powershell -NoProfile -ExecutionPolicy Bypass -Command "call C:\OSGeo4W\bin\o4w_env.bat; call ..\resolve_python.bat; if($LASTEXITCODE -ne 0){ exit $LASTEXITCODE }; if($env:ST_COG_DIR -eq $null -or $env:ST_COG_DIR -eq ''){ if(Test-Path 'D:\data\st_dgm1_cog'){ $env:ST_COG_DIR='D:\data\st_dgm1_cog' } }; if($env:WEATHER_PROVIDER -eq $null -or $env:WEATHER_PROVIDER -eq ''){ $env:WEATHER_PROVIDER='icon2d' }; if($env:ICON2D_TRANSPORT -eq $null -or $env:ICON2D_TRANSPORT -eq ''){ $env:ICON2D_TRANSPORT='direct' }; if($env:ICON2D_BATCH_PATH -eq $null -or $env:ICON2D_BATCH_PATH -eq ''){ $env:ICON2D_BATCH_PATH='/weather/batch' }; if($env:RADAR_PROVIDER -eq $null -or $env:RADAR_PROVIDER -eq ''){ $env:RADAR_PROVIDER='dwd_radolan' }; if($env:RADAR_MAX_HOURS -eq $null -or $env:RADAR_MAX_HOURS -eq ''){ $env:RADAR_MAX_HOURS='4320' }; cd ..\backend; & $env:PYTHON_EXE -m uvicorn main:app --reload --host 127.0.0.1 --port %BACKEND_PORT% 2>&1 | Tee-Object -FilePath backend_safe.log"

echo.
echo Servers starting... check new windows for details.
echo Frontend: http://127.0.0.1:%FRONTEND_PORT%
echo Backend:  http://127.0.0.1:%BACKEND_PORT%
endlocal
