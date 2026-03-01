@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0

set "OUT=paper\exports\c_sensitivity"
if not exist "%OUT%" mkdir "%OUT%"

set "CONFIGS=data\config\c_factor_method_v1.json,data\config\c_factor_method_v1_low.json,data\config\c_factor_method_v1_high.json"

echo === C-Factor Sensitivitaetslauf ===
echo Window: 2024-04-01..2024-10-31
echo.

"%PYTHON_EXE%" -u backend\run_c_factor_sensitivity.py ^
  --west 10.534168657927681 --south 50.97330866288862 --east 13.283020232424267 --north 52.98996620841052 ^
  --start 2024-04-01 --end 2024-10-31 ^
  --template-raster data\layers\st_mwl_erosion_sa_tiled\K_Faktor\K_Faktor.vrt ^
  --configs "%CONFIGS%" ^
  --out-dir "%OUT%" %*

exit /b %ERRORLEVEL%

