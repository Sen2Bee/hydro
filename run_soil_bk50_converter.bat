@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0backend
echo.
echo === BK50 -> Soil Raster Converter ===
echo.
echo Usage example:
echo   "%PYTHON_EXE%" soil_bk50_to_raster.py --input-gpkg C:\data\ISBK50.gpkg --list-only
echo   "%PYTHON_EXE%" soil_bk50_to_raster.py --input-gpkg C:\data\ISBK50.gpkg --layer YOUR_LAYER --value-field YOUR_NUMERIC_FIELD --output-tif C:\data\nrw_soil.tif
echo.
"%PYTHON_EXE%" soil_bk50_to_raster.py %*
