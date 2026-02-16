@echo off
call C:\OSGeo4W\bin\o4w_env.bat
cd /d %~dp0backend
echo.
echo === BK50 -> Soil Raster Converter ===
echo.
echo Usage example:
echo   python soil_bk50_to_raster.py --input-gpkg C:\data\ISBK50.gpkg --list-only
echo   python soil_bk50_to_raster.py --input-gpkg C:\data\ISBK50.gpkg --layer YOUR_LAYER --value-field YOUR_NUMERIC_FIELD --output-tif C:\data\nrw_soil.tif
echo.
python soil_bk50_to_raster.py %*

