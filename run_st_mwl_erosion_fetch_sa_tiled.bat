@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === ST MWL Erosion WMS Fetch (SA-weit, gekachelt) ===
echo Ziel: 10 m Raster ueber Sachsen-Anhalt, Layer K/R/S/Wasser_Erosion
echo.
echo Default-Aufruf:
echo   --target-res-m 10 --tile-px 5000
echo.
"%PYTHON_EXE%" backend\fetch_st_mwl_erosion_layers_sa_tiled.py ^
  --target-res-m 10 ^
  --tile-px 5000 ^
  --layers "K-Faktor,R-Faktor,S-Faktor,Wasser_Erosion" ^
  %*
