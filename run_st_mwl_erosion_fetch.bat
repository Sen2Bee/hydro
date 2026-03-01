@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
echo.
echo === ST MWL Erosion WMS Fetch (Test) ===
echo.
echo Beispiel-BBox (Halle Umgebung):
echo   west=11.80 south=51.40 east=12.10 north=51.60
echo.
"%PYTHON_EXE%" backend\fetch_st_mwl_erosion_layers.py --west 11.80 --south 51.40 --east 12.10 --north 51.60 %*
