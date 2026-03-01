@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)
cd /d %~dp0
set "LOG_DIR=%~dp0data\layers\st_mwl_erosion\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\c_pipeline.log"
echo.
echo === Build C-Faktor Proxy (Feldblock + NDVI) ===
echo.
echo Beispiel-BBox (Halle Umgebung):
echo   west=11.80 south=51.40 east=12.10 north=51.60
echo.
echo [%date% %time%] run_build_c_factor_proxy.bat %*>>"%LOG_FILE%"
"%PYTHON_EXE%" backend\build_c_factor_proxy.py --west 11.80 --south 51.40 --east 12.10 --north 51.60 %*
set "ERR=%ERRORLEVEL%"
if "%ERR%"=="0" (
  echo [%date% %time%] C_PROXY_OK>>"%LOG_FILE%"
) else (
  echo [%date% %time%] C_PROXY_FAIL errorlevel=%ERR%>>"%LOG_FILE%"
)
exit /b %ERR%
