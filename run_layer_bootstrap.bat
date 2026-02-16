@echo off
call C:\OSGeo4W\bin\o4w_env.bat
cd /d %~dp0backend
echo.
echo === Hydrowatch Layer Bootstrap ===
echo.
python layer_bootstrap.py

