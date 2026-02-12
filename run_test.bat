@echo off
call C:\OSGeo4W\bin\o4w_env.bat
cd /d %~dp0backend
echo === Running Hydrowatch Smoke Test ===
echo.
python test_core.py
pause
