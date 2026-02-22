@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "c:\Users\thoko\.gemini\antigravity\scratch\hydrowatch-berlin\resolve_python.bat"
if errorlevel 1 exit /b 1
cd /d "c:\Users\thoko\.gemini\antigravity\scratch\hydrowatch-berlin\backend"
set WEATHER_PROVIDER=icon2d
set ICON2D_TRANSPORT=direct
set ICON2D_BATCH_PATH=/weather/batch
set RADAR_PROVIDER=dwd_radolan
set RADAR_MAX_HOURS=4320
if exist D:\data\st_dgm1_cog set ST_COG_DIR=D:\data\st_dgm1_cog
"%PYTHON_EXE%" -m uvicorn main:app --reload --host 127.0.0.1 --port 8010
