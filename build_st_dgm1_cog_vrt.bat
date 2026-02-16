@echo off
setlocal enabledelayedexpansion

REM Build a GDAL VRT mosaic from a folder of Sachsen-Anhalt DGM1 COG tiles.
REM Usage:
REM   build_st_dgm1_cog_vrt.bat "D:\data\st_dgm1_cog"
REM Optional:
REM   build_st_dgm1_cog_vrt.bat "D:\data\st_dgm1_cog" "D:\data\st_dgm1_cog\st_dgm1.vrt"
REM
REM Requires: OSGeo4W GDAL (gdalbuildvrt available via o4w_env.bat)

set "SRC_DIR=%~1"
set "OUT_VRT=%~2"

if "%SRC_DIR%"=="" (
  echo Usage: %~nx0 ^<src_dir^> [out_vrt]
  exit /b 2
)

if "%OUT_VRT%"=="" (
  set "OUT_VRT=%SRC_DIR%\st_dgm1.vrt"
)

if not exist "%SRC_DIR%" (
  echo [ERROR] Source dir not found: %SRC_DIR%
  exit /b 1
)

set "LIST=%SRC_DIR%\st_dgm1_cog_files.txt"

echo Scanning COG tiles under: %SRC_DIR%
if exist "%LIST%" del /f /q "%LIST%"

REM Use PowerShell for fast file listing (avoids cmd for /r overhead on large dirs)
powershell -NoProfile -Command ^
  "$src='%SRC_DIR%'; $list='%LIST%';" ^
  "Get-ChildItem -Path $src -Recurse -File -Filter '*_cog.tif' |" ^
  "Select-Object -ExpandProperty FullName |" ^
  "Set-Content -Encoding ascii -Path $list"
if errorlevel 1 (
  echo [ERROR] Failed to build file list.
  exit /b 1
)

for %%A in ("%LIST%") do if %%~zA LSS 1 (
  echo [ERROR] No '*_cog.tif' files found.
  exit /b 1
)

echo Building VRT: %OUT_VRT%
call C:\OSGeo4W\bin\o4w_env.bat
gdalbuildvrt -overwrite -input_file_list "%LIST%" "%OUT_VRT%"
if errorlevel 1 (
  echo [ERROR] gdalbuildvrt failed.
  exit /b 1
)

echo [OK] VRT ready: %OUT_VRT%
echo Set ST_DEM_LOCAL_PATH to this VRT (must be readable by the backend/worker).
exit /b 0

