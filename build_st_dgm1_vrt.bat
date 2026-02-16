@echo off
setlocal enabledelayedexpansion

REM Build a GDAL VRT mosaic from extracted Sachsen-Anhalt DGM1 tiles.
REM Usage:
REM   build_st_dgm1_vrt.bat 1
REM   build_st_dgm1_vrt.bat 2
REM   build_st_dgm1_vrt.bat 3
REM   build_st_dgm1_vrt.bat 4
REM
REM Output:
REM   .\data\st_dgm1\DGM1_<part>.vrt

if "%~1"=="" (
  echo Usage: %~nx0 ^<part 1-4^>
  exit /b 2
)

set "PART=%~1"
if not "%PART%"=="1" if not "%PART%"=="2" if not "%PART%"=="3" if not "%PART%"=="4" (
  echo Invalid part: %PART%
  echo Expected: 1, 2, 3, or 4
  exit /b 2
)

set "BASEDIR=%~dp0data\st_dgm1"
set "EXTRACTDIR=%BASEDIR%\DGM1_%PART%"
set "VRT=%BASEDIR%\DGM1_%PART%.vrt"
set "LIST=%BASEDIR%\DGM1_%PART%_files.txt"

if not exist "%EXTRACTDIR%" (
  echo Extract dir not found: %EXTRACTDIR%
  echo Run download_st_dgm1.bat %PART% first.
  exit /b 1
)

echo Scanning GeoTIFF tiles under: %EXTRACTDIR%
if exist "%LIST%" del /f /q "%LIST%"

for /r "%EXTRACTDIR%" %%F in (*.tif) do (
  echo %%F>>"%LIST%"
)

for %%A in ("%LIST%") do if %%~zA LSS 1 (
  echo No .tif files found.
  exit /b 1
)

echo Building VRT: %VRT%
call C:\\OSGeo4W\\bin\\o4w_env.bat
gdalbuildvrt -overwrite -input_file_list "%LIST%" "%VRT%"
if errorlevel 1 (
  echo gdalbuildvrt failed.
  exit /b 1
)

echo Done.
