@echo off
setlocal enabledelayedexpansion

REM Download official LVermGeo Sachsen-Anhalt DGM1 GeoTIFF packages (ZIP).
REM Usage:
REM   download_st_dgm1.bat 1
REM   download_st_dgm1.bat 2
REM   download_st_dgm1.bat 3
REM   download_st_dgm1.bat 4
REM
REM Notes:
REM - Files are very large (several GB).
REM - After download, we extract to .\data\st_dgm1\DGM1_<part>\

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

set "OUTDIR=%~dp0data\st_dgm1"
set "ZIPNAME=DGM1_%PART%.zip"
set "ZIPPATH=%OUTDIR%\%ZIPNAME%"
set "EXTRACTDIR=%OUTDIR%\DGM1_%PART%"

set "BASEURL=https://www.geodatenportal.sachsen-anhalt.de/gfds_webshare/download/LVermGeo/Geodatenportal/Online-Bereitstellung-LVermGeo/DGM"
set "URL=%BASEURL%/%ZIPNAME%"

if not exist "%OUTDIR%" mkdir "%OUTDIR%"

echo Downloading: %URL%
echo To: %ZIPPATH%
curl -L --fail -o "%ZIPPATH%" "%URL%"
if errorlevel 1 (
  echo Download failed.
  exit /b 1
)

if exist "%EXTRACTDIR%" (
  echo Extract dir already exists: %EXTRACTDIR%
  echo Skipping extraction.
  exit /b 0
)

echo Extracting to: %EXTRACTDIR%
powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '%EXTRACTDIR%' | Out-Null; Expand-Archive -LiteralPath '%ZIPPATH%' -DestinationPath '%EXTRACTDIR%' -Force"
if errorlevel 1 (
  echo Extraction failed.
  exit /b 1
)

echo Done.
