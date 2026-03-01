@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

call "%~dp0resolve_python.bat"
if errorlevel 1 (
  echo [ERROR] Kein lauffaehiger Python-Interpreter gefunden.
  exit /b 1
)

set "START_CHUNK=%~1"
if "%START_CHUNK%"=="" set "START_CHUNK=1"
set "MAX_CHUNKS=%~2"
if "%MAX_CHUNKS%"=="" set "MAX_CHUNKS=100"

set /a _max=%MAX_CHUNKS% 2>nul
if errorlevel 1 (
  echo [ERROR] MAX_CHUNKS ist ungueltig: %MAX_CHUNKS%
  exit /b 1
)
if %_max% LSS 2 (
  echo [ERROR] MAX_CHUNKS muss mindestens 2 sein.
  exit /b 1
)

set "EXTRA_ARGS="
shift
shift
:args_loop
if "%~1"=="" goto args_done
set "EXTRA_ARGS=!EXTRA_ARGS! %1"
shift
goto args_loop
:args_done

for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmss')"') do set "RUN_TAG=%%i"
set "RUN_DIR=paper\exports\automation\stage_b_2workers_%RUN_TAG%"
if not exist "%RUN_DIR%" mkdir "%RUN_DIR%"

set /a W1=(%_max%+1)/2
set /a W2=%_max%-W1
set /a S1=%START_CHUNK%
set /a S2=%START_CHUNK%+W1

set "LOG1=%RUN_DIR%\worker1.log"
set "LOG2=%RUN_DIR%\worker2.log"
set "M1=%RUN_DIR%\worker1_cmd.txt"
set "M2=%RUN_DIR%\worker2_cmd.txt"
set "MANIFEST=%RUN_DIR%\manifest.txt"

set "DEFAULT_WHITELIST=data\derived\whitelists\acker_ids.txt"
if not exist "%DEFAULT_WHITELIST%" (
  echo [ERROR] Acker-Whitelist fehlt: %DEFAULT_WHITELIST%
  echo         Paper-Run wird aus fachlichen Gruenden nicht ohne Whitelist gestartet.
  exit /b 2
)

set "BASE_ARGS=--chunk-size 1000 --checkpoint-every 100 --events-auto-source icon2d --events-auto-top-n 3 --events-auto-min-severity 1 --events-auto-cache-only --events-auto-use-cached-empty --api-base-url http://127.0.0.1:8001 --analysis-modes erosion_events_ml,abag --provider auto --dem-source cog --threshold 200 --ml-threshold 0.05 --resume --validate-chunk --fail-on-qa-error --continue-on-error --min-field-area-ha 0.05 --field-id-whitelist-file %DEFAULT_WHITELIST% --require-whitelist"
set "PY=%PYTHON_EXE%"

set "CMD1=""%PY%"" -u backend\run_sa_icon2d_multiwindow_chunks.py --start-chunk %S1% --max-chunks %W1% %BASE_ARGS% %EXTRA_ARGS%"
set "CMD2=""%PY%"" -u backend\run_sa_icon2d_multiwindow_chunks.py --start-chunk %S2% --max-chunks %W2% %BASE_ARGS% %EXTRA_ARGS%"

echo %CMD1%>"%M1%"
echo %CMD2%>"%M2%"

echo run_tag=%RUN_TAG%>"%MANIFEST%"
echo started_utc=%RUN_TAG%>>"%MANIFEST%"
echo start_chunk=%START_CHUNK%>>"%MANIFEST%"
echo max_chunks=%MAX_CHUNKS%>>"%MANIFEST%"
echo worker1_start=%S1%>>"%MANIFEST%"
echo worker1_max=%W1%>>"%MANIFEST%"
echo worker1_log=%LOG1%>>"%MANIFEST%"
echo worker2_start=%S2%>>"%MANIFEST%"
echo worker2_max=%W2%>>"%MANIFEST%"
echo worker2_log=%LOG2%>>"%MANIFEST%"
echo python=%PY%>>"%MANIFEST%"

echo.
echo === Stage B mit 2 Workern ===
echo Run dir: %RUN_DIR%
echo Worker 1: chunk %S1% .. +%W1%-1
echo Worker 2: chunk %S2% .. +%W2%-1
echo.

start "stage_b_w1_%RUN_TAG%" /min cmd /c "cd /d ""%~dp0"" && %CMD1% > ""%LOG1%"" 2>&1"
start "stage_b_w2_%RUN_TAG%" /min cmd /c "cd /d ""%~dp0"" && %CMD2% > ""%LOG2%"" 2>&1"

echo [OK] gestartet.
echo [OK] Manifest: %~dp0%MANIFEST%
echo [OK] Logs:
echo      %~dp0%LOG1%
echo      %~dp0%LOG2%
echo.
echo Tipp fuer VSCode-Log:
echo   %~dp0%LOG1%
echo   %~dp0%LOG2%
echo.

endlocal
exit /b 0
