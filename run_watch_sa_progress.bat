@echo off
setlocal
cd /d "%~dp0"

set "RUN_LOG=%~1"
if "%RUN_LOG%"=="" set "RUN_LOG=paper\exports\sa_chunks_cog_auto_run_8010_20260224_224309.log"

set "OUT_LOG=paper\exports\automation\sa_progress_watch_%DATE:~6,4%%DATE:~3,2%%DATE:~0,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%.log"
set "OUT_LOG=%OUT_LOG: =0%"

echo === SA Progress Watch ===
echo Run-Log: %RUN_LOG%
echo Out-Log: %OUT_LOG%

python backend\watch_sa_chunk_progress.py ^
  --run-log "%RUN_LOG%" ^
  --state-file "paper\exports\sa_chunks\sa_chunk_run_state.json" ^
  --exports-dir "paper\exports\sa_chunks" ^
  --out-log "%OUT_LOG%" ^
  --interval-sec 60

endlocal

