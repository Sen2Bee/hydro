@echo off
setlocal

echo === Stopping Hydrowatch Servers (ports 5180, 8010; fallback 5173, 8001) ===

REM Kill Vite + Uvicorn reliably.
REM Note: Uvicorn --reload can lead to "phantom" port owners in Get-NetTCPConnection; therefore kill by command line.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$targets=@(); $targets += Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'node.exe' -and ($_.CommandLine -match 'vite') -and (($_.CommandLine -match '5180') -or ($_.CommandLine -match '5173')) }; $targets += Get-CimInstance Win32_Process | Where-Object { ($_.Name -in @('python.exe','pythonw.exe')) -and ($_.CommandLine -match 'uvicorn') -and ($_.CommandLine -match 'main:app') -and (($_.CommandLine -match '--port\\s+8010') -or ($_.CommandLine -match '--port\\s+8001')) }; $targets += Get-CimInstance Win32_Process | Where-Object { ($_.Name -in @('python.exe','pythonw.exe')) -and ($_.CommandLine -match 'spawn_main') -and ($_.CommandLine -match 'parent_pid') }; $pids = $targets | Select-Object -ExpandProperty ProcessId -Unique; if(-not $pids){ Write-Host 'No matching processes found (fallback to ports)'; } foreach($id in $pids){ try { $proc=Get-Process -Id $id -ErrorAction Stop; Write-Host \"Stopping PID $id ($($proc.ProcessName))\"; Stop-Process -Id $id -Force } catch { Write-Host \"Failed to stop PID ${id}: $($_.Exception.Message)\" } }; $ports=@(5180,8010,5173,8001); foreach($p in $ports){ $conns=Get-NetTCPConnection -State Listen -LocalPort $p -ErrorAction SilentlyContinue; if(-not $conns){ continue }; $portPids=$conns | Select-Object -ExpandProperty OwningProcess -Unique; foreach($id in $portPids){ try { Stop-Process -Id $id -Force -ErrorAction Stop; Write-Host \"Stopped PID $id on port $p\" } catch {} } }"

echo Done.
