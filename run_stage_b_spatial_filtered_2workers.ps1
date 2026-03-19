param(
    [int]$StartChunk = 1,
    [int]$MaxChunks = 50,
    [string]$WindowStart = "2024-04-01",
    [string]$WindowEnd = "2024-10-31",
    [string]$ApiBaseUrl = "http://127.0.0.1:8011"
)

$ErrorActionPreference = "Stop"

if ($MaxChunks -lt 2) {
    throw "MaxChunks muss mindestens 2 sein."
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = $null
$resolver = Join-Path $root "resolve_python.bat"

if ($env:PYTHON_EXE -and (Test-Path $env:PYTHON_EXE)) {
    $python = $env:PYTHON_EXE
} elseif (Test-Path $resolver) {
    $resolved = cmd.exe /d /c "call `"$resolver`" && echo PYTHON_EXE=%PYTHON_EXE%"
    if ($LASTEXITCODE -eq 0) {
        $line = $resolved | Select-Object -Last 1
        if ($line -match "^PYTHON_EXE=(.+)$" -and (Test-Path $Matches[1])) {
            $python = $Matches[1]
        }
    }
}
if (-not $python) {
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) {
        $python = $cmd.Source
    }
}
if (-not $python -or -not (Test-Path $python)) {
    throw "Python nicht gefunden."
}

$runner = Join-Path $root "backend\run_field_event_batch_sa_chunks.py"
$whitelist = Join-Path $root "data\derived\whitelists\acker_ids.txt"
$chunkDir = Join-Path $root "paper\input\sa_chunks_icon2d_3y_spatial_filtered_50k\20240401_20241031"
$exportsRoot = Join-Path $root "paper\exports\sa_chunks_icon2d_3y_spatial_filtered_50k"
$runsRoot = Join-Path $root "paper\exports\runs"
$eventCacheDir = Join-Path $root "data\events\sa_2km\icon2d_20240401_20241031"
$eventCellCacheDir = Join-Path $eventCacheDir "cell_cache"

if (-not (Test-Path $python)) {
    throw "Python nicht gefunden: $python"
}
if (-not (Test-Path $runner)) {
    throw "Runner nicht gefunden: $runner"
}
if (-not (Test-Path $whitelist)) {
    throw "Whitelist nicht gefunden: $whitelist"
}
if (-not (Test-Path $chunkDir)) {
    throw "Chunk-Verzeichnis nicht gefunden: $chunkDir"
}
if (-not (Test-Path $eventCacheDir)) {
    throw "Event-Cache nicht gefunden: $eventCacheDir"
}

$tag = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$runDir = Join-Path $runsRoot "stageb_spatial_filtered_2workers_$tag"
$null = New-Item -ItemType Directory -Force -Path $runDir

$combinedLog = Join-Path $runDir "stageb_combined_live.log"
$worker1Log = Join-Path $runDir "worker1.log"
$worker2Log = Join-Path $runDir "worker2.log"
$worker1Err = Join-Path $runDir "worker1.err.log"
$worker2Err = Join-Path $runDir "worker2.err.log"
$manifest = Join-Path $runDir "manifest.json"

$w1 = [int][Math]::Ceiling($MaxChunks / 2.0)
$w2 = $MaxChunks - $w1
$s1 = $StartChunk
$s2 = $StartChunk + $w1

$winKey = "{0}_{1}" -f $WindowStart.Replace("-", ""), $WindowEnd.Replace("-", "")
$worker1Exports = Join-Path $exportsRoot ($winKey + "_w1")
$worker2Exports = Join-Path $exportsRoot ($winKey + "_w2")
$null = New-Item -ItemType Directory -Force -Path $worker1Exports, $worker2Exports

function New-WorkerCommand {
    param(
        [string]$WorkerTag,
        [int]$ChunkStart,
        [int]$ChunkCount,
        [string]$WorkerLog,
        [string]$ExportsDir
    )

    $args = @(
        "--chunk-size", "1000",
        "--start-chunk", "$ChunkStart",
        "--max-chunks", "$ChunkCount",
        "--events-source", "auto",
        "--events-auto-source", "icon2d",
        "--events-auto-start", $WindowStart,
        "--events-auto-end", $WindowEnd,
        "--events-auto-top-n", "5",
        "--events-auto-min-severity", "0",
        "--events-auto-cache-only",
        "--events-auto-use-cached-empty",
        "--events-auto-cache-dir", $eventCacheDir,
        "--events-auto-cell-cache-dir", $eventCellCacheDir,
        "--api-base-url", $ApiBaseUrl,
        "--analysis-modes", "erosion_events_ml,abag",
        "--provider", "auto",
        "--dem-source", "cog",
        "--threshold", "200",
        "--ml-threshold", "0.05",
        "--resume",
        "--validate-chunk",
        "--fail-on-qa-error",
        "--continue-on-error",
        "--checkpoint-every", "100",
        "--min-field-area-ha", "0.05",
        "--field-id-whitelist-file", $whitelist,
        "--require-whitelist",
        "--use-existing-chunks",
        "--chunks-dir", $chunkDir,
        "--exports-dir", $ExportsDir
    )

    return $args
}

$manifestObj = [ordered]@{
    started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    window_start = $WindowStart
    window_end = $WindowEnd
    api_base_url = $ApiBaseUrl
    chunk_dir = $chunkDir
    combined_log = $combinedLog
    worker1 = @{
        start_chunk = $s1
        max_chunks = $w1
        exports_dir = $worker1Exports
        log = $worker1Log
        err_log = $worker1Err
    }
    worker2 = @{
        start_chunk = $s2
        max_chunks = $w2
        exports_dir = $worker2Exports
        log = $worker2Log
        err_log = $worker2Err
    }
}
$manifestObj | ConvertTo-Json -Depth 6 | Out-File -FilePath $manifest -Encoding utf8

"$(Get-Date -Format o) [RUN] start worker1=$s1+$w1 worker2=$s2+$w2 api=$ApiBaseUrl" | Out-File -FilePath $combinedLog -Encoding utf8
$null = New-Item -ItemType File -Force -Path $worker1Log, $worker2Log, $worker1Err, $worker2Err

$worker1Args = @("-u", $runner) + (New-WorkerCommand -WorkerTag "W1" -ChunkStart $s1 -ChunkCount $w1 -WorkerLog $worker1Log -ExportsDir $worker1Exports)
$worker2Args = @("-u", $runner) + (New-WorkerCommand -WorkerTag "W2" -ChunkStart $s2 -ChunkCount $w2 -WorkerLog $worker2Log -ExportsDir $worker2Exports)

$p1 = Start-Process -FilePath $python -WorkingDirectory $root -ArgumentList $worker1Args -RedirectStandardOutput $worker1Log -RedirectStandardError $worker1Err -WindowStyle Minimized -PassThru
$p2 = Start-Process -FilePath $python -WorkingDirectory $root -ArgumentList $worker2Args -RedirectStandardOutput $worker2Log -RedirectStandardError $worker2Err -WindowStyle Minimized -PassThru

$mergeScript1 = "Get-Content -Path '$worker1Log' -Wait | ForEach-Object { ""{0} [W1] {1}"" -f (Get-Date -Format o), `$_.ToString() | Out-File -FilePath '$combinedLog' -Append -Encoding utf8 }"
$mergeScript2 = "Get-Content -Path '$worker2Log' -Wait | ForEach-Object { ""{0} [W2] {1}"" -f (Get-Date -Format o), `$_.ToString() | Out-File -FilePath '$combinedLog' -Append -Encoding utf8 }"
$m1 = Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-Command", $mergeScript1) -WindowStyle Minimized -PassThru
$m2 = Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-Command", $mergeScript2) -WindowStyle Minimized -PassThru

$manifestObj.worker1.pid = $p1.Id
$manifestObj.worker2.pid = $p2.Id
$manifestObj.worker1.merge_pid = $m1.Id
$manifestObj.worker2.merge_pid = $m2.Id
$manifestObj | ConvertTo-Json -Depth 6 | Out-File -FilePath $manifest -Encoding utf8

Write-Output "[OK] Worker 1 PID: $($p1.Id)"
Write-Output "[OK] Worker 2 PID: $($p2.Id)"
Write-Output "[OK] Combined log: $combinedLog"
Write-Output "[OK] Manifest: $manifest"
