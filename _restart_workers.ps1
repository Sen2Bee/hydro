$wd = "D:\__GeoFlux\hydrowatch"
$runDir = "D:\__GeoFlux\hydrowatch\paper\exports\runs\stageb_spatial_filtered_2workers_20260317_073320"
$python = $null
$resolver = Join-Path $wd "resolve_python.bat"

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
    throw "Python interpreter could not be resolved."
}

$commonArgs = @(
    "-u", "backend\run_field_event_batch_sa_chunks.py",
    "--chunk-size", "1000",
    "--events-source", "auto",
    "--events-auto-source", "icon2d",
    "--events-auto-start", "2024-04-01",
    "--events-auto-end", "2024-10-31",
    "--events-auto-top-n", "5",
    "--events-auto-min-severity", "0",
    "--events-auto-cache-only",
    "--events-auto-use-cached-empty",
    "--events-auto-cache-dir", "data\events\sa_2km\icon2d_20240401_20241031",
    "--events-auto-cell-cache-dir", "data\events\sa_2km\icon2d_20240401_20241031\cell_cache",
    "--api-base-url", "http://127.0.0.1:8011",
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
    "--field-id-whitelist-file", "data\derived\whitelists\acker_ids.txt",
    "--require-whitelist",
    "--use-existing-chunks",
    "--chunks-dir", "paper\input\sa_chunks_icon2d_3y_spatial_filtered_50k\20240401_20241031"
)

# W1: chunks 1-25
$w1Args = $commonArgs + @("--start-chunk", "1", "--max-chunks", "25", "--exports-dir", "paper\exports\sa_chunks_icon2d_3y_spatial_filtered_50k\20240401_20241031_w1")
$w1 = Start-Process -FilePath $python -WorkingDirectory $wd -ArgumentList $w1Args -RedirectStandardOutput "$runDir\worker1_restart2.log" -RedirectStandardError "$runDir\worker1_restart2.err.log" -WindowStyle Minimized -PassThru
Write-Output "W1 PID: $($w1.Id)"

# W2: chunks 26-50
$w2Args = $commonArgs + @("--start-chunk", "26", "--max-chunks", "25", "--exports-dir", "paper\exports\sa_chunks_icon2d_3y_spatial_filtered_50k\20240401_20241031_w2")
$w2 = Start-Process -FilePath $python -WorkingDirectory $wd -ArgumentList $w2Args -RedirectStandardOutput "$runDir\worker2_restart2.log" -RedirectStandardError "$runDir\worker2_restart2.err.log" -WindowStyle Minimized -PassThru
Write-Output "W2 PID: $($w2.Id)"
