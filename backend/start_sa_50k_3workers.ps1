param(
  [string]$PythonExe = "",
  [string]$RepoRoot = "D:\__GeoFlux\hydrowatch"
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path $RepoRoot).Path

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
  if ($env:PYTHON_EXE -and (Test-Path $env:PYTHON_EXE)) {
    $PythonExe = $env:PYTHON_EXE
  } else {
    $resolver = Join-Path $root "resolve_python.bat"
    if (Test-Path $resolver) {
      $resolved = cmd.exe /d /c "call `"$resolver`" && echo PYTHON_EXE=%PYTHON_EXE%"
      if ($LASTEXITCODE -eq 0) {
        $line = $resolved | Select-Object -Last 1
        if ($line -match "^PYTHON_EXE=(.+)$" -and (Test-Path $Matches[1])) {
          $PythonExe = $Matches[1]
        }
      }
    }
  }
}
if ([string]::IsNullOrWhiteSpace($PythonExe)) {
  $cmd = Get-Command python -ErrorAction SilentlyContinue
  if ($cmd) {
    $PythonExe = $cmd.Source
  }
}
if (-not (Test-Path $PythonExe)) {
  throw "Python not found: $PythonExe"
}

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$runRoot = Join-Path $RepoRoot ("paper\exports\automation\sa50k_3workers_" + $ts)
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$workers = @(
  @{ Name = "w1"; Start = 1;  Max = 17 },
  @{ Name = "w2"; Start = 18; Max = 17 },
  @{ Name = "w3"; Start = 35; Max = 16 }
)

$eventStoreRoot = Join-Path $RepoRoot "data\events\sa_2km\icon2d_20230401_20231031"
$fieldCacheDir = Join-Path $eventStoreRoot "field_cache"
$cellCacheDir = Join-Path $eventStoreRoot "cell_cache"
New-Item -ItemType Directory -Force -Path $fieldCacheDir | Out-Null
New-Item -ItemType Directory -Force -Path $cellCacheDir | Out-Null

$launched = @()
foreach ($w in $workers) {
  $name = [string]$w.Name
  $chunksDir = Join-Path $RepoRoot ("paper\input\sa_chunks_50k\" + $name)
  $exportsDir = Join-Path $RepoRoot ("paper\exports\sa_chunks_50k\" + $name)
  $logFile = Join-Path $runRoot ("worker_" + $name + ".log")

  $args = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", (Join-Path $RepoRoot "backend\run_chunk_worker_logged.ps1"),
    "-PythonExe", $PythonExe,
    "-RepoRoot", $RepoRoot,
    "-LogFile", $logFile,
    "-StartChunk", ([string]$w.Start),
    "-MaxChunks", ([string]$w.Max),
    "-ChunkSize", "1000",
    "-ChunksDir", $chunksDir,
    "-ExportsDir", $exportsDir,
    "-DemSource", "cog",
    "-EventsAutoSource", "icon2d",
    "-EventsAutoStart", "2023-04-01",
    "-EventsAutoEnd", "2023-10-31",
    "-EventsAutoTopN", "5",
    "-EventsAutoMinSeverity", "0",
    "-EventsAutoCacheDir", $fieldCacheDir,
    "-EventsAutoCellCacheDir", $cellCacheDir,
    "-EventsAutoWeatherCellKm", "2.0",
    "-CheckpointEvery", "50"
  )

  $p = Start-Process -FilePath "powershell.exe" -ArgumentList $args -WindowStyle Minimized -PassThru
  $launched += [pscustomobject]@{
    worker = $name
    pid = $p.Id
    start_chunk = $w.Start
    max_chunks = $w.Max
    log = $logFile
    chunks_dir = $chunksDir
    exports_dir = $exportsDir
  }
}

$manifest = Join-Path $runRoot "run_manifest.json"
$obj = [pscustomobject]@{
  launched_at = (Get-Date).ToString("o")
  repo_root = $RepoRoot
  python = $PythonExe
  target_fields = 50000
  chunk_size = 1000
  event_store = [pscustomobject]@{
    weather_cell_km = 2.0
    source = "icon2d"
    start = "2023-04-01"
    end = "2023-10-31"
    top_n = 5
    min_severity = 0
    field_cache_dir = $fieldCacheDir
    cell_cache_dir = $cellCacheDir
    stage_b_cache_only = $true
  }
  workers = $launched
}
$obj | ConvertTo-Json -Depth 5 | Set-Content -Path $manifest -Encoding UTF8

Write-Output ("run_root=" + $runRoot)
Write-Output ("manifest=" + $manifest)
foreach ($x in $launched) {
  Write-Output ("worker=" + $x.worker + " pid=" + $x.pid + " log=" + $x.log)
}
