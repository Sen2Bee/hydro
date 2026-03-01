param(
  [Parameter(Mandatory=$true)][string]$PythonExe,
  [Parameter(Mandatory=$true)][string]$RepoRoot,
  [Parameter(Mandatory=$true)][string]$LogFile,
  [Parameter(Mandatory=$true)][int]$StartChunk,
  [Parameter(Mandatory=$true)][int]$MaxChunks,
  [int]$ChunkSize = 1000,
  [Parameter(Mandatory=$true)][string]$ChunksDir,
  [Parameter(Mandatory=$true)][string]$ExportsDir,
  [string]$DemSource = "cog",
  [string]$EventsAutoSource = "icon2d",
  [string]$EventsAutoStart = "2023-04-01",
  [string]$EventsAutoEnd = "2023-10-31",
  [int]$EventsAutoTopN = 5,
  [int]$EventsAutoMinSeverity = 0,
  [string]$EventsAutoCacheDir = "data/events/sa_2km/icon2d_20230401_20231031/field_cache",
  [string]$EventsAutoCellCacheDir = "data/events/sa_2km/icon2d_20230401_20231031/cell_cache",
  [double]$EventsAutoWeatherCellKm = 2.0,
  [int]$CheckpointEvery = 50
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LogFile) | Out-Null
New-Item -ItemType Directory -Force -Path $ChunksDir | Out-Null
New-Item -ItemType Directory -Force -Path $ExportsDir | Out-Null

function Log-Line([string]$msg) {
  $line = "[" + (Get-Date -Format o) + "] " + $msg
  Add-Content -Path $LogFile -Value $line
}

Log-Line "worker_start start_chunk=$StartChunk max_chunks=$MaxChunks chunk_size=$ChunkSize"
Log-Line "paths chunks_dir=$ChunksDir exports_dir=$ExportsDir field_cache=$EventsAutoCacheDir cell_cache=$EventsAutoCellCacheDir weather_cell_km=$EventsAutoWeatherCellKm"

Push-Location $RepoRoot
try {
  & $PythonExe -u backend\run_field_event_batch_sa_chunks.py `
    --chunk-size $ChunkSize `
    --start-chunk $StartChunk `
    --max-chunks $MaxChunks `
    --chunks-dir $ChunksDir `
    --exports-dir $ExportsDir `
    --events-source auto `
    --events-auto-source $EventsAutoSource `
    --events-auto-start $EventsAutoStart `
    --events-auto-end $EventsAutoEnd `
    --events-auto-top-n $EventsAutoTopN `
    --events-auto-min-severity $EventsAutoMinSeverity `
    --events-auto-cache-dir $EventsAutoCacheDir `
    --events-auto-cell-cache-dir $EventsAutoCellCacheDir `
    --events-auto-weather-cell-km $EventsAutoWeatherCellKm `
    --events-auto-cache-only `
    --events-auto-use-cached-empty `
    --dem-source $DemSource `
    --checkpoint-every $CheckpointEvery `
    --continue-on-error 2>&1 | ForEach-Object {
      Log-Line "$_"
    }
  $code = $LASTEXITCODE
  Log-Line "worker_exit_code=$code"
  exit $code
}
finally {
  Pop-Location
}
