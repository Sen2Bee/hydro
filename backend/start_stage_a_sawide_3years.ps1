param(
  [string]$RepoRoot = "D:\__GeoFlux\hydrowatch",
  [string]$PythonExe = "C:\Users\thoko\AppData\Local\Programs\Python\Python313\python.exe",
  [string]$SourceSqlite = "data\raw\sa_flurstuecke\cache\flurstuecke.sqlite",
  [int]$ChunkSize = 1000,
  [int]$TopN = 5,
  [int]$MinSeverity = 0,
  [double]$WeatherCellKm = 2.0,
  [int]$RequestRetries = 3,
  [double]$BackoffInitialS = 2.0,
  [double]$BackoffMaxS = 20.0,
  [double]$MinIntervalS = 1.5,
  [int]$CheckpointEvery = 100,
  [switch]$Headless
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path $RepoRoot).Path
Set-Location $root

function Fmt([double]$v) {
  return $v.ToString([System.Globalization.CultureInfo]::InvariantCulture)
}

if (-not (Test-Path $PythonExe)) {
  throw "Python nicht gefunden: $PythonExe"
}
$srcSqliteAbs = Join-Path $root $SourceSqlite
if (-not (Test-Path $srcSqliteAbs)) {
  throw "SQLite nicht gefunden: $srcSqliteAbs"
}

$tag = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$runRoot = Join-Path $root ("paper\exports\automation\stage_a_sawide_3years_" + $tag)
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$windows = @(
  @{ key = "20230401_20231031"; start = "2023-04-01"; end = "2023-10-31" },
  @{ key = "20240401_20241031"; start = "2024-04-01"; end = "2024-10-31" },
  @{ key = "20250401_20251031"; start = "2025-04-01"; end = "2025-10-31" }
)

$launched = @()
foreach ($w in $windows) {
  $winKey = [string]$w.key
  $chunksDir = Join-Path $root ("paper\input\sa_precompute_chunks\" + $winKey)
  $exportsDir = Join-Path $root ("paper\exports\sa_precompute\" + $winKey)
  $cacheRoot = Join-Path $root ("data\events\sa_2km\icon2d_" + $winKey)
  $fieldCache = Join-Path $cacheRoot "field_cache"
  $cellCache = Join-Path $cacheRoot "cell_cache"
  $logFile = Join-Path $runRoot ("stage_a_" + $winKey + ".log")

  New-Item -ItemType Directory -Force -Path $chunksDir | Out-Null
  New-Item -ItemType Directory -Force -Path $exportsDir | Out-Null
  New-Item -ItemType Directory -Force -Path $fieldCache | Out-Null
  New-Item -ItemType Directory -Force -Path $cellCache | Out-Null

  $cmd = @(
    "`"$PythonExe`"",
    "-u",
    "backend\precompute_sa_events_chunks.py",
    "--source-sqlite", "`"$srcSqliteAbs`"",
    "--chunk-size", $ChunkSize,
    "--start-chunk", "1",
    "--max-chunks", "0",
    "--chunks-dir", "`"$chunksDir`"",
    "--exports-dir", "`"$exportsDir`"",
    "--cache-dir", "`"$fieldCache`"",
    "--cell-cache-dir", "`"$cellCache`"",
    "--source", "icon2d",
    "--start", [string]$w.start,
    "--end", [string]$w.end,
    "--top-n", $TopN,
    "--min-severity", $MinSeverity,
    "--weather-cell-km", (Fmt $WeatherCellKm),
    "--request-retries", $RequestRetries,
    "--retry-backoff-initial-s", (Fmt $BackoffInitialS),
    "--retry-backoff-max-s", (Fmt $BackoffMaxS),
    "--min-interval-s", (Fmt $MinIntervalS),
    "--checkpoint-every", $CheckpointEvery,
    "--resume"
  ) -join " "

  $shell = "cd /d `"$root`" && $cmd > `"$logFile`" 2>&1"
  if ($Headless) {
    $p = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $shell -PassThru -WindowStyle Hidden
  } else {
    $p = Start-Process -FilePath "cmd.exe" -ArgumentList "/k", $shell -PassThru
  }

  $launched += [pscustomobject]@{
    year_window = $winKey
    start = $w.start
    end = $w.end
    pid = $p.Id
    log_file = $logFile
    exports_dir = $exportsDir
    field_cache_dir = $fieldCache
    cell_cache_dir = $cellCache
    command = $cmd
  }

  Write-Host ("[StageA-SA {0}] pid={1} log={2}" -f $winKey, $p.Id, $logFile)
}

$manifest = Join-Path $runRoot "run_manifest.json"
$obj = [pscustomobject]@{
  launched_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  repo_root = $root
  python = $PythonExe
  source_sqlite = $srcSqliteAbs
  chunk_size = $ChunkSize
  top_n = $TopN
  min_severity = $MinSeverity
  weather_cell_km = (Fmt $WeatherCellKm)
  request_retries = $RequestRetries
  retry_backoff_initial_s = (Fmt $BackoffInitialS)
  retry_backoff_max_s = (Fmt $BackoffMaxS)
  min_interval_s = (Fmt $MinIntervalS)
  checkpoint_every = $CheckpointEvery
  workers = $launched
}
$obj | ConvertTo-Json -Depth 6 | Set-Content -Path $manifest -Encoding UTF8
Write-Host ("[OK] Manifest: {0}" -f $manifest)

