param(
  [string]$RepoRoot = "",
  [string]$FieldsGeoJson = "paper\input\schlaege_50k.geojson",
  [string]$CacheRoot = "paper\cache\auto_events_icon2d_t05_2km",
  [string]$CellCacheRoot = "paper\cache\auto_events_icon2d_t05_2km_cell",
  [string]$ExportRoot = "paper\exports\precompute_3y_3workers",
  [string]$Source = "icon2d",
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
$root = if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
  (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
} else {
  (Resolve-Path $RepoRoot).Path
}
Set-Location $root

function Fmt([double]$v) {
  return $v.ToString([System.Globalization.CultureInfo]::InvariantCulture)
}

$tag = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$exportDir = Join-Path $root $ExportRoot
$cacheBase = Join-Path $root $CacheRoot
$cellCacheBase = Join-Path $root $CellCacheRoot
$autoDir = Join-Path $exportDir "automation"
New-Item -ItemType Directory -Force -Path $exportDir | Out-Null
New-Item -ItemType Directory -Force -Path $autoDir | Out-Null
New-Item -ItemType Directory -Force -Path $cacheBase | Out-Null
New-Item -ItemType Directory -Force -Path $cellCacheBase | Out-Null

$fieldsPath = Join-Path $root $FieldsGeoJson
if (-not (Test-Path $fieldsPath)) {
  throw "Fields GeoJSON nicht gefunden: $fieldsPath"
}

$windows = @(
  @{ key = "2023"; start = "2023-04-01"; end = "2023-10-31" },
  @{ key = "2024"; start = "2024-04-01"; end = "2024-10-31" },
  @{ key = "2025"; start = "2025-04-01"; end = "2025-10-31" }
)

$manifest = @{
  started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  tag = $tag
  root = $root
  fields_geojson = $fieldsPath
  source = $Source
  top_n = $TopN
  min_severity = $MinSeverity
  weather_cell_km = (Fmt $WeatherCellKm)
  request_retries = $RequestRetries
  retry_backoff_initial_s = (Fmt $BackoffInitialS)
  retry_backoff_max_s = (Fmt $BackoffMaxS)
  min_interval_s = (Fmt $MinIntervalS)
  checkpoint_every = $CheckpointEvery
  workers = @()
}

foreach ($w in $windows) {
  $cacheDir = Join-Path $cacheBase ("{0}_{1}" -f $w.start.Replace("-", ""), $w.end.Replace("-", ""))
  $cellCacheDir = Join-Path $cellCacheBase ("{0}_{1}" -f $w.start.Replace("-", ""), $w.end.Replace("-", ""))
  New-Item -ItemType Directory -Force -Path $cacheDir | Out-Null
  New-Item -ItemType Directory -Force -Path $cellCacheDir | Out-Null

  $csvOut = Join-Path $exportDir ("precompute_{0}_{1}.csv" -f $w.key, $tag)
  $logOut = Join-Path $exportDir ("precompute_{0}_{1}.log" -f $w.key, $tag)

  $cmd = @(
    "run_precompute_auto_events_cache.bat",
    "--fields-geojson", "`"$fieldsPath`"",
    "--cache-dir", "`"$cacheDir`"",
    "--cell-cache-dir", "`"$cellCacheDir`"",
    "--source", $Source,
    "--start", $w.start,
    "--end", $w.end,
    "--top-n", $TopN,
    "--min-severity", $MinSeverity,
    "--weather-cell-km", (Fmt $WeatherCellKm),
    "--request-retries", $RequestRetries,
    "--retry-backoff-initial-s", (Fmt $BackoffInitialS),
    "--retry-backoff-max-s", (Fmt $BackoffMaxS),
    "--min-interval-s", (Fmt $MinIntervalS),
    "--checkpoint-every", $CheckpointEvery,
    "--out-csv", "`"$csvOut`"",
    "--log-file", "`"$logOut`""
  ) -join " "

  if ($Headless) {
    $ps = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "cd /d `"$root`" && $cmd" -PassThru -WindowStyle Hidden
  } else {
    $ps = Start-Process -FilePath "cmd.exe" -ArgumentList "/k", "cd /d `"$root`" && $cmd" -PassThru
  }

  $worker = @{
    key = $w.key
    window_start = $w.start
    window_end = $w.end
    pid = $ps.Id
    cache_dir = $cacheDir
    cell_cache_dir = $cellCacheDir
    out_csv = $csvOut
    log_file = $logOut
    command = $cmd
  }
  $manifest.workers += $worker
  Write-Host ("[StageA-{0}] pid={1} log={2}" -f $w.key, $ps.Id, $logOut)
}

$manifestPath = Join-Path $autoDir ("stage_a_3workers_{0}.json" -f $tag)
$manifest | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $manifestPath
Write-Host ("[OK] Manifest: {0}" -f $manifestPath)


