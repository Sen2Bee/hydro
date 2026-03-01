param(
  [Parameter(Mandatory=$true)][string]$PythonExe,
  [Parameter(Mandatory=$true)][string]$RepoRoot,
  [Parameter(Mandatory=$true)][string]$FieldsGeoJson,
  [Parameter(Mandatory=$true)][string]$CacheDir,
  [Parameter(Mandatory=$true)][string]$OutCsv,
  [Parameter(Mandatory=$true)][string]$MasterLog,
  [Parameter(Mandatory=$true)][string]$DetailLog,
  [string]$EventsAutoSource = "icon2d",
  [string]$EventsAutoStart = "2023-04-01",
  [string]$EventsAutoEnd = "2023-10-31",
  [int]$EventsAutoTopN = 5,
  [int]$EventsAutoMinSeverity = 0,
  [int]$CheckpointEvery = 10
)

$ErrorActionPreference = "Stop"

function Log-Line([string]$msg) {
  $line = "[" + (Get-Date -Format o) + "] " + $msg
  Add-Content -Path $DetailLog -Value $line
  Add-Content -Path $MasterLog -Value $line
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $MasterLog) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $DetailLog) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null

Log-Line "Phase B rerun(start) top_n=$EventsAutoTopN min_severity=$EventsAutoMinSeverity checkpoint=$CheckpointEvery"
Log-Line "Phase B rerun(out_csv) $OutCsv"
Log-Line "Phase B rerun(cache_dir) $CacheDir"

Push-Location $RepoRoot
try {
  & $PythonExe -u backend\run_field_event_batch.py `
    --fields-geojson $FieldsGeoJson `
    --events-source auto `
    --events-auto-source $EventsAutoSource `
    --events-auto-start $EventsAutoStart `
    --events-auto-end $EventsAutoEnd `
    --events-auto-top-n $EventsAutoTopN `
    --events-auto-min-severity $EventsAutoMinSeverity `
    --events-auto-cache-dir $CacheDir `
    --events-auto-cache-only `
    --events-auto-use-cached-empty `
    --analysis-modes erosion_events_ml,abag `
    --provider auto `
    --dem-source cog `
    --threshold 200 `
    --ml-threshold 0.05 `
    --checkpoint-every $CheckpointEvery `
    --continue-on-error `
    --out-csv $OutCsv 2>&1 | ForEach-Object {
      Log-Line "$_"
    }
  $code = $LASTEXITCODE
  Log-Line "Phase B rerun(exit_code) $code"
  exit $code
}
finally {
  Pop-Location
}

