$env:ST_COG_DIR = "D:\data\st_dgm1_cog"
$env:WEATHER_PROVIDER = "icon2d"
$env:ICON2D_TRANSPORT = "direct"
$env:ICON2D_BATCH_PATH = "/weather/batch"
$env:RADAR_PROVIDER = "dwd_radolan"
$env:RADAR_MAX_HOURS = "4320"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$resolver = Join-Path $scriptDir "resolve_python.bat"
$pythonExe = $null

if ($env:PYTHON_EXE -and (Test-Path $env:PYTHON_EXE)) {
  $pythonExe = $env:PYTHON_EXE
} elseif (Test-Path $resolver) {
  $resolved = cmd.exe /d /c "call `"$resolver`" && echo PYTHON_EXE=%PYTHON_EXE%"
  if ($LASTEXITCODE -eq 0) {
    $line = $resolved | Select-Object -Last 1
    if ($line -match "^PYTHON_EXE=(.+)$" -and (Test-Path $Matches[1])) {
      $pythonExe = $Matches[1]
    }
  }
}

if (-not $pythonExe) {
  $cmd = Get-Command python -ErrorAction SilentlyContinue
  if ($cmd) {
    $pythonExe = $cmd.Source
  }
}

if (-not $pythonExe -or -not (Test-Path $pythonExe)) {
  throw "Python interpreter could not be resolved."
}

$proc = Start-Process -FilePath $pythonExe `
    -ArgumentList @("-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8011", "--workers", "3") `
    -WorkingDirectory "D:\__GeoFlux\hydrowatch\backend" `
    -WindowStyle Minimized `
    -PassThru
Write-Output "Backend PID: $($proc.Id)"
