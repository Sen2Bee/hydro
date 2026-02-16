@echo off
setlocal ENABLEDELAYEDEXPANSION

set "ROOT=%~dp0"
set "DCMD=C:\Program Files\Docker\Docker\resources\bin\docker.exe"
set "API=http://127.0.0.1:8002"

if not exist "%DCMD%" set "DCMD=docker"

echo [0/6] Check Docker daemon...
set "DOCKER_OK="
for /L %%I in (1,1,60) do (
  "%DCMD%" info >nul 2>nul
  if not errorlevel 1 (
    set "DOCKER_OK=1"
    goto :docker_done
  )
  powershell -NoProfile -Command "Start-Sleep -Seconds 1" >nul
)
:docker_done
if not defined DOCKER_OK (
  echo [ERROR] Docker daemon not reachable.
  echo         Please start Docker Desktop and ensure Linux containers are enabled.
  echo.
  echo --- docker context ls ---
  "%DCMD%" context ls
  exit /b 1
)

echo [1/6] Ensure containers are up...
"%DCMD%" compose -f "%ROOT%docker-compose.dev.yml" up -d >nul
if errorlevel 1 (
  echo [ERROR] docker compose up failed
  exit /b 1
)

echo [2/6] Wait for API health...
set "HEALTH_OK="
for /L %%I in (1,1,120) do (
  powershell -NoProfile -Command "try { $r=Invoke-RestMethod -Uri '%API%/health' -TimeoutSec 4; if($r.status -eq 'ok'){ exit 0 } else { exit 1 } } catch { exit 1 }"
  if not errorlevel 1 (
    set "HEALTH_OK=1"
    goto :health_done
  )
  powershell -NoProfile -Command "Start-Sleep -Seconds 1" >nul
)
:health_done
if not defined HEALTH_OK (
  echo [ERROR] API health check failed
  echo --- docker compose ps ---
  "%DCMD%" compose -f "%ROOT%docker-compose.dev.yml" ps
  echo --- api-service logs tail ---
  "%DCMD%" compose -f "%ROOT%docker-compose.dev.yml" logs --tail=80 api-service
  exit /b 1
)

echo [3/6] Seed demo tenant/project/model...
"%DCMD%" compose -f "%ROOT%docker-compose.dev.yml" exec -T postgres psql -U hydrowatch -d hydrowatch -c "INSERT INTO tenants (id, name, slug) VALUES ('11111111-1111-1111-1111-111111111111', 'Demo Tenant', 'demo') ON CONFLICT (slug) DO NOTHING; INSERT INTO projects (id, tenant_id, name) VALUES ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'Demo Project') ON CONFLICT (id) DO NOTHING; INSERT INTO models (id, key, name, category) VALUES ('33333333-3333-3333-3333-333333333333', 'd8-fast', 'D8 Fast', 'hydrology') ON CONFLICT (key) DO NOTHING;" >nul
if errorlevel 1 (
  echo [ERROR] DB seed failed
  exit /b 1
)

echo [4/6] Create job...
for /f %%J in ('powershell -NoProfile -Command "$payload=@{ project_id='22222222-2222-2222-2222-222222222222'; model_id='33333333-3333-3333-3333-333333333333'; parameters=@{ threshold=200; source='smoke-bat' } }; $body=ConvertTo-Json -InputObject $payload -Depth 5; $resp=Invoke-RestMethod -Uri '%API%/v1/jobs' -Method POST -ContentType 'application/json' -Body $body; $resp.id"') do set "JOB_ID=%%J"
if not defined JOB_ID (
  echo [ERROR] Could not create job
  exit /b 1
)
echo      Job ID: %JOB_ID%

echo [5/6] Poll job status...
set "FINAL_STATUS="
for /L %%I in (1,1,180) do (
  for /f %%S in ('powershell -NoProfile -Command "$r=Invoke-RestMethod -Uri '%API%/v1/jobs/%JOB_ID%' -Method GET; $r.status"') do set "STATUS=%%S"
  echo      Attempt %%I: !STATUS!
  if /I "!STATUS!"=="succeeded" (
    set "FINAL_STATUS=succeeded"
    goto :poll_done
  )
  if /I "!STATUS!"=="failed" (
    set "FINAL_STATUS=failed"
    goto :poll_done
  )
  powershell -NoProfile -Command "Start-Sleep -Seconds 1" >nul
)
:poll_done

echo [6/6] Result
if /I "%FINAL_STATUS%"=="succeeded" (
  echo [OK] Smoke test passed.
  exit /b 0
)
if /I "%FINAL_STATUS%"=="failed" (
  echo [ERROR] Job failed.
  exit /b 1
)

echo [ERROR] Job did not finish in time.
exit /b 1



