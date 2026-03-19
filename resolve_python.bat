@echo off
setlocal

REM Resolve a working Python interpreter and export PYTHON_EXE to caller.
REM Priority:
REM   1) Existing PYTHON_EXE environment variable (if healthy)
REM   2) OSGeo4W Python (if healthy in current env)
REM   3) py launcher (python3) resolved to concrete exe
REM   4) python on PATH

set "CAND1=C:\OSGeo4W\bin\python.exe"

if defined PYTHON_EXE (
  if exist "%PYTHON_EXE%" (
    "%PYTHON_EXE%" -c "import encodings" >nul 2>nul
    if not errorlevel 1 (
      endlocal & set "PYTHON_EXE=%PYTHON_EXE%" & exit /b 0
    )
  )
)

if exist "%CAND1%" (
  "%CAND1%" -c "import encodings" >nul 2>nul
  if not errorlevel 1 (
    endlocal & set "PYTHON_EXE=%CAND1%" & exit /b 0
  )
)

where py >nul 2>nul
if not errorlevel 1 (
  py -3 -c "import encodings,sys;print(sys.executable)" >nul 2>nul
  if not errorlevel 1 (
    for /f "usebackq delims=" %%I in (`py -3 -c "import sys;print(sys.executable)" 2^>nul`) do (
      if exist "%%I" (
        "%%I" -c "import encodings" >nul 2>nul
        if not errorlevel 1 (
          endlocal & set "PYTHON_EXE=%%I" & exit /b 0
        )
      )
    )
  )
)

where python >nul 2>nul
if errorlevel 1 (
  endlocal
  exit /b 1
)

python -c "import encodings" >nul 2>nul
if errorlevel 1 (
  endlocal
  exit /b 1
)

for /f "delims=" %%I in ('where python') do (
  endlocal & set "PYTHON_EXE=%%I" & exit /b 0
)

endlocal
exit /b 1
