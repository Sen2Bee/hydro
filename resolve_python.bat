@echo off
setlocal

REM Resolve a working Python interpreter and export PYTHON_EXE to caller.
REM Priority:
REM   1) OSGeo4W Python (if healthy in current env)
REM   2) User Python 3.13
REM   3) python on PATH

set "CAND1=C:\OSGeo4W\bin\python.exe"
set "CAND2=C:\Users\thoko\AppData\Local\Programs\Python\Python313\python.exe"

if exist "%CAND1%" (
  "%CAND1%" -c "import encodings" >nul 2>nul
  if not errorlevel 1 (
    endlocal & set "PYTHON_EXE=%CAND1%" & exit /b 0
  )
)

if exist "%CAND2%" (
  "%CAND2%" -c "import encodings" >nul 2>nul
  if not errorlevel 1 (
    endlocal & set "PYTHON_EXE=%CAND2%" & exit /b 0
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
