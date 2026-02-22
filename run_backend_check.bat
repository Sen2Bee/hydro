@echo off
setlocal

REM Run a quick sanity check for the backend Python environment.
REM This uses the same OSGeo4W environment as run_backend.bat.

call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 goto :fail_python
cd /d %~dp0

echo(
echo === Hydrowatch Backend Python Env Check ===
echo(

echo [1/4] PATH python resolution:
where python 2>nul
echo selected: %PYTHON_EXE%
echo(

echo [2/4] Python identity:
"%PYTHON_EXE%" -c "import sys; print('sys.executable:', sys.executable); print('sys.version:', sys.version.replace('\n',' ')); print('sys.prefix:', sys.prefix); print('sys.base_prefix:', getattr(sys,'base_prefix',None)); print('sys.path[0:6]:', sys.path[0:6])"
if errorlevel 1 goto :fail_python
echo(

echo [3/4] Core imports:
"%PYTHON_EXE%" -c "import encodings, json, math, ctypes; print('ok core imports')"
if errorlevel 1 goto :fail_core
echo(

echo [4/4] Backend deps imports:
"%PYTHON_EXE%" -c "import fastapi, uvicorn, rasterio, pyproj, pysheds, numpy, requests; print('ok backend deps')"
if errorlevel 1 goto :fail_deps

echo(
echo OK: Python environment looks good.
exit /b 0

:fail_python
echo(
echo ERROR: python failed to start. This usually means a broken PYTHONHOME/sys.prefix or missing stdlib.
echo Try: close terminals, re-run this script, or check environment variables that affect Python.
exit /b 1

:fail_core
echo(
echo ERROR: core imports failed (encodings). Python install/env is broken.
exit /b 1

:fail_deps
echo(
echo ERROR: one or more backend dependencies are missing/broken.
echo You can usually fix this by running: run_backend.bat (it installs some deps) or pip install the missing package.
exit /b 1
