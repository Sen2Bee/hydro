@echo off
call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0..\resolve_python.bat"
if errorlevel 1 (
    echo VERIFICATION_CRASHED > verification_result.txt
    exit /b 1
)
echo Running verification with %PYTHON_EXE%...
"%PYTHON_EXE%" verify_sachsen_integration.py
if errorlevel 1 (
    echo VERIFICATION_CRASHED > verification_result.txt
)
