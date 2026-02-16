@echo off
call C:\OSGeo4W\bin\o4w_env.bat
echo Running verification with OSGeo4W Python...
C:\OSGeo4W\bin\python.exe verify_sachsen_integration.py
if errorlevel 1 (
    echo VERIFICATION_CRASHED > verification_result.txt
)
