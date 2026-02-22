call C:\OSGeo4W\bin\o4w_env.bat
call "%~dp0resolve_python.bat"
if errorlevel 1 exit /b 1
cd backend
"%PYTHON_EXE%" test_core.py
