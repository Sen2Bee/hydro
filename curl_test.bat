@echo off
curl -X GET http://127.0.0.1:8001/
echo.
curl -X POST -F "file=@mock_dem.tif" http://127.0.0.1:8001/analyze
