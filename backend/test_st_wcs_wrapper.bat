@echo off
echo Starting test... > test_output.txt
python -u test_st_wcs.py >> test_output.txt 2>&1
echo Done. >> test_output.txt
