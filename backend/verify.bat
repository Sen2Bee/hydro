@echo off
python verify_sachsen_integration.py > verification.log 2>&1
type verification.log
