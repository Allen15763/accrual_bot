@echo off
REM 僅執行 unit 測試
cd /d "%~dp0\.."
python -m pytest tests/ -m unit -v --cov=accrual_bot --cov-report=term-missing %*
