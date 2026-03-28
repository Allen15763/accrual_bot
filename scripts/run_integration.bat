@echo off
REM 僅執行 integration 測試
cd /d "%~dp0\.."
python -m pytest tests/ -m integration -v --cov=accrual_bot --cov-report=term-missing %*
