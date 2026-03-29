@echo off
REM 產生覆蓋率報告，最低門檻 80%%
cd /d "%~dp0\.."
python -m pytest tests/ --cov=accrual_bot --cov-report=html --cov-report=term-missing --cov-fail-under=80 %*
echo.
echo Coverage HTML report: htmlcov\index.html
