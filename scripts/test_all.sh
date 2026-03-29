#!/bin/bash
# 執行所有測試並產生覆蓋率報告
cd "$(dirname "$0")/.." || exit 1
python -m pytest tests/ -v --cov=accrual_bot --cov-report=html --cov-report=term-missing "$@"
