"""Datasource 測試用 fixtures"""
import pytest
import pandas as pd
import tempfile
import os

from accrual_bot.core.datasources.config import DataSourceConfig
from accrual_bot.core.datasources.base import DataSourceType


@pytest.fixture
def csv_config(sample_csv_file):
    """CSV DataSourceConfig，指向真實暫存檔"""
    return DataSourceConfig(
        source_type=DataSourceType.CSV,
        connection_params={'file_path': sample_csv_file},
        encoding='utf-8',
    )


@pytest.fixture
def excel_config(sample_excel_file):
    """Excel DataSourceConfig，指向真實暫存檔"""
    return DataSourceConfig(
        source_type=DataSourceType.EXCEL,
        connection_params={'file_path': sample_excel_file},
    )


@pytest.fixture
def sample_csv_file(tmp_path):
    """建立含範例資料的暫存 CSV 檔"""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'amount': [100.5, 200.0, 300.75],
    })
    file_path = str(tmp_path / 'sample.csv')
    df.to_csv(file_path, index=False, encoding='utf-8')
    return file_path


@pytest.fixture
def sample_excel_file(tmp_path):
    """建立含範例資料的暫存 Excel 檔"""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'amount': [100.5, 200.0, 300.75],
    })
    file_path = str(tmp_path / 'sample.xlsx')
    df.to_excel(file_path, index=False)
    return file_path
