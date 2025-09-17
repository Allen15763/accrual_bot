"""
數據源模組
提供統一的數據源抽象層，支援多種數據格式和儲存方式
"""

from .base import DataSource, DataSourceType
from .config import DataSourceConfig
from .factory import DataSourceFactory

# 具體實現
from .excel_source import ExcelSource
from .csv_source import CSVSource
from .parquet_source import ParquetSource
from .duckdb_source import DuckDBSource

__all__ = [
    'DataSource',
    'DataSourceType',
    'DataSourceConfig',
    'DataSourceFactory',
    'ExcelSource',
    'CSVSource',
    'ParquetSource',
    'DuckDBSource'
]
