"""
DuckDB Manager 工具模組

包含日誌、類型映射、SQL 安全工具等。
"""

from .logging import get_logger, NullLogger, LoggerProtocol
from .type_mapping import get_duckdb_dtype, PANDAS_TO_DUCKDB_MAPPING
from .query_builder import (
    SafeSQL,
    quote_identifier,
    escape_string,
    quote_value,
    is_safe_identifier,
)

__all__ = [
    # 日誌
    "get_logger",
    "NullLogger",
    "LoggerProtocol",
    # 類型映射
    "get_duckdb_dtype",
    "PANDAS_TO_DUCKDB_MAPPING",
    # SQL 安全工具
    "SafeSQL",
    "quote_identifier",
    "escape_string",
    "quote_value",
    "is_safe_identifier",
]
