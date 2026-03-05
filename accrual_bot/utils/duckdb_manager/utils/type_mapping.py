"""
Pandas 到 DuckDB 的類型映射模組
"""

from typing import Dict

# Pandas dtype 到 DuckDB 類型的映射表
PANDAS_TO_DUCKDB_MAPPING: Dict[str, str] = {
    # 整數類型
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "Int8": "TINYINT",      # nullable int
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",

    # 無符號整數
    "uint8": "UTINYINT",
    "uint16": "USMALLINT",
    "uint32": "UINTEGER",
    "uint64": "UBIGINT",
    "UInt8": "UTINYINT",
    "UInt16": "USMALLINT",
    "UInt32": "UINTEGER",
    "UInt64": "UBIGINT",

    # 浮點數類型
    "float16": "REAL",
    "float32": "REAL",
    "float64": "DOUBLE",
    "Float32": "REAL",
    "Float64": "DOUBLE",

    # 字串類型
    "object": "VARCHAR",
    "string": "VARCHAR",
    "str": "VARCHAR",
    "category": "VARCHAR",

    # 布林類型
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",

    # 日期時間類型
    "datetime64[ns]": "TIMESTAMP",
    "datetime64[us]": "TIMESTAMP",
    "datetime64[ms]": "TIMESTAMP",
    "datetime64[s]": "TIMESTAMP",
    "timedelta64[ns]": "INTERVAL",

    # 日期類型
    "date": "DATE",
}


def get_duckdb_dtype(pandas_dtype: str) -> str:
    """
    將 Pandas dtype 轉換為 DuckDB 類型

    Args:
        pandas_dtype: Pandas 資料類型字串

    Returns:
        str: 對應的 DuckDB 類型

    Example:
        >>> get_duckdb_dtype("int64")
        'BIGINT'
        >>> get_duckdb_dtype("datetime64[ns]")
        'TIMESTAMP'
        >>> get_duckdb_dtype("unknown_type")
        'VARCHAR'
    """
    # 直接匹配
    if pandas_dtype in PANDAS_TO_DUCKDB_MAPPING:
        return PANDAS_TO_DUCKDB_MAPPING[pandas_dtype]

    # 處理複雜的 datetime 格式 (e.g., datetime64[ns, UTC])
    if "datetime64" in pandas_dtype:
        return "TIMESTAMP"

    # 處理 timedelta
    if "timedelta64" in pandas_dtype:
        return "INTERVAL"

    # 預設返回 VARCHAR
    return "VARCHAR"
