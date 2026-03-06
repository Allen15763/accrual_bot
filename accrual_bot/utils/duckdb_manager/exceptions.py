"""
DuckDB Manager 自定義異常模組

所有異常類都以 DuckDB 前綴命名，避免與 Python 內建異常衝突。
為向後相容，保留舊名稱作為別名 (會發出 DeprecationWarning)。
"""

import warnings


class DuckDBManagerError(Exception):
    """DuckDB Manager 基礎異常類"""
    pass


class DuckDBConnectionError(DuckDBManagerError):
    """
    資料庫連線錯誤

    Attributes:
        db_path: 資料庫路徑
        message: 錯誤訊息
    """

    def __init__(self, db_path: str, message: str = None):
        self.db_path = db_path
        self.message = message or f"無法連線到資料庫: {db_path}"
        super().__init__(self.message)


class DuckDBTableError(DuckDBManagerError):
    """
    表格操作錯誤的基礎類

    Attributes:
        table_name: 表格名稱
        message: 錯誤訊息
    """

    def __init__(self, table_name: str, message: str = None):
        self.table_name = table_name
        self.message = message or f"表格操作錯誤: {table_name}"
        super().__init__(self.message)


class DuckDBTableExistsError(DuckDBTableError):
    """表格已存在錯誤"""

    def __init__(self, table_name: str):
        super().__init__(
            table_name,
            f"表格 '{table_name}' 已存在"
        )


class DuckDBTableNotFoundError(DuckDBTableError):
    """表格不存在錯誤"""

    def __init__(self, table_name: str):
        super().__init__(
            table_name,
            f"表格 '{table_name}' 不存在"
        )


class DuckDBQueryError(DuckDBManagerError):
    """
    查詢執行錯誤

    Attributes:
        query: SQL 查詢語句
        original_error: 原始異常
    """

    def __init__(self, query: str, original_error: Exception = None):
        self.query = query
        self.original_error = original_error
        truncated_query = query[:200] + "..." if len(query) > 200 else query
        message = f"查詢執行失敗: {truncated_query}"
        if original_error:
            message += f"\n原始錯誤: {original_error}"
        super().__init__(message)


class DuckDBDataValidationError(DuckDBManagerError):
    """
    資料驗證錯誤

    Attributes:
        column_name: 欄位名稱
        expected_type: 預期類型
        invalid_count: 無效資料筆數
    """

    def __init__(self, column_name: str, expected_type: str, invalid_count: int):
        self.column_name = column_name
        self.expected_type = expected_type
        self.invalid_count = invalid_count
        super().__init__(
            f"欄位 '{column_name}' 有 {invalid_count} 筆資料"
            f"無法轉換為 {expected_type}"
        )


class DuckDBTransactionError(DuckDBManagerError):
    """
    事務處理錯誤

    Attributes:
        operation_index: 失敗的操作索引
        message: 錯誤訊息
    """

    def __init__(self, operation_index: int, message: str = None):
        self.operation_index = operation_index
        self.message = message or f"事務在第 {operation_index} 個操作失敗"
        super().__init__(self.message)


class DuckDBConfigurationError(DuckDBManagerError):
    """
    配置錯誤

    Attributes:
        config_key: 配置鍵名
        message: 錯誤訊息
    """

    def __init__(self, config_key: str, message: str = None):
        self.config_key = config_key
        self.message = message or f"配置錯誤: {config_key}"
        super().__init__(self.message)


class DuckDBMigrationError(DuckDBManagerError):
    """
    Schema 遷移錯誤

    Attributes:
        table_name: 表格名稱
        message: 錯誤訊息
    """

    def __init__(self, table_name: str, message: str = None):
        self.table_name = table_name
        self.message = message or f"Schema 遷移錯誤: {table_name}"
        super().__init__(self.message)


# ========== 向後相容別名 (已棄用) ==========
# 使用這些別名會發出 DeprecationWarning

def _deprecated_alias(old_name: str, new_class: type) -> type:
    """建立已棄用的別名類"""

    class DeprecatedAlias(new_class):
        def __init__(self, *args, **kwargs):
            warnings.warn(
                f"'{old_name}' 已棄用，請使用 '{new_class.__name__}'",
                DeprecationWarning,
                stacklevel=2
            )
            super().__init__(*args, **kwargs)

    DeprecatedAlias.__name__ = old_name
    DeprecatedAlias.__qualname__ = old_name
    return DeprecatedAlias


# 舊名稱別名 (向後相容)
ConnectionError = _deprecated_alias("ConnectionError", DuckDBConnectionError)
TableError = _deprecated_alias("TableError", DuckDBTableError)
TableExistsError = _deprecated_alias("TableExistsError", DuckDBTableExistsError)
TableNotFoundError = _deprecated_alias("TableNotFoundError", DuckDBTableNotFoundError)
QueryError = _deprecated_alias("QueryError", DuckDBQueryError)
DataValidationError = _deprecated_alias("DataValidationError", DuckDBDataValidationError)
TransactionError = _deprecated_alias("TransactionError", DuckDBTransactionError)
ConfigurationError = _deprecated_alias("ConfigurationError", DuckDBConfigurationError)
