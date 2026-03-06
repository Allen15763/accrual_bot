"""
DuckDB Manager 操作模組

提供 CRUD、表格管理、資料清理、事務處理等操作的 Mixin 類。

Example:
    這些 Mixin 類被 DuckDBManager 組合使用，不建議直接實例化。

    >>> from duckdb_manager import DuckDBManager
    >>> with DuckDBManager("./data.duckdb") as db:
    ...     # CRUD 操作 (CRUDMixin)
    ...     db.create_table_from_df("users", df)
    ...     result = db.query_to_df("SELECT * FROM users")
    ...
    ...     # 表格管理 (TableManagementMixin)
    ...     tables = db.list_tables_with_info()
    ...     db.backup_table("users", backup_format="parquet")
    ...
    ...     # 資料清理 (DataCleaningMixin)
    ...     db.clean_numeric_column("users", "salary")
    ...     db.alter_column_type("users", "salary", "BIGINT")
    ...
    ...     # 事務處理 (TransactionMixin)
    ...     db.execute_transaction([
    ...         "UPDATE users SET status = 'active'",
    ...         "DELETE FROM users WHERE status = 'deleted'"
    ...     ])
"""

from .base import OperationMixin
from .crud import CRUDMixin
from .table_management import TableManagementMixin
from .data_cleaning import DataCleaningMixin
from .transaction import TransactionMixin

__all__ = [
    "OperationMixin",
    "CRUDMixin",
    "TableManagementMixin",
    "DataCleaningMixin",
    "TransactionMixin",
]
