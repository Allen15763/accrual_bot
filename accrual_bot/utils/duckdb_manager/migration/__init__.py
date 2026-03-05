"""
DuckDB Manager Schema 遷移模組

提供 DataFrame Schema 與資料庫表格 Schema 的比對與遷移功能。

Example:
    >>> from duckdb_manager import DuckDBManager
    >>> from duckdb_manager.migration import SchemaDiff, SchemaMigrator
    >>>
    >>> with DuckDBManager("./data.duckdb") as db:
    ...     # 比對 schema
    ...     diff = SchemaDiff.compare(db, "users", new_df)
    ...     print(diff.report())
    ...
    ...     # 套用遷移
    ...     migrator = SchemaMigrator(db)
    ...     migrator.migrate("users", new_df, strategy="safe")
"""

from .schema_diff import SchemaDiff, ColumnChange, ChangeType
from .strategies import MigrationStrategy
from .migrator import SchemaMigrator

__all__ = [
    "SchemaDiff",
    "ColumnChange",
    "ChangeType",
    "MigrationStrategy",
    "SchemaMigrator",
]
