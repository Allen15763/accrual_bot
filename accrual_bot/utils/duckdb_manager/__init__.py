"""
DuckDB Manager - 高可用、可移植的 DuckDB 管理模組

這是一個獨立的 DuckDB 管理模組，可以作為資料夾直接複製到其他專案使用。

基本用法:
    from duckdb_manager import DuckDBManager

    # 方式 1: 直接傳路徑
    with DuckDBManager("./data.duckdb") as db:
        df = db.query_to_df("SELECT * FROM users")

    # 方式 2: 使用配置物件
    from duckdb_manager import DuckDBConfig

    config = DuckDBConfig(
        db_path="./data.duckdb",
        timezone="Asia/Taipei",
        log_level="DEBUG"
    )
    with DuckDBManager(config) as db:
        db.create_table_from_df("users", df)

    # 方式 3: 使用字典配置
    with DuckDBManager({"db_path": "./data.duckdb"}) as db:
        db.insert_df_into_table("users", new_users)

    # 方式 4: 從 TOML 檔案載入配置
    config = DuckDBConfig.from_toml("config.toml", section="database")
    db = DuckDBManager(config)

    # 方式 5: 從 YAML 檔案載入配置
    config = DuckDBConfig.from_yaml("config.yaml", section="database")
    db = DuckDBManager(config)

整合專案日誌:
    from duckdb_manager import DuckDBManager, DuckDBConfig
    from my_project.logging import get_logger

    config = DuckDBConfig(
        db_path="./data.duckdb",
        logger=get_logger("database.duckdb")  # 注入專案日誌
    )
    db = DuckDBManager(config)

禁用日誌:
    from duckdb_manager import DuckDBManager, DuckDBConfig
    from duckdb_manager.utils import NullLogger

    config = DuckDBConfig(logger=NullLogger())
    db = DuckDBManager(config)

Schema 遷移:
    from duckdb_manager import DuckDBManager
    from duckdb_manager.migration import SchemaMigrator

    with DuckDBManager("./data.duckdb") as db:
        migrator = SchemaMigrator(db)

        # 比對 schema
        diff = migrator.compare_schema("users", new_df)
        print(diff.report())

        # 執行遷移
        result = migrator.migrate("users", new_df, strategy="safe")
"""

from .config import DuckDBConfig
from .manager import DuckDBManager
from .exceptions import (
    # 新版異常類 (推薦使用)
    DuckDBManagerError,
    DuckDBConnectionError,
    DuckDBTableError,
    DuckDBTableExistsError,
    DuckDBTableNotFoundError,
    DuckDBQueryError,
    DuckDBDataValidationError,
    DuckDBTransactionError,
    DuckDBConfigurationError,
    DuckDBMigrationError,
    # 舊版異常類別名 (向後相容，已棄用)
    ConnectionError,
    TableError,
    TableExistsError,
    TableNotFoundError,
    QueryError,
    DataValidationError,
    TransactionError,
    ConfigurationError,
)

__version__ = "2.1.0"
__author__ = "SPE Bank Recon Team"

__all__ = [
    # 核心類
    "DuckDBManager",
    "DuckDBConfig",

    # 新版異常類 (推薦使用)
    "DuckDBManagerError",
    "DuckDBConnectionError",
    "DuckDBTableError",
    "DuckDBTableExistsError",
    "DuckDBTableNotFoundError",
    "DuckDBQueryError",
    "DuckDBDataValidationError",
    "DuckDBTransactionError",
    "DuckDBConfigurationError",
    "DuckDBMigrationError",

    # 舊版異常類別名 (向後相容，已棄用)
    "ConnectionError",
    "TableError",
    "TableExistsError",
    "TableNotFoundError",
    "QueryError",
    "DataValidationError",
    "TransactionError",
    "ConfigurationError",
]
