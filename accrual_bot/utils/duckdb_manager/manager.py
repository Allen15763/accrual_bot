"""
DuckDB Manager 核心模組

高可用、可移植的 DuckDB 管理器，支援:
- 多種配置方式 (DuckDBConfig, dict, str 路徑, TOML, YAML)
- 可插拔日誌系統
- 完整的 CRUD 操作
- 資料清理與轉換
- 事務處理
- Schema 遷移 (透過 migration 模組)

Example:
    # 方式 1: 直接傳路徑
    with DuckDBManager("./data.duckdb") as db:
        df = db.query_to_df("SELECT * FROM users")

    # 方式 2: 使用配置物件
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

    # 方式 4: 從 TOML 配置檔案
    config = DuckDBConfig.from_toml("config.toml", section="database")
    with DuckDBManager(config) as db:
        ...

    # 方式 5: 從 YAML 配置檔案
    config = DuckDBConfig.from_yaml("config.yaml", section="database")
    with DuckDBManager(config) as db:
        ...
"""

import duckdb
from typing import Optional, Dict, Any, Union
from pathlib import Path

from .config import DuckDBConfig
from .utils.logging import get_logger
from .exceptions import DuckDBConnectionError
from .operations import (
    CRUDMixin,
    TableManagementMixin,
    DataCleaningMixin,
    TransactionMixin,
)


class DuckDBManager(
    CRUDMixin,
    TableManagementMixin,
    DataCleaningMixin,
    TransactionMixin
):
    """
    高可用 DuckDB 管理器

    組合多個 Mixin 提供完整的資料庫操作功能:
    - CRUDMixin: 建立、讀取、更新、刪除操作
    - TableManagementMixin: 表格結構管理
    - DataCleaningMixin: 資料清理與轉換
    - TransactionMixin: 事務處理與資料驗證

    支援多種配置方式，可作為獨立模組移植到其他專案。

    Attributes:
        config: DuckDBConfig 配置物件
        logger: 日誌器實例
        conn: DuckDB 連線物件

    Example:
        # 基本用法
        with DuckDBManager("./data.duckdb") as db:
            df = db.query_to_df("SELECT * FROM users")
            db.create_table_from_df("new_table", df)

        # 使用配置物件
        config = DuckDBConfig(
            db_path="./data.duckdb",
            timezone="Asia/Taipei",
            log_level="DEBUG"
        )
        with DuckDBManager(config) as db:
            db.backup_table("users", backup_format="parquet")
    """

    def __init__(
        self,
        config: Union[DuckDBConfig, Dict[str, Any], str, Path, None] = None
    ):
        """
        初始化 DuckDB 管理器

        Args:
            config: 配置，可以是:
                - DuckDBConfig 實例
                - dict 配置字典
                - str 或 Path 資料庫路徑
                - None 使用預設 :memory: 模式

        Raises:
            DuckDBConnectionError: 連線失敗時
            TypeError: 配置類型不支援時
        """
        self.config = self._resolve_config(config)
        self.logger = self._setup_logger()
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

        # 建立連線
        self._connect()

        # 設定時區
        self._setup_timezone()

    def _resolve_config(
        self,
        config: Union[DuckDBConfig, Dict[str, Any], str, Path, None]
    ) -> DuckDBConfig:
        """
        解析配置

        Args:
            config: 原始配置輸入

        Returns:
            DuckDBConfig: 解析後的配置物件

        Raises:
            TypeError: 配置類型不支援時
        """
        if config is None:
            return DuckDBConfig()
        if isinstance(config, DuckDBConfig):
            return config
        if isinstance(config, dict):
            return DuckDBConfig.from_dict(config)
        if isinstance(config, (str, Path)):
            return DuckDBConfig(db_path=str(config))
        raise TypeError(f"不支援的配置類型: {type(config)}")

    def _setup_logger(self):
        """
        設定日誌器

        Returns:
            日誌器實例
        """
        return get_logger(
            name="duckdb_manager",
            level=self.config.log_level,
            external_logger=self.config.logger,
        )

    def _setup_timezone(self):
        """
        設定時區

        使用 DuckDB 內建時區設定，避免影響全域環境變數。
        """
        try:
            # 使用 DuckDB 內建時區設定
            self.conn.sql(f"SET timezone='{self.config.timezone}'")
            self.logger.debug(f"時區設定為: {self.config.timezone}")
        except Exception as e:
            # 如果 DuckDB 內建設定失敗，回退到環境變數方式
            import os
            import time

            os.environ['TZ'] = self.config.timezone

            # 只在 Unix 系統上調用 tzset
            if hasattr(time, 'tzset'):
                try:
                    time.tzset()
                except Exception:
                    pass

            self.logger.debug(
                f"使用環境變數設定時區: {self.config.timezone} (回退模式)"
            )

    def _connect(self):
        """
        建立資料庫連線

        Raises:
            DuckDBConnectionError: 連線失敗時
        """
        try:
            self.conn = duckdb.connect(
                self.config.db_path,
                read_only=self.config.read_only,
            )
            self.logger.info(f"成功連接到 DuckDB: {self.config.db_path}")
        except Exception as e:
            self.logger.error(f"連接資料庫失敗: {e}")
            raise DuckDBConnectionError(self.config.db_path, str(e))

    def close(self):
        """
        關閉資料庫連接
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("資料庫連接已關閉")

    def __enter__(self):
        """Context manager 入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 出口"""
        self.close()

    def __repr__(self) -> str:
        return f"DuckDBManager(db_path='{self.config.db_path}')"

    # ========== 便利屬性 ==========

    @property
    def database_path(self) -> str:
        """資料庫路徑"""
        return self.config.db_path

    @property
    def is_memory_db(self) -> bool:
        """是否為記憶體資料庫"""
        return self.config.db_path == ":memory:"

    @property
    def is_connected(self) -> bool:
        """是否已連線"""
        return self.conn is not None
