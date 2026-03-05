"""
DuckDB 管理器配置模組

支援多種配置方式:
- DuckDBConfig dataclass 實例
- dict 字典
- str 資料庫路徑
- TOML 檔案
- YAML 檔案
"""

from dataclasses import dataclass, field
from typing import Optional, Any
from pathlib import Path
import logging


@dataclass
class DuckDBConfig:
    """
    DuckDB 管理器配置

    Attributes:
        db_path: 資料庫檔案路徑，預設為 ":memory:" (記憶體模式)
        timezone: 時區設定，預設為 "Asia/Taipei"
        read_only: 是否以唯讀模式開啟資料庫
        connection_timeout: 連線逾時秒數
        logger: 外部注入的日誌器，為 None 時使用內建日誌
        log_level: 日誌級別 ("DEBUG", "INFO", "WARNING", "ERROR")
        enable_query_logging: 是否記錄 SQL 查詢
    """

    # 資料庫設定
    db_path: str = ":memory:"
    timezone: str = "Asia/Taipei"

    # 連線設定
    read_only: bool = False
    connection_timeout: int = 30

    # 日誌設定 (可插拔)
    logger: Optional[logging.Logger] = field(default=None, repr=False)
    log_level: str = "INFO"
    enable_query_logging: bool = True

    def __post_init__(self):
        """初始化後處理"""
        # 驗證 log_level
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_levels:
            raise ValueError(
                f"無效的 log_level: {self.log_level}，"
                f"有效值: {valid_levels}"
            )
        self.log_level = self.log_level.upper()

        # 驗證 db_path
        if self.db_path != ":memory:":
            path = Path(self.db_path)
            # 確保父目錄存在
            if path.parent and not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DuckDBConfig":
        """
        從字典建立配置

        Args:
            data: 配置字典

        Returns:
            DuckDBConfig: 配置實例

        Example:
            >>> config = DuckDBConfig.from_dict({
            ...     "db_path": "./data.duckdb",
            ...     "timezone": "Asia/Taipei"
            ... })
        """
        # 只取出 dataclass 定義的欄位
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {
            k: v for k, v in data.items()
            if k in valid_fields
        }
        return cls(**filtered_data)

    @classmethod
    def from_toml(
        cls,
        path: str | Path,
        section: str = "database"
    ) -> "DuckDBConfig":
        """
        從 TOML 檔案建立配置

        Args:
            path: TOML 檔案路徑
            section: 配置區段名稱，預設為 "database"

        Returns:
            DuckDBConfig: 配置實例

        Raises:
            FileNotFoundError: 檔案不存在
            KeyError: 指定的 section 不存在

        Example:
            >>> config = DuckDBConfig.from_toml("config.toml", section="database")
        """
        import tomllib

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"配置檔案不存在: {path}")

        with open(path, "rb") as f:
            toml_data = tomllib.load(f)

        if section not in toml_data:
            raise KeyError(f"配置檔案中找不到 [{section}] 區段")

        return cls.from_dict(toml_data[section])

    @classmethod
    def from_yaml(
        cls,
        path: str | Path,
        section: str = "database"
    ) -> "DuckDBConfig":
        """
        從 YAML 檔案建立配置

        Args:
            path: YAML 檔案路徑 (.yaml 或 .yml)
            section: 配置區段名稱，預設為 "database"

        Returns:
            DuckDBConfig: 配置實例

        Raises:
            FileNotFoundError: 檔案不存在
            KeyError: 指定的 section 不存在
            ImportError: 未安裝 PyYAML 套件

        Example:
            >>> config = DuckDBConfig.from_yaml("config.yaml", section="database")

        Note:
            需要安裝 PyYAML: pip install pyyaml
        """
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "需要安裝 PyYAML 套件才能讀取 YAML 配置檔案。"
                "請執行: pip install pyyaml"
            )

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"配置檔案不存在: {path}")

        with open(path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        if yaml_data is None:
            raise ValueError(f"YAML 檔案為空或格式錯誤: {path}")

        if section not in yaml_data:
            raise KeyError(f"配置檔案中找不到 '{section}' 區段")

        return cls.from_dict(yaml_data[section])

    @classmethod
    def from_path(cls, db_path: str | Path) -> "DuckDBConfig":
        """
        從資料庫路徑建立配置 (使用預設值)

        Args:
            db_path: 資料庫檔案路徑

        Returns:
            DuckDBConfig: 配置實例

        Example:
            >>> config = DuckDBConfig.from_path("./data.duckdb")
        """
        return cls(db_path=str(db_path))

    def to_dict(self) -> dict[str, Any]:
        """
        轉換為字典 (排除 logger)

        Returns:
            dict: 配置字典
        """
        return {
            "db_path": self.db_path,
            "timezone": self.timezone,
            "read_only": self.read_only,
            "connection_timeout": self.connection_timeout,
            "log_level": self.log_level,
            "enable_query_logging": self.enable_query_logging,
        }

    def copy(self, **overrides) -> "DuckDBConfig":
        """
        建立配置副本，可覆蓋部分設定

        Args:
            **overrides: 要覆蓋的設定

        Returns:
            DuckDBConfig: 新的配置實例

        Example:
            >>> new_config = config.copy(db_path="./other.duckdb")
        """
        data = self.to_dict()
        data.update(overrides)
        if self.logger:
            data["logger"] = self.logger
        return DuckDBConfig.from_dict(data)
