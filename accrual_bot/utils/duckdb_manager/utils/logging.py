"""
DuckDB Manager 日誌模組

提供可插拔的日誌系統，支援:
- 外部日誌器注入
- 內建日誌器 (標準 logging)
- 空日誌器 (NullLogger)
"""

import logging
import sys
from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class LoggerProtocol(Protocol):
    """
    日誌器介面協議

    任何符合此介面的物件都可以作為日誌器使用，
    包括標準 logging.Logger、loguru 等。
    """

    def debug(self, msg: str, *args, **kwargs) -> None:
        """記錄 DEBUG 級別訊息"""
        ...

    def info(self, msg: str, *args, **kwargs) -> None:
        """記錄 INFO 級別訊息"""
        ...

    def warning(self, msg: str, *args, **kwargs) -> None:
        """記錄 WARNING 級別訊息"""
        ...

    def error(self, msg: str, *args, **kwargs) -> None:
        """記錄 ERROR 級別訊息"""
        ...


class NullLogger:
    """
    空日誌器 - 不輸出任何內容

    用於完全禁用日誌輸出的場景。

    Example:
        >>> from duckdb_manager import DuckDBManager, DuckDBConfig
        >>> from duckdb_manager.utils import NullLogger
        >>> config = DuckDBConfig(logger=NullLogger())
        >>> db = DuckDBManager(config)  # 不會有任何日誌輸出
    """

    def debug(self, msg: str, *args, **kwargs) -> None:
        pass

    def info(self, msg: str, *args, **kwargs) -> None:
        pass

    def warning(self, msg: str, *args, **kwargs) -> None:
        pass

    def error(self, msg: str, *args, **kwargs) -> None:
        pass

    def critical(self, msg: str, *args, **kwargs) -> None:
        pass


class ColoredFormatter(logging.Formatter):
    """
    帶顏色的日誌格式化器

    在支援 ANSI 的終端機中顯示彩色日誌。
    """

    # ANSI 顏色碼
    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[35m",  # Magenta
        "RESET": "\033[0m",
    }

    def __init__(self, fmt: str = None, use_colors: bool = True):
        super().__init__(fmt)
        self.use_colors = use_colors and self._supports_color()

    @staticmethod
    def _supports_color() -> bool:
        """檢查終端機是否支援顏色"""
        # Windows 需要特殊處理
        if sys.platform == "win32":
            try:
                import os
                return os.isatty(sys.stdout.fileno())
            except Exception:
                return False
        return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    def format(self, record: logging.LogRecord) -> str:
        if self.use_colors:
            levelname = record.levelname
            if levelname in self.COLORS:
                record.levelname = (
                    f"{self.COLORS[levelname]}{levelname}{self.COLORS['RESET']}"
                )
        return super().format(record)


def get_logger(
    name: str = "duckdb_manager",
    level: str = "INFO",
    external_logger: Optional[logging.Logger] = None,
    use_colors: bool = True,
) -> logging.Logger:
    """
    獲取日誌器

    支援外部日誌器注入，或建立內建日誌器。

    Args:
        name: 日誌器名稱
        level: 日誌級別 ("DEBUG", "INFO", "WARNING", "ERROR")
        external_logger: 外部注入的日誌器，為 None 時建立內建日誌器
        use_colors: 是否使用彩色輸出 (僅對內建日誌器生效)

    Returns:
        logging.Logger: 日誌器實例

    Example:
        # 使用內建日誌器
        >>> logger = get_logger("my_app", level="DEBUG")
        >>> logger.info("Hello!")

        # 注入外部日誌器
        >>> from my_project import project_logger
        >>> logger = get_logger(external_logger=project_logger)
    """
    # 如果提供了外部日誌器，直接返回
    if external_logger is not None:
        return external_logger

    # 建立或獲取日誌器
    logger = logging.getLogger(name)

    # 如果已經配置過，直接返回
    if logger.handlers:
        return logger

    # 設定日誌級別
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 建立 console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logger.level)

    # 設定格式
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    if use_colors:
        formatter = ColoredFormatter(fmt, use_colors=True)
    else:
        formatter = logging.Formatter(fmt)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # 防止日誌傳播到父 logger
    logger.propagate = False

    return logger


def setup_file_logger(
    name: str,
    file_path: str,
    level: str = "DEBUG",
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    設定檔案日誌器

    使用 RotatingFileHandler 自動輪換日誌檔案。

    Args:
        name: 日誌器名稱
        file_path: 日誌檔案路徑
        level: 日誌級別
        max_bytes: 單一檔案最大大小 (bytes)
        backup_count: 保留的備份檔案數量

    Returns:
        logging.Logger: 日誌器實例
    """
    from logging.handlers import RotatingFileHandler
    from pathlib import Path

    # 確保目錄存在
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.DEBUG))

    # 檔案 handler
    file_handler = RotatingFileHandler(
        file_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(logger.level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.propagate = False

    return logger
