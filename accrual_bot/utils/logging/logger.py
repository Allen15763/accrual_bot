"""
日誌處理模組
提供統一的日誌記錄功能，支援多種輸出目標

改進項目:
1. 修復線程安全問題 - 使用鎖保護 get_logger() 方法
2. 增強日誌格式 - 包含模組名、函數名、行號等詳細信息
3. 支援彩色輸出（可選）
4. 支援日誌文件輪轉
"""

import os
import sys
import logging
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler  # 文件大小轮转

from ..config.config_manager import config_manager


# ANSI 顏色代碼（用於終端彩色輸出）
class ColorCodes:
    """終端顏色代碼"""
    GREY = '\033[90m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD_RED = '\033[1;91m'
    RESET = '\033[0m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'


class ColoredFormatter(logging.Formatter):
    """彩色日誌格式化器（僅用於控制台）"""
    
    COLORS = {
        logging.DEBUG: ColorCodes.GREY,
        logging.INFO: ColorCodes.GREEN,
        logging.WARNING: ColorCodes.YELLOW,
        logging.ERROR: ColorCodes.RED,
        logging.CRITICAL: ColorCodes.BOLD_RED
    }
    
    def __init__(self, fmt: str = None, datefmt: str = None, use_color: bool = True):
        """
        初始化彩色格式化器
        
        Args:
            fmt: 日誌格式
            datefmt: 日期格式
            use_color: 是否使用顏色
        """
        super().__init__(fmt, datefmt)
        self.use_color = use_color and self._supports_color()
    
    def _supports_color(self) -> bool:
        """檢測終端是否支援顏色"""
        # Windows 10+ 支援 ANSI 顏色
        if sys.platform == "win32":
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
                return True
            except Exception as err:
                sys.stderr.write(f"{err}\n")
                return False
        # Unix/Linux/Mac 通常支援
        return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
    
    def format(self, record: logging.LogRecord) -> str:
        """格式化日誌記錄（不污染原始 record）"""
        if self.use_color:
            # ✅ 保存原始值
            original_levelname = record.levelname
            original_name = record.name
            
            # 臨時修改用於格式化
            color = self.COLORS.get(record.levelno, ColorCodes.RESET)
            record.levelname = f"{color}{original_levelname}{ColorCodes.RESET}"
            record.name = f"{ColorCodes.CYAN}{original_name}{ColorCodes.RESET}"
            
            # 格式化
            result = super().format(record)
            
            # ✅ 立即恢復原始值（避免影響其他 Handler）
            record.levelname = original_levelname
            record.name = original_name
            
            return result
        else:
            return super().format(record)


class Logger:
    """
    日誌處理器，單例模式（線程安全）
    
    改進:
    - 完全線程安全的實現
    - 詳細的日誌格式
    - 支援彩色輸出
    - 支援日誌文件輪轉
    """
    
    _instance = None
    _initialized = False
    _lock = threading.Lock()
    _logger_lock = threading.Lock()  # ✅ 新增：保護 _loggers 字典的鎖
    
    # 詳細日誌格式配置
    DETAILED_FORMAT = (
        '%(asctime)s | %(levelname)-8s | '
        '%(name)s | '
        '%(funcName)s:%(lineno)d | '
        '%(process)d-%(thread)d | '
        '%(message)s'
    )
    
    SIMPLE_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
    
    FILE_FORMAT = (
        '%(asctime)s | %(levelname)-8s | '
        '%(name)s | '
        '%(module)s.%(funcName)s:%(lineno)d | '
        '%(process)d-%(thread)d | '
        '%(message)s'
    )
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        with Logger._lock:
            if self._initialized:
                return

            self._loggers: Dict[str, logging.Logger] = {}
            self._handlers: Dict[str, logging.Handler] = {}
            self._setup_logging()
            self._initialized = True
    
    def _setup_logging(self) -> None:
        """設置日誌系統"""
        try:
            # 從配置獲取日誌設定
            log_level = config_manager.get('LOGGING', 'level', 'INFO')
            
            # 使用詳細格式
            use_detailed = config_manager.get('LOGGING', 'detailed', True)
            console_format = self.DETAILED_FORMAT if use_detailed else self.SIMPLE_FORMAT
            
            # 是否使用彩色輸出
            use_color = config_manager.get('LOGGING', 'color', True)
            
            # 創建根日誌記錄器（不使用 basicConfig 避免重複）
            self._setup_root_logger(log_level, console_format, use_color)
            
        except Exception as e:
            # 如果設置失敗，使用預設配置
            self._setup_fallback_logger()
            # 使用fallback logger記錄錯誤（避免print）
            sys.stderr.write(f"日誌設置失敗，使用預設配置: {e}\n")
    
    def _setup_root_logger(self, log_level: str, console_format: str, use_color: bool = True) -> None:
        """設置根日誌記錄器
        默認INFO層級(配置檔config.ini設定)
        """
        # 只在需要時創建root logger（減少重複創建）
        if 'root' not in self._loggers:
            root_logger = logging.getLogger('accrual_bot')
            root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            
            # 清除現有的處理器以避免重複
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
                handler.close()
            
            # 阻止傳播到父logger以避免重複輸出
            root_logger.propagate = False
            
            # 儲存logger實例
            self._loggers['root'] = root_logger
        else:
            root_logger = self._loggers['root']
        
        # 控制台處理器（帶顏色）
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        # 使用彩色格式化器
        console_formatter = ColoredFormatter(
            fmt=console_format,
            datefmt='%Y-%m-%d %H:%M:%S',
            use_color=use_color
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        self._handlers['console'] = console_handler
        
        # 檔案處理器（如果配置了日誌路徑）
        log_path = config_manager.get('PATHS', 'log_path')
        if log_path:
            try:
                self._setup_file_handler(root_logger, log_path)
            except Exception as e:
                # 避免print，使用stderr記錄錯誤
                sys.stderr.write(f"設置檔案日誌處理器失敗: {e}\n")
    
    def _setup_file_handler(self, logger: logging.Logger, log_path: str) -> None:
        """
        設置檔案處理器（支援日誌輪轉）
        - 原始file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        - 使用RotatingFileHandler取代

        默認DEBUG層級
        
        Args:
            logger: 日誌記錄器
            log_path: 日誌路徑
        """
        try:
            # 確保日誌目錄存在
            log_dir = Path(log_path)
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 創建日誌檔案名稱（包含日期）
            # Define a timezone with a fixed UTC offset (e.g., +8 hours)
            tz_offset = timezone(timedelta(hours=8))
            aware_datetime = datetime.now(tz_offset).strftime('%Y%m%d_%H%M%S')
            log_filename = f"Accrual_bot_{aware_datetime}.log"

            # log_filename = f"Accrual_bot_{datetime.now().strftime('%Y-%m-%d')}.log"
            log_file_path = log_dir / log_filename
            
            # 使用輪轉檔案處理器（當文件達到 10MB 時自動輪轉，保留 5 個備份）
            file_handler = RotatingFileHandler(
                log_file_path,
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5,
                encoding='utf-8'
            )
            
            file_handler.setLevel(logging.DEBUG)  # 檔案記錄所有級別
            
            # 文件使用更詳細的格式（包含進程ID、線程ID）
            file_formatter = logging.Formatter(
                self.FILE_FORMAT,
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            
            logger.addHandler(file_handler)
            self._handlers['file'] = file_handler
            
        except Exception as e:
            # 避免print，使用stderr記錄錯誤
            sys.stderr.write(f"創建檔案處理器失敗: {e}\n")
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        獲取日誌記錄器（線程安全）
        
        Args:
            name: 日誌記錄器名稱，如果為None則使用根記錄器
            
        Returns:
            logging.Logger: 日誌記錄器
        """
        if name is None:
            name = 'root'
        
        # ✅ 使用鎖保護整個 check-then-act 操作
        with Logger._logger_lock:
            if name not in self._loggers:
                if name == 'root':
                    # 直接返回已創建的root logger，如果不存在則創建基本的
                    if 'root' not in self._loggers:
                        self._loggers['root'] = logging.getLogger('accrual_bot')
                    return self._loggers['root']
                else:
                    # 創建子記錄器，但不添加處理器（使用繼承機制）
                    logger = logging.getLogger(f'accrual_bot.{name}')
                    logger.setLevel(logging.DEBUG)
                    # 不手動添加處理器，讓子記錄器自然繼承父記錄器的設置
                    self._loggers[name] = logger
            
            return self._loggers[name]
    
    def add_custom_handler(self, name: str, handler: logging.Handler) -> None:
        """
        添加自定義處理器
        
        Args:
            name: 處理器名稱
            handler: 日誌處理器
        """
        with Logger._logger_lock:
            self._handlers[name] = handler

            # 只將處理器添加到 root logger
            # 子記錄器的 propagate=True 會自然將訊息傳至 root，避免重複輸出
            root = self._loggers.get('root')
            if root and handler not in root.handlers:
                root.addHandler(handler)
    
    def remove_handler(self, name: str) -> None:
        """
        移除處理器
        
        Args:
            name: 處理器名稱
        """
        with Logger._logger_lock:
            if name in self._handlers:
                handler = self._handlers[name]
                
                # 從所有記錄器中移除處理器
                for logger in self._loggers.values():
                    if handler in logger.handlers:
                        logger.removeHandler(handler)
                
                # 關閉處理器
                handler.close()
                del self._handlers[name]
    
    def set_level(self, level: str, logger_name: Optional[str] = None) -> None:
        """
        設置日誌級別
        
        Args:
            level: 日誌級別 ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
            logger_name: 日誌記錄器名稱，如果為None則設置所有記錄器
        """
        log_level = getattr(logging, level.upper(), logging.INFO)
        
        with Logger._logger_lock:
            if logger_name:
                if logger_name in self._loggers:
                    self._loggers[logger_name].setLevel(log_level)
            else:
                # 設置所有記錄器
                for logger in self._loggers.values():
                    logger.setLevel(log_level)
    
    def get_log_stats(self) -> Dict[str, Any]:
        """
        獲取日誌統計信息
        
        Returns:
            Dict[str, Any]: 日誌統計信息
        """
        with Logger._logger_lock:
            return {
                'loggers_count': len(self._loggers),
                'handlers_count': len(self._handlers),
                'logger_names': list(self._loggers.keys()),
                'handler_names': list(self._handlers.keys())
            }
    
    def _setup_fallback_logger(self) -> None:
        """設置備用的簡單日誌配置"""
        with Logger._logger_lock:
            # 避免重複創建，先檢查是否已存在
            if 'root' not in self._loggers:
                root_logger = logging.getLogger('accrual_bot')
                root_logger.setLevel(logging.INFO)
                
                # 清除現有處理器
                for handler in root_logger.handlers[:]:
                    root_logger.removeHandler(handler)
                    handler.close()
                
                root_logger.propagate = False
                
                # 創建簡單的控制台處理器
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(logging.INFO)
                formatter = ColoredFormatter(
                    fmt=self.SIMPLE_FORMAT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    use_color=True
                )
                console_handler.setFormatter(formatter)
                root_logger.addHandler(console_handler)
                
                self._loggers['root'] = root_logger
                self._handlers['console'] = console_handler
    
    def cleanup(self) -> None:
        """清理日誌資源"""
        with Logger._logger_lock:
            # 關閉所有處理器
            for handler in self._handlers.values():
                try:
                    handler.close()
                except Exception as e:
                    # 避免print，使用stderr記錄錯誤
                    sys.stderr.write(f"關閉日誌處理器失敗: {e}\n")
            
            # 清空字典
            self._handlers.clear()
            self._loggers.clear()
            
            # 重置初始化標記
            self._initialized = False


class StructuredLogger:
    """
    結構化日誌記錄器
    
    提供語義化的日誌記錄方法，便於統一格式和分析
    """
    
    def __init__(self, logger_name: str = None):
        """
        初始化結構化日誌記錄器
        
        Args:
            logger_name: 日誌記錄器名稱
        """
        self.logger = Logger().get_logger(logger_name)
    
    def log_operation_start(self, operation: str, **kwargs) -> None:
        """
        記錄操作開始
        
        Args:
            operation: 操作名稱
            **kwargs: 額外參數
        """
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        msg = f"▶ 開始執行: {operation}"
        if details:
            msg += f" | {details}"
        self.logger.info(msg)
    
    def log_operation_end(self, operation: str, success: bool = True, **kwargs) -> None:
        """
        記錄操作結束
        
        Args:
            operation: 操作名稱
            success: 是否成功
            **kwargs: 額外參數
        """
        status = "✓ 成功" if success else "✗ 失敗"
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        msg = f"{status}: {operation}"
        if details:
            msg += f" | {details}"
        
        level = self.logger.info if success else self.logger.error
        level(msg)
    
    def log_data_processing(self, data_type: str, record_count: int, 
                            processing_time: float = None, **kwargs) -> None:
        """
        記錄數據處理信息
        
        Args:
            data_type: 數據類型
            record_count: 記錄數量
            processing_time: 處理時間（秒）
            **kwargs: 額外參數
        """
        time_info = f"耗時 {processing_time:.2f}s" if processing_time else ""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"📊 處理 {data_type} 數據: {record_count:,} 筆記錄"
        if time_info:
            msg += f" | {time_info}"
        if details:
            msg += f" | {details}"
        
        self.logger.info(msg)
    
    def log_file_operation(self, operation: str, file_path: str, 
                           success: bool = True, **kwargs) -> None:
        """
        記錄檔案操作
        
        Args:
            operation: 操作類型（讀取、寫入等）
            file_path: 檔案路徑
            success: 是否成功
            **kwargs: 額外參數
        """
        status = "✓" if success else "✗"
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"{status} 檔案{operation}: {file_path}"
        if details:
            msg += f" | {details}"
        
        level = self.logger.info if success else self.logger.error
        level(msg)
    
    def log_error(self, error: Exception, context: str = None, **kwargs) -> None:
        """
        記錄錯誤信息
        
        Args:
            error: 異常對象
            context: 錯誤上下文
            **kwargs: 額外參數
        """
        context_info = f"[{context}] " if context else ""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"❌ {context_info}錯誤: {str(error)}"
        if details:
            msg += f" | {details}"
        
        self.logger.error(msg, exc_info=True)
    
    def log_progress(self, current: int, total: int, operation: str = "", **kwargs) -> None:
        """
        記錄進度信息
        
        Args:
            current: 當前進度
            total: 總數
            operation: 操作描述
            **kwargs: 額外參數
        """
        percentage = (current / total * 100) if total > 0 else 0
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"⏳ 進度: {current}/{total} ({percentage:.1f}%)"
        if operation:
            msg += f" | {operation}"
        if details:
            msg += f" | {details}"
        
        self.logger.info(msg)


# 全域日誌管理器實例
logger_manager = Logger()


# 便利函數
def get_logger(name: str = None) -> logging.Logger:
    """
    獲取日誌記錄器的便利函數（線程安全）
    
    Args:
        name: 日誌記錄器名稱
        
    Returns:
        logging.Logger: 日誌記錄器
    """
    return logger_manager.get_logger(name)


def get_structured_logger(name: str = None) -> StructuredLogger:
    """
    獲取結構化日誌記錄器的便利函數
    
    Args:
        name: 日誌記錄器名稱
        
    Returns:
        StructuredLogger: 結構化日誌記錄器
    """
    return StructuredLogger(name)
