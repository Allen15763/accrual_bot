"""
日誌處理模組
提供統一的日誌記錄功能，支援多種輸出目標
"""

import os
import sys
import logging
import threading
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path

from ..config.config_manager import config_manager


class Logger:
    """日誌處理器，單例模式"""
    
    _instance = None
    _initialized = False
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
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
            log_format = config_manager.get('LOGGING', 'format', 
                                            '%(asctime)s %(levelname)s: %(message)s')
            
            # 設置基本配置
            logging.basicConfig(
                level=getattr(logging, log_level.upper(), logging.INFO),
                format=log_format,
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            # 創建根日誌記錄器
            self._setup_root_logger(log_level, log_format)
            
        except Exception as e:
            # 如果設置失敗，使用預設配置
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(levelname)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            print(f"日誌設置失敗，使用預設配置: {e}")
    
    def _setup_root_logger(self, log_level: str, log_format: str) -> None:
        """設置根日誌記錄器"""
        root_logger = logging.getLogger('accrual_bot')
        root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        # 避免重複添加處理器
        if not root_logger.handlers:
            # 控制台處理器
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            console_formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)
            
            # 檔案處理器（如果配置了日誌路徑）
            log_path = config_manager.get('PATHS', 'log_path')
            if log_path:
                try:
                    self._setup_file_handler(root_logger, log_path, log_format)
                except Exception as e:
                    print(f"設置檔案日誌處理器失敗: {e}")
        
        self._loggers['root'] = root_logger
    
    def _setup_file_handler(self, logger: logging.Logger, log_path: str, log_format: str) -> None:
        """設置檔案處理器"""
        try:
            # 確保日誌目錄存在
            log_dir = Path(log_path)
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 創建日誌檔案名稱（包含日期）
            log_filename = f"PRPO_{datetime.now().strftime('%Y-%m-%d')}.log"
            log_file_path = log_dir / log_filename
            
            # 創建檔案處理器
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)  # 檔案記錄所有級別
            file_formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)
            
            logger.addHandler(file_handler)
            self._handlers['file'] = file_handler
            
        except Exception as e:
            print(f"創建檔案處理器失敗: {e}")
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        獲取日誌記錄器
        
        Args:
            name: 日誌記錄器名稱，如果為None則使用根記錄器
            
        Returns:
            logging.Logger: 日誌記錄器
        """
        if name is None:
            name = 'root'
        
        if name not in self._loggers:
            # 創建子記錄器
            logger = logging.getLogger(f'accrual_bot.{name}')
            logger.setLevel(logging.DEBUG)
            
            # 子記錄器繼承根記錄器的處理器
            if not logger.handlers and 'root' in self._loggers:
                for handler in self._loggers['root'].handlers:
                    logger.addHandler(handler)
            
            self._loggers[name] = logger
        
        return self._loggers[name]
    
    def add_custom_handler(self, name: str, handler: logging.Handler) -> None:
        """
        添加自定義處理器
        
        Args:
            name: 處理器名稱
            handler: 日誌處理器
        """
        self._handlers[name] = handler
        
        # 將處理器添加到所有現有記錄器
        for logger in self._loggers.values():
            logger.addHandler(handler)
    
    def remove_handler(self, name: str) -> None:
        """
        移除處理器
        
        Args:
            name: 處理器名稱
        """
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
        return {
            'loggers_count': len(self._loggers),
            'handlers_count': len(self._handlers),
            'logger_names': list(self._loggers.keys()),
            'handler_names': list(self._handlers.keys())
        }
    
    def cleanup(self) -> None:
        """清理日誌資源"""
        # 關閉所有處理器
        for handler in self._handlers.values():
            try:
                handler.close()
            except Exception as e:
                print(f"關閉日誌處理器失敗: {e}")
        
        # 清空字典
        self._handlers.clear()
        self._loggers.clear()
        
        # 重置初始化標記
        self._initialized = False


class StructuredLogger:
    """結構化日誌記錄器"""
    
    def __init__(self, logger_name: str = None):
        """
        初始化結構化日誌記錄器
        
        Args:
            logger_name: 日誌記錄器名稱
        """
        self.logger = Logger().get_logger(logger_name)
    
    def log_operation_start(self, operation: str, **kwargs) -> None:
        """記錄操作開始"""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"開始執行: {operation} {details}")
    
    def log_operation_end(self, operation: str, success: bool = True, **kwargs) -> None:
        """記錄操作結束"""
        status = "成功" if success else "失敗"
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        level = self.logger.info if success else self.logger.error
        level(f"執行{status}: {operation} {details}")
    
    def log_data_processing(self, data_type: str, record_count: int, 
                            processing_time: float = None, **kwargs) -> None:
        """記錄數據處理信息"""
        time_info = f"耗時{processing_time:.2f}秒 " if processing_time else ""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"處理{data_type}數據: {record_count}筆記錄 {time_info}{details}")
    
    def log_file_operation(self, operation: str, file_path: str, 
                           success: bool = True, **kwargs) -> None:
        """記錄檔案操作"""
        status = "成功" if success else "失敗"
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        level = self.logger.info if success else self.logger.error
        level(f"檔案{operation}{status}: {file_path} {details}")
    
    def log_error(self, error: Exception, context: str = None, **kwargs) -> None:
        """記錄錯誤信息"""
        context_info = f"[{context}] " if context else ""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.error(f"{context_info}錯誤: {str(error)} {details}", exc_info=True)


# 全域日誌管理器實例
logger_manager = Logger()

# 便利函數
def get_logger(name: str = None) -> logging.Logger:
    """獲取日誌記錄器的便利函數"""
    return logger_manager.get_logger(name)

def get_structured_logger(name: str = None) -> StructuredLogger:
    """獲取結構化日誌記錄器的便利函數"""
    return StructuredLogger(name)
