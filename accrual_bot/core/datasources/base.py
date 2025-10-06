"""
數據源基礎類
定義數據源的抽象接口和通用功能
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Any, Union
import pandas as pd
import asyncio
from accrual_bot.utils.logging import get_logger
from accrual_bot.core.datasources.config import DataSourceConfig
from datetime import datetime


class DataSourceType(Enum):
    """數據源類型枚舉"""
    EXCEL = "excel"
    CSV = "csv"
    PARQUET = "parquet"
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    GOOGLE_SHEETS = "google_sheets"
    IN_MEMORY = "in_memory"


class DataSource(ABC):
    """數據源抽象基類"""
    
    def __init__(self, config: 'DataSourceConfig'):
        """
        初始化數據源
        
        Args:
            config: 數據源配置
        """
        self.config = config
        self.logger = get_logger(f"datasource.{self.__class__.__name__}")
        self._cache = None
        self._metadata = {}
        
    @abstractmethod
    async def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        異步讀取數據
        
        Args:
            query: 查詢條件（某些數據源支援）
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        pass
    
    @abstractmethod
    async def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        異步寫入數據
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        pass
    
    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取數據源元數據
        
        Returns:
            Dict[str, Any]: 元數據信息
        """
        pass
    
    async def read_with_cache(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        帶快取的讀取
        
        Args:
            query: 查詢條件
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 數據
        """
        if self.config.cache_enabled and self._cache is not None:
            self.logger.debug("Returning cached data")
            return self._cache
        
        data = await self.read(query, **kwargs)
        
        if self.config.cache_enabled:
            self._cache = data.copy()
            
        return data
    
    def clear_cache(self):
        """清除快取"""
        self._cache = None
        self.logger.debug("Cache cleared")
    
    async def validate_connection(self) -> bool:
        """
        驗證連接是否有效
        
        Returns:
            bool: 連接是否有效
        """
        try:
            # 嘗試讀取少量數據來驗證連接
            test_data = await self.read(limit=1)
            return test_data is not None
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False
    
    async def get_row_count(self) -> int:
        """
        獲取數據行數
        
        Returns:
            int: 行數
        """
        try:
            data = await self.read_with_cache()
            return len(data)
        except Exception as e:
            self.logger.error(f"Failed to get row count: {str(e)}")
            return 0
    
    async def get_column_names(self) -> List[str]:
        """
        獲取列名
        
        Returns:
            List[str]: 列名列表
        """
        try:
            data = await self.read_with_cache()
            return data.columns.tolist()
        except Exception as e:
            self.logger.error(f"Failed to get column names: {str(e)}")
            return []
    
    def __repr__(self) -> str:
        """物件表示"""
        return f"{self.__class__.__name__}({self.config.source_type.value})"
    
    async def close(self):
        """關閉數據源連接（某些數據源需要）"""
        pass
    
    async def __aenter__(self):
        """異步上下文管理器進入"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """異步上下文管理器退出"""
        await self.close()
