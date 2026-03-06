"""
數據源基礎類
定義數據源的抽象接口和通用功能
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd
import asyncio
import hashlib
import json
from accrual_bot.utils.logging import get_logger
from accrual_bot.core.datasources.config import DataSourceConfig
from datetime import datetime, timedelta


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
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self._cache_ttl = timedelta(seconds=config.cache_ttl_seconds)
        self._cache_max_size = config.cache_max_items
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
        帶 TTL+LRU 快取的讀取

        Args:
            query: 查詢條件
            **kwargs: 額外參數

        Returns:
            pd.DataFrame: 數據
        """
        if not self.config.cache_enabled:
            return await self.read(query, **kwargs)

        cache_key = self._generate_cache_key(query, kwargs)

        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if datetime.now() - timestamp < self._cache_ttl:
                self.logger.debug(f"快取命中 (key={cache_key[:8]}...)")
                return data.copy()
            else:
                del self._cache[cache_key]
                self.logger.debug(f"快取已過期，重新載入 (key={cache_key[:8]}...)")

        data = await self.read(query, **kwargs)
        self._cache[cache_key] = (data.copy(), datetime.now())

        # LRU 驅逐：超過上限時移除最舊的條目
        if len(self._cache) > self._cache_max_size:
            oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]
            self.logger.debug(f"LRU 驅逐快取條目 (key={oldest_key[:8]}...)")

        return data

    def _generate_cache_key(self, query: Optional[str], kwargs: dict) -> str:
        """
        生成 MD5 快取鍵值

        Args:
            query: 查詢條件
            kwargs: 額外參數

        Returns:
            str: MD5 快取鍵值
        """
        # 排除日誌相關參數，避免影響快取鍵值
        filtered_kwargs = {k: v for k, v in sorted(kwargs.items())
                           if k not in ('logger', 'log_level')}
        key_data = {'query': query, 'kwargs': filtered_kwargs}
        key_json = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_json.encode('utf-8')).hexdigest()

    def clear_cache(self):
        """清除所有快取"""
        count = len(self._cache)
        self._cache.clear()
        self.logger.debug(f"快取已清除（共 {count} 筆）")
    
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
