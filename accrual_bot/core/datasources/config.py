"""
數據源配置類
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from enum import Enum


class DataSourceType(Enum):
    """數據源類型"""
    EXCEL = "excel"
    CSV = "csv"
    PARQUET = "parquet"
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    GOOGLE_SHEETS = "google_sheets"
    IN_MEMORY = "in_memory"


@dataclass
class DataSourceConfig:
    """
    數據源配置
    
    Attributes:
        source_type: 數據源類型
        connection_params: 連接參數
        cache_enabled: 是否啟用快取
        lazy_load: 是否延遲載入
        encoding: 編碼（用於文本檔案）
        chunk_size: 分塊大小（用於大檔案）
    """
    source_type: DataSourceType
    connection_params: Dict[str, Any]
    cache_enabled: bool = True
    lazy_load: bool = False
    encoding: str = 'utf-8'
    chunk_size: Optional[int] = None
    
    def validate(self) -> tuple[bool, List[str]]:
        """
        驗證配置有效性
        
        Returns:
            tuple[bool, List[str]]: (是否有效, 錯誤訊息列表)
        """
        errors = []
        
        # 定義每種數據源類型的必要參數
        required_params = {
            DataSourceType.EXCEL: ['file_path'],
            DataSourceType.CSV: ['file_path'],
            DataSourceType.PARQUET: ['file_path'],
            DataSourceType.DUCKDB: ['db_path'],  # table_name 是可選的
            DataSourceType.POSTGRES: ['host', 'port', 'database', 'user', 'password'],
            DataSourceType.GOOGLE_SHEETS: ['sheet_id', 'credentials'],
            DataSourceType.IN_MEMORY: ['dataframe']
        }
        
        # 檢查必要參數
        required = required_params.get(self.source_type, [])
        for param in required:
            if param not in self.connection_params:
                errors.append(f"Missing required parameter: {param}")
        
        # 驗證檔案路徑是否存在（對於檔案類型的數據源）
        if self.source_type in [DataSourceType.EXCEL, DataSourceType.CSV, DataSourceType.PARQUET]:
            file_path = self.connection_params.get('file_path')
            if file_path:
                from pathlib import Path
                if not Path(file_path).exists():
                    errors.append(f"File not found: {file_path}")
        
        return len(errors) == 0, errors
    
    def get_connection_string(self) -> Optional[str]:
        """
        獲取連接字符串（適用於數據庫類型）
        
        Returns:
            Optional[str]: 連接字符串
        """
        if self.source_type == DataSourceType.POSTGRES:
            params = self.connection_params
            return (f"postgresql://{params['user']}:{params.get('password', '')}@"
                    f"{params['host']}:{params['port']}/{params['database']}")
        elif self.source_type == DataSourceType.DUCKDB:
            return self.connection_params.get('db_path', ':memory:')
        
        return None
    
    def copy(self) -> 'DataSourceConfig':
        """
        創建配置的副本
        
        Returns:
            DataSourceConfig: 配置副本
        """
        return DataSourceConfig(
            source_type=self.source_type,
            connection_params=self.connection_params.copy(),
            cache_enabled=self.cache_enabled,
            lazy_load=self.lazy_load,
            encoding=self.encoding,
            chunk_size=self.chunk_size
        )
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DataSourceConfig':
        """
        從字典創建配置
        
        Args:
            config_dict: 配置字典
            
        Returns:
            DataSourceConfig: 配置物件
        """
        source_type = config_dict.get('source_type')
        if isinstance(source_type, str):
            source_type = DataSourceType(source_type)
        
        return cls(
            source_type=source_type,
            connection_params=config_dict.get('connection_params', {}),
            cache_enabled=config_dict.get('cache_enabled', True),
            lazy_load=config_dict.get('lazy_load', False),
            encoding=config_dict.get('encoding', 'utf-8'),
            chunk_size=config_dict.get('chunk_size')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        轉換為字典
        
        Returns:
            Dict[str, Any]: 配置字典
        """
        return {
            'source_type': self.source_type.value,
            'connection_params': self.connection_params,
            'cache_enabled': self.cache_enabled,
            'lazy_load': self.lazy_load,
            'encoding': self.encoding,
            'chunk_size': self.chunk_size
        }


@dataclass
class ConnectionPool:
    """連接池配置（用於數據庫類型）"""
    min_size: int = 1
    max_size: int = 10
    timeout: float = 120.0
    idle_time: float = 3600.0
    retry_attempts: int = 3
    retry_delay: float = 1.0
