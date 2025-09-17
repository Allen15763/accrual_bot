"""
CSV數據源實現
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Any, List, Union
from pathlib import Path
import asyncio
import logging
import io
from concurrent.futures import ThreadPoolExecutor

try:
    from .base import DataSource
    from .config import DataSourceConfig, DataSourceType
except ImportError:
    from accrual_bot.core.datasources import DataSource
    from accrual_bot.core.datasources import DataSourceConfig, DataSourceType


class CSVSource(DataSource):
    """CSV文件數據源"""
    
    # 類級別的線程池，避免每個實例創建新的線程池
    _executor = ThreadPoolExecutor(max_workers=4)
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化CSV數據源
        
        Args:
            config: 數據源配置
        """
        super().__init__(config)
        self.file_path = Path(config.connection_params['file_path'])
        self.encoding = config.encoding or 'utf-8'
        self.sep = config.connection_params.get('sep', ',')
        self.header = config.connection_params.get('header', 'infer')
        self.dtype = config.connection_params.get('dtype')
        self.na_values = config.connection_params.get('na_values')
        self.parse_dates = config.connection_params.get('parse_dates')
        self.usecols = config.connection_params.get('usecols')
        self.chunk_size = config.chunk_size
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
    
    async def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        異步讀取CSV文件
        
        Args:
            query: SQL查詢（不適用於CSV，但保留接口一致性）
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        # 獲取參數
        nrows = kwargs.get('nrows')
        skiprows = kwargs.get('skiprows')
        chunksize = kwargs.get('chunksize', self.chunk_size)
        
        def read_csv_sync():
            try:
                self.logger.info(f"Reading CSV file: {self.file_path}")
                
                # 構建讀取參數
                read_kwargs = {
                    'sep': self.sep,
                    'encoding': self.encoding,
                    'header': self.header
                }
                
                if self.dtype is not None:
                    read_kwargs['dtype'] = self.dtype
                if self.na_values is not None:
                    read_kwargs['na_values'] = self.na_values
                if self.parse_dates is not None:
                    read_kwargs['parse_dates'] = self.parse_dates
                if self.usecols is not None:
                    read_kwargs['usecols'] = self.usecols
                if nrows is not None:
                    read_kwargs['nrows'] = nrows
                if skiprows is not None:
                    read_kwargs['skiprows'] = skiprows
                
                # 如果指定了chunk_size，返回迭代器
                if chunksize:
                    read_kwargs['chunksize'] = chunksize
                    chunks = []
                    for chunk in pd.read_csv(self.file_path, **read_kwargs):
                        chunks.append(chunk)
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.read_csv(self.file_path, **read_kwargs)
                
                # 如果有查詢條件，應用篩選
                if query:
                    df = self._apply_query(df, query)
                
                self.logger.info(f"Successfully read {len(df)} rows from CSV")
                return df
                
            except Exception as e:
                self.logger.error(f"Error reading CSV file: {str(e)}")
                raise
        
        # 使用類級別的線程池執行器
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, read_csv_sync)
    
    async def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        異步寫入CSV文件
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        # 獲取參數
        index = kwargs.get('index', False)
        mode = kwargs.get('mode', 'w')
        header = kwargs.get('header', True)
        
        def write_csv_sync():
            try:
                self.logger.info(f"Writing {len(data)} rows to CSV: {self.file_path}")
                
                data.to_csv(
                    self.file_path,
                    sep=self.sep,
                    encoding=self.encoding,
                    index=index,
                    mode=mode,
                    header=header
                )
                
                self.logger.info("Successfully wrote data to CSV")
                return True
                
            except Exception as e:
                self.logger.error(f"Error writing CSV file: {str(e)}")
                return False
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, write_csv_sync)
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取CSV文件元數據
        
        Returns:
            Dict[str, Any]: 元數據信息
        """
        metadata = {
            'file_path': str(self.file_path),
            'file_size': self.file_path.stat().st_size if self.file_path.exists() else 0,
            'file_modified': self.file_path.stat().st_mtime if self.file_path.exists() else None,
            'encoding': self.encoding,
            'separator': self.sep
        }
        
        # 嘗試獲取文件的行數和列數（不完全讀取）
        try:
            # 讀取前幾行來推斷結構
            sample_df = pd.read_csv(
                self.file_path,
                sep=self.sep,
                encoding=self.encoding,
                nrows=5
            )
            metadata['num_columns'] = len(sample_df.columns)
            metadata['column_names'] = sample_df.columns.tolist()
            
            # 快速計算行數（對於大文件）
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                metadata['num_rows'] = sum(1 for line in f) - 1  # 減去標題行
                
        except Exception as e:
            self.logger.warning(f"Could not read CSV metadata: {str(e)}")
            metadata['num_columns'] = 0
            metadata['column_names'] = []
            metadata['num_rows'] = 0
        
        return metadata
    
    async def read_in_chunks(self, chunk_size: int = 10000) -> List[pd.DataFrame]:
        """
        分塊讀取CSV文件（適合處理大文件）
        
        Args:
            chunk_size: 每塊的行數
            
        Returns:
            List[pd.DataFrame]: 數據塊列表
        """
        def read_chunks_sync():
            chunks = []
            try:
                for chunk in pd.read_csv(
                    self.file_path,
                    sep=self.sep,
                    encoding=self.encoding,
                    chunksize=chunk_size,
                    dtype=self.dtype,
                    na_values=self.na_values,
                    parse_dates=self.parse_dates
                ):
                    chunks.append(chunk)
                
                self.logger.info(f"Read {len(chunks)} chunks from CSV")
                return chunks
                
            except Exception as e:
                self.logger.error(f"Error reading CSV in chunks: {str(e)}")
                raise
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, read_chunks_sync)
    
    async def append_data(self, data: pd.DataFrame) -> bool:
        """
        追加數據到現有CSV文件
        
        Args:
            data: 要追加的數據
            
        Returns:
            bool: 是否成功
        """
        # 如果檔案存在，以追加模式寫入
        if self.file_path.exists():
            return await self.write(data, mode='a', header=False)
        else:
            # 檔案不存在，正常寫入
            return await self.write(data)
    
    def _apply_query(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """
        應用查詢條件
        
        Args:
            df: 原始數據
            query: 查詢條件
            
        Returns:
            pd.DataFrame: 篩選後的數據
        """
        try:
            return df.query(query)
        except Exception as e:
            self.logger.warning(f"Could not apply query '{query}': {str(e)}")
            return df
    
    @classmethod
    def create_from_file(cls, file_path: str, sep: str = ',', 
                         encoding: str = 'utf-8', **kwargs) -> 'CSVSource':
        """
        便捷方法：從檔案路徑創建CSV數據源
        
        Args:
            file_path: CSV檔案路徑
            sep: 分隔符
            encoding: 編碼
            **kwargs: 其他配置參數
            
        Returns:
            CSVSource: CSV數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.CSV,
            connection_params={
                'file_path': file_path,
                'sep': sep,
                **kwargs
            },
            encoding=encoding
        )
        return cls(config)
    
    async def close(self):
        """關閉資源（CSV不需要特別關閉連接）"""
        pass
    
    @classmethod
    def cleanup_executor(cls):
        """清理類級別的線程池（在程式結束時調用）"""
        if hasattr(cls, '_executor'):
            cls._executor.shutdown(wait=True)
