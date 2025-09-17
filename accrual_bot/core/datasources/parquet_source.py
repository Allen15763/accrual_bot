"""
Parquet數據源實現
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, Optional, Any, List, Union
from pathlib import Path
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

try:
    from .base import DataSource
    from .config import DataSourceConfig, DataSourceType
except ImportError:
    from accrual_bot.core.datasources import DataSource
    from accrual_bot.core.datasources import DataSourceConfig, DataSourceType


class ParquetSource(DataSource):
    """Parquet文件數據源"""
    
    # 類級別的線程池，避免每個實例創建新的線程池
    _executor = ThreadPoolExecutor(max_workers=4)
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化Parquet數據源
        
        Args:
            config: 數據源配置
        """
        super().__init__(config)
        self.file_path = Path(config.connection_params['file_path'])
        self.compression = config.connection_params.get('compression', 'snappy')
        self.engine = config.connection_params.get('engine', 'pyarrow')
        
        # Parquet特定參數
        self.columns = config.connection_params.get('columns')
        self.filters = config.connection_params.get('filters')
        self.use_pandas_metadata = config.connection_params.get('use_pandas_metadata', True)
        
        if not self.file_path.exists():
            # Parquet文件可以不存在（用於寫入新文件）
            self.logger.warning(f"Parquet file does not exist yet: {self.file_path}")
    
    async def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        異步讀取Parquet文件
        
        Args:
            query: 查詢條件（支援PyArrow的filter表達式）
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        if not self.file_path.exists():
            self.logger.warning(f"Parquet file not found: {self.file_path}")
            return pd.DataFrame()
        
        # 獲取參數
        columns = kwargs.get('columns', self.columns)
        filters = kwargs.get('filters', self.filters)
        
        def read_parquet_sync():
            try:
                self.logger.info(f"Reading Parquet file: {self.file_path}")
                
                # 使用PyArrow讀取（支援更多功能）
                if self.engine == 'pyarrow':
                    # 使用PyArrow API進行更精細的控制
                    parquet_file = pq.ParquetFile(self.file_path)
                    
                    # 如果有列篩選
                    if columns:
                        table = parquet_file.read(columns=columns)
                    else:
                        table = parquet_file.read()
                    
                    # 轉換為DataFrame
                    df = table.to_pandas()
                    
                    # 應用過濾器
                    if filters:
                        df = self._apply_filters(df, filters)
                    
                else:
                    # 使用pandas內建方法
                    df = pd.read_parquet(
                        self.file_path,
                        engine=self.engine,
                        columns=columns,
                        filters=filters
                    )
                
                # 如果有查詢條件，應用額外篩選
                if query:
                    df = self._apply_query(df, query)
                
                self.logger.info(f"Successfully read {len(df)} rows from Parquet")
                return df
                
            except Exception as e:
                self.logger.error(f"Error reading Parquet file: {str(e)}")
                raise
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, read_parquet_sync)
    
    async def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        異步寫入Parquet文件
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        # 獲取參數
        compression = kwargs.get('compression', self.compression)
        index = kwargs.get('index', False)
        partition_cols = kwargs.get('partition_cols')
        
        def write_parquet_sync():
            try:
                self.logger.info(f"Writing {len(data)} rows to Parquet: {self.file_path}")
                
                # 確保目錄存在
                self.file_path.parent.mkdir(parents=True, exist_ok=True)
                
                if self.engine == 'pyarrow':
                    # 使用PyArrow寫入（更多控制選項）
                    table = pa.Table.from_pandas(data, preserve_index=index)
                    
                    # 寫入選項
                    pq.write_table(
                        table,
                        self.file_path,
                        compression=compression
                    )
                    
                else:
                    # 使用pandas內建方法
                    data.to_parquet(
                        self.file_path,
                        engine=self.engine,
                        compression=compression,
                        index=index,
                        partition_cols=partition_cols
                    )
                
                self.logger.info("Successfully wrote data to Parquet")
                return True
                
            except Exception as e:
                self.logger.error(f"Error writing Parquet file: {str(e)}")
                return False
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, write_parquet_sync)
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取Parquet文件元數據
        
        Returns:
            Dict[str, Any]: 元數據信息
        """
        metadata = {
            'file_path': str(self.file_path),
            'compression': self.compression,
            'engine': self.engine
        }
        
        if self.file_path.exists():
            metadata['file_size'] = self.file_path.stat().st_size
            metadata['file_modified'] = self.file_path.stat().st_mtime
            
            try:
                # 讀取Parquet元數據
                parquet_file = pq.ParquetFile(self.file_path)
                
                metadata['num_rows'] = parquet_file.metadata.num_rows
                metadata['num_columns'] = len(parquet_file.schema)
                metadata['num_row_groups'] = parquet_file.num_row_groups
                metadata['schema'] = str(parquet_file.schema)
                metadata['created_by'] = parquet_file.metadata.created_by
                
                # 獲取列信息
                columns_info = []
                for i in range(len(parquet_file.schema)):
                    field = parquet_file.schema.field(i)
                    columns_info.append({
                        'name': field.name,
                        'type': str(field.type),
                        'nullable': field.nullable
                    })
                metadata['columns'] = columns_info
                
                # 獲取文件統計信息
                stats = []
                for rg in range(parquet_file.num_row_groups):
                    row_group = parquet_file.metadata.row_group(rg)
                    stats.append({
                        'row_group': rg,
                        'num_rows': row_group.num_rows,
                        'total_byte_size': row_group.total_byte_size
                    })
                metadata['row_groups_stats'] = stats
                
            except Exception as e:
                self.logger.warning(f"Could not read Parquet metadata: {str(e)}")
        
        return metadata
    
    async def read_row_groups(self, row_groups: List[int] = None) -> pd.DataFrame:
        """
        讀取指定的行組
        
        Args:
            row_groups: 要讀取的行組索引列表
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        if not self.file_path.exists():
            return pd.DataFrame()
        
        def read_groups_sync():
            try:
                parquet_file = pq.ParquetFile(self.file_path)
                
                if row_groups is None:
                    # 讀取所有行組
                    table = parquet_file.read()
                else:
                    # 讀取指定行組
                    table = parquet_file.read_row_groups(row_groups)
                
                return table.to_pandas()
                
            except Exception as e:
                self.logger.error(f"Error reading row groups: {str(e)}")
                raise
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, read_groups_sync)
    
    async def get_schema(self) -> pa.Schema:
        """
        獲取Parquet文件的模式
        
        Returns:
            pa.Schema: PyArrow模式
        """
        if not self.file_path.exists():
            return None
        
        def get_schema_sync():
            try:
                parquet_file = pq.ParquetFile(self.file_path)
                return parquet_file.schema
            except Exception as e:
                self.logger.error(f"Error getting schema: {str(e)}")
                return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, get_schema_sync)
    
    async def append_data(self, data: pd.DataFrame) -> bool:
        """
        追加數據到現有Parquet文件（創建新的行組）
        
        Args:
            data: 要追加的數據
            
        Returns:
            bool: 是否成功
        """
        if not self.file_path.exists():
            # 如果檔案不存在，直接寫入
            return await self.write(data)
        
        # Parquet不支援直接追加，需要讀取、合併、重寫
        existing_data = await self.read()
        combined_data = pd.concat([existing_data, data], ignore_index=True)
        
        return await self.write(combined_data)
    
    def _apply_filters(self, df: pd.DataFrame, filters: List) -> pd.DataFrame:
        """
        應用PyArrow過濾器
        
        Args:
            df: 原始數據
            filters: 過濾條件列表
            
        Returns:
            pd.DataFrame: 篩選後的數據
        """
        # 這是一個簡化的實現
        # 實際使用中應該在讀取時應用過濾器以提高效率
        for filter_expr in filters:
            if isinstance(filter_expr, tuple) and len(filter_expr) == 3:
                col, op, value = filter_expr
                if op == '==':
                    df = df[df[col] == value]
                elif op == '!=':
                    df = df[df[col] != value]
                elif op == '>':
                    df = df[df[col] > value]
                elif op == '>=':
                    df = df[df[col] >= value]
                elif op == '<':
                    df = df[df[col] < value]
                elif op == '<=':
                    df = df[df[col] <= value]
                elif op == 'in':
                    df = df[df[col].isin(value)]
        
        return df
    
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
    
    async def close(self):
        """關閉資源（Parquet不需要特別關閉連接）"""
        pass
    
    @classmethod
    def create_from_file(cls, file_path: str, compression: str = 'snappy', 
                         **kwargs) -> 'ParquetSource':
        """
        便捷方法：從檔案路徑創建Parquet數據源
        
        Args:
            file_path: Parquet檔案路徑
            compression: 壓縮方式
            **kwargs: 其他配置參數
            
        Returns:
            ParquetSource: Parquet數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.PARQUET,
            connection_params={
                'file_path': file_path,
                'compression': compression,
                **kwargs
            }
        )
        return cls(config)
    
    @classmethod
    def cleanup_executor(cls):
        """清理類級別的線程池（在程式結束時調用）"""
        if hasattr(cls, '_executor'):
            cls._executor.shutdown(wait=True)
