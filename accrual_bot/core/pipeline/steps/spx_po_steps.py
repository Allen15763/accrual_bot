"""
改進版 SPX 數據載入步驟
使用統一的 datasources 模組，提供更好的架構設計

優勢:
1. 統一的數據源接口，易於擴展
2. 線程安全的並發讀取
3. 自動資源管理
4. 更好的錯誤處理
5. 支援多種數據格式
6. 支援每個文件獨立參數配置

文件位置: core/pipeline/steps/spx_po_steps.py
"""

import os
import re
import time
import traceback
import pandas as pd
import numpy as np
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Any, Union
from pathlib import Path
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline import PipelineBuilder, Pipeline
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.datasources import (
    DataSourceFactory, 
    DataSourcePool,
    DataSourceConfig,
    DataSourceType
)
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.helpers.data_utils import (create_mapping_dict, 
                                                  safe_string_operation,
                                                  extract_date_range_from_description,
                                                  classify_description,
                                                  give_account_by_keyword)
from accrual_bot.utils.config.constants import STATUS_VALUES


# =============================================================================
# 輔助工具類別
# =============================================================================

class StepMetadataBuilder:
    """
    StepResult metadata 構建器
    提供標準化的 metadata 結構和鏈式 API
    """
    
    def __init__(self):
        self.metadata = {
            # 基本統計
            'input_rows': 0,
            'output_rows': 0,
            'rows_changed': 0,
            
            # 處理統計
            'records_processed': 0,
            'records_skipped': 0,
            'records_failed': 0,
            
            # 時間資訊
            'start_time': None,
            'end_time': None,
        }
    
    def set_row_counts(self, input_rows: int, output_rows: int) -> 'StepMetadataBuilder':
        """設置行數統計"""
        self.metadata['input_rows'] = int(input_rows)
        self.metadata['output_rows'] = int(output_rows)
        self.metadata['rows_changed'] = int(output_rows - input_rows)
        return self
    
    def set_process_counts(self, processed: int = 0, skipped: int = 0, 
                           failed: int = 0) -> 'StepMetadataBuilder':
        """設置處理計數"""
        self.metadata['records_processed'] = int(processed)
        self.metadata['records_skipped'] = int(skipped)
        self.metadata['records_failed'] = int(failed)
        return self
    
    def set_time_info(self, start_time: datetime, end_time: datetime) -> 'StepMetadataBuilder':
        """設置時間資訊"""
        self.metadata['start_time'] = start_time.isoformat()
        self.metadata['end_time'] = end_time.isoformat()
        return self
    
    def add_custom(self, key: str, value: Any) -> 'StepMetadataBuilder':
        """添加自定義 metadata"""
        self.metadata[key] = value
        return self
    
    def build(self) -> Dict[str, Any]:
        """構建並返回 metadata 字典"""
        return self.metadata.copy()


def create_error_metadata(error: Exception, context: ProcessingContext, 
                          step_name: str, **kwargs) -> Dict[str, Any]:
    """
    創建增強的錯誤 metadata
    
    Args:
        error: 發生的異常
        context: 處理上下文
        step_name: 步驟名稱
        **kwargs: 額外的上下文資訊
    
    Returns:
        Dict[str, Any]: 錯誤 metadata 字典
    """
    error_metadata = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'error_traceback': traceback.format_exc(),
        'step_name': step_name,
    }
    
    # 添加數據快照
    if context.data is not None:
        error_metadata['data_snapshot'] = {
            'total_rows': len(context.data),
            'total_columns': len(context.data.columns),
            'columns': list(context.data.columns)[:20],  # 只列前20個欄位
        }
    else:
        error_metadata['data_snapshot'] = {'status': 'no_data'}
    
    # 添加上下文變量
    error_metadata['context_variables'] = {
        k: str(v)[:100] for k, v in context.variables.items()  # 限制長度
    }
    
    # 添加額外資訊
    error_metadata.update(kwargs)
    
    return error_metadata


class SPXDataLoadingStep(PipelineStep):
    """
    SPX 數據並發載入步驟 (使用 datasources 模組)
    
    功能:
    1. 並發載入所有必要文件 (使用統一的數據源接口)
    2. 支援每個文件獨立參數配置
    3. 載入參考數據
    4. 將所有數據添加到 ProcessingContext
    5. 自動管理資源釋放
    
    參數格式:
    方式1 (舊格式 - 向後兼容):
        file_paths = {
            'raw_po': 'path/to/file.csv',
            'previous': 'path/to/file.xlsx'
        }
    
    方式2 (新格式 - 支援參數):
        file_paths = {
            'raw_po': {
                'path': 'path/to/file.csv',
                'params': {'encoding': 'utf-8', 'sep': ','}
            },
            'ops_validation': {
                'path': 'path/to/file.xlsx',
                'params': {'header': 1, 'usecols': 'A:AE'}
            }
        }
    """
    
    def __init__(self, 
                 name: str = "SPXDataLoading",
                 file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
                 **kwargs):
        super().__init__(
            name, 
            description="Load all SPX PO files using datasources module", 
            **kwargs
        )
        # 標準化為統一格式 (支援舊格式自動轉換)
        self.file_configs = self._normalize_file_paths(file_paths or {})
        self.pool = DataSourcePool()  # 數據源連接池
    
    def _normalize_file_paths(self, file_paths: Dict[str, Union[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        """
        標準化文件路徑格式，支援向後兼容
        
        Args:
            file_paths: 文件路徑字典（可能是舊格式或新格式）
            
        Returns:
            Dict[str, Dict[str, Any]]: 統一的配置格式
        """
        normalized = {}
        for file_type, config in file_paths.items():
            if isinstance(config, str):
                # 舊格式：直接是路徑字符串
                normalized[file_type] = {
                    'path': config,
                    'params': {}
                }
            elif isinstance(config, dict):
                # 新格式：已經是配置字典
                if 'path' not in config:
                    raise ValueError(f"Missing 'path' in config for {file_type}")
                normalized[file_type] = {
                    'path': config['path'],
                    'params': config.get('params', {})
                }
            else:
                raise ValueError(f"Invalid config type for {file_type}: {type(config)}")
        
        return normalized
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行並發數據載入"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            self.logger.info("Starting concurrent file loading with datasources...")
            
            # 驗證文件存在性
            validated_configs = self._validate_file_configs()
            
            if not validated_configs:
                raise ValueError("No valid files to load")
            
            # 階段 1: 並發載入所有主要文件
            loaded_data = await self._load_all_files_concurrent(validated_configs)
            
            # 階段 2: 提取和驗證主數據 (raw_po)
            if 'raw_po' not in loaded_data:
                raise ValueError("Failed to load raw PO data")
            
            df, date, m = self._extract_raw_po_data(loaded_data['raw_po'])
            
            # 更新 Context 主數據
            context.update_data(df)
            context.set_variable('processing_date', date)
            context.set_variable('processing_month', m)
            # 原處理OPS驗收底稿的方法介面需要路徑，故存成變量至Process_Validation步驟使用
            context.set_variable('validation_file_path', validated_configs.get('ops_validation').get('path'))
            # 提供快速測試支援
            context.set_variable('file_paths', validated_configs)
            
            # 階段 3: 添加輔助數據到 Context
            auxiliary_count = 0
            for data_name, data_content in loaded_data.items():
                if data_name != 'raw_po' and data_content is not None:
                    if isinstance(data_content, pd.DataFrame) and not data_content.empty:
                        context.add_auxiliary_data(data_name, data_content)
                        auxiliary_count += 1
                        self.logger.info(
                            f"Added auxiliary data: {data_name} {data_content.shape} shape)"
                        )
            
            # 階段 4: 載入參考數據
            ref_count = await self._load_reference_data(context)
            
            # ✅ 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            self.logger.info(
                f"Successfully loaded {df.shape} shape of PO data, "
                f"{auxiliary_count} auxiliary datasets, "
                f"{ref_count} reference datasets in {duration:.2f}s"
            )
            
            # ✅ 使用 StepMetadataBuilder 構建標準化 metadata
            metadata = (StepMetadataBuilder()
                        .set_row_counts(0, len(df))
                        .set_process_counts(processed=len(df))
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('po_records', len(df))
                        .add_custom('po_columns', len(df.columns))
                        .add_custom('processing_date', int(date))
                        .add_custom('processing_month', int(m))
                        .add_custom('auxiliary_datasets', auxiliary_count)
                        .add_custom('reference_datasets', ref_count)
                        .add_custom('loaded_files', list(loaded_data.keys()))
                        .add_custom('files_loaded_count', len(loaded_data))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Loaded {len(df)} PO records with {auxiliary_count} auxiliary datasets",
                duration=duration,  # ✅ 新增
                metadata=metadata  # ✅ 標準化
            )
            
        except Exception as e:
            duration = time.time() - start_time  # ✅ 錯誤時也計算時間
            
            self.logger.error(f"Data loading failed: {str(e)}", exc_info=True)
            context.add_error(f"Data loading failed: {str(e)}")
            
            # ✅ 創建增強的錯誤 metadata
            error_metadata = create_error_metadata(
                e, context, self.name,
                validated_files=list(self.file_configs.keys()) if self.file_configs else [],
                stage='data_loading'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Failed to load data: {str(e)}",
                duration=duration,  # ✅ 新增
                metadata=error_metadata  # ✅ 增強錯誤資訊
            )
        
        finally:
            # 清理資源
            await self._cleanup_resources()
    
    async def _load_all_files_concurrent(
        self, 
        validated_configs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        並發載入所有文件
        
        Args:
            validated_configs: 已驗證的文件配置字典
            
        Returns:
            Dict[str, Any]: 載入的數據字典
        """
        # 創建載入任務
        tasks = []
        file_names = []
        
        for file_type, config in validated_configs.items():
            task = self._load_single_file(file_type, config)
            tasks.append(task)
            file_names.append(file_type)
        
        # 並發執行所有任務
        self.logger.info(f"Loading {len(tasks)} files concurrently...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 整理結果
        loaded_data = {}
        for file_type, result in zip(file_names, results):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to load {file_type}: {str(result)}")
                # 可選文件失敗不影響整體流程
                if file_type not in ['raw_po']:  # raw_po 是必需的
                    loaded_data[file_type] = None
                else:
                    raise result
            else:
                loaded_data[file_type] = result
                self.logger.info(f"Successfully loaded: {file_type}")
        
        return loaded_data
    
    async def _load_single_file(
        self, 
        file_type: str, 
        config: Dict[str, Any]
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, int, int]]:
        """
        載入單個文件 (支援參數配置)
        
        Args:
            file_type: 文件類型 (raw_po, previous, procurement等)
            config: 文件配置 {'path': '...', 'params': {...}}
            
        Returns:
            Union[pd.DataFrame, Tuple]: 載入的數據
        """
        try:
            file_path = config['path']
            params = config.get('params', {})
            
            # DataSourceFactory 自動識別文件類型並使用參數創建數據源
            source = DataSourceFactory.create_from_file(file_path, **params)
            
            # 將數據源添加到連接池以便後續管理
            self.pool.add_source(file_type, source)
            
            # 根據文件類型決定載入策略
            if file_type == 'raw_po':
                # 主 PO 數據需要提取日期和月份
                return await self._load_raw_po_file(source, file_path)
            elif file_type == 'ap_invoice':
                return await self._load_ap_invoice(source)
            elif file_type == 'ops_validation':
                self.logger.warning("Pass loading ops_validation. Will load it on Process_Validation Step")
            else:
                # 其他文件直接讀取
                df = await source.read()
                self.logger.debug(f"成功導入 {file_type}, 數據維度: {df.shape}")
                return df
            
        except Exception as e:
            self.logger.error(f"Error loading {file_type} from {config.get('path')}: {str(e)}")
            raise
    
    async def _load_raw_po_file(
        self, 
        source, 
        file_path: str
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        載入原始 PO 文件並提取日期信息
        
        Args:
            source: 數據源實例
            file_path: 文件路徑
            
        Returns:
            Tuple[pd.DataFrame, int, int]: (DataFrame, date, month)
        """
        # 讀取數據
        df = await source.read()
        # 基本數據處理
        if 'Line#' in df.columns:
            df['Line#'] = df['Line#'].astype('Float64').round(0).astype('Int64').astype('string')
        
        if 'GL#' in df.columns:
            df['GL#'] = np.where(df['GL#'] == 'N.A.', '666666', df['GL#'])
            df['GL#'] = df['GL#'].fillna('666666').astype('Float64').round(0).astype('Int64').astype('string')

        self.logger.debug(f"成功導入PO數據, 數據維度: {df.shape}")
        
        # 從文件名提取日期 (假設格式為 YYYYMM_xxx.xlsx)
        file_name = Path(file_path).stem
        
        # 嘗試從文件名提取日期
        import re
        date_pattern = r'(\d{6})'
        match = re.search(date_pattern, file_name)
        
        if match:
            date_str = match.group(1)
            date = int(date_str)
            m = int(date_str[-2:])  # 月份
        else:
            # 如果無法從文件名提取，使用當前日期
            from datetime import datetime
            current = datetime.now()
            date = int(current.strftime('%Y%m'))
            m = current.month
            self.logger.warning(
                f"Could not extract date from filename, using current date: {date}"
            )
        
        return df, date, m
    
    async def _load_ap_invoice(self, source) -> pd.DataFrame:
        """
        在這邊用config_manager之類的方式讀取定義的設定用於source的讀取設定
        """
        
        # source支援kwargs，可以直接覆蓋
        df = await source.read(
            usecols=config_manager.get_list('SPX', 'ap_columns'),
            header=1,
            sheet_name=1,
            dtype=str
        )
        self.logger.debug(f"成功導入AP數據, 數據維度: {df.shape}")

        """不支援動態參數覆蓋就需要重建並投入指定參數

        # 重新創建數據源，指定參數
        await source.close()
        source = DataSourceFactory.create_from_file(
            file_path,
            usecols=ap_columns
        )
        self.pool.add_source(f"{file_type}_new", source)

        df = await source.read()
        """

        return df

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """
        載入參考數據 (科目映射、負債科目等)
        
        Args:
            context: 處理上下文
            
        Returns:
            int: 載入的參考數據集數量
        """
        try:
            # 這裡假設參考數據存放在固定位置
            # 實際使用時應該從配置讀取
            ref_data_path = config_manager._config_data.get('PATHS').get('ref_path_spt')
            
            count = 0
            
            # 載入科目映射/負債科目映射 (SPT 的參考數據)
            if Path(ref_data_path).exists():
                source = DataSourceFactory.create_from_file(str(ref_data_path))
                ref_ac = await source.read(dtype=str)
                context.add_auxiliary_data('reference_account', ref_ac.iloc[:, 1:3].copy())
                context.add_auxiliary_data('reference_liability', ref_ac.loc[:, ['Account', 'Liability']].copy())
                await source.close()
                count += 2
                self.logger.info(f"Loaded account mapping: {len(ref_ac)} records")
            else:
                # 如果找不到，創建空的參考數據
                self.logger.warning(f"Account mapping file not found: {ref_data_path}")
                context.add_auxiliary_data('reference_account', pd.DataFrame())
                context.add_auxiliary_data('reference_liability', pd.DataFrame())
            
            return count
            
        except Exception as e:
            self.logger.error(f"Failed to load reference data: {str(e)}")
            # 創建空的參考數據以避免後續步驟失敗
            context.add_auxiliary_data('reference_account', pd.DataFrame())
            context.add_auxiliary_data('reference_liability', pd.DataFrame())
            return 0
    
    def _validate_file_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        驗證文件配置
        
        Returns:
            Dict[str, Dict[str, Any]]: 驗證通過的文件配置字典
        """
        validated = {}
        
        for file_type, config in self.file_configs.items():
            file_path = config.get('path')
            
            if not file_path:
                self.logger.warning(f"No path provided for {file_type}")
                continue
            
            path = Path(file_path)
            
            if not path.exists():
                self.logger.warning(f"File not found: {file_type} at {file_path}")
                # raw_po 是必需的，其他是可選的
                if file_type == 'raw_po':
                    raise FileNotFoundError(f"Required file not found: {file_path}")
                continue
            
            validated[file_type] = config
            self.logger.debug(f"Validated file: {file_type}")
        
        return validated
    
    def _extract_raw_po_data(
        self, 
        raw_po_result: Tuple[pd.DataFrame, int, int]
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        提取和驗證原始 PO 數據
        
        Args:
            raw_po_result: 原始 PO 數據結果
            
        Returns:
            Tuple[pd.DataFrame, int, int]: 驗證後的數據
        """
        if isinstance(raw_po_result, tuple) and len(raw_po_result) == 3:
            df, date, m = raw_po_result
            
            # 驗證 DataFrame
            if df is None or df.empty:
                raise ValueError("Raw PO data is empty")
            
            # 驗證必要列
            required_columns = ['Product Code', 'Item Description', 'GL#']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            self.logger.info(
                f"Raw PO data validated: {df.shape} shape, "
                f"{len(df.columns)} columns, date={date}, month={m}"
            )
            
            return df, date, m
        else:
            raise ValueError("Invalid raw PO data format")
    
    async def _cleanup_resources(self):
        """清理數據源資源"""
        try:
            await self.pool.close_all()
            self.logger.debug("All data sources closed")
        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {str(e)}")
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if not self.file_configs:
            self.logger.error("No file configs provided")
            context.add_error("No file configs provided")
            return False
        
        # 檢查必要文件
        if 'raw_po' not in self.file_configs:
            self.logger.error("Missing raw PO file config")
            context.add_error("Missing raw PO file config")
            return False
        
        # 檢查文件是否存在
        raw_po_path = self.file_configs['raw_po'].get('path')
        if not raw_po_path or not Path(raw_po_path).exists():
            self.logger.error(f"Raw PO file not found: {raw_po_path}")
            context.add_error(f"Raw PO file not found: {raw_po_path}")
            return False
        
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作 - 清理已載入的資源"""
        self.logger.warning(f"Rolling back data loading due to error: {str(error)}")
        await self._cleanup_resources()


# =============================================================================
# 步驟 2: SPX 產品過濾步驟
# 替代原始: filter_spx_product_code()
# =============================================================================
class SPXProductFilterStep(PipelineStep):
    """
    SPX 產品代碼過濾步驟
    
    功能: 過濾出包含 SPX 產品代碼的記錄
    
    輸入: DataFrame with 'Product Code' column
    輸出: Filtered DataFrame
    """
    
    def __init__(self, 
                 name: str = "SPXProductFilter",
                 product_pattern: Optional[str] = None,
                 **kwargs):
        super().__init__(name, description="Filter SPX product codes", **kwargs)
        # 從配置讀取 pattern，或使用提供的值
        self.product_pattern = product_pattern or config_manager.get(
            'SPX', 'product_pattern', '(?i)LG_SPX'
        )
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行產品過濾"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            df = context.data.copy()
            original_count = len(df)
            
            self.logger.info(f"Filtering products with pattern: {self.product_pattern}")
            
            # 過濾 SPX 產品
            filtered_df = df.loc[
                df['Product Code'].str.contains(self.product_pattern, na=False), :
            ].reset_index(drop=True)
            
            context.update_data(filtered_df)
            
            filtered_count = len(filtered_df)
            removed_count = original_count - filtered_count
            
            # ✅ 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            self.logger.info(
                f"Product filtering complete: {original_count} -> {filtered_count} "
                f"(removed {removed_count} non-SPX items) in {duration:.2f}s"
            )
            
            if filtered_count == 0:
                context.add_warning("No SPX products found after filtering")
            
            # ✅ 標準化 metadata
            filter_rate = filtered_count / original_count * 100
            speed = original_count / duration
            metadata = (StepMetadataBuilder()
                        .set_row_counts(original_count, filtered_count)
                        .set_process_counts(processed=filtered_count, skipped=removed_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('filter_pattern', self.product_pattern)
                        .add_custom('filter_rate', f"{(filter_rate):.2f}%" if original_count > 0 else "N/A")
                        .add_custom('processing_speed_rows_per_sec', f"{(speed):.0f}" if duration > 0 else "N/A")
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=filtered_df,
                message=f"Filtered to {filtered_count} SPX items ({(filtered_count/original_count*100):.1f}%)",
                duration=duration,  # ✅ 新增
                metadata=metadata  # ✅ 標準化
            )
            
        except Exception as e:
            duration = time.time() - start_time  # ✅ 錯誤時也計算時間
            
            self.logger.error(f"Product filtering failed: {str(e)}", exc_info=True)
            context.add_error(f"Product filtering failed: {str(e)}")
            
            # ✅ 創建增強的錯誤 metadata
            error_metadata = create_error_metadata(
                e, context, self.name,
                filter_pattern=self.product_pattern,
                stage='product_filtering'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Product filtering failed: {str(e)}",
                duration=duration,  # ✅ 新增
                metadata=error_metadata  # ✅ 增強錯誤資訊
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data to filter")
            context.add_error("No data to filter")
            return False
        
        if 'Product Code' not in context.data.columns:
            self.logger.error("Missing 'Product Code' column")
            context.add_error("Missing 'Product Code' column")
            return False
        
        return True
    
# =============================================================================
# 步驟 3: 添加欄位步驟
# 替代原始: add_cols()
# =============================================================================

class ColumnAdditionStep(PipelineStep):
    """
    添加 SPX 特有欄位
    
    功能:
    1. 調用 add_basic_columns() 添加基礎欄位
    2. 添加 SPX 特定欄位 (memo, GL DATE等)
    
    輸入: DataFrame
    輸出: DataFrame with additional columns
    """
    
    def __init__(self, name: str = "ColumnAddition", **kwargs):
        super().__init__(name, description="Add SPX-specific columns", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行欄位添加"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()
            input_count = len(df)
            m = context.get_variable('processing_month')
            
            original_columns = set(df.columns)
            
            # 添加基礎欄位 (調用原 processor 的邏輯)
            # 這裡可以直接調用原有的方法，或在此處重新實作
            df, previous_month = self._add_basic_columns(df, m)
            
            # 添加 SPX 特定欄位
            df['memo'] = None
            df['GL DATE'] = None
            df['Remarked by Procurement PR'] = None
            df['Noted by Procurement PR'] = None
            df['Remarked by 上月 FN PR'] = None
            
            # 更新月份變數
            context.set_variable('processing_month', m)
            
            context.update_data(df)
            
            new_columns = set(df.columns) - original_columns
            output_count = len(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            self.logger.info(f"Added {len(new_columns)} columns: {new_columns}")
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, output_count)
                        .set_process_counts(processed=output_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('columns_added', len(new_columns))
                        .add_custom('new_columns', list(new_columns))
                        .add_custom('total_columns', len(df.columns))
                        .add_custom('updated_month', m)
                        .build())
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Added {len(new_columns)} columns",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Column addition failed: {str(e)}", exc_info=True)
            context.add_error(f"Column addition failed: {str(e)}")
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='column_addition'
            )
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Column addition failed: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    def _add_basic_columns(self, df: pd.DataFrame, month: int) -> Tuple[pd.DataFrame, int]:
        """
        添加基本必要列
        
        Args:
            df: 原始PO數據
            month: 月份
            
        Returns:
            Tuple[pd.DataFrame, int]: 添加了必要列的DataFrame和更新的月份
        """
        try:
            df_copy = df.copy()
            
            # 添加狀態欄位
            df_copy['是否結案'] = np.where(
                df_copy.get('Closed For Invoice', '0') == '0', 
                "未結案", 
                '結案'
            )
            
            # 計算結案差異數量
            if '是否結案' in df_copy.columns:
                df_copy['結案是否有差異數量'] = np.where(
                    df_copy['是否結案'] == '結案',
                    pd.to_numeric(df_copy.get('Entry Quantity', 0), errors='coerce') - 
                    pd.to_numeric(df_copy.get('Billed Quantity', 0), errors='coerce'),
                    '未結案'
                )
            
            # 檢查入帳金額
            df_copy['Check with Entry Invoice'] = np.where(
                pd.to_numeric(df_copy.get('Entry Billed Amount', 0), errors='coerce') > 0,
                pd.to_numeric(df_copy.get('Entry Amount', 0), errors='coerce') - 
                pd.to_numeric(df_copy.get('Entry Billed Amount', 0), errors='coerce'),
                '未入帳'
            )
            
            # 生成行號標識
            if 'PR#' in df_copy.columns and 'Line#' in df_copy.columns:
                df_copy['PR Line'] = df_copy['PR#'].astype('string') + '-' + df_copy['Line#'].astype('string')
            
            if 'PO#' in df_copy.columns and 'Line#' in df_copy.columns:
                df_copy['PO Line'] = df_copy['PO#'].astype('string') + '-' + df_copy['Line#'].astype('string')
            
            # 添加標記和備註欄位
            self._add_remark_columns(df_copy)
            
            # 添加計算欄位
            self._add_calculation_columns(df_copy)
            
            # 計算上月
            previous_month = 12 if month == 1 else month - 1
            
            self.logger.info("成功添加基本必要列")
            return df_copy, previous_month
            
        except Exception as e:
            self.logger.error(f"添加基本列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加基本列時出錯")
    
    def _add_remark_columns(self, df: pd.DataFrame) -> None:
        """添加備註相關欄位"""
        columns_to_add = [
            'Remarked by Procurement',
            'Noted by Procurement', 
            'Remarked by FN',
            'Noted by FN',
            'Remarked by 上月 Procurement',
            'Remarked by 上月 FN',
            'PO狀態'
        ]
        
        for col in columns_to_add:
            if col not in df.columns:
                df[col] = pd.NA
    
    def _add_calculation_columns(self, df: pd.DataFrame) -> None:
        """添加計算相關欄位"""
        calculation_columns = [
            '是否估計入帳',
            '是否為FA',
            '是否為S&M',
            'Account code',
            'Account Name',
            'Product code',
            'Region_c',
            'Dep.',
            'Currency_c',
            'Accr. Amount',
            'Liability',
            '是否有預付',
            'PR Product Code Check',
            'Question from Reviewer',
            'Check by AP'
        ]
        
        for col in calculation_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # 設置特定值
        df['是否為FA'] = self._determine_fa_status(df)
        df['是否為S&M'] = self._determine_sm_status(df)
    
    def _determine_fa_status(self, df: pd.DataFrame) -> pd.Series:
        """
        確定是否為FA
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.Series: 是否為FA的結果
        """
        fa_accounts: List = config_manager.get_list('FA_ACCOUNTS', 'spx')
        if 'GL#' in df.columns:
            return np.where(df['GL#'].astype('string').isin([str(x) for x in fa_accounts]), 'Y', '')
        return pd.Series('', index=df.index)
    
    def _determine_sm_status(self, df: pd.DataFrame) -> pd.Series:
        """
        確定是否為S&M
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.Series: 是否為S&M的結果
        """
        if 'GL#' not in df.columns:
            return pd.Series('N', index=df.index)
        
        return np.where(
            (df['GL#'].astype('string') == '650003') | (df['GL#'].astype('string') == '450014'), 
            "Y", "N"
        )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for column addition")
            return False
        
        return True

# =============================================================================
# 步驟 4: AP Invoice 整合步驟
# 替代原始: get_period_from_ap_invoice()
# =============================================================================

class APInvoiceIntegrationStep(PipelineStep):
    """
    AP Invoice 整合步驟
    
    功能:
    從 AP Invoice 數據中提取 GL DATE 並填入 PO 數據
    排除月份 m 之後的期間
    
    輸入: DataFrame + AP Invoice auxiliary data
    輸出: DataFrame with GL DATE column
    """
    
    def __init__(self, name: str = "APInvoiceIntegration", **kwargs):
        super().__init__(name, description="Integrate AP Invoice GL DATE", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 AP Invoice 整合"""
        start_time = time.time()
        start_datetime = datetime.now()
        try:
            df = context.data.copy()
            input_count = len(df)
            df_ap = context.get_auxiliary_data('ap_invoice')
            yyyymm = context.get_variable('processing_date')
            
            if df_ap is None or df_ap.empty:
                self.logger.warning("No AP Invoice data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No AP Invoice data"
                )
            
            self.logger.info("Processing AP Invoice integration...")
            
            # 移除缺少 'PO Number' 的行
            df_ap = df_ap.dropna(subset=['PO Number']).reset_index(drop=True)
            
            # 創建組合鍵
            df_ap['po_line'] = (
                df_ap['Company'].astype('string') + '-' + 
                df_ap['PO Number'].astype('string') + '-' + 
                df_ap['PO_LINE_NUMBER'].astype('string')
            )
            
            # 轉換 Period 為 yyyymm 格式
            df_ap['period'] = (
                pd.to_datetime(df_ap['Period'], format='%b-%y', errors='coerce')
                .dt.strftime('%Y%m')
                .fillna('0')
                .astype('Int32')
            )
            
            df_ap['match_type'] = df_ap['Match Type'].fillna('system_filled')
            
            # 只保留期間在 yyyymm 之前的 AP 發票
            df_ap = (
                df_ap.loc[df_ap['period'] <= yyyymm, :]
                .sort_values(by=['po_line', 'period'])
                .drop_duplicates(subset='po_line', keep='last')
                .reset_index(drop=True)
            )
            
            # 合併到主 DataFrame
            df = df.merge(
                df_ap[['po_line', 'period', 'match_type']], 
                left_on='PO Line', 
                right_on='po_line', 
                how='left'
            )
            
            df['GL DATE'] = df['period']
            df.drop(columns=['po_line', 'period'], inplace=True)
            
            context.update_data(df)
            
            matched_count = df['GL DATE'].notna().sum()
            output_count = len(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            self.logger.info(f"AP Invoice integration completed: {matched_count} records matched")
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, output_count)
                        .set_process_counts(processed=output_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('matched_records', int(matched_count))
                        .add_custom('total_records', len(df))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Integrated GL DATE for {matched_count} records",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"AP Invoice integration failed: {str(e)}", exc_info=True)
            context.add_error(f"AP Invoice integration failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for AP Invoice integration")
            return False
        
        if 'PO Line' not in context.data.columns:
            self.logger.error("Missing 'PO Line' column")
            return False
        
        return True
    
# =============================================================================
# 步驟 5: 前期底稿整合步驟
# 替代原始: judge_previous()
# =============================================================================

class PreviousWorkpaperIntegrationStep(PipelineStep):
    """
    前期底稿整合步驟
    
    功能:
    1. 整合前期 PO 底稿
    2. 整合前期 PR 底稿
    3. 處理 memo 欄位
    
    輸入: DataFrame + Previous WP (PO and PR)
    輸出: DataFrame with previous workpaper info
    """
    
    def __init__(self, name: str = "PreviousWorkpaperIntegration", **kwargs):
        super().__init__(name, description="Integrate previous workpaper", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行前期底稿整合"""
        start_time = time.time()
        try:
            df = context.data.copy()
            previous_wp = context.get_auxiliary_data('previous')
            previous_wp_pr = context.get_auxiliary_data('previous_pr')
            m = context.get_variable('processing_month')
            
            if previous_wp is None and previous_wp_pr is None:
                self.logger.warning("No previous workpaper data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No previous workpaper data"
                )
            
            self.logger.info("Processing previous workpaper integration...")
            
            # 處理 PO 前期底稿
            if previous_wp is not None and not previous_wp.empty:
                df = self._process_previous_po(df, previous_wp, m)
                self.logger.info("Previous PO workpaper integrated")
            
            # 處理 PR 前期底稿
            if previous_wp_pr is not None and not previous_wp_pr.empty:
                df = self._process_previous_pr(df, previous_wp_pr)
                self.logger.info("Previous PR workpaper integrated")
            
            context.update_data(df)
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Previous workpaper integrated successfully",
                duration=duration,
                metadata={
                    'po_integrated': previous_wp is not None,
                    'pr_integrated': previous_wp_pr is not None
                }
            )
            
        except Exception as e:
            self.logger.error(f"Previous workpaper integration failed: {str(e)}", exc_info=True)
            context.add_error(f"Previous workpaper integration failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _process_previous_po(self, df: pd.DataFrame, previous_wp: pd.DataFrame, m: int) -> pd.DataFrame:
        """處理前期 PO 底稿"""
        # 調用父類邏輯處理基本的前期底稿整合
        # 這裡需要實現類似 BasePOProcessor.process_previous_workpaper 的邏輯
        try:
            if previous_wp is None or previous_wp.empty:
                self.logger.info("前期底稿為空，跳過處理")
                return df
            
            # 重命名前期底稿中的列
            previous_wp_renamed = previous_wp.rename(
                columns={
                    'Remarked by FN': 'Remarked by FN_l',
                    'Remarked by Procurement': 'Remark by PR Team_l'
                }
            )

            # 獲取前期FN備註
            if 'PO Line' in df.columns:
                fn_mapping = create_mapping_dict(previous_wp_renamed, 'PO Line', 'Remarked by FN_l')
                df['Remarked by 上月 FN'] = df['PO Line'].map(fn_mapping)
                
                # 獲取前期採購備註
                procurement_mapping = create_mapping_dict(
                    previous_wp_renamed, 'PO Line', 'Remark by PR Team_l'
                )
                df['Remarked by 上月 Procurement'] = \
                    df['PO Line'].map(procurement_mapping)
            
            # 處理 memo 欄位
            if 'memo' in previous_wp.columns and 'PO Line' in df.columns:
                memo_mapping = dict(zip(previous_wp['PO Line'], previous_wp['memo']))
                df['memo'] = df['PO Line'].map(memo_mapping)
            
            # 處理前期 FN 備註
            if 'Remarked by FN' in previous_wp.columns:
                fn_mapping = dict(zip(previous_wp['PO Line'], previous_wp['Remarked by FN']))
                df['Remarked by 上月 FN'] = df['PO Line'].map(fn_mapping)
            return df
    
        except Exception as e:
            self.logger.error(f"處理前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理前期底稿時出錯")
    
    def _process_previous_pr(self, df: pd.DataFrame, previous_wp_pr: pd.DataFrame) -> pd.DataFrame:
        """處理前期 PR 底稿"""
        # 重命名前期 PR 底稿中的列
        if 'Remarked by FN' in previous_wp_pr.columns:
            previous_wp_pr = previous_wp_pr.rename(
                columns={'Remarked by FN': 'Remarked by FN_l'}
            )
        
        # 獲取前期 PR FN 備註
        if 'Remarked by FN_l' in previous_wp_pr.columns and 'PR Line' in df.columns:
            pr_fn_mapping = dict(zip(previous_wp_pr['PR Line'], previous_wp_pr['Remarked by FN_l']))
            df['Remarked by 上月 FN PR'] = df['PR Line'].map(pr_fn_mapping)
        
        return df
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for previous workpaper integration")
            return False
        
        return True


# =============================================================================
# 步驟 6: 採購底稿整合步驟
# 替代原始: judge_procurement()
# =============================================================================

class ProcurementIntegrationStep(PipelineStep):
    """
    採購底稿整合步驟
    
    功能:
    1. 整合採購 PO 底稿
    2. 整合採購 PR 底稿
    3. 移除 SPT 模組給的狀態（SPX 有自己的狀態邏輯）
    
    輸入: DataFrame + Procurement WP (PO and PR)
    輸出: DataFrame with procurement info
    """
    
    def __init__(self, name: str = "ProcurementIntegration", **kwargs):
        super().__init__(name, description="Integrate procurement workpaper", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行採購底稿整合"""
        start_time = time.time()
        try:
            df = context.data.copy()
            procurement = context.get_auxiliary_data('procurement_po')
            procurement_pr = context.get_auxiliary_data('procurement_pr')
            
            if procurement is None and procurement_pr is None:
                self.logger.warning("No procurement data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No procurement data"
                )
            
            self.logger.info("Processing procurement integration...")
            
            # 處理 PO 採購底稿
            if procurement is not None and not procurement.empty:
                df = self._process_procurement_po(df, procurement)
                self.logger.info("Procurement PO integrated")
            
            # 處理 PR 採購底稿
            if procurement_pr is not None and not procurement_pr.empty:
                df = self._process_procurement_pr(df, procurement_pr)
                self.logger.info("Procurement PR integrated")
            
            context.update_data(df)
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Procurement integrated successfully",
                metadata={
                    'po_integrated': procurement is not None,
                    'pr_integrated': procurement_pr is not None
                }
            )
            
        except Exception as e:
            self.logger.error(f"Procurement integration failed: {str(e)}", exc_info=True)
            context.add_error(f"Procurement integration failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _process_procurement_po(self, df: pd.DataFrame, procurement: pd.DataFrame) -> pd.DataFrame:
        """處理採購 PO 底稿"""
        # 調用父類邏輯處理基本的採購底稿整合
        # 這裡需要實現類似 BasePOProcessor.process_procurement_workpaper 的邏輯
        try:
            if procurement is None or procurement.empty:
                self.logger.info("採購底稿為空，跳過處理")
                return df
            
            # 重命名採購底稿中的列
            procurement_wp_renamed = procurement.rename(
                columns={
                    'Remarked by Procurement': 'Remark by PR Team',
                    'Noted by Procurement': 'Noted by PR'
                }
            )
            
            # 通過PO Line獲取備註
            if 'PO Line' in df.columns:
                procurement_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PO Line', 'Remark by PR Team'
                )
                df['Remarked by Procurement'] = df['PO Line'].map(procurement_mapping)
                
                noted_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PO Line', 'Noted by PR'
                )
                df['Noted by Procurement'] = df['PO Line'].map(noted_mapping)
            
            # 通過PR Line獲取備註（如果PO Line沒有匹配到）
            if 'PR Line' in df.columns:
                pr_procurement_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PR Line', 'Remark by PR Team'
                )
                
                # 只更新尚未匹配的記錄
                df['Remarked by Procurement'] = \
                    (df.apply(lambda x: pr_procurement_mapping.get(x['PR Line'], None) 
                              if x['Remarked by Procurement'] is pd.NA else x['Remarked by Procurement'], axis=1))
            
            # 設置FN備註為採購備註
            df['Remarked by FN'] = df['Remarked by Procurement']
            
            # 標記不在採購底稿中的PO
            if 'PO Line' in df.columns and 'PR Line' in df.columns:
                po_list = procurement_wp_renamed.get('PO Line', pd.Series([])).tolist()
                pr_list = procurement_wp_renamed.get('PR Line', pd.Series([])).tolist()
                
                mask_not_in_wp = (
                    (~df['PO Line'].isin(po_list)) & 
                    (~df['PR Line'].isin(pr_list))
                )
                df.loc[mask_not_in_wp, 'PO狀態'] = STATUS_VALUES['NOT_IN_PROCUREMENT']
            
            # 獲取採購備註
            if 'Remarked by Procurement' in procurement.columns and 'PO Line' in df.columns:
                procurement_mapping = dict(zip(procurement['PO Line'], procurement['Remarked by Procurement']))
                df['Remarked by Procurement'] = df['PO Line'].map(procurement_mapping)
            
            # 移除 SPT 模組給的狀態（SPX 有自己的狀態邏輯）
            if 'PO狀態' in df.columns:
                df.loc[df['PO狀態'] == 'Not In Procurement WP', 'PO狀態'] = pd.NA
            
            return df
    
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    def _process_procurement_pr(self, df: pd.DataFrame, procurement_pr: pd.DataFrame) -> pd.DataFrame:
        """處理採購 PR 底稿"""
        # 重命名 PR 採購底稿中的列
        procurement_pr = procurement_pr.rename(
            columns={
                'Remarked by Procurement': 'Remark by PR Team',
                'Noted by Procurement': 'Noted by PR'
            }
        )
        
        # 獲取 PR 採購底稿中的備註
        if 'Remark by PR Team' in procurement_pr.columns and 'PR Line' in df.columns:
            pr_procurement_mapping = dict(zip(procurement_pr['PR Line'], procurement_pr['Remark by PR Team']))
            df['Remarked by Procurement PR'] = df['PR Line'].map(pr_procurement_mapping)
        
        if 'Noted by PR' in procurement_pr.columns and 'PR Line' in df.columns:
            pr_noted_mapping = dict(zip(procurement_pr['PR Line'], procurement_pr['Noted by PR']))
            df['Noted by Procurement PR'] = df['PR Line'].map(pr_noted_mapping)
        
        return df
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for procurement integration")
            return False
        
        return True


# =============================================================================
# 步驟 7: 日期邏輯處理步驟
# 替代原始: apply_date_logic()
# =============================================================================

class DateLogicStep(PipelineStep):
    """
    日期邏輯處理步驟
    
    功能:
    1. 提取和處理 Item Description 中的日期範圍
    2. 轉換 Expected Received Month 格式
    
    輸入: DataFrame
    輸出: DataFrame with processed date columns
    """
    
    def __init__(self, name: str = "DateLogic", **kwargs):
        super().__init__(name, description="Process date logic", **kwargs)
        self.regex_patterns = config_manager.get_regex_patterns()
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行日期邏輯處理"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            self.logger.info("Processing date logic...")
            
            # 調用父類的日期邏輯方法
            # 這裡需要實現類似 BasePOProcessor.apply_date_logic 的邏輯
            
            # 處理分潤合作
            if 'Item Description' in df.columns:
                mask_profit_sharing = safe_string_operation(
                    df['Item Description'], 'contains', '分潤合作', na=False
                )
                
                mask_no_status = (
                    df['PO狀態'].isna() | (df['PO狀態'] == 'nan')
                )
                
                df.loc[mask_profit_sharing & mask_no_status, 'PO狀態'] = '分潤'
                
            # 處理已入帳
            if 'PO Entry full invoiced status' in df.columns and context.metadata.entity_type != 'SPX':
                mask_posted = (
                    (df['PO狀態'].isna() | (df['PO狀態'] == 'nan')) & 
                    (df['PO Entry full invoiced status'].astype('string') == '1')
                )
                df.loc[mask_posted, 'PO狀態'] = STATUS_VALUES['POSTED']
                df.loc[df['PO狀態'] == STATUS_VALUES['POSTED'], '是否估計入帳'] = "N"
            
            # 解析日期
            df = self.parse_date_from_description(df)
                
            context.update_data(df)
            duration = time.time() - start_time
            
            self.logger.info("Date logic processing completed")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                duration=duration,
                message="Date logic processed successfully"
            )
            
        except Exception as e:
            self.logger.error(f"Date logic processing failed: {str(e)}", exc_info=True)
            context.add_error(f"Date logic processing failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for date logic")
            return False
        
        required_columns = ['Item Description']
        missing = [col for col in required_columns if col not in context.data.columns]
        if missing:
            self.logger.error(f"Missing required columns: {missing}")
            return False
        
        return True
    
    def parse_date_from_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        從描述欄位解析日期範圍
        
        Args:
            df: 包含Item Description的DataFrame
            
        Returns:
            pd.DataFrame: 添加了解析結果的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 將Expected Receive Month轉換為數值格式以便比較
            if 'Expected Receive Month' in df_copy.columns:
                df_copy['Expected Received Month_轉換格式'] = pd.to_datetime(
                    df_copy['Expected Receive Month'], 
                    format='%b-%y',
                    errors='coerce'
                ).dt.strftime('%Y%m').fillna('0').astype('Int32')
            
            # 解析Item Description中的日期範圍
            if 'Item Description' in df_copy.columns:
                df_copy['YMs of Item Description'] = df_copy['Item Description'].apply(
                    lambda x: extract_date_range_from_description(x, self.regex_patterns)
                )
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"解析描述中的日期時出錯: {str(e)}", exc_info=True)
            raise ValueError("解析日期時出錯")

# =============================================================================
# 步驟 8: 關單清單整合步驟
# 替代原始: get_closing_note() + partial give_status_stage_1()
# =============================================================================

class ClosingListIntegrationStep(PipelineStep):
    """
    關單清單整合步驟
    
    功能:
    1. 從 Google Sheets 獲取 SPX 關單數據
    2. 合併多個年份的關單記錄
    3. 清理和處理數據
    4. 將關單信息整合到主數據中
    
    輸入: DataFrame
    輸出: DataFrame with closing list info
    
    參考: async_data_importer.import_spx_closing_list()
    """
    
    def __init__(self, name: str = "ClosingListIntegration", **kwargs):
        super().__init__(name, description="Integrate closing list from Google Sheets", **kwargs)
        self.sheets_importer = None
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行關單清單整合"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date
            
            self.logger.info("Getting SPX closing list from Google Sheets...")
            
            # 準備配置
            config = self._prepare_config()
            
            # 獲取關單數據
            df_spx_closing = self._get_closing_note(config)
            
            if df_spx_closing is None or df_spx_closing.empty:
                self.logger.warning("No closing list data available")
                context.add_auxiliary_data('closing_list', pd.DataFrame())
            else:
                context.add_auxiliary_data('closing_list', df_spx_closing)
                self.logger.info(f"Loaded {len(df_spx_closing)} closing records")
            
            context.update_data(df)
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Closing list integrated: {len(df_spx_closing) if df_spx_closing is not None else 0} records",
                duration=duration,
                metadata={
                    'closing_records': len(df_spx_closing) if df_spx_closing is not None else 0
                }
            )
            
        except Exception as e:
            self.logger.error(f"Closing list integration failed: {str(e)}", exc_info=True)
            context.add_error(f"Closing list integration failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _prepare_config(self) -> Dict[str, Any]:
        """準備 Google Sheets API 配置"""
        
        config = {
            'certificate_path': config_manager.get_credentials_config().get('certificate_path', None),
            'scopes': config_manager.get_credentials_config().get('scopes', None)
        }
        
        return config
    
    def _get_closing_note(self, config: Dict[str, Any]) -> pd.DataFrame:
        """獲取 SPX 關單數據
        
        參考 async_data_importer.import_spx_closing_list() 的實現
        從多個工作表讀取並合併關單記錄
        """
        try:
            # 初始化 Google Sheets Importer
            from ....data.importers.google_sheets_importer import GoogleSheetsImporter
            
            if self.sheets_importer is None:
                self.sheets_importer = GoogleSheetsImporter(config)
            
            # 定義要查詢的工作表
            # Spreadsheet ID: SPX 關單清單
            spreadsheet_id = '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE'
            
            queries = [
                (spreadsheet_id, '2023年_done', 'A:J'),
                (spreadsheet_id, '2024年', 'A:J'),
                (spreadsheet_id, '2025年', 'A:J')
            ]
            
            dfs = []
            for sheet_id, sheet_name, range_value in queries:
                try:
                    self.logger.info(f"Reading sheet: {sheet_name}")
                    df = self.sheets_importer.get_sheet_data(
                        sheet_id, 
                        sheet_name, 
                        range_value,
                        header_row=True,
                        skip_first_row=True
                    )
                    
                    if df is not None and not df.empty:
                        dfs.append(df)
                        self.logger.info(f"Successfully read {len(df)} records from {sheet_name}")
                    else:
                        self.logger.warning(f"Sheet {sheet_name} is empty")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to read sheet {sheet_name}: {str(e)}")
                    continue
            
            if not dfs:
                self.logger.warning("No closing list data retrieved from any sheet")
                return pd.DataFrame()
            
            # 合併所有 DataFrame
            combined_df = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"Combined {len(combined_df)} total records from {len(dfs)} sheets")
            
            # 數據清理和重命名
            combined_df = self._clean_closing_data(combined_df)
            
            self.logger.info(f"After cleaning: {len(combined_df)} valid closing records")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error getting closing note: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    def _clean_closing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理和處理關單數據
        
        參考 async_data_importer.import_spx_closing_list() 的清理邏輯
        """
        try:
            # 移除 Date 為空的記錄
            df_clean = df.dropna(subset=['Date']).copy()
            
            # 重命名欄位
            df_clean.rename(columns={
                'Date': 'date',
                'Type': 'type',
                'PO Number': 'po_no',
                'Requester': 'requester',
                'Supplier': 'supplier',
                'Line Number / ALL': 'line_no',
                'Reason': 'reason',
                'New PR Number': 'new_pr_no',
                'Remark': 'remark',
                'Done(V)': 'done_by_fn'
            }, inplace=True)
            
            # 過濾空的日期記錄
            df_clean = df_clean.query("date != ''").reset_index(drop=True)
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error cleaning closing data: {str(e)}")
            # 如果清理失敗，返回原始數據
            return df
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for closing list integration")
            return False
        
        return True
    
# =============================================================================
# 步驟 9: 第一階段狀態判斷步驟
# 替代原始: give_status_stage_1()
# =============================================================================

class StatusStage1Step(PipelineStep):
    """
    第一階段狀態判斷步驟
    
    功能:
    根據關單清單給予初始狀態
    
    輸入: DataFrame + Closing list
    輸出: DataFrame with initial status
    """
    
    def __init__(self, name: str = "StatusStage1", **kwargs):
        super().__init__(name, description="Evaluate status stage 1", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行第一階段狀態判斷"""
        start_time = time.time()
        try:
            df = context.data.copy()
            df_spx_closing = context.get_auxiliary_data('closing_list')
            processing_date = context.metadata.processing_date
            
            self.logger.info("Evaluating status stage 1...")
            
            if df_spx_closing is None or df_spx_closing.empty:
                self.logger.warning("No closing list data, skipping status stage 1")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No closing list data"
                )
            
            # 給予第一階段狀態
            df = self._give_status_stage_1(df, 
                                           df_spx_closing, 
                                           processing_date,
                                           entity_type=context.metadata.entity_type)
            
            context.update_data(df)
            
            status_counts = df['PO狀態'].value_counts().to_dict() if 'PO狀態' in df.columns else {}
            
            self.logger.info("Status stage 1 evaluation completed")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Status stage 1 evaluated",
                duration=duration,
                metadata={'status_counts': status_counts}
            )
            
        except Exception as e:
            self.logger.error(f"Status stage 1 evaluation failed: {str(e)}", exc_info=True)
            context.add_error(f"Status stage 1 evaluation failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _give_status_stage_1(self, 
                             df: pd.DataFrame, 
                             df_spx_closing: pd.DataFrame, 
                             date, 
                             **kwargs) -> pd.DataFrame:
        #     # 這裡實現類似原始 give_status_stage_1 的邏輯
        #     # 根據關單清單標記已關單的 PO
        """給予第一階段狀態 - SPX特有邏輯
        
        Args:
            df: PO/PR DataFrame
            df_spx_closing: SPX關單數據DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        if 'entity_type' in kwargs:
            entity_type = kwargs.get('entity_type')
        else:
            entity_type = 'context transfer error'

        utility_suppliers = config_manager.get(entity_type, 'utility_suppliers')
        if 'PO狀態' in df.columns:
            tag_column = 'PO狀態'
            # 依據已關單條件取得對應的PO#
            c1, c2 = self.is_closed_spx(df_spx_closing)
            to_be_close = df_spx_closing.loc[c1, 'po_no'].unique() if c1.any() else []
            closed = df_spx_closing.loc[c2, 'po_no'].unique() if c2.any() else []
            
            # 定義「上月FN」備註關單條件
            remarked_close_by_fn_last_month = (
                (df['Remarked by 上月 FN'].str.contains('刪|關', na=False)) | 
                (df['Remarked by 上月 FN PR'].astype('string').str.contains('刪|關', na=False))
            )
            
            # 統一轉換日期格式
            df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
            df['Remarked by 上月 FN PR'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN PR'])
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
            cond1 = \
                df['Item Description'].str.contains(config_manager.get(entity_type, 'deposit_keywords'), 
                                                    na=False)
            is_fa = df['GL#'].astype('string') == config_manager.get('FA_ACCOUNTS', entity_type, '199999')
            cond_exclude = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA
            df.loc[cond1 & ~is_fa & ~cond_exclude, tag_column] = \
                config_manager.get(entity_type, 'deposit_keywords_label')
            
            # 條件2：供應商與類別對應，做GL調整
            bao_supplier: list = config_manager.get_list(entity_type, 'bao_supplier')
            bao_categories: list = config_manager.get_list(entity_type, 'bao_categories')
            cond2 = (df['PO Supplier'].isin(bao_supplier)) & (df['Category'].isin(bao_categories))
            df.loc[cond2, tag_column] = 'GL調整'
            
            # 條件3：該PO#在待關單清單中
            cond3 = df['PO#'].astype('string').isin([str(x) for x in to_be_close])
            df.loc[cond3, tag_column] = '待關單'
            
            # 條件4：該PO#在已關單清單中
            cond4 = df['PO#'].astype('string').isin([str(x) for x in closed])
            df.loc[cond4, tag_column] = '已關單'
            
            # 條件5：上月FN備註含有「刪」或「關」
            cond5 = remarked_close_by_fn_last_month
            df.loc[cond5, tag_column] = '參照上月關單'
            
            # 條件6：若「Remarked by 上月 FN」含有「入FA」，則提取該數字，並更新狀態(xxxxxx入FA)
            # 部分完成xxxxxx入FA不計入，前期FN備註如果是部分完成的會掉到erm邏輯判斷
            cond6 = (
                (df['Remarked by 上月 FN'].str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN'].str.contains('部分完成', na=False))
            )
            if cond6.any():
                extracted_fn = self.extract_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                df.loc[cond6, tag_column] = extracted_fn
            
            # 條件7：若「Remarked by 上月 FN PR」含有「入FA」，則提取該數字，並更新狀態
            cond7 = (
                (df['Remarked by 上月 FN PR'].astype('string').str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN PR'].astype('string').str.contains('部分完成', na=False))
            )
            if cond7.any():
                extracted_pr = self.extract_fa_remark(df.loc[cond7, 'Remarked by 上月 FN PR'])
                df.loc[cond7, tag_column] = extracted_pr

            # 條件8：該筆資料supplier是"台電"、"台水"、"北水"等公共費用
            cond8 = df['PO Supplier'].fillna('system_filled').str.contains(utility_suppliers)
            df.loc[cond8, tag_column] = '授扣GL調整'

            # 費用類按申請人篩選
            is_non_labeled = (df[tag_column].isna()) | (df[tag_column] == '') | (df[tag_column] == 'nan')
            ops_rent: str = config_manager.get(entity_type, 'ops_for_rent')
            account_rent: str = config_manager.get(entity_type, 'account_rent')
            ops_intermediary: str = config_manager.get(entity_type, 'ops_for_intermediary')
            ops_other: str = config_manager.get(entity_type, 'ops_for_other')
            
            mask_erm_equals_current = df['Expected Received Month_轉換格式'] == date
            mask_account_rent = df['GL#'] == account_rent
            mask_ops_rent = df['PR Requester'] == ops_rent
            mask_descerm_equals_current = df['YMs of Item Description'].str[:6].astype('Int64') == date
            mask_desc_contains_intermediary = df['Item Description'].fillna('na').str.contains('(?i)intermediary')
            mask_ops_intermediary = df['PR Requester'] == ops_intermediary

            combined_cond = is_non_labeled & mask_erm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            combined_cond = is_non_labeled & mask_descerm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            # 租金已入帳
            booked_in_ap = (~df['GL DATE'].isna()) & ((df['GL DATE'] != '') | (df['GL DATE'] != 'nan'))
            df.loc[(df[tag_column] == '已完成_租金') & (booked_in_ap), tag_column] = '已入帳'

            uncompleted_rent = (
                ((df['Remarked by Procurement'] != 'error') &
                    is_non_labeled &
                    mask_ops_rent &
                    mask_account_rent &
                    (df['Item Description'].str.contains('(?i)租金', na=False))
                 ) &
                
                (
                    ((df['Expected Received Month_轉換格式'] <= df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     ) |
                    ((df['Expected Received Month_轉換格式'] > df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     )
                )
                    

            )
            df.loc[uncompleted_rent, tag_column] = '未完成_租金'

            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                ((df['Expected Received Month_轉換格式'] == date) |
                    ((df['Expected Received Month_轉換格式'] < date) & (df['Remarked by 上月 FN'].str.contains('已完成')))
                 )
            df.loc[combined_cond, tag_column] = '已完成_intermediary'
            
            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                (df['Expected Received Month_轉換格式'] > date)
            df.loc[combined_cond, tag_column] = '未完成_intermediary'

            # 要判斷OPS驗收數
            kiosk_suppliers: list = config_manager.get_list(entity_type, 'kiosk_suppliers')
            locker_suppliers: list = config_manager.get_list(entity_type, 'locker_suppliers')
            asset_suppliers: list = kiosk_suppliers + locker_suppliers

            # Exclude both general '入FA' but Include specific patterns(部分入)
            po_general_fa = df['Remarked by 上月 FN'].str.contains('入FA', na=False)
            po_specific_pattern = df['Remarked by 上月 FN'].str.contains(r'部分完成.*\d{6}入FA', na=False, regex=True)

            pr_general_fa = df['Remarked by 上月 FN PR'].astype('string').str.contains('入FA', na=False)
            pr_specific_pattern = (df['Remarked by 上月 FN PR']
                                   .astype('string').str.contains(r'部分完成.*\d{6}入FA', na=False, regex=True))

            doesnt_contain_fa = (~pr_general_fa & ~po_general_fa)
            specific_pattern = (pr_specific_pattern | po_specific_pattern)
            ignore_closed = ~df[tag_column].str.contains('關', na=False)
            mask = ((df['PO Supplier'].isin(asset_suppliers)) & 
                    (doesnt_contain_fa | specific_pattern) & 
                    (ignore_closed))
            df.loc[mask, tag_column] = 'Pending_validating'
            
            self.logger.info("成功給予第一階段狀態")
            return df
        else:
            tag_column = 'PR狀態'
            # 依據已關單條件取得對應的PO#
            c1, c2 = self.is_closed_spx(df_spx_closing)
            to_be_close = df_spx_closing.loc[c1, 'new_pr_no'].unique() if c1.any() else []
            closed = df_spx_closing.loc[c2, 'new_pr_no'].unique() if c2.any() else []
            
            # 定義「上月FN」備註關單條件
            remarked_close_by_fn_last_month = (
                df['Remarked by 上月 FN'].astype('string').str.contains('刪|關', na=False)
            )
            
            # 統一轉換日期格式
            df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
            cond1 = \
                df['Item Description'].str.contains(config_manager.get(entity_type, 'deposit_keywords'), 
                                                    na=False)
            is_fa = df['GL#'].astype('string') == config_manager.get('FA_ACCOUNTS', entity_type, '199999')
            cond_exclude = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA
            df.loc[cond1 & ~is_fa & ~cond_exclude, tag_column] = \
                config_manager.get(entity_type, 'deposit_keywords_label')
            
            # 條件2：供應商與類別對應，做GL調整
            bao_supplier: list = config_manager.get_list(entity_type, 'bao_supplier')
            bao_categories: list = config_manager.get_list(entity_type, 'bao_categories')
            cond2 = (df['PR Supplier'].isin(bao_supplier)) & (df['Category'].isin(bao_categories))
            df.loc[cond2, tag_column] = 'GL調整'
            
            # 條件3：該PR#在待關單清單中
            cond3 = df['PR#'].astype('string').isin([str(x) for x in to_be_close])
            df.loc[cond3, tag_column] = '待關單'
            
            # 條件4：該PR#在已關單清單中
            cond4 = df['PR#'].astype('string').isin([str(x) for x in closed])
            df.loc[cond4, tag_column] = '已關單'
            
            # 條件5：上月FN備註含有「刪」或「關」
            cond5 = remarked_close_by_fn_last_month
            df.loc[cond5, tag_column] = '參照上月關單'
            
            # 條件6：若「Remarked by 上月 FN」含有「入FA」，則提取該數字，並更新狀態(xxxxxx入FA)
            # 部分完成xxxxxx入FA不計入，前期FN備註如果是部分完成的會掉到erm邏輯判斷
            cond6 = (
                (df['Remarked by 上月 FN'].astype('string').str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN'].astype('string').str.contains('部分完成', na=False))
            )
            if cond6.any():
                extracted_fn = self.extract_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                df.loc[cond6, tag_column] = extracted_fn
            
            # 條件8：該筆資料supplier是"台電"、"台水"、"北水"等公共費用
            cond8 = df['PR Supplier'].fillna('system_filled').str.contains(utility_suppliers)
            df.loc[cond8, tag_column] = '授扣GL調整'

            # 費用類按申請人篩選
            is_non_labeled = (df[tag_column].isna()) | (df[tag_column] == '') | (df[tag_column] == 'nan')
            ops_rent: str = config_manager.get(entity_type, 'ops_for_rent')
            account_rent: str = config_manager.get(entity_type, 'account_rent')
            ops_intermediary: str = config_manager.get(entity_type, 'ops_for_intermediary')
            ops_other: str = config_manager.get(entity_type, 'ops_for_other')
            
            mask_erm_equals_current = df['Expected Received Month_轉換格式'] == date
            mask_account_rent = df['GL#'] == account_rent
            mask_ops_rent = df['Requester'] == ops_rent
            mask_descerm_equals_current = df['YMs of Item Description'].str[:6].astype('Int64') == date
            mask_desc_contains_intermediary = df['Item Description'].fillna('na').str.contains('(?i)intermediary')
            mask_ops_intermediary = df['Requester'] == ops_intermediary

            combined_cond = is_non_labeled & mask_erm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            combined_cond = is_non_labeled & mask_descerm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            uncompleted_rent = (
                ((df['Remarked by Procurement'] != 'error') &
                    is_non_labeled &
                    mask_ops_rent &
                    mask_account_rent &
                    (df['Item Description'].str.contains('(?i)租金', na=False))
                 ) &
                
                (
                    ((df['Expected Received Month_轉換格式'] <= df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     ) |
                    ((df['Expected Received Month_轉換格式'] > df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     )
                )

            )
            df.loc[uncompleted_rent, tag_column] = '未完成_租金'

            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                ((df['Expected Received Month_轉換格式'] == date) |
                    ((df['Expected Received Month_轉換格式'] < date) & (df['Remarked by 上月 FN']
                                                                    .astype('string').str.contains('已完成')))
                 )
            df.loc[combined_cond, tag_column] = '已完成_intermediary'
            
            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                (df['Expected Received Month_轉換格式'] > date)
            df.loc[combined_cond, tag_column] = '未完成_intermediary'

            # PR的智取櫃與繳費機，不會在PR驗收不估
            kiosk_suppliers: list = config_manager.get_list(entity_type, 'kiosk_suppliers')
            locker_suppliers: list = config_manager.get_list(entity_type, 'locker_suppliers')
            asset_suppliers: list = kiosk_suppliers + locker_suppliers
            ignore_closed = ~df[tag_column].str.contains('關', na=False)
            mask = ((df['PR Supplier'].isin(asset_suppliers)) & 
                    (ignore_closed))
            df.loc[mask, tag_column] = '智取櫃與繳費機'

            self.logger.info("成功給予第一階段狀態")
            # return df
        
        if 'PO#' in df_spx_closing.columns and 'PO#' in df.columns:
            closed_po_list = df_spx_closing['PO#'].unique().tolist()
            
            # 標記已關單的 PO
            df.loc[df['PO#'].isin(closed_po_list), 'Closing_Status'] = 'Closed'
        
        return df
    
    def is_closed_spx(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """判斷SPX關單狀態
        
        Args:
            df: 關單數據DataFrame
            
        Returns:
            Tuple[pd.Series, pd.Series]: (待關單條件, 已關單條件)
        """
        # [0]有新的PR編號，但FN未上系統關單的
        condition_to_be_closed = (
            (~df['new_pr_no'].isna()) & 
            (df['new_pr_no'] != '') & 
            (df['done_by_fn'].isna())
        )
        
        # [1]有新的PR編號，但FN已經上系統關單的
        condition_closed = (
            (~df['new_pr_no'].isna()) & 
            (df['new_pr_no'] != '') & 
            (~df['done_by_fn'].isna())
        )
        
        return condition_to_be_closed, condition_closed
    
    def convert_date_format_in_remark(self, series: pd.Series) -> pd.Series:
        """轉換備註中的日期格式 (YYYY/MM -> YYYYMM)
        
        Args:
            series: 包含日期的Series
            
        Returns:
            pd.Series: 轉換後的Series
        """
        try:
            return series.astype('string').str.replace(r'(\d{4})/(\d{2})', r'\1\2', regex=True)
        except Exception as e:
            self.logger.error(f"轉換日期格式時出錯: {str(e)}", exc_info=True)
            return series
        
    def extract_fa_remark(self, series: pd.Series) -> pd.Series:
        """提取FA備註中的日期
        
        Args:
            series: 包含FA備註的Series
            
        Returns:
            pd.Series: 提取的日期Series
        """
        try:
            return series.astype('string').str.extract(r'(\d{6}入FA)', expand=False)
        except Exception as e:
            self.logger.error(f"提取FA備註時出錯: {str(e)}", exc_info=True)
            return series
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for status stage 1")
            return False
        
        return True


# =============================================================================
# 步驟 10: ERM 邏輯步驟 (核心業務邏輯)
# 替代原始: erm()
# =============================================================================

@dataclass
class ERMConditions:
    """ERM 判斷條件集合 - 提高可讀性"""
    # 基礎條件組件
    no_status: pd.Series
    in_date_range: pd.Series
    erm_before_or_equal_file_date: pd.Series
    erm_after_file_date: pd.Series
    quantity_matched: pd.Series
    not_billed: pd.Series
    has_billing: pd.Series
    fully_billed: pd.Series
    has_unpaid_amount: pd.Series
    
    # 備註條件
    procurement_completed_or_rent: pd.Series
    fn_completed_or_posted: pd.Series
    pr_not_incomplete: pd.Series
    
    # FA 條件
    is_fa: pd.Series
    
    # 錯誤條件
    procurement_not_error: pd.Series
    out_of_date_range: pd.Series
    format_error: pd.Series


class SPXERMLogicStep(PipelineStep):
    """
    SPX ERM 邏輯步驟 - 完整實現版本
    
    功能：
    1. 設置檔案日期
    2. 判斷 11 種 PO 狀態（已入帳、已完成、Check收貨等）
    3. 根據狀態設置是否估計入帳
    4. 設置會計相關欄位（Account code, Product code, Dep.等）
    5. 計算預估金額（Accr. Amount）
    6. 處理預付款和負債科目
    7. 檢查 PR Product Code
    
    業務規則：
    - SPX 邏輯：「已完成」狀態的項目需要估列入帳
    - 其他狀態一律不估列（是否估計入帳 = N）
    
    輸入：
    - DataFrame with required columns
    - Reference data (科目映射、負債科目)
    - Processing date
    
    輸出：
    - DataFrame with PO狀態, 是否估計入帳, and accounting fields
    """
    
    def __init__(self, name: str = "SPX_ERM_Logic", **kwargs):
        super().__init__(
            name=name,
            description="Apply SPX ERM logic with 11 status conditions",
            **kwargs
        )
        
        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list('SPX', 'fa_accounts', ['199999'])
        self.dept_accounts = config_manager.get_list('SPX', 'dept_accounts', [])
        
        self.logger.info(f"Initialized {name} with FA accounts: {self.fa_accounts}")
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 ERM 邏輯"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.get_variable('processing_date')
            
            # 獲取參考數據
            ref_account = context.get_auxiliary_data('reference_account')
            ref_liability = context.get_auxiliary_data('reference_liability')
            
            if ref_account is None or ref_liability is None:
                raise ValueError("缺少參考數據：科目映射或負債科目")
            
            self.logger.info(f"開始 ERM 邏輯處理，處理日期：{processing_date}")
            
            # ========== 階段 1: 設置基本欄位 ==========
            df = self._set_file_date(df, processing_date)
            
            # ========== 階段 2: 構建判斷條件 ==========
            conditions = self._build_conditions(df, processing_date)
            
            # ========== 階段 3: 應用 11 個狀態條件 ==========
            df = self._apply_status_conditions(df, conditions)
            
            # ========== 階段 4: 處理格式錯誤 ==========
            df = self._handle_format_errors(df, conditions)
            
            # ========== 階段 5: 設置是否估計入帳 ==========
            df = self._set_accrual_flag(df)
            
            # ========== 階段 6: 設置會計欄位 ==========
            df = self._set_accounting_fields(df, ref_account, ref_liability)
            
            # ========== 階段 7: 檢查 PR Product Code ==========
            df = self._check_pr_product_code(df)
            
            # 更新上下文
            context.update_data(df)
            
            # 生成統計資訊
            stats = self._generate_statistics(df)
            
            self.logger.info(
                f"ERM 邏輯完成 - "
                f"需估列: {stats['accrual_count']} 筆, "
                f"總計: {stats['total_count']} 筆"
            )
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"ERM 邏輯已應用，{stats['accrual_count']} 筆需估列",
                duration=duration,
                metadata=stats
            )
            
        except Exception as e:
            self.logger.error(f"ERM 邏輯處理失敗: {str(e)}", exc_info=True)
            context.add_error(f"ERM 邏輯失敗: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    # ========== 階段 1: 基本設置 ==========
    
    def _set_file_date(self, df: pd.DataFrame, processing_date: int) -> pd.DataFrame:
        """設置檔案日期"""
        df['檔案日期'] = processing_date
        self.logger.debug(f"已設置檔案日期：{processing_date}")
        return df
    
    # ========== 階段 2: 構建條件 ==========
    
    def _build_conditions(self, df: pd.DataFrame, file_date: int) -> ERMConditions:
        """
        構建所有判斷條件
        
        將條件邏輯集中在此處，提高可讀性和維護性
        """
        # 基礎狀態條件
        no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # 日期範圍條件
        ym_start = df['YMs of Item Description'].str[:6].astype('Int32')
        ym_end = df['YMs of Item Description'].str[7:].astype('Int32')
        erm = df['Expected Received Month_轉換格式']
        
        in_date_range = erm.between(ym_start, ym_end, inclusive='both')
        erm_before_or_equal_file_date = erm <= file_date
        erm_after_file_date = erm > file_date
        
        # 數量條件
        quantity_matched = df['Entry Quantity'] == df['Received Quantity']
        
        # 帳務條件
        not_billed = df['Entry Billed Amount'].astype('Float64') == 0
        has_billing = df['Billed Quantity'] != '0'
        fully_billed = (
            df['Entry Amount'].astype('Float64') - 
            df['Entry Billed Amount'].astype('Float64')
        ) == 0
        has_unpaid_amount = (
            df['Entry Amount'].astype('Float64') - 
            df['Entry Billed Amount'].astype('Float64')
        ) != 0
        
        # 備註條件
        procurement_completed_or_rent = df['Remarked by Procurement'].str.contains(
            '(?i)已完成|rent', na=False
        )
        fn_completed_or_posted = df['Remarked by 上月 FN'].str.contains(
            '(?i)已完成|已入帳', na=False
        )
        pr_not_incomplete = ~df['Remarked by 上月 FN PR'].str.contains(
            '(?i)未完成', na=False
        )
        
        # FA 條件
        is_fa = df['GL#'].astype('string').isin([str(x) for x in self.fa_accounts])
        
        # 錯誤條件
        procurement_not_error = df['Remarked by Procurement'] != 'error'
        out_of_date_range = (
            (in_date_range == False) & 
            (df['YMs of Item Description'] != '100001,100002')
        )
        format_error = df['YMs of Item Description'] == '100001,100002'
        
        return ERMConditions(
            no_status=no_status,
            in_date_range=in_date_range,
            erm_before_or_equal_file_date=erm_before_or_equal_file_date,
            erm_after_file_date=erm_after_file_date,
            quantity_matched=quantity_matched,
            not_billed=not_billed,
            has_billing=has_billing,
            fully_billed=fully_billed,
            has_unpaid_amount=has_unpaid_amount,
            procurement_completed_or_rent=procurement_completed_or_rent,
            fn_completed_or_posted=fn_completed_or_posted,
            pr_not_incomplete=pr_not_incomplete,
            is_fa=is_fa,
            procurement_not_error=procurement_not_error,
            out_of_date_range=out_of_date_range,
            format_error=format_error
        )
    
    # ========== 階段 3: 應用狀態條件 ==========
    
    def _apply_status_conditions(self, df: pd.DataFrame, 
                                 cond: ERMConditions) -> pd.DataFrame:
        """
        應用 11 個狀態判斷條件
        
        條件優先順序從上到下，符合的條件會被優先設置
        """
        
        # === 條件 1: 已入帳（前期FN明確標註）===
        condition_1 = df['Remarked by 上月 FN'].str.contains('(?i)已入帳', na=False)
        df.loc[condition_1, 'PO狀態'] = '已入帳'
        self._log_condition_result("已入帳（前期FN明確標註）", condition_1.sum())
        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 2: 已入帳（有 GL DATE 且符合其他條件）===
        condition_2 = (
            (~df['GL DATE'].isna()) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            cond.has_billing &
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            (~cond.is_fa)
        )
        df.loc[condition_2, 'PO狀態'] = '已入帳'
        self._log_condition_result("已入帳（GL DATE）", condition_2.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 3: 已完成 ===
        condition_3 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.pr_not_incomplete &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            cond.not_billed
        )
        df.loc[condition_3, 'PO狀態'] = '已完成'
        self._log_condition_result("已完成", condition_3.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 4: 全付完，未關單 ===
        condition_4 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            (df['Entry Billed Amount'].astype('Float64') != 0) &
            cond.fully_billed
        )
        df.loc[condition_4, 'PO狀態'] = '全付完，未關單?'
        self._log_condition_result("全付完，未關單", condition_4.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 5: 已完成但有未付款部分 ===
        condition_5 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            (df['Entry Billed Amount'].astype('Float64') != 0) &
            cond.has_unpaid_amount
        )
        df.loc[condition_5, 'PO狀態'] = '已完成'
        self._log_condition_result("已完成（有未付款）", condition_5.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 6: Check收貨 ===
        condition_6 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            (~cond.quantity_matched)
        )
        df.loc[condition_6, 'PO狀態'] = 'Check收貨'
        self._log_condition_result("Check收貨", condition_6.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 7: 未完成 ===
        condition_7 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.in_date_range &
            cond.erm_after_file_date
        )
        df.loc[condition_7, 'PO狀態'] = '未完成'
        self._log_condition_result("未完成", condition_7.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 8: 範圍錯誤_租金 ===
        condition_8 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Item Description'].str.contains('(?i)租金', na=False))
        )
        df.loc[condition_8, 'PO狀態'] = 'error(Description Period is out of ERM)_租金'
        self._log_condition_result("範圍錯誤_租金", condition_8.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 9: 範圍錯誤_薪資 ===
        condition_9 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Item Description'].str.contains('(?i)派遣|Salary|Agency Fee', na=False))
        )
        df.loc[condition_9, 'PO狀態'] = 'error(Description Period is out of ERM)_薪資'
        self._log_condition_result("範圍錯誤_薪資", condition_9.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 10: 範圍錯誤（一般）===
        condition_10 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range
        )
        df.loc[condition_10, 'PO狀態'] = 'error(Description Period is out of ERM)'
        self._log_condition_result("範圍錯誤（一般）", condition_10.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 11: 部分完成ERM ===
        condition_11 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Received Quantity'].astype('Float64') != 0) &
            (~cond.quantity_matched)
        )
        df.loc[condition_11, 'PO狀態'] = '部分完成ERM'
        self._log_condition_result("部分完成ERM", condition_11.sum())
        
        return df
    
    def _log_condition_result(self, condition_name: str, count: int):
        """記錄條件判斷結果"""
        if count > 0:
            self.logger.debug(f"條件 [{condition_name}]: {count} 筆符合")
    
    # ========== 階段 4: 處理格式錯誤 ==========
    
    def _handle_format_errors(self, df: pd.DataFrame, 
                              cond: ERMConditions) -> pd.DataFrame:
        """處理格式錯誤的記錄"""
        mask_format_error = cond.no_status & cond.format_error
        df.loc[mask_format_error, 'PO狀態'] = '格式錯誤，退單'
        
        error_count = mask_format_error.sum()
        if error_count > 0:
            self.logger.warning(f"發現 {error_count} 筆格式錯誤")
        
        return df
    
    # ========== 階段 5: 設置是否估計入帳 ==========
    
    def _set_accrual_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根據 PO狀態 設置是否估計入帳
        
        SPX 邏輯：只有「已完成」狀態需要估列入帳
        """
        mask_completed = df['PO狀態'].str.contains('已完成', na=False)
        
        df.loc[mask_completed, '是否估計入帳'] = 'Y'
        df.loc[~mask_completed, '是否估計入帳'] = 'N'
        
        accrual_count = mask_completed.sum()
        self.logger.info(f"設置估列標記：{accrual_count} 筆需估列")
        
        return df
    
    # ========== 階段 6: 設置會計欄位 ==========
    
    def _set_accounting_fields(self, df: pd.DataFrame,
                               ref_account: pd.DataFrame,
                               ref_liability: pd.DataFrame) -> pd.DataFrame:
        """設置所有會計相關欄位"""
        
        need_accrual = df['是否估計入帳'] == 'Y'
        
        # 1. Account code
        df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']
        
        # 2. Account Name（通過 merge）
        df = self._set_account_name(df, ref_account, need_accrual)
        
        # 3. Product code
        df.loc[need_accrual, 'Product code'] = df.loc[need_accrual, 'Product Code']
        
        # 4. Region_c（SPX 固定值）
        df.loc[need_accrual, 'Region_c'] = "TW"
        
        # 5. Dep.（部門代碼）
        df = self._set_department(df, need_accrual)
        
        # 6. Currency_c
        df.loc[need_accrual, 'Currency_c'] = df.loc[need_accrual, 'Currency']
        
        # 7. Accr. Amount（預估金額）
        df = self._calculate_accrual_amount(df, need_accrual)
        
        # 8. 預付款處理
        df = self._handle_prepayment(df, need_accrual, ref_liability)
        
        self.logger.info("會計欄位設置完成")
        
        return df
    
    def _set_account_name(self, df: pd.DataFrame, ref_account: pd.DataFrame,
                          mask: pd.Series) -> pd.DataFrame:
        """設置會計科目名稱"""
        if ref_account.empty:
            self.logger.warning("參考科目資料為空")
            return df
        
        # 使用 merge 從參考資料取得科目名稱
        merged = pd.merge(
            df, 
            ref_account[['Account', 'Account Desc']],
            how='left',
            left_on='Account code',
            right_on='Account'
        )
        
        df['Account Name'] = merged['Account Desc']
        
        return df
    
    def _set_department(self, df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
        """
        設置部門代碼
        
        規則：
        - 如果科目在 dept_accounts 清單中，取 Department 前3碼
        - 否則設為 '000'
        """
        isin_dept = df['Account code'].astype('string').isin(
            [str(x) for x in self.dept_accounts]
        )
        
        # 在 dept_accounts 中的科目
        df.loc[mask & isin_dept, 'Dep.'] = \
            df.loc[mask & isin_dept, 'Department'].str[:3]
        
        # 不在 dept_accounts 中的科目
        df.loc[mask & ~isin_dept, 'Dep.'] = '000'
        
        return df
    
    def _calculate_accrual_amount(self, df: pd.DataFrame, 
                                  mask: pd.Series) -> pd.DataFrame:
        """
        計算預估金額
        
        公式：Unit Price × (Entry Quantity - Billed Quantity)
        """
        df['temp_amount'] = (
            df['Unit Price'].astype('Float64') * 
            (df['Entry Quantity'].astype('Float64') - 
             df['Billed Quantity'].astype('Float64'))
        )
        
        df.loc[mask, 'Accr. Amount'] = df.loc[mask, 'temp_amount']
        df.drop('temp_amount', axis=1, inplace=True)
        
        return df
    
    def _handle_prepayment(self, df: pd.DataFrame, mask: pd.Series,
                           ref_liability: pd.DataFrame) -> pd.DataFrame:
        """
        處理預付款和負債科目
        
        規則：
        - 有預付款：是否有預付 = 'Y'，Liability = '111112'
        - 無預付款：從參考資料查找 Liability
        """
        is_prepayment = df['Entry Prepay Amount'] != '0'
        df.loc[mask & is_prepayment, '是否有預付'] = 'Y'
        
        # 設置 Liability（無預付款的情況）
        if not ref_liability.empty:
            merged = pd.merge(
                df,
                ref_liability[['Account', 'Liability']],
                how='left',
                left_on='Account code',
                right_on='Account'
            )
            df['Liability'] = merged['Liability_y']
        
        # 有預付款的情況，覆蓋為 '111112'
        df.loc[mask & is_prepayment, 'Liability'] = '111112'
        
        return df
    
    # ========== 階段 7: PR Product Code 檢查 ==========
    
    def _check_pr_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        檢查 PR 的 Product Code 是否與 Project 一致
        
        規則：
        從 Project 欄位提取第一個詞，與 Product code 比對
        - 一致：good
        - 不一致：bad
        """
        if 'Product code' not in df.columns or 'Project' not in df.columns:
            self.logger.warning("缺少 Product code 或 Project 欄位，跳過檢查")
            return df
        
        mask = df['Product code'].notnull()
        
        try:
            # 提取 Project 的第一個詞
            project_first_word = df.loc[mask, 'Project'].str.findall(
                r'^(\w+(?:))'
            ).apply(lambda x: x[0] if len(x) > 0 else '')
            
            # 比對
            product_match = (project_first_word == df.loc[mask, 'Product code'])
            
            df.loc[mask, 'PR Product Code Check'] = np.where(
                product_match, 'good', 'bad'
            )
            
            bad_count = (~product_match).sum()
            if bad_count > 0:
                self.logger.warning(f"發現 {bad_count} 筆 PR Product Code 不一致")
                
        except Exception as e:
            self.logger.error(f"PR Product Code 檢查失敗: {str(e)}")
        
        return df
    
    # ========== 輔助方法 ==========
    
    def _generate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """生成統計資訊"""
        stats = {
            'total_count': len(df),
            'accrual_count': (df['是否估計入帳'] == 'Y').sum(),
            'status_distribution': {}
        }
        
        if 'PO狀態' in df.columns:
            status_counts = df['PO狀態'].value_counts().to_dict()
            stats['status_distribution'] = {
                str(k): int(v) for k, v in status_counts.items()
            }
        
        return stats
    
    # ========== 驗證方法 ==========
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        df = context.data
        
        if df is None or df.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False
        
        # 檢查必要欄位
        required_columns = [
            'GL#', 'Expected Received Month_轉換格式',
            'YMs of Item Description', 'Entry Quantity',
            'Received Quantity', 'Billed Quantity',
            'Entry Amount', 'Entry Billed Amount',
            'Item Description', 'Remarked by Procurement',
            'Remarked by 上月 FN', 'Unit Price', 'Currency',
            'Product Code'
        ]
        
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            self.logger.error(f"缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False
        
        # 檢查參考數據
        ref_account = context.get_auxiliary_data('reference_account')
        ref_liability = context.get_auxiliary_data('reference_liability')
        
        if ref_account is None or ref_liability is None:
            self.logger.error("缺少參考數據")
            context.add_error("缺少參考數據")
            return False
        
        # 檢查處理日期
        processing_date = context.get_variable('processing_date')
        if processing_date is None:
            self.logger.error("缺少處理日期")
            context.add_error("缺少處理日期")
            return False
        
        self.logger.info("輸入驗證通過")
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作（如需要）"""
        self.logger.warning(f"回滾 ERM 邏輯：{str(error)}")
        # SPX ERM 步驟通常不需要特殊回滾操作
    

# =============================================================================
# 步驟 11: 驗收數據處理步驟
# 替代原始: process_validation_data() + apply_validation_data_to_po()
# =============================================================================

class ValidationDataProcessingStep(PipelineStep):
    """
    驗收數據處理步驟
    
    功能:
    1. 處理智取櫃驗收明細
    2. 處理繳費機驗收明細
    3. 將驗收數據應用到 PO DataFrame
    
    輸入: DataFrame + Validation file path
    輸出: DataFrame with validation data applied
    """
    
    def __init__(self, 
                 name: str = "ValidationDataProcessing",
                 validation_file_path: Optional[str] = None,
                 **kwargs):
        super().__init__(name, description="Process validation data", **kwargs)
        self.validation_file_path = validation_file_path
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行驗收數據處理"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date
            
            # 從 context 獲取驗收文件路徑
            validation_path = self.validation_file_path or context.get_variable('validation_file_path')
            
            if not validation_path:
                self.logger.warning("No validation file path provided, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No validation file path"
                )
            
            if not os.path.exists(validation_path):
                self.logger.warning(f"Validation file not found: {validation_path}")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="Validation file not found"
                )
            
            self.logger.info("Processing validation data...")
            
            # 處理驗收數據
            locker_non_discount, locker_discount, discount_rate, kiosk_data = \
                self._process_validation_data(validation_path, processing_date)
            
            # 應用驗收數據
            df = self._apply_validation_data(df, locker_non_discount, locker_discount, 
                                             discount_rate, kiosk_data)
            
            context.update_data(df)
            
            validation_count = (df['本期驗收數量/金額'] != 0).sum() if '本期驗收數量/金額' in df.columns else 0
            
            self.logger.info(f"Validation data processed: {validation_count} records updated")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Validation data applied to {validation_count} records",
                duration=duration,
                metadata={
                    'validation_records': int(validation_count),
                    'locker_non_discount_count': len(locker_non_discount),
                    'locker_discount_count': len(locker_discount),
                    'kiosk_count': len(kiosk_data)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Validation data processing failed: {str(e)}", exc_info=True)
            context.add_error(f"Validation data processing failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _process_validation_data(self, validation_file_path: str, target_date: int) -> Tuple[Dict, Dict, float, Dict]:
        """
        處理驗收數據 - 智取櫃和繳費機驗收明細
        
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Tuple[Dict, Dict, Dict]: (智取櫃非折扣驗收數量, 智取櫃折扣驗收數量, 折扣率, 繳費機驗收數量)
        """
        
        # 處理智取櫃驗收明細
        locker_data = self._process_locker_validation_data(validation_file_path, target_date)
        
        # 處理繳費機驗收明細
        kiosk_data = self._process_kiosk_validation_data(validation_file_path, target_date)
        
        return locker_data['non_discount'], locker_data['discount'], locker_data.get('discount_rate'), kiosk_data
    
    def _process_locker_validation_data(self, validation_file_path: str, target_date: int) -> Dict:
        """
        處理智取櫃驗收明細
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Dict[str, dict]: 包含非折扣和折扣驗收數量的字典
        """
        
        try:
            # 讀取智取櫃驗收明細
            df_locker = pd.read_excel(
                validation_file_path, 
                sheet_name=config_manager.get('SPX', 'locker_sheet_name'), 
                header=int(config_manager.get('SPX', 'locker_header')), 
                usecols=config_manager.get('SPX', 'locker_usecols')
            )
            
            if df_locker.empty:
                return {'non_discount': {}, 'discount': {}, 'discount_rate': None}
            
            # 設置欄位名稱
            locker_columns = config_manager.get_list('SPX', 'locker_columns')
            df_locker.columns = locker_columns
            
            # 過濾和轉換
            df_locker = df_locker.loc[~df_locker['驗收月份'].isna(), :].reset_index(drop=True)
            df_locker['validated_month'] = pd.to_datetime(
                df_locker['驗收月份'], errors='coerce'
            ).dt.strftime('%Y%m').astype('Int64')
            
            # 移除無效日期的記錄
            df_locker = df_locker.dropna(subset=['validated_month']).reset_index(drop=True)
            # 篩選目標月份的數據
            df_locker_filtered = df_locker.loc[df_locker['validated_month'] == target_date, :]
            
            if df_locker_filtered.empty:
                return {'non_discount': {}, 'discount': {}, 'discount_rate': None}
            
            # 聚合欄位
            agg_cols = [
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'DA', 
                'XA', 'XB', 'XC', 'XD', 'XE', 'XF',
                '超出櫃體安裝費', '超出櫃體運費', '裝運費'
            ]
            
            return self._categorize_validation_data(df_locker_filtered, agg_cols)
            
        except Exception as e:
            self.logger.error(f"Processing locker validation data failed: {str(e)}")
            return {'non_discount': {}, 'discount': {}, 'discount_rate': None}
    
    def _process_kiosk_validation_data(self, validation_file_path: str, target_date: int) -> Dict:
        """
        處理繳費機驗收明細
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Dict[str, dict]: 繳費機驗收數量字典
        """
        try:
            # 讀取繳費機驗收明細
            df_kiosk = pd.read_excel(
                validation_file_path, 
                sheet_name=config_manager.get('SPX', 'kiosk_sheet_name'), 
                usecols=config_manager.get('SPX', 'kiosk_usecols')
            )
            
            if df_kiosk.empty:
                return {}
            
            # 過濾和轉換
            df_kiosk = df_kiosk.loc[~df_kiosk['驗收月份'].isna(), :].reset_index(drop=True)
            df_kiosk['validated_month'] = pd.to_datetime(
                df_kiosk['驗收月份'], errors='coerce'
            ).dt.strftime('%Y%m').astype('Int64')
            
            df_kiosk = df_kiosk.dropna(subset=['validated_month']).reset_index(drop=True)
            df_kiosk_filtered = df_kiosk.loc[df_kiosk['validated_month'] == target_date, :]
            
            if df_kiosk_filtered.empty:
                return {}
            
            # 取得當期驗收數
            kiosk_validation = df_kiosk_filtered['PO單號'].value_counts().to_dict()
            return kiosk_validation
            
        except Exception as e:
            self.logger.error(f"Processing kiosk validation data failed: {str(e)}")
            return {}
    
    def _categorize_validation_data(self, df: pd.DataFrame, agg_cols: List[str]) -> Dict:
        """
        分類驗收數據為折扣和非折扣
        Args:
            df: 驗收數據DataFrame
            agg_cols: 需要聚合的欄位列表
            
        Returns:
            Dict[str, dict]: 包含 'non_discount' 和 'discount'， 'discount_rate' 鍵的字典
        """
        
        validation_results = {'non_discount': {}, 'discount': {}, 'discount_rate': None}
        
        if 'discount' not in df.columns:
            self.logger.warning("智取櫃數據中沒有 discount 欄位，所有數據將歸類為非折扣")
            df['discount'] = ''
        
        # 確保 discount 欄位為字符串類型
        df['discount'] = df['discount'].fillna('').astype('string')
        
        # 非折扣驗收 (不包含 X折驗收/出貨 的記錄)
        locker_discount_pattern = config_manager.get('SPX', 'locker_discount_pattern', r'\d+折')
        non_discount_condition = ~df['discount'].str.contains(locker_discount_pattern, na=False, regex=True)
        df_non_discount = df.loc[non_discount_condition, :]
        
        if not df_non_discount.empty and 'PO單號' in df_non_discount.columns:
            validation_results['non_discount'] = (
                df_non_discount.groupby(['PO單號'])[agg_cols]
                .sum()
                .to_dict('index')
            )
        
        # 折扣驗收
        discount_condition = df['discount'].str.contains(locker_discount_pattern, na=False, regex=True)
        df_discount = df.loc[discount_condition, :]
        
        if not df_discount.empty and 'PO單號' in df_discount.columns:
            validation_results['discount'] = (
                df_discount.groupby(['PO單號'])[agg_cols]
                .sum()
                .to_dict('index')
            )
            # 提取折扣率
            validation_results['discount_rate'] = self._extract_discount_rate(df_discount['discount'].unique())
        
        return validation_results
    
    def _extract_discount_rate(self, discount_input: Optional[Union[str, np.ndarray]]) -> Optional[float]:
        """從輸入中提取折扣率。
        
        此函數能處理字串或包含字串的 NumPy 陣列。
        如果輸入為陣列，預設只會處理第一個元素。

        Args:
            discount_input: 折扣字串 (e.g., "8折驗收") 或包含此類字串的 NumPy 陣列。
            
        Returns:
            折扣率 (e.g., 0.8)，若無法提取或輸入無效則返回 None。
        """
        # --- 步驟 1: 輸入正規化 (Input Normalization) ---
        target_str: Optional[str] = None

        if discount_input is None:
            return None
        
        if isinstance(discount_input, str):
            target_str = discount_input
        elif isinstance(discount_input, np.ndarray):
            if discount_input.size == 0:
                self.logger.info("輸入的 ndarray 為空，無法提取折扣率。")
                return None
            
            if discount_input.size > 1:
                self.logger.warning(
                    f"輸入為多值陣列，只處理第一個元素 '{discount_input[0]}'. "
                    f"被忽略的值: {list(discount_input[1:])}"
                )
            target_str = str(discount_input[0])  # 確保取出的元素是字串
        else:
            self.logger.error(f"不支援的輸入類型: {type(discount_input)}")
            raise TypeError(f"Input must be str or np.ndarray, not {type(discount_input)}")

        # --- 步驟 2: 核心提取邏輯 (Core Extraction Logic) ---
        if not target_str:  # 處理空字串或 None 的情況
            return None

        match = re.search(r'(\d+)折', target_str)
        if match:
            try:
                discount_num = int(match.group(1))
                rate = discount_num / 10.0
                self.logger.info(f"從 '{target_str}' 成功提取折扣率: {rate}")
                return rate
            except (ValueError, IndexError):
                self.logger.error(f"從 '{target_str}' 匹配到數字但轉換失敗。")
                return None

        self.logger.debug(f"在 '{target_str}' 中未找到符合 'N折' 格式的內容。")
        return None
    
    def _apply_validation_data(self, df: pd.DataFrame, locker_non_discount: Dict, 
                               locker_discount: Dict, discount_rate: float, kiosk_data: Dict) -> pd.DataFrame:
        """
        應用驗收數據到 PO DataFrame
        Args:
            df: PO DataFrame
            locker_non_discount: 智取櫃非折扣驗收數據 {PO#: {A:value, B:value, ...}}
            locker_discount: 智取櫃折扣驗收數據 {PO#: {A:value, B:value, ...}}
            discount_rate: 折扣率
            kiosk_data: 繳費機驗收數據 {PO#: value}
            
        Returns:
            pd.DataFrame: 更新後的PO DataFrame
        """
        
        # 初始化欄位
        df['本期驗收數量/金額'] = 0
        
        # 獲取供應商配置
        locker_suppliers = config_manager.get('SPX', 'locker_suppliers', '')
        kiosk_suppliers = config_manager.get('SPX', 'kiosk_suppliers', '')
        
        # 轉換為列表
        if isinstance(locker_suppliers, str):
            locker_suppliers = [s.strip() for s in locker_suppliers.split(',')]
        if isinstance(kiosk_suppliers, str):
            kiosk_suppliers = [s.strip() for s in kiosk_suppliers.split(',')]
        
        # 應用智取櫃非折扣驗收
        df = self._apply_locker_validation(df, locker_non_discount, locker_suppliers, is_discount=False)
        
        # 應用智取櫃折扣驗收
        df = self._apply_locker_validation(df, locker_discount, locker_suppliers, discount_rate, is_discount=True)
        
        # 應用繳費機驗收
        df = self._apply_kiosk_validation(df, kiosk_data, kiosk_suppliers)
        
        # 修改相關欄位
        df = self._modify_relevant_columns(df)
        
        return df
    
    def _apply_locker_validation(self, df: pd.DataFrame, locker_data: Dict, 
                                 locker_suppliers: List[str], discount_rate: float = None,
                                 is_discount: bool = False) -> pd.DataFrame:
        """
        應用智取櫃驗收數據
        Args:
            df: PO DataFrame
            locker_data: 智取櫃驗收數據 {PO#: {A:value, B:value, ...}}
            locker_suppliers: 智取櫃供應商列表
            discount_rate: 折扣率
            is_discount: 是否為折扣驗收
            
        Returns:
            pd.DataFrame: 更新後的DataFrame
        """
        if not locker_data:
            return df
        
        # 定義櫃體種類的正則表達式模式
        patterns = {
            # A~K類櫃體，後面非英文字母數字組合，但允許中文字符
            'A': r'locker\s*A(?![A-Za-z0-9])',
            'B': r'locker\s*B(?![A-Za-z0-9])',
            'C': r'locker\s*C(?![A-Za-z0-9])',
            'D': r'locker\s*D(?![A-Za-z0-9])',
            'E': r'locker\s*E(?![A-Za-z0-9])',
            'F': r'locker\s*F(?![A-Za-z0-9])',
            'G': r'locker\s*G(?![A-Za-z0-9])',
            'H': r'locker\s*H(?![A-Za-z0-9])',
            'I': r'locker\s*I(?![A-Za-z0-9])',
            'J': r'locker\s*J(?![A-Za-z0-9])',
            'K': r'locker\s*K(?![A-Za-z0-9])',
            'DA': r'locker\s*控制主[櫃|機]',
            'XA': r'locker\s*XA(?![A-Za-z0-9])',
            'XB': r'locker\s*XB(?![A-Za-z0-9])',
            'XC': r'locker\s*XC(?![A-Za-z0-9])',
            'XD': r'locker\s*XD(?![A-Za-z0-9])',
            'XE': r'locker\s*XE(?![A-Za-z0-9])',
            'XF': r'locker\s*XF(?![A-Za-z0-9])',
            '裝運費': r'locker\s*安裝運費',
            '超出櫃體安裝費': r'locker\s*超出櫃體安裝費',
            '超出櫃體運費': r'locker\s*超出櫃體運費'
        }
        
        # 遍歷DataFrame
        for idx, row in df.iterrows():
            try:
                po_number = row.get('PO#')
                item_desc = str(row.get('Item Description', ''))
                po_supplier = str(row.get('PO Supplier', ''))
                
                # 檢查條件; 不符合的資料該圈將被跳過
                # 檢查PO#是否在字典keys中
                if po_number not in locker_data:
                    continue
                # 檢查Item Description是否包含"門市智取櫃"
                if '門市智取櫃' not in item_desc:
                    continue
                # 對於非折扣驗收，檢查是否不包含"減價"
                if not is_discount and '減價' in item_desc:
                    continue
                # 檢查PO Supplier是否在配置的suppliers中
                if locker_suppliers and po_supplier not in locker_suppliers:
                    continue
                
                # 提取櫃體種類
                cabinet_type = None
                priority_order = ['DA', '裝運費', '超出櫃體安裝費', '超出櫃體運費',
                                  'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
                                  'XA', 'XB', 'XC', 'XD', 'XE', 'XF']
                
                for ctype in priority_order:
                    if ctype in patterns:
                        if re.search(patterns[ctype], item_desc, re.IGNORECASE):
                            cabinet_type = ctype
                            break
                
                if cabinet_type and cabinet_type in locker_data[po_number]:
                    current_value = df.at[idx, '本期驗收數量/金額']
                    if current_value == 0:  # 只有當前值為0時才設置新值
                        validation_value = locker_data[po_number][cabinet_type]
                        df.at[idx, '本期驗收數量/金額'] = validation_value
                        
                        # 如果是折扣驗收，記錄折扣率
                        if is_discount and discount_rate:
                            df.at[idx, '折扣率'] = discount_rate
                
            except Exception as e:
                self.logger.debug(f"Error processing locker validation for row {idx}: {str(e)}")
                continue
        
        return df
    
    def _apply_kiosk_validation(self, df: pd.DataFrame, kiosk_data: Dict, 
                                kiosk_suppliers: List[str]) -> pd.DataFrame:
        """應用繳費機驗收數據"""
        if not kiosk_data:
            return df
        
        for idx, row in df.iterrows():
            try:
                po_number = row.get('PO#')
                item_desc = str(row.get('Item Description', ''))
                po_supplier = str(row.get('PO Supplier', ''))
                
                # 檢查條件
                if po_number not in kiosk_data:
                    continue
                if '門市繳費機' not in item_desc:
                    continue
                if kiosk_suppliers and po_supplier not in kiosk_suppliers:
                    continue
                
                current_value = df.at[idx, '本期驗收數量/金額']
                if current_value == 0:
                    validation_value = kiosk_data[po_number]
                    df.at[idx, '本期驗收數量/金額'] = validation_value
                
            except Exception as e:
                self.logger.debug(f"Error processing kiosk validation for row {idx}: {str(e)}")
                continue
        
        return df
    
    def _modify_relevant_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """修改相關欄位"""
        
        need_to_accrual = df['本期驗收數量/金額'] != 0
        df.loc[need_to_accrual, '是否估計入帳'] = 'Y'
        
        # 設置 Account code
        fa_accounts = config_manager.get_list('SPX', 'fa_accounts')
        if fa_accounts:
            df.loc[need_to_accrual, 'Account code'] = fa_accounts[0]
        
        # 設置 Account Name
        df.loc[need_to_accrual, 'Account Name'] = 'AP,FA Clear Account'
        
        # 設置其他欄位
        df.loc[need_to_accrual, 'Product code'] = df.loc[need_to_accrual, 'Product Code']
        df.loc[need_to_accrual, 'Region_c'] = "TW"
        df.loc[need_to_accrual, 'Dep.'] = '000'
        df.loc[need_to_accrual, 'Currency_c'] = df.loc[need_to_accrual, 'Currency']
        
        # 計算 Accr. Amount
        df['temp_amount'] = (
            df['Unit Price'].astype('Float64') * df['本期驗收數量/金額'].fillna(0).astype('Float64')
        )
        
        # 套用折扣
        if '折扣率' in df.columns:
            has_discount = df['折扣率'].notna()
            df.loc[has_discount, 'temp_amount'] = (
                df.loc[has_discount, 'temp_amount'] * df.loc[has_discount, '折扣率'].astype('Float64')
            )
            df.drop('折扣率', axis=1, inplace=True)
        
        non_shipping = ~df['Item Description'].str.contains('運費|安裝費', na=False)
        df.loc[need_to_accrual & non_shipping, 'Accr. Amount'] = \
            df.loc[need_to_accrual & non_shipping, 'temp_amount']
        df.loc[need_to_accrual & ~non_shipping, 'Accr. Amount'] = \
            df.loc[need_to_accrual & ~non_shipping, '本期驗收數量/金額']
        df.drop('temp_amount', axis=1, inplace=True)
        
        # 設置 Liability
        df.loc[need_to_accrual, 'Liability'] = '200414'
        
        return df
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for validation processing")
            return False
        
        return True
    
# =============================================================================
# 步驟 12: 數據格式化和重組步驟
# 替代原始: reformate()、give_account_by_keyword()、is_installment()
# =============================================================================

class DataReformattingStep(PipelineStep):
    """
    數據格式化和重組步驟
    
    功能:
    1. 格式化數值列
    2. 格式化日期列
    3. 清理 nan 值
    4. 重新排列欄位順序
    5. 添加分類和關鍵字匹配
    
    輸入: DataFrame
    輸出: Formatted DataFrame ready for export
    """
    
    def __init__(self, name: str = "DataReformatting", **kwargs):
        super().__init__(name, description="Reformat and reorganize data", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據格式化"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            self.logger.info("Reformatting data...")
            
            # 格式化數值列
            df = self._format_numeric_columns(df)
            
            # 格式化日期列
            df = self._reformat_dates(df)
            
            # 清理 nan 值
            df = self._clean_nan_values(df)
            
            # 重新排列欄位
            df = self._rearrange_columns(df)
            
            # 添加分類
            df = self._add_classification(df)
            
            # 添加關鍵字匹配
            df = self._add_keyword_matching(df)
            
            # 添加分期標記
            df = self._add_installment_flag(df)
            
            # 將含有暫時性計算欄位的結果存為附件
            if isinstance(df, pd.DataFrame) and not df.empty:
                data_name = 'result_with_temp_cols'
                data_copy = df.copy()
                context.add_auxiliary_data(data_name, data_copy)
                self.logger.info(
                    f"Added auxiliary data: {data_name} {data_copy.shape} shape)"
                )

            # 移除臨時欄位
            df = self._remove_temp_columns(df)
            
            context.update_data(df)
            
            self.logger.info("Data reformatting completed")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Data reformatted successfully",
                duration=duration,
                metadata={
                    'total_columns': len(df.columns),
                    'total_rows': len(df)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data reformatting failed: {str(e)}", exc_info=True)
            context.add_error(f"Data reformatting failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _format_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化數值列"""
        # 整數列
        int_columns = ['Line#', 'GL#']
        for col in int_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
                except Exception as e:
                    self.logger.warning(f"Failed to format {col}: {str(e)}")
        
        # 浮點數列
        float_columns = ['Unit Price', 'Entry Amount', 'Entry Invoiced Amount', 
                         'Entry Billed Amount', 'Entry Prepay Amount', 
                         'PO Entry full invoiced status', 'Accr. Amount']
        for col in float_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                except Exception as e:
                    self.logger.warning(f"Failed to format {col}: {str(e)}")
        
        return df
    
    def _reformat_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化日期列"""
        date_columns = ['Creation Date', 'Expected Received Month', 'Last Update Date']
        
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.warning(f"Failed to format date column {col}: {str(e)}")
        
        return df
    
    def _remove_temp_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """移除臨時計算列"""
        temp_columns = ['檔案日期', 'Expected Received Month_轉換格式', 'YMs of Item Description']
        
        for col in temp_columns:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        
        return df
    
    def _clean_nan_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理 nan 值"""
        columns_to_clean = [
            '是否估計入帳', 'PR Product Code Check', 'PO狀態',
            'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
        ]
        
        for col in columns_to_clean:
            if col in df.columns:
                df[col] = df[col].replace('nan', pd.NA)
                df[col] = df[col].replace('<NA>', pd.NA)
        
        # 特殊處理 Accr. Amount
        if 'Accr. Amount' in df.columns:
            try:
                df['Accr. Amount'] = (
                    df['Accr. Amount'].astype('string').str.replace(',', '')
                    .replace('nan', '0')
                    .replace('<NA>', '0')
                    .astype('Float64')
                )
                df['Accr. Amount'] = df['Accr. Amount'].apply(lambda x: x if x != 0 else None)
            except Exception as e:
                self.logger.warning(f"Failed to clean Accr. Amount: {str(e)}")
        
        return df
    
    def _rearrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列欄位順序"""
        # 重新排列上月備註欄位位置
        if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
            fn_index = df.columns.get_loc('Remarked by FN') + 1
            last_month_col = df.pop('Remarked by 上月 FN')
            df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)
        
        if 'Remarked by 上月 FN' in df.columns and 'Remarked by 上月 FN PR' in df.columns:
            fn_pr_index = df.columns.get_loc('Remarked by 上月 FN') + 1
            last_month_pr_col = df.pop('Remarked by 上月 FN PR')
            df.insert(fn_pr_index, 'Remarked by 上月 FN PR', last_month_pr_col)
        
        # 重新排列 PO 狀態欄位位置
        if 'PO狀態' in df.columns and '是否估計入帳' in df.columns:
            accrual_index = df.columns.get_loc('是否估計入帳')
            po_status_col = df.pop('PO狀態')
            df.insert(accrual_index, 'PO狀態', po_status_col)
        
        # 重新排列 PR 欄位位置
        if 'Noted by Procurement' in df.columns:
            noted_index = df.columns.get_loc('Noted by Procurement') + 1
            
            for col_name in ['Remarked by Procurement PR', 'Noted by Procurement PR']:
                if col_name in df.columns:
                    col = df.pop(col_name)
                    df.insert(noted_index, col_name, col)
                    noted_index += 1
        
        # 把本期驗收數量/金額移到 memo 前面
        if '本期驗收數量/金額' in df.columns and 'memo' in df.columns:
            memo_index = df.columns.get_loc('memo')
            validation_col = df.pop('本期驗收數量/金額')
            df.insert(memo_index, '本期驗收數量/金額', validation_col)
        
        return df
    
    def _add_classification(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加分類"""
        try:
            df['CATEGORY'] = df['Item Description'].apply(classify_description)
        except Exception as e:
            self.logger.warning(f"Failed to add classification: {str(e)}")
            df['CATEGORY'] = pd.NA
        
        return df
    
    def _add_keyword_matching(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加關鍵字匹配"""
        try:
            df = give_account_by_keyword(df, 'Item Description', export_keyword=True)
        except Exception as e:
            self.logger.warning(f"Failed to add keyword matching: {str(e)}")
        
        return df
    
    def _add_installment_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加分期標記"""
        if 'Item Description' not in df.columns:
            return df
        
        mask1 = df['Item Description'].str.contains('裝修', na=False)
        mask2 = df['Item Description'].str.contains('第[一|二|三]期款項', na=False)
        
        conditions = [
            (mask1 & mask2),  # Condition for 'Installment'
            (mask1)           # Condition for 'General'
        ]
        choices = ['分期', '一般']
        
        df['裝修一般/分期'] = np.select(conditions, choices, default=pd.NA)
        
        return df
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for reformatting")
            return False
        
        return True

# =============================================================================
# 步驟 13: 導出步驟
# 替代原始: _save_output()
# =============================================================================

class SPXExportStep(PipelineStep):
    """
    SPX 導出步驟
    
    功能: 將處理完成的數據導出到 Excel
    
    輸入: Processed DataFrame
    輸出: Excel file path
    """
    
    def __init__(self, 
                 name: str = "SPXExport",
                 output_dir: str = "output",
                 **kwargs):
        super().__init__(name, description="Export SPX processed data", **kwargs)
        self.output_dir = output_dir
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行導出"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            # 清理 <NA> 值
            df_export = df.replace('<NA>', pd.NA)
            
            # 生成文件名
            processing_date = context.metadata.processing_date
            entity_type = context.metadata.entity_type
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            
            filename = f"{entity_type}_PO_{processing_date}_processed_{timestamp}.xlsx"
            
            # 創建輸出目錄
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
            
            output_path = os.path.join(self.output_dir, filename)
            
            # 確保文件名唯一
            counter = 1
            while os.path.exists(output_path):
                filename = f"{entity_type}_PO_{processing_date}_processed_{timestamp}_{counter}.xlsx"
                output_path = os.path.join(self.output_dir, filename)
                counter += 1
            
            # 導出 Excel
            df_export.to_excel(output_path, index=False)
            
            self.logger.info(f"Data exported to: {output_path}")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Exported to {output_path}",
                duration=duration,
                metadata={
                    'output_path': output_path,
                    'rows_exported': len(df_export),
                    'columns_exported': len(df_export.columns)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Export failed: {str(e)}", exc_info=True)
            context.add_error(f"Export failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data to export")
            return False
        
        return True

# =============================================================================
# 載入步驟使用範例
# =============================================================================

async def example_usage_for_loading():
    """範例：如何使用改進的 SPXDataLoadingStep (新格式 - 支援參數)"""
    import warnings
    warnings.filterwarnings('ignore')
    
    # 新格式：每個文件可以指定獨立參數
    file_paths = {
        'raw_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_purchase_order.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PO_FN.xlsx",
            'params': {'sheet_name': 0, 'header': 0, 'dtype': str, }
        },
        'procurement_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PO_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ap_invoice': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_AP_Invoice_Match_Monitoring_Ext.xlsx",
            'params': {}
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PR_FN.xlsx",
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ops_validation': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\SPX智取櫃及繳費機驗收明細(For FN)_2509.xlsx",
            'params': {
                'sheet_name': '智取櫃驗收明細',
                'header': 1,  # 第二行作為表頭
                'usecols': 'A:AE',
                # 'dtype': str, 
            }
        }
    }
    
    # 創建步驟
    step = SPXDataLoadingStep(
        name="Load_SPX_Data",
        file_paths=file_paths,
        required=True,
        timeout=300.0
    )
    
    # 創建上下文
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPX',
        processing_date=202509,
        processing_type='PO'
    )
    
    # 執行步驟
    result = await step.execute(context)
    
    # 檢查結果
    if result.is_success:
        print("✅ 載入成功！")
        print(f"   主數據: {len(context.data)} 行")
        print(f"   輔助數據: {len(context.list_auxiliary_data())} 個")
        print(f"   處理日期: {context.get_variable('processing_date')}")
        print("\n輔助數據列表:")
        for aux_name in context.list_auxiliary_data():
            aux_data = context.get_auxiliary_data(aux_name)
            if aux_data is not None and not aux_data.empty:
                print(f"   - {aux_name}: {len(aux_data)} 行")
            else:
                print(f"   - {aux_name}: 空")
        """
        How to get loaded dataset(DataFrame):
            - raw po csv: result.data

            - Others:
                - context.list_auxiliary_data()
                    - ['previous', 'procurement_po', 'ap_invoice', 'previous_pr', 
                    'procurement_pr', 'ops_validation', 'reference_account', 'reference_liability']
                - context.get_auxiliary_data('${name}')
        """
    else:
        print(f"❌ 載入失敗: {result.message}")


async def example_old_format():
    """範例：使用舊格式 (向後兼容)"""
    import warnings
    warnings.filterwarnings('ignore')
    
    # 舊格式：直接提供路徑字符串 (仍然支援)
    file_paths = {
        'raw_po': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_purchase_order.csv",
        'previous': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PO_FN.xlsx",
        'procurement_po': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PO_PQ.xlsx"
    }
    
    step = SPXDataLoadingStep(
        name="Load_SPX_Data",
        file_paths=file_paths
    )
    
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPX',
        processing_date=202509,
        processing_type='PO'
    )
    
    result = await step.execute(context)
    
    if result.is_success:
        print("✅ 舊格式載入成功！")
# =============================================================================
# 組裝管道與執行
# =============================================================================
def create_spx_po_complete_pipeline(file_paths: Dict[str, str]) -> Pipeline:
    """
    創建完整的 SPX PO 處理 Pipeline
    
    這是將原始 SPXPOProcessor.process() 方法完全重構後的版本
    
    Args:
        file_paths: 文件路徑字典，包含:
            - raw_po: 原始 PO 文件
            - previous: 前期底稿 (PO)
            - procurement_po: 採購底稿 (PO)
            - ap_invoice: AP Invoice
            - previous_pr: 前期底稿 (PR)
            - procurement_pr: 採購底稿 (PR)
            - validation: OPS 驗收文件
            
    Returns:
        Pipeline: 完整配置的 SPX PO Pipeline
    """
    
    # 創建 Pipeline Builder
    builder = PipelineBuilder("SPX_PO_Complete", "SPX")
    
    # 配置 Pipeline
    pipeline = (builder
                .with_description("Complete SPX PO processing pipeline - refactored from process()")
                .with_stop_on_error(False)  # 不要遇錯即停，收集所有錯誤
                #  ========== 階段 1: 數據載入 ==========
                .add_step(
                    SPXDataLoadingStep(
                        name="Load_All_Data",
                        file_paths=file_paths,
                        required=True,
                        retry_count=2,  # 載入失敗重試2次
                        timeout=300.0   # 5分鐘超時
                    )
                )
        
                # ========== 階段 2: 數據準備與整合 ==========
                .add_step(
                    SPXProductFilterStep(
                        name="Filter_SPX_Products",
                        product_pattern='(?i)LG_SPX',
                        required=True
                    )
                )
                .add_step(ColumnAdditionStep(name="Add_Columns", required=True))
                .add_step(APInvoiceIntegrationStep(name="Integrate_AP_Invoice", required=True))
                .add_step(PreviousWorkpaperIntegrationStep(name="Integrate_Previous_WP", required=True))
                .add_step(ProcurementIntegrationStep(name="Integrate_Procurement", required=True))
                
                # ========== 階段3: 業務邏輯 ==========
                .add_step(DateLogicStep(name="Process_Dates", required=True))
                .add_step(ClosingListIntegrationStep(name="Integrate_Closing_List", required=True))
                .add_step(StatusStage1Step(name="Evaluate_Status_Stage1", required=True))
                .add_step(SPXERMLogicStep(name="Apply_ERM_Logic", required=True, retry_count=0))
                # .add_step(ERMLogicStep(name="Apply_ERM_Logic", required=True))
                .add_step(ValidationDataProcessingStep(name="Process_Validation", required=False))

                # ========== 階段4: 後處理 ==========
                .add_step(DataReformattingStep(name="Reformat_Data", required=True))
                .add_step(SPXExportStep(name="Export_Results", output_dir="output", required=True))

                )
    
    return pipeline.build()

async def execute_spx_po_pipeline(
    file_paths: Dict[str, str],
    processing_date: int,
    mode: str = "complete"
) -> Dict[str, any]:
    """
    執行 SPX PO Pipeline
    
    Args:
        file_paths: 文件路徑字典
        processing_date: 處理日期 (YYYYMM)
        mode: 執行模式 ("complete", "quick", "debug")
        
    Returns:
        Dict: 執行結果
    """
    
    # 根據模式創建 Pipeline
    if mode == "complete":
        pipeline = create_spx_po_complete_pipeline(file_paths)
    # elif mode == "quick":
    #     pipeline = create_spx_po_quick_pipeline(file_paths)
    # elif mode == "debug":
    #     pipeline = create_spx_po_debug_pipeline(file_paths)
    else:
        raise ValueError(f"Unknown mode: {mode}")
    
    # 創建處理上下文
    # 注意: data 為空 DataFrame，會由第一步載入
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPX",
        processing_date=processing_date,
        processing_type="PO"
    )
    
    # 執行 Pipeline
    result = await pipeline.execute(context)
    
    # 添加處理後的數據到結果
    if result['success']:
        
        result['output_data'] = context.data
        result['output_path'] = context.get_variable('output_path')
        if (result.get('successful_steps') - result.get('failed_steps') - result.get('skipped_steps') == 
           result.get('total_steps')):
            print("All successfully")
    
    return result


if __name__ == "__main__":
    # 運行範例
    # print("=== 新格式 (支援參數) ===")
    # asyncio.run(example_usage_for_loading())
    
    # print("\n=== 舊格式 (向後兼容) ===")
    # asyncio.run(example_old_format())

    file_paths = {
        'raw_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_purchase_order.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PO_FN.xlsx",
            'params': {'sheet_name': 0, 'header': 0, 'dtype': str, }
        },
        'procurement_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PO_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ap_invoice': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_AP_Invoice_Match_Monitoring_Ext.xlsx",
            'params': {}
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PR_FN.xlsx",
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ops_validation': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\SPX智取櫃及繳費機驗收明細(For FN)_2509.xlsx",
            'params': {
                'sheet_name': '智取櫃驗收明細',
                'header': 1,  # 第二行作為表頭
                'usecols': 'A:AE',
                # 'dtype': str, 
            }
        }
    }
    
    asyncio.run(execute_spx_po_pipeline(file_paths, 202509))
