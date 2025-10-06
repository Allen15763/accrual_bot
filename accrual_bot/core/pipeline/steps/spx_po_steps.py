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
import pandas as pd
import numpy as np
import asyncio
from typing import Optional, Dict, List, Tuple, Any, Union
from pathlib import Path

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
            
            # 階段 3: 添加輔助數據到 Context
            auxiliary_count = 0
            for data_name, data_content in loaded_data.items():
                if data_name != 'raw_po' and data_content is not None:
                    if isinstance(data_content, pd.DataFrame) and not data_content.empty:
                        context.add_auxiliary_data(data_name, data_content)
                        auxiliary_count += 1
                        self.logger.info(
                            f"Added auxiliary data: {data_name} ({len(data_content)} rows)"
                        )
            
            # 階段 4: 載入參考數據
            ref_count = await self._load_reference_data(context)
            
            self.logger.info(
                f"Successfully loaded {len(df)} rows of PO data, "
                f"{auxiliary_count} auxiliary datasets, "
                f"{ref_count} reference datasets"
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Loaded {len(df)} PO records with {auxiliary_count} auxiliary datasets",
                metadata={
                    'po_records': len(df),
                    'processing_date': date,
                    'processing_month': m,
                    'auxiliary_datasets': auxiliary_count,
                    'reference_datasets': ref_count,
                    'loaded_files': list(loaded_data.keys())
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data loading failed: {str(e)}", exc_info=True)
            context.add_error(f"Data loading failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Failed to load data: {str(e)}"
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
            df['Line#'] = df['Line#'].astype(float).round(0).astype(int).astype(str)
        
        if 'GL#' in df.columns:
            df['GL#'] = np.where(df['GL#'] == 'N.A.', '666666', df['GL#'])
            df['GL#'] = df['GL#'].fillna('666666').astype(float).round(0).astype(int).astype(str)

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
                f"Raw PO data validated: {len(df)} rows, "
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
            
            self.logger.info(
                f"Product filtering complete: {original_count} -> {filtered_count} "
                f"(removed {removed_count} non-SPX items)"
            )
            
            if filtered_count == 0:
                context.add_warning("No SPX products found after filtering")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=filtered_df,
                message=f"Filtered to {filtered_count} SPX items",
                metadata={
                    'original_count': original_count,
                    'filtered_count': filtered_count,
                    'removed_count': removed_count,
                    'filter_pattern': self.product_pattern
                }
            )
            
        except Exception as e:
            self.logger.error(f"Product filtering failed: {str(e)}", exc_info=True)
            context.add_error(f"Product filtering failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
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
        try:
            df = context.data.copy()
            m = context.get_variable('processing_month')
            
            original_columns = set(df.columns)
            
            # 添加基礎欄位 (調用原 processor 的邏輯)
            # 這裡可以直接調用原有的方法，或在此處重新實作
            df = self._add_basic_columns(df, m)
            
            # 添加 SPX 特定欄位
            df['memo'] = np.nan
            df['GL DATE'] = np.nan
            df['Remarked by Procurement PR'] = np.nan
            df['Noted by Procurement PR'] = np.nan
            df['Remarked by 上月 FN PR'] = np.nan
            
            # 更新月份變數
            context.set_variable('processing_month', m)
            
            context.update_data(df)
            
            new_columns = set(df.columns) - original_columns
            
            self.logger.info(f"Added {len(new_columns)} columns: {new_columns}")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Added {len(new_columns)} columns",
                metadata={
                    'new_columns': list(new_columns),
                    'total_columns': len(df.columns),
                    'updated_month': m
                }
            )
            
        except Exception as e:
            self.logger.error(f"Column addition failed: {str(e)}", exc_info=True)
            context.add_error(f"Column addition failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
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
                df_copy['PR Line'] = df_copy['PR#'].astype(str) + '-' + df_copy['Line#'].astype(str)
            
            if 'PO#' in df_copy.columns and 'Line#' in df_copy.columns:
                df_copy['PO Line'] = df_copy['PO#'].astype(str) + '-' + df_copy['Line#'].astype(str)
            
            # 添加標記和備註欄位
            self._add_remark_columns(df_copy, month)
            
            # 添加計算欄位
            self._add_calculation_columns(df_copy)
            
            # 計算上月
            previous_month = 12 if month == 1 else month - 1
            
            self.logger.info("成功添加基本必要列")
            return df_copy, previous_month
            
        except Exception as e:
            self.logger.error(f"添加基本列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加基本列時出錯")
    
    def _add_remark_columns(self, df: pd.DataFrame, month: int) -> None:
        """添加備註相關欄位"""
        columns_to_add = [
            'Remarked by Procurement',
            'Noted by Procurement', 
            'Remarked by FN',
            'Noted by FN',
            f'Remarked by {self.calculate_month(month)}月 Procurement',
            'Remarked by 上月 FN',
            'PO狀態'
        ]
        
        for col in columns_to_add:
            if col not in df.columns:
                df[col] = np.nan
    
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
                df[col] = np.nan
        
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
        if 'GL#' in df.columns:
            return np.where(df['GL#'].astype(str).isin([str(x) for x in self.fa_accounts]), 'Y', '')
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
        
        if self.entity_type == 'MOB':
            return np.where(df['GL#'].astype(str).str.startswith('65'), "Y", "N")
        else:  # SPT or SPX
            return np.where(
                (df['GL#'].astype(str) == '650003') | (df['GL#'].astype(str) == '450014'), 
                "Y", "N"
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for column addition")
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
        
                # ========== 階段 2: 數據準備 ==========
                .add_step(
                    SPXProductFilterStep(
                        name="Filter_SPX_Products",
                        product_pattern='(?i)LG_SPX',
                        required=True
                    )
                )
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
