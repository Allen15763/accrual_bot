import time
import re
import asyncio
from typing import Optional, Dict, List, Tuple, Any, Union
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.datasources import (
    DataSourceFactory, 
    DataSourcePool
)
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder, 
    create_error_metadata
)
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.helpers import get_ref_on_colab


class SPTDataLoadingStep(PipelineStep):
    """
    SPT 數據並發載入步驟 (使用 datasources 模組)
    
    功能:
    1. 並發載入所有必要文件 (使用統一的數據源接口)
    2. 支援每個文件獨立參數配置
    3. 載入參考數據
    4. 將所有數據添加到 ProcessingContext
    5. 自動管理資源釋放
    
    參數格式:
    方式 (新格式 - 支援參數):
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
                 name: str = "SPTDataLoading",
                 file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
                 **kwargs):
        super().__init__(
            name, 
            description="Load all SPT PO files using datasources module", 
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
        date_pattern = r'(\d{6})'
        match = re.search(date_pattern, file_name)
        
        if match:
            date_str = match.group(1)
            date = int(date_str)
            m = int(date_str[-2:])  # 月份
        else:
            # 如果無法從文件名提取，使用當前日期
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
            ref_data_path = config_manager._config_data.get('PATHS').get('ref_path_spt')
            
            count = 0
            
            # if in colab, return dataframe, otherwise, return None
            ref_ac = get_ref_on_colab(ref_data_path)
            
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


class SPTPRDataLoadingStep(PipelineStep):
    """
    SPT PR 數據並發載入步驟
    
    專為 PR (請購單) 管道設計，與 PO 管道的 SPTDataLoadingStep 平行。
    
    功能:
    1. 並發載入所有必要的 PR 相關檔案
    2. 支援每個檔案獨立參數配置
    3. 載入 PR 專用參考數據
    4. 將所有數據添加到 ProcessingContext
    5. 自動管理資源釋放
    
    參數格式:
    方式1 (簡單格式):
        file_paths = {
            'raw_pr': 'path/to/pr_file.xlsx',
            'pr_approval': 'path/to/approval.xlsx'
        }
    
    方式2 (完整格式 - 支援參數):
        file_paths = {
            'raw_pr': {
                'path': 'path/to/pr_file.xlsx',
                'params': {'sheet_name': 0, 'header': 1}
            },
            'pr_approval': {
                'path': 'path/to/approval.xlsx',
                'params': {'usecols': 'A:M'}
            }
        }
    
    使用範例:
        # 創建步驟
        step = SPTPRDataLoadingStep(
            name="SPTPRLoading",
            file_paths={
                'raw_pr': {
                    'path': 'pr_202410.xlsx',
                    'params': {'sheet_name': 0}
                },
                'pr_approval': 'approval_202410.xlsx',
                'budget_ref': 'budget_mapping.xlsx'
            }
        )
        
        # 在 pipeline 中使用
        pipeline.add_step(step)
    """
    
    def __init__(self, 
                 name: str = "SPTPRDataLoading",
                 file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
                 **kwargs):
        """
        初始化 SPT PR 數據載入步驟
        
        Args:
            name: 步驟名稱
            file_paths: 檔案路徑配置字典
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(
            name, 
            description="Load all SPT PR files using datasources module", 
            **kwargs
        )
        # 標準化為統一格式（支援舊格式自動轉換）
        self.file_configs = self._normalize_file_paths(file_paths or {})
        self.pool = DataSourcePool()  # 數據源連接池
    
    def _normalize_file_paths(
        self, 
        file_paths: Dict[str, Union[str, Dict[str, Any]]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        標準化檔案路徑格式，支援向後兼容
        
        將簡單格式（純字串路徑）和完整格式（包含參數）統一為標準格式。
        
        Args:
            file_paths: 原始檔案路徑字典
            
        Returns:
            Dict[str, Dict[str, Any]]: 統一的配置格式
            
        Raises:
            ValueError: 當配置格式無效時
        """
        normalized = {}
        
        for file_type, config in file_paths.items():
            if isinstance(config, str):
                # 簡單格式：直接是路徑字串
                normalized[file_type] = {
                    'path': config,
                    'params': {}
                }
            elif isinstance(config, dict):
                # 完整格式：已經是配置字典
                if 'path' not in config:
                    raise ValueError(
                        f"Missing 'path' in config for {file_type}"
                    )
                normalized[file_type] = {
                    'path': config['path'],
                    'params': config.get('params', {})
                }
            else:
                raise ValueError(
                    f"Invalid config type for {file_type}: {type(config)}"
                )
        
        return normalized
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行並發數據載入
        
        執行流程:
        1. 驗證檔案存在性
        2. 並發讀取所有檔案
        3. 提取和驗證主數據 (raw_pr)
        4. 更新 Context
        5. 載入參考數據
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 步驟執行結果
        """
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            self.logger.info("Starting concurrent PR file loading with datasources...")
            
            # 階段 1: 驗證檔案存在性
            validated_configs = self._validate_file_configs()
            
            if not validated_configs:
                raise ValueError("No valid files to load")
            
            # 階段 2: 並發載入所有主要檔案
            loaded_data = await self._load_all_files_concurrent(validated_configs)
            
            # 階段 3: 提取和驗證主數據 (raw_pr)
            if 'raw_pr' not in loaded_data:
                raise ValueError("Failed to load raw PR data")
            
            df, date, m = self._extract_raw_pr_data(loaded_data['raw_pr'])
            
            # 更新 Context 主數據
            context.update_data(df)
            context.set_variable('processing_date', date)
            context.set_variable('processing_month', m)
            context.set_variable('file_paths', validated_configs)
            
            # 階段 4: 添加輔助數據到 Context
            auxiliary_count = 0
            for data_name, data_content in loaded_data.items():
                if data_name != 'raw_pr' and data_content is not None:
                    if isinstance(data_content, pd.DataFrame) and not data_content.empty:
                        context.add_auxiliary_data(data_name, data_content)
                        auxiliary_count += 1
                        self.logger.info(
                            f"Added auxiliary data: {data_name} "
                            f"(shape: {data_content.shape})"
                        )
            
            # 階段 5: 載入參考數據
            ref_count = await self._load_reference_data(context)
            
            # 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            self.logger.info(
                f"Successfully loaded PR data: {df.shape}, "
                f"{auxiliary_count} auxiliary datasets, "
                f"{ref_count} reference datasets in {duration:.2f}s"
            )
            
            # 使用 StepMetadataBuilder 構建標準化 metadata
            metadata = (
                StepMetadataBuilder()
                .set_row_counts(0, len(df))
                .set_process_counts(processed=len(df))
                .set_time_info(start_datetime, end_datetime)
                .add_custom('pr_records', len(df))
                .add_custom('pr_columns', len(df.columns))
                .add_custom('processing_date', int(date))
                .add_custom('processing_month', int(m))
                .add_custom('auxiliary_datasets', auxiliary_count)
                .add_custom('reference_datasets', ref_count)
                .add_custom('loaded_files', list(loaded_data.keys()))
                .add_custom('files_loaded_count', len(loaded_data))
                .build()
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=(
                    f"Loaded {len(df)} PR records with "
                    f"{auxiliary_count} auxiliary datasets"
                ),
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            self.logger.error(
                f"PR data loading failed: {str(e)}", 
                exc_info=True
            )
            context.add_error(f"PR data loading failed: {str(e)}")
            
            # 創建增強的錯誤 metadata
            error_metadata = create_error_metadata(
                e, context, self.name,
                validated_files=list(self.file_configs.keys()) if self.file_configs else [],
                stage='pr_data_loading'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Failed to load PR data: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
        
        finally:
            # 清理資源
            await self._cleanup_resources()
    
    async def _load_all_files_concurrent(
        self, 
        validated_configs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        並發載入所有檔案
        
        使用 asyncio.gather 並發執行所有檔案讀取任務，
        提高載入效率。
        
        Args:
            validated_configs: 已驗證的檔案配置字典
            
        Returns:
            Dict[str, Any]: 載入的數據字典
            
        Raises:
            Exception: 當必要檔案載入失敗時
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
                # 可選檔案失敗不影響整體流程
                if file_type == 'raw_pr':  # raw_pr 是必需的
                    raise result
                else:
                    loaded_data[file_type] = None
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
        載入單個檔案（支援參數配置）
        
        根據檔案類型採用不同的載入策略：
        - raw_pr: 需要提取日期和月份
        - 其他: 直接讀取
        
        Args:
            file_type: 檔案類型 (raw_pr, pr_approval, budget_ref等)
            config: 檔案配置 {'path': '...', 'params': {...}}
            
        Returns:
            Union[pd.DataFrame, Tuple]: 載入的數據
            
        Raises:
            Exception: 檔案讀取失敗時
        """
        try:
            file_path = config['path']
            params = config.get('params', {})
            
            # DataSourceFactory 自動識別檔案類型並使用參數創建數據源
            source = DataSourceFactory.create_from_file(file_path, **params)
            
            # 將數據源添加到連接池以便後續管理
            self.pool.add_source(file_type, source)
            
            # 根據檔案類型決定載入策略
            if file_type == 'raw_pr':
                # 主 PR 數據需要提取日期和月份
                return await self._load_raw_pr_file(source, file_path)
            else:
                # 其他檔案直接讀取
                df = await source.read()
                self.logger.debug(
                    f"成功導入 {file_type}, 數據維度: {df.shape}"
                )
                return df
            
        except Exception as e:
            self.logger.error(
                f"Error loading {file_type} from {config.get('path')}: {str(e)}"
            )
            raise
    
    async def _load_raw_pr_file(
        self, 
        source, 
        file_path: str
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        載入原始 PR 檔案並提取日期信息
        
        處理流程:
        1. 讀取 PR 數據
        2. 基本數據清洗（Line#, GL# 等）
        3. 從檔名提取日期和月份
        
        Args:
            source: 數據源實例
            file_path: 檔案路徑
            
        Returns:
            Tuple[pd.DataFrame, int, int]: (DataFrame, date, month)
            
        Note:
            假設檔名格式為 YYYYMM_xxx.xlsx
        """
        # 讀取數據
        df = await source.read()
        
        # 基本數據處理（與 PO 類似）
        if 'Line#' in df.columns:
            df['Line#'] = (
                df['Line#']
                .astype('Float64')
                .round(0)
                .astype('Int64')
                .astype('string')
            )
        
        if 'GL#' in df.columns:
            df['GL#'] = np.where(df['GL#'] == 'N.A.', '666666', df['GL#'])
            df['GL#'] = (
                df['GL#']
                .fillna('666666')
                .astype('Float64')
                .round(0)
                .astype('Int64')
                .astype('string')
            )
        
        self.logger.debug(f"成功導入 PR 數據, 數據維度: {df.shape}")
        
        # 從檔案名提取日期（假設格式為 YYYYMM_xxx.xlsx）
        file_name = Path(file_path).stem
        
        # 嘗試從檔案名提取日期
        date_pattern = r'(\d{6})'
        match = re.search(date_pattern, file_name)
        
        if match:
            date_str = match.group(1)
            date = int(date_str)
            m = int(date_str[-2:])  # 月份
        else:
            # 如果無法從檔案名提取，使用當前日期
            current = datetime.now()
            date = int(current.strftime('%Y%m'))
            m = current.month
            self.logger.warning(
                f"Could not extract date from filename, using current date: {date}"
            )
        
        return df, date, m
    
    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """
        載入 PR 專用參考數據
        
        載入項目可能包括:
        - 科目映射表
        - 預算對照表
        - 部門代碼表
        - 等等
        
        Args:
            context: 處理上下文
            
        Returns:
            int: 載入的參考數據集數量
        """
        try:
            # 從配置讀取參考數據路徑
            # 注意：這裡使用與 PO 相同的參考數據路徑
            # 如果 PR 有專用的參考數據，需要在 config.ini 中新增對應配置
            ref_data_path = config_manager._config_data.get('PATHS').get('ref_path_spt')
            
            count = 0
            
            # 載入科目映射/負債科目映射
            if Path(ref_data_path).exists():
                source = DataSourceFactory.create_from_file(str(ref_data_path))
                ref_ac = await source.read(dtype=str)
                
                # 儲存參考數據到 Context
                context.add_auxiliary_data(
                    'reference_account', 
                    ref_ac.iloc[:, 1:3].copy()
                )
                context.add_auxiliary_data(
                    'reference_liability', 
                    ref_ac.loc[:, ['Account', 'Liability']].copy()
                )
                
                await source.close()
                count += 2
                self.logger.info(f"Loaded account mapping: {len(ref_ac)} records")
            else:
                # 如果找不到，創建空的參考數據
                self.logger.warning(
                    f"Account mapping file not found: {ref_data_path}"
                )
                context.add_auxiliary_data('reference_account', pd.DataFrame())
                context.add_auxiliary_data('reference_liability', pd.DataFrame())
            
            # TODO: 載入其他 PR 專用參考數據
            # 例如：預算對照表、部門代碼表等
            
            return count
            
        except Exception as e:
            self.logger.error(f"Failed to load reference data: {str(e)}")
            # 創建空的參考數據以避免後續步驟失敗
            context.add_auxiliary_data('reference_account', pd.DataFrame())
            context.add_auxiliary_data('reference_liability', pd.DataFrame())
            return 0
    
    def _validate_file_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        驗證檔案配置
        
        檢查項目:
        1. 檔案路徑是否提供
        2. 檔案是否存在
        3. raw_pr 為必要檔案
        
        Returns:
            Dict[str, Dict[str, Any]]: 驗證通過的檔案配置字典
            
        Raises:
            FileNotFoundError: 當必要檔案不存在時
        """
        validated = {}
        
        for file_type, config in self.file_configs.items():
            file_path = config.get('path')
            
            if not file_path:
                self.logger.warning(f"No path provided for {file_type}")
                continue
            
            path = Path(file_path)
            
            if not path.exists():
                self.logger.warning(
                    f"File not found: {file_type} at {file_path}"
                )
                # raw_pr 是必需的，其他是可選的
                if file_type == 'raw_pr':
                    raise FileNotFoundError(
                        f"Required file not found: {file_path}"
                    )
                continue
            
            validated[file_type] = config
            self.logger.debug(f"Validated file: {file_type}")
        
        return validated
    
    def _extract_raw_pr_data(
        self, 
        raw_pr_result: Tuple[pd.DataFrame, int, int]
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        提取和驗證原始 PR 數據
        
        驗證項目:
        1. 數據格式正確（Tuple 包含 3 個元素）
        2. DataFrame 非空
        3. 必要欄位存在
        
        Args:
            raw_pr_result: 原始 PR 數據結果
            
        Returns:
            Tuple[pd.DataFrame, int, int]: 驗證後的數據
            
        Raises:
            ValueError: 當數據格式或內容無效時
        """
        if isinstance(raw_pr_result, tuple) and len(raw_pr_result) == 3:
            df, date, m = raw_pr_result
            
            # 驗證 DataFrame
            if df is None or df.empty:
                raise ValueError("Raw PR data is empty")
            
            # 驗證必要列
            # TODO: 根據實際 PR 檔案格式調整必要欄位
            required_columns = ['Product Code', 'Item Description', 'GL#']
            missing_columns = [
                col for col in required_columns 
                if col not in df.columns
            ]
            
            if missing_columns:
                raise ValueError(
                    f"Missing required columns: {missing_columns}"
                )
            
            self.logger.info(
                f"Raw PR data validated: {df.shape}, "
                f"date={date}, month={m}"
            )
            
            return df, date, m
        else:
            raise ValueError("Invalid raw PR data format")
    
    async def _cleanup_resources(self):
        """
        清理數據源資源
        
        關閉所有打開的數據源連接，釋放資源。
        即使發生錯誤也會嘗試清理。
        """
        try:
            await self.pool.close_all()
            self.logger.debug("All data sources closed")
        except Exception as e:
            self.logger.error(
                f"Error during resource cleanup: {str(e)}"
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入配置
        
        在執行前檢查：
        1. 是否提供了檔案配置
        2. 是否包含必要的 raw_pr 檔案
        3. raw_pr 檔案是否存在
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 驗證是否通過
        """
        if not self.file_configs:
            self.logger.error("No file configs provided")
            context.add_error("No file configs provided")
            return False
        
        # 檢查必要檔案
        if 'raw_pr' not in self.file_configs:
            self.logger.error("Missing raw PR file config")
            context.add_error("Missing raw PR file config")
            return False
        
        # 檢查檔案是否存在
        raw_pr_path = self.file_configs['raw_pr'].get('path')
        if not raw_pr_path or not Path(raw_pr_path).exists():
            self.logger.error(f"Raw PR file not found: {raw_pr_path}")
            context.add_error(f"Raw PR file not found: {raw_pr_path}")
            return False
        
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """
        回滾操作 - 清理已載入的資源
        
        當步驟執行失敗時，清理已分配的資源。
        
        Args:
            context: 處理上下文
            error: 觸發回滾的錯誤
        """
        self.logger.warning(
            f"Rolling back PR data loading due to error: {str(error)}"
        )
        await self._cleanup_resources()