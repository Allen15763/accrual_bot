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
from accrual_bot.data.importers.google_sheets_importer import GoogleSheetsImporter


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
            # 這裡假設參考數據存放在固定位置
            # 實際使用時應該從配置讀取
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


class PPEDataLoadingStep(PipelineStep):
    """
    PPE 數據載入步驟
    
    功能：
    1. 讀取合約歸檔清單（本地 Excel）
    2. 從 Google Sheets 讀取續約清單
    3. 將數據添加到 ProcessingContext
    """
    
    def __init__(self,
                 name: str = "PPEDataLoading",
                 contract_filing_list_url: Optional[str] = None,
                 **kwargs):
        super().__init__(name, description="Load PPE contract data", **kwargs)
        self.contract_filing_list_url = contract_filing_list_url
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據載入"""
        start_time = datetime.now()
        
        try:
            # 從 context 或參數獲取檔案路徑
            file_url = (self.contract_filing_list_url or 
                        context.get_variable('contract_filing_list_url'))
            
            if not file_url:
                raise ValueError("未提供合約歸檔清單檔案路徑")
            
            self.logger.info(f"開始載入 PPE 數據: {file_url}")
            
            # 1. 讀取合約歸檔清單
            df_filing = await self._load_filing_list(file_url, context)
            
            # 2. 讀取續約清單（從 Google Sheets）
            df_renewal = await self._load_renewal_list(context)
            
            # 3. 儲存到 context
            context.add_auxiliary_data('filing_list', df_filing)
            context.add_auxiliary_data('renewal_list', df_renewal)
            
            # 設置為主數據（用於後續步驟）
            context.update_data(df_filing)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(0, len(df_filing))
                        .set_time_info(start_time, datetime.now())
                        .add_custom('filing_records', len(df_filing))
                        .add_custom('renewal_records', len(df_renewal))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_filing,
                message=f"成功載入 {len(df_filing)} 筆歸檔記錄和 {len(df_renewal)} 筆續約記錄",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"數據載入失敗: {str(e)}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"載入失敗: {str(e)}"
            )
    
    async def _load_filing_list(self, file_url: str, 
                                context: ProcessingContext) -> pd.DataFrame:
        """讀取合約歸檔清單"""
        
        # 使用 datasources 模組讀取
        source = DataSourceFactory.create_from_file(file_url)
        
        # 從配置讀取參數
        sheet_name = config_manager.get('SPX', 'contract_filing_list_sheet', 0)
        use_cols = config_manager.get('SPX', 'contract_filing_list_range', None)
        
        df = await source.read(sheet_name=sheet_name, usecols=use_cols)
        await source.close()
        
        self.logger.info(f"成功讀取歸檔清單: {len(df)} 筆")
        return df
    
    async def _load_renewal_list(self, 
                                 context: ProcessingContext) -> pd.DataFrame:
        """從 Google Sheets 讀取續約清單"""
        
        # 初始化 Google Sheets Importer
        credentials_config = config_manager.get_credentials_config()
        sheets_importer = GoogleSheetsImporter(credentials_config)
        
        # 讀取配置
        spreadsheet_id = config_manager.get('SPX', 'expanded_contract_wb')
        
        # 讀取三個工作表
        df_thi = sheets_importer.get_sheet_data(
            spreadsheet_id,
            config_manager.get('SPX', 'expanded_contract_sheet3'),
            config_manager.get('SPX', 'expanded_contract_range')
        )
        df_thi = df_thi.iloc[:, [0, 2, *range(13, 17)]]
        
        df_sec = sheets_importer.get_sheet_data(
            spreadsheet_id,
            config_manager.get('SPX', 'expanded_contract_sheet2'),
            config_manager.get('SPX', 'expanded_contract_range')
        )
        df_sec = df_sec.iloc[:, [0, 2, *range(13, 17)]]
        
        # 統一欄位名稱
        std_cols = df_sec.columns
        df_thi.columns = std_cols
        
        df_ft = sheets_importer.get_sheet_data(
            spreadsheet_id,
            config_manager.get('SPX', 'expanded_contract_sheet1'),
            config_manager.get('SPX', 'expanded_contract_range1')
        )
        df_ft = df_ft.iloc[:2576, [0, 6, 21, 24]].query(
            "起租日!='' and 起租日!='#N/A' and ~起租日.isna()"
        ).assign(
            tempA=df_ft.iloc[:, 21],
            tempB=df_ft.iloc[:, 24]
        )
        df_ft.columns = std_cols
        
        # 合併所有數據
        df_renewal = pd.concat([df_ft, df_sec, df_thi], ignore_index=True)
        
        self.logger.info(f"成功讀取續約清單: {len(df_renewal)} 筆")
        return df_renewal
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        file_url = (self.contract_filing_list_url or 
                    context.get_variable('contract_filing_list_url'))
        
        if not file_url:
            self.logger.error("未提供合約歸檔清單檔案路徑")
            return False
        
        return True


class AccountingOPSDataLoadingStep(PipelineStep):
    """
    會計與 OPS 底稿並發載入步驟
    用於讀取並驗證會計前期底稿與 OPS 驗收檔案底稿
    
    功能:
    1. 並發讀取會計前期底稿和 OPS 驗收檔案底稿
    2. 驗證必要欄位存在性
    3. 標準化資料格式（PO Line 統一為 string）
    4. 將資料儲存到 ProcessingContext
    5. 自動管理資源釋放
    
    使用範例:
        step = AccountingOPSDataLoadingStep(
            name="LoadAccountingOPS",
            file_paths={
                'accounting_workpaper': {
                    'path': 'accounting_202410.xlsx',
                    'params': {'sheet_name': 0, 'usecols': 'A:AE', 'header': 1}
                },
                'ops_validation': {
                    'path': 'ops_validation_202410.xlsx',
                    'params': {'sheet_name': 'Sheet1', 'header': 0}
                }
            },
            required_columns={
                'accounting': ['PO Line', 'memo'],
                'ops': ['PO Number', 'Line', 'Validation Status']
            }
        )
    """
    
    def __init__(
        self,
        name: str = "AccountingOPSDataLoading",
        file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
        required_columns: Optional[Dict[str, List[str]]] = None,
        **kwargs
    ):
        """
        初始化步驟
        
        Args:
            name: 步驟名稱
            file_paths: 檔案路徑配置，格式:
                {
                    'accounting_workpaper': {
                        'path': '檔案路徑',
                        'params': {'sheet_name': 0, 'usecols': 'A:Z', ...}
                    },
                    'ops_validation': {
                        'path': '檔案路徑',
                        'params': {...}
                    }
                }
            required_columns: 必要欄位配置，格式:
                {
                    'accounting': ['PO Line', 'memo'],
                    'ops': ['PO Number', 'Line', 'Status']
                }
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(
            name,
            description="Load accounting workpaper and OPS validation files",
            **kwargs
        )
        
        # 標準化檔案配置格式
        self.file_configs = self._normalize_file_paths(file_paths or {})
        
        # 必要欄位配置
        self.required_columns = required_columns or {
            'accounting': ['PO Line', 'memo'],
            'ops': config_manager.get_list('SPX', 'locker_columns')[5:23]  # 第2行的櫃型
        }
        
        # 數據源連接池
        self.pool = DataSourcePool()
    
    def _normalize_file_paths(
        self, 
        file_paths: Dict[str, Union[str, Dict[str, Any]]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        標準化檔案路徑格式
        
        支援兩種格式:
        1. 簡單格式: {'accounting_workpaper': 'path/to/file.xlsx'}
        2. 完整格式: {'accounting_workpaper': {'path': '...', 'params': {...}}}
        
        Args:
            file_paths: 原始檔案路徑字典
            
        Returns:
            標準化後的配置字典
        """
        normalized = {}
        
        for file_type, config in file_paths.items():
            if isinstance(config, str):
                # 簡單格式：直接路徑字串
                normalized[file_type] = {
                    'path': config,
                    'params': {}
                }
            elif isinstance(config, dict):
                # 完整格式：已包含參數
                if 'path' not in config:
                    raise ValueError(f"Missing 'path' in config for {file_type}")
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
        """執行數據載入"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            self.logger.info("Starting concurrent loading of accounting and OPS files...")
            
            # 階段 1: 驗證檔案存在性
            validated_configs = self._validate_file_configs()
            
            # 階段 2: 並發讀取兩個檔案
            loaded_data = await self._load_files_concurrent(validated_configs)
            
            # 階段 3: 驗證必要欄位
            self._validate_required_columns(loaded_data)
            
            # 階段 4: 標準化資料格式
            processed_data = self._standardize_data(loaded_data, context.metadata.processing_date)
            
            # 階段 5: 儲存到 Context
            self._store_to_context(context, processed_data)
            
            # 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            # 構建標準化 metadata
            accounting_shape = processed_data['accounting_workpaper'].shape
            ops_shape = processed_data['ops_validation'].shape
            
            metadata = (
                StepMetadataBuilder()
                .set_row_counts(0, accounting_shape[0] + ops_shape[0])
                .set_process_counts(processed=accounting_shape[0] + ops_shape[0])
                .set_time_info(start_datetime, end_datetime)
                .add_custom('accounting_records', accounting_shape[0])
                .add_custom('accounting_columns', accounting_shape[1])
                .add_custom('ops_records', ops_shape[0])
                .add_custom('ops_columns', ops_shape[1])
                .add_custom('loaded_files', list(loaded_data.keys()))
                .build()
            )
            
            self.logger.info(
                f"Successfully loaded accounting ({accounting_shape}) "
                f"and OPS ({ops_shape}) data in {duration:.2f}s"
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=processed_data['accounting_workpaper'],  # 主數據
                message=(
                    f"Loaded {accounting_shape[0]} accounting records "
                    f"and {ops_shape[0]} OPS validation records"
                ),
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            self.logger.error(f"Data loading failed: {str(e)}", exc_info=True)
            context.add_error(f"Accounting/OPS loading failed: {str(e)}")
            
            # 創建錯誤 metadata
            error_metadata = create_error_metadata(
                e, context, self.name,
                validated_files=list(self.file_configs.keys()),
                stage='accounting_ops_loading'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Failed to load data: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
        
        finally:
            # 清理資源
            await self._cleanup_resources()
    
    async def _load_files_concurrent(
        self,
        validated_configs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, pd.DataFrame]:
        """
        並發讀取所有檔案
        
        Args:
            validated_configs: 已驗證的檔案配置
            
        Returns:
            載入的資料字典
        """
        # 創建載入任務
        tasks = []
        file_names = []
        
        for file_type, config in validated_configs.items():
            task = self._load_single_file(file_type, config)
            tasks.append(task)
            file_names.append(file_type)
        
        # 並發執行
        self.logger.info(f"Loading {len(tasks)} files concurrently...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 整理結果
        loaded_data = {}
        for file_type, result in zip(file_names, results):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to load {file_type}: {str(result)}")
                raise result
            else:
                loaded_data[file_type] = result
                self.logger.info(
                    f"Successfully loaded {file_type}: {result.shape}"
                )
        
        return loaded_data
    
    async def _load_single_file(
        self,
        file_type: str,
        config: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        載入單個檔案
        
        Args:
            file_type: 檔案類型 (accounting_workpaper 或 ops_validation)
            config: 檔案配置
            
        Returns:
            pd.DataFrame: 載入的資料
        """
        try:
            file_path = config['path']
            params = config.get('params', {})
            
            # 創建數據源
            source = DataSourceFactory.create_from_file(file_path, **params)
            
            # 添加到連接池
            self.pool.add_source(file_type, source)
            
            # 讀取資料
            df = await source.read(**params)
            
            self.logger.debug(
                f"Successfully loaded {file_type} from {file_path}: {df.shape}"
            )
            
            return df
            
        except Exception as e:
            self.logger.error(
                f"Error loading {file_type} from {config.get('path')}: {str(e)}"
            )
            raise
    
    def _validate_required_columns(self, loaded_data: Dict[str, pd.DataFrame]):
        """
        驗證必要欄位存在性
        
        Args:
            loaded_data: 載入的資料字典
            
        Raises:
            ValueError: 如果缺少必要欄位
        """
        # 驗證會計底稿欄位
        if 'accounting_workpaper' in loaded_data:
            df_acc = loaded_data['accounting_workpaper']
            missing_cols = [
                col for col in self.required_columns.get('accounting', [])
                if col not in df_acc.columns
            ]
            if missing_cols:
                raise ValueError(
                    f"Accounting workpaper missing required columns: {missing_cols}"
                )
        
        # 驗證 OPS 底稿欄位
        if 'ops_validation' in loaded_data:
            df_ops = loaded_data['ops_validation']
            missing_cols = [
                col for col in self.required_columns.get('ops', [])
                if col not in df_ops.columns
            ]
            if missing_cols:
                raise ValueError(
                    f"OPS validation file missing required columns: {missing_cols}"
                )
        
        self.logger.info("All required columns validated successfully")
    
    def _standardize_data(
        self,
        loaded_data: Dict[str, pd.DataFrame],
        target_date: int
    ) -> Dict[str, pd.DataFrame]:
        """
        標準化資料格式
        
        主要處理:
        1. PO Line 統一轉換為 string 類型
        2. 清理空白字符
        3. 處理 NaN 值
        
        Args:
            loaded_data: 原始載入的資料
            
        Returns:
            標準化後的資料字典
        """
        standardized = {}
        
        # 標準化會計底稿
        if 'accounting_workpaper' in loaded_data:
            df_acc = loaded_data['accounting_workpaper'].copy()
            
            # PO Line 標準化
            if 'PO Line' in df_acc.columns:
                df_acc['PO Line'] = (
                    df_acc['PO Line']
                    .astype(str)
                    .str.strip()
                    .str.replace(r'\.0$', '', regex=True)  # 移除 .0
                )
            
            # memo 欄位清理
            if 'memo' in df_acc.columns:
                df_acc['memo'] = (
                    df_acc['memo']
                    .fillna('')
                    .astype(str)
                    .str.strip()
                )
            
            standardized['accounting_workpaper'] = df_acc
            self.logger.debug("Standardized accounting workpaper")
        
        # 標準化 OPS 底稿
        if 'ops_validation' in loaded_data:
            df_ops = loaded_data['ops_validation'].copy()
            
            # 設置欄位名稱
            locker_columns = config_manager.get_list('SPX', 'locker_columns')
            df_ops.columns = locker_columns
            
            # 過濾和轉換
            df_ops = df_ops.loc[~df_ops['驗收月份'].isna(), :].reset_index(drop=True)
            df_ops['validated_month'] = pd.to_datetime(
                df_ops['驗收月份'], errors='coerce'
            ).dt.strftime('%Y%m').astype('Int64')
            
            # 移除無效日期的記錄
            df_ops = df_ops.dropna(subset=['validated_month']).reset_index(drop=True)
            # 篩選小於目標月份的數據
            df_ops_filtered = df_ops.loc[df_ops['validated_month'] < target_date, :]
            
            # 清理其他文字欄位
            text_columns = df_ops.select_dtypes(include=['object']).columns
            for col in text_columns:
                df_ops[col] = df_ops[col].fillna('').astype(str).str.strip()
            
            standardized['ops_validation'] = df_ops
            self.logger.debug("Standardized OPS validation data")
        
        return standardized
    
    def _store_to_context(
        self,
        context: ProcessingContext,
        processed_data: Dict[str, pd.DataFrame]
    ):
        """
        將處理好的資料儲存到 ProcessingContext
        
        Args:
            context: 處理上下文
            processed_data: 已處理的資料字典
        """
        # 儲存會計底稿
        if 'accounting_workpaper' in processed_data:
            context.add_auxiliary_data(
                'accounting_workpaper',
                processed_data['accounting_workpaper']
            )
            self.logger.info(
                f"Added accounting workpaper: "
                f"{processed_data['accounting_workpaper'].shape}"
            )
        
        # 儲存 OPS 底稿
        if 'ops_validation' in processed_data:
            context.add_auxiliary_data(
                'ops_validation',
                processed_data['ops_validation']
            )
            self.logger.info(
                f"Added OPS validation data: "
                f"{processed_data['ops_validation'].shape}"
            )
        
        # 將會計底稿設為主數據（供後續步驟使用）
        if 'accounting_workpaper' in processed_data:
            context.update_data(processed_data['accounting_workpaper'])
    
    def _validate_file_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        驗證檔案配置
        
        Returns:
            驗證通過的檔案配置字典
            
        Raises:
            FileNotFoundError: 如果必要檔案不存在
        """
        validated = {}
        
        # 必要檔案列表
        required_files = ['accounting_workpaper', 'ops_validation']
        
        for file_type, config in self.file_configs.items():
            file_path = config.get('path')
            
            if not file_path:
                if file_type in required_files:
                    raise ValueError(f"No path provided for required file: {file_type}")
                continue
            
            path = Path(file_path)
            
            if not path.exists():
                if file_type in required_files:
                    raise FileNotFoundError(
                        f"Required file not found: {file_type} at {file_path}"
                    )
                else:
                    self.logger.warning(f"Optional file not found: {file_path}")
                    continue
            
            validated[file_type] = config
            self.logger.debug(f"Validated file: {file_type} at {file_path}")
        
        # 檢查是否所有必要檔案都存在
        missing_files = [
            f for f in required_files if f not in validated
        ]
        if missing_files:
            raise FileNotFoundError(
                f"Missing required files: {', '.join(missing_files)}"
            )
        
        return validated
    
    async def _cleanup_resources(self):
        """清理數據源資源"""
        try:
            await self.pool.close_all()
            self.logger.debug("All data sources closed")
        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {str(e)}")
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入配置
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 驗證是否通過
        """
        # 檢查是否提供了檔案配置
        if not self.file_configs:
            self.logger.error("No file configs provided")
            context.add_error("No file configs provided")
            return False
        
        # 檢查必要檔案配置
        required_files = ['accounting_workpaper', 'ops_validation']
        for file_type in required_files:
            if file_type not in self.file_configs:
                self.logger.error(f"Missing required file config: {file_type}")
                context.add_error(f"Missing required file config: {file_type}")
                return False
        
        # 檢查檔案是否存在
        for file_type in required_files:
            file_path = self.file_configs[file_type].get('path')
            if not file_path or not Path(file_path).exists():
                self.logger.error(f"File not found: {file_type} at {file_path}")
                context.add_error(f"File not found: {file_type} at {file_path}")
                return False
        
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """
        回滾操作 - 清理已載入的資源
        
        Args:
            context: 處理上下文
            error: 觸發回滾的錯誤
        """
        self.logger.warning(
            f"Rolling back accounting/OPS data loading due to error: {str(error)}"
        )
        
        # 清理 Context 中的資料
        if 'accounting_workpaper' in context.auxiliary_data:
            del context.auxiliary_data['accounting_workpaper']
        if 'ops_validation' in context.auxiliary_data:
            del context.auxiliary_data['ops_validation']
        
        # 清理資源
        await self._cleanup_resources()


class SPXPRDataLoadingStep(PipelineStep):
    """
    SPX PR 數據並發載入步驟
    
    專為 PR (請購單) 管道設計，與 PO 管道的 SPXDataLoadingStep 平行。
    
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
        step = SPXPRDataLoadingStep(
            name="SPXPRLoading",
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
                 name: str = "SPXPRDataLoading",
                 file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
                 **kwargs):
        """
        初始化 SPX PR 數據載入步驟
        
        Args:
            name: 步驟名稱
            file_paths: 檔案路徑配置字典
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(
            name, 
            description="Load all SPX PR files using datasources module", 
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