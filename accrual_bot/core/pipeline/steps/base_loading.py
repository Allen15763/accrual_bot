"""
Base Loading Step - 數據載入步驟的基類

使用模板方法模式（Template Method Pattern）提取共用的數據載入邏輯，
減少 SPT/SPX 載入步驟之間的代碼重複。

Usage:
    class SPTDataLoadingStep(BaseLoadingStep):
        def get_required_file_type(self) -> str:
            return 'raw_po'

        async def _load_primary_file(self, source, file_path) -> Tuple[pd.DataFrame, int, int]:
            # SPT specific loading logic
            ...
"""

import time
import re
import asyncio
from abc import abstractmethod
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


class BaseLoadingStep(PipelineStep):
    """
    數據載入步驟的抽象基類

    提供通用的並發文件載入功能，子類只需實現實體特定的邏輯。

    Template Method Pattern:
    - 模板方法: execute(), _load_all_files_concurrent(), _validate_file_configs()
    - 鉤子方法: get_required_file_type(), _load_primary_file(), _load_reference_data()

    Attributes:
        file_configs: 標準化後的文件配置字典
        pool: 數據源連接池
    """

    def __init__(
        self,
        name: str,
        file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
        description: str = "Load data files using datasources module",
        **kwargs
    ):
        """
        初始化載入步驟

        Args:
            name: 步驟名稱
            file_paths: 文件路徑配置字典
            description: 步驟描述
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(name, description=description, **kwargs)
        self.file_configs = self._normalize_file_paths(file_paths or {})
        self.pool = DataSourcePool()

    # ========== 模板方法 (Template Methods) - 共用實現 ==========

    def _normalize_file_paths(
        self,
        file_paths: Dict[str, Union[str, Dict[str, Any]]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        標準化文件路徑格式，支援向後兼容

        支援兩種格式:
        - 簡單格式: {'raw_po': 'path/to/file.csv'}
        - 完整格式: {'raw_po': {'path': 'path/to/file.csv', 'params': {...}}}

        Args:
            file_paths: 原始文件路徑字典

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
                    raise ValueError(f"Missing 'path' in config for {file_type}")
                normalized[file_type] = {
                    'path': config['path'],
                    'params': config.get('params', {})
                }
            else:
                raise ValueError(f"Invalid config type for {file_type}: {type(config)}")

        return normalized

    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行並發數據載入（模板方法）

        執行流程:
        1. 驗證文件存在性
        2. 並發讀取所有文件
        3. 提取和驗證主數據
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
            self.logger.info(f"Starting concurrent file loading for {self.name}...")

            # 階段 1: 驗證文件存在性
            validated_configs = self._validate_file_configs()

            if not validated_configs:
                raise ValueError("No valid files to load")

            # 階段 2: 並發載入所有主要文件
            loaded_data = await self._load_all_files_concurrent(validated_configs)

            # 階段 3: 提取和驗證主數據
            required_file_type = self.get_required_file_type()
            if required_file_type not in loaded_data:
                raise ValueError(f"Failed to load {required_file_type} data")

            df, date, m = self._extract_primary_data(loaded_data[required_file_type])

            # 更新 Context 主數據
            context.update_data(df)
            context.set_variable('processing_date', date)
            context.set_variable('processing_month', m)
            context.set_variable('file_paths', validated_configs)

            # 子類特定的 context 設置
            self._set_additional_context_variables(context, validated_configs, loaded_data)

            # 階段 4: 添加輔助數據到 Context
            auxiliary_count = self._add_auxiliary_data_to_context(
                context, loaded_data, required_file_type
            )

            # 階段 5: 載入參考數據
            ref_count = await self._load_reference_data(context)

            # 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()

            self.logger.info(
                f"Successfully loaded {df.shape} data, "
                f"{auxiliary_count} auxiliary datasets, "
                f"{ref_count} reference datasets in {duration:.2f}s"
            )

            # 構建標準化 metadata
            metadata = self._build_success_metadata(
                df, date, m, auxiliary_count, ref_count,
                loaded_data, start_datetime, end_datetime
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Loaded {len(df)} records with {auxiliary_count} auxiliary datasets",
                duration=duration,
                metadata=metadata
            )

        except Exception as e:
            duration = time.time() - start_time

            self.logger.error(f"Data loading failed: {str(e)}", exc_info=True)
            context.add_error(f"Data loading failed: {str(e)}")

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
                duration=duration,
                metadata=error_metadata
            )

        finally:
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
        tasks = []
        file_names = []

        for file_type, config in validated_configs.items():
            task = self._load_single_file(file_type, config)
            tasks.append(task)
            file_names.append(file_type)

        self.logger.info(f"Loading {len(tasks)} files concurrently...")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        loaded_data = {}
        required_file_type = self.get_required_file_type()

        for file_type, result in zip(file_names, results):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to load {file_type}: {str(result)}")
                if file_type == required_file_type:
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
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, int, int], None]:
        """
        載入單個文件（支援參數配置）

        Args:
            file_type: 文件類型
            config: 文件配置 {'path': '...', 'params': {...}}

        Returns:
            Union[pd.DataFrame, Tuple, None]: 載入的數據
        """
        try:
            file_path = config['path']
            params = config.get('params', {})

            source = DataSourceFactory.create_from_file(file_path, **params)
            self.pool.add_source(file_type, source)

            required_file_type = self.get_required_file_type()

            if file_type == required_file_type:
                return await self._load_primary_file(source, file_path)
            else:
                # 使用子類的自定義載入邏輯（如果有）
                custom_loader = self._get_custom_file_loader(file_type)
                if custom_loader:
                    return await custom_loader(source, config)
                else:
                    df = await source.read()
                    self.logger.debug(f"成功導入 {file_type}, 數據維度: {df.shape}")
                    return df

        except Exception as e:
            self.logger.error(f"Error loading {file_type} from {config.get('path')}: {str(e)}")
            raise

    def _validate_file_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        驗證文件配置

        Returns:
            Dict[str, Dict[str, Any]]: 驗證通過的文件配置字典

        Raises:
            FileNotFoundError: 當必要文件不存在時
        """
        validated = {}
        required_file_type = self.get_required_file_type()

        for file_type, config in self.file_configs.items():
            file_path = config.get('path')

            if not file_path:
                self.logger.warning(f"No path provided for {file_type}")
                continue

            path = Path(file_path)

            if not path.exists():
                self.logger.warning(f"File not found: {file_type} at {file_path}")
                if file_type == required_file_type:
                    raise FileNotFoundError(f"Required file not found: {file_path}")
                continue

            validated[file_type] = config
            self.logger.debug(f"Validated file: {file_type}")

        return validated

    def _add_auxiliary_data_to_context(
        self,
        context: ProcessingContext,
        loaded_data: Dict[str, Any],
        exclude_key: str
    ) -> int:
        """
        添加輔助數據到 Context

        Args:
            context: 處理上下文
            loaded_data: 已載入的數據
            exclude_key: 要排除的主數據鍵

        Returns:
            int: 添加的輔助數據數量
        """
        count = 0
        for data_name, data_content in loaded_data.items():
            if data_name != exclude_key and data_content is not None:
                if isinstance(data_content, pd.DataFrame) and not data_content.empty:
                    context.add_auxiliary_data(data_name, data_content)
                    count += 1
                    self.logger.info(f"Added auxiliary data: {data_name} ({data_content.shape})")
        return count

    def _build_success_metadata(
        self,
        df: pd.DataFrame,
        date: int,
        month: int,
        auxiliary_count: int,
        ref_count: int,
        loaded_data: Dict[str, Any],
        start_datetime: datetime,
        end_datetime: datetime
    ) -> Dict[str, Any]:
        """構建成功執行的 metadata"""
        return (
            StepMetadataBuilder()
            .set_row_counts(0, len(df))
            .set_process_counts(processed=len(df))
            .set_time_info(start_datetime, end_datetime)
            .add_custom('records', len(df))
            .add_custom('columns', len(df.columns))
            .add_custom('processing_date', int(date))
            .add_custom('processing_month', int(month))
            .add_custom('auxiliary_datasets', auxiliary_count)
            .add_custom('reference_datasets', ref_count)
            .add_custom('loaded_files', list(loaded_data.keys()))
            .add_custom('files_loaded_count', len(loaded_data))
            .build()
        )

    async def _cleanup_resources(self):
        """清理數據源資源"""
        try:
            await self.pool.close_all()
            self.logger.debug("All data sources closed")
        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {str(e)}")

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入配置"""
        if not self.file_configs:
            self.logger.error("No file configs provided")
            context.add_error("No file configs provided")
            return False

        required_file_type = self.get_required_file_type()
        if required_file_type not in self.file_configs:
            self.logger.error(f"Missing {required_file_type} file config")
            context.add_error(f"Missing {required_file_type} file config")
            return False

        file_path = self.file_configs[required_file_type].get('path')
        if not file_path or not Path(file_path).exists():
            self.logger.error(f"Required file not found: {file_path}")
            context.add_error(f"Required file not found: {file_path}")
            return False

        return True

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作 - 清理已載入的資源"""
        self.logger.warning(f"Rolling back data loading due to error: {str(error)}")
        await self._cleanup_resources()

    # ========== 共用工具方法 ==========

    def _extract_date_from_filename(self, file_path: str) -> Tuple[int, int]:
        """
        從文件名提取日期信息

        Args:
            file_path: 文件路徑

        Returns:
            Tuple[int, int]: (date YYYYMM, month)
        """
        file_name = Path(file_path).stem
        date_pattern = r'(\d{6})'
        match = re.search(date_pattern, file_name)

        if match:
            date_str = match.group(1)
            date = int(date_str)
            m = int(date_str[-2:])
        else:
            current = datetime.now()
            date = int(current.strftime('%Y%m'))
            m = current.month
            self.logger.warning(
                f"Could not extract date from filename, using current date: {date}"
            )

        return date, m

    def _process_common_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        處理常見的欄位格式

        Args:
            df: 原始 DataFrame

        Returns:
            pd.DataFrame: 處理後的 DataFrame
        """
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

        if 'Project Number' in df.columns:
            df = df.rename(columns={'Project Number': 'Project'})

        return df

    # ========== 鉤子方法 (Hook Methods) - 子類實現 ==========

    @abstractmethod
    def get_required_file_type(self) -> str:
        """
        返回必要的文件類型

        Returns:
            str: 必要文件類型 (如 'raw_po' 或 'raw_pr')
        """
        pass

    @abstractmethod
    async def _load_primary_file(
        self,
        source,
        file_path: str
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        載入主要數據文件

        Args:
            source: 數據源實例
            file_path: 文件路徑

        Returns:
            Tuple[pd.DataFrame, int, int]: (DataFrame, date, month)
        """
        pass

    @abstractmethod
    def _extract_primary_data(
        self,
        primary_result: Tuple[pd.DataFrame, int, int]
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        提取和驗證主數據

        Args:
            primary_result: 主數據載入結果

        Returns:
            Tuple[pd.DataFrame, int, int]: 驗證後的數據
        """
        pass

    @abstractmethod
    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """
        載入參考數據

        Args:
            context: 處理上下文

        Returns:
            int: 載入的參考數據集數量
        """
        pass

    # ========== 可選覆寫方法 ==========

    def _set_additional_context_variables(
        self,
        context: ProcessingContext,
        validated_configs: Dict[str, Dict[str, Any]],
        loaded_data: Dict[str, Any]
    ) -> None:
        """
        設置額外的 context 變量（可選覆寫）

        子類可以覆寫此方法來設置實體特定的 context 變量。

        Args:
            context: 處理上下文
            validated_configs: 已驗證的配置
            loaded_data: 已載入的數據
        """
        pass

    def _get_custom_file_loader(
        self,
        file_type: str
    ) -> Optional[callable]:
        """
        獲取自定義文件載入器（可選覆寫）

        子類可以覆寫此方法來為特定文件類型提供自定義載入邏輯。

        Args:
            file_type: 文件類型

        Returns:
            Optional[callable]: 自定義載入函數或 None
        """
        return None

    def get_required_columns(self) -> List[str]:
        """
        獲取必要的欄位列表（可選覆寫）

        Returns:
            List[str]: 必要欄位名稱列表
        """
        return ['Product Code', 'Item Description', 'GL#']
