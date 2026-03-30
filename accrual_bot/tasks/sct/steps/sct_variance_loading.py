"""
SCT 差異分析 - 數據載入步驟

載入當期底稿和前期底稿兩個 Excel 檔案。
不使用 BaseLoadingStep，因為需要載入兩個對等的主要檔案（非一主多輔模式）。
"""

from typing import Any, Dict, Optional, Union

import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.datasources import DataSourceFactory
from accrual_bot.utils.logging import get_logger


class SCTVarianceDataLoadingStep(PipelineStep):
    """
    SCT 差異分析數據載入步驟

    載入兩個 Excel 檔案：
    - current_worksheet: 當期 PO 底稿（讀取指定 sheet）
    - previous_worksheet: 前期 PO 底稿
    """

    def __init__(self, name: str = "SCTVarianceDataLoading", **kwargs):
        # 從 kwargs 提取 file_paths 後再傳給父類
        self.file_paths: Dict[str, Any] = kwargs.pop('file_paths', {})
        super().__init__(name=name, **kwargs)
        self.logger = get_logger(__name__)

    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        載入當期與前期底稿

        當期底稿 → context.data
        前期底稿 → context.auxiliary_data['previous_worksheet']
        """
        try:
            # 載入當期底稿
            current_df = await self._load_file('current_worksheet')
            if current_df is None or current_df.empty:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="當期底稿為空或無法載入",
                )

            self.logger.info(f"當期底稿載入成功: {current_df.shape}")

            # 載入前期底稿
            previous_df = await self._load_file('previous_worksheet')
            if previous_df is None or previous_df.empty:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="前期底稿為空或無法載入",
                )

            self.logger.info(f"前期底稿載入成功: {previous_df.shape}")

            # 儲存到 context
            context.data = current_df
            context.set_auxiliary_data('previous_worksheet', previous_df)

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"載入完成: 當期 {current_df.shape}, 前期 {previous_df.shape}",
                metadata={
                    'current_rows': len(current_df),
                    'previous_rows': len(previous_df),
                },
            )

        except Exception as e:
            self.logger.error(f"數據載入失敗: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"數據載入失敗: {e}",
            )

    async def _load_file(self, file_key: str) -> Optional[pd.DataFrame]:
        """
        載入單一 Excel 檔案

        支援兩種 file_paths 格式：
        - 字串: 直接作為路徑
        - 字典: {'path': str, 'params': dict}（由 _enrich_file_paths 產生）
        """
        file_info = self.file_paths.get(file_key)
        if file_info is None:
            raise ValueError(f"缺少必要檔案: {file_key}")

        if isinstance(file_info, dict):
            file_path = file_info['path']
            params = file_info.get('params', {})
        else:
            file_path = str(file_info)
            params = {}

        self.logger.debug(f"載入 {file_key}: {file_path}, params={params}")

        source = DataSourceFactory.create_from_file(file_path, **params)
        df = await source.read()
        return df

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證必要檔案路徑已提供"""
        required_keys = ['current_worksheet', 'previous_worksheet']
        for key in required_keys:
            if key not in self.file_paths:
                self.logger.error(f"缺少必要檔案路徑: {key}")
                return False
        return True
