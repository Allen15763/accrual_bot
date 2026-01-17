"""
SPT Procurement Data Loading Steps

採購任務資料載入步驟
"""

from typing import Tuple
import pandas as pd

from accrual_bot.core.pipeline.steps.base_loading import BaseLoadingStep
from accrual_bot.core.pipeline.context import ProcessingContext


class SPTProcurementDataLoadingStep(BaseLoadingStep):
    """
    SPT 採購任務 PO 資料載入步驟

    功能:
    1. 載入 raw_po 主要資料
    2. 載入採購前期底稿 (PO sheet)
    """

    def get_required_file_type(self) -> str:
        """返回主要檔案類型"""
        return 'raw_po'

    async def _load_primary_file(self, source, path: str) -> Tuple[pd.DataFrame, int, int]:
        """
        載入主要 PO 檔案

        Returns:
            Tuple[DataFrame, date_YYYYMM, month]
        """
        df = await source.read()
        # 從檔名提取日期
        date, month = self._extract_date_from_filename(path)
        return df, date, month

    def _extract_primary_data(self, primary_result) -> Tuple[pd.DataFrame, int, int]:
        """提取主要資料"""
        return primary_result

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """
        載入參考資料：採購前期底稿 (PO sheet)

        Returns:
            載入的參考資料數量
        """
        count = 0

        # 載入採購前期底稿 (PO sheet)
        if 'procurement_previous' in self.file_paths:
            self.logger.info("Loading procurement previous workpaper (PO sheet)...")

            file_config = self.file_paths['procurement_previous']
            params = file_config.get('params', {}).copy() if isinstance(file_config, dict) else {}
            params['sheet_name'] = 'PO'  # 指定 PO sheet

            procurement_prev = await self._load_reference_file_with_params(
                'procurement_previous',
                params
            )

            if procurement_prev is not None and not procurement_prev.empty:
                context.set_auxiliary_data('procurement_previous', procurement_prev)
                self.logger.info(f"Loaded procurement previous PO: {len(procurement_prev)} rows")
                count += 1
            else:
                self.logger.warning("Procurement previous PO workpaper is empty")

        return count


class SPTProcurementPRDataLoadingStep(BaseLoadingStep):
    """
    SPT 採購任務 PR 資料載入步驟

    功能:
    1. 載入 raw_pr 主要資料
    2. 載入採購前期底稿 (PR sheet)
    """

    def get_required_file_type(self) -> str:
        """返回主要檔案類型"""
        return 'raw_pr'

    async def _load_primary_file(self, source, path: str) -> Tuple[pd.DataFrame, int, int]:
        """
        載入主要 PR 檔案

        Returns:
            Tuple[DataFrame, date_YYYYMM, month]
        """
        df = await source.read()
        # 從檔名提取日期
        date, month = self._extract_date_from_filename(path)
        return df, date, month

    def _extract_primary_data(self, primary_result) -> Tuple[pd.DataFrame, int, int]:
        """提取主要資料"""
        return primary_result

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """
        載入參考資料：採購前期底稿 (PR sheet)

        Returns:
            載入的參考資料數量
        """
        count = 0

        # 載入採購前期底稿 (PR sheet)
        if 'procurement_previous' in self.file_paths:
            self.logger.info("Loading procurement previous workpaper (PR sheet)...")

            file_config = self.file_paths['procurement_previous']
            params = file_config.get('params', {}).copy() if isinstance(file_config, dict) else {}
            params['sheet_name'] = 'PR'  # 指定 PR sheet

            procurement_prev = await self._load_reference_file_with_params(
                'procurement_previous',
                params
            )

            if procurement_prev is not None and not procurement_prev.empty:
                context.set_auxiliary_data('procurement_previous', procurement_prev)
                self.logger.info(f"Loaded procurement previous PR: {len(procurement_prev)} rows")
                count += 1
            else:
                self.logger.warning("Procurement previous PR workpaper is empty")

        return count
