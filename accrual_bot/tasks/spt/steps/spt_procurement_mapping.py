"""
SPT Procurement Previous Workpaper Mapping Step

採購前期底稿映射步驟 - 配置驅動版本
參考: PreviousWorkpaperIntegrationStep 的配置驅動設計
"""

from typing import Dict, List
import pandas as pd
import time
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.helpers.data_utils import create_mapping_dict
from accrual_bot.utils.helpers.column_utils import ColumnResolver


class ProcurementPreviousMappingStep(PipelineStep):
    """
    採購前期底稿映射步驟 - 配置驅動版本

    功能:
    1. 從配置讀取映射規則
    2. 支援透過 TOML 配置新增欄位映射
    3. 使用 ColumnResolver 解析欄位名稱
    4. 自動判斷處理類型 (PO/PR)

    配置路徑: config/stagging.toml -> [spt_procurement_previous_mapping]
    """

    def __init__(self, name: str = "ProcurementPreviousMapping", **kwargs):
        super().__init__(name, description="Map procurement previous workpaper", **kwargs)
        self._load_mapping_config()

    def _load_mapping_config(self) -> None:
        """從配置載入映射規則"""
        config = config_manager._config_toml.get('spt_procurement_previous_mapping', {})
        self.column_patterns = config.get('column_patterns', {})
        self.po_mappings = config.get('po_mappings', {}).get('fields', [])
        self.pr_mappings = config.get('pr_mappings', {}).get('fields', [])

        self.logger.debug(
            f"Loaded procurement mapping config: {len(self.po_mappings)} PO mappings, "
            f"{len(self.pr_mappings)} PR mappings"
        )

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行採購前期底稿映射"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()
            prev_df = context.get_auxiliary_data('procurement_previous')

            if prev_df is None or prev_df.empty:
                self.logger.warning("No procurement previous workpaper data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No procurement previous workpaper data",
                    duration=time.time() - start_time
                )

            self.logger.info("Processing procurement previous workpaper mapping...")

            # 判斷處理類型 (PO 或 PR)
            is_po = 'PO#' in df.columns
            key_type = 'po' if is_po else 'pr'
            mappings = self.po_mappings if is_po else self.pr_mappings

            self.logger.info(f"Detected {key_type.upper()} processing type")

            # 應用配置驅動映射
            df = self._apply_field_mappings(df, prev_df, mappings, key_type)

            context.update_data(df)
            duration = time.time() - start_time

            self.logger.info(
                f"Procurement previous mapping complete: applied {len(mappings)} field mappings "
                f"in {duration:.2f}s"
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Applied {len(mappings)} field mappings",
                duration=duration,
                metadata={
                    'processing_type': key_type.upper(),
                    'mappings_applied': len(mappings),
                    'prev_rows': len(prev_df)
                }
            )

        except Exception as e:
            self.logger.error(f"Procurement previous mapping failed: {str(e)}", exc_info=True)
            context.add_error(f"Procurement previous mapping failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    def _apply_field_mappings(
        self,
        df: pd.DataFrame,
        source_df: pd.DataFrame,
        mappings: List[Dict],
        key_type: str
    ) -> pd.DataFrame:
        """
        通用欄位映射應用 (參考 PreviousWorkpaperIntegrationStep)

        Args:
            df: 目標 DataFrame
            source_df: 來源 DataFrame (前期底稿)
            mappings: 映射配置列表
            key_type: 'po' 或 'pr'，用於決定鍵值欄位

        Returns:
            更新後的 DataFrame
        """
        # 一次性解析鍵值欄位
        key_col_canonical = f'{key_type}_line'
        df_key = ColumnResolver.resolve(df, key_col_canonical)
        source_key = ColumnResolver.resolve(source_df, key_col_canonical)

        if df_key is None or source_key is None:
            self.logger.warning(f"Cannot resolve key column for {key_type}")
            return df

        self.logger.info(f"Using key columns: df[{df_key}] <- source[{source_key}]")

        # 應用所有映射
        for mapping in mappings:
            df = self._apply_single_mapping(df, source_df, mapping, df_key, source_key)

        return df

    def _apply_single_mapping(
        self,
        df: pd.DataFrame,
        source_df: pd.DataFrame,
        mapping: Dict,
        df_key: str,
        source_key: str
    ) -> pd.DataFrame:
        """應用單一欄位映射"""
        source_col = ColumnResolver.resolve(source_df, mapping['source'])
        if source_col is None:
            self.logger.debug(f"Source column '{mapping['source']}' not found, skipping")
            return df

        target_col = mapping['target']
        fill_na = mapping.get('fill_na', True)

        # 建立映射字典
        mapping_dict = create_mapping_dict(source_df, source_key, source_col)

        # 應用映射
        if fill_na:
            # 只填充空值
            df[target_col] = df[df_key].map(mapping_dict).fillna(pd.NA)
        else:
            # 允許覆蓋 - 先映射，然後用原值填充空值
            mapped = df[df_key].map(mapping_dict)
            if target_col in df.columns:
                df[target_col] = mapped.fillna(df[target_col])
            else:
                df[target_col] = mapped

        self.logger.debug(
            f"Mapped {source_col} -> {target_col} "
            f"(fill_na={fill_na}, {len(mapping_dict)} mappings)"
        )

        return df

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for procurement previous mapping")
            return False

        # 檢查是否有 PO# 或 PR# 欄位
        has_po = 'PO#' in context.data.columns
        has_pr = 'PR#' in context.data.columns

        if not has_po and not has_pr:
            self.logger.error("Missing PO# or PR# column")
            return False

        return True
