"""
SCT Column Addition Step

為 SCT 數據添加必要欄位，基於 SPX 的 ColumnAdditionStep，
但移除了「累計至本期驗收數量/金額」等 SPX 特有欄位。

包含：
- 是否結案、結案差異數量、Check with Entry Invoice
- PO/PR Line 組合鍵
- 備註欄位（Remarked/Noted by Procurement/FN 等）
- 計算欄位（是否估計入帳、Account code、Accr. Amount 等）
- FA / S&M 判斷
"""

import time
import numpy as np
import pandas as pd
from typing import List, Tuple
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder,
    create_error_metadata
)


class SCTColumnAdditionStep(PipelineStep):
    """
    添加 SCT 所需欄位

    功能:
    1. 調用 _add_basic_columns() 添加基礎欄位
    2. 添加備註與狀態欄位（不含 SPX 特有的累計驗收欄位）

    輸入: DataFrame
    輸出: DataFrame with additional columns
    """

    def __init__(self, name: str = "SCTColumnAddition", **kwargs):
        super().__init__(name, description="Add SCT-specific columns", **kwargs)

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行欄位添加"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()
            input_count = len(df)
            m = context.metadata.processing_date % 100

            original_columns = set(df.columns)

            # 添加基礎欄位
            df, previous_month = self._add_basic_columns(df, m)

            # 添加 SCT 特定欄位（不含 SPX 的「累計至本期驗收數量/金額」）
            df['Remarked by Procurement PR'] = None
            df['Noted by Procurement PR'] = None
            df['Remarked by 上月 FN PR'] = None

            # 更新月份變數
            context.set_variable('processing_month', m)

            # 依 processing_type 決定欄位名稱（PR pipeline 使用 PR狀態）
            if context.metadata.processing_type == 'PR':
                df = df.rename(columns={'PO狀態': 'PR狀態'})
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
            self.logger.error(f"SCT column addition failed: {str(e)}", exc_info=True)
            context.add_error(f"SCT column addition failed: {str(e)}")
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='column_addition'
            )
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"SCT column addition failed: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )

    def _add_basic_columns(self, df: pd.DataFrame, month: int) -> Tuple[pd.DataFrame, int]:
        """
        添加基本必要列

        Args:
            df: 原始 PO/PR 數據
            month: 月份

        Returns:
            Tuple[pd.DataFrame, int]: 添加了必要列的 DataFrame 和上月月份
        """
        try:
            df_copy = df.copy()

            # 添加結案狀態欄位
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
        確定是否為 FA

        使用 SCT 的 FA accounts 設定（從 [fa_accounts] sct 讀取）。
        """
        fa_accounts: List = config_manager.get_list('FA_ACCOUNTS', 'sct')
        if 'GL#' in df.columns:
            return np.where(df['GL#'].astype('string').isin([str(x) for x in fa_accounts]), 'Y', '')
        return pd.Series('', index=df.index)

    def _determine_sm_status(self, df: pd.DataFrame) -> pd.Series:
        """
        確定是否為 S&M

        使用 SCT 的 S&M accounts 設定。
        """
        if 'GL#' not in df.columns:
            return pd.Series('N', index=df.index)

        sm_accounts = config_manager._config_toml.get(
            'sct_column_defaults', {}
        ).get('sm_accounts', ['650003', '450014'])
        sm_mask = df['GL#'].astype('string').isin([str(a) for a in sm_accounts])
        return np.where(sm_mask, "Y", "N")

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for SCT column addition")
            return False

        return True
