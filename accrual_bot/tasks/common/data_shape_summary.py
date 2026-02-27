"""
Data Shape Summary Step - 資料完整性驗證步驟

產出原始資料與處理後資料的分布摘要（pivot table），
用於驗證管道處理過程中資料的完整性。

支援兩種執行模式：
1. 管道步驟模式：作為管道最後一步，從 context 取得資料
2. 獨立執行模式：直接載入 checkpoint parquet 或原始檔案

Usage (管道模式):
    # 在 orchestrator 的 _create_step() 中註冊
    'DataShapeSummary': lambda: DataShapeSummaryStep(name="DataShapeSummary")

Usage (獨立模式):
    import asyncio
    from accrual_bot.tasks.common.data_shape_summary import run_standalone_summary

    asyncio.run(run_standalone_summary(
        raw_data_path='checkpoints/SPX_PO_202601_after_SPXDataLoading/data.parquet',
        processed_data_path='checkpoints/SPX_PO_202601_after_DataReformatting/data.parquet',
    ))
"""

import time
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder,
    create_error_metadata
)
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)


class DataShapeSummaryStep(PipelineStep):
    """
    資料完整性驗證步驟

    從 context 中取得原始資料快照與最終處理結果，
    建立 pivot table 摘要並進行比較，可選導出 Excel。

    資料來源:
    - auxiliary_data['raw_data_snapshot']: 由 loading step 存入的原始資料副本
    - context.data: 管道最終處理結果

    產出:
    - auxiliary_data['shape_summary_raw_data']: 原始資料 pivot
    - auxiliary_data['shape_summary_processed_data']: 處理後資料 pivot
    - auxiliary_data['shape_summary_comparison']: 行數/金額比較表
    - DataShape_Summary_{entity}_{type}_{date}.xlsx (可選)
    """

    def __init__(
        self,
        name: str = "DataShapeSummary",
        export_excel: bool = True,
        output_dir: str = "output",
        required: bool = False,
        **kwargs
    ):
        """
        初始化資料完整性驗證步驟

        Args:
            name: 步驟名稱
            export_excel: 是否導出 Excel 檔案
            output_dir: Excel 輸出目錄
            required: 是否為必要步驟（預設 False，失敗不中斷管道）
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(
            name,
            description="產生資料完整性驗證摘要（raw vs processed pivot tables）",
            required=required,
            **kwargs
        )
        self.export_excel = export_excel
        self.output_dir = Path(output_dir)
        self._summary_config = config_manager._config_toml.get('data_shape_summary', {})

    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行資料完整性驗證

        流程:
        1. 從 auxiliary_data 取得原始資料快照 → 建立 pivot
        2. 從 context.data 取得最終資料 → 建立 pivot
        3. 建立比較摘要表
        4. 存入 auxiliary_data（UI 自動顯示為 tab）
        5. 可選導出 Excel
        """
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            raw_cols = self._summary_config.get('raw_columns', {})
            processed_cols = self._summary_config.get('processed_columns', {})
            summaries: Dict[str, pd.DataFrame] = {}

            # 1. 原始資料 pivot
            raw_snapshot = context.get_auxiliary_data('raw_data_snapshot')
            if raw_snapshot is not None and not raw_snapshot.empty:
                raw_pivot = self._create_pivot_summary(
                    raw_snapshot,
                    product_col=raw_cols.get('product_col', 'Product Code'),
                    currency_col=raw_cols.get('currency_col', 'Currency'),
                    amount_col=raw_cols.get('amount_col', 'Entry Amount'),
                )
                if not raw_pivot.empty:
                    summaries['raw_data'] = raw_pivot

            # 2. 處理後資料 pivot
            final_df = context.data
            if final_df is not None and not final_df.empty:
                processed_pivot = self._create_pivot_summary(
                    final_df,
                    product_col=processed_cols.get('product_col', 'product_code'),
                    currency_col=processed_cols.get('currency_col', 'currency'),
                    amount_col=processed_cols.get('amount_col', 'entry_amount'),
                )
                if not processed_pivot.empty:
                    summaries['processed_data'] = processed_pivot

            # 3. 比較摘要
            if raw_snapshot is not None and final_df is not None:
                comparison = self._create_comparison_summary(
                    raw_df=raw_snapshot,
                    final_df=final_df,
                    raw_amount_col=raw_cols.get('amount_col', 'Entry Amount'),
                    processed_amount_col=processed_cols.get('amount_col', 'entry_amount'),
                )
                summaries['comparison'] = comparison

            # 4. 存入 auxiliary_data
            for key, df in summaries.items():
                context.add_auxiliary_data(f'shape_summary_{key}', df)

            # 5. 可選導出 Excel
            output_path = None
            if self.export_excel and summaries:
                output_path = self._export_to_excel(context, summaries)

            duration = time.time() - start_time
            end_datetime = datetime.now()

            metadata = (
                StepMetadataBuilder()
                .set_time_info(start_datetime, end_datetime)
                .add_custom('sheets_generated', list(summaries.keys()))
                .add_custom('output_path', str(output_path) if output_path else None)
                .add_custom('raw_snapshot_available', raw_snapshot is not None)
                .build()
            )

            msg = f"資料驗證摘要: 產出 {len(summaries)} 個分頁"
            if output_path:
                msg += f", 已導出至 {output_path}"

            self.logger.info(msg)

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=msg,
                duration=duration,
                metadata=metadata
            )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"資料驗證摘要失敗: {e}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e),
                duration=duration,
                metadata=create_error_metadata(e, context, self.name)
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入 - 至少需要主數據非空"""
        if context.data is None or context.data.empty:
            self.logger.warning("無可用資料進行驗證摘要")
            return False
        return True

    @staticmethod
    def _create_pivot_summary(
        df: pd.DataFrame,
        product_col: str,
        currency_col: str,
        amount_col: str,
    ) -> pd.DataFrame:
        """
        建立產品代碼與幣別的 pivot table 摘要

        Args:
            df: 來源資料框
            product_col: 產品代碼欄位名稱
            currency_col: 幣別欄位名稱
            amount_col: 金額欄位名稱

        Returns:
            pd.DataFrame: pivot table（index=product, columns=currency,
                          values=amount, aggfunc=[sum, count], margins=True）
        """
        required_cols = [product_col, currency_col, amount_col]
        available_cols = [c for c in required_cols if c in df.columns]
        if len(available_cols) < 3:
            logger.warning(
                f"欄位不足，需要 {required_cols}，"
                f"實際可用 {available_cols}"
            )
            return pd.DataFrame()

        return (
            df[required_cols]
            .assign(amt=lambda row: pd.to_numeric(
                row[amount_col], errors='coerce'
            ).fillna(0))
            .pivot_table(
                index=[product_col],
                columns=[currency_col],
                values='amt',
                aggfunc=['sum', 'count'],
                margins=True,
                margins_name='Total'
            )
        )

    @staticmethod
    def _create_comparison_summary(
        raw_df: pd.DataFrame,
        final_df: pd.DataFrame,
        raw_amount_col: str,
        processed_amount_col: str,
    ) -> pd.DataFrame:
        """
        建立原始 vs 最終資料的比較摘要

        Args:
            raw_df: 原始資料框
            final_df: 處理後資料框
            raw_amount_col: 原始資料金額欄位
            processed_amount_col: 處理後資料金額欄位

        Returns:
            pd.DataFrame: 比較摘要表
        """
        raw_amount = pd.to_numeric(
            raw_df[raw_amount_col], errors='coerce'
        ).fillna(0).sum() if raw_amount_col in raw_df.columns else 0

        processed_amount = pd.to_numeric(
            final_df[processed_amount_col], errors='coerce'
        ).fillna(0).sum() if processed_amount_col in final_df.columns else 0

        comparison = {
            '指標': [
                '資料列數',
                '欄位數',
                '金額合計',
            ],
            '原始資料': [
                len(raw_df),
                len(raw_df.columns),
                raw_amount,
            ],
            '處理後資料': [
                len(final_df),
                len(final_df.columns),
                processed_amount,
            ],
            '差異': [
                len(final_df) - len(raw_df),
                len(final_df.columns) - len(raw_df.columns),
                processed_amount - raw_amount,
            ],
        }
        return pd.DataFrame(comparison)

    def _export_to_excel(
        self,
        context: ProcessingContext,
        summaries: Dict[str, pd.DataFrame]
    ) -> Path:
        """
        導出摘要至 Excel 檔案

        Args:
            context: 處理上下文（用於取得 entity/type/date）
            summaries: 各分頁資料

        Returns:
            Path: 輸出檔案路徑
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        entity = context.metadata.entity_type
        proc_type = context.metadata.processing_type
        date = context.metadata.processing_date
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        filename = f"DataShape_Summary_{entity}_{proc_type}_{date}_{timestamp}.xlsx"
        output_path = self.output_dir / filename

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet_name, df in summaries.items():
                df.to_excel(writer, sheet_name=sheet_name)

        self.logger.info(f"資料驗證摘要已導出: {output_path}")
        context.set_variable('data_shape_summary_path', str(output_path))
        return output_path


# ============================================================================
# 獨立執行模式
# ============================================================================

async def run_standalone_summary(
    raw_data_path: str,
    processed_data_path: str,
    entity: str = 'SPX',
    processing_type: str = 'PO',
    processing_date: int = 202601,
    output_dir: str = 'output'
) -> StepResult:
    """
    獨立執行 Data Shape Summary（不需完整管道）

    可從 checkpoint parquet 或原始檔案直接產出摘要。

    Args:
        raw_data_path: 原始資料路徑（parquet/csv/xlsx）
        processed_data_path: 處理後資料路徑
        entity: 實體類型（SPX/SPT）
        processing_type: 處理類型（PO/PR）
        processing_date: 處理日期（YYYYMM）
        output_dir: 輸出目錄

    Returns:
        StepResult: 執行結果

    Usage:
        import asyncio
        result = asyncio.run(run_standalone_summary(
            raw_data_path='checkpoints/SPX_PO_202601_after_SPXDataLoading/data.parquet',
            processed_data_path='checkpoints/SPX_PO_202601_after_DataReformatting/data.parquet',
        ))
    """
    raw_df = _load_file(raw_data_path)
    processed_df = _load_file(processed_data_path)

    context = ProcessingContext(
        data=processed_df,
        entity_type=entity,
        processing_date=processing_date,
        processing_type=processing_type
    )
    context.add_auxiliary_data('raw_data_snapshot', raw_df)

    step = DataShapeSummaryStep(
        export_excel=True,
        output_dir=output_dir
    )
    result = await step(context)

    logger.info(f"獨立執行結果: {result.status.value} - {result.message}")
    return result


def _load_file(path: str) -> pd.DataFrame:
    """
    自動偵測檔案格式並載入

    Args:
        path: 檔案路徑

    Returns:
        pd.DataFrame: 載入的資料
    """
    p = Path(path)
    match p.suffix.lower():
        case '.parquet':
            return pd.read_parquet(path)
        case '.xlsx' | '.xls':
            return pd.read_excel(path)
        case '.csv':
            return pd.read_csv(path, dtype=str)
        case _:
            raise ValueError(f"不支援的檔案格式: {p.suffix}")


if __name__ == '__main__':
    import asyncio

    asyncio.run(run_standalone_summary(
        raw_data_path='checkpoints/SPX_PO_202601_after_SPXDataLoading/data.parquet',
        processed_data_path='checkpoints/SPX_PO_202601_after_DataReformatting/data.parquet',
    ))
