"""
SPT Combined Procurement Processing Step

COMBINED 模式處理步驟 - 分別處理 PO 和 PR 資料
"""

from typing import Dict, Any, List
import pandas as pd
import time
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.tasks.spt.steps import (
    ColumnInitializationStep,
    ProcurementPreviousMappingStep,
    SPTProcurementStatusEvaluationStep,
)
from accrual_bot.core.pipeline.steps import DateLogicStep


class CombinedProcurementProcessingStep(PipelineStep):
    """
    COMBINED 模式處理步驟

    功能:
    1. 從 auxiliary_data 讀取 PO 和 PR 資料
    2. 分別執行處理步驟（欄位初始化、前期映射、狀態判斷）
    3. 將處理結果存回 auxiliary_data

    設計原則:
    - 使用獨立的 sub-context 避免互相干擾
    - 複用現有的處理步驟
    - 不影響單一模式的處理流程
    """

    def __init__(self, name: str = "CombinedProcurementProcessing", **kwargs):
        super().__init__(name, description="Process PO and PR data separately", **kwargs)

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 COMBINED 處理"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            processing_summary = {
                'po_processed': False,
                'pr_processed': False,
                'po_final_rows': 0,
                'pr_final_rows': 0,
                'po_status_distribution': {},
                'pr_status_distribution': {}
            }

            # 1. 處理 PO 資料
            po_data = context.get_auxiliary_data('po_data')
            if po_data is not None and not po_data.empty:
                self.logger.info("Processing PO data...")
                po_result = await self._process_po_data(context, po_data)

                if po_result is not None:
                    context.set_auxiliary_data('po_result', po_result)
                    processing_summary['po_processed'] = True
                    processing_summary['po_final_rows'] = len(po_result)

                    # 統計狀態分佈
                    if 'PO狀態' in po_result.columns:
                        processing_summary['po_status_distribution'] = (
                            po_result['PO狀態'].value_counts().to_dict()
                        )

                    self.logger.info(f"✓ PO processing complete: {len(po_result)} rows")
                else:
                    self.logger.warning("✗ PO processing failed")
            else:
                self.logger.info("No PO data to process")

            # 2. 處理 PR 資料
            pr_data = context.get_auxiliary_data('pr_data')
            if pr_data is not None and not pr_data.empty:
                self.logger.info("Processing PR data...")
                pr_result = await self._process_pr_data(context, pr_data)

                if pr_result is not None:
                    context.set_auxiliary_data('pr_result', pr_result)
                    processing_summary['pr_processed'] = True
                    processing_summary['pr_final_rows'] = len(pr_result)

                    # 統計狀態分佈
                    if 'PR狀態' in pr_result.columns:
                        processing_summary['pr_status_distribution'] = (
                            pr_result['PR狀態'].value_counts().to_dict()
                        )

                    self.logger.info(f"✓ PR processing complete: {len(pr_result)} rows")
                else:
                    self.logger.warning("✗ PR processing failed")
            else:
                self.logger.info("No PR data to process")

            # 檢查是否至少處理一個
            if not processing_summary['po_processed'] and not processing_summary['pr_processed']:
                raise ValueError("Failed to process both PO and PR data")

            duration = time.time() - start_time

            # 生成摘要訊息
            summary_msg = self._generate_processing_summary(processing_summary)
            self.logger.info(f"\n{summary_msg}")

            po_final_rows = processing_summary['po_final_rows']
            pr_final_rows = processing_summary['pr_final_rows']

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Processed PO: {po_final_rows}, PR: {pr_final_rows}",
                duration=duration,
                metadata=processing_summary
            )

        except Exception as e:
            self.logger.error(f"Combined processing failed: {str(e)}", exc_info=True)
            context.add_error(f"Processing failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    async def _process_po_data(
        self,
        parent_context: ProcessingContext,
        po_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        處理 PO 資料

        Args:
            parent_context: 父 context（用於獲取共享變數）
            po_data: PO 原始資料

        Returns:
            處理後的 PO DataFrame
        """
        try:
            # 創建 sub-context
            sub_context = ProcessingContext()
            sub_context.update_data(po_data.copy())

            # 複製必要變數
            file_date = parent_context.metadata.processing_date
            sub_context.set_variable('file_date', file_date)

            # 複製前期底稿
            prev_po = parent_context.get_auxiliary_data('procurement_previous_po')
            if prev_po is not None:
                sub_context.set_auxiliary_data('procurement_previous', prev_po)

            # 定義處理步驟
            steps = [
                ColumnInitializationStep(status_column="PO狀態"),
                ProcurementPreviousMappingStep(),
                DateLogicStep(),
                SPTProcurementStatusEvaluationStep(status_column="PO狀態"),
            ]

            # 執行步驟
            for step in steps:
                self.logger.debug(f"Executing PO sub-step: {step.name}")
                result = await step.execute(sub_context)

                if result.status == StepStatus.FAILED:
                    self.logger.error(f"PO sub-step failed: {step.name} - {result.message}")
                    return None

            return sub_context.data

        except Exception as e:
            self.logger.error(f"PO processing error: {str(e)}", exc_info=True)
            return None

    async def _process_pr_data(
        self,
        parent_context: ProcessingContext,
        pr_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        處理 PR 資料

        Args:
            parent_context: 父 context
            pr_data: PR 原始資料

        Returns:
            處理後的 PR DataFrame
        """
        try:
            # 創建 sub-context
            sub_context = ProcessingContext()
            sub_context.update_data(pr_data.copy())

            # 複製必要變數
            file_date = parent_context.metadata.processing_date
            sub_context.set_variable('file_date', file_date)

            # 複製前期底稿
            prev_pr = parent_context.get_auxiliary_data('procurement_previous_pr')
            if prev_pr is not None:
                sub_context.set_auxiliary_data('procurement_previous', prev_pr)

            # 定義處理步驟
            steps = [
                ColumnInitializationStep(status_column="PR狀態"),
                ProcurementPreviousMappingStep(),
                DateLogicStep(),
                SPTProcurementStatusEvaluationStep(status_column="PR狀態"),
            ]

            # 執行步驟
            for step in steps:
                self.logger.debug(f"Executing PR sub-step: {step.name}")
                result = await step.execute(sub_context)

                if result.status == StepStatus.FAILED:
                    self.logger.error(f"PR sub-step failed: {step.name} - {result.message}")
                    return None

            return sub_context.data

        except Exception as e:
            self.logger.error(f"PR processing error: {str(e)}", exc_info=True)
            return None

    def _generate_processing_summary(self, summary: Dict) -> str:
        """生成處理摘要報告"""
        lines = ["=" * 60, "Combined Procurement Processing Summary", "=" * 60]

        # PO 處理結果
        lines.append(f"PO Processing: {'✓ Success' if summary['po_processed'] else '✗ Failed'}")
        if summary['po_processed']:
            lines.append(f"  - Final rows: {summary['po_final_rows']}")
            if summary['po_status_distribution']:
                lines.append("  - Status distribution:")
                for status, count in summary['po_status_distribution'].items():
                    lines.append(f"    - {status}: {count}")

        # PR 處理結果
        lines.append(f"PR Processing: {'✓ Success' if summary['pr_processed'] else '✗ Failed'}")
        if summary['pr_processed']:
            lines.append(f"  - Final rows: {summary['pr_final_rows']}")
            if summary['pr_status_distribution']:
                lines.append("  - Status distribution:")
                for status, count in summary['pr_status_distribution'].items():
                    lines.append(f"    - {status}: {count}")

        lines.append("=" * 60)
        return "\n".join(lines)

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        # 檢查至少有 po_data 或 pr_data
        po_data = context.get_auxiliary_data('po_data')
        pr_data = context.get_auxiliary_data('pr_data')

        if (po_data is None or po_data.empty) and (pr_data is None or pr_data.empty):
            self.logger.error("Neither PO nor PR data available")
            return False

        return True
