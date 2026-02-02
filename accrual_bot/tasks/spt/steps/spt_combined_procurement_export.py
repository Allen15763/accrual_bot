"""
SPT Combined Procurement Export Step

COMBINED 模式匯出步驟 - 將 PO 和 PR 結果輸出到同一個 Excel
"""

from typing import Dict, Any
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


class CombinedProcurementExportStep(PipelineStep):
    """
    COMBINED 模式匯出步驟

    功能:
    1. 從 auxiliary_data 讀取 PO 和 PR 處理結果
    2. 輸出到同一個 Excel 檔案的兩個 sheets (PO, PR)
    3. 提供詳細的匯出統計資訊

    設計原則:
    - 不影響現有的單一模式匯出步驟
    - 支援自訂輸出路徑和檔名
    - 提供重試機制
    """

    def __init__(
        self,
        name: str = "CombinedProcurementExport",
        output_dir: str = "output",
        filename_template: str = "{YYYYMM}_PROCUREMENT_COMBINED.xlsx",
        include_index: bool = False,
        retry_count: int = 3,
        **kwargs
    ):
        """
        初始化匯出步驟

        Args:
            name: 步驟名稱
            output_dir: 輸出目錄
            filename_template: 檔名模板（支援 {YYYYMM} 變數）
            include_index: 是否包含 index
            retry_count: 重試次數
            **kwargs: 其他參數
        """
        super().__init__(name, description="Export PO and PR to single Excel file", **kwargs)
        self.output_dir = output_dir
        self.filename_template = filename_template
        self.include_index = include_index
        self.retry_count = retry_count

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行匯出"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            export_summary = {
                'po_exported': False,
                'pr_exported': False,
                'po_rows': 0,
                'pr_rows': 0,
                'output_path': None,
                'file_size': 0
            }

            # 1. 從 auxiliary_data 讀取處理結果
            po_result = context.get_auxiliary_data('po_result')
            pr_result = context.get_auxiliary_data('pr_result')

            if po_result is None and pr_result is None:
                raise ValueError("No PO or PR result data to export")

            # 2. 準備輸出路徑
            output_path = self._prepare_output_path(context)
            export_summary['output_path'] = str(output_path)

            # 3. 創建 Excel Writer
            self.logger.info(f"Exporting to: {output_path}")

            # 使用重試機制
            success = False
            last_error = None

            for attempt in range(self.retry_count):
                try:
                    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                        # 匯出 PO sheet
                        if po_result is not None and not po_result.empty:
                            po_result.to_excel(
                                writer,
                                sheet_name='PO',
                                index=self.include_index
                            )
                            export_summary['po_exported'] = True
                            export_summary['po_rows'] = len(po_result)
                            self.logger.info(f"✓ Exported PO sheet: {len(po_result)} rows")

                        # 匯出 PR sheet
                        if pr_result is not None and not pr_result.empty:
                            pr_result.to_excel(
                                writer,
                                sheet_name='PR',
                                index=self.include_index
                            )
                            export_summary['pr_exported'] = True
                            export_summary['pr_rows'] = len(pr_result)
                            self.logger.info(f"✓ Exported PR sheet: {len(pr_result)} rows")

                    success = True
                    break

                except PermissionError as e:
                    last_error = e
                    self.logger.warning(
                        f"Attempt {attempt + 1}/{self.retry_count} failed: {str(e)}"
                    )
                    if attempt < self.retry_count - 1:
                        # 嘗試替代檔名
                        output_path = self._prepare_output_path(context, suffix=f"_{attempt + 1}")
                        self.logger.info(f"Retrying with alternative filename: {output_path}")
                    else:
                        raise

                except Exception as e:
                    last_error = e
                    self.logger.error(f"Export attempt {attempt + 1} failed: {str(e)}")
                    if attempt == self.retry_count - 1:
                        raise

            if not success:
                raise last_error

            # 4. 獲取檔案大小
            if output_path.exists():
                export_summary['file_size'] = output_path.stat().st_size
                self.logger.info(
                    f"✓ File size: {export_summary['file_size'] / 1024:.2f} KB"
                )

            duration = time.time() - start_time

            # 生成摘要訊息
            summary_msg = self._generate_export_summary(export_summary)
            self.logger.info(f"\n{summary_msg}")

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Exported to {output_path.name}",
                duration=duration,
                metadata=export_summary
            )

        except Exception as e:
            self.logger.error(f"Combined export failed: {str(e)}", exc_info=True)
            context.add_error(f"Export failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    def _prepare_output_path(self, context: ProcessingContext, suffix: str = "") -> Path:
        """
        準備輸出路徑

        Args:
            context: ProcessingContext
            suffix: 檔名後綴（用於重試時產生不同檔名）

        Returns:
            Path: 輸出檔案路徑
        """
        # 獲取日期
        file_date = context.metadata.processing_date
        if file_date:
            yyyymm = str(file_date)
        else:
            yyyymm = datetime.now().strftime('%Y%m')

        # 替換模板變數
        filename = self.filename_template.replace('{YYYYMM}', yyyymm)

        # 添加後綴（如果有）
        if suffix:
            name_parts = filename.rsplit('.', 1)
            if len(name_parts) == 2:
                filename = f"{name_parts[0]}{suffix}.{name_parts[1]}"
            else:
                filename = f"{filename}{suffix}"

        # 創建輸出目錄
        output_dir_path = Path(self.output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)

        return output_dir_path / filename

    def _generate_export_summary(self, summary: Dict) -> str:
        """生成匯出摘要報告"""
        lines = ["=" * 60, "Combined Procurement Export Summary", "=" * 60]

        # 輸出路徑
        lines.append(f"Output Path: {summary['output_path']}")
        lines.append(f"File Size:   {summary['file_size'] / 1024:.2f} KB")
        lines.append("")

        # PO 匯出
        lines.append(f"PO Sheet:    {'✓ Exported' if summary['po_exported'] else '✗ Not Exported'}")
        if summary['po_exported']:
            lines.append(f"  - Rows: {summary['po_rows']}")

        # PR 匯出
        lines.append(f"PR Sheet:    {'✓ Exported' if summary['pr_exported'] else '✗ Not Exported'}")
        if summary['pr_exported']:
            lines.append(f"  - Rows: {summary['pr_rows']}")

        lines.append("=" * 60)
        return "\n".join(lines)

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        # 檢查至少有 po_result 或 pr_result
        po_result = context.get_auxiliary_data('po_result')
        pr_result = context.get_auxiliary_data('pr_result')

        if po_result is None and pr_result is None:
            self.logger.error("No result data to export")
            return False

        return True
