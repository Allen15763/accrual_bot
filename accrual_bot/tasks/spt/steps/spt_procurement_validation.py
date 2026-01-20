"""
SPT Procurement Previous Workpaper Validation Step

採購前期底稿格式驗證步驟
"""

from typing import Dict, List, Optional
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.helpers.column_utils import ColumnResolver


class ProcurementPreviousValidationStep(PipelineStep):
    """
    採購前期底稿格式驗證步驟

    功能:
    1. 驗證前期底稿為 Excel 檔案
    2. 驗證包含 PO 和 PR 兩個 sheets
    3. 驗證必要欄位存在（使用 ColumnResolver 靈活匹配）

    設計原則:
    - 使用 ColumnResolver 進行欄位匹配（支援不同命名格式）
    - 提供詳細的驗證錯誤訊息
    - 不影響現有功能（驗證失敗時返回 SKIPPED，而非 FAILED）
    """

    def __init__(
        self,
        name: str = "ProcurementPreviousValidation",
        strict_mode: bool = False,
        **kwargs
    ):
        """
        初始化驗證步驟

        Args:
            name: 步驟名稱
            strict_mode: 嚴格模式（True: 驗證失敗則 FAILED，False: 驗證失敗則 SKIPPED）
            **kwargs: 其他參數
        """
        super().__init__(name, description="Validate procurement previous workpaper format", **kwargs)
        self.strict_mode = strict_mode

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行格式驗證"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            # 從 context 獲取前期底稿資料
            prev_df_po = context.get_auxiliary_data('procurement_previous_po')
            prev_df_pr = context.get_auxiliary_data('procurement_previous_pr')
            file_path = context.get_variable('procurement_previous_path')

            # 如果沒有前期底稿，跳過驗證
            if prev_df_po is None and prev_df_pr is None:
                self.logger.info("No procurement previous workpaper found, skipping validation")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    message="No procurement previous workpaper",
                    duration=time.time() - start_time
                )

            validation_results = {
                'file_format_valid': False,
                'po_sheet_exists': False,
                'pr_sheet_exists': False,
                'po_columns_valid': False,
                'pr_columns_valid': False,
                'errors': [],
                'warnings': []
            }

            # 1. 驗證檔案格式
            if file_path:
                file_ext = Path(file_path).suffix.lower()
                if file_ext in ['.xlsx', '.xls']:
                    validation_results['file_format_valid'] = True
                    self.logger.info(f"✓ File format valid: {file_ext}")
                else:
                    validation_results['errors'].append(
                        f"Invalid file format: {file_ext}. Expected .xlsx or .xls"
                    )
                    self.logger.error(f"✗ Invalid file format: {file_ext}")

            # 2. 驗證 sheets 存在
            if prev_df_po is not None:
                validation_results['po_sheet_exists'] = True
                self.logger.info("✓ PO sheet exists")
            else:
                validation_results['warnings'].append("PO sheet not found")
                self.logger.warning("⚠ PO sheet not found")

            if prev_df_pr is not None:
                validation_results['pr_sheet_exists'] = True
                self.logger.info("✓ PR sheet exists")
            else:
                validation_results['warnings'].append("PR sheet not found")
                self.logger.warning("⚠ PR sheet not found")

            # 3. 驗證必要欄位
            required_columns = {
                'po': ['po_line', 'remarked_by_procurement'],
                'pr': ['pr_line', 'remarked_by_procurement']
            }

            # 驗證 PO sheet 欄位
            if prev_df_po is not None:
                po_validation = self._validate_required_columns(
                    prev_df_po, required_columns['po'], 'PO'
                )
                validation_results['po_columns_valid'] = po_validation['valid']
                validation_results['errors'].extend(po_validation['errors'])
                validation_results['warnings'].extend(po_validation['warnings'])

            # 驗證 PR sheet 欄位
            if prev_df_pr is not None:
                pr_validation = self._validate_required_columns(
                    prev_df_pr, required_columns['pr'], 'PR'
                )
                validation_results['pr_columns_valid'] = pr_validation['valid']
                validation_results['errors'].extend(pr_validation['errors'])
                validation_results['warnings'].extend(pr_validation['warnings'])

            # 判斷整體驗證結果
            has_errors = len(validation_results['errors']) > 0
            has_warnings = len(validation_results['warnings']) > 0

            duration = time.time() - start_time

            # 生成驗證報告
            validation_summary = self._generate_validation_summary(validation_results)
            self.logger.info(f"\n{validation_summary}")

            # 決定返回狀態
            if has_errors:
                if self.strict_mode:
                    # 嚴格模式：驗證失敗則返回 FAILED
                    return StepResult(
                        step_name=self.name,
                        status=StepStatus.FAILED,
                        message=f"Validation failed: {len(validation_results['errors'])} error(s)",
                        duration=duration,
                        metadata=validation_results
                    )
                else:
                    # 寬鬆模式：驗證失敗則返回 SKIPPED（不中斷 pipeline）
                    self.logger.warning(
                        f"Validation failed with {len(validation_results['errors'])} error(s), "
                        "but continuing due to non-strict mode"
                    )
                    return StepResult(
                        step_name=self.name,
                        status=StepStatus.SKIPPED,
                        message=f"Validation failed but skipped (non-strict mode)",
                        duration=duration,
                        metadata=validation_results
                    )

            # 驗證成功
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Validation passed ({len(validation_results['warnings'])} warning(s))",
                duration=duration,
                metadata=validation_results
            )

        except Exception as e:
            self.logger.error(f"Validation step failed: {str(e)}", exc_info=True)
            context.add_error(f"Validation failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    def _validate_required_columns(
        self,
        df: pd.DataFrame,
        required_canonical_columns: List[str],
        sheet_name: str
    ) -> Dict:
        """
        驗證必要欄位是否存在（使用 ColumnResolver）

        Args:
            df: DataFrame
            required_canonical_columns: 必要欄位的 canonical names
            sheet_name: Sheet 名稱（用於錯誤訊息）

        Returns:
            驗證結果字典
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'resolved_columns': {}
        }

        for canonical_name in required_canonical_columns:
            resolved_column = ColumnResolver.resolve(df, canonical_name)

            if resolved_column is None:
                result['valid'] = False
                result['errors'].append(
                    f"{sheet_name} sheet: Required column '{canonical_name}' not found"
                )
                self.logger.error(f"✗ {sheet_name}: Missing column '{canonical_name}'")
            else:
                result['resolved_columns'][canonical_name] = resolved_column
                self.logger.debug(
                    f"✓ {sheet_name}: Resolved '{canonical_name}' -> '{resolved_column}'"
                )

        return result

    def _generate_validation_summary(self, results: Dict) -> str:
        """生成驗證報告摘要"""
        lines = ["=" * 60, "Procurement Previous Workpaper Validation Report", "=" * 60]

        # 檔案格式
        lines.append(f"File Format: {'✓ Valid' if results['file_format_valid'] else '✗ Invalid'}")

        # Sheets 存在性
        lines.append(f"PO Sheet:    {'✓ Exists' if results['po_sheet_exists'] else '✗ Missing'}")
        lines.append(f"PR Sheet:    {'✓ Exists' if results['pr_sheet_exists'] else '✗ Missing'}")

        # 欄位驗證
        if results['po_sheet_exists']:
            lines.append(
                f"PO Columns:  {'✓ Valid' if results['po_columns_valid'] else '✗ Invalid'}"
            )
        if results['pr_sheet_exists']:
            lines.append(
                f"PR Columns:  {'✓ Valid' if results['pr_columns_valid'] else '✗ Invalid'}"
            )

        # 錯誤訊息
        if results['errors']:
            lines.append(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors']:
                lines.append(f"  - {error}")

        # 警告訊息
        if results['warnings']:
            lines.append(f"\nWarnings ({len(results['warnings'])}):")
            for warning in results['warnings']:
                lines.append(f"  - {warning}")

        lines.append("=" * 60)
        return "\n".join(lines)

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入（總是返回 True，因為驗證邏輯在 execute 中）"""
        return True
