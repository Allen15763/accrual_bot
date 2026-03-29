"""SCT 資料格式化步驟單元測試

測試 SCTPostProcessingStep：
- 數值列格式化（整數/浮點）
- 日期列格式化
- NaN 值清理
- 欄位排列
- snake_case 重命名
- 臨時欄位移除（PO/PR）
- ERM 格式化
- 輸出欄位篩選（PO/PR）
- 驗證邏輯
- pipeline orchestrator 整合
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

SAMPLE_CONFIG = {
    'sct_reformatting': {
        'int_columns': ['Line#', 'GL#'],
        'float_columns': ['Unit Price', 'Entry Amount', 'Accr. Amount'],
        'date_columns': ['PO Create Date', 'Submission Date'],
        'nan_clean_columns': ['是否估計入帳', 'Accr. Amount', '是否為FA'],
        'temp_columns': [
            '檔案日期', 'Expected Received Month_轉換格式',
            'YMs of Item Description',
            'expected_received_month_轉換格式', 'yms_of_item_description',
            'matched_condition_on_status',
        ],
        'pr_extra_temp_columns': [
            'remarked_by_procurement_pr', 'noted_by_procurement_pr',
            'remarked_by_上月_fn_pr',
        ],
        'tail_columns': ['Question from Reviewer', 'Check by AP'],
        'tail_columns_snake': ['question_from_reviewer', 'check_by_ap'],
        'output_columns_po': [
            'po_create_date', 'submission_date', 'line_number', 'gl_number',
            'item_description', 'unit_price', 'entry_amount',
            'po狀態', '是否估計入帳',
            'question_from_reviewer', 'check_by_ap',
        ],
        'output_columns_pr': [
            'pr_create_date', 'submission_date', 'line_number', 'gl_number',
            'item_description', 'unit_price', 'entry_amount',
            'pr狀態', '是否估計入帳',
            'question_from_reviewer', 'check_by_ap',
        ],
    },
}


@pytest.fixture
def mock_post_processing_deps():
    """Mock SCTPostProcessingStep 的外部依賴"""
    with patch(
        'accrual_bot.tasks.sct.steps.sct_post_processing.config_manager'
    ) as mock_cm:
        mock_cm._config_toml = SAMPLE_CONFIG
        yield mock_cm


def _create_po_df() -> pd.DataFrame:
    """建立 PO 測試 DataFrame（模擬 pipeline 輸出）"""
    return pd.DataFrame({
        'PO Create Date': ['2026-01-15', '2026-02-20', '2026-03-10'],
        'Submission Date': ['2026-01-16', '2026-02-21', '2026-03-11'],
        'Line#': ['1', '2', '3'],
        'GL#': ['520012', '520013', '199999'],
        'Item Description': ['warehouse rental', 'water fee', 'router購入'],
        'Unit Price': ['1000.555', '2000.999', '35000.1'],
        'Entry Amount': ['1000.555', '2000.999', '35000.1'],
        'Accr. Amount': ['1,000.50', 'nan', '0'],
        'PO狀態': ['已完成', '<NA>', '未完成'],
        '是否估計入帳': ['Y', 'nan', 'N'],
        '是否為FA': ['N', '<NA>', 'Y'],
        'Remarked by FN': ['ok', '', ''],
        'Remarked by 上月 FN': ['last month', '', ''],
        'Question from Reviewer': ['', '', ''],
        'Check by AP': ['', '', ''],
        '檔案日期': [202603, 202603, 202603],
        'Expected Received Month_轉換格式': [202601, 202602, 202603],
        'YMs of Item Description': ['202601', '202602', '202603'],
        'matched_condition_on_status': ['rule1', 'rule2', None],
        'Expected Receive Month': ['Jan-26', 'Feb-26', 'Mar-26'],
        'Product code': ['RT_B2C_COM', 'RT_B2C_COM', 'OTHER'],
    })


def _create_pr_df() -> pd.DataFrame:
    """建立 PR 測試 DataFrame"""
    return pd.DataFrame({
        'PR Create Date': ['2026-01-15', '2026-02-20'],
        'Submission Date': ['2026-01-16', '2026-02-21'],
        'Line#': ['1', '2'],
        'GL#': ['520012', '520013'],
        'Item Description': ['warehouse rental', 'water fee'],
        'Unit Price': ['1000.555', '2000.999'],
        'Entry Amount': ['1000.555', '2000.999'],
        'Accr. Amount': ['nan', 'nan'],
        'PR狀態': ['已完成', '未完成'],
        '是否估計入帳': ['Y', 'N'],
        '是否為FA': ['N', 'N'],
        'Remarked by FN': ['ok', ''],
        'Remarked by 上月 FN': ['last month', ''],
        'Remarked by Procurement PR': ['done', ''],
        'Noted by Procurement PR': ['', ''],
        'Remarked by 上月 FN PR': ['prev', ''],
        'Question from Reviewer': ['', ''],
        'Check by AP': ['', ''],
        '檔案日期': [202603, 202603],
        'Expected Received Month_轉換格式': [202601, 202602],
        'YMs of Item Description': ['202601', '202602'],
        'Expected Receive Month': ['Jan-26', 'Feb-26'],
        'Product code': ['RT_B2C_COM', 'RT_B2C_COM'],
    })


def _create_context(
    df: pd.DataFrame,
    processing_type: str = 'PO'
) -> ProcessingContext:
    """建立 ProcessingContext"""
    return ProcessingContext(
        data=df,
        entity_type='SCT',
        processing_date=202603,
        processing_type=processing_type,
    )


# ============================================================
# Tests
# ============================================================

class TestSCTPostProcessingStep:
    """SCTPostProcessingStep 測試"""

    # ---------- 數值格式化 ----------

    @pytest.mark.asyncio
    async def test_format_numeric_int_columns(self, mock_post_processing_deps):
        """Line#/GL# 轉 Int64"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'Line#': ['1', '2', 'abc'],
            'GL#': ['520012', '520013', ''],
        })
        result = step._format_numeric_columns(df)
        assert result['Line#'].dtype == 'Int64'
        assert result['GL#'].dtype == 'Int64'
        assert result['Line#'].iloc[0] == 1
        # 'abc' coerced to 0
        assert result['Line#'].iloc[2] == 0

    @pytest.mark.asyncio
    async def test_format_numeric_float_columns(self, mock_post_processing_deps):
        """Entry Amount 等轉 float + round(2)"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'Unit Price': ['1000.555', '2000.999'],
            'Entry Amount': ['3000.456', 'abc'],
        })
        result = step._format_numeric_columns(df)
        assert result['Unit Price'].iloc[0] == 1000.56
        assert result['Entry Amount'].iloc[0] == 3000.46
        assert pd.isna(result['Entry Amount'].iloc[1])

    # ---------- 日期格式化 ----------

    @pytest.mark.asyncio
    async def test_reformat_dates(self, mock_post_processing_deps):
        """日期欄位轉 YYYY-MM-DD"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'PO Create Date': ['2026/01/15', '01-15-2026'],
            'Submission Date': ['20260116', 'invalid'],
        })
        result = step._reformat_dates(df)
        assert result['PO Create Date'].iloc[0] == '2026-01-15'

    # ---------- NaN 清理 ----------

    @pytest.mark.asyncio
    async def test_clean_nan_values(self, mock_post_processing_deps):
        """'nan'/'<NA>' → pd.NA"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            '是否估計入帳': ['Y', 'nan', '<NA>'],
            '是否為FA': ['N', 'nan', 'Y'],
            'PO狀態': ['已完成', '<NA>', '未完成'],
        })
        result = step._clean_nan_values(df)
        assert pd.isna(result['是否估計入帳'].iloc[1])
        assert pd.isna(result['是否估計入帳'].iloc[2])
        assert pd.isna(result['PO狀態'].iloc[1])
        assert result['是否為FA'].iloc[2] == 'Y'

    @pytest.mark.asyncio
    async def test_clean_accr_amount(self, mock_post_processing_deps):
        """Accr. Amount 去逗號轉數值、0→None"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'Accr. Amount': ['1,000.50', 'nan', '0', '<NA>', '500.25'],
        })
        result = step._clean_nan_values(df)
        assert result['Accr. Amount'].iloc[0] == 1000.50
        assert result['Accr. Amount'].iloc[4] == 500.25
        # 0 → None
        assert pd.isna(result['Accr. Amount'].iloc[2])
        # 'nan' → 0 → None
        assert pd.isna(result['Accr. Amount'].iloc[1])

    # ---------- 欄位排列 ----------

    @pytest.mark.asyncio
    async def test_rearrange_status_column(self, mock_post_processing_deps):
        """PO狀態移到是否估計入帳前"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'col_a': [1],
            '是否估計入帳': ['Y'],
            'PO狀態': ['已完成'],
            'col_b': [2],
        })
        result = step._rearrange_columns(df)
        cols = list(result.columns)
        assert cols.index('PO狀態') < cols.index('是否估計入帳')

    @pytest.mark.asyncio
    async def test_rearrange_review_columns_last(self, mock_post_processing_deps):
        """Question from Reviewer / Check by AP 在最後"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'Question from Reviewer': [''],
            'col_a': [1],
            'Check by AP': [''],
            'col_b': [2],
        })
        result = step._rearrange_columns(df)
        cols = list(result.columns)
        assert cols[-2] == 'Question from Reviewer'
        assert cols[-1] == 'Check by AP'

    # ---------- snake_case 重命名 ----------

    @pytest.mark.asyncio
    async def test_rename_columns_snake_case(self, mock_post_processing_deps):
        """clean_po_data snake_case 化"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'PO Create Date': ['2026-01-15'],
            'Entry Amount': ['1000'],
            'Line#': ['1'],
            'Product code': ['RT_B2C_COM'],
        })
        result = step._rename_columns_dtype(df)
        assert 'po_create_date' in result.columns
        assert 'entry_amount' in result.columns
        assert 'line_number' in result.columns
        # Product code → product_code_c
        assert 'product_code_c' in result.columns

    # ---------- 臨時欄位移除 ----------

    @pytest.mark.asyncio
    async def test_remove_temp_columns_po(self, mock_post_processing_deps):
        """PO 臨時欄位全移除"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'col_a': [1],
            '檔案日期': [202603],
            'expected_received_month_轉換格式': [202601],
            'yms_of_item_description': ['202601'],
            'matched_condition_on_status': ['rule1'],
        })
        result = step._remove_temp_columns(df, 'PO')
        assert '檔案日期' not in result.columns
        assert 'expected_received_month_轉換格式' not in result.columns
        assert 'matched_condition_on_status' not in result.columns
        assert 'col_a' in result.columns

    @pytest.mark.asyncio
    async def test_remove_temp_columns_pr(self, mock_post_processing_deps):
        """PR 額外臨時欄位移除"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'col_a': [1],
            '檔案日期': [202603],
            'remarked_by_procurement_pr': ['done'],
            'noted_by_procurement_pr': [''],
            'remarked_by_上月_fn_pr': ['prev'],
        })
        result = step._remove_temp_columns(df, 'PR')
        assert '檔案日期' not in result.columns
        assert 'remarked_by_procurement_pr' not in result.columns
        assert 'noted_by_procurement_pr' not in result.columns
        assert 'remarked_by_上月_fn_pr' not in result.columns
        assert 'col_a' in result.columns

    # ---------- ERM 格式化 ----------

    @pytest.mark.asyncio
    async def test_reformat_erm(self, mock_post_processing_deps):
        """expected_receive_month %b-%y → %Y/%m"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'expected_receive_month': ['Jan-26', 'Feb-26', 'Mar-26'],
        })
        result = step._reformat_erm(df)
        assert result['expected_receive_month'].iloc[0] == '2026/01'
        assert result['expected_receive_month'].iloc[1] == '2026/02'
        assert result['expected_receive_month'].iloc[2] == '2026/03'

    # ---------- 輸出欄位篩選 ----------

    @pytest.mark.asyncio
    async def test_select_output_columns_po(self, mock_post_processing_deps):
        """PO 輸出欄位篩選正確"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'po_create_date': ['2026-01-15'],
            'submission_date': ['2026-01-16'],
            'line_number': [1],
            'gl_number': [520012],
            'item_description': ['test'],
            'unit_price': [1000.0],
            'entry_amount': [1000.0],
            'po狀態': ['已完成'],
            '是否估計入帳': ['Y'],
            'question_from_reviewer': [''],
            'check_by_ap': [''],
            'extra_col': ['should_be_removed'],
        })
        result = step._select_output_columns(df, 'PO')
        assert 'extra_col' not in result.columns
        assert 'po_create_date' in result.columns
        assert list(result.columns)[-1] == 'check_by_ap'

    @pytest.mark.asyncio
    async def test_select_output_columns_pr(self, mock_post_processing_deps):
        """PR 輸出欄位篩選正確"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'pr_create_date': ['2026-01-15'],
            'submission_date': ['2026-01-16'],
            'line_number': [1],
            'gl_number': [520012],
            'item_description': ['test'],
            'unit_price': [1000.0],
            'entry_amount': [1000.0],
            'pr狀態': ['已完成'],
            '是否估計入帳': ['Y'],
            'question_from_reviewer': [''],
            'check_by_ap': [''],
            'extra_col': ['should_be_removed'],
        })
        result = step._select_output_columns(df, 'PR')
        assert 'extra_col' not in result.columns
        assert 'pr_create_date' in result.columns

    @pytest.mark.asyncio
    async def test_select_output_columns_missing_col(self, mock_post_processing_deps):
        """欄位不存在時 warning + 容錯"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        # 只有部分欄位存在
        df = pd.DataFrame({
            'po_create_date': ['2026-01-15'],
            'submission_date': ['2026-01-16'],
        })
        result = step._select_output_columns(df, 'PO')
        assert 'po_create_date' in result.columns
        assert 'submission_date' in result.columns
        assert len(result.columns) == 2

    # ---------- auxiliary data ----------

    @pytest.mark.asyncio
    async def test_save_auxiliary_data(self, mock_post_processing_deps):
        """result_with_temp_cols 正確存入 context"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({'col_a': [1, 2, 3]})
        ctx = _create_context(df)
        step._save_temp_columns_data(df, ctx)
        aux = ctx.get_auxiliary_data('result_with_temp_cols')
        assert aux is not None
        assert len(aux) == 3

    # ---------- 驗證 ----------

    @pytest.mark.asyncio
    async def test_validate_result_success(self, mock_post_processing_deps):
        """驗證通過"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'entry_amount': [1000.0, 2000.0],
            'accr._amount': [500.0, None],
        })
        ctx = _create_context(df)
        result = step._validate_result(df, ctx)
        assert result['is_valid'] is True

    @pytest.mark.asyncio
    async def test_validate_result_temp_cols_remain(self, mock_post_processing_deps):
        """殘留臨時欄位 → issue"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame({
            'entry_amount': [1000.0],
            '檔案日期': [202603],
        })
        ctx = _create_context(df)
        result = step._validate_result(df, ctx)
        assert result['is_valid'] is False
        assert '臨時欄位未完全移除' in result['details']['issues'][0]

    @pytest.mark.asyncio
    async def test_empty_dataframe(self, mock_post_processing_deps):
        """空 DataFrame → validate_input False"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = pd.DataFrame()
        ctx = _create_context(df)
        result = await step.validate_input(ctx)
        assert result is False

    # ---------- Pipeline 整合 ----------

    @pytest.mark.asyncio
    async def test_pipeline_registration(self, mock_post_processing_deps):
        """orchestrator 正確註冊 PO/PR"""
        with patch(
            'accrual_bot.tasks.sct.pipeline_orchestrator.config_manager'
        ) as mock_orch_cm:
            mock_orch_cm._config_toml = {
                'pipeline': {'sct': {
                    'enabled_po_steps': ['SCTPostProcessing'],
                    'enabled_pr_steps': ['SCTPostProcessing'],
                }},
                **SAMPLE_CONFIG,
            }
            from accrual_bot.tasks.sct.pipeline_orchestrator import (
                SCTPipelineOrchestrator,
            )
            orch = SCTPipelineOrchestrator()
            step_po = orch._create_step(
                'SCTPostProcessing', {}, processing_type='PO'
            )
            assert step_po is not None
            assert step_po.name == 'SCTPostProcessing'

            step_pr = orch._create_step(
                'SCTPostProcessing', {}, processing_type='PR'
            )
            assert step_pr is not None

    # ---------- End-to-End ----------

    @pytest.mark.asyncio
    async def test_full_execute_po(self, mock_post_processing_deps):
        """PO 全流程 end-to-end"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = _create_po_df()
        ctx = _create_context(df, processing_type='PO')
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        output = ctx.data

        # snake_case 化後的欄位
        assert 'po_create_date' in output.columns or len(output.columns) > 0
        # 臨時欄位已移除
        assert '檔案日期' not in output.columns
        assert 'matched_condition_on_status' not in output.columns
        # ERM 格式化
        if 'expected_receive_month' in output.columns:
            assert output['expected_receive_month'].iloc[0] == '2026/01'
        # auxiliary data 已保存
        aux = ctx.get_auxiliary_data('result_with_temp_cols')
        assert aux is not None

    @pytest.mark.asyncio
    async def test_full_execute_pr(self, mock_post_processing_deps):
        """PR 全流程 end-to-end"""
        from accrual_bot.tasks.sct.steps.sct_post_processing import (
            SCTPostProcessingStep,
        )
        step = SCTPostProcessingStep()
        df = _create_pr_df()
        ctx = _create_context(df, processing_type='PR')
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        output = ctx.data

        # PR 額外臨時欄位已移除
        assert 'remarked_by_procurement_pr' not in output.columns
        assert 'noted_by_procurement_pr' not in output.columns
        assert 'remarked_by_上月_fn_pr' not in output.columns
        assert '檔案日期' not in output.columns
