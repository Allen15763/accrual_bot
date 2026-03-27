"""
SPTPostProcessingStep 單元測試

測試 SPTPostProcessingStep 的核心功能：
- 步驟初始化
- _format_numeric_columns() 數值列格式化
- _clean_nan_values() NaN 值清理
- _rearrange_columns() 欄位重新排列
- _process_data() 主要處理流程
- _remove_temp_columns() 臨時欄位移除
- _reformat_erm() ERM 格式化
- _rearrange_reviewer_col() reviewer 欄位排列
- _validate_result() 結果驗證
- execute() 完整流程
- 邊界情況：空 DataFrame、缺少欄位
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.tasks.spt.steps.spt_steps import SPTPostProcessingStep


# ============================================================
# 測試用資料建構
# ============================================================

def _create_spt_post_processing_df(n: int = 5) -> pd.DataFrame:
    """建立 SPT 後處理測試用 DataFrame"""
    return pd.DataFrame({
        'PO#': [f'PO{i:03d}' for i in range(n)],
        'Line#': [str(i + 1) for i in range(n)],
        'GL#': [str(300000 + i) for i in range(n)],
        'Item Description': [f'Test Item {i}' for i in range(n)],
        'Unit Price': ['100.50'] * n,
        'Entry Amount': ['10000.123'] * n,
        'Entry Invoiced Amount': ['5000.456'] * n,
        'Entry Billed Amount': ['3000.789'] * n,
        'Entry Prepay Amount': ['0'] * n,
        'PO Entry full invoiced status': ['0'] * n,
        'Accr. Amount': ['1000.50'] * n,
        'PO狀態': ['已完成(not_billed)'] * n,
        '是否估計入帳': ['Y'] * n,
        'PR Product Code Check': ['OK'] * n,
        'Product code': [f'PROD{i:03d}' for i in range(n)],
        'Region_c': ['TW'] * n,
        'Dep.': ['100'] * n,
        '是否為FA': ['N'] * n,
        'Creation Date': ['2025-01-15'] * n,
        'Expected Received Month': ['2025-12-01'] * n,
        'Last Update Date': ['2025-06-01'] * n,
        'Remarked by FN': [pd.NA] * n,
        'Remarked by 上月 FN': [pd.NA] * n,
        'Remarked by 上月 FN PR': [pd.NA] * n,
        'Noted by Procurement': [pd.NA] * n,
        'Remarked by Procurement PR': [pd.NA] * n,
        'Noted by Procurement PR': [pd.NA] * n,
        'Question from Reviewer': [pd.NA] * n,
        'Check by AP': [pd.NA] * n,
        'expected_receive_month': ['Dec-25'] * n,
        'previous_month_reviewed_by': [pd.NA] * n,
        'current_month_reviewed_by': [pd.NA] * n,
    })


def _create_spt_post_context(df: pd.DataFrame = None) -> ProcessingContext:
    """建立 SPT 後處理測試用 ProcessingContext"""
    if df is None:
        df = _create_spt_post_processing_df()
    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202512,
        processing_type='PO',
    )
    return ctx


# ============================================================
# SPTPostProcessingStep 初始化測試
# ============================================================

@pytest.mark.unit
class TestSPTPostProcessingStepInit:
    """SPTPostProcessingStep 初始化測試"""

    def test_default_name(self):
        """測試預設步驟名稱為 SPT_Data_Reformatting"""
        step = SPTPostProcessingStep()
        assert step.name == "SPT_Data_Reformatting"

    def test_custom_name(self):
        """測試自訂步驟名稱"""
        step = SPTPostProcessingStep(name="CustomPostProcessing")
        assert step.name == "CustomPostProcessing"

    def test_description(self):
        """測試步驟描述"""
        step = SPTPostProcessingStep()
        assert "SPT" in step.description


# ============================================================
# _format_numeric_columns 測試
# ============================================================

@pytest.mark.unit
class TestFormatNumericColumns:
    """_format_numeric_columns 數值列格式化測試"""

    def test_int_columns_formatted(self):
        """測試整數列 Line#, GL# 正確轉換為 Int64"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'Line#': ['1', '2', '3'],
            'GL#': ['300000', '300001', '300002'],
        })
        result = step._format_numeric_columns(df)
        assert result['Line#'].dtype == pd.Int64Dtype()
        assert result['GL#'].dtype == pd.Int64Dtype()

    def test_float_columns_rounded(self):
        """測試浮點列正確四捨五入到兩位小數"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'Unit Price': ['100.555', '200.444'],
            'Entry Amount': ['10000.126', '20000.999'],
        })
        result = step._format_numeric_columns(df)
        assert result['Unit Price'].iloc[0] == pytest.approx(100.56, abs=0.01)
        assert result['Entry Amount'].iloc[1] == pytest.approx(20001.0, abs=0.01)

    def test_missing_columns_skipped(self):
        """測試缺少的欄位不會導致錯誤"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        result = step._format_numeric_columns(df)
        assert 'other_col' in result.columns
        assert len(result) == 3

    def test_non_numeric_values_coerced(self):
        """測試非數值資料被轉換為 NaN 後填充為 0（整數列）"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'Line#': ['1', 'abc', '3'],
            'GL#': ['300000', None, '300002'],
        })
        result = step._format_numeric_columns(df)
        assert result['Line#'].iloc[1] == 0
        assert result['GL#'].iloc[1] == 0


# ============================================================
# _clean_nan_values 測試
# ============================================================

@pytest.mark.unit
class TestCleanNanValues:
    """_clean_nan_values NaN 值清理測試"""

    def test_string_nan_replaced(self):
        """測試字串 'nan' 被替換為 pd.NA"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'PO狀態': ['nan', '已完成', '<NA>'],
            '是否估計入帳': ['Y', 'nan', 'N'],
        })
        result = step._clean_nan_values(df)
        assert pd.isna(result['PO狀態'].iloc[0])
        assert pd.isna(result['PO狀態'].iloc[2])
        assert pd.isna(result['是否估計入帳'].iloc[1])

    def test_accr_amount_special_handling(self):
        """測試 Accr. Amount 的特殊清理邏輯（逗號移除、nan→0→None）"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'PO狀態': ['已完成'],
            'Accr. Amount': ['1,000.50'],
        })
        result = step._clean_nan_values(df)
        assert result['Accr. Amount'].iloc[0] == pytest.approx(1000.50, abs=0.01)

    def test_accr_amount_zero_becomes_none(self):
        """測試 Accr. Amount 值為 0 時轉為 None"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'PO狀態': ['已完成'],
            'Accr. Amount': ['0'],
        })
        result = step._clean_nan_values(df)
        assert result['Accr. Amount'].iloc[0] is None or pd.isna(result['Accr. Amount'].iloc[0])

    def test_missing_columns_skipped(self):
        """測試缺少要清理的欄位時不報錯"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'PO狀態': ['已完成'],
            'other_col': ['value'],
        })
        result = step._clean_nan_values(df)
        assert len(result) == 1


# ============================================================
# _rearrange_columns 測試
# ============================================================

@pytest.mark.unit
class TestRearrangeColumns:
    """_rearrange_columns 欄位重新排列測試"""

    def test_question_and_check_moved_to_end(self):
        """測試 Question from Reviewer 和 Check by AP 被移到最後"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'Question from Reviewer': ['Q1'],
            'Check by AP': ['C1'],
            'PO狀態': ['已完成'],
            '是否估計入帳': ['Y'],
            'col_a': ['a'],
        })
        result = step._rearrange_columns(df)
        cols = list(result.columns)
        assert cols[-2] == 'Question from Reviewer'
        assert cols[-1] == 'Check by AP'

    def test_status_column_before_accrual(self):
        """測試狀態欄位被移到是否估計入帳前面"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'col_a': ['a'],
            '是否估計入帳': ['Y'],
            'col_b': ['b'],
            'PO狀態': ['已完成'],
        })
        result = step._rearrange_columns(df)
        cols = list(result.columns)
        status_idx = cols.index('PO狀態')
        accrual_idx = cols.index('是否估計入帳')
        assert status_idx < accrual_idx

    def test_no_target_columns_no_error(self):
        """測試沒有目標欄位時不報錯"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'PO狀態': ['已完成'],
            'col_a': ['a'],
        })
        # 沒有 '是否估計入帳'，不應報錯
        result = step._rearrange_columns(df)
        assert len(result) == 1


# ============================================================
# _remove_temp_columns 測試
# ============================================================

@pytest.mark.unit
class TestRemoveTempColumns:
    """_remove_temp_columns 臨時欄位移除測試"""

    def test_po_temp_columns_removed(self):
        """測試 PO 類型的臨時欄位被正確移除"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'col_a': [1],
            '檔案日期': [202512],
            'Expected Received Month_轉換格式': [202512],
            'YMs of Item Description': ['202510,202512'],
            'PR Product Code Check': ['OK'],
        })
        result = step._remove_temp_columns(df, 'PO')
        assert '檔案日期' not in result.columns
        assert 'Expected Received Month_轉換格式' not in result.columns
        assert 'YMs of Item Description' not in result.columns
        assert 'PR Product Code Check' not in result.columns
        assert 'col_a' in result.columns

    def test_pr_extra_columns_removed(self):
        """測試 PR 類型額外的臨時欄位被移除"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'col_a': [1],
            'remarked_by_procurement_pr': ['note'],
            'noted_by_procurement_pr': ['note2'],
            'remarked_by_上月_fn_pr': ['note3'],
        })
        result = step._remove_temp_columns(df, 'PR')
        assert 'remarked_by_procurement_pr' not in result.columns
        assert 'noted_by_procurement_pr' not in result.columns
        assert 'remarked_by_上月_fn_pr' not in result.columns


# ============================================================
# _reformat_erm 測試
# ============================================================

@pytest.mark.unit
class TestReformatErm:
    """_reformat_erm ERM 格式化測試"""

    def test_erm_format_conversion(self):
        """測試 ERM 從 'MMM-YY' 轉為 'YYYY/MM' 格式"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'expected_receive_month': ['Dec-25', 'Jan-26', 'Feb-26'],
        })
        result = step._reformat_erm(df)
        assert result['expected_receive_month'].iloc[0] == '2025/12'
        assert result['expected_receive_month'].iloc[1] == '2026/01'

    def test_erm_missing_column_no_error(self):
        """測試缺少 expected_receive_month 欄位時不報錯"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({'col_a': [1, 2]})
        result = step._reformat_erm(df)
        assert 'col_a' in result.columns


# ============================================================
# _rearrange_reviewer_col 測試
# ============================================================

@pytest.mark.unit
class TestRearrangeReviewerCol:
    """_rearrange_reviewer_col reviewer 欄位排列測試"""

    def test_reviewer_cols_moved_to_end(self):
        """測試 reviewer 欄位被移到最後"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'previous_month_reviewed_by': ['A'],
            'current_month_reviewed_by': ['B'],
            'col_a': ['x'],
            'col_b': ['y'],
        })
        result = step._rearrange_reviewer_col(df)
        cols = list(result.columns)
        assert cols[-2] == 'previous_month_reviewed_by'
        assert cols[-1] == 'current_month_reviewed_by'

    def test_missing_reviewer_cols_skipped(self):
        """測試缺少 reviewer 欄位時跳過並不報錯"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({'col_a': ['x'], 'col_b': ['y']})
        result = step._rearrange_reviewer_col(df)
        assert list(result.columns) == ['col_a', 'col_b']


# ============================================================
# _validate_result 測試
# ============================================================

@pytest.mark.unit
class TestValidateResult:
    """_validate_result 結果驗證測試"""

    def test_valid_result(self):
        """測試正確數據通過驗證"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'Entry Amount': [1000.0, 2000.0],
            'Accr. Amount': [100.0, 200.0],
            'category_from_desc': ['cat1', 'cat2'],
        })
        ctx = _create_spt_post_context(df)
        result = step._validate_result(df, ctx)
        assert result['is_valid'] is True

    def test_missing_category_column(self):
        """測試缺少 category_from_desc 欄位時驗證失敗"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'Entry Amount': [1000.0],
        })
        ctx = _create_spt_post_context(df)
        result = step._validate_result(df, ctx)
        assert result['is_valid'] is False

    def test_temp_columns_not_removed(self):
        """測試臨時欄位未移除時驗證失敗"""
        step = SPTPostProcessingStep()
        df = pd.DataFrame({
            'category_from_desc': ['cat1'],
            '檔案日期': [202512],
        })
        ctx = _create_spt_post_context(df)
        result = step._validate_result(df, ctx)
        assert result['is_valid'] is False
        assert '臨時欄位未完全移除' in result['message'] or any('臨時欄位' in d for d in result.get('details', {}).get('issues', []))


# ============================================================
# execute 完整流程測試
# ============================================================

@pytest.mark.unit
class TestSPTPostProcessingExecute:
    """execute 完整流程測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_steps.classify_description', return_value='Miscellaneous')
    @patch('accrual_bot.tasks.spt.steps.spt_steps.clean_po_data', side_effect=lambda df: df)
    async def test_execute_success(self, mock_clean, mock_classify):
        """測試完整流程成功執行"""
        step = SPTPostProcessingStep()
        df = _create_spt_post_processing_df(3)
        ctx = _create_spt_post_context(df)

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.data is not None
        assert len(result.data) == 3

    @pytest.mark.asyncio
    async def test_execute_empty_data_fails(self):
        """測試空數據時驗證失敗（validate_input 回傳 False）"""
        step = SPTPostProcessingStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SPT',
            processing_date=202512,
            processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_steps.classify_description', return_value='Miscellaneous')
    @patch('accrual_bot.tasks.spt.steps.spt_steps.clean_po_data', side_effect=lambda df: df)
    async def test_execute_saves_temp_data(self, mock_clean, mock_classify):
        """測試執行後暫時性數據被保存到 auxiliary_data"""
        step = SPTPostProcessingStep()
        df = _create_spt_post_processing_df(3)
        ctx = _create_spt_post_context(df)

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        aux = ctx.get_auxiliary_data('result_with_temp_cols')
        assert aux is not None
        assert isinstance(aux, pd.DataFrame)

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_steps.classify_description', return_value='Miscellaneous')
    @patch('accrual_bot.tasks.spt.steps.spt_steps.clean_po_data', side_effect=lambda df: df)
    async def test_execute_metadata_has_notes(self, mock_clean, mock_classify):
        """測試執行結果的 metadata 包含處理備註"""
        step = SPTPostProcessingStep()
        df = _create_spt_post_processing_df(3)
        ctx = _create_spt_post_context(df)

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'processing_notes' in result.metadata
        assert len(result.metadata['processing_notes']) > 0
