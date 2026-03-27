"""
SPT ERM 邏輯步驟測試

測試 SPTERMLogicStep 的核心功能：
- 步驟初始化
- _build_conditions() 條件構建
- _apply_status_conditions() 狀態判斷（11 個條件）
- _set_accrual_flag() 是否估計入帳
- _set_accounting_fields() 會計欄位設置
- _set_file_date() 設置檔案日期
- _get_status_column() 動態判斷狀態欄位
- _handle_format_errors() 格式錯誤處理
- _generate_statistics() 統計資訊生成
- execute() 完整流程
- validate_input() 輸入驗證
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, Mock, AsyncMock, MagicMock

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# SPTERMLogicStep 初始化測試
# ============================================================

@pytest.mark.unit
class TestSPTERMLogicStepInit:
    """SPTERMLogicStep 初始化測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_default_name(self, mock_cm):
        """測試預設步驟名稱為 SPT_ERM_Logic"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        assert step.name == "SPT_ERM_Logic"

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_custom_name(self, mock_cm):
        """測試自訂步驟名稱"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep(name="CustomERM")
        assert step.name == "CustomERM"

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_fa_accounts_loaded_from_config(self, mock_cm):
        """測試 FA 科目從配置正確載入"""
        mock_cm.get_list.return_value = ['199999', '299999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        assert '199999' in step.fa_accounts
        assert '299999' in step.fa_accounts


# ============================================================
# _set_file_date / _get_status_column 測試
# ============================================================

@pytest.mark.unit
class TestSPTERMBasicMethods:
    """基本方法測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_set_file_date(self, mock_cm):
        """測試 _set_file_date 正確設置檔案日期欄位"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        df = pd.DataFrame({'col1': [1, 2, 3]})
        result = step._set_file_date(df, 202512)
        assert '檔案日期' in result.columns
        assert (result['檔案日期'] == 202512).all()

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_get_status_column_po(self, mock_cm):
        """測試有 PO狀態 欄位時返回 PO狀態"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        df = pd.DataFrame({'PO狀態': [pd.NA]})
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        assert step._get_status_column(df, ctx) == 'PO狀態'

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_get_status_column_pr(self, mock_cm):
        """測試有 PR狀態 欄位時返回 PR狀態"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        df = pd.DataFrame({'PR狀態': [pd.NA]})
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PR'
        )
        assert step._get_status_column(df, ctx) == 'PR狀態'

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_get_status_column_fallback(self, mock_cm):
        """測試兩個欄位都不存在時，根據 processing_type 動態生成"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        df = pd.DataFrame({'other_col': [1]})
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        assert step._get_status_column(df, ctx) == 'PO狀態'


# ============================================================
# _build_conditions 測試
# ============================================================

@pytest.mark.unit
class TestSPTERMBuildConditions:
    """_build_conditions 條件構建測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_build_conditions_returns_erm_conditions(self, mock_cm, spt_erm_df):
        """測試 _build_conditions 返回 ERMConditions 實例"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep, ERMConditions
        step = SPTERMLogicStep()
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        assert isinstance(conditions, ERMConditions)

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_build_conditions_no_status_all_na(self, mock_cm, spt_erm_df):
        """測試所有 PO狀態 為 NA 時，no_status 應全為 True"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        assert conditions.no_status.all()

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_build_conditions_quantity_matched(self, mock_cm, spt_erm_df):
        """測試 Entry Quantity == Received Quantity 時 quantity_matched 為 True"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        # spt_erm_df 中 Entry Quantity == Received Quantity == '100'
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        assert conditions.quantity_matched.all()

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_build_conditions_format_error(self, mock_cm, spt_erm_df):
        """測試 YMs of Item Description 為 100001,100002 時 format_error 為 True"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        spt_erm_df['YMs of Item Description'] = '100001,100002'
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        assert conditions.format_error.all()


# ============================================================
# _apply_status_conditions 測試
# ============================================================

@pytest.mark.unit
class TestSPTERMApplyStatusConditions:
    """_apply_status_conditions 狀態條件應用測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_condition_1_posted_by_fn(self, mock_cm, spt_erm_df):
        """測試條件 1：前期 FN 標註「已入帳」"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        spt_erm_df.loc[0, 'Remarked by 上月 FN'] = '已入帳'
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        result = step._apply_status_conditions(spt_erm_df, conditions, 'PO狀態')

        assert result.loc[0, 'PO狀態'] == '已入帳'

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_condition_3_completed_not_billed(self, mock_cm, spt_erm_df):
        """測試條件 3：已完成(not_billed) - 採購完成 + 在範圍內 + 數量相符 + 未開票"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        # 設定符合條件 3 的數據
        spt_erm_df.loc[0, 'Remarked by Procurement'] = '已完成'
        spt_erm_df.loc[0, 'Remarked by 上月 FN PR'] = ''
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        result = step._apply_status_conditions(spt_erm_df, conditions, 'PO狀態')

        assert result.loc[0, 'PO狀態'] == '已完成(not_billed)'

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_condition_6_check_delivery(self, mock_cm, spt_erm_df):
        """測試條件 6：Check收貨 - 數量不匹配"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        # 設置數量不匹配
        spt_erm_df['Received Quantity'] = '50'
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        result = step._apply_status_conditions(spt_erm_df, conditions, 'PO狀態')

        # 所有行都應被標記為 Check收貨（因為無備註、在範圍、ERM<=file_date、數量不匹配）
        check_rows = result[result['PO狀態'] == 'Check收貨']
        assert len(check_rows) > 0

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_condition_7_incomplete(self, mock_cm, spt_erm_df):
        """測試條件 7：未完成 - ERM 在結帳月之後"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        # ERM 在結帳月之後，且在日期範圍內
        spt_erm_df['Expected Received Month_轉換格式'] = 202601
        spt_erm_df['YMs of Item Description'] = '202510,202602'
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        result = step._apply_status_conditions(spt_erm_df, conditions, 'PO狀態')

        incomplete_rows = result[result['PO狀態'] == '未完成']
        assert len(incomplete_rows) > 0


# ============================================================
# _handle_format_errors 測試
# ============================================================

@pytest.mark.unit
class TestSPTERMHandleFormatErrors:
    """_handle_format_errors 格式錯誤處理測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_format_error_marked(self, mock_cm, spt_erm_df):
        """測試格式錯誤的記錄被標記為「格式錯誤，退單」"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        # 設置格式錯誤
        spt_erm_df['YMs of Item Description'] = '100001,100002'
        conditions = step._build_conditions(spt_erm_df, 202512, 'PO狀態')
        result = step._handle_format_errors(spt_erm_df, conditions, 'PO狀態')

        format_error_rows = result[result['PO狀態'] == '格式錯誤，退單']
        assert len(format_error_rows) == 5


# ============================================================
# _set_accrual_flag 測試
# ============================================================

@pytest.mark.unit
class TestSPTERMSetAccrualFlag:
    """_set_accrual_flag 是否估計入帳測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_completed_status_set_y(self, mock_cm):
        """測試「已完成」狀態的記錄，是否估計入帳設為 Y"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        df = pd.DataFrame({'PO狀態': ['已完成(not_billed)', '已完成(fully_billed)', '已入帳']})
        result = step._set_accrual_flag(df, 'PO狀態')
        assert result.loc[0, '是否估計入帳'] == 'Y'
        assert result.loc[1, '是否估計入帳'] == 'Y'
        assert result.loc[2, '是否估計入帳'] == 'N'

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_non_completed_status_set_n(self, mock_cm):
        """測試非「已完成」狀態的記錄，是否估計入帳設為 N"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        df = pd.DataFrame({'PO狀態': ['已入帳', 'Check收貨', '未完成', pd.NA]})
        result = step._set_accrual_flag(df, 'PO狀態')
        assert (result['是否估計入帳'] == 'N').all()


# ============================================================
# _generate_statistics 測試
# ============================================================

@pytest.mark.unit
class TestSPTERMGenerateStatistics:
    """_generate_statistics 統計資訊生成測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    def test_statistics_content(self, mock_cm):
        """測試統計資訊包含正確的 total_count 和 accrual_count"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        df = pd.DataFrame({
            'PO狀態': ['已完成(not_billed)', '已入帳', '未完成'],
            '是否估計入帳': ['Y', 'N', 'N'],
        })
        stats = step._generate_statistics(df, 'PO狀態')
        assert stats['total_count'] == 3
        assert stats['accrual_count'] == 1
        assert '已完成(not_billed)' in stats['status_distribution']


# ============================================================
# execute 完整流程測試
# ============================================================

@pytest.mark.unit
class TestSPTERMLogicExecute:
    """execute 完整流程測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    async def test_execute_success(self, mock_cm, spt_erm_context):
        """測試完整流程成功執行"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        result = await step.execute(spt_erm_context)

        assert result.status == StepStatus.SUCCESS
        assert result.data is not None
        assert '是否估計入帳' in result.data.columns
        assert 'PO狀態' in result.data.columns

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    async def test_execute_missing_reference_data_fails(self, mock_cm):
        """測試缺少參考數據時流程應失敗"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        # 建立不含參考數據的 context
        from tests.unit.tasks.conftest import _create_spt_erm_df
        df = _create_spt_erm_df(3)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        ctx.set_variable('processing_date', 202512)
        # 故意不添加 reference_account / reference_liability

        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    async def test_execute_metadata_contains_stats(self, mock_cm, spt_erm_context):
        """測試執行結果的 metadata 包含統計資訊"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        result = await step.execute(spt_erm_context)
        assert 'total_count' in result.metadata
        assert 'accrual_count' in result.metadata
        assert 'status_distribution' in result.metadata


# ============================================================
# validate_input 測試
# ============================================================

@pytest.mark.unit
class TestSPTERMValidateInput:
    """validate_input 輸入驗證測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    async def test_validate_success(self, mock_cm, spt_erm_context):
        """測試完整數據驗證通過"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        result = await step.validate_input(spt_erm_context)
        assert result is True

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    async def test_validate_empty_data_returns_false(self, mock_cm):
        """測試空數據驗證失敗"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    async def test_validate_missing_columns_returns_false(self, mock_cm):
        """測試缺少必要欄位驗證失敗"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        # 只有部分欄位
        df = pd.DataFrame({'GL#': ['100000'], 'SomeColumn': ['x']})
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        ctx.set_variable('processing_date', 202512)
        ctx.add_auxiliary_data('reference_account', pd.DataFrame())
        ctx.add_auxiliary_data('reference_liability', pd.DataFrame())

        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_erm.config_manager')
    async def test_validate_missing_processing_date_returns_false(self, mock_cm, spt_erm_df):
        """測試缺少 processing_date 變數驗證失敗"""
        mock_cm.get_list.return_value = ['199999']
        from accrual_bot.tasks.spt.steps.spt_evaluation_erm import SPTERMLogicStep
        step = SPTERMLogicStep()

        ctx = ProcessingContext(
            data=spt_erm_df, entity_type='SPT',
            processing_date=0, processing_type='PO'
        )
        # processing_date=0 模擬未設定處理日期（metadata 為單一來源）
        ctx.add_auxiliary_data('reference_account', pd.DataFrame())
        ctx.add_auxiliary_data('reference_liability', pd.DataFrame())

        result = await step.validate_input(ctx)
        assert result is False
