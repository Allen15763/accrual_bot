"""SPX 評估步驟單元測試

測試 StatusStage1Step、SPXERMLogicStep 和 DepositStatusUpdateStep：
- StatusStage1Step 第一階段狀態判斷
- SPXERMLogicStep ERM 邏輯判斷
- DepositStatusUpdateStep 訂金狀態更新
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, AsyncMock
from dataclasses import dataclass

from accrual_bot.core.pipeline.base import StepStatus, StepResult
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_evaluation_deps():
    """Mock 評估步驟的外部依賴"""
    with patch('accrual_bot.tasks.spx.steps.spx_evaluation.config_manager') as mock_cm, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.config_manager') as mock_cm_engine, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.get_logger', return_value=MagicMock()):
        config_toml = {
            'spx_status_stage1_rules': {'conditions': []},
            'spx_erm_status_rules': {'conditions': []},
            'spx_column_defaults': {
                'region': 'TW',
                'default_department': '000',
                'prepay_liability': '111112',
            },
            'fa_accounts': {'spx': ['199999']},
            'spx': {
                'deposit_keywords': '訂金|押金|保證金',
                'kiosk_suppliers': ['益欣'],
                'locker_suppliers': ['掌櫃'],
            },
        }
        mock_cm._config_toml = config_toml
        mock_cm.get_list.return_value = ['199999']
        mock_cm_engine._config_toml = config_toml
        yield mock_cm


@pytest.fixture
def mock_evaluation_2_deps():
    """DepositStatusUpdateStep 不需要額外 mock（無外部依賴）"""
    yield


def _create_erm_df(n=5):
    """建立 ERM 測試用 DataFrame"""
    return pd.DataFrame({
        'GL#': [str(100000 + i) for i in range(n)],
        'Expected Received Month_轉換格式': [202512] * n,
        'YMs of Item Description': ['202510,202512'] * n,
        'Entry Quantity': ['100'] * n,
        'Received Quantity': ['100'] * n,
        'Billed Quantity': ['0'] * n,
        'Entry Amount': ['10000'] * n,
        'Entry Billed Amount': ['0'] * n,
        'Entry Prepay Amount': ['0'] * n,
        'Item Description': [f'Test Item {i}' for i in range(n)],
        'Remarked by Procurement': [pd.NA] * n,
        'Remarked by 上月 FN': [pd.NA] * n,
        'Remarked by 上月 FN PR': [pd.NA] * n,
        'Unit Price': ['100.0'] * n,
        'Currency': ['TWD'] * n,
        'Product Code': [f'PROD{i:03d}' for i in range(n)],
        'PO狀態': [pd.NA] * n,
        'Account code': [str(100000 + i) for i in range(n)],
        'Department': [f'{i:03d}' for i in range(n)],
        'PO#': [f'SPTTW-PO{i:03d}' for i in range(n)],
        'PO Line': [f'SPTTW-PO{i:03d}-1' for i in range(n)],
        'PO Supplier': [f'Supplier {i}' for i in range(n)],
        'GL DATE': [pd.NA] * n,
        'match_type': ['ITEM_TO_RECEIPT'] * n,
        'matched_condition_on_status': [pd.NA] * n,
        'Liability': [pd.NA] * n,
        '是否有預付': ['N'] * n,
    })


@pytest.fixture
def erm_df():
    return _create_erm_df(5)


@pytest.fixture
def erm_context(erm_df):
    """ERM 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=erm_df,
        entity_type='SPX',
        processing_date=202512,
        processing_type='PO',
    )
    ctx.set_variable('processing_date', 202512)
    ctx.add_auxiliary_data('reference_account', pd.DataFrame({
        'Account': ['100000', '100001', '199999'],
        'Account Desc': ['Cash', 'Receivables', 'FA'],
    }))
    ctx.add_auxiliary_data('reference_liability', pd.DataFrame({
        'Account': ['100000', '100001'],
        'Liability': ['211111', '211112'],
    }))
    return ctx


# ============================================================
# StatusStage1Step 測試
# ============================================================

class TestStatusStage1Step:
    """測試第一階段狀態判斷步驟"""

    @pytest.mark.unit
    def test_step_name_default(self, mock_evaluation_deps):
        """預設步驟名稱應為 StatusStage1"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        assert step.name == "StatusStage1"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_skips_when_no_closing_list(self, mock_evaluation_deps, erm_context):
        """關單清單為空時應跳過"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        # 不添加 closing_list → get_auxiliary_data 回傳 None
        result = await step.execute(erm_context)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_with_closing_list(self, mock_evaluation_deps, erm_context):
        """有關單清單時應正常執行"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        # 添加關單清單
        closing_df = pd.DataFrame({
            'po_no': ['PO000'],
            'new_pr_no': ['PR001'],
            'done_by_fn': [pd.NA],  # 未完成 → 待關單
            'line_no': ['ALL'],
        })
        erm_context.add_auxiliary_data('closing_list', closing_df)
        result = await step.execute(erm_context)
        assert result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty_data(self, mock_evaluation_deps):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SPX',
            processing_date=202512,
            processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.unit
    def test_is_closed_spx(self, mock_evaluation_deps):
        """判斷關單狀態的條件應正確"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        df = pd.DataFrame({
            'new_pr_no': ['PR001', 'PR002', pd.NA],
            'done_by_fn': [pd.NA, '2025/01', pd.NA],
        })
        to_be_closed, closed = step.is_closed_spx(df)
        # 第 0 列：有 new_pr_no，done_by_fn 為空 → 待關單
        assert to_be_closed.iloc[0] == True
        assert closed.iloc[0] == False
        # 第 1 列：有 new_pr_no，done_by_fn 不為空 → 已關單
        assert to_be_closed.iloc[1] == False
        assert closed.iloc[1] == True
        # 第 2 列：無 new_pr_no → 不在任何清單
        assert to_be_closed.iloc[2] == False
        assert closed.iloc[2] == False

    @pytest.mark.unit
    def test_convert_date_format_in_remark(self, mock_evaluation_deps):
        """日期格式轉換 YYYY/MM → YYYYMM"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        series = pd.Series(['已完成 2025/01', '無日期', pd.NA])
        result = step.convert_date_format_in_remark(series)
        assert '202501' in str(result.iloc[0])

    @pytest.mark.unit
    def test_extract_fa_remark(self, mock_evaluation_deps):
        """FA 備註提取應正確"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        series = pd.Series(['202501入FA', '普通備註'])
        result = step.extract_fa_remark(series)
        assert result.iloc[0] == '202501入FA'
        assert pd.isna(result.iloc[1])

    @pytest.mark.unit
    def test_generate_label_summary(self, mock_evaluation_deps, erm_df):
        """標籤摘要應包含必要的統計欄位"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import StatusStage1Step
        step = StatusStage1Step()
        df = erm_df.copy()
        df.loc[0, 'PO狀態'] = '已完成_租金'
        df.loc[1, 'PO狀態'] = '待關單'
        summary = step._generate_label_summary(df, 'PO狀態')
        assert 'total_records' in summary
        assert 'labeled_count' in summary
        assert 'unlabeled_count' in summary
        assert 'category_stats' in summary
        assert summary['labeled_count'] == 2


# ============================================================
# SPXERMLogicStep 測試
# ============================================================

class TestSPXERMLogicStep:
    """測試 SPX ERM 邏輯步驟"""

    @pytest.mark.unit
    def test_step_name_default(self, mock_evaluation_deps):
        """預設步驟名稱應為 SPX_ERM_Logic"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        assert step.name == "SPX_ERM_Logic"

    @pytest.mark.unit
    def test_set_file_date(self, mock_evaluation_deps, erm_df):
        """_set_file_date 應正確設置檔案日期欄位"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        df = step._set_file_date(erm_df.copy(), 202512)
        assert '檔案日期' in df.columns
        assert (df['檔案日期'] == 202512).all()

    @pytest.mark.unit
    def test_get_status_column_po(self, mock_evaluation_deps, erm_context):
        """有 PO狀態 欄位時應回傳 PO狀態"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        col = step._get_status_column(erm_context.data, erm_context)
        assert col == 'PO狀態'

    @pytest.mark.unit
    def test_get_status_column_pr(self, mock_evaluation_deps):
        """有 PR狀態 欄位時應回傳 PR狀態"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        df = pd.DataFrame({'PR狀態': [pd.NA]})
        ctx = ProcessingContext(
            data=df, entity_type='SPX',
            processing_date=202512, processing_type='PR'
        )
        col = step._get_status_column(df, ctx)
        assert col == 'PR狀態'

    @pytest.mark.unit
    def test_build_conditions(self, mock_evaluation_deps, erm_df):
        """_build_conditions 應回傳 ERMConditions 且欄位齊全"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep, ERMConditions
        step = SPXERMLogicStep()
        cond = step._build_conditions(erm_df.copy(), 202512, 'PO狀態')
        assert isinstance(cond, ERMConditions)
        # 驗證幾個關鍵條件
        assert len(cond.no_status) == 5
        assert len(cond.quantity_matched) == 5
        assert len(cond.is_fa) == 5

    @pytest.mark.unit
    def test_build_conditions_quantity_matched(self, mock_evaluation_deps):
        """數量匹配條件應正確計算"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        df = _create_erm_df(3)
        df.loc[2, 'Received Quantity'] = '50'  # 不匹配
        cond = step._build_conditions(df, 202512, 'PO狀態')
        assert cond.quantity_matched.iloc[0] == True
        assert cond.quantity_matched.iloc[2] == False

    @pytest.mark.unit
    def test_build_conditions_erm_date(self, mock_evaluation_deps):
        """ERM 日期條件應正確比較"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        df = _create_erm_df(2)
        df.loc[0, 'Expected Received Month_轉換格式'] = 202512  # <= 202512
        df.loc[1, 'Expected Received Month_轉換格式'] = 202601  # > 202512
        cond = step._build_conditions(df, 202512, 'PO狀態')
        assert cond.erm_before_or_equal_file_date.iloc[0] == True
        assert cond.erm_before_or_equal_file_date.iloc[1] == False
        assert cond.erm_after_file_date.iloc[0] == False
        assert cond.erm_after_file_date.iloc[1] == True

    @pytest.mark.unit
    def test_build_conditions_format_error(self, mock_evaluation_deps):
        """格式錯誤條件應識別 100001,100002"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        df = _create_erm_df(2)
        df.loc[1, 'YMs of Item Description'] = '100001,100002'
        cond = step._build_conditions(df, 202512, 'PO狀態')
        assert cond.format_error.iloc[0] == False
        assert cond.format_error.iloc[1] == True

    @pytest.mark.unit
    def test_set_accrual_flag(self, mock_evaluation_deps):
        """已完成狀態應設為 Y，其他為 N"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        df = pd.DataFrame({'PO狀態': ['已完成', '未完成', '已完成_租金']})
        df = step._set_accrual_flag(df, 'PO狀態')
        assert df.loc[0, '是否估計入帳'] == 'Y'
        assert df.loc[1, '是否估計入帳'] == 'N'
        assert df.loc[2, '是否估計入帳'] == 'Y'  # 包含「已完成」

    @pytest.mark.unit
    def test_handle_format_errors(self, mock_evaluation_deps):
        """格式錯誤且無狀態的列應標為「格式錯誤，退單」"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep, ERMConditions
        step = SPXERMLogicStep()
        df = _create_erm_df(3)
        df.loc[0, 'PO狀態'] = '已完成'  # 已有狀態，不應被覆蓋
        cond = step._build_conditions(df, 202512, 'PO狀態')
        # 人為設定第 2 列為格式錯誤
        cond.format_error = pd.Series([False, False, True])
        df = step._handle_format_errors(df, cond, 'PO狀態')
        assert df.loc[0, 'PO狀態'] == '已完成'  # 保留原狀態
        assert df.loc[2, 'PO狀態'] == '格式錯誤，退單'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success(self, mock_evaluation_deps, erm_context):
        """完整執行應回傳 SUCCESS"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        result = await step.execute(erm_context)
        assert result.status == StepStatus.SUCCESS
        assert '是否估計入帳' in erm_context.data.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_missing_reference_fails(self, mock_evaluation_deps):
        """缺少參考數據應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_evaluation import SPXERMLogicStep
        step = SPXERMLogicStep()
        ctx = ProcessingContext(
            data=_create_erm_df(3),
            entity_type='SPX',
            processing_date=202512,
            processing_type='PO',
        )
        ctx.set_variable('processing_date', 202512)
        # 不添加 reference_account / reference_liability
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED


# ============================================================
# DepositStatusUpdateStep 測試
# ============================================================

class TestDepositStatusUpdateStep:
    """測試訂金狀態更新步驟"""

    @pytest.mark.unit
    def test_step_name_default(self, mock_evaluation_2_deps):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_evaluation_2 import DepositStatusUpdateStep
        step = DepositStatusUpdateStep()
        assert step.name == "DepositStatusUpdate"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_no_deposit_records(self, mock_evaluation_2_deps):
        """無訂金記錄時應跳過"""
        from accrual_bot.tasks.spx.steps.spx_evaluation_2 import DepositStatusUpdateStep
        step = DepositStatusUpdateStep()
        df = pd.DataFrame({
            'PO#': ['PO001'],
            'Item Description': ['一般品項'],
            'Expected Received Month_轉換格式': [202512],
            'PO狀態': [pd.NA],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPX',
            processing_date=202512, processing_type='PO',
        )
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_with_deposit(self, mock_evaluation_2_deps):
        """有訂金且最大 ERM > 當月時應更新狀態"""
        from accrual_bot.tasks.spx.steps.spx_evaluation_2 import DepositStatusUpdateStep
        step = DepositStatusUpdateStep()
        df = pd.DataFrame({
            'PO#': ['PO001', 'PO001', 'PO002'],
            'Item Description': ['訂金-設備', '設備安裝', '一般採購'],
            'Expected Received Month_轉換格式': [202601, 202601, 202512],
            'PO狀態': [pd.NA, pd.NA, pd.NA],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPX',
            processing_date=202512, processing_type='PO',
        )
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        # PO001 的所有列應被更新（因為含訂金且 max ERM > 當月）
        updated_df = ctx.data
        assert updated_df.loc[0, 'PO狀態'] == '未完成(deposit)'
        assert updated_df.loc[1, 'PO狀態'] == '未完成(deposit)'
        # PO002 不含訂金，不應被更新
        assert pd.isna(updated_df.loc[2, 'PO狀態'])

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_success(self, mock_evaluation_2_deps):
        """有效的輸入應通過驗證"""
        from accrual_bot.tasks.spx.steps.spx_evaluation_2 import DepositStatusUpdateStep
        step = DepositStatusUpdateStep()
        df = pd.DataFrame({
            'PO#': ['PO001'],
            'Item Description': ['test'],
            'Expected Received Month_轉換格式': [202512],
            'PO狀態': [pd.NA],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPX',
            processing_date=202512, processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty_df(self, mock_evaluation_2_deps):
        """空 DataFrame 應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_evaluation_2 import DepositStatusUpdateStep
        step = DepositStatusUpdateStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPX',
            processing_date=202512, processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False
