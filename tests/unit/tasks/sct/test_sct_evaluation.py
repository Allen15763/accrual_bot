"""SCT ERM 評估步驟單元測試

測試 SCTERMLogicStep 和 SCTPRERMLogicStep：
- SCTERMLogicStep PO ERM 邏輯判斷
- SCTPRERMLogicStep PR ERM 邏輯判斷
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

@pytest.fixture
def mock_sct_evaluation_deps():
    """Mock SCT 評估步驟的外部依賴"""
    with patch('accrual_bot.tasks.sct.steps.sct_evaluation.config_manager') as mock_cm, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.config_manager') as mock_cm_engine, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.get_logger', return_value=MagicMock()):
        config_toml = {
            'sct_erm_status_rules': {'conditions': []},
            'sct_pr_erm_status_rules': {'conditions': []},
            'sct_column_defaults': {
                'region': 'TW',
                'default_department': '000',
                'prepay_liability': '111112',
            },
            'fa_accounts': {'sct': ['199999']},
        }
        mock_cm._config_toml = config_toml
        mock_cm.get_list.return_value = ['199999']
        mock_cm_engine._config_toml = config_toml
        yield mock_cm


@pytest.fixture
def mock_sct_pr_evaluation_deps():
    """Mock SCT PR 評估步驟的外部依賴"""
    with patch('accrual_bot.tasks.sct.steps.sct_pr_evaluation.config_manager') as mock_cm, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.config_manager') as mock_cm_engine, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.get_logger', return_value=MagicMock()):
        config_toml = {
            'sct_erm_status_rules': {'conditions': []},
            'sct_pr_erm_status_rules': {'conditions': []},
            'sct_column_defaults': {
                'region': 'TW',
                'default_department': '000',
                'prepay_liability': '111112',
            },
            'fa_accounts': {'sct': ['199999']},
        }
        mock_cm._config_toml = config_toml
        mock_cm.get_list.return_value = ['199999']
        mock_cm_engine._config_toml = config_toml
        yield mock_cm


def _create_sct_po_df(n=5):
    """建立 SCT PO 測試用 DataFrame"""
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
        'PO#': [f'SCTTW-PO{i:03d}' for i in range(n)],
        'PO Line': [f'SCTTW-PO{i:03d}-1' for i in range(n)],
        'GL DATE': [pd.NA] * n,
        'Liability': [pd.NA] * n,
        '是否有預付': ['N'] * n,
    })


def _create_sct_pr_df(n=5):
    """建立 SCT PR 測試用 DataFrame"""
    return pd.DataFrame({
        'GL#': [str(100000 + i) for i in range(n)],
        'Expected Received Month_轉換格式': [202512] * n,
        'YMs of Item Description': ['202510,202512'] * n,
        'Entry Amount': ['10000'] * n,
        'Item Description': [f'Test PR Item {i}' for i in range(n)],
        'Remarked by Procurement': [pd.NA] * n,
        'Remarked by 上月 FN': [pd.NA] * n,
        'Currency': ['TWD'] * n,
        'Product Code': [f'PROD{i:03d}' for i in range(n)],
        'PR狀態': [pd.NA] * n,
        'Account code': [str(100000 + i) for i in range(n)],
        'Department': [f'{i:03d}' for i in range(n)],
        'Requester': [f'User{i}' for i in range(n)],
        'PR Supplier': [f'Supplier{i}' for i in range(n)],
    })


@pytest.fixture
def sct_po_df():
    return _create_sct_po_df(5)


@pytest.fixture
def sct_pr_df():
    return _create_sct_pr_df(5)


@pytest.fixture
def sct_po_context(sct_po_df):
    """SCT PO 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=sct_po_df,
        entity_type='SCT',
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


@pytest.fixture
def sct_pr_context(sct_pr_df):
    """SCT PR 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=sct_pr_df,
        entity_type='SCT',
        processing_date=202512,
        processing_type='PR',
    )
    ctx.set_variable('processing_date', 202512)
    ctx.add_auxiliary_data('reference_account', pd.DataFrame({
        'Account': ['100000', '100001', '199999'],
        'Account Desc': ['Cash', 'Receivables', 'FA'],
    }))
    return ctx


# ============================================================
# SCTERMLogicStep 測試
# ============================================================

class TestSCTERMLogicStep:
    """測試 SCT PO ERM 邏輯步驟"""

    @pytest.mark.unit
    def test_instantiation(self, mock_sct_evaluation_deps):
        """正確初始化（fa_accounts、engine config_section）"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        assert step.name == "SCTERMLogic"
        assert step.fa_accounts == ['199999']
        assert step.engine.config_section == 'sct_erm_status_rules'
        assert step.engine.entity_type == 'SCT'

    @pytest.mark.unit
    def test_instantiation_custom_name(self, mock_sct_evaluation_deps):
        """支援自定義步驟名稱"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep(name="CustomSCTERM")
        assert step.name == "CustomSCTERM"

    @pytest.mark.unit
    def test_set_file_date(self, mock_sct_evaluation_deps, sct_po_df):
        """檔案日期欄位正確設置"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        result = step._set_file_date(sct_po_df, 202512)
        assert '檔案日期' in result.columns
        assert result['檔案日期'].iloc[0] == 202512

    @pytest.mark.unit
    def test_get_status_column_po(self, mock_sct_evaluation_deps, sct_po_context):
        """PO 狀態欄位應為 'PO狀態'"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        column = step._get_status_column(sct_po_context.data, sct_po_context)
        assert column == 'PO狀態'

    @pytest.mark.unit
    def test_build_conditions(self, mock_sct_evaluation_deps, sct_po_df):
        """SCTERMConditions 16 個 mask 全部為 pd.Series"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep, SCTERMConditions
        step = SCTERMLogicStep()
        conditions = step._build_conditions(sct_po_df, 202512, 'PO狀態')
        assert isinstance(conditions, SCTERMConditions)
        # 驗證所有欄位都是 pd.Series
        for field_name in SCTERMConditions.__dataclass_fields__:
            assert isinstance(getattr(conditions, field_name), pd.Series), \
                f"{field_name} should be pd.Series"

    @pytest.mark.unit
    def test_build_conditions_no_status(self, mock_sct_evaluation_deps, sct_po_df):
        """所有 PO 都沒有狀態時，no_status 應全為 True"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        conditions = step._build_conditions(sct_po_df, 202512, 'PO狀態')
        assert conditions.no_status.all()

    @pytest.mark.unit
    def test_build_conditions_quantity_matched(self, mock_sct_evaluation_deps, sct_po_df):
        """Entry Quantity == Received Quantity 時 quantity_matched 為 True"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        conditions = step._build_conditions(sct_po_df, 202512, 'PO狀態')
        # 預設 Entry Quantity == Received Quantity == '100'
        assert conditions.quantity_matched.all()

    @pytest.mark.unit
    def test_handle_format_errors(self, mock_sct_evaluation_deps, sct_po_df):
        """格式錯誤→退單"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        # 設置格式錯誤條件
        sct_po_df.loc[0, 'YMs of Item Description'] = '100001,100002'
        conditions = step._build_conditions(sct_po_df, 202512, 'PO狀態')
        result = step._handle_format_errors(sct_po_df, conditions, 'PO狀態')
        assert result.loc[0, 'PO狀態'] == '格式錯誤，退單'

    @pytest.mark.unit
    def test_set_accrual_flag_completed(self, mock_sct_evaluation_deps, sct_po_df):
        """已完成→Y"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        sct_po_df['PO狀態'] = '已完成(not_billed)'
        result = step._set_accrual_flag(sct_po_df, 'PO狀態')
        assert (result['是否估計入帳'] == 'Y').all()

    @pytest.mark.unit
    def test_set_accrual_flag_not_completed(self, mock_sct_evaluation_deps, sct_po_df):
        """未完成→N"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        sct_po_df['PO狀態'] = '未完成'
        result = step._set_accrual_flag(sct_po_df, 'PO狀態')
        assert (result['是否估計入帳'] == 'N').all()

    @pytest.mark.unit
    def test_set_accounting_fields(self, mock_sct_evaluation_deps, sct_po_df):
        """會計欄位設置正確"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        sct_po_df['是否估計入帳'] = 'Y'
        ref_account = pd.DataFrame({
            'Account': ['100000', '100001'],
            'Account Desc': ['Cash', 'Receivables'],
        })
        ref_liability = pd.DataFrame({
            'Account': ['100000', '100001'],
            'Liability': ['211111', '211112'],
        })
        result = step._set_accounting_fields(sct_po_df, ref_account, ref_liability)
        # Account code 應等於 GL#
        assert result.loc[0, 'Account code'] == result.loc[0, 'GL#']
        # Region_c 應為 TW
        assert result.loc[0, 'Region_c'] == 'TW'
        # Currency_c 應等於 Currency
        assert result.loc[0, 'Currency_c'] == 'TWD'
        # Dep. 應為 '000'（沒有 dept_accounts 配置）
        assert result.loc[0, 'Dep.'] == '000'

    @pytest.mark.unit
    def test_calculate_accrual_amount(self, mock_sct_evaluation_deps, sct_po_df):
        """Accr. Amount = Unit Price × (Entry Qty - Billed Qty)"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        mask = pd.Series([True] * len(sct_po_df))
        result = step._calculate_accrual_amount(sct_po_df, mask)
        # 100.0 × (100 - 0) = 10000
        assert result.loc[0, 'Accr. Amount'] == 10000.0

    @pytest.mark.unit
    def test_handle_prepayment(self, mock_sct_evaluation_deps, sct_po_df):
        """預付金→Liability='111112'"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        sct_po_df.loc[0, 'Entry Prepay Amount'] = '500'
        mask = pd.Series([True] * len(sct_po_df))
        ref_liability = pd.DataFrame({
            'Account': ['100000'],
            'Liability': ['211111'],
        })
        result = step._handle_prepayment(sct_po_df, mask, ref_liability)
        assert result.loc[0, '是否有預付'] == 'Y'
        assert result.loc[0, 'Liability'] == '111112'

    @pytest.mark.unit
    def test_check_pr_product_code(self, mock_sct_evaluation_deps, sct_po_df):
        """Product Code 檢查"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        sct_po_df['Product code'] = 'PROJ1'
        sct_po_df['Project'] = 'PROJ1 Description'
        result = step._check_pr_product_code(sct_po_df)
        assert 'PR Product Code Check' in result.columns
        assert result.loc[0, 'PR Product Code Check'] == 'good'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_full_flow(self, mock_sct_evaluation_deps, sct_po_context):
        """完整 execute 流程"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        result = await step.execute(sct_po_context)
        assert result.status == StepStatus.SUCCESS
        assert '是否估計入帳' in sct_po_context.data.columns
        assert '檔案日期' in sct_po_context.data.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_pass(self, mock_sct_evaluation_deps, sct_po_context):
        """驗證通過"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        result = await step.validate_input(sct_po_context)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty_data(self, mock_sct_evaluation_deps):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SCT',
            processing_date=202512,
            processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_columns(self, mock_sct_evaluation_deps):
        """缺少必要欄位應驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        ctx = ProcessingContext(
            data=pd.DataFrame({'col1': [1]}),
            entity_type='SCT',
            processing_date=202512,
            processing_type='PO',
        )
        ctx.set_variable('processing_date', 202512)
        ctx.add_auxiliary_data('reference_account', pd.DataFrame())
        ctx.add_auxiliary_data('reference_liability', pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_ref_data(self, mock_sct_evaluation_deps, sct_po_df):
        """缺少參考數據應驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        ctx = ProcessingContext(
            data=sct_po_df,
            entity_type='SCT',
            processing_date=202512,
            processing_type='PO',
        )
        ctx.set_variable('processing_date', 202512)
        # 不添加 reference_account / reference_liability
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_failure_no_ref_data(self, mock_sct_evaluation_deps, sct_po_df):
        """缺少參考數據時 execute 應失敗"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        ctx = ProcessingContext(
            data=sct_po_df,
            entity_type='SCT',
            processing_date=202512,
            processing_type='PO',
        )
        ctx.set_variable('processing_date', 202512)
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.unit
    def test_generate_statistics(self, mock_sct_evaluation_deps, sct_po_df):
        """統計資訊正確"""
        from accrual_bot.tasks.sct.steps.sct_evaluation import SCTERMLogicStep
        step = SCTERMLogicStep()
        sct_po_df['是否估計入帳'] = 'Y'
        sct_po_df['PO狀態'] = '已完成(not_billed)'
        stats = step._generate_statistics(sct_po_df, 'PO狀態')
        assert stats['total_count'] == 5
        assert stats['accrual_count'] == 5
        assert '已完成(not_billed)' in stats['status_distribution']


# ============================================================
# SCTPRERMLogicStep 測試
# ============================================================

class TestSCTPRERMLogicStep:
    """測試 SCT PR ERM 邏輯步驟"""

    @pytest.mark.unit
    def test_instantiation(self, mock_sct_pr_evaluation_deps):
        """正確初始化"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        assert step.name == "SCTPRERMLogic"
        assert step.engine.config_section == 'sct_pr_erm_status_rules'
        assert step.engine.entity_type == 'SCT'

    @pytest.mark.unit
    def test_pr_status_column(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """狀態欄位為 PR狀態"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        column = step._get_status_column(sct_pr_df)
        assert column == 'PR狀態'

    @pytest.mark.unit
    def test_pr_status_column_created(self, mock_sct_pr_evaluation_deps):
        """沒有 PR狀態 欄位時自動建立"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        df = pd.DataFrame({'col1': [1, 2]})
        column = step._get_status_column(df)
        assert column == 'PR狀態'
        assert 'PR狀態' in df.columns

    @pytest.mark.unit
    def test_pr_accrual_amount(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """Accr. Amount = Entry Amount（非計算）"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        sct_pr_df['是否估計入帳'] = 'Y'
        ref_account = pd.DataFrame({
            'Account': ['100000'],
            'Account Desc': ['Cash'],
        })
        result = step._set_pr_accounting_fields(sct_pr_df, ref_account)
        # PR 的 Accr. Amount 直接使用 Entry Amount
        assert result.loc[0, 'Accr. Amount'] == 10000.0

    @pytest.mark.unit
    def test_pr_no_prepayment(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """PR 不處理預付"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        sct_pr_df['是否估計入帳'] = 'Y'
        ref_account = pd.DataFrame({
            'Account': ['100000'],
            'Account Desc': ['Cash'],
        })
        result = step._set_pr_accounting_fields(sct_pr_df, ref_account)
        # PR 不應有 Liability 或 是否有預付 欄位
        assert 'Liability' not in result.columns or result['Liability'].isna().all()

    @pytest.mark.unit
    def test_pr_set_accrual_flag(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """已完成→Y, 其他→N"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        sct_pr_df['PR狀態'] = ['已完成', '未完成', '已入帳', '其他', '已完成']
        result = step._set_accrual_flag(sct_pr_df, 'PR狀態')
        expected = ['Y', 'N', 'N', 'N', 'Y']
        assert result['是否估計入帳'].tolist() == expected

    @pytest.mark.unit
    def test_pr_handle_format_errors(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """格式錯誤和其他處理"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        sct_pr_df.loc[0, 'YMs of Item Description'] = '100001,100002'
        result = step._handle_format_errors(sct_pr_df, 'PR狀態')
        assert result.loc[0, 'PR狀態'] == '格式錯誤，退單'
        # 其餘應為 '其他'（因為沒有被引擎設定狀態）
        assert result.loc[1, 'PR狀態'] == '其他'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_full_flow(self, mock_sct_pr_evaluation_deps, sct_pr_context):
        """完整 PR execute 流程"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        result = await step.execute(sct_pr_context)
        assert result.status == StepStatus.SUCCESS
        assert '是否估計入帳' in sct_pr_context.data.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_pass(self, mock_sct_pr_evaluation_deps, sct_pr_context):
        """驗證通過"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        result = await step.validate_input(sct_pr_context)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self, mock_sct_pr_evaluation_deps):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SCT',
            processing_date=202512,
            processing_type='PR',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_ref(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """缺少參考數據應驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        ctx = ProcessingContext(
            data=sct_pr_df,
            entity_type='SCT',
            processing_date=202512,
            processing_type='PR',
        )
        ctx.set_variable('processing_date', 202512)
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.unit
    def test_pr_department_setting(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """部門代碼預設為 '000'"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        mask = pd.Series([True] * len(sct_pr_df))
        result = step._set_department(sct_pr_df, mask)
        # 沒有 dept_accounts 配置，全部應為 '000'
        assert (result['Dep.'] == '000').all()

    @pytest.mark.unit
    def test_pr_generate_statistics(self, mock_sct_pr_evaluation_deps, sct_pr_df):
        """PR 統計資訊正確"""
        from accrual_bot.tasks.sct.steps.sct_pr_evaluation import SCTPRERMLogicStep
        step = SCTPRERMLogicStep()
        sct_pr_df['是否估計入帳'] = ['Y', 'N', 'Y', 'N', 'Y']
        sct_pr_df['PR狀態'] = ['已完成', '未完成', '已完成', '其他', '已完成']
        stats = step._generate_statistics(sct_pr_df, 'PR狀態')
        assert stats['total_count'] == 5
        assert stats['accrual_count'] == 3
        assert stats['accrual_percentage'] == 60.0


# ============================================================
# SCT Orchestrator 測試（步驟註冊驗證）
# ============================================================

class TestSCTOrchestratorERMSteps:
    """測試 SCT Orchestrator 正確註冊 ERM 步驟"""

    @pytest.fixture
    def mock_sct_orchestrator_config(self):
        """Mock SCT pipeline 配置"""
        with patch('accrual_bot.tasks.sct.pipeline_orchestrator.config_manager') as mock, \
             patch('accrual_bot.core.pipeline.engines.condition_engine.config_manager') as mock_engine, \
             patch('accrual_bot.core.pipeline.engines.condition_engine.get_logger', return_value=MagicMock()):
            config_toml = {
                'pipeline': {
                    'sct': {
                        'enabled_po_steps': [
                            'SCTDataLoading', 'SCTColumnAddition',
                            'APInvoiceIntegration', 'PreviousWorkpaperIntegration',
                            'ProcurementIntegration', 'DateLogic', 'SCTERMLogic',
                        ],
                        'enabled_pr_steps': [
                            'SCTPRDataLoading', 'SCTColumnAddition',
                            'PreviousWorkpaperIntegration', 'ProcurementIntegration',
                            'DateLogic', 'SCTPRERMLogic',
                        ],
                    },
                },
                'sct_erm_status_rules': {'conditions': []},
                'sct_pr_erm_status_rules': {'conditions': []},
                'sct_column_defaults': {
                    'region': 'TW',
                    'default_department': '000',
                    'prepay_liability': '111112',
                    'sm_accounts': ['650003', '450014'],
                },
                'fa_accounts': {'sct': ['199999']},
            }
            mock._config_toml = config_toml
            mock.get_list.return_value = ['199999']
            mock_engine._config_toml = config_toml
            yield mock

    @pytest.mark.unit
    def test_po_steps_include_erm(self, mock_sct_orchestrator_config):
        """PO pipeline 應包含 SCTERMLogic"""
        from accrual_bot.tasks.sct.pipeline_orchestrator import SCTPipelineOrchestrator
        orchestrator = SCTPipelineOrchestrator()
        steps = orchestrator.get_enabled_steps('PO')
        assert 'SCTERMLogic' in steps
        assert 'DateLogic' in steps

    @pytest.mark.unit
    def test_pr_steps_include_erm(self, mock_sct_orchestrator_config):
        """PR pipeline 應包含 SCTPRERMLogic"""
        from accrual_bot.tasks.sct.pipeline_orchestrator import SCTPipelineOrchestrator
        orchestrator = SCTPipelineOrchestrator()
        steps = orchestrator.get_enabled_steps('PR')
        assert 'SCTPRERMLogic' in steps
        assert 'DateLogic' in steps

    @pytest.mark.unit
    def test_build_po_pipeline(self, mock_sct_orchestrator_config):
        """構建 PO pipeline 包含 ERM 步驟"""
        from accrual_bot.tasks.sct.pipeline_orchestrator import SCTPipelineOrchestrator
        orchestrator = SCTPipelineOrchestrator()
        pipeline = orchestrator.build_po_pipeline(
            file_paths={'raw_po': '/tmp/test.xlsx'}
        )
        step_names = [s.name for s in pipeline.steps]
        assert 'SCTERMLogic' in step_names

    @pytest.mark.unit
    def test_build_pr_pipeline(self, mock_sct_orchestrator_config):
        """構建 PR pipeline 包含 ERM 步驟"""
        from accrual_bot.tasks.sct.pipeline_orchestrator import SCTPipelineOrchestrator
        orchestrator = SCTPipelineOrchestrator()
        pipeline = orchestrator.build_pr_pipeline(
            file_paths={'raw_pr': '/tmp/test.xlsx'}
        )
        step_names = [s.name for s in pipeline.steps]
        assert 'SCTPRERMLogic' in step_names
