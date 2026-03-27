"""SCT Column Addition Step 單元測試

測試 SCTColumnAdditionStep：
- 基礎欄位添加（是否結案、結案差異、Check with Entry Invoice）
- PO/PR Line 組合鍵
- 備註欄位與計算欄位
- FA / S&M 判斷
- PR pipeline 欄位重命名
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
def mock_sct_column_deps():
    """Mock SCT column addition 的外部依賴"""
    with patch('accrual_bot.tasks.sct.steps.sct_column_addition.config_manager') as mock_cm:
        mock_cm.get_list.return_value = ['199999']
        mock_cm._config_toml = {
            'sct_column_defaults': {
                'sm_accounts': ['650003', '450014'],
            },
            'fa_accounts': {'sct': ['199999']},
        }
        yield mock_cm


def _create_sct_df(n=5):
    """建立 SCT 測試用 DataFrame（含 column addition 所需欄位）"""
    return pd.DataFrame({
        'GL#': ['100000', '100001', '199999', '650003', '100004'],
        'Closed For Invoice': ['0', '1', '0', '1', '0'],
        'Entry Quantity': ['100', '200', '300', '400', '500'],
        'Billed Quantity': ['50', '200', '0', '100', '500'],
        'Entry Amount': ['10000', '20000', '30000', '40000', '50000'],
        'Entry Billed Amount': ['5000', '0', '0', '10000', '50000'],
        'Product Code': [f'PROD{i:03d}' for i in range(n)],
        'Item Description': [f'Item {i}' for i in range(n)],
        'PR#': [f'PR{i:03d}' for i in range(n)],
        'PO#': [f'PO{i:03d}' for i in range(n)],
        'Line#': [str(i + 1) for i in range(n)],
        'Currency': ['TWD'] * n,
    })


@pytest.fixture
def sct_df():
    return _create_sct_df(5)


@pytest.fixture
def sct_po_context(sct_df):
    """SCT PO 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=sct_df,
        entity_type='SCT',
        processing_date=202503,
        processing_type='PO',
    )
    return ctx


@pytest.fixture
def sct_pr_context(sct_df):
    """SCT PR 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=sct_df,
        entity_type='SCT',
        processing_date=202503,
        processing_type='PR',
    )
    return ctx


# ============================================================
# SCTColumnAdditionStep 測試
# ============================================================

class TestSCTColumnAdditionStep:
    """測試 SCT 欄位添加步驟"""

    @pytest.mark.unit
    def test_instantiation(self, mock_sct_column_deps):
        """正確初始化"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        assert step.name == "SCTColumnAddition"

    @pytest.mark.unit
    def test_instantiation_custom_name(self, mock_sct_column_deps):
        """支援自定義步驟名稱"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep(name="CustomColumnAdd")
        assert step.name == "CustomColumnAdd"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success(self, mock_sct_column_deps, sct_po_context):
        """execute 成功執行並添加新欄位"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        result = await step.execute(sct_po_context)
        assert result.status == StepStatus.SUCCESS
        assert '是否結案' in sct_po_context.data.columns
        assert '結案是否有差異數量' in sct_po_context.data.columns
        assert 'Check with Entry Invoice' in sct_po_context.data.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_adds_remark_columns(self, mock_sct_column_deps, sct_po_context):
        """添加備註相關欄位"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        await step.execute(sct_po_context)
        df = sct_po_context.data
        for col in ['Remarked by Procurement', 'Noted by Procurement',
                     'Remarked by FN', 'Noted by FN', 'PO狀態']:
            assert col in df.columns, f"Missing column: {col}"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_adds_calculation_columns(self, mock_sct_column_deps, sct_po_context):
        """添加計算欄位"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        await step.execute(sct_po_context)
        df = sct_po_context.data
        for col in ['是否估計入帳', '是否為FA', '是否為S&M', 'Account code',
                     'Accr. Amount', 'Liability']:
            assert col in df.columns, f"Missing column: {col}"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_pr_pipeline_renames_status_column(self, mock_sct_column_deps, sct_pr_context):
        """PR pipeline 將 PO狀態 重命名為 PR狀態"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        await step.execute(sct_pr_context)
        df = sct_pr_context.data
        assert 'PR狀態' in df.columns
        assert 'PO狀態' not in df.columns

    @pytest.mark.unit
    def test_determine_fa_status(self, mock_sct_column_deps, sct_df):
        """FA 帳戶判斷正確"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        result = step._determine_fa_status(sct_df)
        # GL# '199999' 在 fa_accounts 中；np.where 回傳 ndarray
        assert result[2] == 'Y'
        assert result[0] == ''

    @pytest.mark.unit
    def test_determine_sm_status(self, mock_sct_column_deps, sct_df):
        """S&M 帳戶判斷正確"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        result = step._determine_sm_status(sct_df)
        # GL# '650003' 在 sm_accounts 中；np.where 回傳 ndarray
        assert result[3] == 'Y'
        assert result[0] == 'N'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_pass(self, mock_sct_column_deps, sct_po_context):
        """有資料時驗證通過"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        result = await step.validate_input(sct_po_context)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self, mock_sct_column_deps):
        """空資料時驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.unit
    def test_add_basic_columns_closed_status(self, mock_sct_column_deps, sct_df):
        """是否結案根據 Closed For Invoice 正確設置"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        result_df, prev_month = step._add_basic_columns(sct_df, 3)
        # Closed For Invoice == '0' → 未結案, '1' → 結案
        assert result_df.loc[0, '是否結案'] == '未結案'
        assert result_df.loc[1, '是否結案'] == '結案'

    @pytest.mark.unit
    def test_add_basic_columns_previous_month(self, mock_sct_column_deps, sct_df):
        """上月計算正確（含跨年）"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        _, prev_month = step._add_basic_columns(sct_df, 1)
        assert prev_month == 12  # 1月的上月是12月
        _, prev_month = step._add_basic_columns(sct_df, 6)
        assert prev_month == 5

    @pytest.mark.unit
    def test_add_basic_columns_po_pr_line(self, mock_sct_column_deps, sct_df):
        """PO Line 和 PR Line 組合鍵正確"""
        from accrual_bot.tasks.sct.steps.sct_column_addition import SCTColumnAdditionStep
        step = SCTColumnAdditionStep()
        result_df, _ = step._add_basic_columns(sct_df, 3)
        assert result_df.loc[0, 'PO Line'] == 'PO000-1'
        assert result_df.loc[0, 'PR Line'] == 'PR000-1'
