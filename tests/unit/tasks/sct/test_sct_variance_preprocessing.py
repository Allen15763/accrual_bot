"""
SCT 差異分析 - 預處理步驟單元測試
"""

import pytest
import pandas as pd

from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.tasks.sct.steps.sct_variance_preprocessing import (
    SCTVariancePreprocessingStep,
)


@pytest.fixture
def context():
    """建立測試用 ProcessingContext"""
    return ProcessingContext(
        data=pd.DataFrame(), entity_type='SCT',
        processing_date=202603, processing_type='VARIANCE'
    )


@pytest.fixture
def current_df_raw():
    """當期底稿（原始欄位，未清理）"""
    return pd.DataFrame({
        'Item Description': ['Item A', 'Item B', 'Item C'],
        'PO Line': ['PO001-1', 'PO002-1', 'PO003-1'],
        'Account Code': ['5100', '5200', '5300'],
        'Currency C': ['TWD', 'USD', 'TWD'],
        'Accr. Amount': ['1000', '2000', '3000'],
        '是否估計入帳': ['Y', 'Y', 'N'],
    })


@pytest.fixture
def current_df_cleaned():
    """當期底稿（已清理欄位）"""
    return pd.DataFrame({
        'item_description': ['Item A', 'Item B', 'Item C'],
        'po_line': ['PO001-1', 'PO002-1', 'PO003-1'],
        'account_code': ['5100', '5200', '5300'],
        'currency_c': ['TWD', 'USD', 'TWD'],
        'amount': ['1000', '2000', '3000'],
        '是否估計入帳': ['Y', 'Y', 'N'],
    })


@pytest.fixture
def previous_df_normalized():
    """前期底稿（原始大寫欄位）"""
    return pd.DataFrame({
        'Item Description': ['Item A', 'Item B'],
        'PO Line': ['PO001-1', 'PO002-1'],
        'Account Code': ['5100', '5200'],
        'Currency C': ['TWD', 'USD'],
        'Accr. Amount': ['900', '1800'],
        '是否需要估計入帳': ['Y', 'Y'],
    })


@pytest.fixture
def previous_df_missing_cols():
    """前期底稿（需要公式合成 + 別名解析）"""
    return pd.DataFrame({
        'Item Description': ['Item A'],
        'PO#': ['PO001'],
        'Line#': ['1'],
        'Account Code': ['5100'],
        'Currency': ['TWD'],
        'Amount-未稅': ['900'],
        '是否需要估計入帳': ['Y'],
    })


class TestSCTVariancePreprocessingStep:
    """測試差異分析預處理步驟"""

    def test_instantiation(self):
        """基本實例化"""
        step = SCTVariancePreprocessingStep()
        assert step.name == "SCTVariancePreprocessing"

    # ===== 別名解析測試 =====

    def test_resolve_aliases_direct_match(self):
        """欄位已是標準名，不需解析"""
        df = pd.DataFrame({'amount': [1], 'currency_c': [2]})
        aliases = {'amount': ['amount', 'accr._amount'], 'currency_c': ['currency_c', 'currency']}
        result = SCTVariancePreprocessingStep._resolve_aliases(df, aliases)
        assert result == {}

    def test_resolve_aliases_rename(self):
        """欄位需要從別名解析"""
        df = pd.DataFrame({'accr._amount': [1], 'currency': [2]})
        aliases = {'amount': ['amount', 'accr._amount'], 'currency_c': ['currency_c', 'currency']}
        result = SCTVariancePreprocessingStep._resolve_aliases(df, aliases)
        assert result == {'accr._amount': 'amount', 'currency': 'currency_c'}

    def test_resolve_aliases_priority(self):
        """別名解析優先取清單中靠前的"""
        df = pd.DataFrame({'amount': [1], 'accr._amount': [2]})
        aliases = {'amount': ['amount', 'accr._amount']}
        result = SCTVariancePreprocessingStep._resolve_aliases(df, aliases)
        # amount 已存在，不需 rename
        assert result == {}

    # ===== 原始底稿（未清理）處理測試 =====

    @pytest.mark.asyncio
    async def test_raw_current_filter_and_alias(self, context, current_df_raw):
        """原始當期底稿：正規化 + 別名解析 + 篩選"""
        context.data = current_df_raw
        context.set_auxiliary_data('previous_worksheet', current_df_raw.copy())

        step = SCTVariancePreprocessingStep()
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        # 原本 3 筆，篩選後只有 Y 的 2 筆
        assert result.metadata['current_rows_after_filter'] == 2
        # 最終欄位應包含 amount（從 accr._amount 別名解析）
        assert 'amount' in context.data.columns
        assert 'accr._amount' not in context.data.columns

    # ===== 已清理底稿處理測試 =====

    @pytest.mark.asyncio
    async def test_cleaned_current_passthrough(self, context, current_df_cleaned):
        """已清理當期底稿：欄位已是標準名，直接通過"""
        context.data = current_df_cleaned
        context.set_auxiliary_data('previous_worksheet', current_df_cleaned.copy())

        step = SCTVariancePreprocessingStep()
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['current_rows_after_filter'] == 2
        assert 'amount' in context.data.columns

    # ===== 前期底稿處理測試 =====

    @pytest.mark.asyncio
    async def test_previous_column_normalization(
        self, context, current_df_cleaned, previous_df_normalized
    ):
        """前期底稿欄位正規化（小寫 + 底線）+ 別名解析"""
        context.data = current_df_cleaned
        context.set_auxiliary_data('previous_worksheet', previous_df_normalized)

        step = SCTVariancePreprocessingStep()
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        previous = context.get_auxiliary_data('previous_worksheet')
        # 欄位應已正規化為標準名
        assert all(c == c.lower().replace(' ', '_') for c in previous.columns)
        # accr._amount 應被解析為 amount
        assert 'amount' in previous.columns

    @pytest.mark.asyncio
    async def test_previous_formula_concat(
        self, context, current_df_cleaned, previous_df_missing_cols
    ):
        """前期底稿缺失 po_line 時用 po# + line# 串接"""
        context.data = current_df_cleaned
        context.set_auxiliary_data('previous_worksheet', previous_df_missing_cols)

        step = SCTVariancePreprocessingStep()
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        previous = context.get_auxiliary_data('previous_worksheet')
        if 'po_line' in previous.columns:
            assert previous['po_line'].iloc[0] == 'PO0011'

    @pytest.mark.asyncio
    async def test_previous_alias_fallback(
        self, context, current_df_cleaned, previous_df_missing_cols
    ):
        """前期底稿缺失 currency_c / amount 時用別名解析"""
        context.data = current_df_cleaned
        context.set_auxiliary_data('previous_worksheet', previous_df_missing_cols)

        step = SCTVariancePreprocessingStep()
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        previous = context.get_auxiliary_data('previous_worksheet')
        if 'currency_c' in previous.columns:
            assert previous['currency_c'].iloc[0] == 'TWD'
        if 'amount' in previous.columns:
            assert previous['amount'].iloc[0] == '900'

    # ===== 驗證 =====

    @pytest.mark.asyncio
    async def test_validate_input_no_data(self, context):
        """驗證：無當期資料"""
        step = SCTVariancePreprocessingStep()
        assert not await step.validate_input(context)

    @pytest.mark.asyncio
    async def test_validate_input_no_previous(self, context, current_df_cleaned):
        """驗證：無前期資料"""
        context.data = current_df_cleaned
        step = SCTVariancePreprocessingStep()
        assert not await step.validate_input(context)

    @pytest.mark.asyncio
    async def test_validate_input_success(self, context, current_df_cleaned):
        """驗證：通過"""
        context.data = current_df_cleaned
        context.set_auxiliary_data('previous_worksheet', current_df_cleaned.copy())
        step = SCTVariancePreprocessingStep()
        assert await step.validate_input(context)

    @pytest.mark.asyncio
    async def test_empty_after_filter(self, context):
        """篩選後為空（所有都是 N）"""
        df = pd.DataFrame({
            'item_description': ['A'],
            'po_line': ['1'],
            'account_code': ['5100'],
            'currency_c': ['TWD'],
            'amount': ['100'],
            '是否估計入帳': ['N'],
        })
        context.data = df
        context.set_auxiliary_data('previous_worksheet', df.copy())

        step = SCTVariancePreprocessingStep()
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['current_rows_after_filter'] == 0
