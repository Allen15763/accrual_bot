"""
SPTAccountPredictionStep 單元測試

測試會計科目預測步驟的各方法：
- 初始化與規則載入
- 欄位初始化
- 條件構建
- 規則應用（部門、關鍵字、金額上限、優先順序）
- 輸入驗證
- 統計資訊生成
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.context import ProcessingContext


# =============================================================================
# Fixtures
# =============================================================================

MOCK_RULES = [
    {
        'rule_id': 1,
        'account': '6001',
        'condition_desc': 'IT部門',
        'departments': ['IT'],
    },
    {
        'rule_id': 2,
        'account': '6002',
        'condition_desc': '含維護關鍵字',
        'description_keywords': '維護',
    },
    {
        'rule_id': 3,
        'account': '6003',
        'condition_desc': '小額雜項',
        'max_amount': 30000,
    },
]


def _make_config_mock(rules=None):
    """建立 config_manager mock"""
    mock = MagicMock()
    mock._config_toml = {
        'spt_account_prediction': {
            'rules': rules if rules is not None else MOCK_RULES,
        }
    }
    return mock


def _make_step(config_mock=None):
    """建立 SPTAccountPredictionStep 實例"""
    if config_mock is None:
        config_mock = _make_config_mock()
    with patch(
        'accrual_bot.tasks.spt.steps.spt_account_prediction.config_manager',
        config_mock,
    ):
        from accrual_bot.tasks.spt.steps.spt_account_prediction import (
            SPTAccountPredictionStep,
        )
        return SPTAccountPredictionStep()


def _sample_df():
    """建立測試用 DataFrame"""
    return pd.DataFrame({
        'Department': ['IT', 'HR', 'IT', 'Finance', 'HR'],
        'PO Supplier': ['VendorA', 'VendorB', 'VendorC', 'VendorD', 'VendorE'],
        'Item Description': ['伺服器維護', '辦公用品', '網路設備', '維護合約', '影印紙'],
        'Entry Amount': ['10000', '50000', '20000', '25000', '5000'],
    })


def _make_context(df=None, processing_type='PO'):
    """建立測試用 ProcessingContext"""
    if df is None:
        df = _sample_df()
    return ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202512,
        processing_type=processing_type,
    )


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.unit
class TestSPTAccountPredictionInit:
    """初始化與規則載入"""

    def test_init_loads_rules_from_config(self):
        """Test 1: 從 mock config 正確讀取規則"""
        step = _make_step()
        assert len(step.rules) == 3
        assert step.rules[0]['rule_id'] == 1
        assert step.rules[1]['account'] == '6002'

    def test_init_with_empty_rules(self):
        """Test 2: 空規則列表不報錯"""
        mock = _make_config_mock(rules=[])
        step = _make_step(mock)
        assert step.rules == []


@pytest.mark.unit
class TestInitializeFields:
    """_initialize_fields 測試"""

    def test_initialize_fields_adds_two_columns(self):
        """Test 3: 新增 predicted_account 和 matched_conditions 欄位"""
        step = _make_step()
        df = _sample_df()
        result = step._initialize_fields(df)
        assert 'predicted_account' in result.columns
        assert 'matched_conditions' in result.columns
        assert result['predicted_account'].isna().all()
        assert result['matched_conditions'].isna().all()


@pytest.mark.unit
class TestBuildConditions:
    """_build_conditions 測試"""

    def test_build_conditions_returns_all_false(self):
        """Test 4: matched 初始全為 False"""
        step = _make_step()
        df = _sample_df()
        cond = step._build_conditions(df)
        assert cond.matched.sum() == 0
        assert len(cond.matched) == len(df)


@pytest.mark.unit
class TestApplyPredictionRules:
    """_apply_prediction_rules 測試"""

    def test_department_rule_matches(self):
        """Test 5: 部門條件正確匹配"""
        step = _make_step(_make_config_mock(rules=[MOCK_RULES[0]]))
        df = _sample_df()
        df = step._initialize_fields(df)
        cond = step._build_conditions(df)
        result = step._apply_prediction_rules(df, cond)

        it_mask = result['Department'] == 'IT'
        assert (result.loc[it_mask, 'predicted_account'] == '6001').all()
        assert result.loc[~it_mask, 'predicted_account'].isna().all()

    def test_keyword_rule_matches(self):
        """Test 6: 關鍵字條件正確匹配"""
        step = _make_step(_make_config_mock(rules=[MOCK_RULES[1]]))
        df = _sample_df()
        df = step._initialize_fields(df)
        cond = step._build_conditions(df)
        result = step._apply_prediction_rules(df, cond)

        # '維護' 出現在 index 0 (伺服器維護) 和 index 3 (維護合約)
        assert result.loc[0, 'predicted_account'] == '6002'
        assert result.loc[3, 'predicted_account'] == '6002'
        assert result.loc[1, 'predicted_account'] is pd.NA

    def test_first_match_wins(self):
        """Test 7: 優先順序 — 第一個匹配的規則生效"""
        # IT 部門 + 維護關鍵字 => row 0 應被 rule_id=1 (部門) 先匹配
        step = _make_step()
        df = _sample_df()
        df = step._initialize_fields(df)
        cond = step._build_conditions(df)
        result = step._apply_prediction_rules(df, cond)

        # row 0: IT 部門 → 先被 rule 1 匹配 (account 6001)
        assert result.loc[0, 'predicted_account'] == '6001'
        assert result.loc[0, 'matched_conditions'] == 'IT部門'

    def test_no_rules_returns_unchanged(self):
        """Test 8: 無規則時 DataFrame 不變"""
        step = _make_step(_make_config_mock(rules=[]))
        df = _sample_df()
        df = step._initialize_fields(df)
        cond = step._build_conditions(df)
        result = step._apply_prediction_rules(df, cond)
        assert result['predicted_account'].isna().all()


@pytest.mark.unit
class TestBuildRuleCondition:
    """_build_rule_condition 測試"""

    def test_max_amount_condition(self):
        """Test 9: max_amount 條件正確過濾"""
        step = _make_step()
        df = _sample_df()
        already_matched = pd.Series([False] * len(df), index=df.index)

        rule = {
            'rule_id': 99,
            'account': '9999',
            'condition_desc': '小額',
            'max_amount': 30000,
        }
        condition = step._build_rule_condition(df, rule, already_matched)

        # Entry Amount: 10000, 50000, 20000, 25000, 5000
        # < 30000: index 0, 2, 3, 4
        assert condition[0] is True or condition[0] == True  # noqa: E712
        assert condition[1] is False or condition[1] == False  # noqa: E712
        assert condition.sum() == 4


@pytest.mark.unit
class TestValidateInput:
    """validate_input 測試"""

    @pytest.mark.asyncio
    async def test_validate_passes_with_required_columns(self):
        """Test 10: 有必要欄位時驗證通過"""
        step = _make_step()
        ctx = _make_context()
        assert await step.validate_input(ctx) is True

    @pytest.mark.asyncio
    async def test_validate_fails_with_missing_columns(self):
        """Test 11: 缺少欄位時驗證失敗"""
        step = _make_step()
        df = pd.DataFrame({'Department': ['IT'], 'Entry Amount': [100]})
        ctx = _make_context(df=df)
        assert await step.validate_input(ctx) is False


@pytest.mark.unit
class TestGenerateStatistics:
    """_generate_statistics 測試"""

    def test_statistics_calculates_correctly(self):
        """Test 12: 統計數值正確"""
        step = _make_step()
        df = pd.DataFrame({
            'predicted_account': ['6001', '6002', pd.NA, pd.NA, '6001'],
            'matched_conditions': ['R1', 'R2', pd.NA, pd.NA, 'R1'],
        })
        stats = step._generate_statistics(df)

        assert stats['total_count'] == 5
        assert stats['matched_count'] == 3
        assert stats['unmatched_count'] == 2
        assert abs(stats['match_rate'] - 60.0) < 0.01
        assert stats['account_distribution']['6001'] == 2
        assert stats['account_distribution']['6002'] == 1
