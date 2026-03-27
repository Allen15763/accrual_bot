"""
SPT 會計標籤標記步驟測試

測試 SPTStatusLabelStep 的核心功能：
- 步驟初始化與配置載入
- _build_rule_condition() 各條件類型
  (keywords, supplier, dept, dept_prefix, dept_exclude_prefix,
   dept_include, dept_exclude, requester, status_value_contains,
   remarked_by_procurement)
- _apply_rules() 規則應用與優先級
- _update_accrual_col() 是否估計入帳更新
- _get_status_column() 動態欄位判斷
- execute() 完整流程
- validate_input() 輸入驗證
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# 共用 helpers
# ============================================================

def _make_df(n: int = 5, **overrides) -> pd.DataFrame:
    """建立測試用 DataFrame，包含 SPTStatusLabelStep 所需欄位"""
    base = {
        'Item Description': [f'Test Item {i}' for i in range(n)],
        'Department': [f'IT-{i:03d}' for i in range(n)],
        'Supplier Name': [f'Vendor {i}' for i in range(n)],
        'Requester': [f'User {i}' for i in range(n)],
        'PO狀態': [pd.NA] * n,
        'Remarked by FN': [pd.NA] * n,
        'matched_condition_on_status': [pd.NA] * n,
        '是否估計入帳': ['N'] * n,
        'Remarked by Procurement': [pd.NA] * n,
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _make_context(df: pd.DataFrame = None) -> ProcessingContext:
    """建立測試用 ProcessingContext"""
    if df is None:
        df = _make_df()
    return ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202503,
        processing_type='PO',
    )


def _empty_rules_config():
    """空規則配置"""
    return {'spt_status_label_rules': {}}


def _priority_rules_config(rules: dict):
    """只含 priority_conditions 的配置"""
    return {'spt_status_label_rules': {'priority_conditions': rules}}


def _erm_rules_config(rules: dict):
    """只含 erm_conditions 的配置"""
    return {'spt_status_label_rules': {'erm_conditions': rules}}


def _full_rules_config(priority: dict, erm: dict):
    """同時含 priority + erm 的配置"""
    return {
        'spt_status_label_rules': {
            'priority_conditions': priority,
            'erm_conditions': erm,
        }
    }


# ============================================================
# 初始化測試
# ============================================================

@pytest.mark.unit
class TestSPTStatusLabelStepInit:
    """SPTStatusLabelStep 初始化測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_default_name(self, mock_cm):
        """測試預設步驟名稱"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        assert step.name == "Accounting_Label_Marking"

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_custom_name(self, mock_cm):
        """測試自訂步驟名稱"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep(name="CustomLabel")
        assert step.name == "CustomLabel"

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_loads_priority_rules(self, mock_cm):
        """測試成功載入 priority_conditions 規則"""
        mock_cm._config_toml = _priority_rules_config({
            'rule_a': {'remark': 'A', 'status': '已完成'},
            'rule_b': {'remark': 'B', 'status': '未完成'},
        })
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        assert len(step.priority_rules) == 2

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_loads_erm_rules(self, mock_cm):
        """測試成功載入 erm_conditions 規則"""
        mock_cm._config_toml = _erm_rules_config({
            'erm_a': {'remark': 'EA'},
        })
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        assert len(step.erm_rules) == 1

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_empty_config_returns_empty_rules(self, mock_cm):
        """測試配置為空時，規則為空字典"""
        mock_cm._config_toml = {}
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        assert step.priority_rules == {}
        assert step.erm_rules == {}


# ============================================================
# _get_status_column 測試
# ============================================================

@pytest.mark.unit
class TestGetStatusColumn:
    """_get_status_column 動態判斷狀態欄位"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_po_status_column(self, mock_cm):
        """測試有 PO狀態 欄位時返回 PO狀態"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        df = pd.DataFrame({'PO狀態': [pd.NA], 'col': [1]})
        assert step._get_status_column(df) == 'PO狀態'

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_pr_status_column(self, mock_cm):
        """測試有 PR狀態 欄位時返回 PR狀態"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        df = pd.DataFrame({'PR狀態': [pd.NA], 'col': [1]})
        assert step._get_status_column(df) == 'PR狀態'

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_fallback_creates_po_status(self, mock_cm):
        """測試兩者都不存在時，自動創建 PO狀態"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        df = pd.DataFrame({'col': [1]})
        result = step._get_status_column(df)
        assert result == 'PO狀態'
        assert 'PO狀態' in df.columns


# ============================================================
# _build_rule_condition 測試 — 各條件類型
# ============================================================

@pytest.mark.unit
class TestBuildRuleCondition:
    """_build_rule_condition 條件構建測試"""

    @pytest.fixture(autouse=True)
    def setup_step(self):
        with patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager') as mock_cm:
            mock_cm._config_toml = _empty_rules_config()
            from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
            self.step = SPTStatusLabelStep()
            self.step.status_column = 'PO狀態'

    def test_keywords_default_field(self):
        """測試 keywords 條件 — 預設搜尋 Item Description"""
        df = _make_df(3, **{
            'Item Description': ['Office supplies', 'IT equipment', 'Office chair'],
        })
        rule = {'keywords': 'Office'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_keywords_custom_field(self):
        """測試 keywords 條件 — 指定其他欄位"""
        df = _make_df(3, **{
            'Department': ['IT-Sales', 'HR-Admin', 'IT-Ops'],
        })
        rule = {'keywords': 'IT', 'field': 'Department'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_supplier_exact_match(self):
        """測試 supplier 精確匹配"""
        df = _make_df(3, **{
            'Supplier Name': ['Vendor A', 'Vendor B', 'Vendor A'],
        })
        rule = {'supplier': 'Vendor A'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_dept_exact_match(self):
        """測試 dept 精確匹配"""
        df = _make_df(3, **{
            'Department': ['IT-001', 'HR-002', 'IT-001'],
        })
        rule = {'dept': 'IT-001'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_dept_prefix(self):
        """測試 dept_prefix 前綴匹配"""
        df = _make_df(3, **{
            'Department': ['IT-001', 'HR-002', 'IT-003'],
        })
        rule = {'dept_prefix': 'IT'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_dept_exclude_prefix(self):
        """測試 dept_exclude_prefix 非前綴匹配"""
        df = _make_df(3, **{
            'Department': ['IT-001', 'HR-002', 'IT-003'],
        })
        rule = {'dept_exclude_prefix': 'IT'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [False, True, False]

    def test_dept_include_regex(self):
        """測試 dept_include 包含匹配 (regex)"""
        df = _make_df(3, **{
            'Department': ['IT-001', 'HR-Admin', 'IT-Sales'],
        })
        rule = {'dept_include': r'IT.*'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_dept_exclude_regex(self):
        """測試 dept_exclude 不包含匹配 (regex)"""
        df = _make_df(3, **{
            'Department': ['IT-001', 'HR-Admin', 'IT-Sales'],
        })
        rule = {'dept_exclude': r'IT'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [False, True, False]

    def test_requester_exact_match(self):
        """測試 requester 精確匹配"""
        df = _make_df(3, **{
            'Requester': ['Alice', 'Bob', 'Alice'],
        })
        rule = {'requester': 'Alice'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_status_value_contains(self):
        """測試 status_value_contains regex 匹配"""
        df = _make_df(3, **{
            'PO狀態': ['已完成(not_billed)', '未完成', '已完成(fully_billed)'],
        })
        rule = {'status_value_contains': '已完成'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_remarked_by_procurement(self):
        """測試 remarked_by_procurement 精確匹配"""
        df = _make_df(3, **{
            'Remarked by Procurement': ['已確認', pd.NA, '已確認'],
        })
        rule = {'remarked_by_procurement': '已確認'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]

    def test_combined_conditions(self):
        """測試多條件組合 (AND 邏輯)"""
        df = _make_df(4, **{
            'Item Description': ['Office supplies', 'Office chair', 'IT cable', 'Office pen'],
            'Department': ['IT-001', 'HR-002', 'IT-003', 'IT-004'],
        })
        rule = {'keywords': 'Office', 'dept_prefix': 'IT'}
        cond = self.step._build_rule_condition(df, rule)
        # Office + IT prefix: row 0 (Office+IT), row 3 (Office+IT)
        assert cond.tolist() == [True, False, False, True]

    def test_empty_rule_matches_all(self):
        """測試空規則匹配所有記錄"""
        df = _make_df(3)
        rule = {}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.all()

    def test_keywords_with_na_values(self):
        """測試 keywords 條件在 NA 值時不匹配"""
        df = _make_df(3, **{
            'Item Description': ['Office supplies', pd.NA, 'Office chair'],
        })
        rule = {'keywords': 'Office'}
        cond = self.step._build_rule_condition(df, rule)
        assert cond.tolist() == [True, False, True]


# ============================================================
# _apply_rules 測試
# ============================================================

@pytest.mark.unit
class TestApplyRules:
    """_apply_rules 規則應用測試"""

    @pytest.fixture(autouse=True)
    def setup_step(self):
        with patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager') as mock_cm:
            mock_cm._config_toml = _empty_rules_config()
            from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
            self.step = SPTStatusLabelStep()
            self.step.status_column = 'PO狀態'

    def test_apply_rules_updates_remark(self):
        """測試規則匹配時更新 Remarked by FN"""
        df = _make_df(3, **{
            'Item Description': ['Office supplies', 'IT equipment', 'Office chair'],
        })
        rules = {
            'office_rule': {
                'keywords': 'Office',
                'remark': '辦公用品',
                'note': '辦公用品條件',
                'status': '已標記',
            }
        }
        stats = self.step._apply_rules(df, rules, update_status=True)
        assert stats['office_rule'] == 2
        assert df.loc[0, 'Remarked by FN'] == '辦公用品'
        assert df.loc[2, 'Remarked by FN'] == '辦公用品'

    def test_apply_rules_update_status_true(self):
        """測試 update_status=True 時更新狀態欄位"""
        df = _make_df(2, **{
            'Item Description': ['Office supplies', 'IT equipment'],
        })
        rules = {
            'r1': {'keywords': 'Office', 'remark': 'R1', 'status': '特殊狀態', 'note': 'n1'},
        }
        self.step._apply_rules(df, rules, update_status=True)
        assert df.loc[0, 'PO狀態'] == '特殊狀態'
        # 不匹配的不應改變
        assert pd.isna(df.loc[1, 'PO狀態'])

    def test_apply_rules_update_status_false(self):
        """測試 update_status=False 時不更新狀態欄位（ERM 條件行為）"""
        df = _make_df(2, **{
            'Item Description': ['Office supplies', 'IT equipment'],
        })
        rules = {
            'r1': {'keywords': 'Office', 'remark': 'R1', 'status': '特殊狀態', 'note': 'n1'},
        }
        self.step._apply_rules(df, rules, update_status=False)
        # 狀態不應被更新
        assert pd.isna(df.loc[0, 'PO狀態'])
        # 備註應被更新
        assert df.loc[0, 'Remarked by FN'] == 'R1'

    def test_apply_rules_no_matching(self):
        """測試沒有匹配的規則時返回空 stats"""
        df = _make_df(2, **{
            'Item Description': ['IT equipment', 'Server rack'],
        })
        rules = {
            'r1': {'keywords': 'Office', 'remark': 'R1', 'note': 'n1'},
        }
        stats = self.step._apply_rules(df, rules, update_status=True)
        assert stats == {}

    def test_apply_rules_empty_rules(self):
        """測試空規則字典"""
        df = _make_df(2)
        stats = self.step._apply_rules(df, {}, update_status=True)
        assert stats == {}

    def test_apply_rules_priority_ordering(self):
        """測試規則按順序應用，後面的規則覆蓋前面的（同匹配行）"""
        df = _make_df(1, **{
            'Item Description': ['Office supplies'],
            'Department': ['IT-001'],
        })
        rules = {
            'rule_first': {'keywords': 'Office', 'remark': 'First', 'status': '狀態A', 'note': 'n1'},
            'rule_second': {'dept_prefix': 'IT', 'remark': 'Second', 'status': '狀態B', 'note': 'n2'},
        }
        self.step._apply_rules(df, rules, update_status=True)
        # 兩個規則都匹配，後者覆蓋前者
        assert df.loc[0, 'Remarked by FN'] == 'Second'
        assert df.loc[0, 'PO狀態'] == '狀態B'

    def test_apply_rules_sets_matched_condition(self):
        """測試規則匹配時設置 matched_condition_on_status"""
        df = _make_df(1, **{
            'Item Description': ['Office supplies'],
        })
        rules = {
            'r1': {'keywords': 'Office', 'remark': 'R1', 'note': '辦公條件', 'status': 'S1'},
        }
        self.step._apply_rules(df, rules, update_status=True)
        assert df.loc[0, 'matched_condition_on_status'] == '辦公條件'


# ============================================================
# _update_accrual_col 測試
# ============================================================

@pytest.mark.unit
class TestUpdateAccrualCol:
    """_update_accrual_col 測試"""

    @pytest.fixture(autouse=True)
    def setup_step(self):
        with patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager') as mock_cm:
            mock_cm._config_toml = _empty_rules_config()
            from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
            self.step = SPTStatusLabelStep()
            self.step.status_column = 'PO狀態'

    def test_completed_status_sets_y(self):
        """測試「已完成」狀態的記錄，是否估計入帳設為 Y"""
        df = pd.DataFrame({
            'PO狀態': ['已完成(not_billed)', '未完成', '已完成(fully_billed)'],
            '是否估計入帳': ['N', 'N', 'N'],
        })
        result = self.step._update_accrual_col(df)
        assert result.loc[0, '是否估計入帳'] == 'Y'
        assert result.loc[1, '是否估計入帳'] == 'N'
        assert result.loc[2, '是否估計入帳'] == 'Y'

    def test_non_completed_status_unchanged(self):
        """測試非「已完成」狀態的記錄不改變"""
        df = pd.DataFrame({
            'PO狀態': ['已入帳', 'Check收貨', pd.NA],
            '是否估計入帳': ['N', 'N', 'N'],
        })
        result = self.step._update_accrual_col(df)
        assert (result['是否估計入帳'] == 'N').all()


# ============================================================
# execute 完整流程測試
# ============================================================

@pytest.mark.unit
class TestSPTStatusLabelExecute:
    """execute 完整流程測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_execute_success(self, mock_cm):
        """測試完整流程成功執行"""
        mock_cm._config_toml = _full_rules_config(
            priority={
                'p1': {
                    'keywords': 'Office',
                    'remark': '辦公',
                    'note': 'n1',
                    'status': '已完成',
                },
            },
            erm={
                'e1': {
                    'dept_prefix': 'IT',
                    'remark': 'IT部門',
                    'note': 'n2',
                },
            },
        )
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = _make_df(3, **{
            'Item Description': ['Office supplies', 'IT equipment', 'Office chair'],
            'Department': ['IT-001', 'IT-002', 'HR-001'],
        })
        ctx = _make_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.data is not None
        assert 'priority_labeled' in result.metadata
        assert 'erm_labeled' in result.metadata

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_execute_with_no_rules(self, mock_cm):
        """測試無規則時仍成功執行（0 筆標記）"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        ctx = _make_context(_make_df(3))
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['priority_labeled'] == 0
        assert result.metadata['erm_labeled'] == 0

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_execute_updates_context_data(self, mock_cm):
        """測試 execute 更新 context.data"""
        mock_cm._config_toml = _priority_rules_config({
            'p1': {'keywords': 'Office', 'remark': '辦公', 'note': 'n1', 'status': '已完成'},
        })
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = _make_df(2, **{'Item Description': ['Office supplies', 'IT equipment']})
        ctx = _make_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        # context.data 應被更新
        updated_df = ctx.data
        assert updated_df is not None
        assert len(updated_df) == 2

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_execute_pr_status_column(self, mock_cm):
        """測試 PR 類型時使用 PR狀態 欄位"""
        mock_cm._config_toml = _priority_rules_config({
            'p1': {'keywords': 'Office', 'remark': '辦公', 'note': 'n1', 'status': '已完成'},
        })
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = pd.DataFrame({
            'Item Description': ['Office supplies', 'IT equipment'],
            'Department': ['IT-001', 'HR-002'],
            'Supplier Name': ['V1', 'V2'],
            'PR狀態': [pd.NA, pd.NA],
            'Remarked by FN': [pd.NA, pd.NA],
            'matched_condition_on_status': [pd.NA, pd.NA],
            '是否估計入帳': ['N', 'N'],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT', processing_date=202503, processing_type='PR')
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.data.loc[0, 'PR狀態'] == '已完成'

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_execute_metadata_statistics(self, mock_cm):
        """測試 metadata 包含完整統計資訊"""
        mock_cm._config_toml = _full_rules_config(
            priority={'p1': {'keywords': 'Office', 'remark': 'R1', 'note': 'n1', 'status': 'S1'}},
            erm={'e1': {'dept_prefix': 'HR', 'remark': 'R2', 'note': 'n2'}},
        )
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = _make_df(3, **{
            'Item Description': ['Office supplies', 'IT equipment', 'Office chair'],
            'Department': ['IT-001', 'HR-002', 'HR-003'],
        })
        ctx = _make_context(df)
        result = await step.execute(ctx)

        assert 'statistics' in result.metadata
        stats = result.metadata['statistics']
        assert stats['total_records'] == 3
        assert 'label_rate' in stats

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_execute_error_handling(self, mock_cm):
        """測試 execute 異常時返回 FAILED"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        # 用 None data 強制觸發 AttributeError
        ctx = _make_context(_make_df(1))
        ctx.data = None  # 強制讓 context.data.copy() 拋出 AttributeError

        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED
        assert result.error is not None


# ============================================================
# validate_input 測試
# ============================================================

@pytest.mark.unit
class TestSPTStatusLabelValidateInput:
    """validate_input 輸入驗證測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_validate_success(self, mock_cm):
        """測試完整數據驗證通過"""
        mock_cm._config_toml = _priority_rules_config({
            'r1': {'remark': 'R1'},
        })
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        ctx = _make_context(_make_df(3))
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_validate_empty_data_returns_false(self, mock_cm):
        """測試空 DataFrame 驗證失敗"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        ctx = ProcessingContext(data=pd.DataFrame(), entity_type='SPT', processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_validate_missing_item_description(self, mock_cm):
        """測試缺少 Item Description 欄位驗證失敗"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = pd.DataFrame({
            'Department': ['IT-001'],
            'Supplier Name': ['V1'],
        })
        ctx = _make_context(df)
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_validate_missing_department(self, mock_cm):
        """測試缺少 Department 欄位驗證失敗"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = pd.DataFrame({
            'Item Description': ['Test'],
            'Supplier Name': ['V1'],
        })
        ctx = _make_context(df)
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_validate_missing_supplier(self, mock_cm):
        """測試缺少 Supplier 欄位驗證失敗"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = pd.DataFrame({
            'Item Description': ['Test'],
            'Department': ['IT-001'],
        })
        ctx = _make_context(df)
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_validate_creates_remark_column_if_missing(self, mock_cm):
        """測試驗證時自動創建 Remarked by FN 欄位"""
        mock_cm._config_toml = _priority_rules_config({'r1': {'remark': 'R'}})
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        df = pd.DataFrame({
            'Item Description': ['Test'],
            'Department': ['IT-001'],
            'Supplier Name': ['V1'],
        })
        ctx = _make_context(df)
        result = await step.validate_input(ctx)
        assert result is True
        assert 'Remarked by FN' in ctx.data.columns

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_validate_no_rules_still_passes(self, mock_cm):
        """測試無規則時驗證仍通過（僅 warning）"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        ctx = _make_context(_make_df(1))
        result = await step.validate_input(ctx)
        assert result is True


# ============================================================
# _load_rules 測試
# ============================================================

@pytest.mark.unit
class TestLoadRules:
    """_load_rules 配置載入測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_load_rules_warns_missing_remark(self, mock_cm):
        """測試規則缺少 remark 時記錄警告（非 exception 規則）"""
        mock_cm._config_toml = _priority_rules_config({
            'normal_rule': {'status': 'S1'},  # 缺少 remark
        })
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        # 不應拋出異常，但 rules 仍被載入
        assert 'normal_rule' in step.priority_rules

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_load_rules_exception_rules_skip_remark_check(self, mock_cm):
        """測試 exception 規則（如 hris_bug）不需要 remark"""
        mock_cm._config_toml = _priority_rules_config({
            'hris_bug': {'status': 'BUG'},
        })
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        assert 'hris_bug' in step.priority_rules

    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    def test_load_rules_config_exception_returns_empty(self, mock_cm):
        """測試配置讀取異常時返回空規則"""
        # 讓 _config_toml.get 拋出異常
        mock_cm._config_toml = MagicMock()
        mock_cm._config_toml.get.side_effect = RuntimeError("config error")
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()
        assert step.priority_rules == {}
        assert step.erm_rules == {}


# ============================================================
# rollback 測試
# ============================================================

@pytest.mark.unit
class TestRollback:
    """rollback 測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_evaluation_accountant.config_manager')
    async def test_rollback_does_not_raise(self, mock_cm):
        """測試 rollback 不拋出異常"""
        mock_cm._config_toml = _empty_rules_config()
        from accrual_bot.tasks.spt.steps.spt_evaluation_accountant import SPTStatusLabelStep
        step = SPTStatusLabelStep()

        ctx = _make_context(_make_df(1))
        await step.rollback(ctx, RuntimeError("test error"))
        # 只要不拋出異常即可
