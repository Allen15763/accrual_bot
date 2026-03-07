"""SPX 配置驅動條件引擎單元測試

測試 SPXConditionEngine 的核心功能：
- 規則載入與排序
- 條件評估（各種 check type）
- 規則應用與狀態更新
- 優先順序處理
- 邊界情況與錯誤處理
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepStatus, StepResult


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_engine_deps():
    """Mock SPXConditionEngine 的外部依賴"""
    with patch('accrual_bot.tasks.spx.steps.spx_condition_engine.config_manager') as mock_cm, \
         patch('accrual_bot.tasks.spx.steps.spx_condition_engine.get_logger', return_value=MagicMock()):
        mock_cm._config_toml = {
            'spx_erm_status_rules': {
                'conditions': []
            },
            'spx_status_stage1_rules': {
                'conditions': []
            },
            'spx': {
                'deposit_keywords': '訂金|押金|保證金',
                'kiosk_suppliers': ['益欣'],
                'locker_suppliers': ['掌櫃'],
            },
            'fa_accounts': {
                'spx': ['199999'],
            },
        }
        yield mock_cm


@pytest.fixture
def engine_with_rules(mock_engine_deps):
    """建立包含測試規則的引擎"""
    mock_engine_deps._config_toml['spx_erm_status_rules'] = {
        'conditions': [
            {
                'priority': 10,
                'status_value': '已完成',
                'note': '數量相符且已到期',
                'combine': 'and',
                'checks': [
                    {'type': 'qty_matched'},
                    {'type': 'erm_le_date'},
                ],
            },
            {
                'priority': 20,
                'status_value': '未完成',
                'note': '未到期',
                'combine': 'and',
                'checks': [
                    {'type': 'erm_gt_date'},
                ],
            },
            {
                'priority': 30,
                'status_value': '格式錯誤，退單',
                'note': '格式錯誤',
                'combine': 'and',
                'checks': [
                    {'type': 'format_error'},
                ],
            },
        ]
    }
    from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
    return SPXConditionEngine('spx_erm_status_rules')


@pytest.fixture
def engine_empty(mock_engine_deps):
    """建立無規則的引擎"""
    from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
    return SPXConditionEngine('spx_erm_status_rules')


@pytest.fixture
def sample_df():
    """建立測試用 DataFrame"""
    return pd.DataFrame({
        'PO狀態': [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
        'Expected Received Month_轉換格式': [202512, 202601, 202512, 202511, 202512],
        'YMs of Item Description': [
            '202510,202512',
            '202510,202512',
            '100001,100002',  # 格式錯誤
            '202510,202512',
            '202510,202512',
        ],
        'Entry Quantity': ['100', '200', '100', '100', '100'],
        'Received Quantity': ['100', '200', '50', '100', '100'],
        'Entry Amount': ['10000', '20000', '10000', '10000', '10000'],
        'Entry Billed Amount': ['0', '0', '0', '10000', '0'],
        'Billed Quantity': ['0', '0', '0', '100', '0'],
        'GL#': ['100000', '100001', '199999', '100000', '100001'],
        'Remarked by Procurement': [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
        'Remarked by 上月 FN': [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
        'Remarked by 上月 FN PR': [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
        'matched_condition_on_status': [pd.NA] * 5,
    })


@pytest.fixture
def engine_context():
    """建立引擎執行 context"""
    return {
        'processing_date': 202512,
        'prebuilt_masks': {},
    }


# ============================================================
# 規則載入測試
# ============================================================

class TestRuleLoading:
    """測試規則載入與排序"""

    @pytest.mark.unit
    def test_load_rules_empty(self, engine_empty):
        """無條件時應回傳空列表"""
        assert engine_empty.rules == []

    @pytest.mark.unit
    def test_load_rules_sorted_by_priority(self, engine_with_rules):
        """規則應按 priority 排序"""
        priorities = [r['priority'] for r in engine_with_rules.rules]
        assert priorities == sorted(priorities)

    @pytest.mark.unit
    def test_load_rules_count(self, engine_with_rules):
        """應載入正確數量的規則"""
        assert len(engine_with_rules.rules) == 3

    @pytest.mark.unit
    def test_load_missing_section(self, mock_engine_deps):
        """不存在的配置區段應回傳空規則"""
        from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
        engine = SPXConditionEngine('nonexistent_section')
        assert engine.rules == []


# ============================================================
# 規則應用測試
# ============================================================

class TestApplyRules:
    """測試規則應用邏輯"""

    @pytest.mark.unit
    def test_apply_rules_empty_engine(self, engine_empty, sample_df, engine_context):
        """無規則時不應修改任何狀態"""
        df_result, stats = engine_empty.apply_rules(
            sample_df.copy(), 'PO狀態', engine_context
        )
        # 所有狀態應仍為 NA
        assert df_result['PO狀態'].isna().all()
        assert stats == {}

    @pytest.mark.unit
    def test_apply_rules_with_prebuilt_masks(self, engine_with_rules, sample_df, engine_context):
        """使用預先計算的 mask 應能正確應用規則"""
        df = sample_df.copy()
        # 提供 prebuilt_masks
        engine_context['prebuilt_masks'] = {
            'qty_matched': df['Entry Quantity'] == df['Received Quantity'],
            'erm_le_date': df['Expected Received Month_轉換格式'] <= 202512,
            'erm_gt_date': df['Expected Received Month_轉換格式'] > 202512,
            'format_error': df['YMs of Item Description'] == '100001,100002',
        }
        df_result, stats = engine_with_rules.apply_rules(
            df, 'PO狀態', engine_context
        )
        # 驗證有狀態被賦值
        assert not df_result['PO狀態'].isna().all()

    @pytest.mark.unit
    def test_priority_ordering(self, engine_with_rules, sample_df, engine_context):
        """高優先級規則應先執行，低優先級不覆蓋已有狀態"""
        df = sample_df.copy()
        engine_context['prebuilt_masks'] = {
            'qty_matched': pd.Series([True] * 5),
            'erm_le_date': pd.Series([True] * 5),
            'erm_gt_date': pd.Series([False] * 5),
            'format_error': pd.Series([False] * 5),
        }
        df_result, stats = engine_with_rules.apply_rules(
            df, 'PO狀態', engine_context
        )
        # priority=10 的「已完成」應先命中所有列，後續規則不應覆蓋
        assert (df_result['PO狀態'] == '已完成').all()

    @pytest.mark.unit
    def test_already_has_status_skipped(self, engine_with_rules, sample_df, engine_context):
        """已有狀態的列不應被引擎覆蓋"""
        df = sample_df.copy()
        df.loc[0, 'PO狀態'] = '已關單'
        engine_context['prebuilt_masks'] = {
            'qty_matched': pd.Series([True] * 5),
            'erm_le_date': pd.Series([True] * 5),
            'erm_gt_date': pd.Series([False] * 5),
            'format_error': pd.Series([False] * 5),
        }
        df_result, _ = engine_with_rules.apply_rules(
            df, 'PO狀態', engine_context
        )
        # 第 0 列應保留原始狀態
        assert df_result.loc[0, 'PO狀態'] == '已關單'

    @pytest.mark.unit
    def test_apply_to_filter(self, mock_engine_deps):
        """apply_to 過濾應正確排除不適用的處理類型"""
        mock_engine_deps._config_toml['spx_erm_status_rules'] = {
            'conditions': [
                {
                    'priority': 10,
                    'status_value': '僅PO',
                    'note': '僅適用PO',
                    'combine': 'and',
                    'apply_to': ['PO'],
                    'checks': [{'type': 'erm_le_date'}],
                },
            ]
        }
        from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
        engine = SPXConditionEngine('spx_erm_status_rules')
        df = pd.DataFrame({
            'PR狀態': [pd.NA],
            'Expected Received Month_轉換格式': [202512],
        })
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        df_result, stats = engine.apply_rules(
            df, 'PR狀態', context, processing_type='PR'
        )
        # PR 類型不應被此規則命中
        assert df_result['PR狀態'].isna().all()

    @pytest.mark.unit
    def test_override_statuses(self, mock_engine_deps):
        """規則設定 override_statuses 時應能覆蓋指定的既有狀態"""
        mock_engine_deps._config_toml['spx_erm_status_rules'] = {
            'conditions': [
                {
                    'priority': 10,
                    'status_value': '已完成_覆蓋',
                    'note': '可覆蓋',
                    'combine': 'and',
                    'override_statuses': ['暫定'],
                    'checks': [{'type': 'erm_le_date'}],
                },
            ]
        }
        from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
        engine = SPXConditionEngine('spx_erm_status_rules')
        df = pd.DataFrame({
            'PO狀態': ['暫定', pd.NA],
            'Expected Received Month_轉換格式': [202512, 202512],
        })
        context = {
            'processing_date': 202512,
            'prebuilt_masks': {
                'erm_le_date': pd.Series([True, True]),
            },
        }
        df_result, _ = engine.apply_rules(df, 'PO狀態', context)
        # 「暫定」狀態應被覆蓋
        assert df_result.loc[0, 'PO狀態'] == '已完成_覆蓋'

    @pytest.mark.unit
    def test_matched_condition_note_recorded(self, engine_with_rules, sample_df, engine_context):
        """命中規則時應在 matched_condition_on_status 記錄備註"""
        df = sample_df.copy()
        engine_context['prebuilt_masks'] = {
            'qty_matched': pd.Series([True] * 5),
            'erm_le_date': pd.Series([True] * 5),
            'erm_gt_date': pd.Series([False] * 5),
            'format_error': pd.Series([False] * 5),
        }
        df_result, _ = engine_with_rules.apply_rules(
            df, 'PO狀態', engine_context
        )
        # 被命中的列應有備註
        assert df_result['matched_condition_on_status'].notna().any()


# ============================================================
# 條件評估測試
# ============================================================

class TestEvaluateCheck:
    """測試單一 check 評估"""

    @pytest.mark.unit
    def test_contains_check(self, engine_with_rules):
        """contains 類型應能正確匹配正則"""
        df = pd.DataFrame({'Description': ['含訂金的品項', '一般品項', '押金']})
        check = {'type': 'contains', 'field': 'Description', 'pattern': '訂金|押金'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_not_contains_check(self, engine_with_rules):
        """not_contains 應為 contains 的反向"""
        df = pd.DataFrame({'Description': ['含訂金', '一般']})
        check = {'type': 'not_contains', 'field': 'Description', 'pattern': '訂金'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [False, True]

    @pytest.mark.unit
    def test_equals_check(self, engine_with_rules):
        """equals 類型應能精確比對"""
        df = pd.DataFrame({'Status': ['Open', 'Closed', 'Open']})
        check = {'type': 'equals', 'field': 'Status', 'value': 'Closed'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_in_list_check(self, engine_with_rules):
        """in_list 類型應能匹配列表中的值"""
        df = pd.DataFrame({'GL#': ['100000', '199999', '200000']})
        check = {'type': 'in_list', 'field': 'GL#', 'values': ['199999', '200000']}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [False, True, True]

    @pytest.mark.unit
    def test_is_null_check(self, engine_with_rules):
        """is_null 應正確識別空值"""
        df = pd.DataFrame({'Col': [pd.NA, 'value', '', 'nan']})
        check = {'type': 'is_null', 'field': 'Col'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        # NA、空字串、'nan' 都算 null
        assert result.iloc[0] == True
        assert result.iloc[1] == False
        assert result.iloc[2] == True
        assert result.iloc[3] == True

    @pytest.mark.unit
    def test_is_not_null_check(self, engine_with_rules):
        """is_not_null 應正確識別非空值"""
        df = pd.DataFrame({'Col': [pd.NA, 'value', '']})
        check = {'type': 'is_not_null', 'field': 'Col'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.iloc[1] == True

    @pytest.mark.unit
    def test_missing_field_returns_none(self, engine_with_rules):
        """欄位不存在時 contains 應回傳 None"""
        df = pd.DataFrame({'OtherCol': ['val']})
        check = {'type': 'contains', 'field': 'NonExistent', 'pattern': 'test'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is None

    @pytest.mark.unit
    def test_unknown_check_type_returns_none(self, engine_with_rules):
        """未知的 check type 應回傳 None"""
        df = pd.DataFrame({'Col': ['val']})
        check = {'type': 'unknown_type', 'field': 'Col'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is None

    @pytest.mark.unit
    def test_prebuilt_mask_used(self, engine_with_rules):
        """prebuilt_masks 中存在的 check type 應直接使用"""
        df = pd.DataFrame({'Col': [1, 2, 3]})
        prebuilt = pd.Series([True, False, True])
        context = {'prebuilt_masks': {'custom_mask': prebuilt}}
        check = {'type': 'custom_mask'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]


# ============================================================
# 組合 mask 測試
# ============================================================

class TestBuildCombinedMask:
    """測試組合 mask 建構"""

    @pytest.mark.unit
    def test_combine_and(self, engine_with_rules):
        """'and' 組合應為交集"""
        df = pd.DataFrame({
            'A': ['yes', 'yes', 'no'],
            'B': ['yes', 'no', 'yes'],
        })
        checks = [
            {'type': 'equals', 'field': 'A', 'value': 'yes'},
            {'type': 'equals', 'field': 'B', 'value': 'yes'},
        ]
        mask = engine_with_rules._build_combined_mask(
            df, checks, 'and', 'PO狀態', {}
        )
        assert mask is not None
        assert mask.tolist() == [True, False, False]

    @pytest.mark.unit
    def test_combine_or(self, engine_with_rules):
        """'or' 組合應為聯集"""
        df = pd.DataFrame({
            'A': ['yes', 'no', 'no'],
            'B': ['no', 'yes', 'no'],
        })
        checks = [
            {'type': 'equals', 'field': 'A', 'value': 'yes'},
            {'type': 'equals', 'field': 'B', 'value': 'yes'},
        ]
        mask = engine_with_rules._build_combined_mask(
            df, checks, 'or', 'PO狀態', {}
        )
        assert mask is not None
        assert mask.tolist() == [True, True, False]

    @pytest.mark.unit
    def test_type_placeholder_replacement(self, engine_with_rules):
        """{TYPE} 佔位符應被實際處理類型替換"""
        df = pd.DataFrame({'PO狀態': [pd.NA, '已完成']})
        checks = [{'type': 'is_null', 'field': '{TYPE}狀態'}]
        mask = engine_with_rules._build_combined_mask(
            df, checks, 'and', 'PO狀態', {},
            processing_type='PO'
        )
        assert mask is not None
        assert mask.iloc[0] == True
        # 第 1 列有值，不為 null
        assert mask.iloc[1] == False


# ============================================================
# 值解析測試
# ============================================================

class TestValueResolution:
    """測試值解析輔助方法"""

    @pytest.mark.unit
    def test_resolve_status_value_direct(self, engine_with_rules):
        """直接值應正確回傳"""
        rule = {'status_value': '已完成'}
        assert engine_with_rules._resolve_status_value(rule) == '已完成'

    @pytest.mark.unit
    def test_resolve_status_value_empty(self, engine_with_rules):
        """無值時應回傳空字串"""
        rule = {}
        assert engine_with_rules._resolve_status_value(rule) == ''

    @pytest.mark.unit
    def test_resolve_ref_dot_path(self, engine_with_rules, mock_engine_deps):
        """點分隔路徑應能正確解析"""
        result = engine_with_rules._resolve_ref('spx.deposit_keywords')
        assert result == '訂金|押金|保證金'

    @pytest.mark.unit
    def test_resolve_ref_asset_suppliers(self, engine_with_rules, mock_engine_deps):
        """spx.asset_suppliers 應合併 kiosk + locker"""
        result = engine_with_rules._resolve_ref('spx.asset_suppliers')
        assert '益欣' in result
        assert '掌櫃' in result
