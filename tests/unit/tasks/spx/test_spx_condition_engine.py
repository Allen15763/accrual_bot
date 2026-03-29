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
    with patch('accrual_bot.core.pipeline.engines.condition_engine.config_manager') as mock_cm, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.get_logger', return_value=MagicMock()):
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

    @pytest.mark.unit
    def test_resolve_ref_missing_key(self, engine_with_rules, mock_engine_deps):
        """不存在的配置路徑應回傳 None"""
        result = engine_with_rules._resolve_ref('nonexistent.path')
        assert result is None

    @pytest.mark.unit
    def test_resolve_ref_case_insensitive(self, engine_with_rules, mock_engine_deps):
        """配置引用應支援不區分大小寫的 fallback"""
        mock_engine_deps._config_toml['FA_ACCOUNTS'] = {'SPX': ['199999']}
        # 'fa_accounts' 已存在（小寫），所以直接命中；測試大寫 key
        result = engine_with_rules._resolve_ref('FA_ACCOUNTS.SPX')
        assert result == ['199999']

    @pytest.mark.unit
    def test_resolve_ref_non_dict_intermediate(self, engine_with_rules, mock_engine_deps):
        """中間路徑非 dict 時應回傳 None"""
        mock_engine_deps._config_toml['scalar_val'] = 'just_a_string'
        result = engine_with_rules._resolve_ref('scalar_val.sub_key')
        assert result is None

    @pytest.mark.unit
    def test_resolve_pattern_key(self, engine_with_rules, mock_engine_deps):
        """pattern_key 應透過 _resolve_ref 解析"""
        check = {'pattern_key': 'spx.deposit_keywords'}
        result = engine_with_rules._resolve_pattern(check)
        assert result == '訂金|押金|保證金'

    @pytest.mark.unit
    def test_resolve_value_key(self, engine_with_rules, mock_engine_deps):
        """value_key 應透過 _resolve_ref 解析"""
        mock_engine_deps._config_toml['test_section'] = {'test_value': 'hello'}
        check = {'value_key': 'test_section.test_value'}
        result = engine_with_rules._resolve_value(check)
        assert result == 'hello'

    @pytest.mark.unit
    def test_resolve_list_key(self, engine_with_rules, mock_engine_deps):
        """list_key 應透過 _resolve_ref 解析列表"""
        check = {'list_key': 'fa_accounts.spx'}
        result = engine_with_rules._resolve_list(check)
        assert result == ['199999']

    @pytest.mark.unit
    def test_resolve_list_key_string(self, engine_with_rules, mock_engine_deps):
        """list_key 指向字串時應包裝為列表"""
        mock_engine_deps._config_toml['single'] = {'val': 'one_item'}
        check = {'list_key': 'single.val'}
        result = engine_with_rules._resolve_list(check)
        assert result == ['one_item']

    @pytest.mark.unit
    def test_resolve_status_value_key(self, engine_with_rules, mock_engine_deps):
        """status_value_key 應透過 _resolve_ref 解析"""
        mock_engine_deps._config_toml['status'] = {'completed': '已完成'}
        rule = {'status_value_key': 'status.completed'}
        result = engine_with_rules._resolve_status_value(rule)
        assert result == '已完成'


# ============================================================
# 額外 check type 測試（提升覆蓋率）
# ============================================================

class TestEvaluateCheckExtended:
    """測試 _evaluate_check 中尚未覆蓋的 check type"""

    @pytest.mark.unit
    def test_is_not_null_with_values(self, engine_with_rules):
        """is_not_null 應排除 NA、空字串、'nan'"""
        df = pd.DataFrame({'Col': [pd.NA, 'value', '', 'nan', 'data']})
        check = {'type': 'is_not_null', 'field': 'Col'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        expected = [False, True, False, False, True]
        assert result.tolist() == expected

    @pytest.mark.unit
    def test_is_not_null_missing_field(self, engine_with_rules):
        """is_not_null 欄位不存在時應回傳 None"""
        df = pd.DataFrame({'Other': [1]})
        check = {'type': 'is_not_null', 'field': 'Missing'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is None

    @pytest.mark.unit
    def test_no_status_check(self, engine_with_rules):
        """no_status 應識別狀態欄位為空的列"""
        df = pd.DataFrame({'PO狀態': [pd.NA, '已完成', '', 'nan', '進行中']})
        check = {'type': 'no_status'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        expected = [True, False, True, True, False]
        assert result.tolist() == expected

    @pytest.mark.unit
    def test_erm_le_date_without_prebuilt(self, engine_with_rules):
        """erm_le_date 無 prebuilt 時應即時計算"""
        df = pd.DataFrame({
            'Expected Received Month_轉換格式': [202512, 202601, 202511],
        })
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        check = {'type': 'erm_le_date'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_erm_le_date_no_processing_date(self, engine_with_rules):
        """erm_le_date 無 processing_date 時應回傳 None"""
        df = pd.DataFrame({
            'Expected Received Month_轉換格式': [202512],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'erm_le_date'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is None

    @pytest.mark.unit
    def test_erm_gt_date_without_prebuilt(self, engine_with_rules):
        """erm_gt_date 無 prebuilt 時應即時計算"""
        df = pd.DataFrame({
            'Expected Received Month_轉換格式': [202512, 202601, 202511],
        })
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        check = {'type': 'erm_gt_date'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_erm_gt_date_no_column(self, engine_with_rules):
        """erm_gt_date 欄位不存在時應回傳 None"""
        df = pd.DataFrame({'Other': [1]})
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        check = {'type': 'erm_gt_date'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is None

    @pytest.mark.unit
    def test_erm_in_range_without_prebuilt(self, engine_with_rules):
        """erm_in_range 無 prebuilt 時應即時計算"""
        df = pd.DataFrame({
            'Expected Received Month_轉換格式': [202511, 202601, 202510],
            'YMs of Item Description': ['202510,202512', '202510,202512', '202510,202512'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'erm_in_range'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        # 202511 在 [202510, 202512] 內 -> True
        # 202601 不在 [202510, 202512] 內 -> False
        # 202510 在 [202510, 202512] 內 -> True
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_erm_in_range_missing_columns(self, engine_with_rules):
        """erm_in_range 缺少必要欄位時應回傳 None"""
        df = pd.DataFrame({'Other': [1]})
        context = {'prebuilt_masks': {}}
        check = {'type': 'erm_in_range'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is None

    @pytest.mark.unit
    def test_qty_matched_without_prebuilt(self, engine_with_rules):
        """qty_matched 無 prebuilt 時應即時比較數量"""
        df = pd.DataFrame({
            'Entry Quantity': [100, 200, 50],
            'Received Quantity': [100, 150, 50],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'qty_matched'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_qty_matched_missing_columns(self, engine_with_rules):
        """qty_matched 缺少欄位時應回傳 None"""
        df = pd.DataFrame({'Other': [1]})
        context = {'prebuilt_masks': {}}
        check = {'type': 'qty_matched'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is None

    @pytest.mark.unit
    def test_qty_not_matched(self, engine_with_rules):
        """qty_not_matched 應為 qty_matched 的反向"""
        df = pd.DataFrame({
            'Entry Quantity': [100, 200],
            'Received Quantity': [100, 150],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'qty_not_matched'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [False, True]

    @pytest.mark.unit
    def test_not_billed_without_prebuilt(self, engine_with_rules):
        """not_billed 無 prebuilt 時應檢查 Entry Billed Amount 是否為 0"""
        df = pd.DataFrame({
            'Entry Billed Amount': ['0', '5000', '0'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'not_billed'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_has_billing_without_prebuilt(self, engine_with_rules):
        """has_billing 無 prebuilt 時應檢查 Billed Quantity 非 '0'"""
        df = pd.DataFrame({
            'Billed Quantity': ['0', '100', '0'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'has_billing'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_fully_billed_without_prebuilt(self, engine_with_rules):
        """fully_billed 無 prebuilt 時應比較 Entry Amount 與 Entry Billed Amount"""
        df = pd.DataFrame({
            'Entry Amount': ['10000', '20000', '5000'],
            'Entry Billed Amount': ['10000', '15000', '5000'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'fully_billed'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_has_unpaid_without_prebuilt(self, engine_with_rules):
        """has_unpaid 無 prebuilt 時應檢查金額差異"""
        df = pd.DataFrame({
            'Entry Amount': ['10000', '20000', '5000'],
            'Entry Billed Amount': ['10000', '15000', '5000'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'has_unpaid'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        # 差額：0, 5000, 0 -> 非零為 True
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_format_error_without_prebuilt(self, engine_with_rules):
        """format_error 無 prebuilt 時應檢查 YMs of Item Description"""
        df = pd.DataFrame({
            'YMs of Item Description': ['100001,100002', '202510,202512', '100001,100002'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'format_error'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_not_equals_check(self, engine_with_rules):
        """not_equals 應為 equals 的反向"""
        df = pd.DataFrame({'Status': ['Open', 'Closed', 'Open']})
        check = {'type': 'not_equals', 'field': 'Status', 'value': 'Closed'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_not_in_list_check(self, engine_with_rules):
        """not_in_list 應為 in_list 的反向"""
        df = pd.DataFrame({'GL#': ['100000', '199999', '200000']})
        check = {'type': 'not_in_list', 'field': 'GL#', 'values': ['199999']}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_is_fa_check(self, engine_with_rules):
        """is_fa 應根據 fa_accounts 配置識別 FA 帳號"""
        df = pd.DataFrame({'GL#': ['100000', '199999', '200000']})
        context = {'prebuilt_masks': {}}
        check = {'type': 'is_fa'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        # 只有 '199999' 在 fa_accounts.spx 中
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_not_fa_check(self, engine_with_rules):
        """not_fa 應為 is_fa 的反向"""
        df = pd.DataFrame({'GL#': ['100000', '199999', '200000']})
        context = {'prebuilt_masks': {}}
        check = {'type': 'not_fa'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_remark_completed_check(self, engine_with_rules):
        """remark_completed 應識別採購或 FN 備註中的完成關鍵字"""
        df = pd.DataFrame({
            'Remarked by Procurement': ['已完成', pd.NA, 'rent', pd.NA],
            'Remarked by 上月 FN': [pd.NA, '已入帳', pd.NA, pd.NA],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'remark_completed'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, True, True, False]

    @pytest.mark.unit
    def test_pr_not_incomplete_check(self, engine_with_rules):
        """pr_not_incomplete 應排除 FN PR 備註中含「未完成」的列"""
        df = pd.DataFrame({
            'Remarked by 上月 FN PR': ['正常', '未完成', pd.NA],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'pr_not_incomplete'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_pr_not_incomplete_missing_column(self, engine_with_rules):
        """pr_not_incomplete 欄位不存在時應回傳全 True"""
        df = pd.DataFrame({'Other': [1, 2]})
        context = {'prebuilt_masks': {}}
        check = {'type': 'pr_not_incomplete'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, True]

    @pytest.mark.unit
    def test_not_error_check(self, engine_with_rules):
        """not_error 應排除 Remarked by Procurement 為 'error' 的列"""
        df = pd.DataFrame({
            'Remarked by Procurement': ['normal', 'error', 'ok'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'not_error'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_not_error_missing_column(self, engine_with_rules):
        """not_error 欄位不存在時應回傳全 True"""
        df = pd.DataFrame({'Other': [1, 2]})
        context = {'prebuilt_masks': {}}
        check = {'type': 'not_error'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, True]

    @pytest.mark.unit
    def test_out_of_range_without_prebuilt(self, engine_with_rules):
        """out_of_range 無 prebuilt 時應計算不在範圍且非格式錯誤的列"""
        df = pd.DataFrame({
            'Expected Received Month_轉換格式': [202511, 202601, 202510],
            'YMs of Item Description': ['202510,202512', '202510,202512', '100001,100002'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'out_of_range'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        # 202511 在範圍 -> in_range=True -> out=False
        # 202601 不在範圍 -> in_range=False, 非格式錯誤 -> out=True
        # 202510 -> YMs='100001,100002' 格式錯誤 -> 被排除 -> out=False
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_desc_erm_le_date(self, engine_with_rules):
        """desc_erm_le_date 應比較 YMs of Item Description 結尾日期"""
        df = pd.DataFrame({
            'YMs of Item Description': ['202510,202512', '202510,202601', '202510,202511'],
        })
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        check = {'type': 'desc_erm_le_date'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        # str[7:] -> '202512', '202601', '202511'
        # <= 202512 -> True, False, True
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_desc_erm_gt_date(self, engine_with_rules):
        """desc_erm_gt_date 應比較 YMs of Item Description 起始日期"""
        df = pd.DataFrame({
            'YMs of Item Description': ['202510,202512', '202601,202603', '202512,202512'],
        })
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        check = {'type': 'desc_erm_gt_date'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        # str[:6] -> '202510', '202601', '202512'
        # > 202512 -> False, True, False
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_desc_erm_not_error(self, engine_with_rules):
        """desc_erm_not_error 應排除 YMs 起始為 100001 的列"""
        df = pd.DataFrame({
            'YMs of Item Description': ['202510,202512', '100001,100002'],
        })
        context = {'prebuilt_masks': {}}
        check = {'type': 'desc_erm_not_error'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        assert result.tolist() == [True, False]

    @pytest.mark.unit
    def test_equals_with_cast(self, engine_with_rules):
        """equals 帶 cast 參數時應正確轉型比較"""
        df = pd.DataFrame({'Amount': ['100', '200', '100']})
        check = {'type': 'equals', 'field': 'Amount', 'value': 100, 'cast': 'int'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_contains_with_pattern_key(self, engine_with_rules, mock_engine_deps):
        """contains 使用 pattern_key 應從配置解析正則"""
        df = pd.DataFrame({'Description': ['含訂金品項', '一般品項', '押金']})
        check = {'type': 'contains', 'field': 'Description', 'pattern_key': 'spx.deposit_keywords'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [True, False, True]

    @pytest.mark.unit
    def test_in_list_with_list_key(self, engine_with_rules, mock_engine_deps):
        """in_list 使用 list_key 應從配置解析列表"""
        df = pd.DataFrame({'GL#': ['100000', '199999', '200000']})
        check = {'type': 'in_list', 'field': 'GL#', 'list_key': 'fa_accounts.spx'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', {})
        assert result is not None
        assert result.tolist() == [False, True, False]

    @pytest.mark.unit
    def test_prebuilt_mask_no_status_not_used(self, engine_with_rules):
        """no_status 類型不應使用 prebuilt_masks，需即時計算"""
        df = pd.DataFrame({'PO狀態': [pd.NA, '已完成']})
        # 即使 prebuilt 中有 no_status，也不應使用
        prebuilt_value = pd.Series([False, False])
        context = {'prebuilt_masks': {'no_status': prebuilt_value}}
        check = {'type': 'no_status'}
        result = engine_with_rules._evaluate_check(df, check, 'PO狀態', context)
        assert result is not None
        # 應即時計算而非使用 prebuilt
        assert result.iloc[0] == True
        assert result.iloc[1] == False


# ============================================================
# _build_combined_mask 額外測試
# ============================================================

class TestBuildCombinedMaskExtended:
    """測試 _build_combined_mask 的邊界情況"""

    @pytest.mark.unit
    def test_empty_checks_returns_none(self, engine_with_rules):
        """空 checks 列表應回傳 None"""
        df = pd.DataFrame({'A': [1, 2]})
        result = engine_with_rules._build_combined_mask(
            df, [], 'and', 'PO狀態', {}
        )
        assert result is None

    @pytest.mark.unit
    def test_all_checks_return_none(self, engine_with_rules):
        """所有 check 都回傳 None 時，組合結果應為 None"""
        df = pd.DataFrame({'A': [1, 2]})
        checks = [
            {'type': 'contains', 'field': 'NonExistent', 'pattern': 'x'},
            {'type': 'equals', 'field': 'NonExistent', 'value': 'y'},
        ]
        result = engine_with_rules._build_combined_mask(
            df, checks, 'and', 'PO狀態', {}
        )
        assert result is None

    @pytest.mark.unit
    def test_or_combine_multiple_masks(self, engine_with_rules):
        """'or' 組合多個 mask 應正確聯集"""
        df = pd.DataFrame({
            'A': ['x', 'y', 'z'],
            'B': ['p', 'q', 'x'],
            'C': ['m', 'n', 'x'],
        })
        checks = [
            {'type': 'equals', 'field': 'A', 'value': 'x'},
            {'type': 'equals', 'field': 'B', 'value': 'q'},
            {'type': 'equals', 'field': 'C', 'value': 'x'},
        ]
        result = engine_with_rules._build_combined_mask(
            df, checks, 'or', 'PO狀態', {}
        )
        assert result is not None
        # row 0: A=x -> True; row 1: B=q -> True; row 2: B=x, C=x -> True
        assert result.tolist() == [True, True, True]

    @pytest.mark.unit
    def test_type_placeholder_in_field(self, engine_with_rules):
        """{TYPE} 佔位符在 field 中應被替換為 processing_type"""
        df = pd.DataFrame({
            'PR狀態': [pd.NA, '已完成', pd.NA],
        })
        checks = [{'type': 'is_null', 'field': '{TYPE}狀態'}]
        result = engine_with_rules._build_combined_mask(
            df, checks, 'and', 'PR狀態', {},
            processing_type='PR'
        )
        assert result is not None
        assert result.tolist() == [True, False, True]


# ============================================================
# _compute_erm_in_range 測試
# ============================================================

class TestComputeErmInRange:
    """測試 _compute_erm_in_range 方法"""

    @pytest.mark.unit
    def test_erm_in_range_basic(self, engine_with_rules):
        """ERM 在 YMs 範圍內應回傳 True"""
        df = pd.DataFrame({
            'Expected Received Month_轉換格式': [202510, 202512, 202511],
            'YMs of Item Description': ['202510,202512', '202510,202512', '202510,202512'],
        })
        result = engine_with_rules._compute_erm_in_range(df)
        assert result is not None
        # 全部都在 [202510, 202512] 範圍內
        assert result.tolist() == [True, True, True]

    @pytest.mark.unit
    def test_erm_out_of_range(self, engine_with_rules):
        """ERM 不在 YMs 範圍內應回傳 False"""
        df = pd.DataFrame({
            'Expected Received Month_轉換格式': [202509, 202601],
            'YMs of Item Description': ['202510,202512', '202510,202512'],
        })
        result = engine_with_rules._compute_erm_in_range(df)
        assert result is not None
        assert result.tolist() == [False, False]

    @pytest.mark.unit
    def test_erm_in_range_missing_columns(self, engine_with_rules):
        """缺少必要欄位應回傳 None"""
        df = pd.DataFrame({'Other': [1]})
        result = engine_with_rules._compute_erm_in_range(df)
        assert result is None


# ============================================================
# apply_rules 進階測試
# ============================================================

class TestApplyRulesExtended:
    """apply_rules 方法的進階測試"""

    @pytest.mark.unit
    def test_update_no_status_in_prebuilt(self, engine_with_rules, sample_df, engine_context):
        """update_no_status=True 時應同步更新 prebuilt_masks 中的 no_status"""
        df = sample_df.copy()
        engine_context['prebuilt_masks'] = {
            'qty_matched': pd.Series([True] * 5),
            'erm_le_date': pd.Series([True] * 5),
            'erm_gt_date': pd.Series([False] * 5),
            'format_error': pd.Series([False] * 5),
        }
        engine_with_rules.apply_rules(
            df, 'PO狀態', engine_context, update_no_status=True
        )
        # prebuilt_masks 中應有 no_status
        assert 'no_status' in engine_context['prebuilt_masks']

    @pytest.mark.unit
    def test_rule_with_empty_checks_skipped(self, mock_engine_deps):
        """checks 為空列表的規則應被跳過"""
        mock_engine_deps._config_toml['test_rules'] = {
            'conditions': [
                {
                    'priority': 10,
                    'status_value': '不應命中',
                    'note': '空 checks',
                    'combine': 'and',
                    'checks': [],
                },
            ]
        }
        from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
        engine = SPXConditionEngine('test_rules')
        df = pd.DataFrame({'PO狀態': [pd.NA]})
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        df_result, stats = engine.apply_rules(df, 'PO狀態', context)
        assert df_result['PO狀態'].isna().all()

    @pytest.mark.unit
    def test_multiple_rules_sequential(self, mock_engine_deps):
        """多個規則應依序應用，已被命中的列不再被後續規則處理"""
        mock_engine_deps._config_toml['test_rules'] = {
            'conditions': [
                {
                    'priority': 10,
                    'status_value': '第一批',
                    'note': '第一規則',
                    'combine': 'and',
                    'checks': [{'type': 'equals', 'field': 'Type', 'value': 'A'}],
                },
                {
                    'priority': 20,
                    'status_value': '第二批',
                    'note': '第二規則',
                    'combine': 'and',
                    'checks': [{'type': 'equals', 'field': 'Type', 'value': 'B'}],
                },
            ]
        }
        from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
        engine = SPXConditionEngine('test_rules')
        df = pd.DataFrame({
            'PO狀態': [pd.NA, pd.NA, pd.NA],
            'Type': ['A', 'B', 'C'],
        })
        context = {'processing_date': 202512, 'prebuilt_masks': {}}
        df_result, stats = engine.apply_rules(df, 'PO狀態', context)
        assert df_result.loc[0, 'PO狀態'] == '第一批'
        assert df_result.loc[1, 'PO狀態'] == '第二批'
        # 第三列不匹配任何規則，應仍為 NA
        assert pd.isna(df_result.loc[2, 'PO狀態'])
