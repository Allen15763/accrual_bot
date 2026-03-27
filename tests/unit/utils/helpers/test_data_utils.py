"""data_utils 工具函數單元測試"""
import pytest
import pandas as pd
import numpy as np
import logging

from accrual_bot.utils.helpers.data_utils import (
    clean_nan_values,
    safe_string_operation,
    format_numeric_with_thousands,
    format_numeric_columns,
    parse_date_string,
    _validate_date_format,
    convert_date_format_in_string,
    extract_pattern_from_string,
    safe_numeric_operation,
    create_mapping_dict,
    apply_mapping_safely,
    validate_dataframe_columns,
    concat_dataframes_safely,
    extract_date_range_from_description,
    extract_clean_description,
    give_account_by_keyword,
    parallel_apply,
    memory_efficient_operation,
    classify_description,
)


@pytest.mark.unit
class TestCleanNanValues:
    """測試 clean_nan_values"""

    def test_replaces_nan_with_empty_string(self):
        """nan 字串應被替換為空字串"""
        df = pd.DataFrame({'col1': ['hello', float('nan'), 'world']})
        result = clean_nan_values(df, ['col1'])
        assert result['col1'].iloc[1] == ''

    def test_does_not_modify_original(self):
        """應返回副本，不修改原始 DataFrame"""
        df = pd.DataFrame({'col1': [float('nan')]})
        _ = clean_nan_values(df, ['col1'])
        # 原始 df 中仍為 NaN
        assert pd.isna(df['col1'].iloc[0])

    def test_skips_missing_columns(self):
        """指定的欄位不存在時不報錯"""
        df = pd.DataFrame({'col1': ['a']})
        result = clean_nan_values(df, ['col1', 'nonexistent'])
        assert result['col1'].iloc[0] == 'a'


@pytest.mark.unit
class TestSafeStringOperation:
    """測試 safe_string_operation"""

    def test_contains(self):
        """contains 操作應正確匹配"""
        s = pd.Series(['hello world', 'foo bar', None])
        result = safe_string_operation(s, 'contains', pattern='world')
        assert result.iloc[0] is True or result.iloc[0] == True
        assert result.iloc[1] is False or result.iloc[1] == False

    def test_replace(self):
        """replace 操作應正確替換"""
        s = pd.Series(['abc123'])
        result = safe_string_operation(s, 'replace', pattern=r'\d+', replacement='X', regex=True)
        assert result.iloc[0] == 'abcX'

    def test_unsupported_operation_returns_original(self):
        """不支援的操作應返回原始 Series"""
        s = pd.Series(['test'])
        result = safe_string_operation(s, 'unknown_op')
        assert result.iloc[0] == 'test'


@pytest.mark.unit
class TestFormatNumericWithThousands:
    """測試 format_numeric_with_thousands"""

    def test_integer_format(self):
        assert format_numeric_with_thousands(1234567, 0) == '1,234,567'

    def test_decimal_format(self):
        assert format_numeric_with_thousands(1234.5, 2) == '1,234.50'

    def test_nan_returns_zero(self):
        assert format_numeric_with_thousands(float('nan')) == '0'

    def test_empty_string_returns_zero(self):
        assert format_numeric_with_thousands('') == '0'

    def test_string_numeric(self):
        assert format_numeric_with_thousands('9999', 0) == '9,999'


@pytest.mark.unit
class TestFormatNumericColumns:
    """測試 format_numeric_columns"""

    def test_formats_int_and_float_columns(self):
        df = pd.DataFrame({'int_col': [1000], 'float_col': [1234.5]})
        result = format_numeric_columns(df, int_cols=['int_col'], float_cols=['float_col'])
        assert result['int_col'].iloc[0] == '1,000'
        assert result['float_col'].iloc[0] == '1,234.50'


@pytest.mark.unit
class TestParseDateString:
    """測試 parse_date_string"""

    def test_with_explicit_format(self):
        result = parse_date_string('2024-01-15', input_format='%Y-%m-%d', output_format='%Y/%m/%d')
        assert result == '2024/01/15'

    def test_empty_returns_none(self):
        assert parse_date_string('') is None

    def test_nan_returns_none(self):
        assert parse_date_string('nan') is None

    def test_default_output_format(self):
        result = parse_date_string('2024-06-01', input_format='%Y-%m-%d')
        assert result == '2024/06/01'


@pytest.mark.unit
class TestValidateDateFormat:
    """測試 _validate_date_format"""

    def test_valid_ym(self):
        assert _validate_date_format('2024/01') is True

    def test_invalid_month(self):
        assert _validate_date_format('2024/13') is False

    def test_valid_ymd(self):
        assert _validate_date_format('2024/01/15', has_day=True) is True

    def test_invalid_day(self):
        assert _validate_date_format('2024/01/32', has_day=True) is False


@pytest.mark.unit
class TestConvertDateFormatInString:
    """測試 convert_date_format_in_string"""

    def test_default_conversion(self):
        result = convert_date_format_in_string('2024/01 - 2024/12')
        assert result == '202401 - 202412'

    def test_empty_returns_empty(self):
        assert convert_date_format_in_string('') == ''


@pytest.mark.unit
class TestExtractPatternFromString:
    """測試 extract_pattern_from_string"""

    def test_extract_match(self):
        result = extract_pattern_from_string('Order 12345', r'\d+')
        assert result == '12345'

    def test_no_match_returns_none(self):
        result = extract_pattern_from_string('no numbers', r'\d+')
        assert result is None

    def test_empty_returns_none(self):
        assert extract_pattern_from_string('', r'\d+') is None


@pytest.mark.unit
class TestSafeNumericOperation:
    """測試 safe_numeric_operation"""

    def test_add(self):
        s = pd.Series([10, 20])
        result = safe_numeric_operation(s, 'add', value=5)
        assert list(result) == [15, 25]

    def test_divide_by_zero_returns_original(self):
        s = pd.Series([10])
        result = safe_numeric_operation(s, 'divide', value=0)
        assert result.iloc[0] == 10.0

    def test_round(self):
        s = pd.Series([1.456])
        result = safe_numeric_operation(s, 'round', decimals=1)
        assert result.iloc[0] == 1.5


@pytest.mark.unit
class TestCreateMappingDict:
    """測試 create_mapping_dict"""

    def test_basic_mapping(self):
        df = pd.DataFrame({'key': ['a', 'b'], 'value': [1, 2]})
        result = create_mapping_dict(df, 'key', 'value')
        assert result == {'a': 1, 'b': 2}

    def test_missing_column_returns_empty(self):
        df = pd.DataFrame({'key': [1]})
        result = create_mapping_dict(df, 'key', 'nonexistent')
        assert result == {}

    def test_dict_input_returns_as_is(self):
        d = {'a': 1}
        assert create_mapping_dict(d, 'key', 'value') == d


@pytest.mark.unit
class TestApplyMappingSafely:
    """測試 apply_mapping_safely"""

    def test_maps_values(self):
        s = pd.Series(['a', 'b', 'c'])
        mapping = {'a': 1, 'b': 2}
        result = apply_mapping_safely(s, mapping, default_value=0)
        assert list(result) == [1, 2, 0]


@pytest.mark.unit
class TestValidateDataframeColumns:
    """測試 validate_dataframe_columns"""

    def test_all_columns_present(self):
        df = pd.DataFrame({'a': [1], 'b': [2]})
        assert validate_dataframe_columns(df, ['a', 'b']) is True

    def test_missing_columns_raises(self):
        df = pd.DataFrame({'a': [1]})
        with pytest.raises(ValueError, match="缺少必要的列"):
            validate_dataframe_columns(df, ['a', 'missing'])

    def test_missing_columns_no_raise(self):
        df = pd.DataFrame({'a': [1]})
        assert validate_dataframe_columns(df, ['missing'], raise_error=False) is False


@pytest.mark.unit
class TestConcatDataframesSafely:
    """測試 concat_dataframes_safely"""

    def test_concat_multiple(self):
        df1 = pd.DataFrame({'a': [1]})
        df2 = pd.DataFrame({'a': [2]})
        result = concat_dataframes_safely([df1, df2])
        assert len(result) == 2

    def test_filters_empty_and_none(self):
        df1 = pd.DataFrame({'a': [1]})
        result = concat_dataframes_safely([None, df1, pd.DataFrame()])
        assert len(result) == 1

    def test_all_empty_returns_empty_df(self):
        result = concat_dataframes_safely([None, pd.DataFrame()])
        assert result.empty


# ============================================================
# 以下為 Phase 1d 新增測試：覆蓋率提升
# ============================================================


@pytest.fixture
def mock_logger():
    """測試用 logger"""
    return logging.getLogger('test_data_utils')


@pytest.mark.unit
class TestExtractDateRangeFromDescription:
    """測試 extract_date_range_from_description — 日期範圍擷取"""

    # 定義測試用的日期模式（避免依賴模組級全域變數載入）
    @pytest.fixture
    def date_patterns(self):
        return {
            'DATE_YMD_TO_YMD': r'(\d{4}/\d{2}/\d{2})\s*[-~]\s*(\d{4}/\d{2}/\d{2})',
            'DATE_YM_TO_YM': r'(\d{4}/\d{2})\s*[-~]\s*(\d{4}/\d{2})',
            'DATE_YMD': r'(\d{4}/\d{2}/\d{2})',
            'DATE_YM': r'(\d{4}/\d{2})',
        }

    def test_ymd_to_ymd_range(self, date_patterns, mock_logger):
        """YYYY/MM/DD-YYYY/MM/DD 格式應正確解析"""
        result = extract_date_range_from_description(
            "期間：2024/01/01-2024/12/31",
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "202401,202412"

    def test_ym_to_ym_range(self, date_patterns, mock_logger):
        """YYYY/MM-YYYY/MM 格式應正確解析"""
        result = extract_date_range_from_description(
            "合約 2024/01-2024/06",
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "202401,202406"

    def test_single_ymd(self, date_patterns, mock_logger):
        """單一 YYYY/MM/DD 應返回相同起迄"""
        result = extract_date_range_from_description(
            "發票日期 2024/06/15",
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "202406,202406"

    def test_single_ym(self, date_patterns, mock_logger):
        """單一 YYYY/MM 應返回相同起迄"""
        result = extract_date_range_from_description(
            "2024/03 月份",
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "202403,202403"

    def test_no_date_returns_default(self, date_patterns, mock_logger):
        """無日期格式應返回預設值"""
        result = extract_date_range_from_description(
            "沒有任何日期的描述文字",
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "100001,100002"

    def test_empty_returns_default(self, date_patterns, mock_logger):
        """空字串應返回預設值"""
        result = extract_date_range_from_description(
            "",
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "100001,100002"

    def test_nan_returns_default(self, date_patterns, mock_logger):
        """NaN 值應返回預設值"""
        result = extract_date_range_from_description(
            float('nan'),
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "100001,100002"

    def test_none_returns_default(self, date_patterns, mock_logger):
        """None 值應返回預設值"""
        result = extract_date_range_from_description(
            None,
            patterns=date_patterns,
            logger=mock_logger
        )
        assert result == "100001,100002"


@pytest.mark.unit
class TestExtractCleanDescription:
    """測試 extract_clean_description — SPX 描述清理"""

    def test_store_decoration_with_address_and_payment(self):
        """門市裝修工程含地址和期數"""
        desc = "2024/06_SVP_SPX 門市裝修工程-北投建民 (台北市北投區建民路1號) SPX store decoration 第一期款項#ABC123"
        result = extract_clean_description(desc)
        assert "門市裝修工程-北投建民" in result
        assert "台北市北投區建民路1號" in result

    def test_project_with_address_no_payment(self):
        """工程項目含地址但無期數"""
        desc = "SVP_SPX 冷氣裝修工程(台中市西屯區123號)"
        result = extract_clean_description(desc)
        assert "冷氣裝修工程" in result
        assert "台中市西屯區123號" in result

    def test_generic_description_cleanup(self):
        """通用清理：移除 # 標籤和日期前綴"""
        desc = "2024/06 SVP_SPX IT設備採購 #TAG123"
        result = extract_clean_description(desc)
        # 應移除日期前綴和 # 標籤
        assert "#TAG123" not in result
        assert "IT設備採購" in result

    def test_series_input(self):
        """接受 pd.Series 輸入應逐元素處理"""
        series = pd.Series(["2024/06 SVP_SPX 測試項目 #TAG", "簡單描述"])
        result = extract_clean_description(series)
        assert isinstance(result, pd.Series)
        assert len(result) == 2

    def test_already_spx_prefix(self):
        """SPX 開頭的內容不重複加前綴"""
        desc = "SPX 測試項目"
        result = extract_clean_description(desc)
        assert result.startswith("SPX_")
        # 不應出現 "SPX SPX" 或 "SPX_SPX"
        assert "SPX_SPX" not in result


@pytest.mark.unit
class TestGiveAccountByKeyword:
    """測試 give_account_by_keyword — 科目預測"""

    def test_adds_predicted_account_column(self):
        """應新增 Predicted_Account 欄位"""
        df = pd.DataFrame({'Item Description': ['測試租金', '清潔服務', '不匹配項目']})
        result = give_account_by_keyword(df, 'Item Description')
        assert 'Predicted_Account' in result.columns

    def test_non_string_returns_none(self):
        """非字串值的 Predicted_Account 應為 None"""
        df = pd.DataFrame({'desc': [123, None, float('nan')]})
        result = give_account_by_keyword(df, 'desc')
        # 數字和 None 不應匹配任何規則
        assert result['Predicted_Account'].isna().sum() >= 2

    def test_export_keyword_flag(self):
        """export_keyword=True 應新增 Matched_Keyword 欄位"""
        df = pd.DataFrame({'Item Description': ['測試']})
        result = give_account_by_keyword(df, 'Item Description', export_keyword=True)
        assert 'Matched_Keyword' in result.columns

    def test_no_export_keyword_by_default(self):
        """預設不應有 Matched_Keyword 欄位"""
        df = pd.DataFrame({'Item Description': ['測試']})
        result = give_account_by_keyword(df, 'Item Description')
        assert 'Matched_Keyword' not in result.columns


@pytest.mark.unit
class TestParallelApply:
    """測試 parallel_apply — 並行處理"""

    def test_basic_apply(self):
        """基本應用：對欄位套用函式"""
        df = pd.DataFrame({'val': [1, 2, 3, 4, 5]})
        result = parallel_apply(df, lambda x: x * 2, column='val', max_workers=2)
        assert list(result) == [2, 4, 6, 8, 10]

    def test_fallback_on_small_data(self):
        """小資料集應自動使用單線程"""
        df = pd.DataFrame({'val': [10]})
        result = parallel_apply(df, lambda x: x + 1, column='val')
        assert result.iloc[0] == 11

    def test_without_column_applies_to_dataframe(self):
        """不指定 column 時應作用於整個 DataFrame"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        result = parallel_apply(df, lambda x: x * 10, max_workers=1)
        # 不指定 column 時返回 DataFrame
        assert len(result) == 3


@pytest.mark.unit
class TestMemoryEfficientOperation:
    """測試 memory_efficient_operation — 記憶體高效操作"""

    def test_small_df_no_chunking(self):
        """小 DataFrame 應直接處理（不分塊）"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        result = memory_efficient_operation(df, lambda d: d * 2, chunk_size=10000)
        assert list(result['a']) == [2, 4, 6]

    def test_large_df_chunked(self):
        """大 DataFrame 應分塊處理"""
        df = pd.DataFrame({'a': range(100)})
        result = memory_efficient_operation(df, lambda d: d * 2, chunk_size=30)
        assert len(result) == 100
        assert result['a'].iloc[0] == 0
        assert result['a'].iloc[99] == 198

    def test_operation_error_raises(self):
        """操作函式出錯應拋出 ValueError"""
        df = pd.DataFrame({'a': [1]})
        with pytest.raises(ValueError, match="記憶體高效操作時出錯"):
            memory_efficient_operation(df, lambda d: 1 / 0)


@pytest.mark.unit
class TestClassifyDescription:
    """測試 classify_description — 描述分類"""

    def test_no_match_returns_miscellaneous(self):
        """無匹配規則應返回 Miscellaneous"""
        result = classify_description("完全隨機的文字 xyz123")
        assert result == 'Miscellaneous'

    def test_returns_string(self):
        """結果應為字串"""
        result = classify_description("任意描述")
        assert isinstance(result, str)
