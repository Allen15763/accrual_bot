"""data_utils 工具函數單元測試"""
import pytest
import pandas as pd
import numpy as np

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
