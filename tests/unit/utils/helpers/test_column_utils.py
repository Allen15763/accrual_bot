"""ColumnResolver 單元測試"""
import pytest
import pandas as pd

from accrual_bot.utils.helpers.column_utils import ColumnResolver


@pytest.mark.unit
class TestColumnResolverResolve:
    """測試 ColumnResolver.resolve 方法"""

    def test_resolve_with_predefined_pattern_standard_format(self):
        """使用預定義模式解析標準格式欄位名稱（例如 'PO Line'）"""
        df = pd.DataFrame({'PO Line': [1, 2, 3]})
        result = ColumnResolver.resolve(df, 'po_line')
        assert result == 'PO Line'

    def test_resolve_with_predefined_pattern_snake_case(self):
        """使用預定義模式解析 snake_case 格式欄位名稱（例如 'po_line'）"""
        df = pd.DataFrame({'po_line': [1, 2, 3]})
        result = ColumnResolver.resolve(df, 'po_line')
        assert result == 'po_line'

    def test_resolve_fallback_case_insensitive(self):
        """非預定義模式時回退到大小寫不敏感的直接匹配"""
        df = pd.DataFrame({'My Custom Col': [1, 2]})
        result = ColumnResolver.resolve(df, 'my_custom_col')
        assert result == 'My Custom Col'

    def test_resolve_returns_none_when_column_missing(self):
        """找不到欄位時返回 None"""
        df = pd.DataFrame({'other_col': [1]})
        result = ColumnResolver.resolve(df, 'po_line')
        assert result is None

    def test_resolve_multiple_pattern_matches_returns_first(self):
        """多個匹配時返回第一個"""
        df = pd.DataFrame({'PO Line': [1], 'PO_Line': [2]})
        result = ColumnResolver.resolve(df, 'po_line')
        # 應該返回其中一個匹配結果（不為 None）
        assert result is not None
        assert result in ('PO Line', 'PO_Line')


@pytest.mark.unit
class TestColumnResolverResolveOrRaise:
    """測試 ColumnResolver.resolve_or_raise 方法"""

    def test_resolve_or_raise_success(self):
        """找到欄位時正常返回"""
        df = pd.DataFrame({'PR Line': [1]})
        result = ColumnResolver.resolve_or_raise(df, 'pr_line')
        assert result == 'PR Line'

    def test_resolve_or_raise_raises_value_error(self):
        """找不到欄位時拋出 ValueError"""
        df = pd.DataFrame({'other': [1]})
        with pytest.raises(ValueError, match="Cannot resolve column 'pr_line'"):
            ColumnResolver.resolve_or_raise(df, 'pr_line')


@pytest.mark.unit
class TestColumnResolverHasColumn:
    """測試 ColumnResolver.has_column 方法"""

    def test_has_column_returns_true(self):
        """欄位存在時返回 True"""
        df = pd.DataFrame({'Liability': [100]})
        assert ColumnResolver.has_column(df, 'liability') is True

    def test_has_column_returns_false(self):
        """欄位不存在時返回 False"""
        df = pd.DataFrame({'other': [100]})
        assert ColumnResolver.has_column(df, 'liability') is False


@pytest.mark.unit
class TestColumnResolverResolveMultiple:
    """測試 ColumnResolver.resolve_multiple 方法"""

    def test_resolve_multiple_all_found(self):
        """一次解析多個欄位，全部找到"""
        df = pd.DataFrame({'PO Line': [1], 'PR Line': [2]})
        result = ColumnResolver.resolve_multiple(df, ['po_line', 'pr_line'])
        assert result == {'po_line': 'PO Line', 'pr_line': 'PR Line'}

    def test_resolve_multiple_partial_found(self):
        """一次解析多個欄位，部分找到"""
        df = pd.DataFrame({'PO Line': [1]})
        result = ColumnResolver.resolve_multiple(df, ['po_line', 'pr_line'])
        assert result['po_line'] == 'PO Line'
        assert result['pr_line'] is None


@pytest.mark.unit
class TestColumnResolverAddPattern:
    """測試 ColumnResolver.add_pattern 方法"""

    def test_add_pattern_and_resolve(self):
        """動態新增模式後可以正確解析"""
        ColumnResolver.add_pattern('custom_field', r'(?i)^(custom[_\s]?field)$')
        df = pd.DataFrame({'Custom Field': [1]})
        result = ColumnResolver.resolve(df, 'custom_field')
        assert result == 'Custom Field'
        # 清理：移除新增的模式以避免影響其他測試
        del ColumnResolver.COLUMN_PATTERNS['custom_field']
