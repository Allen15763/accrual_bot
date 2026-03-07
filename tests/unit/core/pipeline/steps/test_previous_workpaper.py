"""
PreviousWorkpaperIntegrationStep 單元測試

測試前期底稿整合步驟的配置驅動映射功能
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from accrual_bot.core.pipeline.steps.common import PreviousWorkpaperIntegrationStep
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.helpers.column_utils import ColumnResolver


class TestColumnResolver:
    """測試 ColumnResolver 工具類"""

    def test_resolve_standard_po_line(self):
        """標準 'PO Line' 應該被解析"""
        df = pd.DataFrame({'PO Line': [1, 2, 3]})
        result = ColumnResolver.resolve(df, 'po_line')
        assert result == 'PO Line'

    def test_resolve_snake_po_line(self):
        """Snake_case 'po_line' 應該被解析"""
        df = pd.DataFrame({'po_line': [1, 2, 3]})
        result = ColumnResolver.resolve(df, 'po_line')
        assert result == 'po_line'

    def test_resolve_pr_line_standard(self):
        """標準 'PR Line' 應該被解析"""
        df = pd.DataFrame({'PR Line': ['PR001', 'PR002']})
        result = ColumnResolver.resolve(df, 'pr_line')
        assert result == 'PR Line'

    def test_resolve_pr_line_snake(self):
        """Snake_case 'pr_line' 應該被解析"""
        df = pd.DataFrame({'pr_line': ['PR001', 'PR002']})
        result = ColumnResolver.resolve(df, 'pr_line')
        assert result == 'pr_line'

    def test_resolve_not_found(self):
        """不存在的欄位應返回 None"""
        df = pd.DataFrame({'Other Column': [1, 2, 3]})
        result = ColumnResolver.resolve(df, 'po_line')
        assert result is None

    def test_has_column_true(self):
        """has_column 應正確檢測存在的欄位"""
        df = pd.DataFrame({'PO Line': [1, 2, 3]})
        assert ColumnResolver.has_column(df, 'po_line') is True

    def test_has_column_false(self):
        """has_column 應正確檢測不存在的欄位"""
        df = pd.DataFrame({'Other': [1, 2, 3]})
        assert ColumnResolver.has_column(df, 'po_line') is False

    def test_resolve_multiple(self):
        """resolve_multiple 應一次解析多個欄位"""
        df = pd.DataFrame({'PO Line': [1], 'PR Line': [2]})
        result = ColumnResolver.resolve_multiple(df, ['po_line', 'pr_line'])
        assert result == {'po_line': 'PO Line', 'pr_line': 'PR Line'}


class TestPreviousWorkpaperIntegration:
    """測試 PreviousWorkpaperIntegrationStep"""

    @pytest.fixture
    def mock_config(self):
        """模擬配置"""
        return {
            'previous_workpaper_integration': {
                'column_patterns': {
                    'po_line': r'(?i)^(po[_\s]?line)$',
                    'pr_line': r'(?i)^(pr[_\s]?line)$',
                    'remarked_by_fn': r'(?i)^(remarked[_\s]?by[_\s]?fn)$',
                    'remarked_by_procurement': r'(?i)^(remarked?[_\s]?by[_\s]?(procurement|pr[_\s]?team))$',
                    'noted_by_fn': r'(?i)^(noted[_\s]?by[_\s]?fn)$',
                    'liability': r'(?i)^(liability)$',
                    'current_month_reviewed_by': r'(?i)^(current[_\s]?month[_\s]?reviewed[_\s]?by)$',
                    'cumulative_qty': r'(?i)^(累計至本期驗收數量/金額)$',
                },
                'po_mappings': {
                    'fields': [
                        {'source': 'remarked_by_fn', 'target': 'Remarked by 上月 FN', 'fill_na': True},
                        {'source': 'remarked_by_procurement', 'target': 'Remarked by 上月 Procurement', 'fill_na': True},
                        {'source': 'noted_by_fn', 'target': 'Noted by FN', 'fill_na': True},
                        {'source': 'liability', 'target': 'Liability', 'fill_na': True},
                    ]
                },
                'pr_mappings': {
                    'fields': [
                        {'source': 'remarked_by_fn', 'target': 'Remarked by 上月 FN PR', 'fill_na': True},
                        {'source': 'noted_by_fn', 'target': 'Noted by FN', 'fill_na': False},
                    ]
                },
                'reviewer_mapping': {
                    'enabled_entities': ['SPT'],
                    'source': 'current_month_reviewed_by',
                    'targets': ['previous_month_reviewed_by', 'current_month_reviewed_by']
                }
            }
        }

    @pytest.fixture
    def step(self, mock_config):
        """建立步驟實例"""
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm._config_toml = mock_config
            step = PreviousWorkpaperIntegrationStep()
            return step

    @pytest.fixture
    def sample_df_standard(self):
        """標準欄位名稱的 DataFrame"""
        return pd.DataFrame({
            'PO Line': ['PO001', 'PO002', 'PO003'],
            'Item Description': ['A', 'B', 'C'],
        })

    @pytest.fixture
    def sample_df_snake(self):
        """Snake_case 欄位名稱的 DataFrame"""
        return pd.DataFrame({
            'po_line': ['PO001', 'PO002', 'PO003'],
            'item_description': ['A', 'B', 'C'],
        })

    @pytest.fixture
    def previous_wp_standard(self):
        """標準欄位的前期底稿"""
        return pd.DataFrame({
            'PO Line': ['PO001', 'PO002'],
            'Remarked by FN': ['Remark 1', 'Remark 2'],
            'Remarked by Procurement': ['PQ Remark 1', 'PQ Remark 2'],
            'Noted by FN': ['Note 1', 'Note 2'],
            'Liability': ['110001', '110002'],
        })

    @pytest.fixture
    def previous_wp_snake(self):
        """Snake_case 欄位的前期底稿"""
        return pd.DataFrame({
            'po_line': ['PO001', 'PO002'],
            'remarked_by_fn': ['Remark 1', 'Remark 2'],
            'remarked_by_procurement': ['PQ Remark 1', 'PQ Remark 2'],
            'noted_by_fn': ['Note 1', 'Note 2'],
            'liability': ['110001', '110002'],
        })

    # ========== PO 映射測試 ==========

    @pytest.mark.parametrize("df_fixture,wp_fixture", [
        ("sample_df_standard", "previous_wp_standard"),
        ("sample_df_snake", "previous_wp_snake"),
        ("sample_df_standard", "previous_wp_snake"),  # 混合模式
        ("sample_df_snake", "previous_wp_standard"),  # 反向混合
    ])
    def test_po_mapping_output_equivalence(self, step, df_fixture, wp_fixture, request):
        """所有欄位名稱組合應產生等價的輸出"""
        df = request.getfixturevalue(df_fixture).copy()
        previous_wp = request.getfixturevalue(wp_fixture)

        result = step._process_previous_po(df, previous_wp, 202512)

        # 驗證輸出欄位存在
        assert 'Remarked by 上月 FN' in result.columns
        assert 'Remarked by 上月 Procurement' in result.columns
        assert 'Noted by FN' in result.columns
        assert 'Liability' in result.columns

        # 驗證映射正確性
        # 取得 PO Line 欄位（不管大小寫）
        po_col = ColumnResolver.resolve(result, 'po_line')

        po001_idx = result[result[po_col] == 'PO001'].index[0]
        po002_idx = result[result[po_col] == 'PO002'].index[0]
        po003_idx = result[result[po_col] == 'PO003'].index[0]

        assert result.loc[po001_idx, 'Remarked by 上月 FN'] == 'Remark 1'
        assert result.loc[po002_idx, 'Remarked by 上月 FN'] == 'Remark 2'
        assert pd.isna(result.loc[po003_idx, 'Remarked by 上月 FN'])

        assert result.loc[po001_idx, 'Liability'] == '110001'
        assert result.loc[po002_idx, 'Liability'] == '110002'
        assert pd.isna(result.loc[po003_idx, 'Liability'])

    # ========== PR 映射測試 ==========

    def test_pr_mapping_basic(self, step):
        """測試基本 PR 映射"""
        df = pd.DataFrame({
            'PR Line': ['PR001', 'PR002', 'PR003'],
        })
        previous_wp_pr = pd.DataFrame({
            'PR Line': ['PR001', 'PR002'],
            'Remarked by FN': ['PR Remark 1', 'PR Remark 2'],
        })

        result = step._process_previous_pr(df, previous_wp_pr)

        assert 'Remarked by 上月 FN PR' in result.columns
        assert result.loc[0, 'Remarked by 上月 FN PR'] == 'PR Remark 1'
        assert result.loc[1, 'Remarked by 上月 FN PR'] == 'PR Remark 2'
        assert pd.isna(result.loc[2, 'Remarked by 上月 FN PR'])

    def test_pr_note_override(self, step):
        """測試 PR noted_by_fn 覆蓋邏輯 (fill_na=False)"""
        df = pd.DataFrame({
            'PR Line': ['PR001', 'PR002'],
            'Noted by FN': ['Old Note 1', pd.NA],  # PR001 已有值，PR002 沒有
        })
        previous_wp_pr = pd.DataFrame({
            'PR Line': ['PR001', 'PR002'],
            'noted_by_fn': ['New Note 1', 'New Note 2'],
        })

        result = step._process_previous_pr(df, previous_wp_pr)

        # PR001 應該被覆蓋，PR002 應該填入新值
        assert result.loc[0, 'Noted by FN'] == 'New Note 1'
        assert result.loc[1, 'Noted by FN'] == 'New Note 2'

    # ========== Reviewer 映射測試 ==========

    def test_reviewer_info_po(self, step):
        """測試 PO Reviewer 資訊處理"""
        df = pd.DataFrame({'PO Line': ['PO001', 'PO002']})
        df_ref = pd.DataFrame({
            'PO Line': ['PO001', 'PO002'],
            'Current month Reviewed by': ['Alice', 'Bob'],
        })

        result = step._process_reviewer_info(df, df_ref)

        assert 'previous_month_reviewed_by' in result.columns
        assert 'current_month_reviewed_by' in result.columns
        assert result.loc[0, 'previous_month_reviewed_by'] == 'Alice'
        assert result.loc[1, 'previous_month_reviewed_by'] == 'Bob'
        assert result.loc[0, 'current_month_reviewed_by'] == 'Alice'
        assert result.loc[1, 'current_month_reviewed_by'] == 'Bob'

    def test_reviewer_info_pr(self, step):
        """測試 PR Reviewer 資訊處理"""
        df = pd.DataFrame({'PR Line': ['PR001', 'PR002']})
        df_ref = pd.DataFrame({
            'PR Line': ['PR001', 'PR002'],
            'current_month_reviewed_by': ['Charlie', 'David'],  # snake_case 變體
        })

        result = step._process_reviewer_info(df, df_ref)

        assert result.loc[0, 'previous_month_reviewed_by'] == 'Charlie'
        assert result.loc[1, 'previous_month_reviewed_by'] == 'David'

    def test_determine_key_type_po(self, step):
        """_determine_key_type 應正確判斷 PO"""
        df = pd.DataFrame({'PO Line': [1]})
        df_ref = pd.DataFrame({'PO Line': [1]})

        result = step._determine_key_type(df, df_ref)
        assert result == 'po'

    def test_determine_key_type_pr(self, step):
        """_determine_key_type 應正確判斷 PR"""
        df = pd.DataFrame({'PR Line': [1]})
        df_ref = pd.DataFrame({'PR Line': [1]})

        result = step._determine_key_type(df, df_ref)
        assert result == 'pr'

    def test_determine_key_type_none(self, step):
        """_determine_key_type 應在無法判斷時返回 None"""
        df = pd.DataFrame({'Other': [1]})
        df_ref = pd.DataFrame({'Other': [1]})

        result = step._determine_key_type(df, df_ref)
        assert result is None

    # ========== 整合測試 ==========

    @pytest.mark.asyncio
    async def test_execute_with_po_workpaper(self, step, sample_df_standard, previous_wp_standard):
        """測試完整 execute 流程（PO）"""
        context = ProcessingContext(
            data=sample_df_standard,
            entity_type='SPX',
            processing_date=202512,
            processing_type='PO'
        )
        context.add_auxiliary_data('previous', previous_wp_standard)
        context.set_variable('processing_month', 202512)
        context.set_variable('file_paths', {'raw_po': 'test.xlsx'})

        result = await step.execute(context)

        assert result.status.name == 'SUCCESS'
        assert 'Remarked by 上月 FN' in result.data.columns
        assert 'Liability' in result.data.columns

    @pytest.mark.asyncio
    async def test_execute_no_workpaper(self, step, sample_df_standard):
        """測試無前期底稿的情況"""
        context = ProcessingContext(
            data=sample_df_standard,
            entity_type='SPX',
            processing_date=202512,
            processing_type='PO'
        )
        context.set_variable('processing_month', 202512)
        context.set_variable('file_paths', {})

        result = await step.execute(context)

        assert result.status.name == 'SKIPPED'
        assert result.message == "No previous workpaper data"
