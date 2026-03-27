"""DataShapeSummaryStep 單元測試

測試資料完整性驗證步驟：
- execute() 各分支（有/無 raw_snapshot、有/無 processed data）
- _create_pivot_summary 靜態方法
- _create_comparison_summary 靜態方法
- validate_input
- _export_to_excel
- _load_file 工具函數
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from pathlib import Path

from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_summary_deps():
    """Mock DataShapeSummaryStep 的外部依賴"""
    with patch('accrual_bot.tasks.common.data_shape_summary.config_manager') as mock_cm:
        mock_cm._config_toml = {
            'data_shape_summary': {
                'enabled': True,
                'raw_columns': {
                    'product_col': 'Product Code',
                    'currency_col': 'Currency',
                    'amount_col': 'Entry Amount',
                },
                'processed_columns': {
                    'product_col': 'product_code',
                    'currency_col': 'currency',
                    'amount_col': 'entry_amount',
                },
            }
        }
        yield mock_cm


def _create_raw_df(n=10):
    """建立原始測試資料"""
    return pd.DataFrame({
        'Product Code': ['P001', 'P002'] * (n // 2),
        'Currency': ['TWD', 'USD'] * (n // 2),
        'Entry Amount': ['1000', '2000'] * (n // 2),
        'Item Description': [f'Item {i}' for i in range(n)],
    })


def _create_processed_df(n=8):
    """建立處理後測試資料"""
    return pd.DataFrame({
        'product_code': ['P001', 'P002'] * (n // 2),
        'currency': ['TWD', 'USD'] * (n // 2),
        'entry_amount': ['900', '1800'] * (n // 2),
        'status': ['done'] * n,
    })


@pytest.fixture
def raw_df():
    return _create_raw_df(10)


@pytest.fixture
def processed_df():
    return _create_processed_df(8)


@pytest.fixture
def summary_context(raw_df, processed_df):
    """有完整資料的 context"""
    ctx = ProcessingContext(
        data=processed_df,
        entity_type='SPX',
        processing_date=202503,
        processing_type='PO',
    )
    ctx.add_auxiliary_data('raw_data_snapshot', raw_df)
    return ctx


# ============================================================
# Tests
# ============================================================

class TestDataShapeSummaryStep:
    """測試 DataShapeSummaryStep"""

    @pytest.mark.unit
    def test_instantiation(self, mock_summary_deps):
        """正確初始化"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        step = DataShapeSummaryStep()
        assert step.name == "DataShapeSummary"
        assert step.export_excel is True

    @pytest.mark.unit
    def test_instantiation_no_export(self, mock_summary_deps):
        """不導出 Excel"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        step = DataShapeSummaryStep(export_excel=False)
        assert step.export_excel is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success_full(self, mock_summary_deps, summary_context):
        """完整流程：raw + processed + comparison"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        step = DataShapeSummaryStep(export_excel=False)
        result = await step.execute(summary_context)
        assert result.status == StepStatus.SUCCESS
        # 應產出 3 個分頁
        assert 'sheets_generated' in result.metadata
        assert 'raw_data' in result.metadata['sheets_generated']
        assert 'processed_data' in result.metadata['sheets_generated']
        assert 'comparison' in result.metadata['sheets_generated']

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_no_raw_snapshot(self, mock_summary_deps, processed_df):
        """無原始資料快照時仍成功（只有 processed pivot）"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        step = DataShapeSummaryStep(export_excel=False)
        ctx = ProcessingContext(
            data=processed_df,
            entity_type='SPX',
            processing_date=202503,
            processing_type='PO',
        )
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        sheets = result.metadata.get('sheets_generated', [])
        assert 'processed_data' in sheets
        assert 'raw_data' not in sheets

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_with_excel_export(self, mock_summary_deps, summary_context, tmp_path):
        """導出 Excel 檔案"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        step = DataShapeSummaryStep(export_excel=True, output_dir=str(tmp_path))
        result = await step.execute(summary_context)
        assert result.status == StepStatus.SUCCESS
        assert result.metadata.get('output_path') is not None
        output_path = Path(result.metadata['output_path'])
        assert output_path.exists()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_pass(self, mock_summary_deps, summary_context):
        """有資料時驗證通過"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        step = DataShapeSummaryStep()
        result = await step.validate_input(summary_context)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self, mock_summary_deps):
        """空資料時驗證失敗"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        step = DataShapeSummaryStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SPX',
            processing_date=202503,
            processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.unit
    def test_create_pivot_summary_valid(self, mock_summary_deps, raw_df):
        """pivot table 正確生成"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        pivot = DataShapeSummaryStep._create_pivot_summary(
            raw_df, 'Product Code', 'Currency', 'Entry Amount'
        )
        assert not pivot.empty
        # pivot table 有 margins
        assert 'Total' in pivot.index

    @pytest.mark.unit
    def test_create_pivot_summary_missing_columns(self, mock_summary_deps):
        """欄位不足時返回空 DataFrame"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        df = pd.DataFrame({'col1': [1, 2]})
        pivot = DataShapeSummaryStep._create_pivot_summary(
            df, 'Product Code', 'Currency', 'Entry Amount'
        )
        assert pivot.empty

    @pytest.mark.unit
    def test_create_comparison_summary(self, mock_summary_deps, raw_df, processed_df):
        """比較摘要正確"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        comparison = DataShapeSummaryStep._create_comparison_summary(
            raw_df, processed_df, 'Entry Amount', 'entry_amount'
        )
        assert len(comparison) == 3  # 3 個指標
        assert '指標' in comparison.columns
        assert '原始資料' in comparison.columns
        assert '處理後資料' in comparison.columns
        assert '差異' in comparison.columns
        # 行數差異
        row_diff = comparison.loc[comparison['指標'] == '資料列數', '差異'].values[0]
        assert row_diff == len(processed_df) - len(raw_df)

    @pytest.mark.unit
    def test_create_comparison_missing_amount_col(self, mock_summary_deps):
        """金額欄位不存在時使用 0"""
        from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep
        raw = pd.DataFrame({'col1': [1, 2]})
        final = pd.DataFrame({'col2': [3, 4]})
        comparison = DataShapeSummaryStep._create_comparison_summary(
            raw, final, 'nonexistent_raw', 'nonexistent_final'
        )
        amount_row = comparison.loc[comparison['指標'] == '金額合計']
        assert amount_row['原始資料'].values[0] == 0
        assert amount_row['處理後資料'].values[0] == 0

    @pytest.mark.unit
    def test_load_file_unsupported_format(self, mock_summary_deps):
        """不支援的檔案格式引發 ValueError"""
        from accrual_bot.tasks.common.data_shape_summary import _load_file
        with pytest.raises(ValueError, match="不支援"):
            _load_file('/tmp/test.json')
