"""通用處理步驟單元測試"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.steps.common import (
    DataCleaningStep,
    DateFormattingStep,
    DateParsingStep,
    ValidationStep,
    ExportStep,
    ProductFilterStep,
    PreviousWorkpaperIntegrationStep,
    ProcurementIntegrationStep,
    DateLogicStep,
    StepMetadataBuilder,
    create_error_metadata,
)
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus


@pytest.mark.unit
class TestDataCleaningStep:
    """DataCleaningStep 測試套件"""

    @pytest.fixture
    def dirty_context(self):
        df = pd.DataFrame({
            'Name': ['  Alice  ', 'Bob', '  nan  ', 'None', 'N/A'],
            'Amount': ['100', '200', '300', '400', '500'],
            'Code': [pd.NA, 'A', 'B', pd.NA, 'C'],
        })
        return ProcessingContext(
            data=df, entity_type='TEST', processing_date=202512, processing_type='PO'
        )

    @pytest.mark.asyncio
    async def test_strips_whitespace(self, dirty_context):
        step = DataCleaningStep()
        result = await step.execute(dirty_context)
        assert result.is_success
        assert dirty_context.data['Name'].iloc[0] == 'Alice'

    @pytest.mark.asyncio
    async def test_replaces_nan_string(self, dirty_context):
        step = DataCleaningStep()
        result = await step.execute(dirty_context)
        assert result.is_success
        assert dirty_context.data['Name'].iloc[2] == ''

    @pytest.mark.asyncio
    async def test_replaces_none_string(self, dirty_context):
        step = DataCleaningStep()
        result = await step.execute(dirty_context)
        assert dirty_context.data['Name'].iloc[3] == ''

    @pytest.mark.asyncio
    async def test_replaces_na_string(self, dirty_context):
        step = DataCleaningStep()
        result = await step.execute(dirty_context)
        assert dirty_context.data['Name'].iloc[4] == ''

    @pytest.mark.asyncio
    async def test_specific_columns_only(self):
        df = pd.DataFrame({
            'Col1': ['  x  '],
            'Col2': ['  y  '],
        })
        ctx = ProcessingContext(
            data=df, entity_type='TEST', processing_date=202512, processing_type='PO'
        )
        step = DataCleaningStep(columns_to_clean=['Col1'])
        result = await step.execute(ctx)
        assert result.is_success
        assert ctx.data['Col1'].iloc[0] == 'x'

    @pytest.mark.asyncio
    async def test_validate_input_empty_data_fails(self):
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='TEST',
            processing_date=202512, processing_type='PO'
        )
        step = DataCleaningStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_with_data_passes(self, dirty_context):
        step = DataCleaningStep()
        assert await step.validate_input(dirty_context) is True

    @pytest.mark.asyncio
    async def test_metadata_reports_cleaned_columns(self, dirty_context):
        step = DataCleaningStep()
        result = await step.execute(dirty_context)
        assert 'cleaned_columns' in result.metadata


@pytest.mark.unit
class TestStepMetadataBuilder:
    """StepMetadataBuilder 測試套件"""

    def test_build_empty(self):
        builder = StepMetadataBuilder()
        result = builder.build()
        assert isinstance(result, dict)

    def test_set_row_counts(self):
        builder = StepMetadataBuilder()
        result = builder.set_row_counts(input_rows=100, output_rows=90).build()
        assert result['input_rows'] == 100
        assert result['output_rows'] == 90

    def test_set_process_counts(self):
        builder = StepMetadataBuilder()
        result = builder.set_process_counts(
            processed=80, skipped=10, failed=5
        ).build()
        assert result['records_processed'] == 80
        assert result['records_skipped'] == 10
        assert result['records_failed'] == 5

    def test_add_custom(self):
        builder = StepMetadataBuilder()
        result = builder.add_custom('my_key', 'my_value').build()
        assert result['my_key'] == 'my_value'

    def test_fluent_chaining(self):
        result = (
            StepMetadataBuilder()
            .set_row_counts(100, 90)
            .set_process_counts(80, 10, 5)
            .add_custom('entity', 'SPX')
            .build()
        )
        assert result['input_rows'] == 100
        assert result['entity'] == 'SPX'


@pytest.mark.unit
class TestCreateErrorMetadata:
    """create_error_metadata 測試"""

    @pytest.fixture
    def dummy_context(self):
        return ProcessingContext(
            data=pd.DataFrame({'col': [1, 2]}),
            entity_type='TEST', processing_date=202512, processing_type='PO'
        )

    def test_basic_error(self, dummy_context):
        try:
            raise ValueError("test error")
        except ValueError as e:
            meta = create_error_metadata(e, dummy_context, step_name="TestStep")
        assert meta['step_name'] == 'TestStep'
        assert 'error_type' in meta
        assert meta['error_type'] == 'ValueError'
        assert 'test error' in meta['error_message']

    def test_with_traceback(self, dummy_context):
        try:
            raise RuntimeError("oops")
        except RuntimeError as e:
            meta = create_error_metadata(e, dummy_context, step_name="S")
        assert 'error_traceback' in meta


# =============================================================================
# 以下為新增測試：提升覆蓋率至 ~70%
# =============================================================================


def _make_context(df, entity='TEST', processing_date=202512, processing_type='PO'):
    """建立測試用 ProcessingContext 的輔助函式"""
    return ProcessingContext(
        data=df, entity_type=entity,
        processing_date=processing_date, processing_type=processing_type,
    )


# ---- DateFormattingStep 測試 ----

@pytest.mark.unit
class TestDateFormattingStep:
    """DateFormattingStep 測試套件"""

    @pytest.mark.asyncio
    async def test_format_month_column(self):
        """含 Month 的欄位應輸出 YYYY-MM 格式"""
        df = pd.DataFrame({
            'Expected Receive Month': ['Jan-25', 'Feb-25'],
        })
        ctx = _make_context(df)
        step = DateFormattingStep(date_columns={'Expected Receive Month': '%b-%y'})
        result = await step.execute(ctx)
        assert result.is_success
        assert ctx.data['Expected Receive Month'].iloc[0] == '2025-01'

    @pytest.mark.asyncio
    async def test_format_date_column(self):
        """非 Month 欄位應輸出 YYYY-MM-DD 格式"""
        df = pd.DataFrame({
            'Submission Date': ['15-Jan-25', '20-Feb-25'],
        })
        ctx = _make_context(df)
        step = DateFormattingStep(date_columns={'Submission Date': '%d-%b-%y'})
        result = await step.execute(ctx)
        assert result.is_success
        assert ctx.data['Submission Date'].iloc[0] == '2025-01-15'

    @pytest.mark.asyncio
    async def test_missing_column_skipped(self):
        """不存在的欄位不會導致錯誤"""
        df = pd.DataFrame({'col': [1]})
        ctx = _make_context(df)
        step = DateFormattingStep(date_columns={'NonExistent': '%Y-%m-%d'})
        result = await step.execute(ctx)
        assert result.is_success
        assert result.metadata['formatted_columns'] == 0

    @pytest.mark.asyncio
    async def test_validate_input_empty_fails(self):
        """空 DataFrame 驗證失敗"""
        ctx = _make_context(pd.DataFrame())
        step = DateFormattingStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_with_data_passes(self):
        """有資料的 DataFrame 驗證通過"""
        ctx = _make_context(pd.DataFrame({'col': [1]}))
        step = DateFormattingStep()
        assert await step.validate_input(ctx) is True


# ---- DateParsingStep 測試 ----

@pytest.mark.unit
class TestDateParsingStep:
    """DateParsingStep 測試套件"""

    @pytest.mark.asyncio
    async def test_execute_success(self):
        """基本執行成功"""
        df = pd.DataFrame({'Item Description': ['Service Jan-25 to Mar-25']})
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get_regex_patterns.return_value = {}
            step = DateParsingStep()
            result = await step.execute(ctx)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validate_input_missing_column(self):
        """缺少 Item Description 欄位驗證失敗"""
        df = pd.DataFrame({'other': [1]})
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get_regex_patterns.return_value = {}
            step = DateParsingStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_has_column(self):
        """有 Item Description 欄位驗證通過"""
        df = pd.DataFrame({'Item Description': ['test']})
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get_regex_patterns.return_value = {}
            step = DateParsingStep()
        assert await step.validate_input(ctx) is True


# ---- ValidationStep 測試 ----

@pytest.mark.unit
class TestValidationStep:
    """ValidationStep 測試套件"""

    @pytest.mark.asyncio
    async def test_validation_pass_po(self):
        """PO 處理 - 所有必要欄位存在，驗證通過"""
        df = pd.DataFrame({
            'PO#': ['P001'],
            'Item Description': ['Item A'],
            'GL#': ['100000'],
            'Entry Amount': [1000],
        })
        ctx = _make_context(df, processing_type='PO')
        step = ValidationStep(validations=['required_columns'])
        result = await step.execute(ctx)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validation_fail_missing_columns_po(self):
        """PO 處理 - 缺少必要欄位，驗證失敗"""
        df = pd.DataFrame({'col': [1]})
        ctx = _make_context(df, processing_type='PO')
        step = ValidationStep(validations=['required_columns'])
        result = await step.execute(ctx)
        assert result.is_failed

    @pytest.mark.asyncio
    async def test_validation_pass_pr(self):
        """PR 處理 - 所有必要欄位存在"""
        df = pd.DataFrame({
            'PR#': ['R001'],
            'Item Description': ['Item A'],
            'GL#': ['100000'],
        })
        ctx = _make_context(df, processing_type='PR')
        step = ValidationStep(validations=['required_columns'])
        result = await step.execute(ctx)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_data_types_validation(self):
        """資料型別驗證 - 數值欄位包含非數值不崩潰"""
        df = pd.DataFrame({
            'Entry Amount': ['abc', '100', '200'],
            'Entry Quantity': [1, 2, 3],
        })
        ctx = _make_context(df)
        step = ValidationStep(validations=['data_types'])
        result = await step.execute(ctx)
        # data_types 驗證即使有非數值也只是加 warning，不 fail
        assert result.is_success

    @pytest.mark.asyncio
    async def test_business_rules_negative_amounts(self):
        """業務規則驗證 - 負金額產生警告"""
        df = pd.DataFrame({
            'Entry Amount': [-100, 200, -50],
        })
        ctx = _make_context(df)
        step = ValidationStep(validations=['business_rules'])
        result = await step.execute(ctx)
        # 負金額只是警告，不 fail
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validate_input_empty(self):
        """空 DataFrame 驗證不通過"""
        ctx = _make_context(pd.DataFrame())
        step = ValidationStep()
        assert await step.validate_input(ctx) is False


# ---- ExportStep 測試 ----

@pytest.mark.unit
class TestExportStep:
    """ExportStep 測試套件"""

    @pytest.mark.asyncio
    async def test_export_excel(self, tmp_path):
        """Excel 格式導出成功"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        ctx = _make_context(df, entity='SPT')
        step = ExportStep(format='excel', output_path=str(tmp_path))
        result = await step.execute(ctx)
        assert result.is_success
        assert result.metadata['rows'] == 3

    @pytest.mark.asyncio
    async def test_export_csv(self, tmp_path):
        """CSV 格式導出成功"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        ctx = _make_context(df, entity='SPX')
        step = ExportStep(format='csv', output_path=str(tmp_path))
        result = await step.execute(ctx)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_export_unsupported_format(self, tmp_path):
        """不支援的格式返回 FAILED"""
        df = pd.DataFrame({'col': [1]})
        ctx = _make_context(df)
        step = ExportStep(format='json', output_path=str(tmp_path))
        result = await step.execute(ctx)
        assert result.is_failed

    @pytest.mark.asyncio
    async def test_validate_input_empty(self):
        """空 DataFrame 驗證不通過"""
        ctx = _make_context(pd.DataFrame())
        step = ExportStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_with_data(self):
        """有資料的 DataFrame 驗證通過"""
        ctx = _make_context(pd.DataFrame({'a': [1]}))
        step = ExportStep()
        assert await step.validate_input(ctx) is True


# ---- ProductFilterStep 測試 ----

@pytest.mark.unit
class TestProductFilterStep:
    """ProductFilterStep 測試套件"""

    @pytest.mark.asyncio
    async def test_filter_include(self):
        """包含模式過濾"""
        df = pd.DataFrame({
            'Product Code': ['LG_SPX_001', 'OTHER_002', 'LG_SPX_003'],
        })
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get.return_value = '(?i)LG_SPX'
            step = ProductFilterStep(product_pattern='(?i)LG_SPX')
        result = await step.execute(ctx)
        assert result.is_success
        assert len(ctx.data) == 2

    @pytest.mark.asyncio
    async def test_filter_exclude(self):
        """排除模式過濾"""
        df = pd.DataFrame({
            'Product Code': ['LG_SPX_001', 'OTHER_002', 'LG_SPX_003'],
        })
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get.return_value = '(?i)LG_SPX'
            step = ProductFilterStep(product_pattern='(?i)LG_SPX', exclude=True)
        result = await step.execute(ctx)
        assert result.is_success
        assert len(ctx.data) == 1
        assert ctx.data['Product Code'].iloc[0] == 'OTHER_002'

    @pytest.mark.asyncio
    async def test_filter_all_removed_adds_warning(self):
        """全部被過濾掉時加入警告"""
        df = pd.DataFrame({
            'Product Code': ['OTHER_001', 'OTHER_002'],
        })
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get.return_value = '(?i)LG_SPX'
            step = ProductFilterStep(product_pattern='(?i)LG_SPX')
        result = await step.execute(ctx)
        assert result.is_success
        assert len(ctx.data) == 0
        assert len(ctx.warnings) > 0

    @pytest.mark.asyncio
    async def test_validate_input_no_product_code(self):
        """缺少 Product Code 欄位驗證失敗"""
        ctx = _make_context(pd.DataFrame({'other': [1]}))
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get.return_value = '(?i)LG_SPX'
            step = ProductFilterStep(product_pattern='(?i)LG_SPX')
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_empty_data(self):
        """空 DataFrame 驗證失敗"""
        ctx = _make_context(pd.DataFrame())
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get.return_value = '(?i)LG_SPX'
            step = ProductFilterStep(product_pattern='(?i)LG_SPX')
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_success(self):
        """有 Product Code 欄位驗證通過"""
        ctx = _make_context(pd.DataFrame({'Product Code': ['A']}))
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get.return_value = '(?i)LG_SPX'
            step = ProductFilterStep(product_pattern='(?i)LG_SPX')
        assert await step.validate_input(ctx) is True


# ---- PreviousWorkpaperIntegrationStep 測試 ----

@pytest.mark.unit
class TestPreviousWorkpaperIntegrationStep:
    """PreviousWorkpaperIntegrationStep 測試套件"""

    @pytest.mark.asyncio
    async def test_skip_when_no_previous_data(self):
        """無前期底稿資料時跳過"""
        df = pd.DataFrame({'PO Line': ['P001-1'], 'GL#': ['100000']})
        ctx = _make_context(df)
        ctx.set_variable('file_paths', {})
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm._config_toml = {}
            step = PreviousWorkpaperIntegrationStep()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_validate_input_empty(self):
        """空 DataFrame 驗證失敗"""
        ctx = _make_context(pd.DataFrame())
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm._config_toml = {}
            step = PreviousWorkpaperIntegrationStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_with_data(self):
        """有資料的 DataFrame 驗證通過"""
        df = pd.DataFrame({'PO Line': ['P001-1']})
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm._config_toml = {}
            step = PreviousWorkpaperIntegrationStep()
        assert await step.validate_input(ctx) is True


# ---- ProcurementIntegrationStep 測試 ----

@pytest.mark.unit
class TestProcurementIntegrationStep:
    """ProcurementIntegrationStep 測試套件"""

    @pytest.mark.asyncio
    async def test_skip_when_no_procurement_data(self):
        """無採購底稿時跳過"""
        df = pd.DataFrame({'PO Line': ['P001-1'], 'GL#': ['100000']})
        ctx = _make_context(df)
        ctx.set_variable('file_paths', {})
        step = ProcurementIntegrationStep()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_validate_input_empty(self):
        """空 DataFrame 驗證失敗"""
        ctx = _make_context(pd.DataFrame())
        step = ProcurementIntegrationStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_with_data(self):
        """有資料的 DataFrame 驗證通過"""
        df = pd.DataFrame({'col': [1]})
        ctx = _make_context(df)
        step = ProcurementIntegrationStep()
        assert await step.validate_input(ctx) is True

    @pytest.mark.asyncio
    async def test_process_procurement_po_basic(self):
        """基本採購 PO 底稿處理"""
        df = pd.DataFrame({
            'PO Line': ['P001-1', 'P002-1'],
            'PR Line': ['R001-1', 'R002-1'],
            'PO狀態': [pd.NA, pd.NA],
        })
        procurement = pd.DataFrame({
            'PO Line': ['P001-1'],
            'Remarked by Procurement': ['已確認'],
        })
        ctx = _make_context(df)
        ctx.add_auxiliary_data('procurement_po', procurement)
        ctx.set_variable('file_paths', {'raw_po': '/tmp/po.xlsx'})
        step = ProcurementIntegrationStep()
        result = await step.execute(ctx)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_process_procurement_pr_basic(self):
        """基本採購 PR 底稿處理"""
        df = pd.DataFrame({
            'PR Line': ['R001-1', 'R002-1'],
        })
        procurement_pr = pd.DataFrame({
            'PR Line': ['R001-1'],
            'Remarked by Procurement': ['已確認'],
        })
        ctx = _make_context(df)
        ctx.add_auxiliary_data('procurement_pr', procurement_pr)
        ctx.set_variable('file_paths', {})
        step = ProcurementIntegrationStep()
        result = await step.execute(ctx)
        assert result.is_success


# ---- DateLogicStep 測試 ----

@pytest.mark.unit
class TestDateLogicStep:
    """DateLogicStep 測試套件"""

    @pytest.mark.asyncio
    async def test_validate_input_empty(self):
        """空 DataFrame 驗證失敗"""
        ctx = _make_context(pd.DataFrame())
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get_regex_patterns.return_value = {}
            step = DateLogicStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_missing_column(self):
        """缺少 Item Description 欄位驗證失敗"""
        df = pd.DataFrame({'other': [1]})
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get_regex_patterns.return_value = {}
            step = DateLogicStep()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_success(self):
        """有 Item Description 欄位驗證通過"""
        df = pd.DataFrame({'Item Description': ['test']})
        ctx = _make_context(df)
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get_regex_patterns.return_value = {}
            step = DateLogicStep()
        assert await step.validate_input(ctx) is True

    @pytest.mark.asyncio
    async def test_execute_profit_sharing(self):
        """分潤合作邏輯 - 匹配的行設定狀態"""
        df = pd.DataFrame({
            'Item Description': ['分潤合作 Jan-25', 'Regular item'],
            'PO狀態': [pd.NA, pd.NA],
        })
        ctx = _make_context(df, entity='SPX')
        ctx.set_variable('file_paths', {'raw_po': '/tmp/po.xlsx'})
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm:
            mock_cm.get_regex_patterns.return_value = {}
            step = DateLogicStep()
        result = await step.execute(ctx)
        assert result.is_success
        assert ctx.data.loc[0, 'PO狀態'] == '分潤'

    @pytest.mark.asyncio
    async def test_execute_posted_spt(self):
        """已入帳邏輯 - SPT 實體且 full invoiced status=1"""
        df = pd.DataFrame({
            'Item Description': ['Service'],
            'PO狀態': [pd.NA],
            'PO Entry full invoiced status': ['1'],
        })
        ctx = _make_context(df, entity='SPT')
        ctx.set_variable('file_paths', {'raw_po': '/tmp/po.xlsx'})
        with patch('accrual_bot.core.pipeline.steps.common.config_manager') as mock_cm, \
             patch('accrual_bot.core.pipeline.steps.common.STATUS_VALUES', {'POSTED': '已入帳'}):
            mock_cm.get_regex_patterns.return_value = {}
            step = DateLogicStep()
            result = await step.execute(ctx)
        assert result.is_success
