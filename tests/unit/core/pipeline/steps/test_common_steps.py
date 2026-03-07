"""通用處理步驟單元測試"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch

from accrual_bot.core.pipeline.steps.common import (
    DataCleaningStep,
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
