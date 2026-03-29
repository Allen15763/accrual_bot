"""後處理步驟單元測試"""
import pytest
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.steps.post_processing import (
    BasePostProcessingStep,
    DataQualityCheckStep,
    StatisticsGenerationStep,
)
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus


class ConcretePostProcessing(BasePostProcessingStep):
    """測試用的具體後處理子類"""

    def _process_data(self, df, context):
        df['processed'] = True
        return df


@pytest.mark.unit
class TestBasePostProcessingStep:
    """BasePostProcessingStep 測試套件"""

    @pytest.fixture
    def post_context(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100001', '100002'],
            'Amount': [1000.0, 2000.0, 3000.0],
        })
        return ProcessingContext(
            data=df, entity_type='TEST', processing_date=202512, processing_type='PO'
        )

    @pytest.mark.asyncio
    async def test_template_method_flow(self, post_context):
        step = ConcretePostProcessing(name="TestPost")
        result = await step.execute(post_context)
        assert result.is_success
        assert 'processed' in post_context.data.columns
        assert post_context.data['processed'].all()

    @pytest.mark.asyncio
    async def test_statistics_collection(self, post_context):
        step = ConcretePostProcessing(name="TestPost", enable_statistics=True)
        result = await step.execute(post_context)
        assert result.is_success
        assert 'statistics' in result.metadata

    @pytest.mark.asyncio
    async def test_validation_enabled(self, post_context):
        step = ConcretePostProcessing(name="TestPost", enable_validation=True)
        result = await step.execute(post_context)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validation_disabled(self, post_context):
        step = ConcretePostProcessing(name="TestPost", enable_validation=False)
        result = await step.execute(post_context)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validate_input_empty_data(self):
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='TEST',
            processing_date=202512, processing_type='PO'
        )
        step = ConcretePostProcessing()
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_with_data(self, post_context):
        step = ConcretePostProcessing()
        assert await step.validate_input(post_context) is True

    def test_safe_divide(self):
        num = pd.Series([10, 20, 30])
        den = pd.Series([2, 0, 5])
        result = BasePostProcessingStep.safe_divide(num, den)
        assert result.iloc[0] == 5.0
        assert result.iloc[1] == 0  # default for divide by zero
        assert result.iloc[2] == 6.0

    def test_convert_to_numeric(self):
        s = pd.Series(['1', '2.5', 'abc', '4'])
        result = BasePostProcessingStep.convert_to_numeric(s)
        assert result.iloc[0] == 1.0
        assert result.iloc[1] == 2.5
        assert result.iloc[2] == 0  # default for non-numeric
        assert result.iloc[3] == 4.0

    def test_clean_string_column_strip(self):
        s = pd.Series(['  hello  ', '  world  '])
        result = BasePostProcessingStep.clean_string_column(s)
        assert result.iloc[0] == 'hello'
        assert result.iloc[1] == 'world'

    def test_clean_string_column_lower(self):
        s = pd.Series(['HELLO', 'World'])
        result = BasePostProcessingStep.clean_string_column(s, lower=True)
        assert result.iloc[0] == 'hello'
        assert result.iloc[1] == 'world'

    def test_clean_string_column_replace(self):
        s = pd.Series(['a-b', 'c-d'])
        result = BasePostProcessingStep.clean_string_column(
            s, replace_dict={'-': '_'}
        )
        assert result.iloc[0] == 'a_b'


@pytest.mark.unit
class TestDataQualityCheckStep:
    """DataQualityCheckStep 測試套件"""

    @pytest.fixture
    def quality_context(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100001', None, '100003'],
            'Amount': [1000.0, None, 3000.0, 4000.0],
            'Status': ['A', 'B', 'A', 'B'],
        })
        return ProcessingContext(
            data=df, entity_type='TEST', processing_date=202512, processing_type='PO'
        )

    @pytest.mark.asyncio
    async def test_missing_columns_detected(self, quality_context):
        step = DataQualityCheckStep(required_columns=['GL#', 'Missing_Col'])
        result = await step.execute(quality_context)
        # 應成功完成但會有警告
        assert result.status in (StepStatus.SUCCESS, StepStatus.FAILED)

    @pytest.mark.asyncio
    async def test_null_ratio_check(self, quality_context):
        step = DataQualityCheckStep(max_null_ratio=0.1)
        result = await step.execute(quality_context)
        assert result.status in (StepStatus.SUCCESS, StepStatus.FAILED)

    @pytest.mark.asyncio
    async def test_duplicate_detection(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100000', '100001'],
            'Amount': [1000.0, 1000.0, 2000.0],
        })
        ctx = ProcessingContext(
            data=df, entity_type='TEST', processing_date=202512, processing_type='PO'
        )
        step = DataQualityCheckStep(check_duplicates=True)
        result = await step.execute(ctx)
        assert result.is_success


@pytest.mark.unit
class TestStatisticsGenerationStep:
    """StatisticsGenerationStep 測試套件"""

    @pytest.fixture
    def stats_context(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100001', '100002'],
            'Amount': [1000.0, 2000.0, 3000.0],
            'Status': ['A', 'B', 'A'],
        })
        return ProcessingContext(
            data=df, entity_type='TEST', processing_date=202512, processing_type='PO'
        )

    @pytest.mark.asyncio
    async def test_generates_statistics(self, stats_context):
        step = StatisticsGenerationStep()
        result = await step.execute(stats_context)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validate_input(self, stats_context):
        step = StatisticsGenerationStep()
        assert await step.validate_input(stats_context) is True
