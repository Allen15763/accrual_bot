"""業務邏輯步驟單元測試"""
import pytest
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.steps.business import (
    StatusEvaluationStep,
    AccountingAdjustmentStep,
    AccountCodeMappingStep,
    DepartmentConversionStep,
)
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus


@pytest.mark.unit
class TestStatusEvaluationStep:
    """StatusEvaluationStep 測試套件"""

    @pytest.fixture
    def eval_context(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100001', '100002'],
            'Entry Quantity': [100, 200, 300],
            'Billed Quantity': [100, 150, 0],
            'Expected Receive Month': ['202512', '202512', '202501'],
            'Item Description': ['Item A', 'Item B', 'Item C'],
            'PO狀態': [pd.NA, pd.NA, pd.NA],
        })
        return ProcessingContext(
            data=df, entity_type='SPT', processing_date=202512, processing_type='PO'
        )

    @pytest.mark.asyncio
    async def test_evaluates_status(self, eval_context):
        step = StatusEvaluationStep(entity_type='SPT')
        result = await step.execute(eval_context)
        assert result.is_success
        assert 'status_counts' in result.metadata

    @pytest.mark.asyncio
    async def test_sets_status_column(self, eval_context):
        step = StatusEvaluationStep(entity_type='SPT')
        result = await step.execute(eval_context)
        assert 'PO狀態' in eval_context.data.columns
        assert eval_context.data['PO狀態'].notna().all()

    @pytest.mark.asyncio
    async def test_validate_input(self, eval_context):
        """有 Expected Receive Month 和 Item Description 欄位應通過驗證"""
        step = StatusEvaluationStep()
        assert await step.validate_input(eval_context) is True

    @pytest.mark.asyncio
    async def test_validate_missing_columns(self):
        """缺少必要欄位應驗證失敗"""
        ctx = ProcessingContext(
            data=pd.DataFrame({'GL#': ['100000']}),
            entity_type='TEST', processing_date=202512, processing_type='PO'
        )
        step = StatusEvaluationStep()
        assert await step.validate_input(ctx) is False


@pytest.mark.unit
class TestAccountingAdjustmentStep:
    """AccountingAdjustmentStep 測試套件"""

    @pytest.fixture
    def acct_context(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100001', '199999'],
            'PO狀態': ['已完成', '未完成', '已完成'],
            'Entry Quantity': [100, 200, 50],
            'Billed Quantity': [100, 150, 50],
            'Unit Price': [100.0, 200.0, 500.0],
            'Entry Amount': [10000, 40000, 25000],
            'Entry Billed Amount': [10000, 30000, 25000],
            'Currency': ['TWD', 'TWD', 'TWD'],
            'Department': ['001', '002', '003'],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPT', processing_date=202512, processing_type='PO'
        )
        ctx.add_auxiliary_data('reference_account', pd.DataFrame({
            'Account': ['100000', '100001', '199999'],
            'Account Desc': ['Cash', 'Receivables', 'FA']
        }))
        return ctx

    @pytest.mark.asyncio
    async def test_execute_success(self, acct_context):
        """AccountingAdjustmentStep 不接受 entity_type kwargs"""
        step = AccountingAdjustmentStep()
        result = await step.execute(acct_context)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validate_input(self, acct_context):
        step = AccountingAdjustmentStep()
        assert await step.validate_input(acct_context) is True


@pytest.mark.unit
class TestAccountCodeMappingStep:
    """AccountCodeMappingStep 測試套件"""

    @pytest.fixture
    def mapping_context(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100001', '999999'],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPT', processing_date=202512, processing_type='PO'
        )
        # AccountCodeMappingStep 預設使用 'account_mapping' 作為 mapping source
        ctx.add_auxiliary_data('account_mapping', pd.DataFrame({
            'GL#': ['100000', '100001'],
            'Account Code': ['AC001', 'AC002']
        }))
        return ctx

    @pytest.mark.asyncio
    async def test_maps_gl_codes(self, mapping_context):
        step = AccountCodeMappingStep()
        result = await step.execute(mapping_context)
        assert result.is_success
        df = mapping_context.data
        assert 'Mapped_Account_Code' in df.columns

    @pytest.mark.asyncio
    async def test_no_mapping_data_skips(self):
        ctx = ProcessingContext(
            data=pd.DataFrame({'GL#': ['100000']}),
            entity_type='TEST', processing_date=202512, processing_type='PO'
        )
        step = AccountCodeMappingStep()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_validate_input(self, mapping_context):
        step = AccountCodeMappingStep()
        assert await step.validate_input(mapping_context) is True


@pytest.mark.unit
class TestDepartmentConversionStep:
    """DepartmentConversionStep 測試套件"""

    @pytest.fixture
    def dept_context(self):
        df = pd.DataFrame({
            'Department': ['001', '002', '000'],
            'Account code': ['100000', '500000', '900000'],
        })
        return ProcessingContext(
            data=df, entity_type='SPT', processing_date=202512, processing_type='PO'
        )

    @pytest.mark.asyncio
    async def test_execute_success(self, dept_context):
        """DepartmentConversionStep 不接受 entity_type kwargs"""
        step = DepartmentConversionStep()
        result = await step.execute(dept_context)
        assert result.is_success

    @pytest.mark.asyncio
    async def test_validate_input(self, dept_context):
        step = DepartmentConversionStep()
        assert await step.validate_input(dept_context) is True
