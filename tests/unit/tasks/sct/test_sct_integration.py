"""SCT Integration Steps 單元測試

測試 APInvoiceIntegrationStep：
- AP Invoice 數據整合
- period 過濾
- PO Line 匹配
- 空資料處理
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

def _create_main_df(n=3):
    """建立主資料 DataFrame"""
    return pd.DataFrame({
        'PO Line': [f'COMP-PO{i:03d}-{i+1}' for i in range(n)],
        'Entry Amount': ['10000', '20000', '30000'][:n],
        'Item Description': [f'Item {i}' for i in range(n)],
    })


def _create_ap_invoice_df():
    """建立 AP Invoice 測試資料"""
    return pd.DataFrame({
        'Company': ['COMP', 'COMP', 'COMP', 'COMP'],
        'PO Number': ['PO000', 'PO001', 'PO000', 'PO002'],
        'PO_LINE_NUMBER': ['1', '2', '1', '3'],
        'Period': ['Jan-25', 'Feb-25', 'Mar-25', 'Apr-25'],
        'Match Type': ['ITEM_TO_RECEIPT', 'ITEM_TO_PO', None, 'ITEM_TO_RECEIPT'],
        'VOUCHER_NUMBER': ['V001', 'V002', 'V003', 'V004'],
    })


@pytest.fixture
def main_df():
    return _create_main_df(3)


@pytest.fixture
def ap_invoice_df():
    return _create_ap_invoice_df()


@pytest.fixture
def integration_context(main_df, ap_invoice_df):
    """整合測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=main_df,
        entity_type='SCT',
        processing_date=202503,
        processing_type='PO',
    )
    ctx.add_auxiliary_data('ap_invoice', ap_invoice_df)
    return ctx


# ============================================================
# APInvoiceIntegrationStep 測試
# ============================================================

class TestAPInvoiceIntegrationStep:
    """測試 AP Invoice 整合步驟"""

    @pytest.mark.unit
    def test_instantiation(self):
        """正確初始化"""
        from accrual_bot.tasks.sct.steps.sct_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        assert step.name == "APInvoiceIntegration"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_skips_when_no_ap_data(self):
        """無 AP Invoice 資料時 SKIPPED"""
        from accrual_bot.tasks.sct.steps.sct_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        ctx = ProcessingContext(
            data=_create_main_df(3),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        # 不添加 ap_invoice auxiliary data
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_skips_when_ap_empty(self):
        """AP Invoice 為空時 SKIPPED"""
        from accrual_bot.tasks.sct.steps.sct_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        ctx = ProcessingContext(
            data=_create_main_df(3),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        ctx.add_auxiliary_data('ap_invoice', pd.DataFrame())
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success_with_matching(self, integration_context):
        """成功整合 AP Invoice 並匹配 voucher_number"""
        from accrual_bot.tasks.sct.steps.sct_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        result = await step.execute(integration_context)
        assert result.status == StepStatus.SUCCESS
        df = integration_context.data
        assert 'voucher_number' in df.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_pass(self, integration_context):
        """有資料且有 PO Line 欄位時驗證通過"""
        from accrual_bot.tasks.sct.steps.sct_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        result = await step.validate_input(integration_context)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_no_po_line(self):
        """缺少 PO Line 欄位時驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        ctx = ProcessingContext(
            data=pd.DataFrame({'col1': [1, 2]}),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料時驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        result = await step.validate_input(ctx)
        assert result is False
