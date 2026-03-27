"""
ColumnInitializationStep 單元測試

測試 ColumnInitializationStep 的核心功能：
- 步驟初始化
- PO 模式：PO Line 組合鍵建立、Supplier 映射
- PR 模式：PR Line 組合鍵建立、Supplier 映射
- 狀態欄位初始化
- Procurement 欄位初始化
- execute() 完整流程
- validate_input() 輸入驗證
- 邊界情況：空 DataFrame、缺少欄位
"""

import pytest
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.tasks.spt.steps.spt_column_initialization import ColumnInitializationStep


# ============================================================
# 測試用資料建構
# ============================================================

def _create_po_df(n: int = 5) -> pd.DataFrame:
    """建立 PO 測試用 DataFrame"""
    return pd.DataFrame({
        'PO#': [f'PO{i:03d}' for i in range(n)],
        'Line#': [str(i + 1) for i in range(n)],
        'PO Supplier': [f'Supplier {i}' for i in range(n)],
        'GL#': [str(300000 + i) for i in range(n)],
        'Entry Amount': [1000 * (i + 1) for i in range(n)],
    })


def _create_pr_df(n: int = 5) -> pd.DataFrame:
    """建立 PR 測試用 DataFrame"""
    return pd.DataFrame({
        'PR#': [f'PR{i:03d}' for i in range(n)],
        'Line#': [str(i + 1) for i in range(n)],
        'PR Supplier': [f'Supplier {i}' for i in range(n)],
        'GL#': [str(300000 + i) for i in range(n)],
        'Entry Amount': [1000 * (i + 1) for i in range(n)],
    })


# ============================================================
# 初始化測試
# ============================================================

@pytest.mark.unit
class TestColumnInitializationStepInit:
    """ColumnInitializationStep 初始化測試"""

    def test_default_status_column(self):
        """測試預設狀態欄位為 PO狀態"""
        step = ColumnInitializationStep(name="TestInit")
        assert step.status_column == "PO狀態"

    def test_custom_status_column(self):
        """測試自訂狀態欄位"""
        step = ColumnInitializationStep(status_column="PR狀態", name="TestInit")
        assert step.status_column == "PR狀態"

    def test_is_pr_true(self):
        """測試 _is_pr 在 PR 狀態時回傳 True"""
        step = ColumnInitializationStep(status_column="PR狀態", name="TestInit")
        # _is_pr 只檢查 status_column 中是否含 'PR'
        assert step._is_pr(None) is True

    def test_is_pr_false(self):
        """測試 _is_pr 在 PO 狀態時回傳 False"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestInit")
        assert step._is_pr(None) is False


# ============================================================
# PO 模式 execute 測試
# ============================================================

@pytest.mark.unit
class TestColumnInitPOMode:
    """PO 模式 execute 測試"""

    @pytest.mark.asyncio
    async def test_po_line_created(self):
        """測試 PO Line 組合鍵正確建立（PO# + '-' + Line#）"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestPO")
        df = _create_po_df(3)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'PO Line' in ctx.data.columns
        assert ctx.data['PO Line'].iloc[0] == 'PO000-1'
        assert ctx.data['PO Line'].iloc[2] == 'PO002-3'

    @pytest.mark.asyncio
    async def test_po_supplier_mapped(self):
        """測試 Supplier 欄位從 PO Supplier 映射"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestPO")
        df = _create_po_df(3)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.execute(ctx)

        assert 'Supplier' in ctx.data.columns
        assert ctx.data['Supplier'].iloc[0] == 'Supplier 0'

    @pytest.mark.asyncio
    async def test_po_status_column_created(self):
        """測試 PO狀態 欄位被建立且為空"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestPO")
        df = _create_po_df(3)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.execute(ctx)

        assert 'PO狀態' in ctx.data.columns
        assert ctx.data['PO狀態'].isna().all()
        assert result.metadata['created'] is True

    @pytest.mark.asyncio
    async def test_procurement_columns_created(self):
        """測試 Remarked/Noted by Procurement 欄位被建立"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestPO")
        df = _create_po_df(2)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.execute(ctx)

        assert 'Remarked by Procurement' in ctx.data.columns
        assert 'Noted by Procurement' in ctx.data.columns
        assert ctx.data['Remarked by Procurement'].isna().all()


# ============================================================
# PR 模式 execute 測試
# ============================================================

@pytest.mark.unit
class TestColumnInitPRMode:
    """PR 模式 execute 測試"""

    @pytest.mark.asyncio
    async def test_pr_line_created(self):
        """測試 PR Line 組合鍵正確建立（PR# + '-' + Line#）"""
        step = ColumnInitializationStep(status_column="PR狀態", name="TestPR")
        df = _create_pr_df(3)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PR'
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'PR Line' in ctx.data.columns
        assert ctx.data['PR Line'].iloc[0] == 'PR000-1'

    @pytest.mark.asyncio
    async def test_pr_supplier_mapped(self):
        """測試 Supplier 欄位從 PR Supplier 映射"""
        step = ColumnInitializationStep(status_column="PR狀態", name="TestPR")
        df = _create_pr_df(3)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PR'
        )
        result = await step.execute(ctx)

        assert 'Supplier' in ctx.data.columns
        assert ctx.data['Supplier'].iloc[1] == 'Supplier 1'

    @pytest.mark.asyncio
    async def test_pr_status_column_created(self):
        """測試 PR狀態 欄位被建立"""
        step = ColumnInitializationStep(status_column="PR狀態", name="TestPR")
        df = _create_pr_df(2)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PR'
        )
        result = await step.execute(ctx)

        assert 'PR狀態' in ctx.data.columns
        assert result.metadata['status_column'] == 'PR狀態'


# ============================================================
# 邊界情況測試
# ============================================================

@pytest.mark.unit
class TestColumnInitEdgeCases:
    """邊界情況測試"""

    @pytest.mark.asyncio
    async def test_validate_input_empty_data_returns_false(self):
        """測試空數據驗證失敗"""
        step = ColumnInitializationStep(name="TestEmpty")
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SPT',
            processing_date=202512,
            processing_type='PO'
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_input_valid_data_returns_true(self):
        """測試有效數據驗證通過"""
        step = ColumnInitializationStep(name="TestValid")
        df = _create_po_df(1)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.asyncio
    async def test_status_column_already_exists(self):
        """測試狀態欄位已存在時不重新建立"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestExist")
        df = _create_po_df(2)
        df['PO狀態'] = '已完成'
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['created'] is False

    @pytest.mark.asyncio
    async def test_po_line_handles_na_values(self):
        """測試 PO# 或 Line# 含 NaN 時正確處理（fillna 為空字串）"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestNA")
        df = pd.DataFrame({
            'PO#': ['PO001', None, 'PO003'],
            'Line#': ['1', '2', None],
            'PO Supplier': ['S1', 'S2', 'S3'],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert ctx.data['PO Line'].iloc[1] == '-2'
        assert ctx.data['PO Line'].iloc[2] == 'PO003-'

    @pytest.mark.asyncio
    async def test_execute_returns_duration(self):
        """測試執行結果包含 duration"""
        step = ColumnInitializationStep(status_column="PO狀態", name="TestDuration")
        df = _create_po_df(2)
        ctx = ProcessingContext(
            data=df, entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.execute(ctx)

        assert result.duration >= 0
