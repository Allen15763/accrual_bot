"""SPX 導出步驟單元測試

測試 SPXExportStep、SPXPRExportStep 和 AccountingOPSExportingStep：
- SPXExportStep: PO 數據多 sheet 導出
- SPXPRExportStep: PR 數據單 sheet 導出
- AccountingOPSExportingStep: 會計與 OPS 底稿比對結果輸出
"""
import pytest
import pandas as pd
import numpy as np
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.tasks.spx.steps.spx_exporting import (
    SPXExportStep,
    SPXPRExportStep,
    AccountingOPSExportingStep,
)


# ============================================================
# Helpers
# ============================================================

def _make_po_df(n: int = 5) -> pd.DataFrame:
    """建立 PO 測試用 DataFrame"""
    return pd.DataFrame({
        'PO#': [f'SPXTW-PO{i:03d}' for i in range(n)],
        'PO Line': [f'SPXTW-PO{i:03d}-1' for i in range(n)],
        'PO Supplier': [f'Supplier {i}' for i in range(n)],
        'Entry Amount': [10000 + i * 100 for i in range(n)],
        'PO狀態': ['已完成'] * n,
        'Account code': [str(100000 + i) for i in range(n)],
    })


def _make_pr_df(n: int = 5) -> pd.DataFrame:
    """建立 PR 測試用 DataFrame"""
    return pd.DataFrame({
        'PR#': [f'SPXTW-PR{i:03d}' for i in range(n)],
        'PR Line': [f'SPXTW-PR{i:03d}-1' for i in range(n)],
        'Entry Amount': [5000 + i * 50 for i in range(n)],
        'PR狀態': ['待處理'] * n,
    })


def _make_auxiliary_df(n: int = 3) -> pd.DataFrame:
    """建立輔助數據 DataFrame"""
    return pd.DataFrame({
        'col_a': [f'val_{i}' for i in range(n)],
        'col_b': [i * 10 for i in range(n)],
    })


def _make_po_context(df=None) -> ProcessingContext:
    """建立 PO ProcessingContext"""
    if df is None:
        df = _make_po_df()
    ctx = ProcessingContext(
        data=df,
        entity_type='SPX',
        processing_date=202503,
        processing_type='PO',
    )
    return ctx


def _make_pr_context(df=None) -> ProcessingContext:
    """建立 PR ProcessingContext"""
    if df is None:
        df = _make_pr_df()
    ctx = ProcessingContext(
        data=df,
        entity_type='SPX',
        processing_date=202503,
        processing_type='PR',
    )
    return ctx


# ============================================================
# SPXExportStep Tests
# ============================================================

class TestSPXExportStep:
    """SPXExportStep 單元測試"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_success(self, tmp_path):
        """正常導出 PO 數據到 Excel（含多 sheet）"""
        step = SPXExportStep(name="SPXExport", output_dir=str(tmp_path))
        ctx = _make_po_context()
        # 設定輔助數據（locker_non_discount, locker_discount, kiosk_data）
        ctx.add_auxiliary_data('locker_non_discount', _make_auxiliary_df(2))
        ctx.add_auxiliary_data('locker_discount', _make_auxiliary_df(3))
        ctx.add_auxiliary_data('kiosk_data', _make_auxiliary_df(4))

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['rows_exported'] == 5
        assert result.metadata['columns_exported'] == len(_make_po_df().columns)
        assert os.path.exists(result.metadata['output_path'])

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_creates_output_dir(self, tmp_path):
        """輸出目錄不存在時自動創建"""
        out_dir = str(tmp_path / "sub" / "dir")
        step = SPXExportStep(name="SPXExport", output_dir=out_dir)
        ctx = _make_po_context()
        ctx.add_auxiliary_data('locker_non_discount', _make_auxiliary_df())
        ctx.add_auxiliary_data('locker_discount', _make_auxiliary_df())
        ctx.add_auxiliary_data('kiosk_data', _make_auxiliary_df())

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert os.path.isdir(out_dir)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_missing_auxiliary_data(self, tmp_path):
        """缺少輔助數據時應回傳 FAILED"""
        step = SPXExportStep(name="SPXExport", output_dir=str(tmp_path))
        ctx = _make_po_context()
        # 不設定輔助數據

        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert 'error' in str(result.message).lower() or result.error is not None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_filename_contains_entity_and_date(self, tmp_path):
        """導出檔名包含實體類型和處理日期"""
        step = SPXExportStep(name="SPXExport", output_dir=str(tmp_path))
        ctx = _make_po_context()
        ctx.add_auxiliary_data('locker_non_discount', _make_auxiliary_df())
        ctx.add_auxiliary_data('locker_discount', _make_auxiliary_df())
        ctx.add_auxiliary_data('kiosk_data', _make_auxiliary_df())

        result = await step.execute(ctx)

        output_path = result.metadata['output_path']
        filename = os.path.basename(output_path)
        assert 'SPX' in filename
        assert 'PO' in filename
        assert '202503' in filename

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validate_input_with_data(self):
        """有數據時驗證通過"""
        step = SPXExportStep(name="SPXExport")
        ctx = _make_po_context()

        result = await step.validate_input(ctx)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validate_input_empty_data(self):
        """空數據時驗證失敗"""
        step = SPXExportStep(name="SPXExport")
        ctx = _make_po_context(pd.DataFrame())

        result = await step.validate_input(ctx)

        assert result is False


# ============================================================
# SPXPRExportStep Tests
# ============================================================

class TestSPXPRExportStep:
    """SPXPRExportStep 單元測試"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_success(self, tmp_path):
        """正常導出 PR 數據到 Excel"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context()

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert '4' in result.message or 'PR' in result.message
        # 確認檔案存在
        output_path = result.metadata.get('output_path')
        assert output_path is not None
        assert os.path.exists(output_path)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_creates_output_dir(self, tmp_path):
        """輸出目錄不存在時自動創建"""
        out_dir = tmp_path / "nested" / "output"
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(out_dir))
        ctx = _make_pr_context()

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert out_dir.exists()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_empty_data_fails(self, tmp_path):
        """空數據時導出失敗"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context(pd.DataFrame())

        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_filename_format(self, tmp_path):
        """檔名格式正確：{entity}_{type}_{date}_processed_{timestamp}.xlsx"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context()

        result = await step.execute(ctx)

        output_path = result.metadata['output_path']
        filename = os.path.basename(output_path)
        assert filename.startswith('SPX_PR_202503_processed_')
        assert filename.endswith('.xlsx')

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_sets_context_variable(self, tmp_path):
        """導出後在 context 設定輸出路徑變量"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context()

        await step.execute(ctx)

        assert ctx.get_variable('pr_export_output_path') is not None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_cleans_na_values(self, tmp_path):
        """導出時 <NA> 值被清理"""
        df = _make_pr_df()
        df.loc[0, 'PR狀態'] = '<NA>'
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context(df)

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        # 讀回確認 <NA> 被替換
        output_path = result.metadata['output_path']
        df_read = pd.read_excel(output_path, sheet_name='PR')
        assert '<NA>' not in df_read['PR狀態'].astype(str).values

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_custom_sheet_name(self, tmp_path):
        """自訂 sheet 名稱"""
        step = SPXPRExportStep(
            name="SPXPRExport",
            output_dir=str(tmp_path),
            sheet_name="CustomSheet",
        )
        ctx = _make_pr_context()

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['sheet_name'] == 'CustomSheet'
        # 確認 sheet 名稱正確
        output_path = result.metadata['output_path']
        xl = pd.ExcelFile(output_path)
        assert 'CustomSheet' in xl.sheet_names

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validate_input_with_data(self, tmp_path):
        """有數據時驗證通過"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context()

        result = await step.validate_input(ctx)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validate_input_empty_data(self, tmp_path):
        """空數據時驗證失敗"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context(pd.DataFrame())

        result = await step.validate_input(ctx)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validate_input_non_pr_type_still_passes(self, tmp_path):
        """處理類型非 PR 時仍通過驗證（僅發出警告）"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_po_context()  # PO 類型

        result = await step.validate_input(ctx)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_rollback_deletes_partial_file(self, tmp_path):
        """回滾時刪除已產生的部分檔案"""
        step = SPXPRExportStep(name="SPXPRExport", output_dir=str(tmp_path))
        ctx = _make_pr_context()

        # 先正常導出
        result = await step.execute(ctx)
        output_path = ctx.get_variable('pr_export_output_path')
        assert os.path.exists(output_path)

        # 回滾應刪除檔案
        await step.rollback(ctx, RuntimeError("test error"))
        assert not os.path.exists(output_path)


# ============================================================
# AccountingOPSExportingStep Tests
# ============================================================

class TestAccountingOPSExportingStep:
    """AccountingOPSExportingStep 單元測試"""

    def _make_context_with_aux_data(self) -> ProcessingContext:
        """建立帶有完整輔助數據的 context"""
        ctx = _make_po_context()
        ctx.add_auxiliary_data('accounting_workpaper', pd.DataFrame({
            'Account': ['100001', '100002'],
            'Amount': [50000, 60000],
        }))
        ctx.add_auxiliary_data('ops_validation', pd.DataFrame({
            'Vendor': ['V001', 'V002', 'V003'],
            'Status': ['OK', 'Pending', 'OK'],
        }))
        ctx.add_auxiliary_data('validation_comparison', pd.DataFrame({
            'Match': ['Y', 'N'],
            'Diff': [0, 500],
        }))
        return ctx

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_success(self, tmp_path):
        """正常導出會計 OPS 數據"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = self._make_context_with_aux_data()

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'output_path' in result.metadata
        assert result.metadata['accounting_rows'] == 2
        assert result.metadata['ops_rows'] == 3
        assert result.metadata['comparison_rows'] == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_writes_correct_sheets(self, tmp_path):
        """導出的 Excel 包含正確的 sheet 名稱"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = self._make_context_with_aux_data()

        result = await step.execute(ctx)

        output_path = result.metadata['output_path']
        xl = pd.ExcelFile(output_path)
        assert 'acc_raw' in xl.sheet_names
        assert 'ops_raw' in xl.sheet_names
        assert 'result' in xl.sheet_names

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_custom_sheet_names(self, tmp_path):
        """自訂 sheet 名稱對應"""
        custom_sheets = {
            'accounting_workpaper': '會計底稿',
            'ops_validation': 'OPS底稿',
            'validation_comparison': '比對結果',
        }
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
            sheet_names=custom_sheets,
        )
        ctx = self._make_context_with_aux_data()

        result = await step.execute(ctx)

        output_path = result.metadata['output_path']
        xl = pd.ExcelFile(output_path)
        assert '會計底稿' in xl.sheet_names
        assert 'OPS底稿' in xl.sheet_names
        assert '比對結果' in xl.sheet_names

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_missing_required_data(self, tmp_path):
        """缺少必要輔助數據時失敗"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = _make_po_context()
        # 不設定任何輔助數據

        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert 'Missing required data' in str(result.message) or result.error is not None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_execute_sets_context_variable(self, tmp_path):
        """導出後在 context 設定輸出路徑"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = self._make_context_with_aux_data()

        await step.execute(ctx)

        assert ctx.get_variable('export_output_path') is not None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validate_input_all_data_present(self, tmp_path):
        """所有必要數據都存在時驗證通過"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = self._make_context_with_aux_data()

        result = await step.validate_input(ctx)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validate_input_missing_data(self, tmp_path):
        """缺少必要數據時驗證失敗"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = _make_po_context()
        # 只提供部分數據
        ctx.add_auxiliary_data('accounting_workpaper', _make_auxiliary_df())

        result = await step.validate_input(ctx)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_rollback_deletes_file(self, tmp_path):
        """回滾時刪除部分輸出檔案"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = self._make_context_with_aux_data()

        # 先正常導出
        result = await step.execute(ctx)
        output_path = ctx.get_variable('export_output_path')
        assert os.path.exists(output_path)

        # 回滾
        await step.rollback(ctx, RuntimeError("test"))
        assert not os.path.exists(output_path)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_rollback_no_file(self, tmp_path):
        """沒有輸出檔案時回滾不報錯"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = _make_po_context()

        # 沒有導出過，回滾應正常完成不拋錯
        await step.rollback(ctx, RuntimeError("test"))

    @pytest.mark.unit
    def test_default_sheet_names(self):
        """預設 sheet 名稱正確"""
        step = AccountingOPSExportingStep()
        assert step.sheet_names == {
            'accounting_workpaper': 'acc_raw',
            'ops_validation': 'ops_raw',
            'validation_comparison': 'result',
        }

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_generate_output_path_format(self, tmp_path):
        """生成的檔案路徑格式正確"""
        step = AccountingOPSExportingStep(
            name="AccountingOPSExporting",
            output_dir=str(tmp_path),
        )
        ctx = _make_po_context()

        path = step._generate_output_path(ctx)

        assert path.name.startswith('SPX_PO_202503_')
        assert path.name.endswith('.xlsx')
        assert path.parent == tmp_path
