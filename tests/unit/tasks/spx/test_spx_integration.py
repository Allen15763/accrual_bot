"""SPX 整合步驟單元測試

測試 spx_integration.py 中的所有步驟類別：
- ColumnAdditionStep: 欄位添加
- APInvoiceIntegrationStep: AP Invoice 整合
- ClosingListIntegrationStep: 關單清單整合
- ValidationDataProcessingStep: 驗收數據處理
- DataReformattingStep: 數據格式化
- PRDataReformattingStep: PR 數據格式化
- PPEDataCleaningStep: PPE 數據清理
- PPEDataMergeStep: PPE 數據合併
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, PropertyMock

from accrual_bot.core.pipeline.base import StepStatus, StepResult
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# 共用 Mock fixtures
# ============================================================

@pytest.fixture(autouse=True)
def mock_integration_deps():
    """Mock 整合步驟的所有外部依賴"""
    with patch('accrual_bot.tasks.spx.steps.spx_integration.config_manager') as mock_cm, \
         patch('accrual_bot.tasks.spx.steps.spx_integration.GoogleSheetsImporter') as mock_gsi, \
         patch('accrual_bot.tasks.spx.steps.spx_integration.classify_description', return_value='其他'), \
         patch('accrual_bot.tasks.spx.steps.spx_integration.give_account_by_keyword', side_effect=lambda df, *a, **kw: df), \
         patch('accrual_bot.tasks.spx.steps.spx_integration.clean_po_data', side_effect=lambda df: df):
        mock_cm._config_toml = {
            'fa_accounts': {'spx': ['199999']},
            'spx_column_defaults': {
                'region': 'TW',
                'default_department': '000',
                'prepay_liability': '111112',
                'sm_accounts': ['650003', '450014'],
                'validation_account_name': 'AP,FA Clear Account',
                'validation_liability': '200414',
            },
            'spx': {
                'deposit_keywords': '訂金|押金|保證金',
                'kiosk_suppliers': ['益欣'],
                'locker_suppliers': ['掌櫃'],
                'locker_columns': [f'col{i}' for i in range(34)],
                'locker_agg_columns': ['A', 'B'],
                'locker_priority_order': ['A', 'B', 'XA'],
                'locker_discount_pattern': r'\d+折',
                'output_columns_before_nlp': ['PO#', 'Item Description'],
                'output_columns_before_nlp_pr': ['PR#', 'Item Description'],
                'ppe_limit': 80000,
            },
        }
        mock_cm.get_list.return_value = ['199999']
        mock_cm.get.side_effect = lambda section, key, default=None: {
            ('SPX', 'closing_list_spreadsheet_id'): 'sheet_id_123',
            ('SPX', 'closing_list_sheet_range'): 'A:Z',
            ('SPX', 'locker_suppliers'): ['掌櫃'],
            ('SPX', 'kiosk_suppliers'): ['益欣'],
            ('SPX', 'locker_discount_pattern'): r'\d+折',
            ('SPX', 'ppe_limit'): '80000',
        }.get((section, key), default)
        mock_cm.get_list.side_effect = lambda section, key=None: {
            ('FA_ACCOUNTS', 'spx'): ['199999'],
            ('SPX', 'closing_list_sheet_names'): ['2025', '2026'],
        }.get((section, key), [])
        mock_cm.get_credentials_config.return_value = {
            'certificate_path': '/tmp/cert.json',
            'scopes': ['https://www.googleapis.com/auth/spreadsheets.readonly'],
        }
        yield mock_cm


def _create_po_df(n=5):
    """建立 PO 測試用 DataFrame"""
    return pd.DataFrame({
        'PO#': [f'SPXTW-PO{i:03d}' for i in range(n)],
        'PR#': [f'SPXTW-PR{i:03d}' for i in range(n)],
        'Line#': [str(i + 1) for i in range(n)],
        'GL#': [str(100000 + i) for i in range(n)],
        'Item Description': [f'Test Item {i}' for i in range(n)],
        'PO Supplier': [f'Supplier {i}' for i in range(n)],
        'Entry Quantity': ['100'] * n,
        'Received Quantity': ['100'] * n,
        'Billed Quantity': ['0'] * n,
        'Entry Amount': ['10000'] * n,
        'Entry Billed Amount': ['0'] * n,
        'Entry Prepay Amount': ['0'] * n,
        'Entry Invoiced Amount': ['0'] * n,
        'Unit Price': ['100.0'] * n,
        'Currency': ['TWD'] * n,
        'Product Code': [f'PROD{i:03d}' for i in range(n)],
        'Closed For Invoice': ['0'] * n,
        'Creation Date': ['2025-01-15'] * n,
        'Expected Received Month': ['Jan-25'] * n,
        'Last Update Date': ['2025-03-01'] * n,
        'PO Entry full invoiced status': ['0'] * n,
    })


def _create_context(df=None, entity='SPX', ptype='PO', pdate=202503):
    """建立測試用 ProcessingContext"""
    if df is None:
        df = _create_po_df()
    ctx = ProcessingContext(
        data=df,
        entity_type=entity,
        processing_date=pdate,
        processing_type=ptype,
    )
    ctx.set_variable('processing_date', pdate)
    return ctx


# ============================================================
# ColumnAdditionStep 測試
# ============================================================

class TestColumnAdditionStep:
    """測試欄位添加步驟"""

    @pytest.mark.unit
    def test_step_name_default(self):
        """預設步驟名稱應為 ColumnAddition"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        assert step.name == "ColumnAddition"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success_po(self):
        """PO 類型執行應成功並添加必要欄位"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        # 應有 PO 特有欄位
        assert '累計至本期驗收數量/金額' in ctx.data.columns
        assert '是否結案' in ctx.data.columns
        assert 'PO Line' in ctx.data.columns
        assert 'PO狀態' in ctx.data.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success_pr(self):
        """PR 類型執行應重命名 PO狀態 為 PR狀態"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context(ptype='PR')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        assert 'PR狀態' in ctx.data.columns
        assert 'PO狀態' not in ctx.data.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_adds_remark_columns(self):
        """應添加備註相關欄位"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context()
        await step.execute(ctx)
        for col in ['Remarked by Procurement', 'Remarked by FN', 'Remarked by 上月 FN']:
            assert col in ctx.data.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_adds_calculation_columns(self):
        """應添加計算相關欄位"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context()
        await step.execute(ctx)
        for col in ['是否估計入帳', '是否為FA', 'Account code', 'Liability']:
            assert col in ctx.data.columns

    @pytest.mark.unit
    def test_determine_fa_status_with_fa_account(self):
        """GL# 在 FA 帳號列表中應回傳 Y"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        df = pd.DataFrame({'GL#': ['199999', '100000', '199999']})
        result = step._determine_fa_status(df)
        assert result[0] == 'Y'
        assert result[1] == ''
        assert result[2] == 'Y'

    @pytest.mark.unit
    def test_determine_fa_status_no_gl_column(self):
        """沒有 GL# 欄位應回傳空字串"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        df = pd.DataFrame({'other': [1, 2]})
        result = step._determine_fa_status(df)
        assert (result == '').all()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_closed_for_invoice(self):
        """Closed For Invoice 非 0 應標為結案"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        df = _create_po_df(2)
        df.loc[0, 'Closed For Invoice'] = '1'
        ctx = _create_context(df)
        await step.execute(ctx)
        assert ctx.data.loc[0, '是否結案'] == '結案'
        assert ctx.data.loc[1, '是否結案'] == '未結案'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_check_with_entry_invoice(self):
        """Entry Billed Amount > 0 時應計算差額"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        df = _create_po_df(2)
        df.loc[0, 'Entry Billed Amount'] = '5000'
        ctx = _create_context(df)
        await step.execute(ctx)
        # 第 0 行: Entry Amount(10000) - Entry Billed Amount(5000) = 5000
        assert float(ctx.data.loc[0, 'Check with Entry Invoice']) == 5000.0
        assert ctx.data.loc[1, 'Check with Entry Invoice'] == '未入帳'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_valid(self):
        """有資料應驗證成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context()
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_metadata(self):
        """結果 metadata 應包含欄位統計"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context()
        result = await step.execute(ctx)
        assert 'columns_added' in result.metadata
        assert 'total_columns' in result.metadata
        assert result.metadata['columns_added'] > 0

    @pytest.mark.unit
    def test_determine_sm_status(self):
        """S&M 帳號檢測"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        df = pd.DataFrame({'GL#': ['650003', '100000']})
        result = step._determine_sm_status(df)
        assert result[0] == 'Y'
        assert result[1] == 'N'

    @pytest.mark.unit
    def test_determine_sm_status_no_gl(self):
        """沒有 GL# 欄位時應回傳 N"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        df = pd.DataFrame({'other': [1]})
        result = step._determine_sm_status(df)
        assert (result == 'N').all()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_sets_processing_month_variable(self):
        """execute 應設置 processing_month 變數"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context(pdate=202503)
        await step.execute(ctx)
        assert ctx.get_variable('processing_month') == 3

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_previous_month_january(self):
        """1 月的上月應為 12"""
        from accrual_bot.tasks.spx.steps.spx_integration import ColumnAdditionStep
        step = ColumnAdditionStep()
        ctx = _create_context(pdate=202501)
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS


# ============================================================
# APInvoiceIntegrationStep 測試
# ============================================================

class TestAPInvoiceIntegrationStep:
    """測試 AP Invoice 整合步驟"""

    @pytest.mark.unit
    def test_step_name(self):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        assert step.name == "APInvoiceIntegration"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_skips_when_no_ap_data(self):
        """無 AP Invoice 數據應跳過"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        df = _create_po_df(3)
        df['PO Line'] = df['PO#'] + '-' + df['Line#']
        ctx = _create_context(df)
        # 不添加 ap_invoice
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_skips_when_empty_ap_data(self):
        """AP Invoice 為空 DataFrame 時應跳過"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        df = _create_po_df(3)
        df['PO Line'] = df['PO#'] + '-' + df['Line#']
        ctx = _create_context(df)
        ctx.add_auxiliary_data('ap_invoice', pd.DataFrame())
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success_with_matching_ap(self):
        """有匹配的 AP Invoice 應成功整合 GL DATE"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        df = _create_po_df(2)
        df['PO Line'] = 'SPXTW-SPXTW-PO000-1'
        ctx = _create_context(df)

        ap_df = pd.DataFrame({
            'Company': ['SPXTW'],
            'PO Number': ['SPXTW-PO000'],
            'PO_LINE_NUMBER': ['1'],
            'Period': ['Jan-25'],
            'Match Type': ['PO_MATCH'],
        })
        ctx.add_auxiliary_data('ap_invoice', ap_df)
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_filters_future_periods(self):
        """應排除處理日期之後的 AP Invoice 記錄"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        df = _create_po_df(1)
        df['PO Line'] = 'SPXTW-SPXTW-PO000-1'
        ctx = _create_context(df, pdate=202501)

        ap_df = pd.DataFrame({
            'Company': ['SPXTW', 'SPXTW'],
            'PO Number': ['SPXTW-PO000', 'SPXTW-PO000'],
            'PO_LINE_NUMBER': ['1', '1'],
            'Period': ['Jan-25', 'Jun-25'],  # Jun-25 超過 202501
            'Match Type': ['PO_MATCH', 'PO_MATCH'],
        })
        ctx.add_auxiliary_data('ap_invoice', ap_df)
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_po_line(self):
        """缺少 PO Line 欄位應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        ctx = _create_context()  # 沒有 PO Line 欄位
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty_data(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_success(self):
        """有 PO Line 欄位的資料應驗證成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import APInvoiceIntegrationStep
        step = APInvoiceIntegrationStep()
        df = _create_po_df(1)
        df['PO Line'] = 'SPXTW-PO000-1'
        ctx = _create_context(df)
        result = await step.validate_input(ctx)
        assert result is True


# ============================================================
# ClosingListIntegrationStep 測試
# ============================================================

class TestClosingListIntegrationStep:
    """測試關單清單整合步驟"""

    @pytest.mark.unit
    def test_step_name(self):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        assert step.name == "ClosingListIntegration"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_with_empty_sheets(self):
        """Google Sheets 無數據時應仍成功（空 closing_list）"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        # Mock sheets_importer 回傳 None
        mock_importer = MagicMock()
        mock_importer.get_sheet_data.return_value = None
        step.sheets_importer = mock_importer

        ctx = _create_context()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        closing = ctx.get_auxiliary_data('closing_list')
        assert closing is not None

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_with_valid_sheets(self):
        """有數據的 Google Sheets 應成功整合"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()

        sheet_data = pd.DataFrame({
            'Date': ['2025/01/15'],
            'Type': ['Cancel'],
            'PO Number': ['PO001'],
            'Requester': ['User1'],
            'Supplier': ['Vendor1'],
            'Line Number / ALL': ['ALL'],
            'Reason': ['not needed'],
            'New PR Number': ['PR001'],
            'Remark': ['test'],
            'Done(V)': [pd.NA],
        })
        mock_importer = MagicMock()
        mock_importer.get_sheet_data.return_value = sheet_data
        step.sheets_importer = mock_importer

        ctx = _create_context()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS

    @pytest.mark.unit
    def test_clean_closing_data(self):
        """清理邏輯應重命名欄位並移除空 Date"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        df = pd.DataFrame({
            'Date': ['2025/01/15', '', pd.NA],
            'Type': ['Cancel', 'Cancel', 'Cancel'],
            'PO Number': ['PO001', 'PO002', 'PO003'],
            'Requester': ['U1', 'U2', 'U3'],
            'Supplier': ['V1', 'V2', 'V3'],
            'Line Number / ALL': ['ALL', 'ALL', 'ALL'],
            'Reason': ['r1', 'r2', 'r3'],
            'New PR Number': ['PR1', 'PR2', 'PR3'],
            'Remark': ['', '', ''],
            'Done(V)': [pd.NA, pd.NA, pd.NA],
        })
        result = step._clean_closing_data(df)
        assert 'po_no' in result.columns
        assert 'date' in result.columns
        # 空 Date 應被過濾
        assert len(result) == 1

    @pytest.mark.unit
    def test_clean_closing_data_error_returns_original(self):
        """清理失敗應回傳原始 DataFrame"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        df = pd.DataFrame({'col1': [1, 2]})  # 沒有 Date 欄位
        result = step._clean_closing_data(df)
        # 應回傳原始 DataFrame
        assert len(result) == 2

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_valid(self):
        """有資料應驗證成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        ctx = _create_context()
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_exception_returns_failed(self):
        """執行異常應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        # 製造異常：讓 _prepare_config 拋出錯誤
        step._prepare_config = MagicMock(side_effect=RuntimeError("test error"))
        ctx = _create_context()
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.unit
    def test_get_closing_note_no_data(self):
        """所有工作表都為空時應回傳空 DataFrame"""
        from accrual_bot.tasks.spx.steps.spx_integration import ClosingListIntegrationStep
        step = ClosingListIntegrationStep()
        mock_importer = MagicMock()
        mock_importer.get_sheet_data.return_value = pd.DataFrame()
        step.sheets_importer = mock_importer

        config = {'certificate_path': '/tmp/c.json', 'scopes': []}
        result = step._get_closing_note(config)
        assert result.empty


# ============================================================
# ValidationDataProcessingStep 測試
# ============================================================

class TestValidationDataProcessingStep:
    """測試驗收數據處理步驟"""

    @pytest.mark.unit
    def test_step_name(self):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        assert step.name == "ValidationDataProcessing"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_skips_no_path(self):
        """無驗收檔案路徑時應跳過"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        ctx = _create_context()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_skips_file_not_found(self):
        """驗收檔案不存在時應跳過"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep(validation_file_path='/nonexistent/file.xlsx')
        ctx = _create_context()
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED
        assert 'not found' in result.message

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_gets_path_from_context_variable(self):
        """應從 context variable 獲取路徑"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        ctx = _create_context()
        ctx.set_variable('validation_file_path', '/nonexistent/path.xlsx')
        result = await step.execute(ctx)
        # 路徑不存在 -> SKIPPED
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_gets_path_from_file_paths_string(self):
        """應從 file_paths dict（字串格式）獲取路徑"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        ctx = _create_context()
        ctx.set_variable('file_paths', {'ops_validation': '/nonexistent/ops.xlsx'})
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_gets_path_from_file_paths_dict(self):
        """應從 file_paths dict（字典格式含 path）獲取路徑"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        ctx = _create_context()
        ctx.set_variable('file_paths', {
            'ops_validation': {'path': '/nonexistent/ops.xlsx', 'params': {}}
        })
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_valid(self):
        """有資料應驗證成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        ctx = _create_context()
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.unit
    def test_apply_kiosk_validation_empty_data(self):
        """繳費機驗收數據為空時應不做變更"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        df = _create_po_df(2)
        df['本期驗收數量/金額'] = 0
        result = step._apply_kiosk_validation(df, {}, ['益欣'])
        assert (result['本期驗收數量/金額'] == 0).all()

    @pytest.mark.unit
    def test_apply_kiosk_validation_with_match(self):
        """繳費機驗收有匹配 PO# 時應更新"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        df = pd.DataFrame({
            'PO#': ['PO001', 'PO002'],
            'Item Description': ['門市繳費機設備', '一般品項'],
            'PO Supplier': ['益欣', '其他'],
            '本期驗收數量/金額': [0, 0],
        })
        kiosk_data = {'PO001': 5}
        result = step._apply_kiosk_validation(df, kiosk_data, ['益欣'])
        assert result.loc[0, '本期驗收數量/金額'] == 5
        assert result.loc[1, '本期驗收數量/金額'] == 0

    @pytest.mark.unit
    def test_apply_locker_validation_empty_data(self):
        """智取櫃驗收數據為空時應不做變更"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        df = _create_po_df(2)
        df['本期驗收數量/金額'] = 0
        result = step._apply_locker_validation(df, {}, ['掌櫃'])
        pd.testing.assert_frame_equal(result, df)

    @pytest.mark.unit
    def test_update_cumulative_qty_for_ppe(self):
        """更新累計驗收數量"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        df = pd.DataFrame({
            '本期驗收數量/金額': [5, 0],
            '累計至本期驗收數量/金額': ['10', '0'],
        })
        result = step._update_cumulative_qty_for_ppe(df)
        assert '累計至上期驗收數量/金額' in result.columns
        # 5 + 10 = 15
        assert result.loc[0, '累計至本期驗收數量/金額'] == '15.0'

    @pytest.mark.unit
    def test_modify_relevant_columns(self):
        """修改相關欄位應設置帳號等"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        df = pd.DataFrame({
            '本期驗收數量/金額': [5, 0],
            'Unit Price': ['100.0', '200.0'],
            'Product Code': ['P001', 'P002'],
            'Currency': ['TWD', 'TWD'],
            'Item Description': ['門市智取櫃 locker A', '一般品項'],
            '是否估計入帳': [pd.NA, pd.NA],
            'Account code': [pd.NA, pd.NA],
            'Account Name': [pd.NA, pd.NA],
            'Product code': [pd.NA, pd.NA],
            'Region_c': [pd.NA, pd.NA],
            'Dep.': [pd.NA, pd.NA],
            'Currency_c': [pd.NA, pd.NA],
            'Accr. Amount': [pd.NA, pd.NA],
            'Liability': [pd.NA, pd.NA],
        })
        result = step._modify_relevant_columns(df)
        assert result.loc[0, '是否估計入帳'] == 'Y'
        assert result.loc[0, 'Account code'] == '199999'

    @pytest.mark.unit
    def test_categorize_validation_data_no_discount_column(self):
        """沒有 discount 欄位應將所有數據歸為非折扣"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        df = pd.DataFrame({
            'PO單號': ['PO001', 'PO001'],
            'A': [10, 20],
            'B': [5, 5],
        })
        result = step._categorize_validation_data(df, ['A', 'B'])
        assert 'PO001' in result['non_discount']
        assert result['non_discount']['PO001']['A'] == 30

    @pytest.mark.unit
    def test_categorize_validation_data_with_discount(self):
        """有折扣數據應分類"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        df = pd.DataFrame({
            'PO單號': ['PO001', 'PO001'],
            'A': [10, 20],
            'B': [5, 5],
            'discount': ['', '8折驗收'],
        })
        result = step._categorize_validation_data(df, ['A', 'B'])
        assert 'PO001' in result['non_discount']
        assert 'PO001' in result['discount']

    @pytest.mark.unit
    def test_extract_discount_rate_string(self):
        """字串格式折扣率提取"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        assert step._extract_discount_rate('8折驗收') == 0.8
        assert step._extract_discount_rate('8.5折') == 0.85

    @pytest.mark.unit
    def test_extract_discount_rate_none(self):
        """None 應回傳 None"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        assert step._extract_discount_rate(None) is None

    @pytest.mark.unit
    def test_extract_discount_rate_array(self):
        """NumPy 陣列格式折扣率提取"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        arr = np.array(['8折驗收'])
        assert step._extract_discount_rate(arr) == 0.8

    @pytest.mark.unit
    def test_extract_discount_rate_multi_array(self):
        """多值陣列應只處理第一個元素"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        arr = np.array(['8折驗收', '9折出貨'])
        assert step._extract_discount_rate(arr) == 0.8

    @pytest.mark.unit
    def test_extract_discount_rate_empty_array(self):
        """空陣列應回傳 None"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        arr = np.array([])
        assert step._extract_discount_rate(arr) is None

    @pytest.mark.unit
    def test_extract_discount_rate_invalid_range(self):
        """異常折扣數值（超出 0-10）應回傳 None"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        assert step._extract_discount_rate('15折') is None

    @pytest.mark.unit
    def test_extract_discount_rate_no_match(self):
        """無法匹配格式應回傳 None"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        assert step._extract_discount_rate('正常驗收') is None

    @pytest.mark.unit
    def test_extract_discount_rate_invalid_type(self):
        """不支援的類型應拋出 TypeError"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        with pytest.raises(TypeError):
            step._extract_discount_rate(12345)

    @pytest.mark.unit
    def test_extract_discount_rate_empty_string(self):
        """空字串應回傳 None"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        assert step._extract_discount_rate('') is None

    @pytest.mark.unit
    def test_extract_discount_rate_nan_in_array(self):
        """陣列中 NaN 應回傳 None"""
        from accrual_bot.tasks.spx.steps.spx_integration import ValidationDataProcessingStep
        step = ValidationDataProcessingStep()
        arr = np.array([np.nan])
        assert step._extract_discount_rate(arr) is None


# ============================================================
# DataReformattingStep 測試
# ============================================================

class TestDataReformattingStep:
    """測試數據格式化步驟"""

    @pytest.mark.unit
    def test_step_name(self):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        assert step.name == "DataReformatting"

    @pytest.mark.unit
    def test_format_numeric_columns(self):
        """數值列格式化"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({
            'Line#': ['1', '2', 'abc'],
            'GL#': ['100000', '200000', 'xyz'],
            'Unit Price': ['100.456', '200.789', 'bad'],
        })
        result = step._format_numeric_columns(df)
        assert result['Line#'].dtype == 'Int64'
        assert result['GL#'].iloc[0] == 100000

    @pytest.mark.unit
    def test_reformat_dates(self):
        """日期列格式化"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({
            'Creation Date': ['2025-01-15', 'invalid_date'],
            'Expected Received Month': ['Jan-25', pd.NA],
        })
        result = step._reformat_dates(df)
        assert result['Creation Date'].iloc[0] == '2025-01-15'

    @pytest.mark.unit
    def test_clean_nan_values(self):
        """nan 值清理"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({
            '是否估計入帳': ['nan', 'Y', '<NA>'],
            'PO狀態': ['已完成', 'nan', pd.NA],
            'Accr. Amount': ['1000', 'nan', '<NA>'],
        })
        result = step._clean_nan_values(df)
        assert pd.isna(result['是否估計入帳'].iloc[0])
        assert pd.isna(result['PO狀態'].iloc[1])

    @pytest.mark.unit
    def test_remove_temp_columns(self):
        """應移除臨時欄位"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({
            'PO#': ['PO001'],
            '檔案日期': [202503],
            'Expected Received Month_轉換格式': [202503],
            'YMs of Item Description': ['202503'],
        })
        result = step._remove_temp_columns(df)
        assert '檔案日期' not in result.columns
        assert 'Expected Received Month_轉換格式' not in result.columns
        assert 'PO#' in result.columns

    @pytest.mark.unit
    def test_add_classification(self):
        """應添加分類欄位"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({'Item Description': ['test item']})
        result = step._add_classification(df)
        assert 'category_from_desc' in result.columns

    @pytest.mark.unit
    def test_add_installment_flag(self):
        """應正確添加分期標記"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({
            'Item Description': [
                '裝修工程第一期款項',
                '裝修一般工程',
                '一般品項',
            ]
        })
        result = step._add_installment_flag(df)
        assert '裝修一般/分期' in result.columns
        assert result.loc[0, '裝修一般/分期'] == '分期'
        assert result.loc[1, '裝修一般/分期'] == '一般'

    @pytest.mark.unit
    def test_add_installment_flag_no_item_desc(self):
        """沒有 Item Description 欄位應回傳原 DataFrame"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({'other': [1]})
        result = step._add_installment_flag(df)
        assert '裝修一般/分期' not in result.columns

    @pytest.mark.unit
    def test_rearrange_columns_po_status(self):
        """PO狀態 應排在 是否估計入帳 前面"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({
            'col1': [1],
            '是否估計入帳': ['Y'],
            'PO狀態': ['已完成'],
            'col2': [2],
        })
        result = step._rearrange_columns(df)
        cols = list(result.columns)
        assert cols.index('PO狀態') < cols.index('是否估計入帳')

    @pytest.mark.unit
    def test_rearrange_question_check_at_end(self):
        """Question from Reviewer 和 Check by AP 應在最後"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({
            'Question from Reviewer': ['Q1'],
            'col1': [1],
            'Check by AP': ['OK'],
            'col2': [2],
        })
        result = step._rearrange_columns(df)
        cols = list(result.columns)
        assert cols[-1] == 'Check by AP'
        assert cols[-2] == 'Question from Reviewer'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_valid(self):
        """有資料應驗證成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        ctx = _create_context()
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.unit
    def test_reformat_erm(self):
        """ERM 日期應被格式化為 YYYY/MM"""
        from accrual_bot.tasks.spx.steps.spx_integration import DataReformattingStep
        step = DataReformattingStep()
        df = pd.DataFrame({'expected_receive_month': ['Jan-25', 'Feb-26']})
        result = step._reformat_erm(df)
        assert result['expected_receive_month'].iloc[0] == '2025/01'


# ============================================================
# PRDataReformattingStep 測試
# ============================================================

class TestPRDataReformattingStep:
    """測試 PR 數據格式化步驟"""

    @pytest.mark.unit
    def test_step_name(self):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_integration import PRDataReformattingStep
        step = PRDataReformattingStep()
        assert step.name == "DataReformatting"

    @pytest.mark.unit
    def test_inherits_data_reformatting(self):
        """應繼承 DataReformattingStep"""
        from accrual_bot.tasks.spx.steps.spx_integration import PRDataReformattingStep, DataReformattingStep
        step = PRDataReformattingStep()
        assert isinstance(step, DataReformattingStep)

    @pytest.mark.unit
    def test_clean_nan_values_pr(self):
        """PR 版清理應處理 PR狀態"""
        from accrual_bot.tasks.spx.steps.spx_integration import PRDataReformattingStep
        step = PRDataReformattingStep()
        df = pd.DataFrame({
            '是否估計入帳': ['nan'],
            'PR狀態': ['nan'],
            'Accr. Amount': ['nan'],
        })
        result = step._clean_nan_values(df)
        assert pd.isna(result['PR狀態'].iloc[0])

    @pytest.mark.unit
    def test_rearrange_columns_pr_status(self):
        """PR狀態 應排在 是否估計入帳 前面"""
        from accrual_bot.tasks.spx.steps.spx_integration import PRDataReformattingStep
        step = PRDataReformattingStep()
        df = pd.DataFrame({
            'col1': [1],
            '是否估計入帳': ['Y'],
            'PR狀態': ['已完成'],
            'col2': [2],
        })
        result = step._rearrange_columns(df)
        cols = list(result.columns)
        assert cols.index('PR狀態') < cols.index('是否估計入帳')

    @pytest.mark.unit
    def test_remove_temp_columns_pr(self):
        """PR 版應移除 PR 相關臨時欄位"""
        from accrual_bot.tasks.spx.steps.spx_integration import PRDataReformattingStep
        step = PRDataReformattingStep()
        df = pd.DataFrame({
            'PR#': ['PR001'],
            'remarked_by_procurement_pr': ['test'],
            'noted_by_procurement_pr': ['test'],
            'remarked_by_上月_fn_pr': ['test'],
        })
        result = step._remove_temp_columns(df)
        assert 'remarked_by_procurement_pr' not in result.columns
        assert 'PR#' in result.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import PRDataReformattingStep
        step = PRDataReformattingStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False


# ============================================================
# PPEDataCleaningStep 測試
# ============================================================

class TestPPEDataCleaningStep:
    """測試 PPE 數據清理步驟"""

    @pytest.mark.unit
    def test_step_name(self):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        assert step.name == "PPEDataCleaning"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success(self):
        """正常執行應成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()

        filing_df = pd.DataFrame({
            0: [1001, 1002],
            1: ['台北市信義區XX路', '台北市中正區YY路'],
            2: ['2024-01-01 - 2025-01-01', '2024-06-01 - 2025-06-01'],
        })
        renewal_df = pd.DataFrame({
            '店號': [1001],
            '詳細地址': ['台北市信義區XX路'],
            '第一期合約起始日': ['2024-01-01'],
            '第二期合約截止日': ['2026-01-01'],
        })

        ctx = _create_context(filing_df, ptype='PPE')
        ctx.add_auxiliary_data('filing_list', filing_df)
        ctx.add_auxiliary_data('renewal_list', renewal_df)

        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        assert ctx.get_auxiliary_data('filing_list_clean') is not None
        assert ctx.get_auxiliary_data('renewal_list_clean') is not None

    @pytest.mark.unit
    def test_clean_dataframe(self):
        """應移除含 NaN 的行"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        df = pd.DataFrame({'a': [1, np.nan, 3], 'b': [4, 5, np.nan]})
        result = step._clean_dataframe(df)
        assert len(result) == 1

    @pytest.mark.unit
    def test_parse_contract_period_dash(self):
        """解析 - 分隔的合約期間"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        start, end = step._parse_contract_period('2024-01-01 - 2025-01-01')
        assert start == '2024-01-01'
        assert end == '2025-01-01'

    @pytest.mark.unit
    def test_parse_contract_period_tilde(self):
        """解析 ~ 分隔的合約期間"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        start, end = step._parse_contract_period('2024/01/01~2025/01/01')
        assert start is not None
        assert end is not None

    @pytest.mark.unit
    def test_parse_contract_period_na(self):
        """NaN 應回傳 (None, None)"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        start, end = step._parse_contract_period(pd.NA)
        assert start is None
        assert end is None

    @pytest.mark.unit
    def test_parse_contract_period_no_separator(self):
        """無分隔符應回傳 (None, None)"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        start, end = step._parse_contract_period('20240101')
        assert start is None
        assert end is None

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_failure(self):
        """異常應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        ctx = _create_context(ptype='PPE')
        # 沒有 filing_list / renewal_list -> 會拋異常
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.unit
    def test_standardize_renewal_data_missing_column(self):
        """缺少欄位時應跳過"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataCleaningStep
        step = PPEDataCleaningStep()
        df = pd.DataFrame({
            '店號': [1001],
            '詳細地址': ['台北市信義區'],
        })
        result = step._standardize_renewal_data(df)
        assert 'sp_code' in result.columns
        assert 'address' in result.columns


# ============================================================
# PPEDataMergeStep 測試
# ============================================================

class TestPPEDataMergeStep:
    """測試 PPE 數據合併步驟"""

    @pytest.mark.unit
    def test_step_name(self):
        """預設步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataMergeStep
        step = PPEDataMergeStep()
        assert step.name == "PPEDataMerge"

    @pytest.mark.unit
    def test_default_merge_keys(self):
        """預設合併鍵為 address"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataMergeStep
        step = PPEDataMergeStep()
        assert step.merge_keys == ['address']

    @pytest.mark.unit
    def test_custom_merge_keys(self):
        """自訂合併鍵"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataMergeStep
        step = PPEDataMergeStep(merge_keys=['sp_code'])
        assert step.merge_keys == ['sp_code']

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success(self):
        """正常合併應成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataMergeStep
        step = PPEDataMergeStep()

        filing_df = pd.DataFrame({
            'sp_code': [1001, 1002],
            'address': ['addr1', 'addr2'],
            'contract_start_day': ['2024-01-01', '2024-06-01'],
            'contract_end_day': ['2025-01-01', '2025-06-01'],
        })
        renewal_df = pd.DataFrame({
            'sp_code': [1001],
            'address': ['addr1'],
            'contract_start_day': ['2025-01-01'],
            'contract_end_day': ['2026-01-01'],
        })

        ctx = _create_context(filing_df, ptype='PPE')
        ctx.add_auxiliary_data('filing_list_clean', filing_df)
        ctx.add_auxiliary_data('renewal_list_clean', renewal_df)

        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        assert len(ctx.data) >= 2  # outer merge

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_failure(self):
        """缺少數據應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataMergeStep
        step = PPEDataMergeStep()
        ctx = _create_context(ptype='PPE')
        # 沒有 filing_list_clean / renewal_list_clean
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty(self):
        """空資料應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataMergeStep
        step = PPEDataMergeStep()
        ctx = _create_context(pd.DataFrame())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_valid(self):
        """有資料應驗證成功"""
        from accrual_bot.tasks.spx.steps.spx_integration import PPEDataMergeStep
        step = PPEDataMergeStep()
        ctx = _create_context()
        result = await step.validate_input(ctx)
        assert result is True
