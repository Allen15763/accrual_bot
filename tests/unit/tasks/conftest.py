"""tasks 測試共用 fixtures"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from accrual_bot.core.pipeline.context import ProcessingContext


@pytest.fixture
def spt_file_paths():
    """SPT 典型檔案路徑"""
    return {
        'po_file': '/tmp/spt/202512_purchase_order.xlsx',
        'pr_file': '/tmp/spt/202512_purchase_request.xlsx',
        'reference': '/tmp/spt/reference_account.xlsx',
        'liability': '/tmp/spt/reference_liability.xlsx',
        'previous': '/tmp/spt/202511_PO_FN.xlsx',
        'procurement_po': '/tmp/spt/procurement_po.xlsx',
        'commission': '/tmp/spt/commission.xlsx',
    }


@pytest.fixture
def spx_file_paths():
    """SPX 典型檔案路徑"""
    return {
        'po_file': '/tmp/spx/202512_purchase_order.csv',
        'pr_file': '/tmp/spx/202512_purchase_request.csv',
        'reference': '/tmp/spx/reference_account.xlsx',
        'liability': '/tmp/spx/reference_liability.xlsx',
        'previous': '/tmp/spx/202511_PO_FN.xlsx',
        'closing_list': '/tmp/spx/closing_list.xlsx',
        'procurement_po': '/tmp/spx/procurement_po.xlsx',
        'ap_invoice': '/tmp/spx/ap_invoice.xlsx',
        'ops_validation': '/tmp/spx/ops_validation.xlsx',
    }


@pytest.fixture
def mock_spt_orchestrator_config():
    """Mock SPT pipeline 配置（在 orchestrator 模組層級 patch）"""
    with patch('accrual_bot.tasks.spt.pipeline_orchestrator.config_manager') as mock:
        mock._config_toml = {
            'pipeline': {
                'spt': {
                    'enabled_po_steps': [
                        'SPTDataLoading',
                        'CommissionDataUpdate',
                        'PayrollDetection',
                        'SPTERMLogic',
                        'SPTStatusLabel',
                        'SPTAccountPrediction',
                    ],
                    'enabled_pr_steps': [
                        'SPTPRDataLoading',
                        'CommissionDataUpdate',
                        'PayrollDetection',
                        'SPTERMLogic',
                        'SPTStatusLabel',
                        'SPTAccountPrediction',
                    ],
                },
            },
            'fa_accounts': {
                'spt': ['199999'],
                'spx': ['199999'],
            },
        }
        mock.get_list.return_value = ['199999']
        yield mock


@pytest.fixture
def mock_spx_orchestrator_config():
    """Mock SPX pipeline 配置（在 orchestrator 模組層級 patch）"""
    with patch('accrual_bot.tasks.spx.pipeline_orchestrator.config_manager') as mock:
        mock._config_toml = {
            'pipeline': {
                'spx': {
                    'enabled_po_steps': [
                        'SPXDataLoading',
                        'ColumnAddition',
                        'ClosingListIntegration',
                        'StatusStage1',
                        'SPXERMLogic',
                        'DepositStatusUpdate',
                        'ValidationDataProcessing',
                        'SPXExport',
                    ],
                    'enabled_pr_steps': [
                        'SPXPRDataLoading',
                        'ColumnAddition',
                        'StatusStage1',
                        'SPXPRERMLogic',
                        'SPXPRExport',
                    ],
                    'enabled_ppe_steps': [
                        'PPEDataLoading',
                        'PPEDataCleaning',
                        'PPEDataMerge',
                        'PPEContractDateUpdate',
                        'PPEMonthDifference',
                    ],
                },
            },
            'fa_accounts': {
                'spt': ['199999'],
                'spx': ['199999'],
            },
            'spx_column_defaults': {
                'region': 'TW',
                'default_department': '000',
                'prepay_liability': '111112',
            },
            'spx_status_stage1_rules': {
                'conditions': [],
            },
            'spx_erm_status_rules': {
                'conditions': [],
            },
            'spx': {
                'deposit_keywords': '訂金|押金|保證金',
                'kiosk_suppliers': ['益欣資訊股份有限公司', '振樺電子股份有限公司'],
                'locker_suppliers': ['掌櫃智能股份有限公司', '台灣宅配通股份有限公司'],
            },
        }
        mock.get_list.return_value = ['199999']
        yield mock


def _create_spx_erm_df(n: int = 5) -> pd.DataFrame:
    """建立 SPX ERM 測試用 DataFrame（含所有必要欄位）"""
    return pd.DataFrame({
        'GL#': [str(100000 + i) for i in range(n)],
        'Expected Received Month_轉換格式': [202512] * n,
        'YMs of Item Description': ['202510,202512'] * n,
        'Entry Quantity': ['100'] * n,
        'Received Quantity': ['100'] * n,
        'Billed Quantity': ['0'] * n,
        'Entry Amount': ['10000'] * n,
        'Entry Billed Amount': ['0'] * n,
        'Entry Prepay Amount': ['0'] * n,
        'Item Description': [f'Test Item {i}' for i in range(n)],
        'Remarked by Procurement': [pd.NA] * n,
        'Remarked by 上月 FN': [pd.NA] * n,
        'Remarked by 上月 FN PR': [pd.NA] * n,
        'Unit Price': ['100.0'] * n,
        'Currency': ['TWD'] * n,
        'Product Code': [f'PROD{i:03d}' for i in range(n)],
        'PO狀態': [pd.NA] * n,
        'Account code': [str(100000 + i) for i in range(n)],
        'Department': [f'{i:03d}' for i in range(n)],
        'PO#': [f'SPTTW-PO{i:03d}' for i in range(n)],
        'PO Line': [f'SPTTW-PO{i:03d}-1' for i in range(n)],
        'PO Supplier': [f'Supplier {i}' for i in range(n)],
        'GL DATE': [pd.NA] * n,
        'match_type': ['ITEM_TO_RECEIPT'] * n,
        'matched_condition_on_status': [pd.NA] * n,
        'Liability': [pd.NA] * n,
        '是否有預付': ['N'] * n,
    })


@pytest.fixture
def spx_erm_df():
    """SPX ERM 測試 DataFrame"""
    return _create_spx_erm_df(5)


@pytest.fixture
def spx_erm_context(spx_erm_df):
    """SPX ERM 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=spx_erm_df,
        entity_type='SPX',
        processing_date=202512,
        processing_type='PO',
    )
    ctx.set_variable('processing_date', 202512)
    ctx.add_auxiliary_data('reference_account', pd.DataFrame({
        'Account': ['100000', '100001', '199999'],
        'Account Desc': ['Cash', 'Receivables', 'FA'],
    }))
    ctx.add_auxiliary_data('reference_liability', pd.DataFrame({
        'Account': ['100000', '100001'],
        'Liability': ['211111', '211112'],
    }))
    return ctx


def _create_spt_erm_df(n: int = 5) -> pd.DataFrame:
    """建立 SPT ERM 測試用 DataFrame（含所有必要欄位）"""
    return pd.DataFrame({
        'GL#': [str(300000 + i) for i in range(n)],
        'Expected Received Month_轉換格式': [202512] * n,
        'YMs of Item Description': ['202510,202512'] * n,
        'Entry Quantity': ['100'] * n,
        'Received Quantity': ['100'] * n,
        'Billed Quantity': ['0'] * n,
        'Entry Amount': ['10000'] * n,
        'Entry Billed Amount': ['0'] * n,
        'Entry Prepay Amount': ['0'] * n,
        'Item Description': [f'Test Item {i}' for i in range(n)],
        'Remarked by Procurement': [pd.NA] * n,
        'Remarked by 上月 FN': [pd.NA] * n,
        'Remarked by 上月 FN PR': [pd.NA] * n,
        'Unit Price': ['100.0'] * n,
        'Currency': ['TWD'] * n,
        'Product Code': [f'PROD{i:03d}' for i in range(n)],
        'PO狀態': [pd.NA] * n,
        'Account code': [str(300000 + i) for i in range(n)],
        'Department': [f'{i:03d}' for i in range(n)],
        'GL DATE': [pd.NA] * n,
        'match_type': ['ITEM_TO_RECEIPT'] * n,
        'Project': [f'PROD{i:03d} Some Project' for i in range(n)],
        'Product code': [pd.NA] * n,
        'Liability': [pd.NA] * n,
        '是否有預付': ['N'] * n,
    })


@pytest.fixture
def spt_erm_df():
    """SPT ERM 測試 DataFrame"""
    return _create_spt_erm_df(5)


@pytest.fixture
def spt_erm_context(spt_erm_df):
    """SPT ERM 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=spt_erm_df,
        entity_type='SPT',
        processing_date=202512,
        processing_type='PO',
    )
    ctx.set_variable('processing_date', 202512)
    ctx.add_auxiliary_data('reference_account', pd.DataFrame({
        'Account': ['300000', '300001'],
        'Account Desc': ['Cash', 'Receivables'],
    }))
    ctx.add_auxiliary_data('reference_liability', pd.DataFrame({
        'Account': ['300000', '300001'],
        'Liability': ['311111', '311112'],
    }))
    return ctx
