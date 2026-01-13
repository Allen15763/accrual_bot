"""Pytest fixtures 供所有測試使用"""
import pytest
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager


@pytest.fixture
def mock_config_manager():
    """Mock ConfigManager"""
    with patch('accrual_bot.utils.config.config_manager') as mock:
        mock._config_toml = {
            'pipeline': {
                'spt': {
                    'enabled_po_steps': [
                        'SPTDataLoading',
                        'CommissionDataUpdate',
                        'PayrollDetection',
                        'SPTERMLogic',
                        'SPTStatusLabel',
                        'SPTAccountPrediction'
                    ],
                    'enabled_pr_steps': [
                        'SPTPRDataLoading',
                        'CommissionDataUpdate',
                        'PayrollDetection',
                        'SPTERMLogic',
                        'SPTStatusLabel',
                        'SPTAccountPrediction'
                    ]
                },
                'spx': {
                    'enabled_po_steps': [
                        'SPXDataLoading',
                        'ColumnAddition',
                        'ClosingListIntegration',
                        'StatusStage1',
                        'SPXERMLogic',
                        'DepositStatusUpdate',
                        'ValidationDataProcessing',
                        'SPXExport'
                    ],
                    'enabled_pr_steps': [
                        'SPXPRDataLoading',
                        'ColumnAddition',
                        'StatusStage1',
                        'SPXPRERMLogic',
                        'SPXPRExport'
                    ]
                }
            },
            'fa_accounts': {
                'spt': ['199999'],
                'spx': ['199999']
            }
        }
        mock.get_list.return_value = ['199999']
        yield mock


@pytest.fixture
def sample_file_paths():
    """測試用的文件路徑配置"""
    return {
        'input': '/tmp/test_input.xlsx',
        'output': '/tmp/test_output.xlsx',
        'reference': '/tmp/test_reference.xlsx',
        'procurement': '/tmp/test_procurement.xlsx',
        'previous': '/tmp/test_previous.xlsx'
    }


@pytest.fixture
def processing_context():
    """測試用的 ProcessingContext"""
    df = pd.DataFrame({
        'GL#': ['100000', '100001', '100002'],
        'Item Description': ['Item A', 'Item B', 'Item C'],
        'Entry Quantity': [100, 200, 300],
        'Billed Quantity': [50, 150, 250],
        'Unit Price': [10.0, 20.0, 30.0],
        'Entry Amount': [1000, 4000, 9000]
    })

    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202512,
        processing_type='PO'
    )

    # 添加參考數據
    ctx.add_auxiliary_data('reference_account', pd.DataFrame({
        'Account': ['100000', '100001'],
        'Account Desc': ['Cash', 'Receivables']
    }))

    ctx.add_auxiliary_data('reference_liability', pd.DataFrame({
        'Account': ['100000', '100001'],
        'Liability': ['111111', '111112']
    }))

    return ctx


@pytest.fixture
def mock_data_source():
    """Mock DataSource"""
    mock = AsyncMock()
    mock.read = AsyncMock(return_value=pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    }))
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_data_source_factory(mock_data_source):
    """Mock DataSourceFactory"""
    with patch('accrual_bot.core.datasources.DataSourceFactory') as mock:
        mock.create_from_file.return_value = mock_data_source
        yield mock
