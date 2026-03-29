"""SPX 數據載入步驟單元測試

測試 SPXDataLoadingStep 和 SPXPRDataLoadingStep 的核心功能：
- 初始化與參數處理
- 檔案路徑標準化
- 驗證邏輯
- 並發載入流程
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path

from accrual_bot.core.pipeline.base import StepStatus, StepResult
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_logger():
    """Mock logger 避免真實 logger 初始化"""
    with patch('accrual_bot.tasks.spx.steps.spx_loading.get_logger', return_value=MagicMock()):
        yield


@pytest.fixture
def mock_spx_loading_deps():
    """Mock SPXDataLoadingStep 的所有外部依賴"""
    with patch('accrual_bot.tasks.spx.steps.spx_loading.config_manager') as mock_cm, \
         patch('accrual_bot.tasks.spx.steps.spx_loading.DataSourceFactory') as mock_dsf, \
         patch('accrual_bot.tasks.spx.steps.spx_loading.DataSourcePool') as mock_pool, \
         patch('accrual_bot.tasks.spx.steps.spx_loading.get_ref_on_colab', return_value=None):
        mock_cm._config_data = {'PATHS': {'ref_path_spt': '/tmp/ref.xlsx'}}
        mock_cm._config_toml = {'data_shape_summary': {'enabled': False}}
        mock_pool_instance = AsyncMock()
        mock_pool.return_value = mock_pool_instance
        yield {
            'config_manager': mock_cm,
            'data_source_factory': mock_dsf,
            'pool': mock_pool_instance,
        }


@pytest.fixture
def po_loading_step(mock_spx_loading_deps):
    """建立 SPXDataLoadingStep 實例"""
    from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
    return SPXDataLoadingStep(
        name="TestSPXDataLoading",
        file_paths={
            'raw_po': '/tmp/202512_purchase_order.csv',
            'previous': '/tmp/202511_PO_FN.xlsx',
        }
    )


@pytest.fixture
def pr_loading_step(mock_spx_loading_deps):
    """建立 SPXPRDataLoadingStep 實例"""
    from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep
    return SPXPRDataLoadingStep(
        name="TestSPXPRDataLoading",
        file_paths={
            'raw_pr': '/tmp/202512_purchase_request.csv',
        }
    )


@pytest.fixture
def empty_context():
    """空的 ProcessingContext"""
    return ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPX',
        processing_date=202512,
        processing_type='PO',
    )


# ============================================================
# SPXDataLoadingStep 測試
# ============================================================

class TestSPXDataLoadingStepInit:
    """測試 SPXDataLoadingStep 初始化"""

    def test_step_name_default(self, mock_spx_loading_deps):
        """預設步驟名稱應為 SPXDataLoading"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        step = SPXDataLoadingStep()
        assert step.name == "SPXDataLoading"

    def test_step_name_custom(self, mock_spx_loading_deps):
        """可自訂步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        step = SPXDataLoadingStep(name="CustomName")
        assert step.name == "CustomName"

    def test_normalize_file_paths_old_format(self, po_loading_step):
        """舊格式（純字串路徑）應能正確標準化"""
        configs = po_loading_step.file_configs
        assert 'raw_po' in configs
        assert configs['raw_po']['path'] == '/tmp/202512_purchase_order.csv'
        assert configs['raw_po']['params'] == {}

    def test_normalize_file_paths_new_format(self, mock_spx_loading_deps):
        """新格式（包含 params）應能正確標準化"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        step = SPXDataLoadingStep(
            file_paths={
                'raw_po': {
                    'path': '/tmp/po.csv',
                    'params': {'encoding': 'utf-8', 'sep': ','}
                }
            }
        )
        assert step.file_configs['raw_po']['params']['encoding'] == 'utf-8'

    def test_normalize_file_paths_missing_path_raises(self, mock_spx_loading_deps):
        """新格式缺少 path 應拋出 ValueError"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        with pytest.raises(ValueError, match="Missing 'path'"):
            SPXDataLoadingStep(file_paths={'raw_po': {'params': {}}})

    def test_normalize_file_paths_invalid_type_raises(self, mock_spx_loading_deps):
        """無效的配置類型應拋出 ValueError"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        with pytest.raises(ValueError, match="Invalid config type"):
            SPXDataLoadingStep(file_paths={'raw_po': 12345})


class TestSPXDataLoadingStepValidation:
    """測試 SPXDataLoadingStep 驗證邏輯"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_no_configs(self, mock_spx_loading_deps, empty_context):
        """沒有提供檔案配置時，驗證應失敗"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        step = SPXDataLoadingStep(file_paths={})
        result = await step.validate_input(empty_context)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_raw_po(self, mock_spx_loading_deps, empty_context):
        """缺少 raw_po 配置時，驗證應失敗"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        step = SPXDataLoadingStep(file_paths={'previous': '/tmp/prev.xlsx'})
        result = await step.validate_input(empty_context)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_raw_po_not_found(self, mock_spx_loading_deps, empty_context):
        """raw_po 檔案不存在時，驗證應失敗"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep
        step = SPXDataLoadingStep(
            file_paths={'raw_po': '/nonexistent/path.csv'}
        )
        result = await step.validate_input(empty_context)
        assert result is False


class TestSPXDataLoadingStepExtract:
    """測試 SPXDataLoadingStep 數據提取方法"""

    def test_extract_raw_po_data_valid(self, po_loading_step):
        """有效的 raw_po 資料應能正確提取"""
        test_df = pd.DataFrame({
            'Product Code': ['P001'],
            'Item Description': ['Test'],
            'GL#': ['100000'],
        })
        result = po_loading_step._extract_raw_po_data(test_df)
        assert len(result) == 1

    def test_extract_raw_po_data_empty_raises(self, po_loading_step):
        """空的 DataFrame 應拋出 ValueError"""
        with pytest.raises(ValueError, match="Raw PO data is empty"):
            po_loading_step._extract_raw_po_data(pd.DataFrame())

    def test_extract_raw_po_data_missing_columns_raises(self, po_loading_step):
        """缺少必要欄位應拋出 ValueError"""
        test_df = pd.DataFrame({'SomeCol': ['val']})
        with pytest.raises(ValueError, match="Missing required columns"):
            po_loading_step._extract_raw_po_data(test_df)


# ============================================================
# SPXPRDataLoadingStep 測試
# ============================================================

class TestSPXPRDataLoadingStepInit:
    """測試 SPXPRDataLoadingStep 初始化"""

    def test_step_name_default(self, mock_spx_loading_deps):
        """預設步驟名稱應為 SPXPRDataLoading"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep
        step = SPXPRDataLoadingStep()
        assert step.name == "SPXPRDataLoading"

    def test_description(self, pr_loading_step):
        """描述應包含 PR 關鍵字"""
        assert 'PR' in pr_loading_step.description

    def test_normalize_pr_paths(self, pr_loading_step):
        """PR 檔案路徑應正確標準化"""
        assert 'raw_pr' in pr_loading_step.file_configs
        assert pr_loading_step.file_configs['raw_pr']['path'] == '/tmp/202512_purchase_request.csv'
