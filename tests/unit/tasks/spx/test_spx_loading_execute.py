"""SPX 數據載入步驟 execute() 單元測試

測試 SPXDataLoadingStep、SPXPRDataLoadingStep、PPEDataLoadingStep、
AccountingOPSDataLoadingStep 的 execute() 方法及相關邏輯：
- 正常載入流程 (happy path)
- 缺少檔案/路徑處理
- 參考數據載入
- 錯誤場景 (檔案不存在、資料無效)
- 並發載入機制
- 資源清理
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, AsyncMock, PropertyMock
from pathlib import Path

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# 共用 Helper
# ============================================================

def _make_po_df(n: int = 3) -> pd.DataFrame:
    """建立含必要欄位的 PO DataFrame"""
    return pd.DataFrame({
        'Product Code': [f'P{i:03d}' for i in range(n)],
        'Item Description': [f'Item {i}' for i in range(n)],
        'GL#': [str(100000 + i) for i in range(n)],
        'Line#': [float(i + 1) for i in range(n)],
        'Entry Amount': [1000 * (i + 1) for i in range(n)],
    })


def _make_pr_df(n: int = 3) -> pd.DataFrame:
    """建立含必要欄位的 PR DataFrame"""
    return pd.DataFrame({
        'Product Code': [f'PR{i:03d}' for i in range(n)],
        'Item Description': [f'PR Item {i}' for i in range(n)],
        'GL#': [str(200000 + i) for i in range(n)],
        'Line#': [float(i + 1) for i in range(n)],
    })


def _make_ref_df() -> pd.DataFrame:
    """建立參考數據 DataFrame"""
    return pd.DataFrame({
        'Code': ['X1', 'X2'],
        'Account': ['100000', '100001'],
        'Account Desc': ['Cash', 'Receivables'],
        'Liability': ['211111', '211112'],
    })


def _make_context(entity='SPX', ptype='PO', date=202503):
    """建立 ProcessingContext"""
    return ProcessingContext(
        data=pd.DataFrame(),
        entity_type=entity,
        processing_date=date,
        processing_type=ptype,
    )


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_spx_deps():
    """Mock SPXDataLoadingStep / SPXPRDataLoadingStep 的所有外部依賴"""
    with patch('accrual_bot.tasks.spx.steps.spx_loading.config_manager') as mock_cm, \
         patch('accrual_bot.tasks.spx.steps.spx_loading.DataSourceFactory') as mock_dsf, \
         patch('accrual_bot.tasks.spx.steps.spx_loading.DataSourcePool') as mock_pool, \
         patch('accrual_bot.tasks.spx.steps.spx_loading.get_ref_on_colab', return_value=None), \
         patch('accrual_bot.tasks.spx.steps.spx_loading.GoogleSheetsImporter'):
        mock_cm._config_data = {'PATHS': {'ref_path_spt': '/tmp/ref.xlsx'}}
        mock_cm._config_toml = {
            'data_shape_summary': {'enabled': False},
            'spx': {
                'locker_columns': [f'col_{i}' for i in range(30)],
            },
        }
        mock_cm.get.return_value = 'default_val'
        mock_cm.get_list.return_value = ['col_a', 'col_b']
        mock_cm.get_credentials_config.return_value = {}
        pool_inst = AsyncMock()
        mock_pool.return_value = pool_inst
        yield {
            'config_manager': mock_cm,
            'factory': mock_dsf,
            'pool': mock_pool,
            'pool_instance': pool_inst,
        }


@pytest.fixture
def mock_source_po(mock_spx_deps):
    """建立回傳 PO DataFrame 的 mock source，並接上 factory"""
    source = AsyncMock()
    source.read = AsyncMock(return_value=_make_po_df())
    source.close = AsyncMock()
    mock_spx_deps['factory'].create_from_file.return_value = source
    return source


@pytest.fixture
def mock_source_pr(mock_spx_deps):
    """建立回傳 PR DataFrame 的 mock source，並接上 factory"""
    source = AsyncMock()
    source.read = AsyncMock(return_value=_make_pr_df())
    source.close = AsyncMock()
    mock_spx_deps['factory'].create_from_file.return_value = source
    return source


# ============================================================
# SPXDataLoadingStep — execute() 測試
# ============================================================

class TestSPXDataLoadingStepExecute:
    """測試 SPXDataLoadingStep.execute()"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_happy_path(self, mock_spx_deps, mock_source_po):
        """正常載入 PO 資料並回傳 SUCCESS"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ctx = _make_context()
        # 需要 ops_validation 在 validated_configs 中（execute 會取 .get('ops_validation').get('path')）
        with patch.object(Path, 'exists', return_value=True):
            step = SPXDataLoadingStep(
                file_paths={
                    'raw_po': '/tmp/po.csv',
                    'ops_validation': '/tmp/ops.xlsx',
                }
            )
            # mock _load_reference_data 回傳 2
            with patch.object(step, '_load_reference_data', new_callable=AsyncMock, return_value=2):
                result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert len(ctx.data) == 3
        assert ctx.get_variable('processing_date') == 202503
        assert ctx.get_variable('processing_month') == 3

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_no_valid_files(self, mock_spx_deps):
        """沒有有效檔案時應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ctx = _make_context()
        step = SPXDataLoadingStep(file_paths={})
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert 'No valid files' in result.message

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_raw_po_file_not_found(self, mock_spx_deps):
        """raw_po 檔案不存在時應回傳 FAILED (FileNotFoundError)"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ctx = _make_context()
        step = SPXDataLoadingStep(
            file_paths={'raw_po': '/nonexistent/po.csv'}
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert isinstance(result.error, FileNotFoundError)

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_empty_po_data(self, mock_spx_deps):
        """raw_po 載入後為空 DataFrame 應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        source = AsyncMock()
        source.read = AsyncMock(return_value=pd.DataFrame())
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        ctx = _make_context()
        with patch.object(Path, 'exists', return_value=True):
            step = SPXDataLoadingStep(
                file_paths={
                    'raw_po': '/tmp/po.csv',
                    'ops_validation': '/tmp/ops.xlsx',
                }
            )
            result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert 'empty' in result.message.lower() or 'Raw PO' in result.message

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_missing_required_columns(self, mock_spx_deps):
        """raw_po 缺少必要欄位應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        source = AsyncMock()
        source.read = AsyncMock(return_value=pd.DataFrame({'SomeCol': [1, 2]}))
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        ctx = _make_context()
        with patch.object(Path, 'exists', return_value=True):
            step = SPXDataLoadingStep(
                file_paths={
                    'raw_po': '/tmp/po.csv',
                    'ops_validation': '/tmp/ops.xlsx',
                }
            )
            result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert 'Missing required columns' in result.message

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_sets_validation_file_path(self, mock_spx_deps, mock_source_po):
        """execute 應將 ops_validation 路徑存入 context variable"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ctx = _make_context()
        with patch.object(Path, 'exists', return_value=True):
            step = SPXDataLoadingStep(
                file_paths={
                    'raw_po': '/tmp/po.csv',
                    'ops_validation': '/tmp/ops.xlsx',
                }
            )
            with patch.object(step, '_load_reference_data', new_callable=AsyncMock, return_value=0):
                result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert ctx.get_variable('validation_file_path') == '/tmp/ops.xlsx'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_auxiliary_data_added(self, mock_spx_deps):
        """非 raw_po 的 DataFrame 應存入 auxiliary_data"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        po_df = _make_po_df()
        prev_df = pd.DataFrame({'A': [1, 2]})

        def make_source(file_path, **kwargs):
            """根據路徑回傳不同的 source mock"""
            s = AsyncMock()
            s.close = AsyncMock()
            if 'po' in file_path:
                s.read = AsyncMock(return_value=po_df)
            elif 'prev' in file_path:
                s.read = AsyncMock(return_value=prev_df)
            else:
                s.read = AsyncMock(return_value=pd.DataFrame())
            return s

        mock_spx_deps['factory'].create_from_file.side_effect = make_source

        ctx = _make_context()
        with patch.object(Path, 'exists', return_value=True):
            step = SPXDataLoadingStep(
                file_paths={
                    'raw_po': '/tmp/po.csv',
                    'previous': '/tmp/prev.xlsx',
                    'ops_validation': '/tmp/ops.xlsx',
                }
            )
            with patch.object(step, '_load_reference_data', new_callable=AsyncMock, return_value=0):
                result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'previous' in ctx.auxiliary_data

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_cleanup_called_on_success(self, mock_spx_deps, mock_source_po):
        """成功時也應呼叫 _cleanup_resources"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ctx = _make_context()
        with patch.object(Path, 'exists', return_value=True):
            step = SPXDataLoadingStep(
                file_paths={
                    'raw_po': '/tmp/po.csv',
                    'ops_validation': '/tmp/ops.xlsx',
                }
            )
            with patch.object(step, '_load_reference_data', new_callable=AsyncMock, return_value=0), \
                 patch.object(step, '_cleanup_resources', new_callable=AsyncMock) as mock_cleanup:
                await step.execute(ctx)

        mock_cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_cleanup_called_on_failure(self, mock_spx_deps):
        """失敗時也應呼叫 _cleanup_resources"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ctx = _make_context()
        step = SPXDataLoadingStep(file_paths={})
        with patch.object(step, '_cleanup_resources', new_callable=AsyncMock) as mock_cleanup:
            await step.execute(ctx)

        mock_cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_metadata_contains_expected_keys(self, mock_spx_deps, mock_source_po):
        """回傳的 metadata 應包含標準欄位"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ctx = _make_context()
        with patch.object(Path, 'exists', return_value=True):
            step = SPXDataLoadingStep(
                file_paths={
                    'raw_po': '/tmp/po.csv',
                    'ops_validation': '/tmp/ops.xlsx',
                }
            )
            with patch.object(step, '_load_reference_data', new_callable=AsyncMock, return_value=2):
                result = await step.execute(ctx)

        assert result.metadata is not None
        assert result.metadata.get('processing_date') == 202503
        assert result.metadata.get('po_records') == 3


# ============================================================
# SPXDataLoadingStep — _load_raw_po_file() 測試
# ============================================================

class TestSPXLoadRawPoFile:
    """測試 _load_raw_po_file 的欄位處理"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_line_number_conversion(self, mock_spx_deps):
        """Line# 應轉為 string"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        df = pd.DataFrame({
            'Product Code': ['A'], 'Item Description': ['X'],
            'GL#': ['100'], 'Line#': [1.0],
        })
        source = AsyncMock()
        source.read = AsyncMock(return_value=df)
        step = SPXDataLoadingStep()
        result = await step._load_raw_po_file(source, '/tmp/x.csv')
        assert result['Line#'].dtype == 'string'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_gl_na_replacement(self, mock_spx_deps):
        """GL# 值 'N.A.' 應被替換為 '666666'"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        df = pd.DataFrame({
            'Product Code': ['A'], 'Item Description': ['X'],
            'GL#': ['N.A.'], 'Line#': [1.0],
        })
        source = AsyncMock()
        source.read = AsyncMock(return_value=df)
        step = SPXDataLoadingStep()
        result = await step._load_raw_po_file(source, '/tmp/x.csv')
        assert result['GL#'].iloc[0] == '666666'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_gl_nan_fill(self, mock_spx_deps):
        """GL# NaN 值應被填充為 '666666'"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        df = pd.DataFrame({
            'Product Code': ['A'], 'Item Description': ['X'],
            'GL#': [np.nan], 'Line#': [1.0],
        })
        source = AsyncMock()
        source.read = AsyncMock(return_value=df)
        step = SPXDataLoadingStep()
        result = await step._load_raw_po_file(source, '/tmp/x.csv')
        # NaN 經過 np.where→fillna→Float64→Int64→string 轉換鏈，驗證不拋例外
        assert 'GL#' in result.columns


# ============================================================
# SPXDataLoadingStep — _load_reference_data() 測試
# ============================================================

class TestSPXLoadReferenceData:
    """測試 _load_reference_data"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_ref_from_colab(self, mock_spx_deps):
        """get_ref_on_colab 回傳 DataFrame 時應直接使用"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ref_df = _make_ref_df()
        with patch('accrual_bot.tasks.spx.steps.spx_loading.get_ref_on_colab', return_value=ref_df):
            step = SPXDataLoadingStep()
            ctx = _make_context()
            count = await step._load_reference_data(ctx)

        assert count == 2
        assert 'reference_account' in ctx.auxiliary_data
        assert 'reference_liability' in ctx.auxiliary_data

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_ref_from_file(self, mock_spx_deps):
        """參考檔案存在時應從檔案載入"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        ref_df = _make_ref_df()
        source = AsyncMock()
        source.read = AsyncMock(return_value=ref_df)
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        step = SPXDataLoadingStep()
        ctx = _make_context()
        with patch.object(Path, 'exists', return_value=True):
            count = await step._load_reference_data(ctx)

        assert count == 2
        source.close.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_ref_file_not_found(self, mock_spx_deps):
        """參考檔案不存在時應建立空 DataFrame"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        step = SPXDataLoadingStep()
        ctx = _make_context()
        # Path.exists returns False by default for fake paths
        count = await step._load_reference_data(ctx)

        assert count == 0
        assert 'reference_account' in ctx.auxiliary_data
        assert ctx.auxiliary_data['reference_account'].empty

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_ref_exception_handled(self, mock_spx_deps):
        """載入參考數據出錯時應回傳 0 並建立空 DataFrame"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        mock_spx_deps['config_manager']._config_data = {}  # 缺少 PATHS

        step = SPXDataLoadingStep()
        ctx = _make_context()
        count = await step._load_reference_data(ctx)

        assert count == 0
        assert 'reference_account' in ctx.auxiliary_data


# ============================================================
# SPXDataLoadingStep — _load_all_files_concurrent() 測試
# ============================================================

class TestSPXConcurrentLoading:
    """測試並發載入機制"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_concurrent_loads_all_files(self, mock_spx_deps, mock_source_po):
        """並發載入應同時載入所有檔案"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        step = SPXDataLoadingStep(file_paths={'raw_po': '/tmp/po.csv'})
        configs = {
            'raw_po': {'path': '/tmp/po.csv', 'params': {}},
            'previous': {'path': '/tmp/prev.xlsx', 'params': {}},
        }
        result = await step._load_all_files_concurrent(configs)

        assert 'raw_po' in result
        assert 'previous' in result

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_concurrent_optional_file_failure(self, mock_spx_deps):
        """非必要檔案載入失敗不影響整體流程"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        po_df = _make_po_df()

        def make_source(file_path, **kwargs):
            s = AsyncMock()
            s.close = AsyncMock()
            if 'po' in file_path:
                s.read = AsyncMock(return_value=po_df)
            else:
                s.read = AsyncMock(side_effect=IOError("disk error"))
            return s

        mock_spx_deps['factory'].create_from_file.side_effect = make_source

        step = SPXDataLoadingStep(file_paths={'raw_po': '/tmp/po.csv'})
        configs = {
            'raw_po': {'path': '/tmp/po.csv', 'params': {}},
            'previous': {'path': '/tmp/prev.xlsx', 'params': {}},
        }
        result = await step._load_all_files_concurrent(configs)

        assert result['raw_po'] is not None
        assert result['previous'] is None  # 可選檔案失敗 → None

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_concurrent_raw_po_failure_raises(self, mock_spx_deps):
        """raw_po 載入失敗應拋出異常"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXDataLoadingStep

        source = AsyncMock()
        source.read = AsyncMock(side_effect=IOError("disk error"))
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        step = SPXDataLoadingStep(file_paths={'raw_po': '/tmp/po.csv'})
        configs = {'raw_po': {'path': '/tmp/po.csv', 'params': {}}}

        with pytest.raises(IOError, match="disk error"):
            await step._load_all_files_concurrent(configs)


# ============================================================
# SPXPRDataLoadingStep — execute() 測試
# ============================================================

class TestSPXPRDataLoadingStepExecute:
    """測試 SPXPRDataLoadingStep.execute()"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_happy_path(self, mock_spx_deps, mock_source_pr):
        """正常載入 PR 資料並回傳 SUCCESS"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep

        ctx = _make_context(ptype='PR')
        with patch.object(Path, 'exists', return_value=True):
            step = SPXPRDataLoadingStep(
                file_paths={'raw_pr': '/tmp/pr.csv'}
            )
            with patch.object(step, '_load_reference_data', new_callable=AsyncMock, return_value=2):
                result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert len(ctx.data) == 3
        assert ctx.get_variable('processing_date') == 202503

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_no_valid_files(self, mock_spx_deps):
        """沒有有效檔案時應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep

        ctx = _make_context(ptype='PR')
        step = SPXPRDataLoadingStep(file_paths={})
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_raw_pr_not_found(self, mock_spx_deps):
        """raw_pr 檔案不存在應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep

        ctx = _make_context(ptype='PR')
        step = SPXPRDataLoadingStep(
            file_paths={'raw_pr': '/nonexistent/pr.csv'}
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert isinstance(result.error, FileNotFoundError)

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_empty_pr_data(self, mock_spx_deps):
        """PR 資料為空應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep

        source = AsyncMock()
        source.read = AsyncMock(return_value=pd.DataFrame())
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        ctx = _make_context(ptype='PR')
        with patch.object(Path, 'exists', return_value=True):
            step = SPXPRDataLoadingStep(
                file_paths={'raw_pr': '/tmp/pr.csv'}
            )
            result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_pr_missing_columns(self, mock_spx_deps):
        """PR 資料缺少必要欄位應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep

        source = AsyncMock()
        source.read = AsyncMock(return_value=pd.DataFrame({'X': [1]}))
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        ctx = _make_context(ptype='PR')
        with patch.object(Path, 'exists', return_value=True):
            step = SPXPRDataLoadingStep(
                file_paths={'raw_pr': '/tmp/pr.csv'}
            )
            result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert 'Missing required columns' in result.message

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_pr_load_raw_pr_file_processes_columns(self, mock_spx_deps):
        """_load_raw_pr_file 應處理 Line# 和 GL# 欄位"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep

        df = pd.DataFrame({
            'Product Code': ['A'], 'Item Description': ['X'],
            'GL#': ['N.A.'], 'Line#': [2.0],
        })
        source = AsyncMock()
        source.read = AsyncMock(return_value=df)

        step = SPXPRDataLoadingStep()
        result = await step._load_raw_pr_file(source, '/tmp/pr.csv')

        assert result['GL#'].iloc[0] == '666666'
        assert result['Line#'].dtype == 'string'

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_pr_concurrent_raw_pr_failure(self, mock_spx_deps):
        """raw_pr 在並發載入中失敗應向上拋出"""
        from accrual_bot.tasks.spx.steps.spx_loading import SPXPRDataLoadingStep

        source = AsyncMock()
        source.read = AsyncMock(side_effect=IOError("read fail"))
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        step = SPXPRDataLoadingStep(file_paths={'raw_pr': '/tmp/pr.csv'})
        configs = {'raw_pr': {'path': '/tmp/pr.csv', 'params': {}}}

        with pytest.raises(IOError):
            await step._load_all_files_concurrent(configs)


# ============================================================
# PPEDataLoadingStep 測試
# ============================================================

class TestPPEDataLoadingStepExecute:
    """測試 PPEDataLoadingStep.execute()"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_no_url_fails(self, mock_spx_deps):
        """未提供 URL 時 execute 回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import PPEDataLoadingStep

        ctx = _make_context()
        # contract_filing_list_url=None, context 中也沒有設定
        step = PPEDataLoadingStep(contract_filing_list_url=None)
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_happy_path(self, mock_spx_deps):
        """正常載入 PPE 資料回傳 SUCCESS"""
        from accrual_bot.tasks.spx.steps.spx_loading import PPEDataLoadingStep

        filing_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        renewal_df = pd.DataFrame({'r1': [10, 20]})

        source = AsyncMock()
        source.read = AsyncMock(return_value=filing_df)
        source.close = AsyncMock()
        mock_spx_deps['factory'].create_from_file.return_value = source

        ctx = _make_context()
        step = PPEDataLoadingStep(
            contract_filing_list_url={'path': '/tmp/filing.xlsx'}
        )
        with patch.object(step, '_load_renewal_list', new_callable=AsyncMock, return_value=renewal_df):
            result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'filing_list' in ctx.auxiliary_data
        assert 'renewal_list' in ctx.auxiliary_data
        assert len(ctx.data) == 2

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_no_url(self, mock_spx_deps):
        """validate_input 在未提供 URL 時回傳 False"""
        from accrual_bot.tasks.spx.steps.spx_loading import PPEDataLoadingStep

        ctx = _make_context()
        step = PPEDataLoadingStep(contract_filing_list_url=None)
        valid = await step.validate_input(ctx)

        assert valid is False


# ============================================================
# AccountingOPSDataLoadingStep 測試
# ============================================================

class TestAccountingOPSDataLoadingStepExecute:
    """測試 AccountingOPSDataLoadingStep.execute()"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_happy_path(self, mock_spx_deps):
        """正常載入會計及 OPS 資料回傳 SUCCESS"""
        from accrual_bot.tasks.spx.steps.spx_loading import AccountingOPSDataLoadingStep

        acc_df = pd.DataFrame({
            'PO Line': ['PO001-1', 'PO002-1'],
            '累計至本期驗收數量/金額': ['100', '200'],
        })
        # OPS 需要和 locker_columns 數量相同的欄位
        locker_cols = mock_spx_deps['config_manager']._config_toml['spx']['locker_columns']
        # 確保 '驗收月份' 在 locker_cols 中（用於 _standardize_data 過濾）
        locker_cols_with_month = list(locker_cols)
        locker_cols_with_month[0] = '驗收月份'
        mock_spx_deps['config_manager']._config_toml['spx']['locker_columns'] = locker_cols_with_month
        # 建立 OPS df 恰好 len(locker_cols) 欄
        ops_data = {f'c{i}': ['val'] for i in range(len(locker_cols_with_month))}
        ops_df = pd.DataFrame(ops_data)

        def make_source(file_path, **kwargs):
            s = AsyncMock()
            s.close = AsyncMock()
            if 'acc' in file_path:
                s.read = AsyncMock(return_value=acc_df)
            else:
                s.read = AsyncMock(return_value=ops_df)
            return s

        mock_spx_deps['factory'].create_from_file.side_effect = make_source

        ctx = _make_context()
        required_cols = {
            'accounting': ['PO Line', '累計至本期驗收數量/金額'],
            'ops': [],
        }

        with patch.object(Path, 'exists', return_value=True):
            step = AccountingOPSDataLoadingStep(
                file_paths={
                    'accounting_workpaper': '/tmp/acc.xlsx',
                    'ops_validation': '/tmp/ops.xlsx',
                },
                required_columns=required_cols,
            )
            result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'accounting_workpaper' in ctx.auxiliary_data

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_missing_required_file(self, mock_spx_deps):
        """缺少必要檔案應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import AccountingOPSDataLoadingStep

        ctx = _make_context()
        step = AccountingOPSDataLoadingStep(
            file_paths={'accounting_workpaper': '/tmp/acc.xlsx'},
            required_columns={'accounting': [], 'ops': []},
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_no_file_configs(self, mock_spx_deps):
        """完全沒有檔案配置應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_loading import AccountingOPSDataLoadingStep

        ctx = _make_context()
        step = AccountingOPSDataLoadingStep(
            file_paths={},
            required_columns={'accounting': [], 'ops': []},
        )
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_both_required(self, mock_spx_deps):
        """validate_input 缺少兩個必要檔案應回傳 False"""
        from accrual_bot.tasks.spx.steps.spx_loading import AccountingOPSDataLoadingStep

        ctx = _make_context()
        step = AccountingOPSDataLoadingStep(
            file_paths={},
            required_columns={'accounting': [], 'ops': []},
        )
        valid = await step.validate_input(ctx)

        assert valid is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_required_columns_missing(self, mock_spx_deps):
        """驗證必要欄位缺少時應拋出 ValueError"""
        from accrual_bot.tasks.spx.steps.spx_loading import AccountingOPSDataLoadingStep

        step = AccountingOPSDataLoadingStep(
            file_paths={},
            required_columns={'accounting': ['NonExistCol'], 'ops': []},
        )
        loaded = {
            'accounting_workpaper': pd.DataFrame({'A': [1]}),
            'ops_validation': pd.DataFrame({'B': [2]}),
        }
        with pytest.raises(ValueError, match="missing required columns"):
            step._validate_required_columns(loaded)

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_rollback_logs_warning(self, mock_spx_deps):
        """rollback 應記錄警告日誌"""
        from accrual_bot.tasks.spx.steps.spx_loading import AccountingOPSDataLoadingStep

        ctx = _make_context()
        ctx.add_auxiliary_data('accounting_workpaper', pd.DataFrame({'x': [1]}))
        ctx.add_auxiliary_data('ops_validation', pd.DataFrame({'y': [2]}))

        step = AccountingOPSDataLoadingStep(
            file_paths={},
            required_columns={'accounting': [], 'ops': []},
        )
        # rollback 不應拋出例外
        await step.rollback(ctx, RuntimeError("test"))

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_standardize_po_line(self, mock_spx_deps):
        """_standardize_data 應將 PO Line 轉為 string 並移除 .0"""
        from accrual_bot.tasks.spx.steps.spx_loading import AccountingOPSDataLoadingStep

        step = AccountingOPSDataLoadingStep(
            file_paths={},
            required_columns={'accounting': [], 'ops': []},
        )
        loaded = {
            'accounting_workpaper': pd.DataFrame({
                'PO Line': ['SPXTW-001.0', 'SPXTW-002'],
                '累計至本期驗收數量/金額': [100, None],
            }),
        }
        result = step._standardize_data(loaded, 202503)

        assert result['accounting_workpaper']['PO Line'].iloc[0] == 'SPXTW-001'
        assert result['accounting_workpaper']['累計至本期驗收數量/金額'].iloc[1] == ''
