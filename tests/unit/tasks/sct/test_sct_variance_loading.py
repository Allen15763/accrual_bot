"""
SCT 差異分析 - 數據載入步驟單元測試
"""

import pytest
import pandas as pd
from unittest.mock import AsyncMock, patch, MagicMock

from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.tasks.sct.steps.sct_variance_loading import SCTVarianceDataLoadingStep


@pytest.fixture
def context():
    """建立測試用 ProcessingContext"""
    return ProcessingContext(
        data=pd.DataFrame(), entity_type='SCT',
        processing_date=202603, processing_type='VARIANCE'
    )


@pytest.fixture
def sample_current_df():
    """當期底稿樣本"""
    return pd.DataFrame({
        'PO#': ['PO001', 'PO002'],
        'Item Description': ['Item A', 'Item B'],
        '是否估計入帳': ['Y', 'N'],
        'accr._amount': ['1000', '2000'],
    })


@pytest.fixture
def sample_previous_df():
    """前期底稿樣本"""
    return pd.DataFrame({
        'PO#': ['PO001'],
        'Item Description': ['Item A'],
        '是否需要估計入帳': ['Y'],
        'Amount-未稅': ['900'],
    })


class TestSCTVarianceDataLoadingStep:
    """測試差異分析數據載入步驟"""

    def test_instantiation(self):
        """基本實例化"""
        step = SCTVarianceDataLoadingStep(
            file_paths={'current_worksheet': '/path/current.xlsx'}
        )
        assert step.name == "SCTVarianceDataLoading"
        assert 'current_worksheet' in step.file_paths

    @pytest.mark.asyncio
    async def test_load_both_files_success(
        self, context, sample_current_df, sample_previous_df
    ):
        """成功載入兩個檔案"""
        step = SCTVarianceDataLoadingStep(
            file_paths={
                'current_worksheet': '/path/current.xlsx',
                'previous_worksheet': '/path/previous.xlsx',
            }
        )

        mock_source_current = AsyncMock()
        mock_source_current.read.return_value = sample_current_df
        mock_source_previous = AsyncMock()
        mock_source_previous.read.return_value = sample_previous_df

        call_count = 0

        def mock_create(path, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_source_current
            return mock_source_previous

        with patch(
            'accrual_bot.tasks.sct.steps.sct_variance_loading.DataSourceFactory.create_from_file',
            side_effect=mock_create,
        ):
            result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        assert context.data is not None
        assert len(context.data) == 2
        assert context.get_auxiliary_data('previous_worksheet') is not None
        assert len(context.get_auxiliary_data('previous_worksheet')) == 1

    @pytest.mark.asyncio
    async def test_load_empty_current_fails(self, context):
        """當期底稿為空時失敗"""
        step = SCTVarianceDataLoadingStep(
            file_paths={
                'current_worksheet': '/path/current.xlsx',
                'previous_worksheet': '/path/previous.xlsx',
            }
        )

        mock_source = AsyncMock()
        mock_source.read.return_value = pd.DataFrame()

        with patch(
            'accrual_bot.tasks.sct.steps.sct_variance_loading.DataSourceFactory.create_from_file',
            return_value=mock_source,
        ):
            result = await step.execute(context)

        assert result.status == StepStatus.FAILED
        assert "當期底稿為空" in result.message

    @pytest.mark.asyncio
    async def test_missing_file_path_raises(self, context):
        """缺少必要檔案路徑"""
        step = SCTVarianceDataLoadingStep(
            file_paths={'current_worksheet': '/path/current.xlsx'}
            # 缺少 previous_worksheet
        )

        result = await step.execute(context)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    async def test_enriched_file_paths_format(
        self, context, sample_current_df, sample_previous_df
    ):
        """支援 _enrich_file_paths 產生的字典格式"""
        step = SCTVarianceDataLoadingStep(
            file_paths={
                'current_worksheet': {
                    'path': '/path/current.xlsx',
                    'params': {'sheet_name': 'PO', 'dtype': 'str'},
                },
                'previous_worksheet': {
                    'path': '/path/previous.xlsx',
                    'params': {'dtype': 'str'},
                },
            }
        )

        mock_source = AsyncMock()
        mock_source.read.side_effect = [sample_current_df, sample_previous_df]

        with patch(
            'accrual_bot.tasks.sct.steps.sct_variance_loading.DataSourceFactory.create_from_file',
            return_value=mock_source,
        ):
            result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_validate_input_missing_keys(self, context):
        """validate_input 檢查必要 key"""
        step = SCTVarianceDataLoadingStep(file_paths={})
        assert not await step.validate_input(context)

    @pytest.mark.asyncio
    async def test_validate_input_all_keys_present(self, context):
        """validate_input 通過"""
        step = SCTVarianceDataLoadingStep(
            file_paths={
                'current_worksheet': '/path/a.xlsx',
                'previous_worksheet': '/path/b.xlsx',
            }
        )
        assert await step.validate_input(context)

    @pytest.mark.asyncio
    async def test_metadata_contains_row_counts(
        self, context, sample_current_df, sample_previous_df
    ):
        """結果 metadata 包含行數"""
        step = SCTVarianceDataLoadingStep(
            file_paths={
                'current_worksheet': '/path/a.xlsx',
                'previous_worksheet': '/path/b.xlsx',
            }
        )

        call_count = 0

        def mock_create(path, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_src = AsyncMock()
            mock_src.read.return_value = (
                sample_current_df if call_count == 1 else sample_previous_df
            )
            return mock_src

        with patch(
            'accrual_bot.tasks.sct.steps.sct_variance_loading.DataSourceFactory.create_from_file',
            side_effect=mock_create,
        ):
            result = await step.execute(context)

        assert result.metadata['current_rows'] == 2
        assert result.metadata['previous_rows'] == 1
