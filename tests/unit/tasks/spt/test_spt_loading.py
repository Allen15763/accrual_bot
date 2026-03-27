"""
SPT 數據載入步驟測試

測試 SPTDataLoadingStep 和 SPTPRDataLoadingStep 的核心功能：
- 步驟初始化和基本屬性
- 檔案路徑標準化
- 輸入驗證
- 原始數據載入和日期提取
- 參考數據載入到 context
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, Mock, AsyncMock, MagicMock

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# SPTDataLoadingStep 測試
# ============================================================

@pytest.mark.unit
class TestSPTDataLoadingStepInit:
    """SPTDataLoadingStep 初始化與基本屬性測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_default_name(self, mock_cm):
        """測試預設步驟名稱為 SPTDataLoading"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep()
        assert step.name == "SPTDataLoading"

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_custom_name(self, mock_cm):
        """測試自訂步驟名稱"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep(name="CustomLoading")
        assert step.name == "CustomLoading"

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_normalize_string_path(self, mock_cm):
        """測試舊格式路徑字串自動轉換為新格式"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep(file_paths={'raw_po': '/tmp/test.xlsx'})
        assert step.file_configs['raw_po']['path'] == '/tmp/test.xlsx'
        assert step.file_configs['raw_po']['params'] == {}

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_normalize_dict_path(self, mock_cm):
        """測試新格式路徑字典正確解析"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        file_paths = {
            'raw_po': {
                'path': '/tmp/test.xlsx',
                'params': {'header': 1}
            }
        }
        step = SPTDataLoadingStep(file_paths=file_paths)
        assert step.file_configs['raw_po']['path'] == '/tmp/test.xlsx'
        assert step.file_configs['raw_po']['params'] == {'header': 1}

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_normalize_dict_missing_path_raises(self, mock_cm):
        """測試缺少 path 的字典格式應拋出 ValueError"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        with pytest.raises(ValueError, match="Missing 'path'"):
            SPTDataLoadingStep(file_paths={'raw_po': {'params': {}}})

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_normalize_invalid_type_raises(self, mock_cm):
        """測試無效類型的路徑配置應拋出 ValueError"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        with pytest.raises(ValueError, match="Invalid config type"):
            SPTDataLoadingStep(file_paths={'raw_po': 12345})


@pytest.mark.unit
class TestSPTDataLoadingValidation:
    """SPTDataLoadingStep 輸入驗證測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    async def test_validate_no_configs_returns_false(self, mock_cm):
        """測試無檔案配置時驗證應返回 False"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep(file_paths={})
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    async def test_validate_missing_raw_po_returns_false(self, mock_cm):
        """測試缺少 raw_po 配置時驗證應返回 False"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep(file_paths={'previous': '/tmp/prev.xlsx'})
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    async def test_validate_raw_po_file_not_found_returns_false(self, mock_cm):
        """測試 raw_po 檔案不存在時驗證應返回 False"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep(
            file_paths={'raw_po': '/nonexistent/path/file.xlsx'}
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    async def test_validate_raw_po_exists_returns_true(self, mock_cm, tmp_path):
        """測試 raw_po 檔案存在時驗證應返回 True"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        # 建立臨時測試檔案
        test_file = tmp_path / "202512_po.xlsx"
        test_file.touch()

        step = SPTDataLoadingStep(
            file_paths={'raw_po': str(test_file)}
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202512, processing_type='PO'
        )
        result = await step.validate_input(ctx)
        assert result is True


@pytest.mark.unit
class TestSPTDataLoadingExtractRawPO:
    """SPTDataLoadingStep 原始 PO 數據提取測試（使用統一的 _extract_primary_data）"""

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_extract_raw_po_data_valid(self, mock_cm):
        """測試有效的 PO 數據應正確提取"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep()
        df = pd.DataFrame({
            'Product Code': ['P001'],
            'Item Description': ['Test'],
            'GL#': ['100000'],
        })
        result = step._extract_primary_data(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_extract_raw_po_data_empty_raises(self, mock_cm):
        """測試空 DataFrame 應拋出 ValueError"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep()
        with pytest.raises(ValueError, match="Raw raw_po data is empty"):
            step._extract_primary_data(pd.DataFrame())

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_extract_raw_po_data_missing_columns_raises(self, mock_cm):
        """測試缺少必要欄位應拋出 ValueError"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep()
        df = pd.DataFrame({'SomeColumn': [1, 2]})
        with pytest.raises(ValueError, match="Missing required columns"):
            step._extract_primary_data(df)

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_extract_raw_po_data_invalid_format_raises(self, mock_cm):
        """測試無效的數據格式（非 DataFrame）應拋出 ValueError"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTDataLoadingStep
        step = SPTDataLoadingStep()
        with pytest.raises(ValueError, match="Raw raw_po data is empty"):
            step._extract_primary_data("not_a_dataframe")


# ============================================================
# SPTPRDataLoadingStep 測試
# ============================================================

@pytest.mark.unit
class TestSPTPRDataLoadingStepInit:
    """SPTPRDataLoadingStep 初始化與基本屬性測試"""

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_default_name(self, mock_cm):
        """測試預設步驟名稱為 SPTPRDataLoading"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        step = SPTPRDataLoadingStep()
        assert step.name == "SPTPRDataLoading"

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_description(self, mock_cm):
        """測試步驟描述正確設置"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        step = SPTPRDataLoadingStep()
        assert "PR" in step.description

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_normalize_mixed_formats(self, mock_cm):
        """測試混合格式路徑（字串 + 字典）皆可正確標準化"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        step = SPTPRDataLoadingStep(file_paths={
            'raw_pr': '/tmp/pr.xlsx',
            'budget_ref': {
                'path': '/tmp/budget.xlsx',
                'params': {'sheet_name': 0}
            }
        })
        assert step.file_configs['raw_pr']['path'] == '/tmp/pr.xlsx'
        assert step.file_configs['budget_ref']['params'] == {'sheet_name': 0}


@pytest.mark.unit
class TestSPTPRDataLoadingValidation:
    """SPTPRDataLoadingStep 輸入驗證測試"""

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    async def test_validate_missing_raw_pr_returns_false(self, mock_cm):
        """測試缺少 raw_pr 配置時驗證應返回 False"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        step = SPTPRDataLoadingStep(file_paths={'budget_ref': '/tmp/b.xlsx'})
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202512, processing_type='PR'
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    async def test_validate_raw_pr_exists_returns_true(self, mock_cm, tmp_path):
        """測試 raw_pr 檔案存在時驗證應返回 True"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        test_file = tmp_path / "202512_pr.xlsx"
        test_file.touch()

        step = SPTPRDataLoadingStep(
            file_paths={'raw_pr': str(test_file)}
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202512, processing_type='PR'
        )
        result = await step.validate_input(ctx)
        assert result is True


@pytest.mark.unit
class TestSPTPRDataLoadingExtractRawPR:
    """SPTPRDataLoadingStep 原始 PR 數據提取測試（使用統一的 _extract_primary_data）"""

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_extract_raw_pr_data_valid(self, mock_cm):
        """測試有效的 PR 數據應正確提取"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        step = SPTPRDataLoadingStep()
        df = pd.DataFrame({
            'Product Code': ['P001'],
            'Item Description': ['Test PR'],
            'GL#': ['200000'],
        })
        result = step._extract_primary_data(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_extract_raw_pr_data_empty_raises(self, mock_cm):
        """測試空 DataFrame 應拋出 ValueError"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        step = SPTPRDataLoadingStep()
        with pytest.raises(ValueError, match="Raw raw_pr data is empty"):
            step._extract_primary_data(pd.DataFrame())

    @patch('accrual_bot.tasks.spt.steps.spt_loading.config_manager')
    def test_validate_file_configs_missing_raw_pr_raises(self, mock_cm, tmp_path):
        """測試必要 raw_pr 檔案不存在時 _validate_file_configs 應拋出 FileNotFoundError"""
        from accrual_bot.tasks.spt.steps.spt_loading import SPTPRDataLoadingStep
        step = SPTPRDataLoadingStep(file_paths={
            'raw_pr': '/nonexistent/202512_pr.xlsx'
        })
        with pytest.raises(FileNotFoundError):
            step._validate_file_configs()
