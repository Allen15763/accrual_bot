"""Pipeline Orchestrators 集成測試"""
import pytest
import pandas as pd
from pathlib import Path
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.core.pipeline.context import ProcessingContext
from tests.fixtures.sample_data import create_minimal_loading_df


@pytest.mark.integration
class TestPipelineOrchestratorsIntegration:
    """Pipeline Orchestrators 集成測試"""

    @pytest.fixture
    def temp_test_files(self, tmp_path):
        """創建臨時測試文件"""
        # 創建 SPT PO 文件
        spt_po_file = tmp_path / "spt_po_202512.xlsx"
        df = create_minimal_loading_df()
        df.to_excel(spt_po_file, index=False)

        # 創建參考文件
        ref_file = tmp_path / "reference.xlsx"
        ref_df = pd.DataFrame({
            'Account': ['100000', '100001'],
            'Account Desc': ['Cash', 'Receivables']
        })
        ref_df.to_excel(ref_file, index=False)

        return {
            'spt_po': str(spt_po_file),
            'reference': str(ref_file)
        }

    @pytest.mark.asyncio
    async def test_spt_orchestrator_full_pipeline(
        self, temp_test_files, mock_config_manager
    ):
        """測試 SPT orchestrator 完整 pipeline"""
        orchestrator = SPTPipelineOrchestrator()

        file_paths = {
            'po_file': temp_test_files['spt_po'],
            'reference': temp_test_files['reference']
        }

        # 構建 pipeline
        pipeline = orchestrator.build_po_pipeline(file_paths)

        # 驗證 pipeline 結構（步驟數由 config/stagging.toml 決定）
        assert len(pipeline.steps) >= 6
        assert pipeline.config.entity_type == 'SPT'

        # 註：完整執行需要所有步驟的依賴，此處只驗證構建

    @pytest.mark.asyncio
    async def test_spx_orchestrator_full_pipeline(
        self, temp_test_files, mock_config_manager
    ):
        """測試 SPX orchestrator 完整 pipeline"""
        orchestrator = SPXPipelineOrchestrator()

        file_paths = {
            'po_file': temp_test_files['spt_po'],  # 使用相同測試文件
            'reference': temp_test_files['reference']
        }

        # 構建 pipeline
        pipeline = orchestrator.build_po_pipeline(file_paths)

        # 驗證 pipeline 結構（步驟數由 config/stagging.toml 決定）
        assert len(pipeline.steps) >= 8
        assert pipeline.config.entity_type == 'SPX'
