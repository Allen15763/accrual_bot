"""Checkpoint save → load 完整流程整合測試"""
import pytest
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.checkpoint import CheckpointManager
from accrual_bot.core.pipeline.context import ProcessingContext


@pytest.mark.integration
class TestCheckpointRoundtrip:
    """CheckpointManager save → load 完整流程測試"""

    @pytest.fixture
    def manager(self, tmp_path):
        """建立暫存目錄的 CheckpointManager"""
        return CheckpointManager(checkpoint_dir=str(tmp_path / "checkpoints"))

    @pytest.fixture
    def sample_context(self):
        """建立含典型資料的 ProcessingContext"""
        df = pd.DataFrame({
            'PO#': ['PO001', 'PO002', 'PO003'],
            'GL#': ['100000', '100001', '199999'],
            'Entry Amount': [1000.0, 2000.0, 3000.0],
            'PO狀態': ['已完成', '未完成', '待評估'],
            'Item Description': ['Item A', 'Item B', 'Item C'],
        })
        ctx = ProcessingContext(
            data=df,
            entity_type='SPX',
            processing_date=202512,
            processing_type='PO',
        )
        # 添加輔助資料
        ctx.add_auxiliary_data('reference_account', pd.DataFrame({
            'Account': ['100000', '100001'],
            'Account Desc': ['Cash', 'Receivables'],
        }))
        # 添加變數
        ctx.set_variable('processing_date', 202512)
        ctx.set_variable('file_count', 3)
        ctx.set_variable('status', 'in_progress')
        return ctx

    def test_save_and_load_preserves_data(self, manager, sample_context):
        """save → load 應完整保留主數據"""
        name = manager.save_checkpoint(sample_context, step_name="Step1")
        loaded = manager.load_checkpoint(name)

        pd.testing.assert_frame_equal(loaded.data, sample_context.data)

    def test_save_and_load_preserves_auxiliary_data(self, manager, sample_context):
        """save → load 應保留輔助資料"""
        name = manager.save_checkpoint(sample_context, step_name="Step1")
        loaded = manager.load_checkpoint(name)

        assert loaded.has_auxiliary_data('reference_account')
        ref = loaded.get_auxiliary_data('reference_account')
        expected = sample_context.get_auxiliary_data('reference_account')
        pd.testing.assert_frame_equal(ref, expected)

    def test_save_and_load_preserves_variables(self, manager, sample_context):
        """save → load 應保留變數"""
        name = manager.save_checkpoint(sample_context, step_name="Step1")
        loaded = manager.load_checkpoint(name)

        assert loaded.get_variable('processing_date') == 202512
        assert loaded.get_variable('file_count') == 3
        assert loaded.get_variable('status') == 'in_progress'

    def test_save_and_load_preserves_metadata(self, manager, sample_context):
        """save → load 應保留 entity_type、processing_date、processing_type"""
        name = manager.save_checkpoint(sample_context, step_name="Step1")
        loaded = manager.load_checkpoint(name)

        assert loaded.metadata.entity_type == 'SPX'
        assert loaded.metadata.processing_date == 202512
        assert loaded.metadata.processing_type == 'PO'

    def test_checkpoint_name_format(self, manager, sample_context):
        """checkpoint 名稱格式應為 {entity}_{type}_{date}_after_{step}"""
        name = manager.save_checkpoint(sample_context, step_name="DataLoading")
        assert name == "SPX_PO_202512_after_DataLoading"

    def test_list_checkpoints(self, manager, sample_context):
        """list_checkpoints 應列出已儲存的 checkpoint"""
        manager.save_checkpoint(sample_context, step_name="Step1")
        manager.save_checkpoint(sample_context, step_name="Step2")

        checkpoints = manager.list_checkpoints()
        assert len(checkpoints) == 2

    def test_delete_checkpoint(self, manager, sample_context):
        """delete_checkpoint 應成功刪除"""
        name = manager.save_checkpoint(sample_context, step_name="Step1")
        assert manager.delete_checkpoint(name) is True
        assert manager.list_checkpoints() == []

    def test_load_nonexistent_raises(self, manager):
        """載入不存在的 checkpoint 應拋出 FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            manager.load_checkpoint("nonexistent_checkpoint")

    def test_cleanup_old_checkpoints(self, manager, sample_context):
        """cleanup 應只保留最近 N 個"""
        for i in range(5):
            manager.save_checkpoint(sample_context, step_name=f"Step{i}")

        deleted = manager.cleanup_old_checkpoints(keep_last=2)
        assert deleted == 3
        remaining = manager.list_checkpoints()
        assert len(remaining) == 2

    def test_filter_by_entity(self, manager):
        """filter_by_entity 應只列出指定 entity 的 checkpoint"""
        spx_ctx = ProcessingContext(
            data=pd.DataFrame({'a': [1]}),
            entity_type='SPX', processing_date=202512, processing_type='PO',
        )
        spt_ctx = ProcessingContext(
            data=pd.DataFrame({'a': [1]}),
            entity_type='SPT', processing_date=202512, processing_type='PO',
        )
        manager.save_checkpoint(spx_ctx, step_name="S1")
        manager.save_checkpoint(spt_ctx, step_name="S1")

        spx_only = manager.list_checkpoints(filter_by_entity='SPX')
        assert len(spx_only) == 1
        assert spx_only[0]['entity_type'] == 'SPX'
