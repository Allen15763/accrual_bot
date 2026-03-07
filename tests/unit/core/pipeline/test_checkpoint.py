"""CheckpointManager 和 PipelineWithCheckpoint 單元測試"""
import json
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch

from accrual_bot.core.pipeline.checkpoint import (
    CheckpointManager,
    PipelineWithCheckpoint,
    execute_pipeline_with_checkpoint,
    resume_from_step,
    quick_test_step,
    list_available_checkpoints,
)
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.pipeline import Pipeline, PipelineConfig
from accrual_bot.core.pipeline.base import StepResult, StepStatus


@pytest.mark.unit
class TestCheckpointManager:
    """CheckpointManager 測試套件"""

    @pytest.fixture
    def manager(self, tmp_checkpoint_dir):
        return CheckpointManager(checkpoint_dir=tmp_checkpoint_dir)

    @pytest.fixture
    def sample_context(self):
        df = pd.DataFrame({
            'GL#': ['100000', '100001', '100002'],
            'Amount': [1000.0, 2000.0, 3000.0],
            'Status': ['A', 'B', 'C'],
        })
        ctx = ProcessingContext(
            data=df,
            entity_type='SPX',
            processing_date=202512,
            processing_type='PO',
        )
        ctx.add_auxiliary_data('ref_account', pd.DataFrame({
            'Account': ['100000'], 'Desc': ['Cash']
        }))
        ctx.set_variable('file_date', 202512)
        ctx.set_variable('step_count', 5)
        ctx.warnings = ['warn1']
        ctx.errors = ['err1']
        return ctx

    # --- save ---

    def test_save_creates_directory(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        checkpoint_path = Path(manager.checkpoint_dir) / name
        assert checkpoint_path.exists()
        assert (checkpoint_path / 'checkpoint_info.json').exists()

    def test_save_returns_expected_name_format(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'LoadData')
        assert name == 'SPX_PO_202512_after_LoadData'

    def test_save_main_data_parquet(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        checkpoint_path = Path(manager.checkpoint_dir) / name
        assert (checkpoint_path / 'data.parquet').exists()

    def test_save_auxiliary_data(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        aux_dir = Path(manager.checkpoint_dir) / name / 'auxiliary_data'
        assert aux_dir.exists()
        parquet_files = list(aux_dir.glob('*.parquet'))
        assert len(parquet_files) >= 1

    def test_save_variables_in_json(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        info_path = Path(manager.checkpoint_dir) / name / 'checkpoint_info.json'
        with open(info_path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        assert info['variables']['file_date'] == 202512
        assert info['variables']['step_count'] == 5

    def test_save_non_serializable_variable_converted_to_str(self, manager, sample_context):
        sample_context.set_variable('complex_obj', object())
        name = manager.save_checkpoint(sample_context, 'Step1')
        info_path = Path(manager.checkpoint_dir) / name / 'checkpoint_info.json'
        with open(info_path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        assert isinstance(info['variables']['complex_obj'], str)

    def test_save_warnings_and_errors(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        info_path = Path(manager.checkpoint_dir) / name / 'checkpoint_info.json'
        with open(info_path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        assert info['warnings'] == ['warn1']
        assert info['errors'] == ['err1']

    def test_save_empty_data(self, manager):
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO',
        )
        name = manager.save_checkpoint(ctx, 'EmptyStep')
        assert name == 'TEST_PO_202512_after_EmptyStep'

    def test_save_with_metadata(self, manager, sample_context):
        name = manager.save_checkpoint(
            sample_context, 'Step1', metadata={'custom_key': 'custom_value'}
        )
        info_path = Path(manager.checkpoint_dir) / name / 'checkpoint_info.json'
        with open(info_path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        assert info['metadata']['custom_key'] == 'custom_value'

    # --- load ---

    def test_load_restores_main_data(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        loaded = manager.load_checkpoint(name)
        pd.testing.assert_frame_equal(loaded.data, sample_context.data)

    def test_load_restores_auxiliary_data(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        loaded = manager.load_checkpoint(name)
        assert loaded.has_auxiliary_data('ref_account')
        pd.testing.assert_frame_equal(
            loaded.get_auxiliary_data('ref_account'),
            sample_context.get_auxiliary_data('ref_account'),
        )

    def test_load_restores_variables(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        loaded = manager.load_checkpoint(name)
        assert loaded.get_variable('file_date') == 202512
        assert loaded.get_variable('step_count') == 5

    def test_load_restores_warnings_errors(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        loaded = manager.load_checkpoint(name)
        assert loaded.warnings == ['warn1']
        assert loaded.errors == ['err1']

    def test_load_missing_raises_error(self, manager):
        with pytest.raises(FileNotFoundError):
            manager.load_checkpoint('nonexistent_checkpoint')

    def test_load_restores_metadata_fields(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        loaded = manager.load_checkpoint(name)
        assert loaded.metadata.entity_type == 'SPX'
        assert loaded.metadata.processing_date == 202512
        assert loaded.metadata.processing_type == 'PO'

    # --- list / delete / cleanup ---

    def test_list_checkpoints(self, manager, sample_context):
        manager.save_checkpoint(sample_context, 'Step1')
        manager.save_checkpoint(sample_context, 'Step2')
        cps = manager.list_checkpoints()
        assert len(cps) == 2

    def test_list_checkpoints_filter_by_entity(self, manager, sample_context):
        manager.save_checkpoint(sample_context, 'Step1')
        # 建一個不同 entity 的 checkpoint
        ctx2 = ProcessingContext(
            data=pd.DataFrame({'A': [1]}),
            entity_type='SPT',
            processing_date=202512,
            processing_type='PO',
        )
        manager.save_checkpoint(ctx2, 'Step1')
        spx_only = manager.list_checkpoints(filter_by_entity='SPX')
        assert len(spx_only) == 1
        assert spx_only[0]['entity_type'] == 'SPX'

    def test_delete_checkpoint(self, manager, sample_context):
        name = manager.save_checkpoint(sample_context, 'Step1')
        assert manager.delete_checkpoint(name) is True
        assert not (Path(manager.checkpoint_dir) / name).exists()

    def test_delete_nonexistent_returns_false(self, manager):
        assert manager.delete_checkpoint('nonexistent') is False

    def test_cleanup_keeps_recent(self, manager, sample_context):
        for i in range(7):
            manager.save_checkpoint(sample_context, f'Step{i}')
        deleted = manager.cleanup_old_checkpoints(keep_last=3)
        assert deleted == 4
        remaining = manager.list_checkpoints()
        assert len(remaining) == 3


@pytest.mark.unit
class TestPipelineWithCheckpoint:
    """PipelineWithCheckpoint 測試套件"""

    @pytest.fixture
    def manager(self, tmp_checkpoint_dir):
        return CheckpointManager(checkpoint_dir=tmp_checkpoint_dir)

    @pytest.fixture
    def simple_pipeline(self, make_success_step):
        config = PipelineConfig(name="TestPipeline", entity_type="TEST")
        pipeline = Pipeline(config)
        pipeline.add_step(make_success_step("Step1"))
        pipeline.add_step(make_success_step("Step2"))
        pipeline.add_step(make_success_step("Step3"))
        return pipeline

    @pytest.mark.asyncio
    async def test_execute_saves_after_each_step(self, simple_pipeline, manager):
        executor = PipelineWithCheckpoint(simple_pipeline, manager)
        ctx = ProcessingContext(
            data=pd.DataFrame({'A': [1]}),
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO',
        )
        result = await executor.execute_with_checkpoint(ctx, save_after_each_step=True)
        assert result['success'] is True
        assert result['successful_steps'] == 3
        cps = manager.list_checkpoints()
        assert len(cps) == 3

    @pytest.mark.asyncio
    async def test_execute_from_specific_step(self, simple_pipeline, manager):
        executor = PipelineWithCheckpoint(simple_pipeline, manager)
        ctx = ProcessingContext(
            data=pd.DataFrame({'A': [1]}),
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO',
        )
        result = await executor.execute_with_checkpoint(
            ctx, save_after_each_step=False, start_from_step='Step2'
        )
        assert result['success'] is True
        assert result['executed_steps'] == 2

    @pytest.mark.asyncio
    async def test_execute_invalid_start_step_raises(self, simple_pipeline, manager):
        executor = PipelineWithCheckpoint(simple_pipeline, manager)
        ctx = ProcessingContext(
            data=pd.DataFrame({'A': [1]}),
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO',
        )
        with pytest.raises(ValueError, match="找不到步驟"):
            await executor.execute_with_checkpoint(
                ctx, start_from_step='NonExistentStep'
            )

    @pytest.mark.asyncio
    async def test_execute_stops_on_failure(self, make_success_step, manager):
        """步驟失敗時停止執行（execute_with_checkpoint 直接呼叫 step.execute）"""
        from accrual_bot.core.pipeline.base import PipelineStep

        class ReturnFailStep(PipelineStep):
            async def execute(self, context):
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="intentional fail",
                )
            async def validate_input(self, context):
                return True

        config = PipelineConfig(name="FailPipeline", entity_type="TEST", stop_on_error=True)
        pipeline = Pipeline(config)
        pipeline.add_step(make_success_step("Step1"))
        pipeline.add_step(ReturnFailStep(name="Step2"))
        pipeline.add_step(make_success_step("Step3"))

        executor = PipelineWithCheckpoint(pipeline, manager)
        ctx = ProcessingContext(
            data=pd.DataFrame({'A': [1]}),
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO',
        )
        result = await executor.execute_with_checkpoint(ctx)
        assert result['success'] is False
        assert result['executed_steps'] == 2
        # Step1 成功後儲存 checkpoint, Step2 失敗不儲存
        cps = manager.list_checkpoints()
        assert len(cps) == 1


@pytest.mark.unit
class TestConvenienceFunctions:
    """便捷函數測試"""

    def test_list_available_checkpoints_empty(self, tmp_checkpoint_dir):
        result = list_available_checkpoints(checkpoint_dir=tmp_checkpoint_dir)
        assert result == []

    def test_list_available_checkpoints_with_data(self, tmp_checkpoint_dir):
        manager = CheckpointManager(checkpoint_dir=tmp_checkpoint_dir)
        ctx = ProcessingContext(
            data=pd.DataFrame({'A': [1]}),
            entity_type='SPX',
            processing_date=202512,
            processing_type='PO',
        )
        manager.save_checkpoint(ctx, 'Step1')
        result = list_available_checkpoints(checkpoint_dir=tmp_checkpoint_dir)
        assert len(result) == 1
