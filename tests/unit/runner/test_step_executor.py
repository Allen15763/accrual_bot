"""StepByStepExecutor 單元測試

測試逐步執行器：
- run() 完整流程（continue / skip / abort）
- _prompt_action 用戶選擇
- _confirm_continue_on_error 失敗後確認
- _save_checkpoint 儲存
- _build_execution_result 結果建構
- 異常處理
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.pipeline import Pipeline, PipelineConfig
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

class DummyStep(PipelineStep):
    """測試用的虛擬步驟"""

    def __init__(self, name="DummyStep", result_status=StepStatus.SUCCESS):
        super().__init__(name, description="Dummy step for testing")
        self._result_status = result_status

    async def execute(self, context: ProcessingContext) -> StepResult:
        return StepResult(
            step_name=self.name,
            status=self._result_status,
            message=f"{self.name} executed",
            duration=0.1
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


class FailingStep(PipelineStep):
    """會引發異常的步驟"""

    def __init__(self, name="FailingStep"):
        super().__init__(name, description="Failing step")

    async def execute(self, context: ProcessingContext) -> StepResult:
        raise RuntimeError("Step execution error")

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


@pytest.fixture
def test_context():
    """測試用 ProcessingContext"""
    return ProcessingContext(
        data=pd.DataFrame({'col1': [1, 2, 3]}),
        entity_type='SCT',
        processing_date=202503,
        processing_type='PO',
    )


@pytest.fixture
def simple_pipeline():
    """建立含 2 個步驟的簡單 pipeline"""
    config = PipelineConfig(name="TestPipeline", entity_type="SCT")
    pipeline = Pipeline(config)
    pipeline.steps = [DummyStep("Step1"), DummyStep("Step2")]
    return pipeline


@pytest.fixture
def three_step_pipeline():
    """建立含 3 個步驟的 pipeline"""
    config = PipelineConfig(name="TestPipeline3", entity_type="SCT")
    pipeline = Pipeline(config)
    pipeline.steps = [DummyStep("Step1"), DummyStep("Step2"), DummyStep("Step3")]
    return pipeline


# ============================================================
# Tests
# ============================================================

class TestStepByStepExecutor:
    """測試逐步執行器"""

    @pytest.mark.unit
    def test_instantiation(self, simple_pipeline, test_context):
        """正確初始化"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(simple_pipeline, test_context)
        assert executor.pipeline is simple_pipeline
        assert executor.context is test_context
        assert executor.save_checkpoints is True
        assert executor.aborted is False
        assert executor.results == []

    @pytest.mark.unit
    def test_instantiation_no_checkpoints(self, simple_pipeline, test_context):
        """停用 checkpoint"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        assert executor.checkpoint_manager is None

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_run_all_continue(self, simple_pipeline, test_context):
        """所有步驟都選擇繼續"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        with patch('builtins.input', return_value=''):
            result = await executor.run()
        assert result['success'] is True
        assert result['successful_steps'] == 2
        assert result['failed_steps'] == 0
        assert result['aborted'] is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_run_skip_step(self, simple_pipeline, test_context):
        """跳過第一個步驟"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        responses = iter(['s', ''])
        with patch('builtins.input', side_effect=responses):
            result = await executor.run()
        assert result['skipped_steps'] == 1
        assert result['successful_steps'] == 1

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_run_abort(self, three_step_pipeline, test_context):
        """中止執行"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            three_step_pipeline, test_context, save_checkpoints=False
        )
        responses = iter(['', 'q'])
        with patch('builtins.input', side_effect=responses):
            result = await executor.run()
        assert result['aborted'] is True
        assert result['executed_steps'] < 3

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_run_with_failing_step_continue(self, test_context):
        """步驟失敗後選擇繼續"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        config = PipelineConfig(name="FailPipeline", entity_type="SCT")
        pipeline = Pipeline(config)
        pipeline.steps = [
            DummyStep("Step1", result_status=StepStatus.FAILED),
            DummyStep("Step2"),
        ]
        executor = StepByStepExecutor(
            pipeline, test_context, save_checkpoints=False
        )
        # '' = continue to execute step, 'y' = continue after failure, '' = continue next
        responses = iter(['', 'y', ''])
        with patch('builtins.input', side_effect=responses):
            result = await executor.run()
        assert result['failed_steps'] == 1
        assert result['successful_steps'] == 1

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_run_with_failing_step_abort(self, test_context):
        """步驟失敗後選擇中止"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        config = PipelineConfig(name="FailPipeline", entity_type="SCT")
        pipeline = Pipeline(config)
        pipeline.steps = [
            DummyStep("Step1", result_status=StepStatus.FAILED),
            DummyStep("Step2"),
        ]
        executor = StepByStepExecutor(
            pipeline, test_context, save_checkpoints=False
        )
        responses = iter(['', 'n'])
        with patch('builtins.input', side_effect=responses):
            result = await executor.run()
        assert result['aborted'] is True
        assert result['executed_steps'] == 1

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_run_with_exception_step(self, test_context):
        """步驟拋出異常"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        config = PipelineConfig(name="ExcPipeline", entity_type="SCT")
        pipeline = Pipeline(config)
        pipeline.steps = [FailingStep("Boom")]
        executor = StepByStepExecutor(
            pipeline, test_context, save_checkpoints=False
        )
        # '' = continue to execute, 'n' = don't continue after error
        responses = iter(['', 'n'])
        with patch('builtins.input', side_effect=responses):
            result = await executor.run()
        assert result['failed_steps'] == 1

    @pytest.mark.unit
    def test_prompt_action_continue(self, simple_pipeline, test_context):
        """Enter 或 'c' 回傳 continue"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        with patch('builtins.input', return_value=''):
            assert executor._prompt_action("Step1") == "continue"
        with patch('builtins.input', return_value='c'):
            assert executor._prompt_action("Step1") == "continue"

    @pytest.mark.unit
    def test_prompt_action_skip(self, simple_pipeline, test_context):
        """'s' 回傳 skip"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        with patch('builtins.input', return_value='s'):
            assert executor._prompt_action("Step1") == "skip"

    @pytest.mark.unit
    def test_prompt_action_abort(self, simple_pipeline, test_context):
        """'q' 回傳 abort"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        with patch('builtins.input', return_value='q'):
            assert executor._prompt_action("Step1") == "abort"

    @pytest.mark.unit
    def test_prompt_action_eof(self, simple_pipeline, test_context):
        """EOFError 自動繼續"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        with patch('builtins.input', side_effect=EOFError):
            assert executor._prompt_action("Step1") == "continue"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_save_checkpoint_on_success(self, simple_pipeline, test_context, tmp_path):
        """成功步驟自動儲存 checkpoint"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context,
            save_checkpoints=True,
            checkpoint_dir=str(tmp_path)
        )
        with patch.object(executor.checkpoint_manager, 'save_checkpoint', return_value='ckpt_1') as mock_save:
            with patch('builtins.input', return_value=''):
                await executor.run()
            # 每個成功步驟都應調用 save_checkpoint
            assert mock_save.call_count == 2

    @pytest.mark.unit
    def test_build_execution_result(self, simple_pipeline, test_context):
        """執行結果包含所有必要欄位"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        executor.start_time = datetime.now()
        executor.results = [
            StepResult(step_name="S1", status=StepStatus.SUCCESS, duration=1.0),
            StepResult(step_name="S2", status=StepStatus.SKIPPED),
        ]
        result = executor._build_execution_result()
        assert result['pipeline'] == "TestPipeline"
        assert result['success'] is True
        assert result['successful_steps'] == 1
        assert result['skipped_steps'] == 1
        assert result['failed_steps'] == 0
        assert 'results' in result
        assert 'context' in result

    @pytest.mark.unit
    def test_save_checkpoint_exception_handled(self, simple_pipeline, test_context, tmp_path):
        """checkpoint 儲存失敗不中斷流程"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context,
            save_checkpoints=True,
            checkpoint_dir=str(tmp_path)
        )
        with patch.object(
            executor.checkpoint_manager, 'save_checkpoint',
            side_effect=RuntimeError("disk full")
        ):
            # 應不拋出異常
            executor._save_checkpoint("Step1")

    @pytest.mark.unit
    def test_save_checkpoint_no_manager(self, simple_pipeline, test_context):
        """無 checkpoint manager 時安全返回"""
        from accrual_bot.runner.step_executor import StepByStepExecutor
        executor = StepByStepExecutor(
            simple_pipeline, test_context, save_checkpoints=False
        )
        # 應不拋出異常
        executor._save_checkpoint("Step1")
