"""
測試 Pipeline 和 PipelineConfig 類
從 accrual_bot.core.pipeline.pipeline 模組
"""

import pytest
import pandas as pd
from unittest.mock import AsyncMock, patch

from accrual_bot.core.pipeline.pipeline import Pipeline, PipelineConfig
from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# === 輔助用具體步驟類 ===


class DummySuccessStep(PipelineStep):
    """總是成功的步驟"""

    async def execute(self, context: ProcessingContext) -> StepResult:
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS, message="OK")

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


class DummyFailStep(PipelineStep):
    """總是失敗的步驟"""

    async def execute(self, context: ProcessingContext) -> StepResult:
        return StepResult(
            step_name=self.name,
            status=StepStatus.FAILED,
            error=RuntimeError("step failed"),
            message="step failed",
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


class DummyExceptionStep(PipelineStep):
    """執行時拋出例外的步驟"""

    async def execute(self, context: ProcessingContext) -> StepResult:
        raise RuntimeError("unexpected error")

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


# === Fixtures ===


@pytest.fixture
def pipeline_config():
    return PipelineConfig(name="TestPipeline", entity_type="SPT")


@pytest.fixture
def empty_context():
    return ProcessingContext(
        data=pd.DataFrame({"col": [1, 2, 3]}),
        entity_type="SPT",
        processing_date=202512,
        processing_type="PO",
    )


@pytest.fixture
def dummy_success_step():
    return DummySuccessStep(name="SuccessStep")


@pytest.fixture
def dummy_fail_step():
    return DummyFailStep(name="FailStep")


def make_success_step(name: str = "SuccessStep") -> DummySuccessStep:
    return DummySuccessStep(name=name)


def make_fail_step(name: str = "FailStep") -> DummyFailStep:
    return DummyFailStep(name=name)


# === TestPipelineConfig ===


@pytest.mark.unit
class TestPipelineConfig:
    """測試 PipelineConfig dataclass"""

    def test_default_values(self):
        """預設值應正確設定"""
        cfg = PipelineConfig(name="P1")
        assert cfg.name == "P1"
        assert cfg.description == ""
        assert cfg.entity_type == "MOB"
        assert cfg.stop_on_error is True
        assert cfg.parallel_execution is False
        assert cfg.max_concurrent_steps == 5
        assert cfg.enable_cache is False
        assert cfg.log_level == "INFO"

    def test_custom_values(self):
        """自訂值應被保留"""
        cfg = PipelineConfig(
            name="Custom",
            description="desc",
            entity_type="SPX",
            stop_on_error=False,
            parallel_execution=True,
            max_concurrent_steps=10,
            enable_cache=True,
            log_level="DEBUG",
        )
        assert cfg.entity_type == "SPX"
        assert cfg.stop_on_error is False
        assert cfg.parallel_execution is True
        assert cfg.max_concurrent_steps == 10

    def test_to_dict(self):
        """to_dict 應回傳完整欄位"""
        cfg = PipelineConfig(name="P1", entity_type="SPT")
        d = cfg.to_dict()
        assert d["name"] == "P1"
        assert d["entity_type"] == "SPT"
        assert set(d.keys()) == {
            "name",
            "description",
            "entity_type",
            "stop_on_error",
            "parallel_execution",
            "max_concurrent_steps",
            "enable_cache",
            "log_level",
        }


# === TestPipeline ===


@pytest.mark.unit
class TestPipeline:
    """測試 Pipeline 類"""

    # --- 步驟管理 ---

    def test_add_step_returns_self(self, pipeline_config, dummy_success_step):
        """add_step 應回傳 Pipeline（fluent API）"""
        pipeline = Pipeline(pipeline_config)
        result = pipeline.add_step(dummy_success_step)
        assert result is pipeline
        assert len(pipeline.steps) == 1

    def test_add_steps(self, pipeline_config):
        """add_steps 應一次加入多個步驟"""
        pipeline = Pipeline(pipeline_config)
        steps = [make_success_step("S1"), make_success_step("S2"), make_success_step("S3")]
        pipeline.add_steps(steps)
        assert len(pipeline.steps) == 3

    def test_remove_step_existing(self, pipeline_config):
        """remove_step 移除存在的步驟應回傳 True"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("ToRemove"))
        pipeline.add_step(make_success_step("Keep"))
        assert pipeline.remove_step("ToRemove") is True
        assert len(pipeline.steps) == 1
        assert pipeline.steps[0].name == "Keep"

    def test_remove_step_nonexistent(self, pipeline_config):
        """remove_step 移除不存在的步驟應回傳 False"""
        pipeline = Pipeline(pipeline_config)
        assert pipeline.remove_step("NoSuchStep") is False

    def test_get_step_found(self, pipeline_config, dummy_success_step):
        """get_step 找到時應回傳步驟"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(dummy_success_step)
        found = pipeline.get_step("SuccessStep")
        assert found is dummy_success_step

    def test_get_step_not_found(self, pipeline_config):
        """get_step 未找到應回傳 None"""
        pipeline = Pipeline(pipeline_config)
        assert pipeline.get_step("Missing") is None

    def test_clear_steps(self, pipeline_config):
        """clear_steps 應清空所有步驟"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_steps([make_success_step("A"), make_success_step("B")])
        pipeline.clear_steps()
        assert len(pipeline.steps) == 0

    # --- 順序執行 ---

    @pytest.mark.asyncio
    async def test_execute_sequential_all_success(self, pipeline_config, empty_context):
        """所有步驟成功時 Pipeline 應成功"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_steps([make_success_step("S1"), make_success_step("S2")])

        result = await pipeline.execute(empty_context)

        assert result["success"] is True
        assert result["pipeline"] == "TestPipeline"
        assert result["total_steps"] == 2
        assert result["executed_steps"] == 2
        assert result["successful_steps"] == 2
        assert result["failed_steps"] == 0

    @pytest.mark.asyncio
    async def test_execute_sequential_stop_on_error(self, pipeline_config, empty_context):
        """stop_on_error=True 時遇到失敗應停止後續步驟"""
        pipeline_config.stop_on_error = True
        pipeline = Pipeline(pipeline_config)
        pipeline.add_steps([
            make_success_step("S1"),
            make_fail_step("S2"),
            make_success_step("S3"),
        ])

        result = await pipeline.execute(empty_context)

        assert result["success"] is False
        assert result["failed_steps"] == 1
        # S3 不應被執行，所以 executed_steps == 2
        assert result["executed_steps"] == 2

    @pytest.mark.asyncio
    async def test_execute_sequential_continue_on_error(self, empty_context):
        """stop_on_error=False 時失敗後應繼續執行"""
        config = PipelineConfig(name="Continue", stop_on_error=False)
        pipeline = Pipeline(config)
        pipeline.add_steps([
            make_success_step("S1"),
            make_fail_step("S2"),
            make_success_step("S3"),
        ])

        result = await pipeline.execute(empty_context)

        assert result["success"] is False
        # 所有 3 個步驟都應被執行
        assert result["executed_steps"] == 3
        assert result["successful_steps"] == 2
        assert result["failed_steps"] == 1

    @pytest.mark.asyncio
    async def test_execute_empty_pipeline(self, pipeline_config, empty_context):
        """無步驟的 Pipeline 應成功完成"""
        pipeline = Pipeline(pipeline_config)
        result = await pipeline.execute(empty_context)
        assert result["success"] is True
        assert result["total_steps"] == 0

    @pytest.mark.asyncio
    async def test_execute_increments_count(self, pipeline_config, empty_context):
        """每次執行應遞增 _execution_count"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))

        assert pipeline._execution_count == 0
        await pipeline.execute(empty_context)
        assert pipeline._execution_count == 1
        await pipeline.execute(empty_context)
        assert pipeline._execution_count == 2

    @pytest.mark.asyncio
    async def test_execute_records_history(self, pipeline_config, empty_context):
        """執行後應記錄到 _execution_history"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))

        await pipeline.execute(empty_context)
        assert len(pipeline._execution_history) == 1
        assert pipeline._execution_history[0]["success"] is True

    @pytest.mark.asyncio
    async def test_execute_updates_last_execution(self, pipeline_config, empty_context):
        """執行後應更新 _last_execution"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))

        assert pipeline._last_execution is None
        await pipeline.execute(empty_context)
        assert pipeline._last_execution is not None
        assert pipeline._last_execution["success"] is True

    @pytest.mark.asyncio
    async def test_execute_result_keys(self, pipeline_config, empty_context):
        """執行結果應包含必要的欄位"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))
        result = await pipeline.execute(empty_context)

        expected_keys = {
            "pipeline", "success", "start_time", "end_time", "duration",
            "total_steps", "executed_steps", "successful_steps",
            "failed_steps", "skipped_steps", "results",
            "context_summary", "errors", "warnings",
        }
        assert expected_keys.issubset(set(result.keys()))

    @pytest.mark.asyncio
    async def test_execute_exception_in_step(self, pipeline_config, empty_context):
        """步驟拋出例外時 Pipeline 應處理並回傳失敗"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(DummyExceptionStep(name="ExcStep"))

        result = await pipeline.execute(empty_context)
        # 例外會被 PipelineStep.__call__ 捕捉，回傳 FAILED 結果
        assert result["success"] is False

    # --- 統計 ---

    def test_get_statistics_initial(self, pipeline_config):
        """初始統計應為零"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))
        stats = pipeline.get_statistics()
        assert stats["total_executions"] == 0
        assert stats["last_execution"] is None
        assert stats["total_steps"] == 1
        assert stats["step_names"] == ["S1"]

    @pytest.mark.asyncio
    async def test_get_statistics_after_execution(self, pipeline_config, empty_context):
        """執行後統計應更新"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))
        await pipeline.execute(empty_context)

        stats = pipeline.get_statistics()
        assert stats["total_executions"] == 1
        assert stats["last_execution"] is not None

    # --- Clone ---

    def test_clone_creates_independent_copy(self, pipeline_config):
        """clone 應建立獨立的步驟列表副本"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))
        cloned = pipeline.clone()

        assert cloned is not pipeline
        assert len(cloned.steps) == 1
        assert cloned.config is pipeline.config  # 共享同一個 config 物件

        # 修改原始 Pipeline 不應影響 clone
        pipeline.add_step(make_success_step("S2"))
        assert len(pipeline.steps) == 2
        assert len(cloned.steps) == 1

    def test_repr(self, pipeline_config):
        """__repr__ 應回傳可讀字串"""
        pipeline = Pipeline(pipeline_config)
        pipeline.add_step(make_success_step("S1"))
        assert "TestPipeline" in repr(pipeline)
        assert "1" in repr(pipeline)
