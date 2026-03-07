"""
測試 PipelineBuilder 和 PipelineExecutor 類
從 accrual_bot.core.pipeline.pipeline 模組
"""

import pytest
import pandas as pd
from unittest.mock import patch

from accrual_bot.core.pipeline.pipeline import (
    Pipeline,
    PipelineBuilder,
    PipelineConfig,
    PipelineExecutor,
)
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
            error=RuntimeError("fail"),
            message="fail",
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


# === Fixtures ===


@pytest.fixture
def step_a():
    return DummySuccessStep(name="StepA")


@pytest.fixture
def step_b():
    return DummySuccessStep(name="StepB")


@pytest.fixture
def step_c():
    return DummySuccessStep(name="StepC")


@pytest.fixture
def fail_step():
    return DummyFailStep(name="FailStep")


@pytest.fixture
def sample_df():
    return pd.DataFrame({"col": [1, 2, 3]})


# === TestPipelineBuilder ===


@pytest.mark.unit
class TestPipelineBuilder:
    """測試 PipelineBuilder fluent API"""

    def test_init_sets_name_and_entity(self):
        """建構器應設定名稱和實體類型"""
        builder = PipelineBuilder("MyPipeline", entity_type="SPX")
        assert builder.config.name == "MyPipeline"
        assert builder.config.entity_type == "SPX"

    def test_init_default_entity_type(self):
        """未指定實體類型時應預設為 MOB"""
        builder = PipelineBuilder("P1")
        assert builder.config.entity_type == "MOB"

    def test_with_description_returns_self(self):
        """with_description 應回傳自身（fluent）"""
        builder = PipelineBuilder("P1")
        result = builder.with_description("test desc")
        assert result is builder
        assert builder.config.description == "test desc"

    def test_with_stop_on_error(self):
        """with_stop_on_error 應設定 stop_on_error"""
        builder = PipelineBuilder("P1")
        builder.with_stop_on_error(False)
        assert builder.config.stop_on_error is False

        builder.with_stop_on_error(True)
        assert builder.config.stop_on_error is True

    def test_with_parallel_execution(self):
        """with_parallel_execution 應設定 parallel_execution"""
        builder = PipelineBuilder("P1")
        result = builder.with_parallel_execution(True)
        assert result is builder
        assert builder.config.parallel_execution is True

    def test_with_max_concurrent(self):
        """with_max_concurrent 應設定 max_concurrent_steps"""
        builder = PipelineBuilder("P1")
        result = builder.with_max_concurrent(10)
        assert result is builder
        assert builder.config.max_concurrent_steps == 10

    def test_add_step_returns_self(self, step_a):
        """add_step 應回傳自身（fluent）"""
        builder = PipelineBuilder("P1")
        result = builder.add_step(step_a)
        assert result is builder
        assert len(builder.steps) == 1

    def test_add_steps_variadic(self, step_a, step_b, step_c):
        """add_steps 應支援可變引數"""
        builder = PipelineBuilder("P1")
        result = builder.add_steps(step_a, step_b, step_c)
        assert result is builder
        assert len(builder.steps) == 3

    def test_chained_api(self, step_a, step_b):
        """所有方法應支援鏈式呼叫"""
        pipeline = (
            PipelineBuilder("Chained", "SPT")
            .with_description("chain test")
            .with_stop_on_error(False)
            .with_parallel_execution(False)
            .with_max_concurrent(3)
            .add_step(step_a)
            .add_step(step_b)
            .build()
        )
        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "Chained"
        assert pipeline.config.entity_type == "SPT"
        assert pipeline.config.description == "chain test"
        assert pipeline.config.stop_on_error is False
        assert pipeline.config.max_concurrent_steps == 3
        assert len(pipeline.steps) == 2

    def test_build_returns_pipeline(self, step_a):
        """build 應回傳正確建構的 Pipeline"""
        builder = PipelineBuilder("Built", "SPX")
        builder.add_step(step_a)
        pipeline = builder.build()

        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "Built"
        assert pipeline.config.entity_type == "SPX"
        assert len(pipeline.steps) == 1
        assert pipeline.steps[0].name == "StepA"

    def test_build_empty_pipeline(self):
        """build 無步驟時應回傳空 Pipeline"""
        pipeline = PipelineBuilder("Empty").build()
        assert isinstance(pipeline, Pipeline)
        assert len(pipeline.steps) == 0


# === TestPipelineExecutor ===


@pytest.mark.unit
class TestPipelineExecutor:
    """測試 PipelineExecutor"""

    def _make_pipeline(self, name: str, steps=None, entity_type="SPT"):
        """建立測試用 Pipeline"""
        config = PipelineConfig(name=name, entity_type=entity_type)
        pipeline = Pipeline(config)
        if steps:
            pipeline.add_steps(steps)
        return pipeline

    def test_register_pipeline(self):
        """register_pipeline 應將 Pipeline 加入字典"""
        executor = PipelineExecutor()
        pipeline = self._make_pipeline("P1")
        executor.register_pipeline(pipeline)
        assert "P1" in executor.pipelines

    def test_unregister_pipeline_exists(self):
        """unregister_pipeline 移除存在的 Pipeline 應回傳 True"""
        executor = PipelineExecutor()
        executor.register_pipeline(self._make_pipeline("P1"))
        assert executor.unregister_pipeline("P1") is True
        assert "P1" not in executor.pipelines

    def test_unregister_pipeline_not_exists(self):
        """unregister_pipeline 移除不存在的 Pipeline 應回傳 False"""
        executor = PipelineExecutor()
        assert executor.unregister_pipeline("NoSuch") is False

    def test_get_pipeline(self):
        """get_pipeline 應回傳已註冊的 Pipeline"""
        executor = PipelineExecutor()
        pipeline = self._make_pipeline("P1")
        executor.register_pipeline(pipeline)
        assert executor.get_pipeline("P1") is pipeline
        assert executor.get_pipeline("Missing") is None

    def test_list_pipelines(self):
        """list_pipelines 應回傳所有已註冊的名稱"""
        executor = PipelineExecutor()
        executor.register_pipeline(self._make_pipeline("A"))
        executor.register_pipeline(self._make_pipeline("B"))
        names = executor.list_pipelines()
        assert set(names) == {"A", "B"}

    def test_list_pipelines_empty(self):
        """無已註冊 Pipeline 時應回傳空列表"""
        executor = PipelineExecutor()
        assert executor.list_pipelines() == []

    def test_get_pipeline_info(self):
        """get_pipeline_info 應回傳 Pipeline 資訊字典"""
        executor = PipelineExecutor()
        pipeline = self._make_pipeline("P1", steps=[DummySuccessStep(name="S1")])
        executor.register_pipeline(pipeline)

        info = executor.get_pipeline_info("P1")
        assert info is not None
        assert "config" in info
        assert "steps" in info
        assert "statistics" in info
        assert info["steps"] == ["S1"]

    def test_get_pipeline_info_not_found(self):
        """get_pipeline_info 未找到時應回傳 None"""
        executor = PipelineExecutor()
        assert executor.get_pipeline_info("Missing") is None

    @pytest.mark.asyncio
    async def test_execute_pipeline_success(self, sample_df):
        """execute_pipeline 成功執行應回傳 success=True"""
        executor = PipelineExecutor()
        pipeline = self._make_pipeline("P1", steps=[DummySuccessStep(name="S1")])
        executor.register_pipeline(pipeline)

        result = await executor.execute_pipeline("P1", sample_df, 202512)
        assert result["success"] is True
        assert "output_data" in result

    @pytest.mark.asyncio
    async def test_execute_pipeline_not_found(self, sample_df):
        """execute_pipeline 執行不存在的 Pipeline 應拋出 ValueError"""
        executor = PipelineExecutor()
        with pytest.raises(ValueError, match="not found"):
            await executor.execute_pipeline("NoSuch", sample_df, 202512)

    @pytest.mark.asyncio
    async def test_execute_pipeline_already_running(self, sample_df):
        """execute_pipeline 已在執行中應回傳 already running 錯誤"""
        executor = PipelineExecutor()
        pipeline = self._make_pipeline("P1", steps=[DummySuccessStep(name="S1")])
        executor.register_pipeline(pipeline)

        # 模擬 Pipeline 正在執行
        executor._running_pipelines.add("P1")
        result = await executor.execute_pipeline("P1", sample_df, 202512)
        assert result["success"] is False
        assert "already running" in result["error"]

    @pytest.mark.asyncio
    async def test_execute_pipeline_clears_running_on_completion(self, sample_df):
        """execute_pipeline 完成後應從 _running_pipelines 移除"""
        executor = PipelineExecutor()
        pipeline = self._make_pipeline("P1", steps=[DummySuccessStep(name="S1")])
        executor.register_pipeline(pipeline)

        await executor.execute_pipeline("P1", sample_df, 202512)
        assert "P1" not in executor._running_pipelines

    @pytest.mark.asyncio
    async def test_execute_pipeline_with_failed_step(self, sample_df):
        """execute_pipeline 含失敗步驟時 success 應為 False"""
        executor = PipelineExecutor()
        pipeline = self._make_pipeline("P1", steps=[DummyFailStep(name="Fail")])
        executor.register_pipeline(pipeline)

        result = await executor.execute_pipeline("P1", sample_df, 202512)
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_execute_multiple(self, sample_df):
        """execute_multiple 應執行多個 Pipeline 並回傳結果字典"""
        executor = PipelineExecutor()
        executor.register_pipeline(
            self._make_pipeline("A", steps=[DummySuccessStep(name="S1")])
        )
        executor.register_pipeline(
            self._make_pipeline("B", steps=[DummySuccessStep(name="S2")])
        )

        results = await executor.execute_multiple(["A", "B"], sample_df, 202512)
        assert "A" in results
        assert "B" in results
        assert results["A"]["success"] is True
        assert results["B"]["success"] is True

    @pytest.mark.asyncio
    async def test_execute_multiple_with_missing_pipeline(self, sample_df):
        """execute_multiple 含不存在的 Pipeline 應在該項結果記錄錯誤"""
        executor = PipelineExecutor()
        executor.register_pipeline(
            self._make_pipeline("A", steps=[DummySuccessStep(name="S1")])
        )

        results = await executor.execute_multiple(["A", "Missing"], sample_df, 202512)
        assert results["A"]["success"] is True
        assert results["Missing"]["success"] is False
