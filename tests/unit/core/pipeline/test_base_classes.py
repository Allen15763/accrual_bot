"""
Pipeline 基類單元測試
測試 StepStatus, StepResult, PipelineStep.__call__,
ConditionalStep, ParallelStep, SequentialStep
"""

import asyncio
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock

from accrual_bot.core.pipeline.base import (
    StepStatus,
    StepResult,
    PipelineStep,
    ConditionalStep,
    ParallelStep,
    SequentialStep,
)
from accrual_bot.core.pipeline.context import ProcessingContext


# ---------------------------------------------------------------------------
# 測試用具體子類
# ---------------------------------------------------------------------------

class DummySuccessStep(PipelineStep):
    """永遠成功的步驟"""

    async def execute(self, context):
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS, message="ok")

    async def validate_input(self, context):
        return True


class DummyFailStep(PipelineStep):
    """永遠失敗（拋例外）的步驟"""

    async def execute(self, context):
        raise ValueError("deliberate failure")

    async def validate_input(self, context):
        return True


class DummySkipStep(PipelineStep):
    """驗證失敗且 required=False → 應被跳過"""

    def __init__(self, name="skip_step"):
        super().__init__(name=name, required=False)

    async def execute(self, context):
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS)

    async def validate_input(self, context):
        return False


class CountingStep(PipelineStep):
    """記錄被呼叫次數的步驟（用於重試測試）"""

    def __init__(self, name="counting", fail_times=0, **kwargs):
        super().__init__(name=name, **kwargs)
        self.call_count = 0
        self.fail_times = fail_times

    async def execute(self, context):
        self.call_count += 1
        if self.call_count <= self.fail_times:
            raise RuntimeError(f"fail #{self.call_count}")
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS)

    async def validate_input(self, context):
        return True


class SlowStep(PipelineStep):
    """執行時間超長的步驟（用於 timeout 測試）"""

    async def execute(self, context):
        await asyncio.sleep(10)
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS)

    async def validate_input(self, context):
        return True


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ctx():
    """最小化的 ProcessingContext"""
    return ProcessingContext(
        data=pd.DataFrame({"a": [1, 2]}),
        entity_type="TEST",
        processing_date=202601,
        processing_type="PO",
    )


# ===========================================================================
# TestStepStatus
# ===========================================================================

@pytest.mark.unit
class TestStepStatus:

    def test_enum_members(self):
        """StepStatus 應包含 6 個成員"""
        expected = {"PENDING", "RUNNING", "SUCCESS", "FAILED", "SKIPPED", "RETRY"}
        assert set(StepStatus.__members__.keys()) == expected

    def test_enum_values(self):
        """各成員的字串值正確"""
        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.SUCCESS.value == "success"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"
        assert StepStatus.RETRY.value == "retry"


# ===========================================================================
# TestStepResult
# ===========================================================================

@pytest.mark.unit
class TestStepResult:

    def test_is_success_property(self):
        r = StepResult(step_name="s", status=StepStatus.SUCCESS)
        assert r.is_success is True
        assert r.is_failed is False
        assert r.is_skipped is False

    def test_is_failed_property(self):
        r = StepResult(step_name="s", status=StepStatus.FAILED)
        assert r.is_failed is True
        assert r.is_success is False

    def test_is_skipped_property(self):
        r = StepResult(step_name="s", status=StepStatus.SKIPPED)
        assert r.is_skipped is True
        assert r.is_success is False

    def test_to_dict_basic(self):
        r = StepResult(
            step_name="step1",
            status=StepStatus.SUCCESS,
            message="done",
            duration=1.23,
            metadata={"rows": 100},
        )
        d = r.to_dict()
        assert d["step_name"] == "step1"
        assert d["status"] == "success"
        assert d["message"] == "done"
        assert d["duration"] == 1.23
        assert d["metadata"] == {"rows": 100}
        assert d["error"] is None

    def test_to_dict_with_error(self):
        err = ValueError("bad")
        r = StepResult(step_name="s", status=StepStatus.FAILED, error=err)
        d = r.to_dict()
        assert d["error"] == "bad"
        assert d["status"] == "failed"


# ===========================================================================
# TestPipelineStepCall
# ===========================================================================

@pytest.mark.unit
class TestPipelineStepCall:

    @pytest.mark.asyncio
    async def test_success_path(self, ctx):
        """成功路徑：validate → execute → 回傳 SUCCESS，duration > 0"""
        step = DummySuccessStep(name="ok_step")
        result = await step(ctx)
        assert result.is_success
        assert result.duration >= 0

    @pytest.mark.asyncio
    async def test_validation_fail_required_returns_failed(self, ctx):
        """required 步驟驗證失敗 → 外層 try/except 捕獲 ValueError → FAILED"""
        class RequiredBadValidation(PipelineStep):
            async def execute(self, context):
                return StepResult(step_name=self.name, status=StepStatus.SUCCESS)
            async def validate_input(self, context):
                return False

        step = RequiredBadValidation(name="rbv", required=True)
        result = await step(ctx)
        # ValueError 被外層 except 捕獲，回傳 FAILED
        assert result.is_failed
        assert "validation failed" in result.message.lower()

    @pytest.mark.asyncio
    async def test_validation_fail_optional_returns_skipped(self, ctx):
        """非必需步驟驗證失敗 → SKIPPED"""
        step = DummySkipStep()
        result = await step(ctx)
        assert result.is_skipped
        assert result.message == "Input validation failed"

    @pytest.mark.asyncio
    async def test_retry_then_succeed(self, ctx):
        """前 2 次失敗、第 3 次成功（retry_count=2）"""
        step = CountingStep(name="retry_step", fail_times=2, retry_count=2)
        result = await step(ctx)
        assert result.is_success
        assert step.call_count == 3

    @pytest.mark.asyncio
    async def test_retry_exhausted_required_returns_failed(self, ctx):
        """重試用盡 + required → rollback + raise → 外層 except 回傳 FAILED"""
        step = CountingStep(name="exhaust", fail_times=5, retry_count=1, required=True)
        result = await step(ctx)
        assert result.is_failed
        assert step.call_count == 2  # 1 + 1 retry

    @pytest.mark.asyncio
    async def test_retry_exhausted_optional_returns_failed(self, ctx):
        """重試用盡 + not required → FAILED（不 raise）"""
        step = CountingStep(name="exhaust_opt", fail_times=5, retry_count=1, required=False)
        result = await step(ctx)
        assert result.is_failed
        assert step.call_count == 2

    @pytest.mark.asyncio
    async def test_timeout_returns_failed(self, ctx):
        """timeout 觸發 → 最終 FAILED"""
        step = SlowStep(name="slow", timeout=0.05, required=False, retry_count=0)
        result = await step(ctx)
        assert result.is_failed

    @pytest.mark.asyncio
    async def test_prerequisites_and_post_actions(self, ctx):
        """前置 / 後置動作被呼叫"""
        pre_called = []
        post_called = []

        async def pre(context):
            pre_called.append(True)

        async def post(context):
            post_called.append(True)

        step = DummySuccessStep(name="with_hooks")
        step.add_prerequisite(pre)
        step.add_post_action(post)

        result = await step(ctx)
        assert result.is_success
        assert len(pre_called) == 1
        assert len(post_called) == 1

    @pytest.mark.asyncio
    async def test_duration_is_set(self, ctx):
        """結果中 duration 被正確設定"""
        step = DummySuccessStep(name="dur")
        result = await step(ctx)
        assert isinstance(result.duration, float)
        assert result.duration >= 0


# ===========================================================================
# TestConditionalStep
# ===========================================================================

@pytest.mark.unit
class TestConditionalStep:

    @pytest.mark.asyncio
    async def test_condition_true_executes_true_step(self, ctx):
        true_step = DummySuccessStep(name="true_branch")
        cond = ConditionalStep(
            name="cond",
            condition=lambda c: True,
            true_step=true_step,
        )
        result = await cond(ctx)
        assert result.is_success
        assert result.step_name == "true_branch"

    @pytest.mark.asyncio
    async def test_condition_false_executes_false_step(self, ctx):
        true_step = DummySuccessStep(name="true_branch")
        false_step = DummySuccessStep(name="false_branch")
        cond = ConditionalStep(
            name="cond",
            condition=lambda c: False,
            true_step=true_step,
            false_step=false_step,
        )
        result = await cond(ctx)
        assert result.is_success
        assert result.step_name == "false_branch"

    @pytest.mark.asyncio
    async def test_condition_false_no_false_step_skipped(self, ctx):
        true_step = DummySuccessStep(name="true_branch")
        cond = ConditionalStep(
            name="cond",
            condition=lambda c: False,
            true_step=true_step,
            false_step=None,
        )
        result = await cond(ctx)
        assert result.is_skipped

    @pytest.mark.asyncio
    async def test_validate_input_always_true(self, ctx):
        cond = ConditionalStep(
            name="cond",
            condition=lambda c: True,
            true_step=DummySuccessStep(name="t"),
        )
        assert await cond.validate_input(ctx) is True


# ===========================================================================
# TestParallelStep
# ===========================================================================

@pytest.mark.unit
class TestParallelStep:

    @pytest.mark.asyncio
    async def test_all_success(self, ctx):
        steps = [DummySuccessStep(name=f"p{i}") for i in range(3)]
        par = ParallelStep(name="par", steps=steps)
        result = await par(ctx)
        assert result.is_success
        assert "3" in result.message

    @pytest.mark.asyncio
    async def test_one_failure_no_fail_fast(self, ctx):
        """不啟用 fail_fast 時，等全部完成再回報 FAILED"""
        steps = [
            DummySuccessStep(name="ok1"),
            DummyFailStep(name="bad", required=False),
            DummySuccessStep(name="ok2"),
        ]
        par = ParallelStep(name="par", steps=steps)
        result = await par(ctx)
        assert result.is_failed
        assert "bad" in result.message or "bad" in str(result.metadata.get("failed", []))

    @pytest.mark.asyncio
    async def test_validate_input_empty_steps(self, ctx):
        par = ParallelStep(name="empty_par", steps=[])
        valid = await par.validate_input(ctx)
        assert valid is False

    @pytest.mark.asyncio
    async def test_validate_input_non_empty(self, ctx):
        par = ParallelStep(name="par", steps=[DummySuccessStep(name="s")])
        valid = await par.validate_input(ctx)
        assert valid is True


# ===========================================================================
# TestSequentialStep
# ===========================================================================

@pytest.mark.unit
class TestSequentialStep:

    @pytest.mark.asyncio
    async def test_all_success(self, ctx):
        steps = [DummySuccessStep(name=f"s{i}") for i in range(3)]
        seq = SequentialStep(name="seq", steps=steps)
        result = await seq(ctx)
        assert result.is_success
        assert "3" in result.message

    @pytest.mark.asyncio
    async def test_stop_on_failure_true(self, ctx):
        """stop_on_failure=True 時，第一個失敗即停止"""
        steps = [
            DummySuccessStep(name="ok"),
            DummyFailStep(name="bad", required=False),
            DummySuccessStep(name="never"),
        ]
        seq = SequentialStep(name="seq", steps=steps, stop_on_failure=True)
        result = await seq(ctx)
        assert result.is_failed
        assert "bad" in result.message
        # "never" 不應出現在已完成列表
        completed = result.metadata.get("completed", [])
        assert "never" not in completed

    @pytest.mark.asyncio
    async def test_stop_on_failure_false_continues(self, ctx):
        """stop_on_failure=False 時，失敗後繼續執行，最終仍 FAILED"""
        steps = [
            DummySuccessStep(name="ok1"),
            DummyFailStep(name="bad", required=False),
            DummySuccessStep(name="ok2"),
        ]
        seq = SequentialStep(name="seq", steps=steps, stop_on_failure=False)
        result = await seq(ctx)
        assert result.is_failed
        assert "bad" in result.message

    @pytest.mark.asyncio
    async def test_validate_input_empty_steps(self, ctx):
        seq = SequentialStep(name="empty_seq", steps=[])
        valid = await seq.validate_input(ctx)
        assert valid is False

    @pytest.mark.asyncio
    async def test_validate_input_non_empty(self, ctx):
        seq = SequentialStep(name="seq", steps=[DummySuccessStep(name="s")])
        valid = await seq.validate_input(ctx)
        assert valid is True
