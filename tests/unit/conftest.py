"""Unit 測試共用 fixtures"""
import pytest
import pandas as pd
from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


class DummySuccessStep(PipelineStep):
    """測試用：總是成功的步驟"""

    async def execute(self, context: ProcessingContext) -> StepResult:
        return StepResult(
            step_name=self.name,
            status=StepStatus.SUCCESS,
            message="Success",
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


class DummyFailStep(PipelineStep):
    """測試用：總是失敗的步驟"""

    async def execute(self, context: ProcessingContext) -> StepResult:
        raise ValueError(f"Step {self.name} intentional failure")

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


class DummySkipStep(PipelineStep):
    """測試用：驗證不通過會被跳過的步驟（required=False）"""

    async def execute(self, context: ProcessingContext) -> StepResult:
        return StepResult(
            step_name=self.name,
            status=StepStatus.SUCCESS,
            message="Should not reach here",
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return False


class DummyContextModifyStep(PipelineStep):
    """測試用：會修改 context 的步驟"""

    def __init__(self, name: str, key: str, value, **kwargs):
        super().__init__(name, **kwargs)
        self._key = key
        self._value = value

    async def execute(self, context: ProcessingContext) -> StepResult:
        context.set_variable(self._key, self._value)
        return StepResult(
            step_name=self.name,
            status=StepStatus.SUCCESS,
            message=f"Set {self._key}={self._value}",
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return True


@pytest.fixture
def dummy_success_step():
    """返回 SUCCESS 的 concrete step"""
    return DummySuccessStep(name="DummySuccess")


@pytest.fixture
def dummy_fail_step():
    """返回 FAILED 的 concrete step（required=True）"""
    return DummyFailStep(name="DummyFail")


@pytest.fixture
def dummy_skip_step():
    """驗證不通過、required=False 的步驟"""
    return DummySkipStep(name="DummySkip", required=False)


@pytest.fixture
def make_success_step():
    """工廠 fixture：產生帶自訂名稱的成功步驟"""
    def _make(name: str = "Success") -> DummySuccessStep:
        return DummySuccessStep(name=name)
    return _make


@pytest.fixture
def make_fail_step():
    """工廠 fixture：產生帶自訂名稱的失敗步驟"""
    def _make(name: str = "Fail") -> DummyFailStep:
        return DummyFailStep(name=name)
    return _make
