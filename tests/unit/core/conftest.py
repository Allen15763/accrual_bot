"""Core 測試共用 fixtures"""
import pytest
from accrual_bot.core.pipeline.pipeline import Pipeline, PipelineConfig


@pytest.fixture
def pipeline_config():
    """基本 PipelineConfig"""
    return PipelineConfig(name="TestPipeline", entity_type="TEST")


@pytest.fixture
def basic_pipeline(pipeline_config, dummy_success_step):
    """含單一成功步驟的 Pipeline"""
    pipeline = Pipeline(pipeline_config)
    pipeline.add_step(dummy_success_step)
    return pipeline
