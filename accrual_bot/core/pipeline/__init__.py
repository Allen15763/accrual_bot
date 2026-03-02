"""
Pipeline 模組
提供靈活的數據處理Pipeline架構
"""

# 基礎組件
from .base import (
    PipelineStep,
    StepResult,
    StepStatus,
    ConditionalStep,
    ParallelStep,
    SequentialStep
)

# 上下文管理
from .context import (
    ProcessingContext,
    ValidationResult,
    ContextMetadata
)

# Pipeline主類
from .pipeline import (
    Pipeline,
    PipelineBuilder,
    PipelineConfig,
    PipelineExecutor
)

# checkpoint
from .checkpoint import (
    CheckpointManager,
    PipelineWithCheckpoint,
    execute_pipeline_with_checkpoint,
    resume_from_step,
    quick_test_step
)

# 步驟
from .steps import *

__all__ = [
    # Base
    'PipelineStep',
    'StepResult',
    'StepStatus',
    'ConditionalStep',
    'ParallelStep',
    'SequentialStep',

    # Context
    'ProcessingContext',
    'ValidationResult',
    'ContextMetadata',

    # Pipeline
    'Pipeline',
    'PipelineBuilder',
    'PipelineConfig',
    'PipelineExecutor',

    # checkpoint
    'CheckpointManager',
    'PipelineWithCheckpoint',
    'execute_pipeline_with_checkpoint',
    'resume_from_step',
    'quick_test_step'
]
