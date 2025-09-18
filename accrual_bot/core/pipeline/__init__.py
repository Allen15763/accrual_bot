"""
Pipeline 步驟系統
提供可組合、可重用的數據處理管道
"""

from .base import (
    PipelineStep,
    ConditionalStep,
    ParallelStep,
    SequentialStep,
    StepResult,
    StepStatus
)

from .context import (
    ProcessingContext,
    ContextMetadata,
    ValidationResult
)

from .pipeline import (
    Pipeline,
    PipelineBuilder,
    PipelineExecutor
)

from .factory import (
    PipelineFactory,
    StepRegistry
)

__all__ = [
    # Base
    'PipelineStep',
    'ConditionalStep',
    'ParallelStep',
    'SequentialStep',
    'StepResult',
    'StepStatus',
    
    # Context
    'ProcessingContext',
    'ContextMetadata',
    'ValidationResult',
    
    # Pipeline
    'Pipeline',
    'PipelineBuilder',
    'PipelineExecutor',
    
    # Factory
    'PipelineFactory',
    'StepRegistry'
]
