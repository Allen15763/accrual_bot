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

# 通用步驟（抽象基類與共用工具）
from .steps import (
    BaseLoadingStep,
    BaseERMEvaluationStep,
    BaseERMConditions,
    DataCleaningStep,
    DateFormattingStep,
    DateParsingStep,
    ValidationStep,
    ExportStep,
    DataIntegrationStep,
    ProductFilterStep,
    PreviousWorkpaperIntegrationStep,
    ProcurementIntegrationStep,
    DateLogicStep,
    StepMetadataBuilder,
    create_error_metadata,
    StatusEvaluationStep,
    AccountingAdjustmentStep,
    AccountCodeMappingStep,
    DepartmentConversionStep,
    BasePostProcessingStep,
    DataQualityCheckStep,
    StatisticsGenerationStep,
    create_post_processing_chain,
)

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
    'quick_test_step',

    # Generic Steps
    'BaseLoadingStep',
    'BaseERMEvaluationStep',
    'BaseERMConditions',
    'DataCleaningStep',
    'DateFormattingStep',
    'DateParsingStep',
    'ValidationStep',
    'ExportStep',
    'DataIntegrationStep',
    'ProductFilterStep',
    'PreviousWorkpaperIntegrationStep',
    'ProcurementIntegrationStep',
    'DateLogicStep',
    'StepMetadataBuilder',
    'create_error_metadata',
    'StatusEvaluationStep',
    'AccountingAdjustmentStep',
    'AccountCodeMappingStep',
    'DepartmentConversionStep',
    'BasePostProcessingStep',
    'DataQualityCheckStep',
    'StatisticsGenerationStep',
    'create_post_processing_chain',
]
