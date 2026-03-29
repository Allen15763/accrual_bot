"""
Pipeline 步驟實現
包含通用、可跨實體複用的處理步驟

實體特定步驟已移至：
  - accrual_bot.tasks.spt.steps  （SPT 相關步驟）
  - accrual_bot.tasks.spx.steps  （SPX 相關步驟）

向後兼容：各實體步驟的 shim 檔案仍在此目錄，
可透過 from accrual_bot.core.pipeline.steps.spt_loading import ... 等路徑直接匯入。
"""

# 抽象基類
from .base_loading import BaseLoadingStep
from .base_evaluation import BaseERMEvaluationStep, BaseERMConditions

# 基礎通用步驟
from .common import (
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
    create_error_metadata
)

# 業務邏輯步驟
from .business import (
    StatusEvaluationStep,
    AccountingAdjustmentStep,
    AccountCodeMappingStep,
    DepartmentConversionStep
)

# 通用後處理步驟
from .post_processing import (
    BasePostProcessingStep,
    DataQualityCheckStep,
    StatisticsGenerationStep,
    create_post_processing_chain
)

__all__ = [
    # Base Classes
    'BaseLoadingStep',
    'BaseERMEvaluationStep',
    'BaseERMConditions',

    # Common
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

    # Business
    'StatusEvaluationStep',
    'AccountingAdjustmentStep',
    'AccountCodeMappingStep',
    'DepartmentConversionStep',

    # Post Processing
    'BasePostProcessingStep',
    'DataQualityCheckStep',
    'StatisticsGenerationStep',
    'create_post_processing_chain',
]
