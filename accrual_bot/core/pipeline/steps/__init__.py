"""
Pipeline 步驟實現
包含各種通用和實體特定的處理步驟
"""

# 抽象基類
from .base_loading import BaseLoadingStep
from .base_evaluation import BaseERMEvaluationStep, BaseERMConditions

# 基礎步驟
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

# MOB特定步驟
from .mob_steps import (
    MOBStatusStep,
    MOBAccrualStep,
    MOBDepartmentStep,
    MOBValidationStep
)

# SPT特定步驟
from .spt_steps import (
    SPTStatusStep,
    SPTDepartmentStep,
    SPTAccrualStep,
    SPTValidationStep,
    SPTPostProcessingStep
)
from .spt_loading import SPTDataLoadingStep, SPTPRDataLoadingStep
from .spt_evaluation_affiliate import CommissionDataUpdateStep, PayrollDetectionStep
from .spt_evaluation_erm import SPTERMLogicStep
from .spt_evaluation_accountant import SPTStatusLabelStep
from .spt_account_prediction import AccountPredictionConditions, SPTAccountPredictionStep

# SPX特定步驟
from .spx_steps import (
    SPXDepositCheckStep,
    SPXClosingListIntegrationStep,
    SPXRentProcessingStep,
    SPXAssetValidationStep,
    SPXComplexStatusStep,
    SPXPPEProcessingStep
)
from .spx_loading import (
    SPXDataLoadingStep,
    PPEDataLoadingStep,
    AccountingOPSDataLoadingStep,
    SPXPRDataLoadingStep
)
from .spx_integration import (
    ColumnAdditionStep,
    APInvoiceIntegrationStep,
    ClosingListIntegrationStep,
    ValidationDataProcessingStep,
    DataReformattingStep,
    PRDataReformattingStep,
    PPEDataCleaningStep,
    PPEDataMergeStep
)
from .spx_evaluation import (StatusStage1Step,
                             ERMConditions,
                             SPXERMLogicStep,
                             PPEContractDateUpdateStep,
                             PPEMonthDifferenceStep)
from .spx_evaluation_2 import DepositStatusUpdateStep
from .spx_exporting import SPXExportStep, AccountingOPSExportingStep

from .spx_ppe_qty_validation import AccountingOPSValidationStep

# SPX PR 專用步驟
from .spx_pr_evaluation import SPXPRERMLogicStep
from .spx_exporting import SPXPRExportStep

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
    
    # MOB
    'MOBStatusStep',
    'MOBAccrualStep',
    'MOBDepartmentStep',
    'MOBValidationStep',
    
    # SPT
    'SPTStatusStep',
    'SPTDepartmentStep',
    'SPTAccrualStep',
    'SPTValidationStep',
    'SPTPostProcessingStep',
    
    'SPTDataLoadingStep',
    'SPTPRDataLoadingStep',
    'CommissionDataUpdateStep',
    'PayrollDetectionStep',
    'SPTERMLogicStep',
    'SPTStatusLabelStep',
    'AccountPredictionConditions',
    'SPTAccountPredictionStep',
    
    # SPX
    'SPXDepositCheckStep',
    'SPXClosingListIntegrationStep',
    'SPXRentProcessingStep',
    'SPXAssetValidationStep',
    'SPXComplexStatusStep',
    'SPXPPEProcessingStep',
    'SPXDataLoadingStep',
    'PPEDataLoadingStep',
    'AccountingOPSDataLoadingStep',
    'SPXPRDataLoadingStep',
    'ColumnAdditionStep',
    'APInvoiceIntegrationStep',
    'ClosingListIntegrationStep',
    'ValidationDataProcessingStep',
    'DataReformattingStep',
    'PRDataReformattingStep',
    'PPEDataCleaningStep',
    'PPEDataMergeStep',
    'StatusStage1Step',
    'ERMConditions',
    'SPXERMLogicStep',
    'PPEContractDateUpdateStep',
    'PPEMonthDifferenceStep',
    'DepositStatusUpdateStep',
    'SPXExportStep',
    'AccountingOPSExportingStep',
    'AccountingOPSValidationStep',

    
    # SPX PR Logic
    'SPXPRERMLogicStep',
    'SPXPRExportStep',
    
    # Post Processing
    'BasePostProcessingStep',
    'DataQualityCheckStep',
    'StatisticsGenerationStep',
    'create_post_processing_chain',
]
