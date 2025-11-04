"""
Pipeline 步驟實現
包含各種通用和實體特定的處理步驟
"""

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
    SPTValidationStep
)

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
from .spx_pr_evaluation import (
    PRERMConditions,
    SPXPRERMLogicStep
)
from .spx_exporting import SPXPRExportStep

# 基礎管道
from .spx_po_steps import (create_spx_po_complete_pipeline,
                          create_ppe_pipeline)
from .spx_steps import create_spx_pr_complete_pipeline

__all__ = [
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
    'PRERMConditions',
    'SPXPRERMLogicStep',
    'SPXPRExportStep',

    # Basic pipeline
    'create_spx_po_complete_pipeline',
    'create_ppe_pipeline',
    'create_spx_pr_complete_pipeline'
]
