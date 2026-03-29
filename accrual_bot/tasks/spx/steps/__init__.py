"""
SPX Steps - SPX-specific pipeline steps

This module contains all SPX entity-specific processing steps.
"""

from .spx_loading import (
    SPXDataLoadingStep,
    PPEDataLoadingStep,
    AccountingOPSDataLoadingStep,
    SPXPRDataLoadingStep,
)
from .spx_steps import (
    SPXDepositCheckStep,
    SPXClosingListIntegrationStep,
    SPXRentProcessingStep,
    SPXAssetValidationStep,
    SPXComplexStatusStep,
    SPXPPEProcessingStep,
)
from .spx_evaluation import (
    StatusStage1Step,
    ERMConditions,
    SPXERMLogicStep,
    PPEContractDateUpdateStep,
    PPEMonthDifferenceStep,
)
from .spx_evaluation_2 import DepositStatusUpdateStep
from .spx_pr_evaluation import SPXPRERMLogicStep
from .spx_integration import (
    ColumnAdditionStep,
    APInvoiceIntegrationStep,
    ClosingListIntegrationStep,
    ValidationDataProcessingStep,
    DataReformattingStep,
    PRDataReformattingStep,
    PPEDataCleaningStep,
    PPEDataMergeStep,
)
from .spx_exporting import (
    SPXExportStep,
    AccountingOPSExportingStep,
    SPXPRExportStep,
)
from .spx_ppe_qty_validation import AccountingOPSValidationStep
from .spx_ppe_desc import (
    PPEDescDataLoadingStep,
    DescriptionExtractionStep,
    ContractPeriodMappingStep,
    PPEDescExportStep,
)

__all__ = [
    'SPXDataLoadingStep',
    'PPEDataLoadingStep',
    'AccountingOPSDataLoadingStep',
    'SPXPRDataLoadingStep',
    # 注意：SPXDepositCheckStep、SPXClosingListIntegrationStep、SPXRentProcessingStep、
    # SPXAssetValidationStep、SPXComplexStatusStep、SPXPPEProcessingStep
    # 為遺留原型步驟，未整合至 pipeline orchestrator，保留供業務邏輯參考，不在公開 API 中。
    'StatusStage1Step',
    'ERMConditions',
    'SPXERMLogicStep',
    'PPEContractDateUpdateStep',
    'PPEMonthDifferenceStep',
    'DepositStatusUpdateStep',
    'SPXPRERMLogicStep',
    'ColumnAdditionStep',
    'APInvoiceIntegrationStep',
    'ClosingListIntegrationStep',
    'ValidationDataProcessingStep',
    'DataReformattingStep',
    'PRDataReformattingStep',
    'PPEDataCleaningStep',
    'PPEDataMergeStep',
    'SPXExportStep',
    'AccountingOPSExportingStep',
    'SPXPRExportStep',
    'AccountingOPSValidationStep',
    'PPEDescDataLoadingStep',
    'DescriptionExtractionStep',
    'ContractPeriodMappingStep',
    'PPEDescExportStep',
]
