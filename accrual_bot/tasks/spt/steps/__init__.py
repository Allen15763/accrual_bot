"""
SPT Steps - SPT-specific pipeline steps

This module contains all SPT entity-specific processing steps.
"""

from .spt_loading import SPTDataLoadingStep, SPTPRDataLoadingStep
from .spt_steps import (
    SPTStatusStep,
    SPTDepartmentStep,
    SPTAccrualStep,
    SPTValidationStep,
    SPTPostProcessingStep,
)
from .spt_evaluation_erm import SPTERMLogicStep
from .spt_evaluation_affiliate import CommissionDataUpdateStep, PayrollDetectionStep
from .spt_evaluation_accountant import SPTStatusLabelStep
from .spt_account_prediction import AccountPredictionConditions, SPTAccountPredictionStep

# SPT Procurement steps
from .spt_procurement_loading import (
    SPTProcurementDataLoadingStep,
    SPTProcurementPRDataLoadingStep,
)
from .spt_procurement_mapping import ProcurementPreviousMappingStep
from .spt_procurement_evaluation import SPTProcurementStatusEvaluationStep
from .spt_column_initialization import ColumnInitializationStep
from .spt_procurement_validation import ProcurementPreviousValidationStep
from .spt_combined_procurement_loading import CombinedProcurementDataLoadingStep
from .spt_combined_procurement_processing import CombinedProcurementProcessingStep
from .spt_combined_procurement_export import CombinedProcurementExportStep

__all__ = [
    'SPTDataLoadingStep',
    'SPTPRDataLoadingStep',
    # SPTStatusStep, SPTDepartmentStep, SPTAccrualStep, SPTValidationStep 為遺留原型步驟，
    # 未整合至 pipeline orchestrator，已從公開 API 移除。保留在 spt_steps.py 供業務邏輯參考。
    'SPTPostProcessingStep',
    'SPTERMLogicStep',
    'CommissionDataUpdateStep',
    'PayrollDetectionStep',
    'SPTStatusLabelStep',
    'AccountPredictionConditions',
    'SPTAccountPredictionStep',
    # Procurement steps
    'SPTProcurementDataLoadingStep',
    'SPTProcurementPRDataLoadingStep',
    'ProcurementPreviousMappingStep',
    'SPTProcurementStatusEvaluationStep',
    'ColumnInitializationStep',
    'ProcurementPreviousValidationStep',
    # COMBINED Procurement steps
    'CombinedProcurementDataLoadingStep',
    'CombinedProcurementProcessingStep',
    'CombinedProcurementExportStep',
]
