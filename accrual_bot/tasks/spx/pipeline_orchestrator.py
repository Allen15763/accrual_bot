"""
SPX Pipeline Orchestrator

Manages SPX-specific pipeline configuration and construction.
Supports configuration-driven step loading for flexible pipeline composition.
"""

from typing import List, Dict, Any, Optional
from accrual_bot.core.pipeline import Pipeline, PipelineConfig
from accrual_bot.core.pipeline.base import PipelineStep
from accrual_bot.utils.config import config_manager

# Import SPX steps
from accrual_bot.tasks.spx.steps import (
    SPXDataLoadingStep,
    SPXPRDataLoadingStep,
    StatusStage1Step,
    SPXERMLogicStep,
    SPXPRERMLogicStep,
    DepositStatusUpdateStep,
    ColumnAdditionStep,
    ClosingListIntegrationStep,
    ValidationDataProcessingStep,
    SPXExportStep,
    SPXPRExportStep,
    PPEDescDataLoadingStep,
    DescriptionExtractionStep,
    ContractPeriodMappingStep,
    PPEDescExportStep,
)

# Import common steps
from accrual_bot.tasks.common import DataShapeSummaryStep

# Import shared steps from core
from accrual_bot.core.pipeline.steps import (
    ProductFilterStep,
    APInvoiceIntegrationStep,
    PreviousWorkpaperIntegrationStep,
    ProcurementIntegrationStep,
    DateLogicStep,
    DataReformattingStep,
    PRDataReformattingStep,
    PPEDataLoadingStep,
    PPEDataCleaningStep,
    PPEDataMergeStep,
    PPEContractDateUpdateStep,
    PPEMonthDifferenceStep,
)


class SPXPipelineOrchestrator:
    """
    SPX Pipeline 編排器

    功能:
    1. 根據配置動態創建 pipeline
    2. 支援 PO/PR 兩種處理類型
    3. 可選擇啟用/禁用特定步驟
    """

    def __init__(self):
        self.config = config_manager._config_toml.get('pipeline', {}).get('spx', {})
        self.entity_type = 'SPX'

    def build_po_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPX PO 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SPX_PO_Processing",
            description="SPX PO data processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 獲取啟用的步驟列表
        enabled_steps = self.config.get('enabled_po_steps', [])

        if not enabled_steps:
            # 默認步驟順序
            enabled_steps = [
                "SPXDataLoading",
                "ColumnAddition",
                "ClosingListIntegration",
                "StatusStage1",
                "SPXERMLogic",
                "DepositStatusUpdate",
                "ValidationDataProcessing",
                "SPXExport"
            ]

        # 動態添加步驟
        for step_name in enabled_steps:
            step = self._create_step(step_name, file_paths, processing_type='PO')
            if step:
                pipeline.add_step(step)

        # 添加自定義步驟
        if custom_steps:
            for step in custom_steps:
                pipeline.add_step(step)

        return pipeline

    def build_pr_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPX PR 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SPX_PR_Processing",
            description="SPX PR data processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 獲取啟用的步驟列表
        enabled_steps = self.config.get('enabled_pr_steps', [])

        if not enabled_steps:
            # 默認步驟順序
            enabled_steps = [
                "SPXPRDataLoading",
                "ColumnAddition",
                "StatusStage1",
                "SPXPRERMLogic",
                "SPXPRExport"
            ]

        # 動態添加步驟
        for step_name in enabled_steps:
            step = self._create_step(step_name, file_paths, processing_type='PR')
            if step:
                pipeline.add_step(step)

        # 添加自定義步驟
        if custom_steps:
            for step in custom_steps:
                pipeline.add_step(step)

        return pipeline

    def build_ppe_pipeline(
        self,
        file_paths: Dict[str, Any],
        processing_date: int,
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPX PPE (Property, Plant & Equipment) 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            processing_date: 處理日期 (YYYYMM)
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SPX_PPE_Processing",
            description="SPX PPE contract depreciation period calculation",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # PPE 管道的固定步驟順序
        contract_filing_list_path = file_paths.get('contract_filing_list', {})

        # 1. PPE Data Loading
        pipeline.add_step(PPEDataLoadingStep(
            name="PPEDataLoading",
            contract_filing_list_url=contract_filing_list_path
        ))

        # 2. PPE Data Cleaning
        pipeline.add_step(PPEDataCleaningStep(
            name="PPEDataCleaning"
        ))

        # 3. PPE Data Merge
        merge_keys = config_manager.get_list(
            'SPX',
            'key_for_merging_origin_and_renew_contract'
        )
        pipeline.add_step(PPEDataMergeStep(
            name="PPEDataMerge",
            merge_keys=merge_keys
        ))

        # 4. PPE Contract Date Update
        pipeline.add_step(PPEContractDateUpdateStep(
            name="PPEContractDateUpdate"
        ))

        # 5. PPE Month Difference Calculation
        pipeline.add_step(PPEMonthDifferenceStep(
            name="PPEMonthDifference",
            current_month=processing_date
        ))

        # 添加自定義步驟
        if custom_steps:
            for step in custom_steps:
                pipeline.add_step(step)

        return pipeline

    def build_ppe_desc_pipeline(
        self,
        file_paths: Dict[str, Any],
        processing_date: int,
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPX PPE_DESC 後處理 pipeline

        從 PO/PR 底稿提取品項說明摘要，並對應年限表。

        Args:
            file_paths: 文件路徑配置（workpaper, contract_periods）
            processing_date: 處理日期 (YYYYMM)，用於解析 sheet 名稱
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SPX_PPE_DESC_Processing",
            description="SPX PO/PR description extraction with contract period mapping",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 固定步驟順序
        # 1. 載入 PO/PR 底稿和年限表
        pipeline.add_step(PPEDescDataLoadingStep(
            name="PPEDescDataLoading",
            file_paths=file_paths,
            processing_date=processing_date
        ))

        # 2. 說明欄位提取（摘要、地址、智取櫃型號）
        pipeline.add_step(DescriptionExtractionStep(
            name="DescriptionExtraction"
        ))

        # 3. 年限對應
        pipeline.add_step(ContractPeriodMappingStep(
            name="ContractPeriodMapping"
        ))

        # 4. 匯出 3-sheet Excel
        pipeline.add_step(PPEDescExportStep(
            name="PPEDescExport"
        ))

        # 添加自定義步驟
        if custom_steps:
            for step in custom_steps:
                pipeline.add_step(step)

        return pipeline

    def _create_step(
        self,
        step_name: str,
        file_paths: Dict[str, Any],
        processing_type: str = 'PO'
    ) -> Optional[PipelineStep]:
        """
        根據步驟名稱創建步驟實例

        Args:
            step_name: 步驟名稱
            file_paths: 文件路徑配置
            processing_type: 處理類型 (PO/PR)

        Returns:
            Optional[PipelineStep]: 步驟實例或 None
        """
        step_registry = {
            # Data Loading
            'SPXDataLoading': lambda: SPXDataLoadingStep(
                name="SPXDataLoading",
                file_paths=file_paths
            ),
            'SPXPRDataLoading': lambda: SPXPRDataLoadingStep(
                name="SPXPRDataLoading",
                file_paths=file_paths
            ),

            # Data Preparation & Filtering
            'ProductFilter': lambda: ProductFilterStep(
                name="ProductFilter",
                product_pattern='(?i)LG_SPX',
                required=True
            ),
            'ColumnAddition': lambda: ColumnAdditionStep(
                name="ColumnAddition"
            ),

            # Data Integration
            'APInvoiceIntegration': lambda: APInvoiceIntegrationStep(
                name="APInvoiceIntegration",
                required=True
            ),
            'PreviousWorkpaperIntegration': lambda: PreviousWorkpaperIntegrationStep(
                name="PreviousWorkpaperIntegration",
                required=True
            ),
            'ProcurementIntegration': lambda: ProcurementIntegrationStep(
                name="ProcurementIntegration",
                required=True
            ),
            'ClosingListIntegration': lambda: ClosingListIntegrationStep(
                name="ClosingListIntegration"
            ),

            # Business Logic
            'DateLogic': lambda: DateLogicStep(
                name="DateLogic",
                required=True
            ),
            'StatusStage1': lambda: StatusStage1Step(
                name="StatusStage1"
            ),
            'SPXERMLogic': lambda: SPXERMLogicStep(
                name="SPXERMLogic"
            ),
            'SPXPRERMLogic': lambda: SPXPRERMLogicStep(
                name="SPXPRERMLogic"
            ),
            'DepositStatusUpdate': lambda: DepositStatusUpdateStep(
                name="DepositStatusUpdate"
            ),
            'ValidationDataProcessing': lambda: ValidationDataProcessingStep(
                name="ValidationDataProcessing"
            ),

            # Post Processing & Export
            'DataReformatting': lambda: DataReformattingStep(
                name="DataReformatting",
                required=True
            ),
            'PRDataReformatting': lambda: PRDataReformattingStep(
                name="PRDataReformatting",
                required=True
            ),
            'SPXExport': lambda: SPXExportStep(
                name="SPXExport"
            ),
            'SPXPRExport': lambda: SPXPRExportStep(
                name="SPXPRExport"
            ),

            # Data Shape Summary
            'DataShapeSummary': lambda: DataShapeSummaryStep(
                name="DataShapeSummary",
                export_excel=True,
                output_dir="output",
                required=False
            ),
        }

        step_factory = step_registry.get(step_name)
        if step_factory:
            return step_factory()
        else:
            # 記錄未知步驟
            print(f"Warning: Unknown step '{step_name}' for SPX {processing_type} pipeline")
            return None

    def get_enabled_steps(self, processing_type: str = 'PO') -> List[str]:
        """
        獲取啟用的步驟列表

        Args:
            processing_type: 處理類型 (PO/PR)

        Returns:
            List[str]: 步驟名稱列表
        """
        if processing_type == 'PO':
            return self.config.get('enabled_po_steps', [])
        elif processing_type == 'PR':
            return self.config.get('enabled_pr_steps', [])
        elif processing_type == 'PPE_DESC':
            return [
                'PPEDescDataLoading',
                'DescriptionExtraction',
                'ContractPeriodMapping',
                'PPEDescExport',
            ]
        else:
            return self.config.get('enabled_ppe_steps', [])
