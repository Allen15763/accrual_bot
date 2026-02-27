"""
SPT Pipeline Orchestrator

Manages SPT-specific pipeline configuration and construction.
Supports configuration-driven step loading for flexible pipeline composition.
"""

from typing import List, Dict, Any, Optional
from accrual_bot.core.pipeline import Pipeline, PipelineConfig
from accrual_bot.core.pipeline.base import PipelineStep
from accrual_bot.utils.config import config_manager

# Import SPT steps
from accrual_bot.tasks.spt.steps import (
    SPTDataLoadingStep,
    SPTPRDataLoadingStep,
    SPTERMLogicStep,
    SPTStatusLabelStep,
    SPTAccountPredictionStep,
    CommissionDataUpdateStep,
    PayrollDetectionStep,
    # Procurement steps
    SPTProcurementDataLoadingStep,
    SPTProcurementPRDataLoadingStep,
    ProcurementPreviousMappingStep,
    SPTProcurementStatusEvaluationStep,
    ColumnInitializationStep,
    ProcurementPreviousValidationStep,
    # COMBINED Procurement steps
    CombinedProcurementDataLoadingStep,
    CombinedProcurementProcessingStep,
    CombinedProcurementExportStep,
)

from accrual_bot.tasks.spx.steps import (
    SPXPRExportStep,  # SPT 使用 SPX 的 Export (內容相同)
    SPXPRERMLogicStep,
)

# Import common steps
from accrual_bot.tasks.common import DataShapeSummaryStep

# Import shared steps from core
from accrual_bot.core.pipeline.steps import (
    ProductFilterStep,
    ColumnAdditionStep,
    APInvoiceIntegrationStep,
    PreviousWorkpaperIntegrationStep,
    ProcurementIntegrationStep,
    DateLogicStep,
    SPTPostProcessingStep,
)


class SPTPipelineOrchestrator:
    """
    SPT Pipeline 編排器

    功能:
    1. 根據配置動態創建 pipeline
    2. 支援 PO/PR/PROCUREMENT 三種處理類型
    3. 可選擇啟用/禁用特定步驟
    4. PROCUREMENT 類型支援 PO、PR 單獨處理或合併處理
    """

    def __init__(self):
        self.config = config_manager._config_toml.get('pipeline', {}).get('spt', {})
        self.entity_type = 'SPT'

    def build_po_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPT PO 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SPT_PO_Processing",
            description="SPT PO data processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 獲取啟用的步驟列表
        enabled_steps = self.config.get('enabled_po_steps', [])

        if not enabled_steps:
            # 默認步驟順序
            enabled_steps = [
                "SPTDataLoading",
                "CommissionDataUpdate",
                "PayrollDetection",
                "SPTERMLogic",
                "SPTStatusLabel",
                "SPTAccountPrediction"
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
        構建 SPT PR 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SPT_PR_Processing",
            description="SPT PR data processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 獲取啟用的步驟列表
        enabled_steps = self.config.get('enabled_pr_steps', [])

        if not enabled_steps:
            # 默認步驟順序
            enabled_steps = [
                "SPTPRDataLoading",
                "CommissionDataUpdate",
                "PayrollDetection",
                "SPTERMLogic",
                "SPTStatusLabel",
                "SPTAccountPrediction"
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

    def _normalize_procurement_paths(
        self,
        file_paths: Dict[str, Any],
        source_type: str
    ) -> Dict[str, Any]:
        """
        根據 source_type 正規化採購檔案路徑

        將 procurement_previous_po/pr 統一為 procurement_previous，
        並移除不需要的原始資料檔案路徑

        Args:
            file_paths: 原始檔案路徑配置 (從 paths.toml 載入)
            source_type: 處理類型 ('PO', 'PR', 'COMBINED')

        Returns:
            Dict[str, Any]: 正規化後的檔案路徑
        """
        paths = file_paths.copy()

        if source_type == 'PO':
            # 移除 PR 相關路徑
            paths.pop('raw_pr', None)
            paths.pop('procurement_previous_pr', None)
            # 重命名 procurement_previous_po -> procurement_previous
            if 'procurement_previous_po' in paths:
                paths['procurement_previous'] = paths.pop('procurement_previous_po')

        elif source_type == 'PR':
            # 移除 PO 相關路徑
            paths.pop('raw_po', None)
            paths.pop('procurement_previous_po', None)
            # 重命名 procurement_previous_pr -> procurement_previous
            if 'procurement_previous_pr' in paths:
                paths['procurement_previous'] = paths.pop('procurement_previous_pr')

        # COMBINED: 保留所有路徑，不做處理

        return paths

    def build_procurement_pipeline(
        self,
        file_paths: Dict[str, Any],
        source_type: str = 'PO',
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPT 採購處理 pipeline

        Args:
            file_paths: 檔案路徑配置 (從 paths.toml 載入，會自動正規化)
            source_type: 處理類型
                - 'PO': 僅處理 PO
                - 'PR': 僅處理 PR
                - 'COMBINED': 同時處理 PO 和 PR
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        # 正規化檔案路徑
        normalized_paths = self._normalize_procurement_paths(file_paths, source_type)

        pipeline_config = PipelineConfig(
            name=f"SPT_PROCUREMENT_{source_type}_Processing",
            description=f"SPT Procurement {source_type} processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        if source_type == 'COMBINED':
            # COMBINED 模式: 同時處理 PO 和 PR
            enabled_steps = self.config.get('enabled_procurement_combined_steps', [])

            if not enabled_steps:
                # 默認步驟順序
                enabled_steps = [
                    "CombinedProcurementDataLoading",
                    "ProcurementPreviousValidation",
                    "CombinedProcurementProcessing",
                    "CombinedProcurementExport",
                ]

            # 動態添加步驟
            for step_name in enabled_steps:
                step = self._create_step(
                    step_name,
                    normalized_paths,
                    processing_type='PROCUREMENT',
                    source_type='COMBINED'
                )
                if step:
                    pipeline.add_step(step)

            # 添加自定義步驟
            if custom_steps:
                for step in custom_steps:
                    pipeline.add_step(step)

            return pipeline

        # 單一類型處理 (PO 或 PR)
        if source_type == 'PO':
            enabled_steps = self.config.get('enabled_procurement_po_steps', [])
        elif source_type == 'PR':
            enabled_steps = self.config.get('enabled_procurement_pr_steps', [])
        else:
            raise ValueError(f"Invalid source_type: {source_type}. Must be 'PO', 'PR', or 'COMBINED'")

        if not enabled_steps:
            # 默認步驟順序
            if source_type == 'PO':
                enabled_steps = [
                    "SPTProcurementDataLoading",
                    "ColumnInitialization",
                    "ProcurementPreviousMapping",
                    "DateLogic",
                    "SPTProcurementStatusEvaluation",
                    "SPTProcurementExport",
                ]
            else:  # PR
                enabled_steps = [
                    "SPTProcurementPRDataLoading",
                    "ColumnInitialization",
                    "ProcurementPreviousMapping",
                    "DateLogic",
                    "SPTProcurementStatusEvaluation",
                    "SPTProcurementExport",
                ]

        # 動態添加步驟
        for step_name in enabled_steps:
            step = self._create_step(
                step_name,
                normalized_paths,
                processing_type='PROCUREMENT',
                source_type=source_type
            )
            if step:
                pipeline.add_step(step)

        # 添加自定義步驟
        if custom_steps:
            for step in custom_steps:
                pipeline.add_step(step)

        return pipeline

    def _create_step(
        self,
        step_name: str,
        file_paths: Dict[str, Any],
        processing_type: str = 'PO',
        source_type: str = None
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
            'SPTDataLoading': lambda: SPTDataLoadingStep(
                name="SPTDataLoading",
                file_paths=file_paths
            ),
            'SPTPRDataLoading': lambda: SPTPRDataLoadingStep(
                name="SPTPRDataLoading",
                file_paths=file_paths
            ),

            # Data Preparation & Filtering
            'ProductFilter': lambda: ProductFilterStep(
                name="ProductFilter",
                product_pattern='(?i)SPX',
                exclude=True,
                required=True
            ),
            'ColumnAddition': lambda: ColumnAdditionStep(
                name="ColumnAddition",
                required=True
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

            # Business Logic - SPT Specific
            'CommissionDataUpdate': lambda: CommissionDataUpdateStep(
                name="CommissionDataUpdate",
                status_column="PR狀態" if processing_type == 'PR' else "PO狀態",
                required=True
            ),
            'PayrollDetection': lambda: PayrollDetectionStep(
                name="PayrollDetection",
                required=True
            ),
            'DateLogic': lambda: DateLogicStep(
                name="DateLogic",
                required=True
            ),
            'SPTERMLogic': lambda: SPTERMLogicStep(
                name="SPTERMLogic",
                required=True
            ),
            'SPXPRERMLogic': lambda: SPXPRERMLogicStep(
                name="SPXPRERMLogic",
                required=True
            ),
            'SPTStatusLabel': lambda: SPTStatusLabelStep(
                name="SPTStatusLabel",
                status_column="PR狀態" if processing_type == 'PR' else "PO狀態",
                remark_column="Remarked by FN"
            ),
            'SPTAccountPrediction': lambda: SPTAccountPredictionStep(
                name="SPTAccountPrediction",
                required=True
            ),

            # Post Processing & Export
            'SPTPostProcessing': lambda: SPTPostProcessingStep(
                name="SPTPostProcessing",
                required=True
            ),
            'SPTExport': lambda: SPXPRExportStep(
                name="SPTExport",
                output_dir="output",
                sheet_name="PR" if processing_type == 'PR' else "PO",
                include_index=False,
                required=True,
                retry_count=0
            ),

            # PROCUREMENT 步驟
            'SPTProcurementDataLoading': lambda: SPTProcurementDataLoadingStep(
                name="SPTProcurementDataLoading",
                file_paths=file_paths
            ),
            'SPTProcurementPRDataLoading': lambda: SPTProcurementPRDataLoadingStep(
                name="SPTProcurementPRDataLoading",
                file_paths=file_paths
            ),
            'ColumnInitialization': lambda: ColumnInitializationStep(
                name="ColumnInitialization",
                status_column="PR狀態" if source_type == 'PR' else "PO狀態"
            ),
            'ProcurementPreviousMapping': lambda: ProcurementPreviousMappingStep(
                name="ProcurementPreviousMapping"
            ),
            'SPTProcurementStatusEvaluation': lambda: SPTProcurementStatusEvaluationStep(
                name="SPTProcurementStatusEvaluation",
                status_column="PR狀態" if source_type == 'PR' else "PO狀態"
            ),
            'SPTProcurementExport': lambda: SPXPRExportStep(
                name="SPTProcurementExport",
                output_dir="output",
                sheet_name=source_type if source_type else "PO",
                include_index=False,
                required=True,
                retry_count=0
            ),

            # COMBINED PROCUREMENT 步驟
            'CombinedProcurementDataLoading': lambda: CombinedProcurementDataLoadingStep(
                name="CombinedProcurementDataLoading",
                file_paths=file_paths
            ),
            'ProcurementPreviousValidation': lambda: ProcurementPreviousValidationStep(
                name="ProcurementPreviousValidation",
                strict_mode=False  # 寬鬆模式，驗證失敗不中斷 pipeline
            ),
            'CombinedProcurementProcessing': lambda: CombinedProcurementProcessingStep(
                name="CombinedProcurementProcessing"
            ),
            'CombinedProcurementExport': lambda: CombinedProcurementExportStep(
                name="CombinedProcurementExport",
                output_dir="output",
                filename_template="{YYYYMM}_PROCUREMENT_COMBINED.xlsx",
                include_index=False,
                retry_count=3
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
            print(f"Warning: Unknown step '{step_name}' for SPT {processing_type} pipeline")
            return None

    def get_enabled_steps(
        self,
        processing_type: str = 'PO',
        source_type: Optional[str] = None
    ) -> List[str]:
        """
        獲取啟用的步驟列表

        Args:
            processing_type: 處理類型 (PO/PR/PROCUREMENT)
            source_type: 子類型 (僅 PROCUREMENT 使用: PO/PR/COMBINED)

        Returns:
            List[str]: 步驟名稱列表
        """
        if processing_type == 'PO':
            return self.config.get('enabled_po_steps', [])
        elif processing_type == 'PR':
            return self.config.get('enabled_pr_steps', [])
        elif processing_type == 'PROCUREMENT':
            if source_type == 'PR':
                return self.config.get('enabled_procurement_pr_steps', [])
            elif source_type == 'COMBINED':
                return self.config.get('enabled_procurement_combined_steps', [])
            else:
                # 預設或 source_type == 'PO'
                return self.config.get('enabled_procurement_po_steps', [])
        else:
            return []
