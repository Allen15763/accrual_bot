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
            'SPXDataLoading': lambda: SPXDataLoadingStep(
                name="SPXDataLoading",
                file_paths=file_paths
            ),
            'SPXPRDataLoading': lambda: SPXPRDataLoadingStep(
                name="SPXPRDataLoading",
                file_paths=file_paths
            ),
            'ColumnAddition': lambda: ColumnAdditionStep(
                name="ColumnAddition"
            ),
            'ClosingListIntegration': lambda: ClosingListIntegrationStep(
                name="ClosingListIntegration"
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
            'SPXExport': lambda: SPXExportStep(
                name="SPXExport"
            ),
            'SPXPRExport': lambda: SPXPRExportStep(
                name="SPXPRExport"
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
        else:
            return self.config.get('enabled_pr_steps', [])
