"""
SCT Pipeline Orchestrator

Manages SCT-specific pipeline configuration and construction.
Supports configuration-driven step loading for flexible pipeline composition.

支援 PO/PR 兩種處理類型，包含 ERM 邏輯判斷步驟。
"""

from typing import List, Dict, Any, Optional
from accrual_bot.core.pipeline import Pipeline, PipelineConfig
from accrual_bot.core.pipeline.base import PipelineStep
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.logging import get_logger

# Import SCT steps
from accrual_bot.tasks.sct.steps import (
    SCTDataLoadingStep,
    SCTPRDataLoadingStep,
    SCTColumnAdditionStep,
    SCTERMLogicStep,
    SCTPRERMLogicStep,
    APInvoiceIntegrationStep
)

# Import shared steps from core
from accrual_bot.core.pipeline.steps import (
    PreviousWorkpaperIntegrationStep,
    ProcurementIntegrationStep,
    DateLogicStep,
)


class SCTPipelineOrchestrator:
    """
    SCT Pipeline 編排器

    功能:
    1. 根據配置動態創建 pipeline
    2. 支援 PO/PR 兩種處理類型
    3. 可選擇啟用/禁用特定步驟
    """

    def __init__(self):
        self.config = config_manager._config_toml.get('pipeline', {}).get('sct', {})
        self.entity_type = 'SCT'
        self.logger = get_logger(__name__)

    def build_po_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SCT PO 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SCT_PO_Processing",
            description="SCT PO data processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 獲取啟用的步驟列表
        enabled_steps = self.config.get('enabled_po_steps', [])

        if not enabled_steps:
            # 默認步驟順序
            enabled_steps = [
                "SCTDataLoading",
                "SCTColumnAddition",
                "APInvoiceIntegration",
                "PreviousWorkpaperIntegration",
                "ProcurementIntegration",
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
        構建 SCT PR 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SCT_PR_Processing",
            description="SCT PR data processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 獲取啟用的步驟列表
        enabled_steps = self.config.get('enabled_pr_steps', [])

        if not enabled_steps:
            # 默認步驟順序（PR 沒有 AP Invoice）
            enabled_steps = [
                "SCTPRDataLoading",
                "SCTColumnAddition",
                "PreviousWorkpaperIntegration",
                "ProcurementIntegration",
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
        processing_type: str = 'PO',
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
            'SCTDataLoading': lambda: SCTDataLoadingStep(
                name="SCTDataLoading",
                file_paths=file_paths
            ),
            'SCTPRDataLoading': lambda: SCTPRDataLoadingStep(
                name="SCTPRDataLoading",
                file_paths=file_paths
            ),

            # Column Addition (SCT 專屬)
            'SCTColumnAddition': lambda: SCTColumnAdditionStep(
                name="SCTColumnAddition",
                required=True
            ),

            # Data Integration（複用現有步驟）
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

            # 日期邏輯
            'DateLogic': lambda: DateLogicStep(
                name="DateLogic",
                required=True
            ),

            # ERM 邏輯判斷
            'SCTERMLogic': lambda: SCTERMLogicStep(
                name="SCTERMLogic",
                required=True
            ),
            'SCTPRERMLogic': lambda: SCTPRERMLogicStep(
                name="SCTPRERMLogic",
                required=True
            ),
        }

        step_factory = step_registry.get(step_name)
        if step_factory:
            return step_factory()
        else:
            self.logger.warning(f"Unknown step '{step_name}' for SCT {processing_type} pipeline")
            return None

    def get_enabled_steps(
        self,
        processing_type: str = 'PO',
        source_type: Optional[str] = None
    ) -> List[str]:
        """
        獲取啟用的步驟列表

        Args:
            processing_type: 處理類型 (PO/PR)
            source_type: 保留參數，目前未使用

        Returns:
            List[str]: 步驟名稱列表
        """
        if processing_type == 'PO':
            return self.config.get('enabled_po_steps', [])
        elif processing_type == 'PR':
            return self.config.get('enabled_pr_steps', [])
        else:
            return []
