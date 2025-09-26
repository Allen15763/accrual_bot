"""
Pipeline 工廠
創建預定義的Pipeline配置
"""

from typing import Dict, Type, Optional, List, Callable
import logging

from .base import PipelineStep
from .pipeline import Pipeline, PipelineBuilder, PipelineConfig
from .context import ProcessingContext


class StepRegistry:
    """
    步驟註冊表
    管理所有可用的處理步驟
    """
    
    _steps: Dict[str, Type[PipelineStep]] = {}
    _step_factories: Dict[str, Callable] = {}
    
    @classmethod
    def register(cls, name: str, step_class: Type[PipelineStep]):
        """
        註冊步驟類
        
        Args:
            name: 步驟名稱
            step_class: 步驟類
        """
        cls._steps[name] = step_class
        logging.info(f"Registered step: {name}")
    
    @classmethod
    def register_factory(cls, name: str, factory: Callable):
        """
        註冊步驟工廠函數
        
        Args:
            name: 步驟名稱
            factory: 工廠函數
        """
        cls._step_factories[name] = factory
        logging.info(f"Registered step factory: {name}")
    
    @classmethod
    def get_step(cls, name: str, **kwargs) -> Optional[PipelineStep]:
        """
        獲取步驟實例
        
        Args:
            name: 步驟名稱
            **kwargs: 步驟參數
            
        Returns:
            Optional[PipelineStep]: 步驟實例或None
        """
        # 優先使用工廠函數
        if name in cls._step_factories:
            return cls._step_factories[name](**kwargs)
        
        # 使用步驟類
        if name in cls._steps:
            return cls._steps[name](**kwargs)
        
        logging.warning(f"Step not found: {name}")
        return None
    
    @classmethod
    def list_steps(cls) -> List[str]:
        """
        列出所有可用步驟
        
        Returns:
            List[str]: 步驟名稱列表
        """
        return list(set(list(cls._steps.keys()) + list(cls._step_factories.keys())))


class PipelineFactory:
    """
    Pipeline 工廠
    創建預定義的Pipeline配置
    """
    
    @staticmethod
    def create_basic_po_pipeline(entity_type: str = "MOB") -> Pipeline:
        """
        創建基礎PO處理Pipeline
        
        Args:
            entity_type: 實體類型 (MOB/SPT/SPX)
            
        Returns:
            Pipeline: 基礎PO Pipeline
        """
        from .steps import (
            DataCleaningStep,
            DateFormattingStep,
            StatusEvaluationStep,
            AccountingAdjustmentStep
        )
        
        builder = PipelineBuilder(f"{entity_type}_Basic_PO", entity_type)
        
        return (builder
                .with_description(f"Basic PO processing pipeline for {entity_type}")
                .with_stop_on_error(True)
                .add_steps(
                    DataCleaningStep(
                        name="Clean_Data",
                        columns_to_clean=['Item Description', 'GL#', 'Department']
                    ),
                    DateFormattingStep(
                        name="Format_Dates",
                        date_columns={
                            'Expected Receive Month': '%b-%y',
                            'Submission Date': '%d-%b-%y',
                            'PO Create Date': '%Y-%m-%d'
                        }
                    ),
                    StatusEvaluationStep(
                        name="Evaluate_Status",
                        entity_type=entity_type
                    ),
                    AccountingAdjustmentStep(
                        name="Accounting_Adjustment"
                    )
                )
                .build())
    
    @staticmethod
    def create_basic_pr_pipeline(entity_type: str = "MOB") -> Pipeline:
        """
        創建基礎PR處理Pipeline
        
        Args:
            entity_type: 實體類型 (MOB/SPT/SPX)
            
        Returns:
            Pipeline: 基礎PR Pipeline
        """
        from .steps import (
            DataCleaningStep,
            DateFormattingStep,
            StatusEvaluationStep
        )
        
        builder = PipelineBuilder(f"{entity_type}_Basic_PR", entity_type)
        
        return (builder
                .with_description(f"Basic PR processing pipeline for {entity_type}")
                .with_stop_on_error(True)
                .add_steps(
                    DataCleaningStep(
                        name="Clean_Data",
                        columns_to_clean=['Item Description', 'GL#', 'Department']
                    ),
                    DateFormattingStep(
                        name="Format_Dates",
                        date_columns={
                            'Expected Receive Month': '%b-%y',
                            'Submission Date': '%d-%b-%y',
                            'PR Create Date': '%Y-%m-%d'
                        }
                    ),
                    StatusEvaluationStep(
                        name="Evaluate_Status",
                        entity_type=entity_type
                    )
                )
                .build())
    
    @staticmethod
    def create_full_po_pipeline(entity_type: str = "MOB") -> Pipeline:
        """
        創建完整PO處理Pipeline（含輔助數據整合）
        
        Args:
            entity_type: 實體類型
            
        Returns:
            Pipeline: 完整PO Pipeline
        """
        from .steps import (
            DataCleaningStep,
            DataIntegrationStep,
            DateFormattingStep,
            DateParsingStep,
            StatusEvaluationStep,
            AccountingAdjustmentStep,
            ValidationStep,
            ExportStep
        )
        
        builder = PipelineBuilder(f"{entity_type}_Full_PO", entity_type)
        
        pipeline = (builder
                    .with_description(f"Full PO processing pipeline for {entity_type}")
                    .with_stop_on_error(False)  # 繼續執行以收集更多錯誤
                    .add_steps(
                        # 數據準備
                        DataCleaningStep(
                            name="Clean_Data",
                            columns_to_clean=['Item Description', 'GL#', 'Department'],
                            required=True
                        ),
                        
                        # 整合輔助數據
                        DataIntegrationStep(
                            name="Integrate_Previous_WP",
                            source_name="previous_workpaper",
                            join_columns=['PO#'],
                            required=False
                        ),
                        
                        DataIntegrationStep(
                            name="Integrate_Procurement",
                            source_name="procurement",
                            join_columns=['PO#'],
                            required=False
                        ),
                        
                        # 日期處理
                        DateFormattingStep(
                            name="Format_Dates",
                            date_columns={
                                'Expected Receive Month': '%b-%y',
                                'Submission Date': '%d-%b-%y',
                                'PO Create Date': '%Y-%m-%d'
                            }
                        ),
                        
                        DateParsingStep(
                            name="Parse_Description_Dates"
                        ),
                        
                        # 狀態評估
                        StatusEvaluationStep(
                            name="Evaluate_Status",
                            entity_type=entity_type
                        ),
                        
                        # 會計調整
                        AccountingAdjustmentStep(
                            name="Accounting_Adjustment"
                        ),
                        
                        # 驗證
                        ValidationStep(
                            name="Validate_Results",
                            validations=['required_columns', 'data_types', 'business_rules']
                        ),
                        
                        # 導出
                        ExportStep(
                            name="Export_Results",
                            format="excel",
                            required=False
                        )
                    )
                    .build())
        
        return pipeline
    
    @staticmethod
    def create_spx_special_pipeline() -> Pipeline:
        """
        創建SPX特殊處理Pipeline
        
        Returns:
            Pipeline: SPX特殊Pipeline
        """
        from .steps import (
            DataCleaningStep,
            SPXDepositCheckStep,
            SPXClosingListIntegrationStep,
            SPXRentProcessingStep,
            SPXAssetValidationStep,
            DateFormattingStep,
            StatusEvaluationStep,
            AccountingAdjustmentStep
        )
        
        builder = PipelineBuilder("SPX_Special", "SPX")
        
        return (builder
                .with_description("Special processing pipeline for SPX entity")
                .with_stop_on_error(False)
                .add_steps(
                    # 基礎清理
                    DataCleaningStep(
                        name="Clean_Data",
                        columns_to_clean=['Item Description', 'GL#', 'Department']
                    ),
                    
                    # SPX特殊處理
                    SPXDepositCheckStep(
                        name="Check_Deposit",
                        deposit_keywords='押金|保證金|Deposit|找零金'
                    ),
                    
                    SPXClosingListIntegrationStep(
                        name="Integrate_Closing_List",
                        required=False
                    ),
                    
                    SPXRentProcessingStep(
                        name="Process_Rent"
                    ),
                    
                    SPXAssetValidationStep(
                        name="Validate_Assets",
                        required=False
                    ),
                    
                    # 標準處理
                    DateFormattingStep(
                        name="Format_Dates",
                        date_columns={
                            'Expected Receive Month': '%b-%y',
                            'Submission Date': '%d-%b-%y',
                            'PO Create Date': '%Y-%m-%d'
                        }
                    ),
                    
                    StatusEvaluationStep(
                        name="Evaluate_Status",
                        entity_type="SPX"
                    ),
                    
                    AccountingAdjustmentStep(
                        name="Accounting_Adjustment"
                    )
                )
                .build())
    
    @staticmethod
    def create_custom_pipeline(
        name: str,
        entity_type: str,
        step_configs: List[Dict[str, any]]
    ) -> Pipeline:
        """
        根據配置創建自定義Pipeline
        
        Args:
            name: Pipeline名稱
            entity_type: 實體類型
            step_configs: 步驟配置列表
            
        Returns:
            Pipeline: 自定義Pipeline
        """
        builder = PipelineBuilder(name, entity_type)
        
        for config in step_configs:
            step_name = config.get('name')
            step_type = config.get('type')
            step_params = config.get('params', {})
            
            # 從註冊表獲取步驟
            step = StepRegistry.get_step(step_type, name=step_name, **step_params)
            
            if step:
                builder.add_step(step)
            else:
                logging.warning(f"Step type {step_type} not found, skipping")
        
        # 設置Pipeline配置
        pipeline_config = step_configs.get('pipeline_config', {})
        if pipeline_config.get('stop_on_error'):
            builder.with_stop_on_error(pipeline_config['stop_on_error'])
        if pipeline_config.get('parallel'):
            builder.with_parallel_execution(pipeline_config['parallel'])
        
        return builder.build()
    
    @staticmethod
    def create_mode_pipeline(entity_type: str, mode: int) -> Pipeline:
        """
        根據模式創建Pipeline
        
        Args:
            entity_type: 實體類型
            mode: 處理模式 (1-5)
            
        Returns:
            Pipeline: 對應模式的Pipeline
        """
        mode_map = {
            1: PipelineFactory.create_full_po_pipeline,
            2: PipelineFactory.create_basic_po_pipeline,
            3: PipelineFactory.create_basic_pr_pipeline,
            4: lambda et: PipelineFactory.create_basic_po_pipeline(et),  # 最簡模式
            5: PipelineFactory.create_spx_special_pipeline if entity_type == "SPX" else None
        }
        
        factory_func = mode_map.get(mode)
        if not factory_func:
            raise ValueError(f"Unsupported mode: {mode}")
        
        if mode == 5 and entity_type != "SPX":
            raise ValueError("Mode 5 is only for SPX entity")
        
        if mode == 5:
            return factory_func()
        else:
            return factory_func(entity_type)


# 註冊預設步驟類型
def register_default_steps():
    """註冊預設的步驟類型"""
    try:
        from .steps import (
            DataCleaningStep,
            DateFormattingStep,
            StatusEvaluationStep,
            AccountingAdjustmentStep
        )
        
        # 註冊基礎步驟
        StepRegistry.register("data_cleaning", DataCleaningStep)
        StepRegistry.register("date_formatting", DateFormattingStep)
        StepRegistry.register("status_evaluation", StatusEvaluationStep)
        StepRegistry.register("accounting_adjustment", AccountingAdjustmentStep)
    except ImportError as e:
        # 如果步驟模組不存在，不要失敗
        logging.warning(f"Could not import default steps: {e}")
    
    # 註冊工廠函數（用於更複雜的步驟創建）
    def create_conditional_step(**kwargs):
        """創建條件步驟的工廠函數"""
        from .base import ConditionalStep
        return ConditionalStep(**kwargs)
    
    StepRegistry.register_factory("conditional", create_conditional_step)
    
    logging.info("Default steps registered")
