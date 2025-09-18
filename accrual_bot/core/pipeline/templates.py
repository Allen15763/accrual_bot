"""
預定義Pipeline模板
提供常用的Pipeline配置模板
"""

from typing import Dict, List, Optional
from ..pipeline import Pipeline, PipelineBuilder
from ..steps import *
from .config_manager import PipelineConfigManager
from .entity_strategies import EntityStrategyFactory


class PipelineTemplate:
    """Pipeline模板基類"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    def create(self, entity_type: str, **kwargs) -> Pipeline:
        """創建Pipeline"""
        raise NotImplementedError


class StandardPOTemplate(PipelineTemplate):
    """標準PO處理模板"""
    
    def __init__(self):
        super().__init__(
            name="Standard_PO",
            description="標準PO處理流程，包含基本的數據清理、狀態評估和會計調整"
        )
    
    def create(self, entity_type: str, **kwargs) -> Pipeline:
        """創建標準PO Pipeline"""
        builder = PipelineBuilder(f"{entity_type}_Standard_PO", entity_type)
        
        # 基礎步驟
        builder.add_steps(
            DataCleaningStep(name="DataCleaning"),
            DateFormattingStep(name="DateFormatting"),
            DateParsingStep(name="DateParsing")
        )
        
        # 實體特定狀態評估
        if entity_type == "MOB":
            builder.add_step(MOBStatusStep(name="StatusEvaluation"))
            builder.add_step(MOBAccrualStep(name="AccrualProcessing"))
        elif entity_type == "SPT":
            builder.add_step(SPTStatusStep(name="StatusEvaluation"))
            builder.add_step(SPTAccrualStep(name="AccrualProcessing"))
        elif entity_type == "SPX":
            builder.add_step(SPXComplexStatusStep(name="StatusEvaluation"))
        
        # 通用會計處理
        builder.add_step(AccountingAdjustmentStep(name="AccountingAdjustment"))
        
        # 導出
        if kwargs.get('export', True):
            builder.add_step(ExportStep(name="Export", format=kwargs.get('export_format', 'excel')))
        
        return builder.build()


class FullPOWithIntegrationTemplate(PipelineTemplate):
    """完整PO處理模板（含數據整合）"""
    
    def __init__(self):
        super().__init__(
            name="Full_PO_Integration",
            description="完整PO處理流程，包含輔助數據整合、驗證和導出"
        )
    
    def create(self, entity_type: str, **kwargs) -> Pipeline:
        """創建完整PO Pipeline"""
        builder = PipelineBuilder(f"{entity_type}_Full_PO", entity_type)
        
        # 數據清理
        builder.add_step(DataCleaningStep(name="DataCleaning"))
        
        # 數據整合
        if kwargs.get('integrate_previous', True):
            builder.add_step(
                DataIntegrationStep(
                    name="IntegratePreviousWP",
                    source_name="previous_workpaper",
                    join_columns=['PO#']
                )
            )
        
        if kwargs.get('integrate_procurement', False):
            builder.add_step(
                DataIntegrationStep(
                    name="IntegrateProcurement",
                    source_name="procurement",
                    join_columns=['PO#']
                )
            )
        
        # SPX特殊整合
        if entity_type == "SPX" and kwargs.get('integrate_closing', False):
            builder.add_step(
                DataIntegrationStep(
                    name="IntegrateClosingList",
                    source_name="closing_list",
                    join_columns=['PO#']
                )
            )
        
        # 日期處理
        builder.add_steps(
            DateFormattingStep(name="DateFormatting"),
            DateParsingStep(name="DateParsing")
        )
        
        # 實體特定處理
        if entity_type == "MOB":
            builder.add_steps(
                MOBStatusStep(name="StatusEvaluation"),
                MOBAccrualStep(name="AccrualProcessing"),
                MOBDepartmentStep(name="DepartmentProcessing"),
                MOBValidationStep(name="Validation")
            )
        elif entity_type == "SPT":
            builder.add_steps(
                SPTStatusStep(name="StatusEvaluation"),
                SPTAccrualStep(name="AccrualProcessing"),
                SPTDepartmentStep(name="DepartmentProcessing"),
                SPTValidationStep(name="Validation")
            )
        elif entity_type == "SPX":
            builder.add_steps(
                SPXDepositCheckStep(name="DepositCheck"),
                SPXRentProcessingStep(name="RentProcessing"),
                SPXAssetValidationStep(name="AssetValidation"),
                SPXComplexStatusStep(name="StatusEvaluation"),
                SPXPPEProcessingStep(name="PPEProcessing"),
                ValidationStep(name="Validation")
            )
        
        # 導出
        builder.add_step(ExportStep(name="Export", format="excel"))
        
        return builder.build()


class SimplePRTemplate(PipelineTemplate):
    """簡單PR處理模板"""
    
    def __init__(self):
        super().__init__(
            name="Simple_PR",
            description="簡化的PR處理流程，僅包含基本狀態評估"
        )
    
    def create(self, entity_type: str, **kwargs) -> Pipeline:
        """創建簡單PR Pipeline"""
        builder = PipelineBuilder(f"{entity_type}_Simple_PR", entity_type)
        
        builder.add_steps(
            DataCleaningStep(name="DataCleaning"),
            DateFormattingStep(name="DateFormatting")
        )
        
        # 實體特定狀態評估
        if entity_type == "MOB":
            builder.add_step(MOBStatusStep(name="StatusEvaluation"))
        elif entity_type == "SPT":
            builder.add_step(SPTStatusStep(name="StatusEvaluation"))
        elif entity_type == "SPX":
            builder.add_step(SPXComplexStatusStep(name="StatusEvaluation"))
        
        # 可選導出
        if kwargs.get('export', False):
            builder.add_step(ExportStep(name="Export", format="csv"))
        
        return builder.build()


class SPXSpecialTemplate(PipelineTemplate):
    """SPX特殊處理模板"""
    
    def __init__(self):
        super().__init__(
            name="SPX_Special",
            description="SPX實體的特殊處理流程，包含押金、租金、資產驗收等"
        )
    
    def create(self, entity_type: str = "SPX", **kwargs) -> Pipeline:
        """創建SPX特殊Pipeline"""
        if entity_type != "SPX":
            raise ValueError("This template is only for SPX entity")
        
        builder = PipelineBuilder("SPX_Special_Processing", "SPX")
        
        # 數據準備
        builder.add_step(DataCleaningStep(name="DataCleaning"))
        
        # SPX特殊處理鏈
        builder.add_steps(
            # 押金識別（優先級最高）
            SPXDepositCheckStep(
                name="DepositIdentification",
                deposit_keywords=kwargs.get('deposit_keywords', '押金|保證金|Deposit|找零金')
            ),
            
            # 關單清單整合
            SPXClosingListIntegrationStep(
                name="ClosingListIntegration",
                required=False
            ),
            
            # 租金處理
            SPXRentProcessingStep(name="RentProcessing"),
            
            # 資產驗收
            SPXAssetValidationStep(
                name="AssetValidation",
                required=kwargs.get('require_validation', True)
            ),
            
            # PPE處理
            SPXPPEProcessingStep(name="PPEProcessing"),
            
            # 日期處理
            DateFormattingStep(name="DateFormatting"),
            DateParsingStep(name="DateParsing"),
            
            # 複雜狀態評估（11條件）
            SPXComplexStatusStep(name="ComplexStatusEvaluation"),
            
            # 會計調整
            AccountingAdjustmentStep(name="AccountingAdjustment"),
            
            # 驗證
            ValidationStep(name="FinalValidation")
        )
        
        # 導出
        builder.add_step(
            ExportStep(
                name="Export",
                format=kwargs.get('export_format', 'excel'),
                output_path=kwargs.get('output_path', 'output/spx')
            )
        )
        
        return builder.build()


class DataQualityCheckTemplate(PipelineTemplate):
    """數據質量檢查模板"""
    
    def __init__(self):
        super().__init__(
            name="Data_Quality_Check",
            description="專注於數據質量檢查和驗證的Pipeline"
        )
    
    def create(self, entity_type: str, **kwargs) -> Pipeline:
        """創建數據質量檢查Pipeline"""
        builder = PipelineBuilder(f"{entity_type}_DataQuality", entity_type)
        
        # 基礎清理
        builder.add_step(DataCleaningStep(name="InitialCleaning"))
        
        # 驗證步驟
        builder.add_step(
            ValidationStep(
                name="RequiredColumnsCheck",
                validations=['required_columns']
            )
        )
        
        builder.add_step(
            ValidationStep(
                name="DataTypesCheck",
                validations=['data_types']
            )
        )
        
        builder.add_step(
            ValidationStep(
                name="BusinessRulesCheck",
                validations=['business_rules']
            )
        )
        
        # 實體特定驗證
        if entity_type == "MOB":
            builder.add_step(MOBValidationStep(name="MOBSpecificValidation"))
        elif entity_type == "SPT":
            builder.add_step(SPTValidationStep(name="SPTSpecificValidation"))
        
        # 導出驗證報告
        if kwargs.get('export_report', True):
            builder.add_step(
                ExportStep(
                    name="ExportValidationReport",
                    format="excel",
                    output_path=f"validation/{entity_type}"
                )
            )
        
        return builder.build()


class PipelineTemplateManager:
    """Pipeline模板管理器"""
    
    def __init__(self):
        self.templates = {}
        self._register_default_templates()
    
    def _register_default_templates(self):
        """註冊預設模板"""
        self.register_template(StandardPOTemplate())
        self.register_template(FullPOWithIntegrationTemplate())
        self.register_template(SimplePRTemplate())
        self.register_template(SPXSpecialTemplate())
        self.register_template(DataQualityCheckTemplate())
    
    def register_template(self, template: PipelineTemplate):
        """
        註冊模板
        
        Args:
            template: Pipeline模板
        """
        self.templates[template.name] = template
    
    def get_template(self, name: str) -> Optional[PipelineTemplate]:
        """
        獲取模板
        
        Args:
            name: 模板名稱
            
        Returns:
            Optional[PipelineTemplate]: 模板實例
        """
        return self.templates.get(name)
    
    def create_from_template(self, 
                             template_name: str,
                             entity_type: str,
                             **kwargs) -> Pipeline:
        """
        從模板創建Pipeline
        
        Args:
            template_name: 模板名稱
            entity_type: 實體類型
            **kwargs: 其他參數
            
        Returns:
            Pipeline: 創建的Pipeline
        """
        template = self.get_template(template_name)
        if not template:
            raise ValueError(f"Template not found: {template_name}")
        
        return template.create(entity_type, **kwargs)
    
    def list_templates(self) -> List[Dict[str, str]]:
        """
        列出所有模板
        
        Returns:
            List[Dict[str, str]]: 模板列表
        """
        return [
            {
                'name': template.name,
                'description': template.description
            }
            for template in self.templates.values()
        ]
    
    def get_recommended_template(self, 
                                 entity_type: str,
                                 processing_type: str,
                                 has_auxiliary_data: bool = False) -> str:
        """
        獲取推薦的模板
        
        Args:
            entity_type: 實體類型
            processing_type: 處理類型
            has_auxiliary_data: 是否有輔助數據
            
        Returns:
            str: 推薦的模板名稱
        """
        if processing_type == "PR":
            return "Simple_PR"
        
        if entity_type == "SPX":
            return "SPX_Special"
        
        if has_auxiliary_data:
            return "Full_PO_Integration"
        
        return "Standard_PO"
