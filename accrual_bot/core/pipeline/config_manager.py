"""
Pipeline 配置管理器
管理不同實體和模式的Pipeline配置
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import json
import yaml
from pathlib import Path

from ..pipeline import Pipeline, PipelineBuilder, PipelineConfig
from ..factory import PipelineFactory
from ..steps import *


@dataclass
class EntityConfig:
    """實體配置"""
    entity_type: str
    fa_accounts: List[str] = field(default_factory=list)
    rent_account: str = "622101"
    ops_rent: str = ""
    kiosk_suppliers: List[str] = field(default_factory=list)
    locker_suppliers: List[str] = field(default_factory=list)
    special_rules: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntityConfig':
        """從字典創建配置"""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return {
            'entity_type': self.entity_type,
            'fa_accounts': self.fa_accounts,
            'rent_account': self.rent_account,
            'ops_rent': self.ops_rent,
            'kiosk_suppliers': self.kiosk_suppliers,
            'locker_suppliers': self.locker_suppliers,
            'special_rules': self.special_rules
        }


@dataclass
class ProcessingMode:
    """處理模式配置"""
    mode: int
    name: str
    description: str
    entity_types: List[str]
    steps: List[str]
    parallel: bool = False
    stop_on_error: bool = True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessingMode':
        """從字典創建配置"""
        return cls(**data)


class PipelineConfigManager:
    """
    Pipeline配置管理器
    中央管理所有Pipeline配置
    """
    
    # 預設實體配置
    DEFAULT_ENTITY_CONFIGS = {
        "MOB": EntityConfig(
            entity_type="MOB",
            fa_accounts=["151101", "151201"],
            rent_account="622101",
            ops_rent="ShopeeOPS01"
        ),
        "SPT": EntityConfig(
            entity_type="SPT",
            fa_accounts=["151101", "151201"],
            rent_account="622101",
            ops_rent="ShopeeOPS02",
            special_rules={
                "support_early_completion": True,
                "department_conversion": True
            }
        ),
        "SPX": EntityConfig(
            entity_type="SPX",
            fa_accounts=["151101", "151201", "199999"],
            rent_account="622101",
            ops_rent="ShopeeOPS07",
            kiosk_suppliers=["益欣資訊股份有限公司", "振樺電子股份有限公司"],
            locker_suppliers=["掌櫃智能股份有限公司", "台灣宅配通股份有限公司"],
            special_rules={
                "complex_status": True,
                "asset_validation": True,
                "deposit_check": True
            }
        )
    }
    
    # 預設處理模式
    DEFAULT_PROCESSING_MODES = [
        ProcessingMode(
            mode=1,
            name="完整處理",
            description="包含所有步驟的完整處理流程",
            entity_types=["MOB", "SPT", "SPX"],
            steps=[
                "DataCleaning", "DataIntegration", "DateFormatting",
                "DateParsing", "StatusEvaluation", "AccountingAdjustment",
                "Validation", "Export"
            ]
        ),
        ProcessingMode(
            mode=2,
            name="基本處理",
            description="基本的數據處理流程",
            entity_types=["MOB", "SPT", "SPX"],
            steps=[
                "DataCleaning", "DateFormatting", "StatusEvaluation",
                "AccountingAdjustment"
            ]
        ),
        ProcessingMode(
            mode=3,
            name="PR處理",
            description="PR專用處理流程",
            entity_types=["MOB", "SPT", "SPX"],
            steps=[
                "DataCleaning", "DateFormatting", "StatusEvaluation"
            ]
        ),
        ProcessingMode(
            mode=4,
            name="快速處理",
            description="最簡化的處理流程",
            entity_types=["MOB", "SPT"],
            steps=["DataCleaning", "StatusEvaluation"],
            stop_on_error=False
        ),
        ProcessingMode(
            mode=5,
            name="SPX特殊處理",
            description="SPX實體的特殊處理流程",
            entity_types=["SPX"],
            steps=[
                "DataCleaning", "SPXDepositCheck", "SPXClosingList",
                "SPXRentProcessing", "SPXAssetValidation", "SPXComplexStatus",
                "SPXPPEProcessing", "AccountingAdjustment", "Export"
            ]
        )
    ]
    
    def __init__(self, config_file: Optional[str] = None):
        """
        初始化配置管理器
        
        Args:
            config_file: 配置文件路徑
        """
        self.entity_configs = self.DEFAULT_ENTITY_CONFIGS.copy()
        self.processing_modes = {m.mode: m for m in self.DEFAULT_PROCESSING_MODES}
        self.custom_pipelines = {}
        
        if config_file:
            self.load_config(config_file)
    
    def load_config(self, config_file: str):
        """
        載入配置文件
        
        Args:
            config_file: 配置文件路徑
        """
        path = Path(config_file)
        
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        # 根據文件類型載入
        if path.suffix == '.json':
            with open(path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
        elif path.suffix in ['.yml', '.yaml']:
            with open(path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
        else:
            raise ValueError(f"Unsupported config file type: {path.suffix}")
        
        # 更新實體配置
        if 'entities' in config_data:
            for entity_type, entity_config in config_data['entities'].items():
                self.entity_configs[entity_type] = EntityConfig.from_dict(entity_config)
        
        # 更新處理模式
        if 'modes' in config_data:
            for mode_data in config_data['modes']:
                mode = ProcessingMode.from_dict(mode_data)
                self.processing_modes[mode.mode] = mode
        
        # 載入自定義Pipeline
        if 'custom_pipelines' in config_data:
            self.custom_pipelines = config_data['custom_pipelines']
    
    def save_config(self, config_file: str):
        """
        保存配置到文件
        
        Args:
            config_file: 配置文件路徑
        """
        config_data = {
            'entities': {
                k: v.to_dict() for k, v in self.entity_configs.items()
            },
            'modes': [
                mode.__dict__ for mode in self.processing_modes.values()
            ],
            'custom_pipelines': self.custom_pipelines
        }
        
        path = Path(config_file)
        
        if path.suffix == '.json':
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
        elif path.suffix in ['.yml', '.yaml']:
            with open(path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, allow_unicode=True, sort_keys=False)
    
    def get_entity_config(self, entity_type: str) -> EntityConfig:
        """
        獲取實體配置
        
        Args:
            entity_type: 實體類型
            
        Returns:
            EntityConfig: 實體配置
        """
        if entity_type not in self.entity_configs:
            raise ValueError(f"Unknown entity type: {entity_type}")
        return self.entity_configs[entity_type]
    
    def get_processing_mode(self, mode: int) -> ProcessingMode:
        """
        獲取處理模式
        
        Args:
            mode: 模式編號
            
        Returns:
            ProcessingMode: 處理模式配置
        """
        if mode not in self.processing_modes:
            raise ValueError(f"Unknown processing mode: {mode}")
        return self.processing_modes[mode]
    
    def create_pipeline(self, 
                        entity_type: str,
                        mode: int,
                        processing_type: str = "PO") -> Pipeline:
        """
        創建Pipeline
        
        Args:
            entity_type: 實體類型
            mode: 處理模式
            processing_type: 處理類型 (PO/PR)
            
        Returns:
            Pipeline: 配置好的Pipeline
        """
        # 獲取配置
        entity_config = self.get_entity_config(entity_type)
        mode_config = self.get_processing_mode(mode)
        
        # 檢查實體是否支援該模式
        if entity_type not in mode_config.entity_types:
            raise ValueError(f"Mode {mode} not supported for entity {entity_type}")
        
        # 創建Pipeline
        builder = PipelineBuilder(
            name=f"{entity_type}_{mode_config.name}_{processing_type}",
            entity_type=entity_type
        )
        
        builder.with_description(f"{mode_config.description} for {entity_type} {processing_type}")
        builder.with_stop_on_error(mode_config.stop_on_error)
        
        if mode_config.parallel:
            builder.with_parallel_execution(True)
        
        # 添加步驟
        for step_name in mode_config.steps:
            step = self._create_step(step_name, entity_type, processing_type)
            if step:
                builder.add_step(step)
        
        return builder.build()
    
    def _create_step(self, 
                    step_name: str,
                    entity_type: str,
                    processing_type: str) -> Optional[PipelineStep]:
        """
        創建步驟實例
        
        Args:
            step_name: 步驟名稱
            entity_type: 實體類型
            processing_type: 處理類型
            
        Returns:
            Optional[PipelineStep]: 步驟實例
        """
        # 實體特定步驟映射
        entity_steps = {
            "MOB": {
                "StatusEvaluation": MOBStatusStep,
                "AccountingAdjustment": MOBAccrualStep,
                "DepartmentConversion": MOBDepartmentStep,
                "Validation": MOBValidationStep
            },
            "SPT": {
                "StatusEvaluation": SPTStatusStep,
                "DepartmentConversion": SPTDepartmentStep,
                "AccountingAdjustment": SPTAccrualStep,
                "Validation": SPTValidationStep
            },
            "SPX": {
                "StatusEvaluation": SPXComplexStatusStep,
                "SPXDepositCheck": SPXDepositCheckStep,
                "SPXClosingList": SPXClosingListIntegrationStep,
                "SPXRentProcessing": SPXRentProcessingStep,
                "SPXAssetValidation": SPXAssetValidationStep,
                "SPXComplexStatus": SPXComplexStatusStep,
                "SPXPPEProcessing": SPXPPEProcessingStep
            }
        }
        
        # 通用步驟映射
        common_steps = {
            "DataCleaning": DataCleaningStep,
            "DateFormatting": DateFormattingStep,
            "DateParsing": DateParsingStep,
            "DataIntegration": DataIntegrationStep,
            "Export": ExportStep
        }
        
        # 優先使用實體特定步驟
        if entity_type in entity_steps and step_name in entity_steps[entity_type]:
            step_class = entity_steps[entity_type][step_name]
            return step_class(name=f"{entity_type}_{step_name}")
        
        # 使用通用步驟
        if step_name in common_steps:
            step_class = common_steps[step_name]
            return step_class(name=step_name)
        
        # 使用業務步驟
        if step_name == "StatusEvaluation":
            return StatusEvaluationStep(name=step_name, entity_type=entity_type)
        elif step_name == "AccountingAdjustment":
            return AccountingAdjustmentStep(name=step_name)
        elif step_name == "Validation":
            return ValidationStep(name=step_name)
        
        # 步驟未找到
        print(f"Warning: Step {step_name} not found")
        return None
    
    def list_modes(self) -> List[Dict[str, Any]]:
        """
        列出所有處理模式
        
        Returns:
            List[Dict[str, Any]]: 模式列表
        """
        return [
            {
                'mode': mode.mode,
                'name': mode.name,
                'description': mode.description,
                'entities': mode.entity_types
            }
            for mode in self.processing_modes.values()
        ]
    
    def list_entities(self) -> List[str]:
        """
        列出所有實體
        
        Returns:
            List[str]: 實體列表
        """
        return list(self.entity_configs.keys())
    
    def create_custom_pipeline(self,
                              name: str,
                              entity_type: str,
                              steps: List[str],
                              **kwargs) -> Pipeline:
        """
        創建自定義Pipeline
        
        Args:
            name: Pipeline名稱
            entity_type: 實體類型
            steps: 步驟列表
            **kwargs: 其他配置
            
        Returns:
            Pipeline: 自定義Pipeline
        """
        builder = PipelineBuilder(name=name, entity_type=entity_type)
        
        # 設置配置
        if 'description' in kwargs:
            builder.with_description(kwargs['description'])
        if 'stop_on_error' in kwargs:
            builder.with_stop_on_error(kwargs['stop_on_error'])
        if 'parallel' in kwargs:
            builder.with_parallel_execution(kwargs['parallel'])
        
        # 添加步驟
        for step_name in steps:
            step = self._create_step(step_name, entity_type, "PO")
            if step:
                builder.add_step(step)
        
        pipeline = builder.build()
        
        # 保存到自定義Pipeline
        self.custom_pipelines[name] = {
            'entity_type': entity_type,
            'steps': steps,
            'config': kwargs
        }
        
        return pipeline
