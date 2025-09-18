"""
實體策略工廠
管理不同實體的處理策略
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import asyncio

from ..pipeline import Pipeline, PipelineBuilder
from ..context import ProcessingContext
from ..steps import *
from .config_manager import PipelineConfigManager, EntityConfig


class EntityStrategy(ABC):
    """
    實體策略基類
    定義實體特定的處理邏輯
    """
    
    def __init__(self, entity_config: EntityConfig):
        """
        初始化策略
        
        Args:
            entity_config: 實體配置
        """
        self.entity_type = entity_config.entity_type
        self.config = entity_config
    
    @abstractmethod
    def create_po_pipeline(self, mode: int = 1) -> Pipeline:
        """創建PO處理Pipeline"""
        pass
    
    @abstractmethod
    def create_pr_pipeline(self, mode: int = 3) -> Pipeline:
        """創建PR處理Pipeline"""
        pass
    
    @abstractmethod
    def get_validation_rules(self) -> List[Dict[str, Any]]:
        """獲取驗證規則"""
        pass
    
    @abstractmethod
    def get_special_processing_rules(self) -> Dict[str, Any]:
        """獲取特殊處理規則"""
        pass
    
    def create_pipeline(self, processing_type: str, mode: int) -> Pipeline:
        """
        創建Pipeline
        
        Args:
            processing_type: 處理類型 (PO/PR)
            mode: 處理模式
            
        Returns:
            Pipeline: 配置好的Pipeline
        """
        if processing_type == "PO":
            return self.create_po_pipeline(mode)
        elif processing_type == "PR":
            return self.create_pr_pipeline(mode)
        else:
            raise ValueError(f"Unknown processing type: {processing_type}")


class MOBStrategy(EntityStrategy):
    """
    MOB實體策略
    標準處理流程
    """
    
    def create_po_pipeline(self, mode: int = 1) -> Pipeline:
        """創建MOB PO處理Pipeline"""
        builder = PipelineBuilder(f"MOB_PO_Mode{mode}", "MOB")
        
        if mode == 1:  # 完整處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                DateFormattingStep(name="FormatDates"),
                DateParsingStep(name="ParseDates"),
                MOBStatusStep(name="MOBStatus"),
                MOBAccrualStep(name="MOBAccrual"),
                MOBDepartmentStep(name="MOBDepartment"),
                MOBValidationStep(name="MOBValidation"),
                ExportStep(name="Export")
            )
        elif mode == 2:  # 基本處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                DateFormattingStep(name="FormatDates"),
                MOBStatusStep(name="MOBStatus"),
                MOBAccrualStep(name="MOBAccrual")
            )
        elif mode == 4:  # 快速處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                MOBStatusStep(name="MOBStatus")
            )
        
        return builder.build()
    
    def create_pr_pipeline(self, mode: int = 3) -> Pipeline:
        """創建MOB PR處理Pipeline"""
        builder = PipelineBuilder(f"MOB_PR_Mode{mode}", "MOB")
        
        builder.add_steps(
            DataCleaningStep(name="Clean"),
            DateFormattingStep(name="FormatDates"),
            MOBStatusStep(name="MOBStatus")
        )
        
        return builder.build()
    
    def get_validation_rules(self) -> List[Dict[str, Any]]:
        """獲取MOB驗證規則"""
        return [
            {
                'name': 'positive_amounts',
                'description': '預估項金額必須為正',
                'field': 'Entry Amount',
                'condition': 'greater_than_zero'
            },
            {
                'name': 'valid_gl_code',
                'description': 'GL代碼必須在允許範圍',
                'field': 'GL#',
                'condition': 'in_range'
            }
        ]
    
    def get_special_processing_rules(self) -> Dict[str, Any]:
        """獲取MOB特殊處理規則"""
        return {
            'default_to_completed': True,
            'check_closed_status': True,
            'standard_department_code': True
        }


class SPTStrategy(EntityStrategy):
    """
    SPT實體策略
    支援提早完成和部門轉換
    """
    
    def create_po_pipeline(self, mode: int = 1) -> Pipeline:
        """創建SPT PO處理Pipeline"""
        builder = PipelineBuilder(f"SPT_PO_Mode{mode}", "SPT")
        
        if mode == 1:  # 完整處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                DataIntegrationStep(name="IntegratePrev", source_name="previous_wp"),
                DateFormattingStep(name="FormatDates"),
                DateParsingStep(name="ParseDates"),
                SPTStatusStep(name="SPTStatus"),
                SPTAccrualStep(name="SPTAccrual"),
                SPTDepartmentStep(name="SPTDepartment"),
                SPTValidationStep(name="SPTValidation"),
                ExportStep(name="Export")
            )
        elif mode == 2:  # 基本處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                DateFormattingStep(name="FormatDates"),
                SPTStatusStep(name="SPTStatus"),
                SPTAccrualStep(name="SPTAccrual"),
                SPTDepartmentStep(name="SPTDepartment")
            )
        elif mode == 4:  # 快速處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                SPTStatusStep(name="SPTStatus")
            )
        
        return builder.build()
    
    def create_pr_pipeline(self, mode: int = 3) -> Pipeline:
        """創建SPT PR處理Pipeline"""
        builder = PipelineBuilder(f"SPT_PR_Mode{mode}", "SPT")
        
        builder.add_steps(
            DataCleaningStep(name="Clean"),
            DateFormattingStep(name="FormatDates"),
            SPTStatusStep(name="SPTStatus"),
            SPTDepartmentStep(name="SPTDepartment")
        )
        
        return builder.build()
    
    def get_validation_rules(self) -> List[Dict[str, Any]]:
        """獲取SPT驗證規則"""
        return [
            {
                'name': 'department_consistency',
                'description': '部門代碼與Account code一致性',
                'fields': ['Department_Code', 'Account code'],
                'condition': 'consistent'
            },
            {
                'name': 'cross_month_dates',
                'description': '跨月項目日期範圍驗證',
                'fields': ['Date_Start', 'Date_End'],
                'condition': 'valid_range'
            },
            {
                'name': 'early_completion_check',
                'description': '提早完成合理性檢查',
                'field': 'Received Quantity',
                'condition': 'greater_than_zero'
            }
        ]
    
    def get_special_processing_rules(self) -> Dict[str, Any]:
        """獲取SPT特殊處理規則"""
        return {
            'support_early_completion': True,
            'department_conversion': True,
            'cross_month_processing': True,
            'partial_receipt_levels': [0.8, 0.5, 0.3]  # 接近完成、部分收貨、少量收貨
        }


class SPXStrategy(EntityStrategy):
    """
    SPX實體策略
    複雜的11條件判斷和特殊處理
    """
    
    def create_po_pipeline(self, mode: int = 1) -> Pipeline:
        """創建SPX PO處理Pipeline"""
        builder = PipelineBuilder(f"SPX_PO_Mode{mode}", "SPX")
        
        if mode == 1:  # 完整處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                DataIntegrationStep(name="IntegratePrev", source_name="previous_wp"),
                DataIntegrationStep(name="IntegrateClosing", source_name="closing_list"),
                SPXDepositCheckStep(name="DepositCheck"),
                SPXClosingListIntegrationStep(name="ClosingList"),
                DateFormattingStep(name="FormatDates"),
                DateParsingStep(name="ParseDates"),
                SPXRentProcessingStep(name="RentProcessing"),
                SPXAssetValidationStep(name="AssetValidation"),
                SPXPPEProcessingStep(name="PPEProcessing"),
                SPXComplexStatusStep(name="ComplexStatus"),
                ValidationStep(name="Validation"),
                ExportStep(name="Export")
            )
        elif mode == 2:  # 基本處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                SPXDepositCheckStep(name="DepositCheck"),
                DateFormattingStep(name="FormatDates"),
                SPXComplexStatusStep(name="ComplexStatus"),
                AccountingAdjustmentStep(name="Accounting")
            )
        elif mode == 5:  # SPX特殊處理
            builder.add_steps(
                DataCleaningStep(name="Clean"),
                SPXDepositCheckStep(name="DepositCheck"),
                SPXClosingListIntegrationStep(name="ClosingList"),
                SPXRentProcessingStep(name="RentProcessing"),
                SPXAssetValidationStep(name="AssetValidation"),
                SPXPPEProcessingStep(name="PPEProcessing"),
                SPXComplexStatusStep(name="ComplexStatus"),
                ExportStep(name="Export", format="excel")
            )
        
        return builder.build()
    
    def create_pr_pipeline(self, mode: int = 3) -> Pipeline:
        """創建SPX PR處理Pipeline"""
        builder = PipelineBuilder(f"SPX_PR_Mode{mode}", "SPX")
        
        builder.add_steps(
            DataCleaningStep(name="Clean"),
            SPXDepositCheckStep(name="DepositCheck"),
            DateFormattingStep(name="FormatDates"),
            SPXComplexStatusStep(name="ComplexStatus")
        )
        
        return builder.build()
    
    def get_validation_rules(self) -> List[Dict[str, Any]]:
        """獲取SPX驗證規則"""
        return [
            {
                'name': 'deposit_check',
                'description': '押金項目檢查',
                'field': 'GL#',
                'condition': 'check_199999'
            },
            {
                'name': 'asset_validation',
                'description': '資產驗收狀態',
                'fields': ['資產類型', '驗收狀態'],
                'condition': 'valid_status'
            },
            {
                'name': 'rent_period',
                'description': '租金期間合理性',
                'field': 'Rent_Period',
                'condition': 'valid_period'
            },
            {
                'name': 'capitalization_threshold',
                'description': '資本化門檻檢查',
                'fields': ['是否為FA', 'Entry Amount'],
                'condition': 'above_threshold'
            }
        ]
    
    def get_special_processing_rules(self) -> Dict[str, Any]:
        """獲取SPX特殊處理規則"""
        return {
            'complex_status_conditions': 11,
            'deposit_processing': True,
            'asset_validation_required': True,
            'rent_processing': True,
            'ppe_processing': True,
            'capitalization_threshold': 10000,
            'special_suppliers': {
                'kiosk': self.config.kiosk_suppliers,
                'locker': self.config.locker_suppliers
            }
        }


class EntityStrategyFactory:
    """
    實體策略工廠
    創建和管理實體策略
    """
    
    _strategies = {}
    _config_manager = None
    
    @classmethod
    def initialize(cls, config_manager: PipelineConfigManager):
        """
        初始化工廠
        
        Args:
            config_manager: 配置管理器
        """
        cls._config_manager = config_manager
        
        # 註冊預設策略
        cls.register_strategy("MOB", MOBStrategy)
        cls.register_strategy("SPT", SPTStrategy)
        cls.register_strategy("SPX", SPXStrategy)
    
    @classmethod
    def register_strategy(cls, entity_type: str, strategy_class: type):
        """
        註冊策略類
        
        Args:
            entity_type: 實體類型
            strategy_class: 策略類
        """
        cls._strategies[entity_type] = strategy_class
    
    @classmethod
    def create_strategy(cls, entity_type: str) -> EntityStrategy:
        """
        創建策略實例
        
        Args:
            entity_type: 實體類型
            
        Returns:
            EntityStrategy: 策略實例
        """
        if entity_type not in cls._strategies:
            raise ValueError(f"No strategy registered for entity type: {entity_type}")
        
        if not cls._config_manager:
            raise RuntimeError("Factory not initialized. Call initialize() first.")
        
        # 獲取實體配置
        entity_config = cls._config_manager.get_entity_config(entity_type)
        
        # 創建策略實例
        strategy_class = cls._strategies[entity_type]
        return strategy_class(entity_config)
    
    @classmethod
    def create_pipeline(cls,
                        entity_type: str,
                        processing_type: str,
                        mode: int) -> Pipeline:
        """
        創建Pipeline
        
        Args:
            entity_type: 實體類型
            processing_type: 處理類型 (PO/PR)
            mode: 處理模式
            
        Returns:
            Pipeline: 配置好的Pipeline
        """
        strategy = cls.create_strategy(entity_type)
        return strategy.create_pipeline(processing_type, mode)
    
    @classmethod
    def list_strategies(cls) -> List[str]:
        """
        列出所有可用策略
        
        Returns:
            List[str]: 策略列表
        """
        return list(cls._strategies.keys())
    
    @classmethod
    def get_strategy_info(cls, entity_type: str) -> Dict[str, Any]:
        """
        獲取策略資訊
        
        Args:
            entity_type: 實體類型
            
        Returns:
            Dict[str, Any]: 策略資訊
        """
        strategy = cls.create_strategy(entity_type)
        
        return {
            'entity_type': entity_type,
            'validation_rules': strategy.get_validation_rules(),
            'special_rules': strategy.get_special_processing_rules(),
            'config': strategy.config.to_dict()
        }


class AdaptivePipelineManager:
    """
    自適應Pipeline管理器
    根據數據特徵自動選擇最佳Pipeline
    """
    
    def __init__(self, config_manager: PipelineConfigManager):
        """
        初始化管理器
        
        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager
        EntityStrategyFactory.initialize(config_manager)
    
    async def analyze_data(self, context: ProcessingContext) -> Dict[str, Any]:
        """
        分析數據特徵
        
        Args:
            context: 處理上下文
            
        Returns:
            Dict[str, Any]: 數據特徵
        """
        df = context.data
        
        features = {
            'row_count': len(df),
            'has_closing_data': context.has_auxiliary_data('closing_list'),
            'has_previous_wp': context.has_auxiliary_data('previous_wp'),
            'has_procurement': context.has_auxiliary_data('procurement'),
            'columns': list(df.columns),
            'entity_type': context.metadata.entity_type,
            'processing_type': context.metadata.processing_type
        }
        
        # 檢查特殊情況
        if 'Item Description' in df.columns:
            descriptions = df['Item Description'].astype(str)
            features['has_deposits'] = descriptions.str.contains('押金|保證金', na=False).any()
            features['has_rent'] = descriptions.str.contains('租金|Rent', na=False).any()
            features['has_assets'] = descriptions.str.contains('Kiosk|Locker', na=False).any()
        
        return features
    
    def select_optimal_mode(self, features: Dict[str, Any]) -> int:
        """
        根據數據特徵選擇最佳處理模式
        
        Args:
            features: 數據特徵
            
        Returns:
            int: 最佳模式編號
        """
        entity_type = features['entity_type']
        processing_type = features['processing_type']
        
        # PR處理固定使用模式3
        if processing_type == "PR":
            return 3
        
        # SPX特殊情況
        if entity_type == "SPX":
            if features.get('has_deposits') or features.get('has_assets'):
                return 5  # SPX特殊處理
            elif features.get('has_closing_data'):
                return 1  # 完整處理
            else:
                return 2  # 基本處理
        
        # MOB/SPT選擇
        if features.get('has_previous_wp') and features.get('has_procurement'):
            return 1  # 完整處理
        elif features.get('row_count', 0) < 100:
            return 4  # 快速處理
        else:
            return 2  # 基本處理
    
    async def create_adaptive_pipeline(self, context: ProcessingContext) -> Pipeline:
        """
        創建自適應Pipeline
        
        Args:
            context: 處理上下文
            
        Returns:
            Pipeline: 自適應配置的Pipeline
        """
        # 分析數據
        features = await self.analyze_data(context)
        
        # 選擇最佳模式
        optimal_mode = self.select_optimal_mode(features)
        
        # 創建Pipeline
        pipeline = EntityStrategyFactory.create_pipeline(
            entity_type=context.metadata.entity_type,
            processing_type=context.metadata.processing_type,
            mode=optimal_mode
        )
        
        # 記錄選擇
        context.set_variable('selected_mode', optimal_mode)
        context.set_variable('data_features', features)
        
        return pipeline
