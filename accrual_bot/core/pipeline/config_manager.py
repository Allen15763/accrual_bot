"""
Pipeline 配置管理器（重構版）
統一使用原架構的配置管理系統，避免配置雙軌制

重構說明：
1. 移除硬編碼的配置，改為從 utils/config/config_manager 讀取
2. 保持原有的 PipelineConfigManager 接口，確保向後兼容
3. 所有配置統一從 config.ini 讀取
4. 實體配置、正則模式等從統一配置源獲取
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import json
import yaml
from pathlib import Path

# 導入原架構的配置管理器
try:
    from ...utils import config_manager as global_config_manager
    from ...utils.config import ENTITY_TYPES, PROCESSING_MODES, STATUS_VALUES
except ImportError:
    # 備用導入路徑
    import sys
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from utils import config_manager as global_config_manager
    from utils.config import ENTITY_TYPES, PROCESSING_MODES, STATUS_VALUES


@dataclass
class EntityConfig:
    """
    實體配置
    從統一配置源讀取，不再硬編碼
    """
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
    Pipeline配置管理器（重構版）
    
    重構改進：
    1. 不再維護獨立的配置，統一從原架構的 config_manager 讀取
    2. 動態從 config.ini 讀取實體配置
    3. 正則模式從配置檔讀取，不使用 constants.py
    4. 保持原有接口，確保現有代碼無需大幅修改
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路徑（保留參數以向後兼容，實際使用全局配置）
        """
        # 使用全局配置管理器
        self.config_manager = global_config_manager
        
        # 緩存實體配置
        self._entity_configs: Dict[str, EntityConfig] = {}
        
        # 載入實體配置
        self._load_entity_configs()
        
        # 載入處理模式配置
        self._processing_modes = self._load_processing_modes()
    
    def _load_entity_configs(self) -> None:
        """
        從配置檔載入實體配置
        不再使用硬編碼的 DEFAULT_ENTITY_CONFIGS
        """
        for entity_type in ['MOB', 'SPT', 'SPX']:
            self._entity_configs[entity_type] = self._create_entity_config(entity_type)
    
    def _create_entity_config(self, entity_type: str) -> EntityConfig:
        """
        創建實體配置
        從 config.ini 動態讀取配置
        
        Args:
            entity_type: 實體類型
            
        Returns:
            EntityConfig: 實體配置對象
        """
        # 從配置檔讀取FA帳戶
        fa_accounts = self.config_manager.get_fa_accounts(entity_type.lower())
        
        # 讀取實體特定配置
        entity_section = entity_type.upper()
        
        # 基本配置
        config = EntityConfig(
            entity_type=entity_type,
            fa_accounts=fa_accounts,
            rent_account=self.config_manager.get('SPX', 'account_rent', '622101'),
            ops_rent=self._get_ops_rent(entity_type)
        )
        
        # SPX特殊配置
        if entity_type == 'SPX':
            config.kiosk_suppliers = self.config_manager.get_list('SPX', 'kiosk_suppliers')
            config.locker_suppliers = self.config_manager.get_list('SPX', 'locker_suppliers')
            
            # 特殊規則
            config.special_rules = {
                'complex_status': True,
                'asset_validation': True,
                'deposit_check': True,
                'deposit_keywords': self.config_manager.get('SPX', 'deposit_keywords', ''),
                'utility_suppliers': self.config_manager.get_list('SPX', 'utility_suppliers'),
                'bao_supplier': self.config_manager.get_list('SPX', 'bao_supplier'),
                'bao_categories': self.config_manager.get_list('SPX', 'bao_categories')
            }
        
        # SPT特殊配置
        elif entity_type == 'SPT':
            config.special_rules = {
                'support_early_completion': True,
                'department_conversion': True
            }
        
        return config
    
    def _get_ops_rent(self, entity_type: str) -> str:
        """
        獲取OPS租金配置
        
        Args:
            entity_type: 實體類型
            
        Returns:
            str: OPS代碼
        """
        ops_mapping = {
            'MOB': 'ShopeeOPS01',
            'SPT': 'ShopeeOPS02',
            'SPX': self.config_manager.get('SPX', 'ops_for_rent', 'ShopeeOPS07')
        }
        return ops_mapping.get(entity_type, '')
    
    def _load_processing_modes(self) -> List[ProcessingMode]:
        """
        載入處理模式配置
        
        Returns:
            List[ProcessingMode]: 處理模式列表
        """
        # 預設處理模式（這部分邏輯性的配置可以保留在代碼中）
        modes = [
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
                description="PR特定處理流程",
                entity_types=["MOB", "SPT", "SPX"],
                steps=[
                    "DataCleaning", "DateFormatting", "StatusEvaluation"
                ]
            ),
            ProcessingMode(
                mode=4,
                name="快速處理",
                description="最小化處理流程",
                entity_types=["MOB", "SPT", "SPX"],
                steps=["DataCleaning", "StatusEvaluation"]
            ),
            ProcessingMode(
                mode=5,
                name="數據驗證",
                description="僅執行數據驗證",
                entity_types=["MOB", "SPT", "SPX"],
                steps=["DataCleaning", "Validation"]
            )
        ]
        return modes
    
    def get_entity_config(self, entity_type: str) -> EntityConfig:
        """
        獲取實體配置
        
        Args:
            entity_type: 實體類型
            
        Returns:
            EntityConfig: 實體配置
        """
        entity_type = entity_type.upper()
        if entity_type not in self._entity_configs:
            raise ValueError(f"Unknown entity type: {entity_type}")
        return self._entity_configs[entity_type]
    
    def get_processing_mode(self, mode: int) -> Optional[ProcessingMode]:
        """
        獲取處理模式配置
        
        Args:
            mode: 模式編號
            
        Returns:
            Optional[ProcessingMode]: 處理模式配置
        """
        for pm in self._processing_modes:
            if pm.mode == mode:
                return pm
        return None
    
    def get_regex_patterns(self) -> Dict[str, str]:
        """
        獲取正則表達式模式
        從配置檔讀取，不使用 constants.py
        
        Returns:
            Dict[str, str]: 正則表達式字典
        """
        return self.config_manager.get_regex_patterns()
    
    def get_fa_accounts(self, entity_type: str) -> List[str]:
        """
        獲取FA帳戶列表
        
        Args:
            entity_type: 實體類型
            
        Returns:
            List[str]: FA帳戶列表
        """
        return self.config_manager.get_fa_accounts(entity_type.lower())
    
    def get_pivot_config(self, entity_type: str, data_type: str) -> Dict[str, Any]:
        """
        獲取透視表配置
        
        Args:
            entity_type: 實體類型
            data_type: 數據類型 ('pr' or 'po')
            
        Returns:
            Dict[str, Any]: 透視表配置
        """
        return self.config_manager.get_pivot_config(entity_type, data_type)
    
    def get_config_value(self, section: str, key: str, fallback: Any = None) -> Any:
        """
        直接從配置檔獲取配置值
        
        Args:
            section: 配置段落
            key: 配置鍵
            fallback: 預設值
            
        Returns:
            Any: 配置值
        """
        return self.config_manager.get(section, key, fallback)
    
    def list_entity_types(self) -> List[str]:
        """
        列出所有支援的實體類型
        
        Returns:
            List[str]: 實體類型列表
        """
        return list(self._entity_configs.keys())
    
    def list_processing_modes(self) -> List[ProcessingMode]:
        """
        列出所有處理模式
        
        Returns:
            List[ProcessingMode]: 處理模式列表
        """
        return self._processing_modes
    
    def validate_config(self) -> Dict[str, Any]:
        """
        驗證配置完整性
        
        Returns:
            Dict[str, Any]: 驗證結果
        """
        results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # 檢查實體配置
        for entity_type in self.list_entity_types():
            config = self.get_entity_config(entity_type)
            
            if not config.fa_accounts:
                results['warnings'].append(
                    f"{entity_type}: FA帳戶列表為空"
                )
            
            if not config.ops_rent:
                results['warnings'].append(
                    f"{entity_type}: OPS租金配置為空"
                )
        
        # 檢查正則模式
        patterns = self.get_regex_patterns()
        required_patterns = ['pt_YM', 'pt_YMD', 'pt_YMtoYM', 'pt_YMDtoYMD']
        for pattern_key in required_patterns:
            if pattern_key not in patterns or not patterns[pattern_key]:
                results['errors'].append(f"缺少必要的正則模式: {pattern_key}")
                results['valid'] = False
        
        return results
    
    def reload_config(self) -> None:
        """重新載入配置"""
        self.config_manager.reload_config()
        self._load_entity_configs()
        self._processing_modes = self._load_processing_modes()


# 向後兼容：保留原有的導入
__all__ = [
    'PipelineConfigManager',
    'EntityConfig',
    'ProcessingMode'
]
