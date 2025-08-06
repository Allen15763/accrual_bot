"""
實體工廠

提供統一的實體創建和管理功能
"""

from typing import Dict, Type, Optional, Any
from enum import Enum

from .base_entity import BaseEntity
from .mob_entity import MOBEntity
from .spt_entity import SPTEntity
from .spx_entity import SPXEntity
from core.models.data_models import EntityType
from core.models.config_models import EntityConfig, create_default_entity_config
from utils.logging import Logger


class EntityFactory:
    """實體工廠類別"""
    
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        
        # 註冊實體類別
        self._entity_classes: Dict[EntityType, Type[BaseEntity]] = {
            EntityType.MOB: MOBEntity,
            EntityType.SPT: SPTEntity,
            EntityType.SPX: SPXEntity
        }
        
        # 實體實例快取
        self._entity_cache: Dict[str, BaseEntity] = {}
    
    def create_entity(self, entity_type: EntityType, 
                     config: Optional[EntityConfig] = None,
                     use_cache: bool = True) -> BaseEntity:
        """
        創建實體實例
        
        Args:
            entity_type: 實體類型
            config: 實體配置（可選，會使用預設配置）
            use_cache: 是否使用快取
            
        Returns:
            BaseEntity: 實體實例
            
        Raises:
            ValueError: 不支援的實體類型
        """
        if entity_type not in self._entity_classes:
            raise ValueError(f"不支援的實體類型: {entity_type}")
        
        # 快取鍵
        cache_key = f"{entity_type.value}_{id(config) if config else 'default'}"
        
        # 檢查快取
        if use_cache and cache_key in self._entity_cache:
            self.logger.debug(f"從快取返回實體: {entity_type.value}")
            return self._entity_cache[cache_key]
        
        # 創建配置
        if config is None:
            config = create_default_entity_config(entity_type)
        
        # 創建實體
        entity_class = self._entity_classes[entity_type]
        entity = entity_class(config)
        
        # 儲存到快取
        if use_cache:
            self._entity_cache[cache_key] = entity
        
        self.logger.info(f"成功創建實體: {entity_type.value}")
        return entity
    
    def get_entity_by_name(self, entity_name: str, 
                          config: Optional[EntityConfig] = None) -> BaseEntity:
        """
        根據實體名稱創建實體
        
        Args:
            entity_name: 實體名稱 ('MOB', 'SPT', 'SPX', 'MOBTW', 'SPTTW', 'SPXTW')
            config: 實體配置
            
        Returns:
            BaseEntity: 實體實例
            
        Raises:
            ValueError: 不支援的實體名稱
        """
        # 標準化實體名稱
        entity_name = entity_name.upper()
        
        # 名稱映射
        name_mapping = {
            'MOB': EntityType.MOB,
            'MOBTW': EntityType.MOB,
            'SPT': EntityType.SPT,
            'SPTTW': EntityType.SPT,
            'SPX': EntityType.SPX,
            'SPXTW': EntityType.SPX
        }
        
        if entity_name not in name_mapping:
            raise ValueError(f"不支援的實體名稱: {entity_name}")
        
        entity_type = name_mapping[entity_name]
        return self.create_entity(entity_type, config)
    
    def register_entity_class(self, entity_type: EntityType, entity_class: Type[BaseEntity]):
        """
        註冊新的實體類別
        
        Args:
            entity_type: 實體類型
            entity_class: 實體類別
        """
        self._entity_classes[entity_type] = entity_class
        self.logger.info(f"註冊實體類別: {entity_type.value} -> {entity_class.__name__}")
    
    def get_supported_entity_types(self) -> list[EntityType]:
        """獲取支援的實體類型列表"""
        return list(self._entity_classes.keys())
    
    def get_entity_info(self, entity_type: EntityType) -> Dict[str, Any]:
        """
        獲取實體資訊
        
        Args:
            entity_type: 實體類型
            
        Returns:
            Dict[str, Any]: 實體資訊
        """
        if entity_type not in self._entity_classes:
            raise ValueError(f"不支援的實體類型: {entity_type}")
        
        # 創建臨時實例來獲取資訊
        temp_entity = self.create_entity(entity_type, use_cache=False)
        return temp_entity.get_entity_info()
    
    def clear_cache(self):
        """清空實體快取"""
        self._entity_cache.clear()
        self.logger.info("實體快取已清空")
    
    def get_cache_status(self) -> Dict[str, Any]:
        """獲取快取狀態"""
        return {
            "cached_entities": len(self._entity_cache),
            "cache_keys": list(self._entity_cache.keys()),
            "supported_types": [t.value for t in self._entity_classes.keys()]
        }


# 全域實體工廠實例
_factory = EntityFactory()


def create_entity(entity_type: EntityType, 
                 config: Optional[EntityConfig] = None,
                 use_cache: bool = True) -> BaseEntity:
    """
    創建實體的便捷函數
    
    Args:
        entity_type: 實體類型
        config: 實體配置
        use_cache: 是否使用快取
        
    Returns:
        BaseEntity: 實體實例
    """
    return _factory.create_entity(entity_type, config, use_cache)


def create_entity_by_name(entity_name: str, 
                         config: Optional[EntityConfig] = None) -> BaseEntity:
    """
    根據名稱創建實體的便捷函數
    
    Args:
        entity_name: 實體名稱
        config: 實體配置
        
    Returns:
        BaseEntity: 實體實例
    """
    return _factory.get_entity_by_name(entity_name, config)


def get_factory() -> EntityFactory:
    """獲取全域實體工廠實例"""
    return _factory


# 向後相容的便捷函數
def create_mob_entity(config: Optional[EntityConfig] = None) -> MOBEntity:
    """創建MOB實體"""
    return create_entity(EntityType.MOB, config)


def create_spt_entity(config: Optional[EntityConfig] = None) -> SPTEntity:
    """創建SPT實體"""
    return create_entity(EntityType.SPT, config)


def create_spx_entity(config: Optional[EntityConfig] = None) -> SPXEntity:
    """創建SPX實體"""
    return create_entity(EntityType.SPX, config)


# 實體類型檢測
def detect_entity_type_from_filename(filename: str) -> Optional[EntityType]:
    """
    從檔案名稱檢測實體類型
    
    Args:
        filename: 檔案名稱
        
    Returns:
        Optional[EntityType]: 檢測到的實體類型，如果無法檢測則返回None
    """
    filename_upper = filename.upper()
    
    if 'MOB' in filename_upper:
        return EntityType.MOB
    elif 'SPT' in filename_upper:
        return EntityType.SPT
    elif 'SPX' in filename_upper:
        return EntityType.SPX
    
    return None


def auto_create_entity(filename: str, 
                      config: Optional[EntityConfig] = None) -> Optional[BaseEntity]:
    """
    根據檔案名稱自動創建對應的實體
    
    Args:
        filename: 檔案名稱
        config: 實體配置
        
    Returns:
        Optional[BaseEntity]: 創建的實體，如果無法檢測則返回None
    """
    entity_type = detect_entity_type_from_filename(filename)
    
    if entity_type:
        return create_entity(entity_type, config)
    
    return None


# 批量操作
def create_all_entities(config_dict: Optional[Dict[EntityType, EntityConfig]] = None) -> Dict[EntityType, BaseEntity]:
    """
    創建所有支援的實體
    
    Args:
        config_dict: 各實體的配置字典
        
    Returns:
        Dict[EntityType, BaseEntity]: 實體字典
    """
    if config_dict is None:
        config_dict = {}
    
    entities = {}
    
    for entity_type in _factory.get_supported_entity_types():
        config = config_dict.get(entity_type)
        entities[entity_type] = create_entity(entity_type, config)
    
    return entities


def get_entity_summary() -> Dict[str, Any]:
    """獲取所有實體的摘要資訊"""
    summary = {
        "supported_entities": [],
        "factory_status": _factory.get_cache_status()
    }
    
    for entity_type in _factory.get_supported_entity_types():
        try:
            entity_info = _factory.get_entity_info(entity_type)
            summary["supported_entities"].append(entity_info)
        except Exception as e:
            summary["supported_entities"].append({
                "entity_type": entity_type.value,
                "error": str(e)
            })
    
    return summary
