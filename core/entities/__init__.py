"""
業務實體模組

提供統一的實體處理介面，整合不同公司的PO/PR處理邏輯
"""

from .base_entity import BaseEntity, ProcessingMode, EntityProcessor
from .mob_entity import MOBEntity
from .spt_entity import SPTEntity
from .spx_entity import SPXEntity
from .entity_factory import EntityFactory, create_entity

# 便利的創建函數
def create_entity_by_name(entity_name: str):
    """根據實體名稱創建實體"""
    from ..models.data_models import EntityType
    
    # 標準化實體名稱
    entity_name = entity_name.upper().strip()
    
    if entity_name in ['MOB', 'MOBTW']:
        return create_entity(EntityType.MOB)
    elif entity_name in ['SPT', 'SPTTW']:
        return create_entity(EntityType.SPT)
    elif entity_name in ['SPX', 'SPXTW']:
        return create_entity(EntityType.SPX)
    else:
        raise ValueError(f"不支援的實體名稱: {entity_name}")

__all__ = [
    'BaseEntity',
    'ProcessingMode',
    'EntityProcessor',
    'MOBEntity',
    'SPTEntity', 
    'SPXEntity',
    'EntityFactory',
    'create_entity',
    'create_entity_by_name'
]
