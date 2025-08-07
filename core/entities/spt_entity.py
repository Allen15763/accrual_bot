"""
SPT實體處理器

整合SPTTW公司的PO和PR處理邏輯
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from .base_entity import BaseEntity, EntityProcessor, ProcessingFiles, ProcessingMode

try:
    from ...core.models.data_models import EntityType, ProcessingType, ProcessingResult
    from ...core.models.config_models import EntityConfig, create_default_entity_config
    from ...core.processors.spt_po_processor import SptPOProcessor
    from ...core.processors.spt_pr_processor import SptPRProcessor
    from ...utils.logging import Logger
except ImportError:
    import sys
    from pathlib import Path
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
        
    from core.models.data_models import EntityType, ProcessingType, ProcessingResult
    from core.models.config_models import EntityConfig, create_default_entity_config
    from core.processors.spt_po_processor import SptPOProcessor
    from core.processors.spt_pr_processor import SptPRProcessor
    from utils.logging import Logger


class SPTPOProcessor(EntityProcessor):
    """SPT PO處理器"""
    
    def __init__(self, entity_config: EntityConfig, shared_logger=None):
        self.entity_config = entity_config
        # 使用共享的logger以避免重複創建
        self.logger = shared_logger or Logger().get_logger('spt_entity')
        
        # 使用SPT專用處理器
        self.po_processor = SptPOProcessor()
    
    def process_po(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PO數據"""
        try:
            self.logger.info(f"SPT PO {mode.value}: {files.raw_data_filename}")
            
            # 根據模式調用相應的處理方法
            if mode == ProcessingMode.MODE_1:
                return self._process_mode_1(files)
            elif mode == ProcessingMode.MODE_2:
                return self._process_mode_2(files)
            elif mode == ProcessingMode.MODE_3:
                return self._process_mode_3(files)
            elif mode == ProcessingMode.MODE_4:
                return self._process_mode_4(files)
            else:
                raise ValueError(f"不支援的處理模式: {mode}")
                
        except Exception as e:
            self.logger.error(f"SPT PO {mode.value}處理失敗: {e}")
            return ProcessingResult(
                success=False,
                message=f"處理失敗: {str(e)}",
                start_time=datetime.now(),
                end_time=datetime.now()
            )
    
    def _process_mode_1(self, files: ProcessingFiles) -> ProcessingResult:
        """模式1：完整處理流程"""
        return self.po_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper,
            files.procurement_file,
            files.closing_list
        )
    
    def _process_mode_2(self, files: ProcessingFiles) -> ProcessingResult:
        """模式2：無關單清單"""
        return self.po_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper,
            files.procurement_file
        )
    
    def _process_mode_3(self, files: ProcessingFiles) -> ProcessingResult:
        """模式3：基礎處理"""
        return self.po_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper
        )
    
    def _process_mode_4(self, files: ProcessingFiles) -> ProcessingResult:
        """模式4：僅原始數據"""
        return self.po_processor.process(
            files.raw_data_file,
            files.raw_data_filename
        )
    
    def process_pr(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PR數據（SPT PO不支援）"""
        return ProcessingResult(
            success=False,
            message="SPT PO處理器不支援PR處理",
            start_time=datetime.now(),
            end_time=datetime.now()
        )
    
    def get_supported_modes(self, processing_type: ProcessingType) -> List[ProcessingMode]:
        """獲取支援的處理模式"""
        if processing_type == ProcessingType.PO:
            return [ProcessingMode.MODE_1, ProcessingMode.MODE_2, ProcessingMode.MODE_3, ProcessingMode.MODE_4]
        else:
            return []


class SPTPRProcessor(EntityProcessor):
    """SPT PR處理器"""
    
    def __init__(self, entity_config: EntityConfig, shared_logger=None):
        self.entity_config = entity_config
        # 使用共享的logger以避免重複創建
        self.logger = shared_logger or Logger().get_logger('spt_entity')
        
        # 使用SPT專用處理器
        self.pr_processor = SptPRProcessor()
    
    def process_pr(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PR數據"""
        try:
            self.logger.info(f"SPT PR {mode.value}: {files.raw_data_filename}")
            
            # 根據模式調用相應的處理方法
            if mode == ProcessingMode.MODE_1:
                return self._process_mode_1(files)
            elif mode == ProcessingMode.MODE_2:
                return self._process_mode_2(files)
            else:
                raise ValueError(f"不支援的處理模式: {mode}")
                
        except Exception as e:
            self.logger.error(f"SPT PR {mode.value}處理失敗: {e}")
            return ProcessingResult(
                success=False,
                message=f"處理失敗: {str(e)}",
                start_time=datetime.now(),
                end_time=datetime.now()
            )
    
    def _process_mode_1(self, files: ProcessingFiles) -> ProcessingResult:
        """模式1：完整處理流程"""
        return self.pr_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper,
            files.procurement_file
        )
    
    def _process_mode_2(self, files: ProcessingFiles) -> ProcessingResult:
        """模式2：基礎處理"""
        return self.pr_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper
        )
    
    def process_po(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PO數據（SPT PR不支援）"""
        return ProcessingResult(
            success=False,
            message="SPT PR處理器不支援PO處理",
            start_time=datetime.now(),
            end_time=datetime.now()
        )
    
    def get_supported_modes(self, processing_type: ProcessingType) -> List[ProcessingMode]:
        """獲取支援的處理模式"""
        if processing_type == ProcessingType.PR:
            return [ProcessingMode.MODE_1, ProcessingMode.MODE_2]
        else:
            return []


class SPTEntity(BaseEntity):
    """SPT業務實體"""
    
    def __init__(self, config: Optional[EntityConfig] = None):
        super().__init__(EntityType.SPT, config)
    
    def _create_default_config(self) -> EntityConfig:
        """創建SPT的默認配置"""
        return create_default_entity_config(EntityType.SPT)
    
    def _initialize_processors(self):
        """初始化處理器"""
        shared_logger = self.logger
        
        # 使用SPT專用處理器
        self._po_processor = SPTPOProcessor(self.config, shared_logger)
        self._pr_processor = SPTPRProcessor(self.config, shared_logger)
    
    def get_entity_name(self) -> str:
        """獲取實體名稱"""
        return "SPTTW"
    
    def get_entity_description(self) -> str:
        """獲取實體描述"""
        return "SPT Taiwan PO/PR 處理實體"

    # 向後相容的方法別名
    def mode_1(self, raw_data_file: str, filename: str,
               previous_workpaper: str, procurement_file: str,
               closing_list: str, **kwargs):
        """向後相容：SPT PO模式1"""
        return self.process_po_mode_1(raw_data_file, filename, previous_workpaper, 
                                      procurement_file, closing_list, **kwargs)
    
    def mode_2(self, raw_data_file: str, filename: str,
               previous_workpaper: str, procurement_file: str, **kwargs):
        """向後相容：SPT PO模式2"""
        return self.process_po_mode_2(raw_data_file, filename, previous_workpaper, 
                                      procurement_file, **kwargs)
    
    def mode_3(self, raw_data_file: str, filename: str,
               previous_workpaper: str, **kwargs):
        """向後相容：SPT PO模式3"""
        return self.process_po_mode_3(raw_data_file, filename, previous_workpaper, **kwargs)
    
    def mode_4(self, raw_data_file: str, filename: str, **kwargs):
        """向後相容：SPT PO模式4"""
        return self.process_po_mode_4(raw_data_file, filename, **kwargs)


# 向後相容的類別別名
SPTTW_PO = SPTEntity
SPTTW_PR = SPTEntity
