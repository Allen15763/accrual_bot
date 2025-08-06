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
    from ...core.processors.po_processor import BasePOProcessor
    from ...core.processors.pr_processor import BasePRProcessor
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
    from core.processors.po_processor import BasePOProcessor
    from core.processors.pr_processor import BasePRProcessor
    from utils.logging import Logger


class SPTPOProcessor(EntityProcessor):
    """SPT PO處理器"""
    
    def __init__(self, entity_config: EntityConfig):
        self.entity_config = entity_config
        self.logger = Logger().get_logger(__name__)
        
        # 初始化核心處理器
        self.po_processor = BasePOProcessor(self.entity_config.entity_type.value)
    
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
        """SPT PO處理器不處理PR"""
        raise NotImplementedError("PO處理器不處理PR數據")
    
    def get_supported_modes(self, processing_type: ProcessingType) -> List[ProcessingMode]:
        """獲取支援的處理模式"""
        if processing_type == ProcessingType.PO:
            return [
                ProcessingMode.MODE_1,
                ProcessingMode.MODE_2,
                ProcessingMode.MODE_3,
                ProcessingMode.MODE_4
            ]
        else:
            return []


class SPTPRProcessor(EntityProcessor):
    """SPT PR處理器"""
    
    def __init__(self, entity_config: EntityConfig):
        self.entity_config = entity_config
        self.logger = Logger().get_logger(__name__)
        
        # 初始化核心處理器
        self.pr_processor = BasePRProcessor(self.entity_config.entity_type.value)
    
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
                raise ValueError(f"PR不支援的處理模式: {mode}")
                
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
        """SPT PR處理器不處理PO"""
        raise NotImplementedError("PR處理器不處理PO數據")
    
    def get_supported_modes(self, processing_type: ProcessingType) -> List[ProcessingMode]:
        """獲取支援的處理模式"""
        if processing_type == ProcessingType.PR:
            return [
                ProcessingMode.MODE_1,
                ProcessingMode.MODE_2
            ]
        else:
            return []


class SPTEntity(BaseEntity):
    """SPT實體"""
    
    def __init__(self, config: Optional[EntityConfig] = None):
        super().__init__(EntityType.SPT, config)
        self._initialize_processors()
    
    def _create_default_config(self) -> EntityConfig:
        """創建SPT預設配置"""
        return create_default_entity_config(EntityType.SPT)
    
    def _initialize_processors(self):
        """初始化處理器"""
        self._po_processor = SPTPOProcessor(self.config)
        self._pr_processor = SPTPRProcessor(self.config)
    
    def get_entity_name(self) -> str:
        """獲取實體名稱"""
        return "SPTTW"
    
    def get_entity_description(self) -> str:
        """獲取實體描述"""
        return "Super Micro Computer Taiwan PO/PR 處理實體"
    
    def validate_spt_specific_requirements(self, files: ProcessingFiles, mode: ProcessingMode) -> bool:
        """驗證SPT特有的需求"""
        # SPT特有的驗證邏輯
        self.logger.info(f"驗證SPT特有需求: {mode.value}")
        
        # 檢查SPT特有的FA帳戶
        spt_fa_accounts = ["1410", "1420", "1610", "1640", "1650"]
        if set(spt_fa_accounts) != set(self.config.fa_accounts):
            self.logger.warning("SPT FA帳戶配置與預期不符")
        
        return True
    
    def get_spt_specific_settings(self) -> Dict[str, Any]:
        """獲取SPT特有設定"""
        return {
            "entity_code": "SPT",
            "company_name": "Super Micro Computer Taiwan",
            "accounting_system": "ERP",
            "special_processing_rules": {
                "enable_multi_currency": True,
                "default_exchange_rate": "月底匯率",
                "require_department_validation": True
            }
        }
    
    # 為了向後相容，保留原始的方法名稱
    def mode_1(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_p: str, fileUrl_c: str) -> ProcessingResult:
        """向後相容的PO模式1方法"""
        return self.process_po_mode_1(fileUrl, file_name, fileUrl_previwp, fileUrl_p, fileUrl_c)
    
    def mode_2(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_p: str) -> ProcessingResult:
        """向後相容的PO模式2方法"""
        return self.process_po_mode_2(fileUrl, file_name, fileUrl_previwp, fileUrl_p)
    
    def mode_3(self, fileUrl: str, file_name: str, fileUrl_previwp: str) -> ProcessingResult:
        """向後相容的PO模式3方法"""
        return self.process_po_mode_3(fileUrl, file_name, fileUrl_previwp)
    
    def mode_4(self, fileUrl: str, file_name: str) -> ProcessingResult:
        """向後相容的PO模式4方法"""
        return self.process_po_mode_4(fileUrl, file_name)


# 向後相容的類別別名
SPTTW_PO = SPTEntity
SPTTW_PR = SPTEntity


def create_spt_entity(config: Optional[EntityConfig] = None) -> SPTEntity:
    """創建SPT實體的便捷函數"""
    return SPTEntity(config)
