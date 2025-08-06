"""
SPX實體處理器

整合SPXTW公司的PO和PR處理邏輯，包含SPX特有的複雜處理流程
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from .base_entity import BaseEntity, EntityProcessor, ProcessingFiles, ProcessingMode

try:
    from ...core.models.data_models import EntityType, ProcessingType, ProcessingResult
    from ...core.models.config_models import EntityConfig, create_default_entity_config
    from ...core.processors.spx_processor import SpxPOProcessor
    from ...core.processors.pr_processor import PRProcessor
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
    from core.processors.spx_processor import SpxPOProcessor
    from core.processors.pr_processor import PRProcessor
    from utils.logging import Logger


class SPXPOProcessor(EntityProcessor):
    """SPX PO處理器"""
    
    def __init__(self, entity_config: EntityConfig):
        self.entity_config = entity_config
        self.logger = Logger().get_logger(__name__)
        
        # 初始化SPX專用處理器
        self.spx_processor = SpxPOProcessor(EntityType.SPX)
    
    def process_po(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PO數據"""
        try:
            self.logger.info(f"SPX PO {mode.value}: {files.raw_data_filename}")
            
            # 根據模式調用相應的處理方法
            if mode == ProcessingMode.MODE_1:
                return self._process_mode_1(files)
            elif mode == ProcessingMode.MODE_2:
                return self._process_mode_2(files)
            elif mode == ProcessingMode.MODE_3:
                return self._process_mode_3(files)
            elif mode == ProcessingMode.MODE_4:
                return self._process_mode_4(files)
            elif mode == ProcessingMode.MODE_5:
                return self._process_mode_5(files)
            else:
                raise ValueError(f"不支援的處理模式: {mode}")
                
        except Exception as e:
            self.logger.error(f"SPX PO {mode.value}處理失敗: {e}")
            return ProcessingResult(
                success=False,
                message=f"處理失敗: {str(e)}",
                start_time=datetime.now(),
                end_time=datetime.now()
            )
    
    def _process_mode_1(self, files: ProcessingFiles) -> ProcessingResult:
        """模式1：SPX完整處理流程（包含AP invoice和PR處理）"""
        # SPX模式1需要處理PO + AP invoice + PR
        return self.spx_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper,
            files.procurement_file,
            files.ap_invoice_file,
            files.previous_workpaper_pr,
            files.procurement_file_pr
        )
    
    def _process_mode_2(self, files: ProcessingFiles) -> ProcessingResult:
        """模式2：標準處理（無AP invoice）"""
        return self.spx_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper,
            files.procurement_file
        )
    
    def _process_mode_3(self, files: ProcessingFiles) -> ProcessingResult:
        """模式3：基礎處理"""
        return self.spx_processor.process(
            files.raw_data_file,
            files.raw_data_filename,
            files.previous_workpaper
        )
    
    def _process_mode_4(self, files: ProcessingFiles) -> ProcessingResult:
        """模式4：僅原始數據"""
        return self.spx_processor.process(
            files.raw_data_file,
            files.raw_data_filename
        )
    
    def _process_mode_5(self, files: ProcessingFiles) -> ProcessingResult:
        """模式5：SPX採購專用模式"""
        # 採購用模式：PO + 自己的底稿 + (關單)OPTIONAL
        return self.spx_processor.process_procurement_mode(
            files.raw_data_file,
            files.raw_data_filename,
            files.procurement_file,
            files.closing_list
        )
    
    def process_concurrent_spx(self, file_paths: Dict[str, str]) -> ProcessingResult:
        """SPX並發處理方法"""
        try:
            self.logger.info("開始SPX並發處理")
            return self.spx_processor.concurrent_spx_process(file_paths)
        except Exception as e:
            self.logger.error(f"SPX並發處理失敗: {e}")
            return ProcessingResult(
                success=False,
                message=f"並發處理失敗: {str(e)}",
                start_time=datetime.now(),
                end_time=datetime.now()
            )
    
    def process_pr(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """SPX PO處理器不處理PR（但模式1會整合PR處理）"""
        raise NotImplementedError("PO處理器不直接處理PR數據，請使用模式1進行整合處理")
    
    def get_supported_modes(self, processing_type: ProcessingType) -> List[ProcessingMode]:
        """獲取支援的處理模式"""
        if processing_type == ProcessingType.PO:
            return [
                ProcessingMode.MODE_1,  # 完整模式（含AP invoice和PR）
                ProcessingMode.MODE_2,  # 標準模式
                ProcessingMode.MODE_3,  # 基礎模式
                ProcessingMode.MODE_4,  # 僅原始數據
                ProcessingMode.MODE_5   # 採購專用模式
            ]
        else:
            return []


class SPXPRProcessor(EntityProcessor):
    """SPX PR處理器"""
    
    def __init__(self, entity_config: EntityConfig):
        self.entity_config = entity_config
        self.logger = Logger().get_logger(__name__)
        
        # 初始化核心處理器
        self.pr_processor = PRProcessor(EntityType.SPX)
    
    def process_pr(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PR數據"""
        try:
            self.logger.info(f"SPX PR {mode.value}: {files.raw_data_filename}")
            
            # 根據模式調用相應的處理方法
            if mode == ProcessingMode.MODE_1:
                return self._process_mode_1(files)
            elif mode == ProcessingMode.MODE_2:
                return self._process_mode_2(files)
            else:
                raise ValueError(f"PR不支援的處理模式: {mode}")
                
        except Exception as e:
            self.logger.error(f"SPX PR {mode.value}處理失敗: {e}")
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
        """SPX PR處理器不處理PO"""
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


class SPXEntity(BaseEntity):
    """SPX實體"""
    
    def __init__(self, config: Optional[EntityConfig] = None):
        super().__init__(EntityType.SPX, config)
        self._initialize_processors()
    
    def _create_default_config(self) -> EntityConfig:
        """創建SPX預設配置"""
        config = create_default_entity_config(EntityType.SPX)
        # 添加SPX特有設定
        config.special_handling.update({
            "enable_spx_processing": True,
            "spx_closing_list_required": True,
            "enable_ap_invoice_processing": True,
            "enable_concurrent_processing": True
        })
        return config
    
    def _initialize_processors(self):
        """初始化處理器"""
        self._po_processor = SPXPOProcessor(self.config)
        self._pr_processor = SPXPRProcessor(self.config)
    
    def get_entity_name(self) -> str:
        """獲取實體名稱"""
        return "SPXTW"
    
    def get_entity_description(self) -> str:
        """獲取實體描述"""
        return "Super Micro Computer SPX Taiwan PO/PR 處理實體（含特殊業務邏輯）"
    
    def process_po_mode_1_spx(self, raw_data_file: str, filename: str,
                              previous_workpaper: str, procurement_file: str,
                              ap_invoice_file: str, previous_workpaper_pr: str,
                              procurement_file_pr: str) -> ProcessingResult:
        """
        SPX特有的PO模式1：包含AP invoice和PR處理
        
        Args:
            raw_data_file: PO原始數據文件路徑
            filename: PO原始數據文件名
            previous_workpaper: 前期底稿文件路徑
            procurement_file: 採購底稿文件路徑
            ap_invoice_file: AP invoice文件路徑
            previous_workpaper_pr: 前期PR底稿文件路徑
            procurement_file_pr: 採購PR底稿文件路徑
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename,
            previous_workpaper=previous_workpaper,
            procurement_file=procurement_file,
            ap_invoice_file=ap_invoice_file,
            previous_workpaper_pr=previous_workpaper_pr,
            procurement_file_pr=procurement_file_pr
        )
        
        return self.po_processor.process_po(files, ProcessingMode.MODE_1)
    
    def process_po_mode_5(self, raw_data_file: str, filename: str,
                          procurement_file: str, closing_list: Optional[str] = None) -> ProcessingResult:
        """
        SPX採購專用模式：PO + 自己的底稿 + (關單)OPTIONAL
        
        Args:
            raw_data_file: PO原始數據文件路徑
            filename: PO原始數據文件名
            procurement_file: 採購底稿文件路徑
            closing_list: 關單清單文件路徑（可選）
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename,
            procurement_file=procurement_file,
            closing_list=closing_list
        )
        
        return self.po_processor.process_po(files, ProcessingMode.MODE_5)
    
    def concurrent_spx_process(self, file_paths: Dict[str, str]) -> ProcessingResult:
        """
        SPX並發處理方法
        
        Args:
            file_paths: 檔案路徑字典
                - po_file: PO檔案路徑
                - po_file_name: PO檔案名稱
                - previous_wp: 前期底稿
                - procurement: 採購底稿
                - ap_invoice: AP invoice
                - previous_wp_pr: 前期PR底稿
                - procurement_pr: 採購PR底稿
                
        Returns:
            ProcessingResult: 處理結果
        """
        return self.po_processor.process_concurrent_spx(file_paths)
    
    def validate_spx_specific_requirements(self, files: ProcessingFiles, mode: ProcessingMode) -> bool:
        """驗證SPX特有的需求"""
        self.logger.info(f"驗證SPX特有需求: {mode.value}")
        
        # SPX特有驗證
        if mode == ProcessingMode.MODE_1:
            # 模式1需要AP invoice檔案
            if not files.ap_invoice_file:
                self.logger.error("SPX模式1需要AP invoice檔案")
                return False
        
        # 檢查SPX特有的FA帳戶
        spx_fa_accounts = ["1410", "1420", "1610", "1640", "1650"]
        if set(spx_fa_accounts) != set(self.config.fa_accounts):
            self.logger.warning("SPX FA帳戶配置與預期不符")
        
        return True
    
    def get_spx_specific_settings(self) -> Dict[str, Any]:
        """獲取SPX特有設定"""
        return {
            "entity_code": "SPX",
            "company_name": "Super Micro Computer SPX Taiwan",
            "accounting_system": "SAP",
            "special_processing_rules": {
                "enable_spx_processing": True,
                "require_ap_invoice": True,
                "enable_concurrent_processing": True,
                "spx_closing_list_required": True,
                "enable_gl_adjustment": True,
                "support_multi_entity_processing": True
            },
            "supported_modes": {
                "po_modes": ["MODE_1", "MODE_2", "MODE_3", "MODE_4", "MODE_5"],
                "pr_modes": ["MODE_1", "MODE_2"],
                "special_modes": ["concurrent_processing"]
            }
        }
    
    # 為了向後相容，保留原始的方法名稱
    def mode_1(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_p: str, fileUrl_ap: str, fileUrl_previwp_pr: str, 
               fileUrl_p_pr: str) -> ProcessingResult:
        """向後相容的SPX PO模式1方法"""
        return self.process_po_mode_1_spx(
            fileUrl, file_name, fileUrl_previwp, fileUrl_p, 
            fileUrl_ap, fileUrl_previwp_pr, fileUrl_p_pr
        )
    
    def mode_2(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_p: str) -> ProcessingResult:
        """向後相容的SPX PO模式2方法"""
        return self.process_po_mode_2(fileUrl, file_name, fileUrl_previwp, fileUrl_p)
    
    def mode_5(self, fileUrl: str, file_name: str, fileUrl_p: str, 
               fileUrl_c: Optional[str] = None) -> ProcessingResult:
        """向後相容的SPX採購模式方法"""
        return self.process_po_mode_5(fileUrl, file_name, fileUrl_p, fileUrl_c)


# 向後相容的類別別名
SPXTW_PO = SPXEntity
SPXTW_PR = SPXEntity


def create_spx_entity(config: Optional[EntityConfig] = None) -> SPXEntity:
    """創建SPX實體的便捷函數"""
    return SPXEntity(config)
