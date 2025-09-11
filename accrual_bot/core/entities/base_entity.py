"""
基礎實體類別

定義所有業務實體的共同介面和基礎功能
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from pathlib import Path

try:
    from ...core.models.data_models import EntityType, ProcessingType, ProcessingResult
    from ...core.models.config_models import ProcessingConfig, EntityConfig
    from ...utils.logging import Logger
except ImportError:
    import sys
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from core.models.data_models import EntityType, ProcessingType, ProcessingResult
    from core.models.config_models import ProcessingConfig, EntityConfig
    from utils.logging import Logger


class ProcessingMode(Enum):
    """處理模式枚舉"""
    MODE_1 = "mode_1"  # 完整模式：原始數據+前期底稿+採購底稿+關單清單
    MODE_2 = "mode_2"  # 簡化模式：原始數據+前期底稿+採購底稿
    MODE_3 = "mode_3"  # 基礎模式：原始數據+前期底稿
    MODE_4 = "mode_4"  # 最簡模式：僅原始數據
    MODE_5 = "mode_5"  # SPX特殊模式：採購用模式


@dataclass
class ProcessingFiles:
    """處理檔案配置"""
    # 主要檔案
    raw_data_file: str
    raw_data_filename: str
    
    # 可選檔案
    previous_workpaper: Optional[str] = None
    procurement_file: Optional[str] = None
    closing_list: Optional[str] = None
    
    # SPX特有檔案
    ap_invoice_file: Optional[str] = None
    previous_workpaper_pr: Optional[str] = None
    procurement_file_pr: Optional[str] = None
    ops_validation: Optional[str] = None
    
    def validate(self) -> bool:
        """驗證檔案是否存在"""
        required_files = [self.raw_data_file]
        
        for file_path in required_files:
            if not Path(file_path).exists():
                return False
        
        return True
    
    def get_available_files(self) -> Dict[str, str]:
        """獲取可用的檔案"""
        files = {"raw_data": self.raw_data_file}
        
        if self.previous_workpaper and Path(self.previous_workpaper).exists():
            files["previous_workpaper"] = self.previous_workpaper
        
        if self.procurement_file and Path(self.procurement_file).exists():
            files["procurement"] = self.procurement_file
        
        if self.closing_list and Path(self.closing_list).exists():
            files["closing_list"] = self.closing_list
        
        if self.ap_invoice_file and Path(self.ap_invoice_file).exists():
            files["ap_invoice"] = self.ap_invoice_file
        
        if self.previous_workpaper_pr and Path(self.previous_workpaper_pr).exists():
            files["previous_workpaper_pr"] = self.previous_workpaper_pr
        
        if self.procurement_file_pr and Path(self.procurement_file_pr).exists():
            files["procurement_pr"] = self.procurement_file_pr
        
        return files


class EntityProcessor(ABC):
    """實體處理器介面"""
    
    @abstractmethod
    def process_po(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PO數據"""
        pass
    
    @abstractmethod
    def process_pr(self, files: ProcessingFiles, mode: ProcessingMode) -> ProcessingResult:
        """處理PR數據"""
        pass
    
    @abstractmethod
    def get_supported_modes(self, processing_type: ProcessingType) -> List[ProcessingMode]:
        """獲取支援的處理模式"""
        pass


class BaseEntity(ABC):
    """基礎實體類別"""
    
    def __init__(self, entity_type: EntityType, config: Optional[EntityConfig] = None):
        """
        初始化實體
        
        Args:
            entity_type: 實體類型
            config: 實體配置
        """
        self.entity_type = entity_type
        self.config = config or self._create_default_config()
        self.logger = Logger().get_logger(self.__class__.__name__)
        
        # 初始化處理器
        self._po_processor = None
        self._pr_processor = None
    
    @abstractmethod
    def _create_default_config(self) -> EntityConfig:
        """創建預設配置"""
        pass
    
    @abstractmethod
    def _initialize_processors(self):
        """初始化處理器"""
        pass
    
    @property
    def po_processor(self) -> EntityProcessor:
        """PO處理器"""
        if self._po_processor is None:
            self._initialize_processors()
        return self._po_processor
    
    @property
    def pr_processor(self) -> EntityProcessor:
        """PR處理器"""
        if self._pr_processor is None:
            self._initialize_processors()
        return self._pr_processor
    
    def process_po_mode_1(self, raw_data_file: str, filename: str,
                          previous_workpaper: str, procurement_file: str,
                          closing_list: str, **kwargs) -> ProcessingResult:
        """
        PO模式1：處理原始數據+前期底稿+採購底稿+關單清單
        
        Args:
            raw_data_file: PO原始數據文件路徑
            filename: PO原始數據文件名
            previous_workpaper: 前期底稿文件路徑
            procurement_file: 採購底稿文件路徑
            closing_list: 關單清單文件路徑
            **kwargs: 額外參數
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename,
            previous_workpaper=previous_workpaper,
            procurement_file=procurement_file,
            closing_list=closing_list
        )
        
        return self.po_processor.process_po(files, ProcessingMode.MODE_1)
    
    def process_po_mode_2(self, raw_data_file: str, filename: str,
                          previous_workpaper: str, procurement_file: str, **kwargs) -> ProcessingResult:
        """
        PO模式2：處理原始數據+前期底稿+採購底稿
        
        Args:
            raw_data_file: PO原始數據文件路徑
            filename: PO原始數據文件名
            previous_workpaper: 前期底稿文件路徑
            procurement_file: 採購底稿文件路徑
            **kwargs: 額外參數
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename,
            previous_workpaper=previous_workpaper,
            procurement_file=procurement_file
        )
        
        return self.po_processor.process_po(files, ProcessingMode.MODE_2)
    
    def process_po_mode_3(self, raw_data_file: str, filename: str,
                          previous_workpaper: str, **kwargs) -> ProcessingResult:
        """
        PO模式3：處理原始數據+前期底稿
        
        Args:
            raw_data_file: PO原始數據文件路徑
            filename: PO原始數據文件名
            previous_workpaper: 前期底稿文件路徑
            **kwargs: 額外參數
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename,
            previous_workpaper=previous_workpaper
        )
        
        return self.po_processor.process_po(files, ProcessingMode.MODE_3)
    
    def process_po_mode_4(self, raw_data_file: str, filename: str, **kwargs) -> ProcessingResult:
        """
        PO模式4：僅處理原始數據
        
        Args:
            raw_data_file: PO原始數據文件路徑
            filename: PO原始數據文件名
            **kwargs: 額外參數
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename
        )
        
        return self.po_processor.process_po(files, ProcessingMode.MODE_4)
    
    def process_pr_mode_1(self, raw_data_file: str, filename: str,
                          previous_workpaper: str, procurement_file: str, **kwargs) -> ProcessingResult:
        """
        PR模式1：處理原始數據+前期底稿+採購底稿
        
        Args:
            raw_data_file: PR原始數據文件路徑
            filename: PR原始數據文件名
            previous_workpaper: 前期底稿文件路徑
            procurement_file: 採購底稿文件路徑
            **kwargs: 額外參數
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename,
            previous_workpaper=previous_workpaper,
            procurement_file=procurement_file
        )
        
        return self.pr_processor.process_pr(files, ProcessingMode.MODE_1)
    
    def process_pr_mode_2(self, raw_data_file: str, filename: str,
                          previous_workpaper: str, **kwargs) -> ProcessingResult:
        """
        PR模式2：處理原始數據+前期底稿
        
        Args:
            raw_data_file: PR原始數據文件路徑
            filename: PR原始數據文件名
            previous_workpaper: 前期底稿文件路徑
            **kwargs: 額外參數
            
        Returns:
            ProcessingResult: 處理結果
        """
        files = ProcessingFiles(
            raw_data_file=raw_data_file,
            raw_data_filename=filename,
            previous_workpaper=previous_workpaper
        )
        
        return self.pr_processor.process_pr(files, ProcessingMode.MODE_2)
    
    def get_entity_info(self) -> Dict[str, Any]:
        """獲取實體信息"""
        return {
            "entity_type": self.entity_type.value,
            "entity_name": self.config.entity_name,
            "supported_currencies": self.config.supported_currencies,
            "default_currency": self.config.default_currency,
            "fa_accounts": self.config.fa_accounts,
            "departments": self.config.departments,
            "po_supported_modes": [mode.value for mode in self.po_processor.get_supported_modes(ProcessingType.PO)],
            "pr_supported_modes": [mode.value for mode in self.pr_processor.get_supported_modes(ProcessingType.PR)]
        }
    
    def validate_processing_files(self, files: ProcessingFiles, mode: ProcessingMode) -> bool:
        """
        驗證處理檔案
        
        Args:
            files: 處理檔案配置
            mode: 處理模式
            
        Returns:
            bool: 是否有效
        """
        # 基本檔案驗證
        if not files.validate():
            self.logger.error("基本檔案驗證失敗")
            return False
        
        # 根據模式驗證必要檔案
        if mode == ProcessingMode.MODE_1:
            required_files = [files.previous_workpaper, files.procurement_file, files.closing_list]
            if not all(file and Path(file).exists() for file in required_files):
                self.logger.error("MODE_1 需要前期底稿、採購底稿和關單清單")
                return False
        
        elif mode == ProcessingMode.MODE_2:
            required_files = [files.previous_workpaper, files.procurement_file]
            if not all(file and Path(file).exists() for file in required_files):
                self.logger.error("MODE_2 需要前期底稿和採購底稿")
                return False
        
        elif mode == ProcessingMode.MODE_3:
            if not (files.previous_workpaper and Path(files.previous_workpaper).exists()):
                self.logger.error("MODE_3 需要前期底稿")
                return False
        
        return True
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.entity_type.value})"
    
    def __repr__(self) -> str:
        return self.__str__()
