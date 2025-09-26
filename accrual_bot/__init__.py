"""
Accrual Bot v2.0
企業應收應付帳款自動化處理系統

重構版本，採用模組化設計，提供：
- 工具模組 (utils): 配置管理、日誌處理、幫助函數
- 核心模組 (core): 業務邏輯處理器
- Pipeline模組 (core.pipeline): 靈活的處理流程
- 數據模組 (data): 數據導入、匯出和轉換

主要特點：
- 模組化架構，便於維護和擴展
- 靈活的Pipeline處理流程
- 並發處理，提升效能
- 完整的錯誤處理和日誌記錄
- 支援多種數據來源和格式
- 可配置的業務邏輯
"""

# 版本信息
__version__ = "2.0.0"
__author__ = "lia@sea.com"
__description__ = "Enterprise Accounting Processing Automation System with Pipeline"

# 不自動導入所有模組，避免循環依賴
# 使用者需要明確導入所需模組
# # 核心模組導入
# from .utils import *
# from .core import *
# from .data import *

# 基本導入（安全的）
try:
    from .utils.logging import get_logger
    logger = get_logger('accrual_bot')
    logger.info(f"Accrual Bot v{__version__} initialized")
except ImportError as err:
    print(err)
    pass

# 主要組件清單（參考）
__all__ = [
    # 版本信息
    '__version__',
    '__author__', 
    '__description__',

    # 工具模組
    'ConfigManager',
    'config_manager',
    'Logger',
    'get_logger',
    'get_structured_logger',
    
    # 核心處理器
    'BaseDataProcessor',
    'BasePOProcessor',
    'BasePRProcessor',

    # 業務實體
    'create_entity',
    'create_entity_by_name',
    'EntityType',
    'ProcessingType'
    
    # 數據導入器
    'BaseDataImporter',
    'GoogleSheetsImporter',
    'ExcelImporter',
    
    # 常數和配置
    'ENTITY_TYPES',
    'PROCESSING_MODES',
    'STATUS_VALUES',
    'REGEX_PATTERNS'
]
