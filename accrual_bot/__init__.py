"""
Accrual Bot v2.0
企業應收應付帳款自動化處理系統

Pipeline-based architecture:
- core.pipeline: 處理流程框架
- tasks: 實體特定的Pipeline編排 (SPT, SPX)
- data: Google Sheets 導入器
- utils: 配置管理、日誌處理
- ui: Streamlit Web介面
"""

__version__ = "2.0.0"
__author__ = "lia@sea.com"
__description__ = "Enterprise Accounting Processing Automation System with Pipeline"

from .utils import *
from .data import *

try:
    from .utils.logging import get_logger
    logger = get_logger('accrual_bot')
    logger.info(f"Accrual Bot v{__version__} initialized")
except ImportError as err:
    print(err)
    pass

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

    # 數據導入器
    'BaseDataImporter',
    'GoogleSheetsImporter',
    'AsyncGoogleSheetsImporter',

    # 常數和配置
    'ENTITY_TYPES',
    'PROCESSING_MODES',
    'STATUS_VALUES',
    'REGEX_PATTERNS',
]
