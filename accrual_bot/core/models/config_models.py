"""
配置模型定義

定義系統配置相關的數據結構
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
from .data_models import EntityType


@dataclass
class EntityConfig:
    """實體配置模型"""
    entity_type: EntityType
    entity_name: str
    
    # FA帳戶列表
    fa_accounts: List[str] = field(default_factory=list)
    
    # 幣別設定
    supported_currencies: List[str] = field(default_factory=lambda: ["TWD", "USD", "HKD"])
    default_currency: str = "TWD"
    
    # 部門設定
    departments: List[str] = field(default_factory=list)
    
    # 特殊處理設定
    special_handling: Dict[str, Any] = field(default_factory=dict)
    
    # 驗證規則
    validation_rules: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ExportConfig:
    """匯出配置模型"""
    # 匯出格式
    format: str = "excel"  # excel, csv, json
    
    # 檔案設定
    output_filename: str = ""
    include_timestamp: bool = True
    
    # Excel特定設定
    excel_sheet_name: str = "Data"
    include_index: bool = False
    freeze_panes: Optional[tuple] = (1, 0)  # 凍結第一行
    
    # CSV特定設定
    csv_encoding: str = "utf-8-sig"
    csv_separator: str = ","
    
    # 欄位設定
    export_columns: List[str] = field(default_factory=list)
    column_widths: Dict[str, int] = field(default_factory=dict)
    
    # 格式化設定
    number_format: str = "#,##0.00"
    date_format: str = "yyyy-mm-dd"
    
    # 樣式設定
    header_style: Dict[str, Any] = field(default_factory=dict)
    data_style: Dict[str, Any] = field(default_factory=dict)
    
    # 篩選和排序
    enable_autofilter: bool = True
    
    def __post_init__(self):
        """設定默認樣式"""
        if not self.header_style:
            self.header_style = {
                'font_bold': True,
                'bg_color': '#D9D9D9',
                'border': 1
            }
        
        if not self.data_style:
            self.data_style = {
                'border': 1,
                'align': 'left'
            }


@dataclass
class GoogleSheetsConfig:
    """Google Sheets配置模型"""
    # 認證設定
    credentials_file: str = "credentials.json"
    
    # 工作表設定
    spreadsheet_id: str = ""
    worksheet_name: str = "Sheet1"
    range_name: str = ""
    
    # 讀取設定
    include_headers: bool = True
    skip_empty_rows: bool = True
    
    # 快取設定
    enable_cache: bool = True
    cache_ttl_seconds: int = 300
    
    # 重試設定
    max_retries: int = 3
    retry_delay_seconds: float = 1.0


@dataclass
class DatabaseConfig:
    """資料庫配置模型（預留未來使用）"""
    # 連接設定
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    
    # 連接池設定
    min_connections: int = 1
    max_connections: int = 10
    connection_timeout: int = 30
    
    # 查詢設定
    query_timeout: int = 60
    enable_ssl: bool = False


def create_default_entity_config(entity_type: EntityType) -> EntityConfig:
    """創建默認實體配置
    
    Args:
        entity_type: 實體類型
    
    Returns:
        EntityConfig: 默認配置
    """
    if entity_type == EntityType.MOB:
        return EntityConfig(
            entity_type=entity_type,
            entity_name="MOBTW",
            fa_accounts=["199999", "530016", "610303"],
            departments=["HR", "IT", "Finance", "Operations"]
        )
    elif entity_type == EntityType.SPT:
        return EntityConfig(
            entity_type=entity_type,
            entity_name="SPTTW",
            fa_accounts=["199999"],
            departments=["HR", "IT", "Finance", "Sales"]
        )
    elif entity_type == EntityType.SPX:
        return EntityConfig(
            entity_type=entity_type,
            entity_name="SPXTW",
            fa_accounts=["199999"],
            departments=["HR", "IT", "Finance", "Logistics"],
            special_handling={
                "enable_spx_processing": True,
                "spx_closing_list_required": True
            }
        )
    else:
        return EntityConfig(
            entity_type=entity_type,
            entity_name=entity_type.value
        )

