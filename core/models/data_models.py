"""
核心數據模型定義

定義系統中使用的主要數據結構，包括PO、PR數據以及處理結果
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum
import pandas as pd


class EntityType(Enum):
    """實體類型枚舉"""
    MOB = "MOB"
    SPT = "SPT"
    SPX = "SPX"


class ProcessingType(Enum):
    """處理類型枚舉"""
    PO = "PO"
    PR = "PR"


class ValidationStatus(Enum):
    """驗證狀態枚舉"""
    VALID = "valid"
    INVALID = "invalid"
    WARNING = "warning"


@dataclass
class FieldMapping:
    """欄位映射配置"""
    source_column: str
    target_column: str
    data_type: str = "string"
    required: bool = True
    validation_rules: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.validation_rules is None:
            self.validation_rules = {}


@dataclass
class ValidationResult:
    """數據驗證結果"""
    status: ValidationStatus
    message: str
    field: Optional[str] = None
    row_index: Optional[int] = None
    details: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


@dataclass
class POData:
    """PO數據模型"""
    # 基本識別資訊
    po_number: str
    line_number: str
    pr_number: Optional[str] = None
    
    # 會計相關
    account_code: str = ""
    department: str = ""
    currency: str = "TWD"
    
    # 數量和金額
    entry_quantity: float = 0.0
    billed_quantity: float = 0.0
    entry_amount: float = 0.0
    entry_billed_amount: float = 0.0
    
    # 狀態
    closed_for_invoice: str = "0"
    is_closed: Optional[str] = None
    quantity_difference: Optional[Union[str, float]] = None
    invoice_check: Optional[Union[str, float]] = None
    
    # 日期
    po_date: Optional[datetime] = None
    receipt_date: Optional[datetime] = None
    
    # 組合欄位
    pr_line: Optional[str] = None
    po_line: Optional[str] = None
    
    # 額外屬性
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """數據後處理"""
        # 生成組合欄位
        if self.pr_number and self.line_number:
            self.pr_line = f"{self.pr_number}-{self.line_number}"
        
        if self.po_number and self.line_number:
            self.po_line = f"{self.po_number}-{self.line_number}"
        
        # 計算衍生欄位
        self._calculate_derived_fields()
    
    def _calculate_derived_fields(self):
        """計算衍生欄位"""
        # 判斷是否結案
        self.is_closed = "結案" if self.closed_for_invoice != "0" else "未結案"
        
        # 計算數量差異
        if self.is_closed == "結案":
            self.quantity_difference = self.entry_quantity - self.billed_quantity
        else:
            self.quantity_difference = "未結案"
        
        # 計算發票檢查
        if self.entry_billed_amount > 0:
            self.invoice_check = self.entry_amount - self.entry_billed_amount
        else:
            self.invoice_check = "未入帳"


@dataclass
class PRData:
    """PR數據模型"""
    # 基本識別資訊
    pr_number: str
    line_number: str
    
    # 會計相關
    account_code: str = ""
    department: str = ""
    currency: str = "TWD"
    
    # 數量和金額
    quantity: float = 0.0
    amount: float = 0.0
    
    # 狀態
    status: str = ""
    
    # 日期
    pr_date: Optional[datetime] = None
    
    # 組合欄位
    pr_line: Optional[str] = None
    
    # 額外屬性
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """數據後處理"""
        # 生成組合欄位
        if self.pr_number and self.line_number:
            self.pr_line = f"{self.pr_number}-{self.line_number}"


@dataclass
class ProcessingResult:
    """處理結果模型"""
    success: bool
    message: str
    
    # 處理的數據
    processed_data: Optional[pd.DataFrame] = None
    
    # 統計資訊
    total_records: int = 0
    processed_records: int = 0
    error_records: int = 0
    
    # 驗證結果
    validation_results: List[ValidationResult] = field(default_factory=list)
    
    # 處理時間
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    # 匯出檔案路徑
    output_files: List[str] = field(default_factory=list)
    
    # 錯誤詳情
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # 額外資訊
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def processing_time(self) -> Optional[float]:
        """計算處理時間（秒）"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """計算成功率"""
        if self.total_records == 0:
            return 0.0
        return (self.processed_records / self.total_records) * 100
    
    def add_validation_result(self, result: ValidationResult):
        """添加驗證結果"""
        self.validation_results.append(result)
        
        if result.status == ValidationStatus.INVALID:
            self.error_records += 1
            self.errors.append(result.message)
        elif result.status == ValidationStatus.WARNING:
            self.warnings.append(result.message)
    
    def add_error(self, error: str):
        """添加錯誤"""
        self.errors.append(error)
        self.error_records += 1
    
    def add_warning(self, warning: str):
        """添加警告"""
        self.warnings.append(warning)
    
    def get_summary(self) -> Dict[str, Any]:
        """獲取處理摘要"""
        return {
            "success": self.success,
            "message": self.message,
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "error_records": self.error_records,
            "success_rate": self.success_rate,
            "processing_time": self.processing_time,
            "validation_errors": len([r for r in self.validation_results 
                                   if r.status == ValidationStatus.INVALID]),
            "validation_warnings": len([r for r in self.validation_results 
                                     if r.status == ValidationStatus.WARNING]),
            "output_files": self.output_files
        }


def create_po_data_from_row(row: pd.Series, field_mapping: Optional[Dict[str, str]] = None) -> POData:
    """從pandas行創建POData對象
    
    Args:
        row: pandas DataFrame的行
        field_mapping: 欄位映射字典，key為POData欄位名，value為DataFrame列名
    
    Returns:
        POData: 創建的POData對象
    """
    if field_mapping is None:
        field_mapping = {}
    
    # 默認欄位映射
    default_mapping = {
        'po_number': 'PO#',
        'line_number': 'Line#',
        'pr_number': 'PR#',
        'account_code': 'Account',
        'department': 'Department',
        'currency': 'Currency',
        'entry_quantity': 'Entry Quantity',
        'billed_quantity': 'Billed Quantity',
        'entry_amount': 'Entry Amount',
        'entry_billed_amount': 'Entry Billed Amount',
        'closed_for_invoice': 'Closed For Invoice'
    }
    
    # 合併映射
    mapping = {**default_mapping, **field_mapping}
    
    # 安全獲取值的函數
    def safe_get(key: str, default=None):
        col_name = mapping.get(key, key)
        if col_name in row.index:
            value = row[col_name]
            return value if pd.notna(value) else default
        return default
    
    return POData(
        po_number=str(safe_get('po_number', '')),
        line_number=str(safe_get('line_number', '')),
        pr_number=str(safe_get('pr_number', '')),
        account_code=str(safe_get('account_code', '')),
        department=str(safe_get('department', '')),
        currency=str(safe_get('currency', 'TWD')),
        entry_quantity=float(safe_get('entry_quantity', 0)),
        billed_quantity=float(safe_get('billed_quantity', 0)),
        entry_amount=float(safe_get('entry_amount', 0)),
        entry_billed_amount=float(safe_get('entry_billed_amount', 0)),
        closed_for_invoice=str(safe_get('closed_for_invoice', '0'))
    )


def create_pr_data_from_row(row: pd.Series, field_mapping: Optional[Dict[str, str]] = None) -> PRData:
    """從pandas行創建PRData對象
    
    Args:
        row: pandas DataFrame的行
        field_mapping: 欄位映射字典
    
    Returns:
        PRData: 創建的PRData對象
    """
    if field_mapping is None:
        field_mapping = {}
    
    # 默認欄位映射
    default_mapping = {
        'pr_number': 'PR#',
        'line_number': 'Line#',
        'account_code': 'Account',
        'department': 'Department',
        'currency': 'Currency',
        'quantity': 'Quantity',
        'amount': 'Amount',
        'status': 'Status'
    }
    
    # 合併映射
    mapping = {**default_mapping, **field_mapping}
    
    # 安全獲取值的函數
    def safe_get(key: str, default=None):
        col_name = mapping.get(key, key)
        if col_name in row.index:
            value = row[col_name]
            return value if pd.notna(value) else default
        return default
    
    return PRData(
        pr_number=str(safe_get('pr_number', '')),
        line_number=str(safe_get('line_number', '')),
        account_code=str(safe_get('account_code', '')),
        department=str(safe_get('department', '')),
        currency=str(safe_get('currency', 'TWD')),
        quantity=float(safe_get('quantity', 0)),
        amount=float(safe_get('amount', 0)),
        status=str(safe_get('status', ''))
    )
