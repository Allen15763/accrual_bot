"""
UI Configuration Constants

定義 UI 使用的設定常數，包含 entity 配置、檔案標籤、必填檔案清單等。
"""

from typing import Dict, List, Tuple

# Entity 配置 (MOB 暫時隱藏)
ENTITY_CONFIG: Dict[str, Dict] = {
    'SPT': {
        'display_name': 'SPT',
        'types': ['PO', 'PR', 'PROCUREMENT'],
        'description': 'SPT Platform for opened PR/PO',
        'icon': '🛒',
    },
    'SPX': {
        'display_name': 'SPX',
        'types': ['PO', 'PR', 'PPE'],
        'description': 'SPX Platform for opened PR/PO',
        'icon': '📦',
    },
}

# Processing Type 配置
PROCESSING_TYPE_CONFIG: Dict[str, Dict] = {
    'PO': {
        'display_name': '採購單 (PO)',
        'description': 'Purchase Order 處理流程',
        'icon': '📋',
    },
    'PR': {
        'display_name': '請購單 (PR)',
        'description': 'Purchase Request 處理流程',
        'icon': '📝',
    },
    'PPE': {
        'display_name': '固定資產 (PPE)',
        'description': 'Property, Plant & Equipment 處理流程',
        'icon': '🏢',
    },
    'PROCUREMENT': {
        'display_name': '採購審核 (PROCUREMENT)',
        'description': '採購人員專用處理流程，支援 PO/PR 單獨或合併處理',
        'icon': '📋',
    },
}

# 檔案標籤對照
FILE_LABELS: Dict[str, str] = {
    'raw_po': '採購單原始資料 (必填)',
    'raw_pr': '請購單原始資料 (必填)',
    'previous': '前期底稿 (選填)',
    'procurement_po': '採購系統 PO 檔 (選填)',
    'procurement_pr': '採購系統 PR 檔 (選填)',
    'procurement_previous': '採購前期底稿 (選填)',
    'ap_invoice': 'AP 發票明細 (選填)',
    'previous_pr': '前期 PR 底稿 (選填)',
    'ops_validation': 'OPS 驗收明細 (選填)',
    'contract_filing_list': '合約歸檔清單 (必填)',
    'media_finished': '媒體使用完畢清單 (選填)',
    'media_left': '媒體剩餘量清單 (選填)',
    'media_summary': '媒體總表 (選填)',
}

# 各 entity/type 的必要檔案
REQUIRED_FILES: Dict[Tuple[str, str], List[str]] = {
    ('SPT', 'PO'): ['raw_po'],
    ('SPT', 'PR'): ['raw_pr'],
    ('SPT', 'PROCUREMENT'): [],  # 至少需要 raw_po 或 raw_pr (彈性檢查)
    ('SPX', 'PO'): ['raw_po'],
    ('SPX', 'PR'): ['raw_pr'],
    ('SPX', 'PPE'): ['contract_filing_list'],
}

# 各 entity/type 的選填檔案
OPTIONAL_FILES: Dict[Tuple[str, str], List[str]] = {
    ('SPT', 'PO'): [
        'previous',
        'procurement_po',
        'ap_invoice',
        'previous_pr',
        'procurement_pr',
        'media_finished',
        'media_left',
        'media_summary',
    ],
    ('SPT', 'PR'): [
        'previous_pr',
        'procurement_pr',
        'media_finished',
        'media_left',
        'media_summary',
    ],
    ('SPT', 'PROCUREMENT'): [
        'raw_po',
        'raw_pr',
        'procurement_previous',
    ],
    ('SPX', 'PO'): [
        'previous',
        'procurement_po',
        'ap_invoice',
        'previous_pr',
        'procurement_pr',
        'ops_validation',
    ],
    ('SPX', 'PR'): [
        'previous_pr',
        'procurement_pr',
    ],
    ('SPX', 'PPE'): [],
}

# 支援的檔案格式
SUPPORTED_FILE_FORMATS: List[str] = [
    '.csv',
    '.xlsx',
    '.xls',
]

# UI 主題色彩
THEME_COLORS: Dict[str, str] = {
    'primary': '#0068C9',
    'success': '#09AB3B',
    'warning': '#FFA500',
    'error': '#FF2B2B',
    'info': '#00C0F2',
}

# Streamlit 頁面設定
PAGE_CONFIG = {
    'page_title': 'Accrual Bot',
    'page_icon': '📊',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded',
}
