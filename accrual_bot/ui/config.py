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

# PROCUREMENT 子類型配置 (COMBINED 暫時隱藏)
PROCUREMENT_SOURCE_TYPES: Dict[str, Dict] = {
    'PO': {
        'display_name': '僅 PO',
        'description': '僅處理採購單',
        'icon': '📋',
    },
    'PR': {
        'display_name': '僅 PR',
        'description': '僅處理請購單',
        'icon': '📝',
    },
    # 'COMBINED': {  # TODO: 待測試完成後啟用
    #     'display_name': 'PO + PR',
    #     'description': '同時處理採購單與請購單',
    #     'icon': '📑',
    # },
}

# 檔案標籤對照
FILE_LABELS: Dict[str, str] = {
    'raw_po': '採購單原始資料 (必填)',
    'raw_pr': '請購單原始資料 (必填)',
    'previous': 'FN 前期底稿 (選填)',
    'procurement_po': '採購 PO 底稿 (選填)',
    'procurement_pr': '採購 PR 底稿 (選填)',
    'procurement_previous': '採購前期底稿 (選填)',
    'ap_invoice': 'AP Invoice (選填)',
    'previous_pr': 'FN前期 PR 底稿 (選填)',
    'ops_validation': 'OPS 驗收明細 (選填)',
    'contract_filing_list': '合約歸檔清單 (必填)',
    'media_finished': '媒體使用完畢清單 (選填)',
    'media_left': '媒體剩餘量清單 (選填)',
    'media_summary': '媒體總表 (選填)',
}

# 各 entity/type 的必要檔案
REQUIRED_FILES: Dict[Tuple, List[str]] = {
    # 2-tuple keys (標準處理類型)
    ('SPT', 'PO'): ['raw_po'],
    ('SPT', 'PR'): ['raw_pr'],
    ('SPX', 'PO'): ['raw_po'],
    ('SPX', 'PR'): ['raw_pr'],
    ('SPX', 'PPE'): ['contract_filing_list'],
    # 3-tuple keys (PROCUREMENT 子類型)
    ('SPT', 'PROCUREMENT', 'PO'): ['raw_po'],
    ('SPT', 'PROCUREMENT', 'PR'): ['raw_pr'],
    # ('SPT', 'PROCUREMENT', 'COMBINED'): ['raw_po', 'raw_pr'],  # TODO: 待測試完成後啟用
}

# 各 entity/type 的選填檔案
OPTIONAL_FILES: Dict[Tuple, List[str]] = {
    # 2-tuple keys (標準處理類型)
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
    # 3-tuple keys (PROCUREMENT 子類型)
    ('SPT', 'PROCUREMENT', 'PO'): [
        'procurement_previous',
        'media_finished',
        'media_left',
        'media_summary',
    ],
    ('SPT', 'PROCUREMENT', 'PR'): [
        'procurement_previous',
        'media_finished',
        'media_left',
        'media_summary',
    ],
    # ('SPT', 'PROCUREMENT', 'COMBINED'): [  # TODO: 待測試完成後啟用
    #     'procurement_previous',
    #     'media_finished',
    #     'media_left',
    #     'media_summary',
    # ],
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


def get_file_requirements(entity: str, proc_type: str, source_type: str = "") -> Tuple[List[str], List[str]]:
    """
    獲取檔案需求

    Args:
        entity: Entity 名稱 (如 'SPT', 'SPX')
        proc_type: 處理類型 (如 'PO', 'PR', 'PROCUREMENT')
        source_type: 子類型 (僅 PROCUREMENT 使用: 'PO', 'PR', 'COMBINED')

    Returns:
        (required_files, optional_files) 元組
    """
    if proc_type == 'PROCUREMENT' and source_type:
        key = (entity, proc_type, source_type)
    else:
        key = (entity, proc_type)

    required = REQUIRED_FILES.get(key, [])
    optional = OPTIONAL_FILES.get(key, [])
    return required, optional
