"""
系統常數定義
包含所有固定值和配置常數
"""

# 檔案格式常數
SUPPORTED_FILE_EXTENSIONS = ['.xlsx', '.xls', '.csv']
EXCEL_EXTENSIONS = ['.xlsx', '.xls']
CSV_EXTENSIONS = ['.csv']

# 業務實體類型
ENTITY_TYPES = {
    'MOB': 'MOBTW',
    'SPT': 'SPTTW', 
    'SPX': 'SPXTW'
}

# 處理模式
PROCESSING_MODES = {
    'MODE_1': 'raw+previous+procurement+closing',
    'MODE_2': 'raw+previous+procurement',
    'MODE_3': 'raw+previous+closing',
    'MODE_4': 'raw+procurement+closing',
    'MODE_5': 'raw+procurement',
    'MODE_6': 'raw+previous',
    'MODE_7': 'raw+closing',
    'MODE_8': 'raw_only'
}

# 數據欄位常數
COMMON_COLUMNS = {
    'PR_LINE': 'PR Line',
    'PO_LINE': 'PO Line',
    'ACCR_AMOUNT': 'Accr. Amount',
    'ACCOUNT_CODE': 'Account code',
    'ACCOUNT_NAME': 'Account Name',
    'PRODUCT_CODE': 'Product code',
    'CURRENCY': 'Currency',
    'DEPARTMENT': 'Department',
    'REGION': 'Region',
    'LIABILITY': 'Liability'
}

# 狀態值常數
STATUS_VALUES = {
    'COMPLETED': '已完成',
    'INCOMPLETE': '未完成',
    'CLOSED': '已關單',
    'TO_BE_CLOSED': '待關單',
    'POSTED': '已入帳',
    'FORMAT_ERROR': '格式錯誤',
    'NOT_IN_PROCUREMENT': 'Not In Procurement WP',
    'CHECK_RECEIPT': 'Check收貨',
    'PAID_NOT_CLOSED': '全付完，未關單?',
    'ERROR_OUT_OF_ERM': 'error(Description Period is out of ERM)'
}

# 正規表達式模式
REGEX_PATTERNS = {
    'DATE_YM': r'(\d{4}\/(0[1-9]|1[0-2])(\s|$))',
    'DATE_YMD': r'(\d{4}\/(0[1-9]|1[0-2])\/((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))(\s|$))',
    'DATE_YM_TO_YM': r'(\d{4}\/(0[1-9]|1[0-2])[-]\d{4}\/(0[1-9]|1[0-2])(\s|$))',
    'DATE_YMD_TO_YMD': r'(\d{4}\/(0[1-9]|1[0-2])\/((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))[-]\d{4}\/(0[1-9]|1[0-2])\/((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))(\s|$))',
    'PRODUCT_CODE': r'^(\w+(?:))',
    'DATE_FORMAT_CONVERSION': r'(\d{4})/(\d{2})',
    'FA_REMARK': r'(\d{6}入FA)'
}

# 預設日期範圍（用於格式錯誤時）
DEFAULT_DATE_RANGE = '100001,100002'

# Excel 格式化相關
EXCEL_FORMAT = {
    'ENCODING': 'utf-8-sig',
    'ENGINE': 'xlsxwriter',
    'INDEX': False
}

# 並發處理設定
CONCURRENT_SETTINGS = {
    'MAX_WORKERS': 5,
    'TIMEOUT': 300  # 5 minutes
}

# Google Sheets 相關
GOOGLE_SHEETS = {
    'DEFAULT_SCOPES': ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    'CREDENTIALS_FILE': 'credentials.json'
}

# SPX 特定常數
SPX_CONSTANTS = {
    'CLOSING_SHEET_ID': '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE',
    'CLOSING_SHEETS': ['2023年_done', '2024年', '2025年'],
    'CLOSING_RANGE': 'A:J',
    'PRODUCT_CODE_FILTER': r'(?i)LG_SPX',
    'DEFAULT_PRODUCT_CODE': 'LG_SPX_OWN',
    'DEFAULT_REGION': 'TW',
    'DEPT_ACCOUNTS': ['650005', '610104', '630001', '650003', '600301', 
                      '610110', '610105', '600310', '620003', '610311'],
    'BAO_CATEGORIES': ['Cost of Logistics and Warehouse - Water', 
                       'Cost of Logistics and Warehouse - Electricity'],
    'BAO_SUPPLIER': 'TW_寶倉物流股份有限公司'
}

# 記憶體和效能優化
PERFORMANCE_SETTINGS = {
    'CHUNK_SIZE': 10000,
    'MAX_MEMORY_USAGE': '2GB'
}
