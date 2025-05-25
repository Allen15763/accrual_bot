# src/ui_strings.py
# Central storage for UI strings

STRINGS = {
    # Window Titles
    "WINDOW_TITLE_MAIN": "POPR BOT",
    "WINDOW_TITLE_UPLOAD_FORM": "Upload Form",
    "WINDOW_TITLE_HRIS_DIALOG": "產生的PR/PO/AP invoice",
    "WINDOW_TITLE_RAW_DATA_DIALOG": "原始數據",
    "WINDOW_TITLE_CLOSING_LIST_DIALOG": "關單清單",
    "WINDOW_TITLE_PREVIOUS_WP_DIALOG": "前期底稿",
    "WINDOW_TITLE_PROCUREMENT_WP_DIALOG": "採購底稿",
    "WINDOW_TITLE_PURCHASE_PR_DIALOG": "採購PR", # For check2
    "WINDOW_TITLE_PURCHASE_PO_DIALOG": "採購PO", # For check2
    "WINDOW_TITLE_PREVIOUS_WP_XLSM_DIALOG": "前期底稿", # For check2, specifically .xlsm

    # Tab Names
    "TAB_MAIN": "主功能",
    "TAB_LOG": "日誌",
    "TAB_SPX": "SPX模組",

    # Main UI Buttons & Labels
    "MAIN_LBL_IMPORTED_FILES_TITLE": "已匯入檔案",
    "MAIN_BTN_IMPORT_RAW": "原始資料",
    "MAIN_BTN_IMPORT_CLOSING_LIST": "關單清單",
    "MAIN_BTN_IMPORT_PREVIOUS_WP": "前期底稿",
    "MAIN_BTN_IMPORT_PROCUREMENT": "採購底稿",
    "MAIN_BTN_PROCESS": "匯出",
    "MAIN_BTN_DELETE_IMPORTED": "刪除已匯入資料",
    "MAIN_BTN_HRIS_CHECK": "HRIS重複檢查",
    "MAIN_BTN_UPLOAD_FORM": "Upload Form",
    "MAIN_BTN_CHECK2": "兩期檢查",
    "MAIN_LBL_STATUS_PREFIX": "狀態: ",

    # Common Status Messages (to be appended to LBL_STATUS_PREFIX)
    "STATUS_READY": "準備就緒",
    "STATUS_PROCESSING_DOTS": "處理中...", # Generic
    "STATUS_FILE_NOT_SELECTED_CANCEL": "未選擇文件，取消操作",
    "STATUS_PROCESSING_HRIS": "處理HRIS重複檢查...",
    "STATUS_HRIS_COMPLETE": "HRIS重複檢查完成",
    "STATUS_HRIS_ERROR_PROCESSING_FAILED": "錯誤: 重複檢查失敗",
    "STATUS_HRIS_ERROR_FILE_READ_FAILED": "錯誤: 文件讀取失敗",
    "STATUS_HRIS_ERROR_DURING_PROCESSING": "錯誤: 處理過程中出錯",
    "STATUS_IMPORTING_RAW_DATA": "導入原始數據...",
    "STATUS_WARN_RAW_DATA_EXISTS": "警告: 已導入原始資料",
    "STATUS_ERR_FILENAME_FORMAT_YYYYMM": "錯誤: 文件名格式不正確",
    "STATUS_FILE_IMPORTED_PREFIX": "已導入: ", # e.g., "已導入: filename.xlsx"
    "STATUS_ERR_IMPORTING_RAW_DATA": "錯誤: 導入原始數據時出錯",
    "STATUS_IMPORTING_CLOSING_LIST": "導入關單清單...",
    "STATUS_WARN_CLOSING_LIST_EXISTS": "警告: 已導入關單清單",
    "STATUS_ERR_IMPORTING_CLOSING_LIST": "錯誤: 導入關單清單時出錯",
    "STATUS_IMPORTING_PREVIOUS_WP": "導入前期底稿...",
    "STATUS_WARN_PREVIOUS_WP_EXISTS": "警告: 已導入前期底稿",
    "STATUS_ERR_VALIDATING_PREVIOUS_WP": "錯誤: 驗證前期底稿時出錯",
    "STATUS_ERR_IMPORTING_PREVIOUS_WP": "錯誤: 導入前期底稿時出錯",
    "STATUS_IMPORTING_PROCUREMENT_WP": "導入採購底稿...",
    "STATUS_WARN_PROCUREMENT_WP_EXISTS": "警告: 已導入採購底稿",
    "STATUS_ERR_VALIDATING_PROCUREMENT_WP": "錯誤: 驗證採購底稿時出錯",
    "STATUS_ERR_IMPORTING_PROCUREMENT_WP": "錯誤: 導入採購底稿時出錯",
    "STATUS_PROCESSING_DATA": "開始處理數據...",
    "STATUS_ERR_NO_RAW_DATA_TO_PROCESS": "錯誤: 未導入原始數據",
    "STATUS_PROCESS_MOBA_PR_COMPLETE": "MOBA PR處理完成",
    "STATUS_PROCESS_MOBA_PO_COMPLETE": "MOBA PO處理完成",
    "STATUS_PROCESS_SPT_PR_COMPLETE": "SPT PR處理完成",
    "STATUS_PROCESS_SPT_PO_COMPLETE": "SPT PO處理完成",
    "STATUS_ERR_CANNOT_DETERMINE_MODE": "錯誤: 無法確定處理模式",
    "STATUS_ERR_PROCESSING_DATA": "錯誤: 處理數據時出錯",
    "STATUS_ERR_LOCKED_MODE": "錯誤: Locked",
    "STATUS_ERR_PROCESSING_MOBA_PR": "錯誤: 處理MOBA PR時出錯",
    "STATUS_ERR_PROCESSING_MOBA_PO": "錯誤: 處理MOBA PO時出錯",
    "STATUS_ERR_PROCESSING_SPT_PR": "錯誤: 處理SPT PR時出錯",
    "STATUS_ERR_PROCESSING_SPT_PO": "錯誤: 處理SPT PO時出錯",
    "STATUS_WARN_NO_ITEM_TO_DELETE": "警告: 未選擇要刪除的項目",
    "STATUS_ITEM_DELETED_PREFIX": "已刪除: ", # e.g., "已刪除: 原始資料"
    "STATUS_ERR_DELETING_ITEM": "錯誤: 刪除項目時出錯",
    "STATUS_ERR_DELETING_FILE": "錯誤: 刪除文件時出錯",
    "STATUS_OPENING_UPLOAD_FORM_DIALOG": "打開Upload Form對話框...",
    "STATUS_ERR_OPENING_UPLOAD_FORM_DIALOG": "錯誤: 打開Upload Form對話框時出錯",
    "STATUS_PERFORMING_CHECK2": "執行兩期檢查...",
    "STATUS_CHECK2_COMPLETE_FILE_SAVED": "兩期檢查完成，結果已保存到 check_dif_amount.xlsx",
    "STATUS_ERR_PERFORMING_CHECK2_COMPARE": "錯誤: 執行差異比較時出錯",
    "STATUS_ERR_PERFORMING_CHECK2": "錯誤: 兩期檢查時出錯",
    "STATUS_PROCESSING_ERROR_STOP": "處理過程中發生錯誤，停止操作",


    # QMessageBox Titles
    "MSGBOX_TITLE_ERROR": "錯誤",
    "MSGBOX_TITLE_WARNING": "警告",
    "MSGBOX_TITLE_INFO": "完成", # Also for "information"
    "MSGBOX_TITLE_CONFIRM": "警告", # For question/confirm dialogs that are warnings

    # QMessageBox Messages
    "MSGBOX_ERR_FILE_READ_FAILED_DETAILED": "文件讀取失敗:\n{}",
    "MSGBOX_INFO_HRIS_COMPLETE": "HRIS重複檢查完成",
    "MSGBOX_ERR_HRIS_FAILED_DETAILED": "重複檢查失敗:\n{}",
    "MSGBOX_ERR_PROCESSING_FAILED_DETAILED": "處理過程中出錯:\n{}", # Generic processing error
    "MSGBOX_WARN_RAW_DATA_EXISTS": "已導入原始資料",
    "MSGBOX_WARN_FILENAME_FORMAT_YYYYMM": "請檢查文件名是否包含年月(YYYYMM)",
    "MSGBOX_ERR_IMPORTING_RAW_DATA_DETAILED": "導入原始數據時出錯:\n{}",
    "MSGBOX_WARN_CLOSING_LIST_EXISTS": "已導入關單清單",
    "MSGBOX_ERR_IMPORTING_CLOSING_LIST_DETAILED": "導入關單清單時出錯:\n{}",
    "MSGBOX_WARN_PREVIOUS_WP_EXISTS": "已導入前期底稿",
    "MSGBOX_WARN_PREVIOUS_WP_FORMAT_ERROR": "請檢查前期底稿是否包含必要欄位: Remarked by Procurement, Noted by Procurement, Line#",
    "MSGBOX_ERR_VALIDATING_PREVIOUS_WP_DETAILED": "驗證前期底稿時出錯:\n{}",
    "MSGBOX_ERR_IMPORTING_PREVIOUS_WP_DETAILED": "導入前期底稿時出錯:\n{}",
    "MSGBOX_WARN_PROCUREMENT_WP_EXISTS": "已導入採購底稿",
    "MSGBOX_WARN_PROCUREMENT_WP_FORMAT_ERROR": "請檢查採購底稿是否包含必要欄位: Remarked by Procurement, Noted by Procurement, Line#",
    "MSGBOX_ERR_VALIDATING_PROCUREMENT_WP_DETAILED": "驗證採購底稿時出錯:\n{}",
    "MSGBOX_ERR_IMPORTING_PROCUREMENT_WP_DETAILED": "導入採購底稿時出錯:\n{}",
    "MSGBOX_WARN_NO_RAW_DATA": "請先導入原始數據",
    "MSGBOX_INFO_MOBA_PR_COMPLETE": "MOBA PR處理完成",
    "MSGBOX_INFO_MOBA_PO_COMPLETE": "MOBA PO處理完成",
    "MSGBOX_INFO_SPT_PR_COMPLETE": "SPT PR處理完成",
    "MSGBOX_INFO_SPT_PO_COMPLETE": "SPT PO處理完成",
    "MSGBOX_WARN_CANNOT_DETERMINE_MODE": "無法確定處理模式，請檢查選擇類型和導入文件",
    "MSGBOX_ERR_PROCESSING_DATA_DETAILED": "處理數據時出錯:\n{}",
    "MSGBOX_WARN_LOCKED_MODE": "Locked",
    "MSGBOX_ERR_MOBA_PR_DETAILED": "處理MOBA PR時出錯:\n{}",
    "MSGBOX_ERR_MOBA_PO_DETAILED": "處理MOBA PO時出錯:\n{}",
    "MSGBOX_ERR_SPT_PR_DETAILED": "處理SPT PR時出錯:\n{}",
    "MSGBOX_ERR_SPT_PO_DETAILED": "處理SPT PO時出錯:\n{}",
    "MSGBOX_WARN_SELECT_ITEM_TO_DELETE": "請選擇要刪除的項目",
    "MSGBOX_CONFIRM_DELETE_ITEM_PROMPT": "確定要刪除 {} 嗎?", # {} will be item name
    "MSGBOX_ERR_DELETING_ITEM_FAILED": "刪除項目時出錯",
    "MSGBOX_ERR_DELETING_FILE_FAILED_DETAILED": "刪除文件時出錯:\n{}",
    "MSGBOX_ERR_OPENING_UPLOAD_FORM_DIALOG_DETAILED": "打開Upload Form對話框時出錯:\n{}",
    "MSGBOX_INFO_CHECK2_COMPLETE": "兩期檢查完成，結果已保存到 check_dif_amount.xlsx",
    "MSGBOX_ERR_CHECK2_COMPARE_FAILED_DETAILED": "執行差異比較時出錯:\n{}",
    "MSGBOX_ERR_CHECK2_FAILED_DETAILED": "兩期檢查時出錯:\n{}",

    # File Dialog Filters
    "FILE_DIALOG_FILTER_EXCEL_CSV": "Files(*.csv *.xlsx);;CSV(*.csv);;EXCEL(*.xlsx)",
    "FILE_DIALOG_FILTER_EXCEL_ONLY": "EXCEL(*.xlsx)",
    "FILE_DIALOG_FILTER_EXCEL_MACRO_ENABLED": "EXCEL(*.xlsm)", # For check2 previous WP
    "FILE_DIALOG_FILTER_EXCEL_ALL_TYPES": "Files(*.xlsm *.xlsx);;EXCEL(*.xlsx *.xlsm)", # For UploadForm WP

    # UploadFormWidget specific
    "UPLOAD_FORM_LBL_ENTITY": "Entity",
    "UPLOAD_FORM_LBL_PERIOD": "Period",
    "UPLOAD_FORM_LBL_AC_DATE": "Accounting Date",
    "UPLOAD_FORM_LBL_CATEGORY": "Category",
    "UPLOAD_FORM_LBL_ACCOUNTANT": "Accountant",
    "UPLOAD_FORM_LBL_CURRENCY": "Currency",
    "UPLOAD_FORM_LBL_WP": "Working Paper",
    "UPLOAD_FORM_LBL_PROCESS": "Process",
    "UPLOAD_FORM_BTN_SELECT_WP": "Select Working Paper",
    "UPLOAD_FORM_BTN_GENERATE_UPLOAD_FORM": "Generate Upload Form",
    "UPLOAD_FORM_PLACEHOLDER_PERIOD": "JAN-24",
    "UPLOAD_FORM_PLACEHOLDER_AC_DATE": "2024/01/31",
    "UPLOAD_FORM_PLACEHOLDER_CATEGORY": "01 SEA Accrual Expense",
    "UPLOAD_FORM_TEXT_CATEGORY_DEFAULT": "01 SEA Accrual Expense",
    "UPLOAD_FORM_PLACEHOLDER_ACCOUNTANT": "Lynn",
    "UPLOAD_FORM_TEXT_ACCOUNTANT_DEFAULT": "Lynn",
    "UPLOAD_FORM_STATUS_SELECTING_WP": "狀態: 選擇工作底稿...",
    "UPLOAD_FORM_STATUS_SELECTED_WP_PREFIX": "狀態: 已選擇 ",
    "UPLOAD_FORM_STATUS_ERR_SELECTING_WP": "狀態: 錯誤 - 選擇工作底稿時出錯",
    "UPLOAD_FORM_STATUS_GENERATING": "狀態: 正在生成Upload Form...",
    "UPLOAD_FORM_STATUS_ERR_NO_WP_SELECTED": "狀態: 錯誤 - 未選擇工作底稿",
    "UPLOAD_FORM_STATUS_ERR_NO_PERIOD": "狀態: 錯誤 - 未輸入期間",
    "UPLOAD_FORM_STATUS_ERR_NO_AC_DATE": "狀態: 錯誤 - 未輸入會計日期",
    "UPLOAD_FORM_STATUS_ERR_AC_DATE_FORMAT": "狀態: 錯誤 - 會計日期格式錯誤",
    "UPLOAD_FORM_STATUS_ERR_PROCESSING_STOPPED": "狀態: 處理過程中發生錯誤，停止操作",
    "UPLOAD_FORM_STATUS_SUCCESS_PREFIX": "狀態: 已成功生成Upload Form", # Filename will be appended by UI
    "UPLOAD_FORM_STATUS_ERR_GENERATING_DETAILED": "狀態: 錯誤 - 生成Upload Form時出錯",
    "UPLOAD_FORM_STATUS_ERR_PROCESSING_DETAILED": "狀態: 錯誤 - 處理Upload Form時出錯",
    "UPLOAD_FORM_WARN_NO_WP_SELECTED": "請選擇工作底稿",
    "UPLOAD_FORM_WARN_NO_PERIOD": "請輸入期間 (例如: JAN-24)",
    "UPLOAD_FORM_WARN_NO_AC_DATE": "請輸入會計日期 (例如: 2024/01/31)",
    "UPLOAD_FORM_WARN_AC_DATE_FORMAT": "會計日期格式錯誤，應為 YYYY/MM/DD",
    "UPLOAD_FORM_INFO_GENERATED_PREFIX": "Upload Form已生成: ", # Filename will be appended by UI

    # SPXTabWidget specific
    "SPX_TAB_FILE_TYPES_PO_FILE": "原始PO數據", # Used in self.file_types
    "SPX_TAB_FILE_TYPES_PREVIOUS_WP_PO": "前期底稿(PO)",
    "SPX_TAB_FILE_TYPES_PROCUREMENT_PO": "採購底稿(PO)",
    "SPX_TAB_FILE_TYPES_AP_INVOICE": "AP發票文件",
    "SPX_TAB_FILE_TYPES_PREVIOUS_WP_PR": "前期PR底稿",
    "SPX_TAB_FILE_TYPES_PROCUREMENT_PR": "採購PR底稿",
    "SPX_GRP_FILE_UPLOAD": "文件上傳",
    "SPX_LBL_FILENAME_NOT_SELECTED": "未選擇文件",
    "SPX_BTN_SELECT_FILE": "選擇文件",
    "SPX_GRP_PROCESS_PARAMS": "處理參數",
    "SPX_LBL_PERIOD_YYYYMM": "財務年月 (YYYYMM):",
    "SPX_LBL_USER": "處理人員:",
    "SPX_PLACEHOLDER_PERIOD_YYYYMM": "例如: 202504",
    "SPX_PLACEHOLDER_USER": "例如: Blaire",
    "SPX_LBL_UPLOADED_FILES_TITLE": "已上傳文件",
    "SPX_BTN_PROCESS_GENERATE": "處理並產生結果",
    "SPX_BTN_CLEAR_ALL_FILES": "清除所有文件",
    "SPX_BTN_EXPORT_UPLOAD_FORM": "匯出上傳表單",
    "SPX_LBL_MODULE_DESCRIPTION_TITLE": "SPX模組說明:",
    "SPX_LBL_MODULE_DESCRIPTION_CONTENT": ("此模組用於處理SPX相關的PO/PR數據。\n\n"
                                           "使用步驟:\n"
                                           "1. 上傳各項必要文件(標*為必須)\n"
                                           "2. 填寫處理參數\n"
                                           "3. 點擊「處理並產生結果」\n"
                                           "4. 結果將自動保存\n\n"
                                           "TBC:PR, Upload Form\n"),
    "SPX_FILE_DIALOG_SELECT_PREFIX": "選擇", # e.g. "選擇原始PO數據"
    "SPX_FILE_DIALOG_FILTER_EXCEL_ALL": "Excel Files (*.xlsx *.xlsm)",
    "SPX_FILE_DIALOG_FILTER_DEFAULT": "Files (*.csv *.xlsx);;CSV (*.csv);;Excel (*.xlsx *.xlsm)",
    "SPX_STATUS_FILE_SELECTED_PREFIX": "狀態: 已選擇 ",
    "SPX_STATUS_ERR_SELECTING_FILE": "狀態: 選擇文件出錯",
    "SPX_WARN_MISSING_FILES_PREFIX": "缺少必要文件: ", # List of files will be appended
    "SPX_WARN_INVALID_PERIOD_FORMAT": "請輸入有效的財務年月 (格式: YYYYMM)",
    "SPX_WARN_INVALID_USER": "請輸入處理人員名稱",
    "SPX_STATUS_PROCESSING": "狀態: 處理中...",
    "SPX_STATUS_PROCESS_COMPLETE": "狀態: 處理完成!",
    "SPX_STATUS_PROCESS_ERROR": "狀態: 處理出錯",
    "SPX_INFO_PROCESS_COMPLETE": "SPX數據處理完成！",
    "SPX_STATUS_FILES_CLEARED": "狀態: 已清除所有文件",
    "SPX_STATUS_ERR_CLEARING_FILES": "狀態: 清除文件出錯",
    "SPX_WARN_NO_PO_FILE_FOR_UPLOAD_FORM": "請先選擇原始PO數據文件",
    "SPX_STATUS_EXPORTING_UPLOAD_FORM": "狀態: 正在匯出上傳表單...",
    "SPX_ENTITY_NAME_INTERNAL": "SPXTW", # For filename construction
    "SPX_CATEGORY_DEFAULT": "01 SEA Accrual Expense", # For upload form
    "SPX_CURRENCY_DEFAULT": "TWD", # For upload form
    "SPX_STATUS_UPLOAD_FORM_EXPORTED": "狀態: 上傳表單已匯出",
    "SPX_STATUS_ERR_EXPORTING_UPLOAD_FORM": "狀態: 匯出上傳表單出錯",
    "SPX_INFO_UPLOAD_FORM_GENERATED_PREFIX": "上傳表單已生成：", # Filename will be appended

    # Combo box items
    "COMBO_ENTITY_MOBAPR": "MOBA_PR",
    "COMBO_ENTITY_MOBAPO": "MOBA_PO",
    "COMBO_ENTITY_SPTPR": "SPT_PR",
    "COMBO_ENTITY_SPTPO": "SPT_PO",
    "COMBO_ENTITY_MOBTW": "MOBTW",
    "COMBO_ENTITY_SPTTW": "SPTTW",
    "COMBO_CURRENCY_TWD": "TWD",
    "COMBO_CURRENCY_USD": "USD",
    "COMBO_CURRENCY_HKD": "HKD",
}
