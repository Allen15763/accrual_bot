import sys
import os
import re
import logging
import traceback
from datetime import datetime

import pandas as pd
from PyQt5.QtCore import Qt, QObject, pyqtSignal, pyqtSlot
from PyQt5.QtWidgets import (
    QApplication, QWidget, QMainWindow, QVBoxLayout, QHBoxLayout, 
    QLabel, QPushButton, QComboBox, QListWidget, QFileDialog, 
    QMessageBox, QDialog, QLineEdit, QGroupBox, QTextEdit, QSplitter,
    QTabWidget, QGridLayout
)
# Ensure QFontDatabase is definitely here and first in this specific import line
from PyQt5.QtGui import QFontDatabase, QPixmap, QFont, QTextCursor 
# Import qt_material AFTER PyQt5 imports
from qt_material import apply_stylesheet

# 導入處理模塊
from .ui_strings import STRINGS
from .spttwpo import SPTTW_PO
from .spttwpr import SPTTW_PR
from .mobtwpr import MOBTW_PR
from .mobtwpo import MOBTW_PO
from .hris_dup import HRISDuplicateChecker
from .upload_form import get_aggregation_twd, get_aggregation_foreign, get_entries
from .utils import Logger, ReconEntryAmt # Assuming utils is also in src


# 自定義日誌處理器，用於捕獲日誌並將其顯示在UI上
class QTextEditLogger(logging.Handler):
    def __init__(self, widget):
        super().__init__()
        self.widget = widget
        self.widget.setReadOnly(True)
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
    def emit(self, record):
        try:
            msg = self.format(record)
            self.widget.append(msg)
            # 自動滾動到底部
            self.widget.moveCursor(QTextCursor.End)
            # 強制處理事件，確保UI更新
            QApplication.processEvents()
        except Exception as e:
            print(f"日誌輸出錯誤: {e}")

# 修改為使用信號和槽的日誌處理器
class LogHandler(QObject, logging.Handler):
    new_log_signal = pyqtSignal(str)
    
    def __init__(self):
        QObject.__init__(self)
        logging.Handler.__init__(self)
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    def emit(self, record):
        try:
            msg = self.format(record)
            self.new_log_signal.emit(msg)
        except Exception as e:
            print(f"日誌輸出錯誤: {e}")


class Main(QWidget):
    """主界面類"""
    
    def __init__(self):
        """初始化主界面"""
        super().__init__()
        self.had_error = False  # 標記是否有錯誤發生
        
        # 設置窗口基本屬性
        self.setWindowTitle("POPR BOT")
        self.setGeometry(450, 150, 600, 400)
        
        # 創建基本UI組件
        self.createBasicComponents()
        
        # 設置日誌系統
        self.setupLogger()
        
        # 創建完整UI
        self.createCompleteUI()
        
        # 設置標籤切換事件
        self.setupTabEvents()

        # 顯示窗口
        self.show()

    def createBasicComponents(self):
        """創建基本UI組件，包括日誌文本框"""
        # 創建選項卡
        self.tabWidget = QTabWidget()
        
        # 主要操作頁面
        self.mainTab = QWidget()
        
        # 日誌頁面
        self.logTab = QWidget()
        self.logTextEdit = QTextEdit()
        self.logTextEdit.setReadOnly(True)
        
        # 日誌頁面布局
        self.logTabLayout = QVBoxLayout(self.logTab)
        self.logTabLayout.addWidget(self.logTextEdit)

    def createCompleteUI(self):
        """創建完整UI"""
        self.mainDesign()  # 創建主UI組件
        self.layouts()     # 設置布局
        self.add_spx_tab_to_main_ui()  # 添加SPX標籤頁

    def setupLogger(self):
        """設置日誌系統"""
        self.logger = Logger().get_logger(__name__)
        
        # 確保logTextEdit已經被創建
        if not hasattr(self, 'logTextEdit'):
            self.logTextEdit = QTextEdit()
            self.logTextEdit.setReadOnly(True)
        
        # 創建自定義處理器
        self.log_handler = LogHandler()
        self.log_handler.setLevel(logging.INFO)
        
        # 連接信號和槽
        self.log_handler.new_log_signal.connect(self.append_log)
        
        # 獲取根記錄器，添加我們的處理器
        root_logger = logging.getLogger()
        
        # 移除舊處理器（如果存在）以避免重複
        for handler in root_logger.handlers[:]:
            if isinstance(handler, (QTextEditLogger, LogHandler)):
                root_logger.removeHandler(handler)
        
        root_logger.addHandler(self.log_handler)
        
        # 測試日誌記錄
        self.logger.info("POPR BOT 啟動")
        self.logger.info("日誌系統初始化完成")

    @pyqtSlot(str)
    def append_log(self, message):
        """處理新的日誌條目"""
        if hasattr(self, 'logTextEdit') and self.logTextEdit:
            try:
                self.logTextEdit.append(message)
                self.logTextEdit.moveCursor(QTextCursor.End)
                # 處理事件但無需強制更新
                QApplication.processEvents()
            except Exception as e:
                print(f"日誌顯示錯誤: {e}")
    
    def mainDesign(self):
        """創建UI元素"""
        # 創建選項卡
        self.tabWidget = QTabWidget()
        
        # 主要操作頁面
        self.mainTab = QWidget()
        
        # 日誌頁面
        self.logTab = QWidget()
        self.logTextEdit = QTextEdit()
        
        # 文件列表標題
        self.checkList = QLabel('已匯入檔案')
        self.checkList.setStyleSheet("font-size:12pt;background-color:#93FF93;color:#000080;font:Bold")
        self.checkList.setAlignment(Qt.AlignCenter)
        
        # 文件列表
        self.importedList = QListWidget()
        
        # 實體選擇下拉框
        self.entitySelect = QComboBox(self)
        entities = ['MOBA_PR', 'MOBA_PO', 'SPT_PR', 'SPT_PO']
        for name in entities:
            self.entitySelect.addItem(name)
        
        # 導入按鈕
        self.btnImport_raw = QPushButton('原始資料', self)
        self.btnImport_raw.clicked.connect(self.import_raw)
        
        self.btnImport_closinglist = QPushButton('關單清單', self)
        self.btnImport_closinglist.clicked.connect(self.import_closing)
        
        self.btnImport_previouswp = QPushButton('前期底稿', self)
        self.btnImport_previouswp.clicked.connect(self.import_previousup)
        
        self.btnImpot_procuremant = QPushButton('採購底稿', self)
        self.btnImpot_procuremant.clicked.connect(self.import_procurement)
        
        # 處理按鈕
        self.btnProcess = QPushButton('匯出', self)
        self.btnProcess.clicked.connect(self.process)
        
        self.btnDelete = QPushButton('刪除已匯入資料', self)
        self.btnDelete.clicked.connect(self.deleteImportFile)
        
        self.btnHRIS = QPushButton('HRIS重複檢查', self)
        self.btnHRIS.clicked.connect(self.process_hris)
        
        self.btnUploadForm = QPushButton('Upload Form', self)
        self.btnUploadForm.clicked.connect(self.uploadFormWidget)
        
        self.btncheck2 = QPushButton('兩期檢查', self)
        self.btncheck2.clicked.connect(self.check2)
        
        # 日誌狀態標籤
        self.statusLabel = QLabel('狀態: 準備就緒')
        self.statusLabel.setStyleSheet("font-size:10pt;")
    
    def layouts(self):
        """設置布局"""
        # 主布局
        self.mainLayout = QVBoxLayout()
        
        # 主頁面布局
        self.mainTabLayout = QHBoxLayout(self.mainTab)
        self.leftLayout = QVBoxLayout()
        self.rightLayout = QVBoxLayout()
        
        # 設置比例
        self.mainTabLayout.addLayout(self.leftLayout, 60)
        self.mainTabLayout.addLayout(self.rightLayout, 40)
        
        # 左側布局
        self.leftLayout.addWidget(self.checkList)
        self.leftLayout.addWidget(self.importedList)
        
        # 右側布局
        self.rightLayout.addWidget(self.entitySelect)
        self.rightLayout.addWidget(self.btnImport_raw)
        self.rightLayout.addWidget(self.btnImport_closinglist)
        self.rightLayout.addWidget(self.btnImport_previouswp)
        self.rightLayout.addWidget(self.btnImpot_procuremant)
        self.rightLayout.addWidget(self.btnProcess)
        self.rightLayout.addWidget(self.btnDelete)
        self.rightLayout.addWidget(self.btnHRIS)
        self.rightLayout.addWidget(self.btnUploadForm)
        self.rightLayout.addWidget(self.btncheck2)
        self.rightLayout.addStretch()
        self.rightLayout.addWidget(self.statusLabel)
        
        # 日誌頁面布局
        self.logTabLayout = QVBoxLayout(self.logTab)
        self.logTabLayout.addWidget(self.logTextEdit)
        
        # 添加選項卡
        self.tabWidget.addTab(self.mainTab, "主功能")
        self.tabWidget.addTab(self.logTab, "日誌")
        
        # 添加選項卡到主布局
        self.mainLayout.addWidget(self.tabWidget)
        
        # 設置主布局
        self.setLayout(self.mainLayout)
    
    def setupTabEvents(self):
        """設置標籤切換事件"""
        # 連接標籤切換信號到處理函數
        self.tabWidget.currentChanged.connect(self.onTabChanged)
        
        # 保存原始窗口大小作為主功能標籤的默認大小
        self.mainTabSize = (450, 150, 600, 400)  # x, y, width, height
        
        # 可以為其他標籤設置不同的大小（例如SPX標籤）
        self.spxTabSize = (450, 150, 800, 600)  # 更大的SPX窗口大小
        
    def onTabChanged(self, index):
        """處理標籤頁切換事件"""
        try:
            tab_name = self.tabWidget.tabText(index)
            self.logger.info(f"切換到標籤頁: {tab_name}")
            
            # 保存當前窗口中心位置
            center = self.frameGeometry().center()
            
            # 根據切換到的標籤調整窗口大小
            if tab_name == "主功能":
                self.setGeometry(*self.mainTabSize)
            elif tab_name == "SPX模組":
                self.setGeometry(*self.spxTabSize)
            elif tab_name == "日誌":
                self.setGeometry(*self.mainTabSize)
                
            # 重新計算並設置窗口中心位置
            new_rect = self.frameGeometry()
            new_rect.moveCenter(center)
            self.move(new_rect.topLeft())
        except Exception as e:
            self.logger.error(f"切換標籤頁時出錯: {str(e)}", exc_info=True)

    def updateStatus(self, message, error=False):
        """更新狀態標籤"""
        if error:
            self.statusLabel.setStyleSheet("font-size:10pt; color: red;")
            self.had_error = True
        else:
            self.statusLabel.setStyleSheet("font-size:10pt; color: black;")
        
        self.statusLabel.setText(f"狀態: {message}")
    
    def checkHadError(self):
        """檢查處理過程中是否有錯誤"""
        if self.had_error:
            self.had_error = False  # 重置錯誤狀態
            return True
        return False
    
    def process_hris(self):
        """處理HRIS重複檢查"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.logger.info("開始HRIS重複檢查") # Dev log
            self.updateStatus(STRINGS["STATUS_PROCESSING_HRIS"])
            
            # 選擇文件
            urls = QFileDialog.getOpenFileUrls(self, STRINGS["WINDOW_TITLE_HRIS_DIALOG"])
            
            if not urls[0]:
                self.logger.info("未選擇文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
                
            # 提取文件路徑
            file_urls = [url.url() for url in urls[0]]
            
            # 讀取PR、PO和AP文件
            try:
                df_pr = pd.read_excel([url for url in file_urls if 'PR' in os.path.basename(url)][0], dtype=str)
                df_po = pd.read_excel([url for url in file_urls if 'PO' in os.path.basename(url)][0], dtype=str)
                df_ap = pd.read_excel([url for url in file_urls if 'AP' in os.path.basename(url)][0],
                                      dtype=str, header=1, sheet_name=1)
                                      
                self.logger.info(f"成功讀取文件 PR:{df_pr.shape}, PO:{df_po.shape}, AP:{df_ap.shape}") # Dev log
            except Exception as err:
                self.logger.error(f"讀取文件失敗: {str(err)}", exc_info=True) # Dev log
                self.updateStatus(STRINGS["STATUS_HRIS_ERROR_FILE_READ_FAILED"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_FILE_READ_FAILED_DETAILED"].format(str(err)))
                return
            
            # 處理重複檢查
            try:
                # 創建檢查器實例
                checker = HRISDuplicateChecker()
                
                # 檢查PO中的重複項
                df_pr_p = checker.check_duplicates_in_po(df_pr, df_po)
                
                # 檢查AP中的重複項
                df_pr_p, df_po_p = checker.check_duplicates_in_ap(df_pr_p, df_po, df_ap)
                
                # 重新定位列
                df_pr_p, df_po_p = checker.relocate_columns(df_pr_p, df_po_p)
                
                # 檢查是否有錯誤
                if self.checkHadError():
                    self.updateStatus(STRINGS["STATUS_PROCESSING_ERROR_STOP"], error=True)
                    return
                
                # 保存文件
                checker.save_files(df_pr_p, df_po_p)
                
                self.updateStatus(STRINGS["STATUS_HRIS_COMPLETE"])
                QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_HRIS_COMPLETE"])
                self.logger.info("HRIS重複檢查完成") # Dev log
            except Exception as err:
                self.logger.error(f"HRIS重複檢查失敗: {str(err)}", exc_info=True) # Dev log
                self.updateStatus(STRINGS["STATUS_HRIS_ERROR_PROCESSING_FAILED"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_HRIS_FAILED_DETAILED"].format(str(err)))
                
        except Exception as err:
            self.logger.error(f"HRIS處理過程中出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_HRIS_ERROR_DURING_PROCESSING"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_PROCESSING_FAILED_DETAILED"].format(str(err)))
    
    def import_raw(self):
        """導入原始數據文件"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus(STRINGS["STATUS_IMPORTING_RAW_DATA"])
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(
                self, STRINGS["WINDOW_TITLE_RAW_DATA_DIALOG"],
                "",
                STRINGS["FILE_DIALOG_FILTER_EXCEL_CSV"]
            )
            
            if not url[0]:
                self.logger.info("未選擇原始數據文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
                
            self.fileUrl = url[0]
            self.file_name = os.path.basename(self.fileUrl)
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if STRINGS["MAIN_BTN_IMPORT_RAW"] in items:
                self.logger.warning("原始資料已存在") # Dev log
                self.updateStatus(STRINGS["STATUS_WARN_RAW_DATA_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_RAW_DATA_EXISTS"])
                return
            
            # 檢查文件名格式
            ym = self.file_name[0:6]
            if re.match(r'[0-9]{6}', str(ym)) is None:
                self.logger.warning("文件名格式錯誤，需要包含年月") # Dev log
                self.updateStatus(STRINGS["STATUS_ERR_FILENAME_FORMAT_YYYYMM"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_FILENAME_FORMAT_YYYYMM"])
                return
                
            # 添加到列表
            self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_RAW"])
            self.logger.info(f"成功導入原始資料: {self.file_name}") # Dev log
            self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + self.file_name)
            
        except Exception as err:
            self.logger.error(f"導入原始數據時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_RAW_DATA"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_RAW_DATA_DETAILED"].format(str(err)))
    
    def import_closing(self):
        """導入關單清單"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus(STRINGS["STATUS_IMPORTING_CLOSING_LIST"])
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(
                self, STRINGS["WINDOW_TITLE_CLOSING_LIST_DIALOG"],
                "",
                STRINGS["FILE_DIALOG_FILTER_EXCEL_CSV"]
            )
            
            if not url[0]:
                self.logger.info("未選擇關單清單文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
                
            self.fileUrl_c = url[0]
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"] in items:
                self.logger.warning("關單清單已存在") # Dev log
                self.updateStatus(STRINGS["STATUS_WARN_CLOSING_LIST_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_CLOSING_LIST_EXISTS"])
                return
                
            # 添加到列表
            self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"])
            self.logger.info(f"成功導入關單清單: {os.path.basename(self.fileUrl_c)}") # Dev log
            self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + os.path.basename(self.fileUrl_c))
            
        except Exception as err:
            self.logger.error(f"導入關單清單時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_CLOSING_LIST"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_CLOSING_LIST_DETAILED"].format(str(err)))
    
    def import_previousup(self):
        """導入前期底稿"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus(STRINGS["STATUS_IMPORTING_PREVIOUS_WP"])
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PREVIOUS_WP_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            
            if not url[0]:
                self.logger.info("未選擇前期底稿文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
                
            self.fileUrl_previwp = url[0]
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"] in items:
                self.logger.warning("前期底稿已存在") # Dev log
                self.updateStatus(STRINGS["STATUS_WARN_PREVIOUS_WP_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_PREVIOUS_WP_EXISTS"])
                return
            
            # 驗證文件格式
            try:
                column_checking = pd.read_excel(
                    self.fileUrl_previwp, dtype=str, nrows=3, engine='openpyxl').columns.tolist()
                important_col = ['Remarked by Procurement', 'Noted by Procurement', 'Line#'] # Column names, not UI strings
                
                if not set(important_col).issubset(column_checking):
                    self.logger.warning("前期底稿格式錯誤，缺少必要列") # Dev log
                    self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PREVIOUS_WP"], error=True) 
                    warning_text = STRINGS["MSGBOX_WARN_PREVIOUS_WP_FORMAT_ERROR"]
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], warning_text)
                    return
                    
                # 添加到列表
                self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"])
                self.logger.info(f"成功導入前期底稿: {os.path.basename(self.fileUrl_previwp)}") # Dev log
                self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + os.path.basename(self.fileUrl_previwp))
                
            except Exception as e:
                self.logger.error(f"驗證前期底稿時出錯: {str(e)}", exc_info=True) # Dev log
                self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PREVIOUS_WP"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_VALIDATING_PREVIOUS_WP_DETAILED"].format(str(e)))
                
        except Exception as err:
            self.logger.error(f"導入前期底稿時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_PREVIOUS_WP"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_PREVIOUS_WP_DETAILED"].format(str(err)))
    
    def import_procurement(self):
        """導入採購底稿"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus(STRINGS["STATUS_IMPORTING_PROCUREMENT_WP"])
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PROCUREMENT_WP_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            
            if not url[0]:
                self.logger.info("未選擇採購底稿文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
                
            self.fileUrl_p = url[0]
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"] in items:
                self.logger.warning("採購底稿已存在") # Dev log
                self.updateStatus(STRINGS["STATUS_WARN_PROCUREMENT_WP_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_PROCUREMENT_WP_EXISTS"])
                return
            
            # 驗證文件格式
            try:
                column_checking = pd.read_excel(self.fileUrl_p, dtype=str, nrows=3, engine='openpyxl').columns.tolist()
                important_col = ['Remarked by Procurement', 'Noted by Procurement', 'Line#'] # Column names, not UI strings
                
                if not set(important_col).issubset(column_checking):
                    self.logger.warning("採購底稿格式錯誤，缺少必要列") # Dev log
                    self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PROCUREMENT_WP"], error=True) 
                    warning_text = STRINGS["MSGBOX_WARN_PROCUREMENT_WP_FORMAT_ERROR"]
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], warning_text)
                    return
                    
                # 添加到列表
                self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"])
                self.logger.info(f"成功導入採購底稿: {os.path.basename(self.fileUrl_p)}") # Dev log
                self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + os.path.basename(self.fileUrl_p))
                
            except Exception as e:
                self.logger.error(f"驗證採購底稿時出錯: {str(e)}", exc_info=True) # Dev log
                self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PROCUREMENT_WP"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_VALIDATING_PROCUREMENT_WP_DETAILED"].format(str(e)))
                
        except Exception as err:
            self.logger.error(f"導入採購底稿時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_PROCUREMENT_WP"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_PROCUREMENT_WP_DETAILED"].format(str(err)))
    
    def process(self):
        """處理數據"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus(STRINGS["STATUS_PROCESSING_DATA"])
            
            # 獲取導入的文件列表
            items = []
            for x in range(self.importedList.count()):
                items.append(self.importedList.item(x).text())
                
            self.logger.info(f"處理數據，已導入項目: {items}")
            
            # 獲取選擇的實體類型
            module_type = self.entitySelect.currentText()
            
            # 檢查是否已導入原始數據
            if STRINGS["MAIN_BTN_IMPORT_RAW"] not in items:
                self.logger.warning("未導入原始數據") # Dev log
                self.updateStatus(STRINGS["STATUS_ERR_NO_RAW_DATA_TO_PROCESS"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_NO_RAW_DATA"])
                return
            
            # 根據實體類型和導入文件選擇處理模式
            if module_type == STRINGS["COMBO_ENTITY_MOBAPR"] and STRINGS["MAIN_BTN_IMPORT_RAW"] in items:
                self._process_moba_pr(items)
                if not self.had_error:
                    self.updateStatus(STRINGS["STATUS_PROCESS_MOBA_PR_COMPLETE"])
                    QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_MOBA_PR_COMPLETE"])
            elif module_type == STRINGS["COMBO_ENTITY_MOBAPO"] and STRINGS["MAIN_BTN_IMPORT_RAW"] in items:
                self._process_moba_po(items)
                if not self.had_error:
                    self.updateStatus(STRINGS["STATUS_PROCESS_MOBA_PO_COMPLETE"])
                    QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_MOBA_PO_COMPLETE"])
            elif module_type == STRINGS["COMBO_ENTITY_SPTPR"] and STRINGS["MAIN_BTN_IMPORT_RAW"] in items:
                self._process_spt_pr(items)
                if not self.had_error:
                    self.updateStatus(STRINGS["STATUS_PROCESS_SPT_PR_COMPLETE"])
                    QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_SPT_PR_COMPLETE"])
            elif module_type == STRINGS["COMBO_ENTITY_SPTPO"] and STRINGS["MAIN_BTN_IMPORT_RAW"] in items:
                self._process_spt_po(items)
                if not self.had_error:
                    self.updateStatus(STRINGS["STATUS_PROCESS_SPT_PO_COMPLETE"])
                    QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_SPT_PO_COMPLETE"])
            else:
                self.logger.warning("無法確定處理模式") # Dev log
                self.updateStatus(STRINGS["STATUS_ERR_CANNOT_DETERMINE_MODE"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_CANNOT_DETERMINE_MODE"])
                
        except Exception as err:
            self.logger.error(f"處理數據時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_PROCESSING_DATA"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_PROCESSING_DATA_DETAILED"].format(str(err)))
    
    def _process_moba_pr(self, items):
        """處理MOBA PR數據"""
        try:
            processor = MOBTW_PR()
            
            raw_data_item = STRINGS["MAIN_BTN_IMPORT_RAW"]
            prev_wp_item = STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"]
            proc_wp_item = STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"]
            closing_list_item = STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"]

            if prev_wp_item in items:
                if len(items) == 4:
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                    self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                    self.had_error = True
                    return
                elif len(items) == 3 and proc_wp_item in items:
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_previwp)
                elif len(items) == 3 and closing_list_item in items:
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                    self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                    self.had_error = True
                    return
                elif len(items) == 2:
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif len(items) == 1:
                processor.mode_4(self.fileUrl, self.file_name)
            elif set([raw_data_item, closing_list_item, proc_wp_item]).issubset(items):
                processor.mode_1(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set([raw_data_item, proc_wp_item]).issubset(items):
                processor.mode_3(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set([raw_data_item, closing_list_item]).issubset(items):
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_c)
                
            self.logger.info("MOBA PR處理完成") # Dev log
            
        except Exception as e:
            self.logger.error(f"處理MOBA PR時出錯: {str(e)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_PROCESSING_MOBA_PR"], error=True)
            self.had_error = True
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_MOBA_PR_DETAILED"].format(str(e)))
    
    def _process_moba_po(self, items):
        """處理MOBA PO數據"""
        try:
            processor = MOBTW_PO()

            raw_data_item = STRINGS["MAIN_BTN_IMPORT_RAW"]
            prev_wp_item = STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"]
            proc_wp_item = STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"]
            closing_list_item = STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"]
            
            if set([raw_data_item, closing_list_item, proc_wp_item, prev_wp_item]).issubset(items):
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                self.had_error = True
                return
            elif set([raw_data_item, proc_wp_item, prev_wp_item]).issubset(items):
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_previwp, self.fileUrl_p)
            elif set([raw_data_item, closing_list_item, prev_wp_item]).issubset(items):
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                self.had_error = True
                return
            elif set([raw_data_item, closing_list_item, proc_wp_item]).issubset(items):
                processor.mode_4(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set([raw_data_item, proc_wp_item]).issubset(items):
                processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set([raw_data_item, prev_wp_item]).issubset(items):
                processor.mode_6(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif set([raw_data_item, closing_list_item]).issubset(items):
                processor.mode_7(self.fileUrl, self.file_name, self.fileUrl_c)
            else:
                processor.mode_8(self.fileUrl, self.file_name)
                
            self.logger.info("MOBA PO處理完成") # Dev log
            
        except Exception as e:
            self.logger.error(f"處理MOBA PO時出錯: {str(e)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_PROCESSING_MOBA_PO"], error=True)
            self.had_error = True
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_MOBA_PO_DETAILED"].format(str(e)))
    
    def _process_spt_pr(self, items):
        """處理SPT PR數據"""
        try:
            processor = SPTTW_PR()

            raw_data_item = STRINGS["MAIN_BTN_IMPORT_RAW"]
            prev_wp_item = STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"]
            proc_wp_item = STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"]
            closing_list_item = STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"]
            
            if prev_wp_item in items:
                if len(items) == 4:
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                    self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                    self.had_error = True
                    return
                elif len(items) == 3 and proc_wp_item in items:
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_previwp)
                elif len(items) == 3 and closing_list_item in items:
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                    self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                    self.had_error = True
                    return
                elif len(items) == 2:
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif len(items) == 1:
                processor.mode_4(self.fileUrl, self.file_name)
            elif set([raw_data_item, closing_list_item, proc_wp_item]).issubset(items):
                processor.mode_1(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set([raw_data_item, proc_wp_item]).issubset(items):
                processor.mode_3(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set([raw_data_item, closing_list_item]).issubset(items):
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_c)
                
            self.logger.info("SPT PR處理完成") # Dev log
            
        except Exception as e:
            self.logger.error(f"處理SPT PR時出錯: {str(e)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_PROCESSING_SPT_PR"], error=True)
            self.had_error = True
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_SPT_PR_DETAILED"].format(str(e)))
    
    def _process_spt_po(self, items):
        """處理SPT PO數據"""
        try:
            processor = SPTTW_PO()

            raw_data_item = STRINGS["MAIN_BTN_IMPORT_RAW"]
            prev_wp_item = STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"]
            proc_wp_item = STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"]
            closing_list_item = STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"]

            if set([raw_data_item, closing_list_item, proc_wp_item, prev_wp_item]).issubset(items):
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                self.had_error = True
                return
            elif set([raw_data_item, proc_wp_item, prev_wp_item]).issubset(items):
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_previwp, self.fileUrl_p)
            elif set([raw_data_item, closing_list_item, prev_wp_item]).issubset(items):
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_LOCKED_MODE"])
                self.updateStatus(STRINGS["STATUS_ERR_LOCKED_MODE"], error=True)
                self.had_error = True
                return
            elif set([raw_data_item, closing_list_item, proc_wp_item]).issubset(items):
                processor.mode_4(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set([raw_data_item, proc_wp_item]).issubset(items):
                processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set([raw_data_item, prev_wp_item]).issubset(items):
                processor.mode_6(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif set([raw_data_item, closing_list_item]).issubset(items):
                processor.mode_7(self.fileUrl, self.file_name, self.fileUrl_c)
            else:
                processor.mode_8(self.fileUrl, self.file_name)
                
            self.logger.info("SPT PO處理完成") # Dev log
            
        except Exception as e:
            self.logger.error(f"處理SPT PO時出錯: {str(e)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_PROCESSING_SPT_PO"], error=True)
            self.had_error = True
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_SPT_PO_DETAILED"].format(str(e)))

    def deleteImportFile(self):
        """刪除已導入的文件"""
        try:
            # 檢查是否選擇了要刪除的項目
            if not self.importedList.selectedItems():
                self.logger.warning("未選擇要刪除的項目") # Dev log
                self.updateStatus(STRINGS["STATUS_WARN_NO_ITEM_TO_DELETE"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_SELECT_ITEM_TO_DELETE"])
                return
                
            # 獲取選中的項目
            index = self.importedList.currentRow()
            item = self.importedList.currentItem()
            
            # 確認刪除
            mbox = QMessageBox.question(
                self, STRINGS["MSGBOX_TITLE_CONFIRM"], STRINGS["MSGBOX_CONFIRM_DELETE_ITEM_PROMPT"].format(item.text()),
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            
            if mbox == QMessageBox.Yes:
                try:
                    self.importedList.takeItem(index)
                    self.logger.info(f"已刪除項目: {item.text()}") # Dev log
                    self.updateStatus(STRINGS["STATUS_ITEM_DELETED_PREFIX"] + item.text())
                except Exception as err:
                    self.logger.error(f"刪除項目時出錯: {str(err)}", exc_info=True) # Dev log
                    self.updateStatus(STRINGS["STATUS_ERR_DELETING_ITEM"], error=True)
                    QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_DELETING_ITEM_FAILED"])
                    
        except Exception as err:
            self.logger.error(f"刪除文件時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_DELETING_FILE"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_DELETING_FILE_FAILED_DETAILED"].format(str(err)))
    
    def uploadFormWidget(self):
        """打開Upload Form對話框"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus(STRINGS["STATUS_OPENING_UPLOAD_FORM_DIALOG"])
            
            sub_widget = UploadFormWidget(self)
            sub_widget.exec_()
            
            self.updateStatus(STRINGS["STATUS_READY"])
            
        except Exception as err:
            self.logger.error(f"打開Upload Form對話框時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_OPENING_UPLOAD_FORM_DIALOG"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_OPENING_UPLOAD_FORM_DIALOG_DETAILED"].format(str(err)))
    
    def check2(self):
        """執行兩期檢查"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.logger.info("開始兩期檢查") # Dev log
            self.updateStatus(STRINGS["STATUS_PERFORMING_CHECK2"])
            
            # 選擇文件
            url_pr = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PURCHASE_PR_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            if not url_pr[0]:
                self.logger.info("未選擇採購PR文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
                
            url_po = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PURCHASE_PO_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            if not url_po[0]:
                self.logger.info("未選擇採購PO文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
                
            url_ac = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PREVIOUS_WP_XLSM_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_MACRO_ENABLED"])
            if not url_ac[0]:
                self.logger.info("未選擇前期底稿文件，取消操作") # Dev log
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            
            # 執行差異比較
            try:
                a, b, c = ReconEntryAmt.get_difference(url_ac[0], url_pr[0], url_po[0])
                
                # 檢查是否有錯誤
                if self.checkHadError():
                    self.updateStatus(STRINGS["STATUS_PROCESSING_ERROR_STOP"], error=True)
                    return
                
                # 輸出結果
                pd.DataFrame({**a, **b, **c}, index=[0]).T.to_excel('check_dif_amount.xlsx') # Filename, not UI string
                
                self.updateStatus(STRINGS["STATUS_CHECK2_COMPLETE_FILE_SAVED"]) 
                QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_CHECK2_COMPLETE"])
                self.logger.info("兩期檢查完成") # Dev log
            except Exception as err:
                self.logger.error(f"執行差異比較時出錯: {str(err)}", exc_info=True) # Dev log
                self.updateStatus(STRINGS["STATUS_ERR_PERFORMING_CHECK2_COMPARE"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_CHECK2_COMPARE_FAILED_DETAILED"].format(str(err)))
                return
            
        except Exception as err:
            self.logger.error(f"兩期檢查時出錯: {str(err)}", exc_info=True) # Dev log
            self.updateStatus(STRINGS["STATUS_ERR_PERFORMING_CHECK2"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_CHECK2_FAILED_DETAILED"].format(str(err)))

    def add_spx_tab_to_main_ui(self):
        """將SPX tab添加到主UI
        
        這個函數應該添加到Main類中，用於初始化SPX Tab
        """
        # 創建SPX Tab
        self.spxTab = SPXTabWidget(self)
        
        # 將SPX Tab添加到tabWidget
        self.tabWidget.addTab(self.spxTab, STRINGS["TAB_SPX"])
        
        # 日誌記錄 - 增加檢查以防 logger 尚未初始化
        if hasattr(self, 'logger') and self.logger is not None:
            self.logger.info("SPX模組Tab已初始化") # Dev log


class UploadFormWidget(QDialog):
    """Upload Form對話框"""
    
    def __init__(self, parent=None):
        """初始化Upload Form對話框"""
        super().__init__(parent)
        self.logger = Logger().get_logger(__name__)
        self.had_error = False  # 標記是否有錯誤發生
        
        self.setWindowTitle(STRINGS["WINDOW_TITLE_UPLOAD_FORM"])
        self.setupUI()
        self.logger.info("Upload Form對話框已打開") # Dev log
    
    def updateStatus(self, message, error=False):
        """更新狀態"""
        if error:
            self.had_error = True
            
    def setupUI(self):
        """設置UI組件"""
        # 創建布局
        self.mainLayout = QHBoxLayout()
        self.leftLayout = QVBoxLayout()
        self.rightLayout = QVBoxLayout()
        
        # 設置比例
        self.mainLayout.addLayout(self.leftLayout, 60)
        self.mainLayout.addLayout(self.rightLayout, 40)
        
        # 創建UI元素
        self.label_entity = QLabel(STRINGS["UPLOAD_FORM_LBL_ENTITY"])
        self.combo_entity = QComboBox(self)
        entities = [STRINGS["COMBO_ENTITY_MOBTW"], STRINGS["COMBO_ENTITY_SPTTW"]]
        for name in entities:
            self.combo_entity.addItem(name)
        
        self.label_period = QLabel(STRINGS["UPLOAD_FORM_LBL_PERIOD"])
        self.line_period = QLineEdit()
        self.line_period.setPlaceholderText(STRINGS["UPLOAD_FORM_PLACEHOLDER_PERIOD"])
        
        self.label_ac_date = QLabel(STRINGS["UPLOAD_FORM_LBL_AC_DATE"])
        self.line_ac_date = QLineEdit()
        self.line_ac_date.setPlaceholderText(STRINGS["UPLOAD_FORM_PLACEHOLDER_AC_DATE"])
        
        self.label_cate = QLabel(STRINGS["UPLOAD_FORM_LBL_CATEGORY"])
        self.line_cate = QLineEdit()
        self.line_cate.setPlaceholderText(STRINGS["UPLOAD_FORM_PLACEHOLDER_CATEGORY"])
        self.line_cate.setText(STRINGS["UPLOAD_FORM_TEXT_CATEGORY_DEFAULT"])
        
        self.label_accountant = QLabel(STRINGS["UPLOAD_FORM_LBL_ACCOUNTANT"])
        self.line_accountant = QLineEdit()
        self.line_accountant.setPlaceholderText(STRINGS["UPLOAD_FORM_PLACEHOLDER_ACCOUNTANT"])
        self.line_accountant.setText(STRINGS["UPLOAD_FORM_TEXT_ACCOUNTANT_DEFAULT"])
        
        self.label_currency = QLabel(STRINGS["UPLOAD_FORM_LBL_CURRENCY"])
        self.combo_currency = QComboBox(self)
        currencies = [STRINGS["COMBO_CURRENCY_TWD"], STRINGS["COMBO_CURRENCY_USD"], STRINGS["COMBO_CURRENCY_HKD"]]
        for c in currencies:
            self.combo_currency.addItem(c)
        
        self.label_wp = QLabel(STRINGS["UPLOAD_FORM_LBL_WP"])
        self.button_get_wp = QPushButton(STRINGS["UPLOAD_FORM_BTN_SELECT_WP"])
        self.button_get_wp.clicked.connect(self.get_wp)
        
        self.label_start = QLabel(STRINGS["UPLOAD_FORM_LBL_PROCESS"])
        self.button_do_upload = QPushButton(STRINGS["UPLOAD_FORM_BTN_GENERATE_UPLOAD_FORM"])
        self.button_do_upload.clicked.connect(self.process_upload_form)
        
        # 狀態標籤
        self.statusLabel = QLabel(STRINGS["MAIN_LBL_STATUS_PREFIX"] + STRINGS["STATUS_READY"])
        
        # 左側布局
        self.leftLayout.addWidget(self.label_entity)
        self.leftLayout.addWidget(self.label_period)
        self.leftLayout.addWidget(self.label_ac_date)
        self.leftLayout.addWidget(self.label_cate)
        self.leftLayout.addWidget(self.label_accountant)
        self.leftLayout.addWidget(self.label_currency)
        self.leftLayout.addWidget(self.label_wp)
        self.leftLayout.addWidget(self.label_start)
        
        # 右側布局
        self.rightLayout.addWidget(self.combo_entity)
        self.rightLayout.addWidget(self.line_period)
        self.rightLayout.addWidget(self.line_ac_date)
        self.rightLayout.addWidget(self.line_cate)
        self.rightLayout.addWidget(self.line_accountant)
        self.rightLayout.addWidget(self.combo_currency)
        self.rightLayout.addWidget(self.button_get_wp)
        self.rightLayout.addWidget(self.button_do_upload)

        # 將狀態標籤放到整個佈局的底部
        statusLayout = QHBoxLayout()
        statusLayout.addWidget(self.statusLabel)

        # 設置主布局
        mainContainer = QVBoxLayout()
        mainContainer.addLayout(self.mainLayout)
        mainContainer.addStretch()
        mainContainer.addLayout(statusLayout)
        
        self.setLayout(mainContainer)
    
    def get_wp(self):
        """選擇工作底稿文件"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_SELECTING_WP"])
            
            url = QFileDialog.getOpenFileName(
                self, STRINGS["UPLOAD_FORM_LBL_WP"], 
                "",
                STRINGS["FILE_DIALOG_FILTER_EXCEL_ALL_TYPES"]
            )
            
            if not url[0]:
                self.logger.info("未選擇工作底稿文件，取消操作") # Dev log
                self.statusLabel.setText(STRINGS["MAIN_LBL_STATUS_PREFIX"] + STRINGS["STATUS_READY"])
                return
                
            self.fileUrl = url[0]
            self.logger.info(f"已選擇工作底稿: {os.path.basename(self.fileUrl)}") # Dev log
            
            # 更新按鈕文字，顯示已選擇文件
            self.button_get_wp.setText(f"{STRINGS['UPLOAD_FORM_STATUS_SELECTED_WP_PREFIX']}{os.path.basename(self.fileUrl)}")
            self.statusLabel.setText(f"{STRINGS['UPLOAD_FORM_STATUS_SELECTED_WP_PREFIX']}{os.path.basename(self.fileUrl)}")
            
        except Exception as err:
            self.logger.error(f"選擇工作底稿時出錯: {str(err)}", exc_info=True) # Dev log
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_SELECTING_WP"])
            self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_SELECTING_WP"], error=True) 
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['UPLOAD_FORM_STATUS_ERR_SELECTING_WP']}:\n{str(err)}")
    
    def process_upload_form(self):
        """處理並生成Upload Form"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.logger.info("開始生成Upload Form") # Dev log
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_GENERATING"])
            
            # 獲取輸入參數
            entity = self.combo_entity.currentText() 
            period = self.line_period.text()
            ac_date = self.line_ac_date.text()
            cate = self.line_cate.text()
            accountant = self.line_accountant.text()
            currency = self.combo_currency.currentText()
            
            # 驗證輸入
            if not hasattr(self, 'fileUrl') or not self.fileUrl:
                self.logger.warning("未選擇工作底稿") # Dev log
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_WP_SELECTED"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_WP_SELECTED"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_NO_WP_SELECTED"])
                return
                
            if not period:
                self.logger.warning("未輸入期間") # Dev log
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_PERIOD"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_PERIOD"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_NO_PERIOD"])
                return
                
            if not ac_date:
                self.logger.warning("未輸入會計日期") # Dev log
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_AC_DATE"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_AC_DATE"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_NO_AC_DATE"])
                return
            
            # 解析會計日期
            try:
                m_date = datetime.strptime(ac_date, '%Y/%m/%d').date()
                ac_period = datetime.strftime(m_date, '%Y/%m')
            except ValueError:
                self.logger.warning("會計日期格式錯誤") # Dev log
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_AC_DATE_FORMAT"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_AC_DATE_FORMAT"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_AC_DATE_FORMAT"])
                return
            
            # 根據實體和貨幣選擇處理方法
            try:
                if entity == STRINGS["COMBO_ENTITY_MOBTW"]:
                    if currency == STRINGS["COMBO_CURRENCY_TWD"]:
                        self.logger.info(f"處理 {entity} {currency} 表單") # Dev log
                        dfs = get_aggregation_twd(self.fileUrl, ac_period)
                    else:
                        self.logger.info(f"處理 {entity} {currency} 表單") # Dev log
                        dfs = get_aggregation_foreign(self.fileUrl, ac_period, currency=currency)
                else:  # SPTTW (STRINGS["COMBO_ENTITY_SPTTW"])
                    if currency == STRINGS["COMBO_CURRENCY_TWD"]:
                        self.logger.info(f"處理 {entity} {currency} 表單") # Dev log
                        dfs = get_aggregation_twd(self.fileUrl, ac_period, is_mob=False)
                    else:
                        self.logger.info(f"處理 {entity} {currency} 表單") # Dev log
                        dfs = get_aggregation_foreign(self.fileUrl, ac_period, is_mob=False, currency=currency)
                
                # 檢查是否有錯誤
                if self.had_error: 
                    self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_PROCESSING_STOPPED"])
                    return
                
                # 生成條目
                result = get_entries(dfs, entity, period, ac_date, cate, accountant, currency)
                
                # 保存結果
                output_file = f'Upload Form-{entity}-{period[:3]}-{currency}.xlsx' # Filename, not UI string
                result.to_excel(output_file, index=False)
                
                self.logger.info(f"已成功生成Upload Form: {output_file}") # Dev log
                self.statusLabel.setText(f"{STRINGS['UPLOAD_FORM_STATUS_SUCCESS_PREFIX']} {output_file}")
                QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], f"{STRINGS['UPLOAD_FORM_INFO_GENERATED_PREFIX']}{output_file}")
                
            except Exception as e:
                self.logger.error(f"生成Upload Form時出錯: {str(e)}", exc_info=True) # Dev log
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_GENERATING_DETAILED"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_GENERATING_DETAILED"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['UPLOAD_FORM_STATUS_ERR_GENERATING_DETAILED']}:\n{str(e)}")
                return
                
        except Exception as err:
            self.logger.error(f"處理Upload Form時出錯: {str(err)}", exc_info=True) # Dev log
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_PROCESSING_DETAILED"])
            self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_PROCESSING_DETAILED"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['UPLOAD_FORM_STATUS_ERR_PROCESSING_DETAILED']}:\n{str(err)}")


class SPXTabWidget(QWidget):
    """SPX模組專用的tab介面"""
    
    def __init__(self, parent=None):
        """初始化SPX模組tab介面"""
        super(SPXTabWidget, self).__init__(parent)
        self.parent = parent  # 存儲主窗口引用(Main class)，用於訪問日誌等功能(調用Main.logger.info運用)
        self.file_paths = {}  # 存儲所有文件路徑
        # 定義文件類型映射，作為類屬性以便在所有方法中使用
        # Moved initialization to setupUI to ensure STRINGS is available
        self.file_types = [] 
        self.setupUI()
    
    def setupUI(self):
        # Initialize file_types here where STRINGS should be resolved
        self.file_types = [
            ("po_file", STRINGS["SPX_TAB_FILE_TYPES_PO_FILE"]),
            ("previous_wp", STRINGS["SPX_TAB_FILE_TYPES_PREVIOUS_WP_PO"]),
            ("procurement", STRINGS["SPX_TAB_FILE_TYPES_PROCUREMENT_PO"]),
            ("ap_invoice", STRINGS["SPX_TAB_FILE_TYPES_AP_INVOICE"]),
            ("previous_wp_pr", STRINGS["SPX_TAB_FILE_TYPES_PREVIOUS_WP_PR"]),
            ("procurement_pr", STRINGS["SPX_TAB_FILE_TYPES_PROCUREMENT_PR"])
        ]
        """設置UI組件"""
        # 主布局
        main_layout = QHBoxLayout(self)
        
        # 左側和右側布局
        left_layout = QVBoxLayout()
        right_layout = QVBoxLayout()
        
        # 設置左右比例
        main_layout.addLayout(left_layout, 60)
        main_layout.addLayout(right_layout, 40)
        
        # === 左側部分 ===
        # 文件上傳區域
        upload_group = QGroupBox(STRINGS["SPX_GRP_FILE_UPLOAD"])
        grid_layout = QGridLayout()
        
        # 上傳按鈕和標籤
        self.buttons = {}
        self.labels = {}
        
        # 使用類屬性 self.file_types
        for row, (file_key, file_label_from_strings) in enumerate(self.file_types): 
            # 標籤
            label = QLabel(f"{file_label_from_strings}:")
            grid_layout.addWidget(label, row, 0)
            
            # 顯示文件名的標籤
            file_name_label = QLabel(STRINGS["SPX_LBL_FILENAME_NOT_SELECTED"])
            file_name_label.setStyleSheet("color: gray;")
            grid_layout.addWidget(file_name_label, row, 1)
            self.labels[file_key] = file_name_label
            
            # 上傳按鈕
            upload_btn = QPushButton(STRINGS["SPX_BTN_SELECT_FILE"])
            upload_btn.clicked.connect(lambda checked, k=file_key, fl=file_label_from_strings: self.select_file(k, fl))
            grid_layout.addWidget(upload_btn, row, 2)
            self.buttons[file_key] = upload_btn
        
        upload_group.setLayout(grid_layout)
        
        # 批次處理參數
        process_group = QGroupBox(STRINGS["SPX_GRP_PROCESS_PARAMS"])
        process_layout = QGridLayout()
        
        # 財務年月
        process_layout.addWidget(QLabel(STRINGS["SPX_LBL_PERIOD_YYYYMM"]), 0, 0)
        self.period_input = QLineEdit()
        self.period_input.setPlaceholderText(STRINGS["SPX_PLACEHOLDER_PERIOD_YYYYMM"])
        process_layout.addWidget(self.period_input, 0, 1)
        
        # 處理人員
        process_layout.addWidget(QLabel(STRINGS["SPX_LBL_USER"]), 1, 0)
        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText(STRINGS["SPX_PLACEHOLDER_USER"])
        process_layout.addWidget(self.user_input, 1, 1)
        
        process_group.setLayout(process_layout)
        
        # 已上傳文件列表
        self.file_list = QListWidget()
        file_list_label = QLabel(STRINGS["SPX_LBL_UPLOADED_FILES_TITLE"])
        file_list_label.setStyleSheet("font-size:12pt;background-color:#93FF93;color:#000080;font:Bold")
        file_list_label.setAlignment(Qt.AlignCenter)
        
        # 將元素添加到左側布局
        left_layout.addWidget(upload_group)
        left_layout.addWidget(process_group)
        left_layout.addWidget(file_list_label)
        left_layout.addWidget(self.file_list)
        
        # === 右側部分 ===
        # 操作按鈕
        self.process_btn = QPushButton(STRINGS["SPX_BTN_PROCESS_GENERATE"])
        self.process_btn.clicked.connect(self.process_files)
        self.process_btn.setStyleSheet("font-size:12pt;font:Bold;padding:10px;")
        
        self.clear_btn = QPushButton(STRINGS["SPX_BTN_CLEAR_ALL_FILES"])
        self.clear_btn.clicked.connect(self.clear_all_files)
        
        self.export_btn = QPushButton(STRINGS["SPX_BTN_EXPORT_UPLOAD_FORM"])
        self.export_btn.clicked.connect(self.export_upload_form)
        
        # 說明文字
        tips_label = QLabel(STRINGS["SPX_LBL_MODULE_DESCRIPTION_TITLE"])
        tips_label.setStyleSheet("font-size:11pt;font:Bold;")
        
        tips_content = QLabel(STRINGS["SPX_LBL_MODULE_DESCRIPTION_CONTENT"])
        tips_content.setWordWrap(True)
        tips_content.setStyleSheet("color:#000080;padding:10px;border-radius:5px;")
        
        # 狀態欄
        self.status_label = QLabel(STRINGS["MAIN_LBL_STATUS_PREFIX"] + STRINGS["STATUS_READY"])
        self.status_label.setStyleSheet("font-size:10pt;")
        
        # 添加元素到右側佈局
        right_layout.addWidget(self.process_btn)
        right_layout.addWidget(self.clear_btn)
        right_layout.addWidget(self.export_btn)
        right_layout.addSpacing(20)
        right_layout.addWidget(tips_label)
        right_layout.addWidget(tips_content)
        right_layout.addStretch()
        right_layout.addWidget(self.status_label)
    
    def select_file(self, file_key, file_label):
        """選擇文件
        
        Args:
            file_key: 文件類型鍵
            file_label: 文件類型標籤 (already localized from STRINGS)
        """
        try:
            # 根據文件類型選擇過濾器
            if file_key == "ap_invoice": 
                file_filter = STRINGS["SPX_FILE_DIALOG_FILTER_EXCEL_ALL"]
            else:
                file_filter = STRINGS["SPX_FILE_DIALOG_FILTER_DEFAULT"]
            
            # 打開文件選擇對話框
            file_path, _ = QFileDialog.getOpenFileName(
                self, STRINGS["SPX_FILE_DIALOG_SELECT_PREFIX"] + file_label, "", file_filter
            )
            
            if not file_path:
                return
            
            # 保存文件路徑
            self.file_paths[file_key] = file_path
            file_name = os.path.basename(file_path)
            
            # 更新標籤和文件列表
            self.labels[file_key].setText(file_name)
            self.labels[file_key].setStyleSheet("color: blue;")
            
            # 更新文件列表
            list_item = f"{file_label}: {file_name}"
            # 檢查是否已存在於列表
            existing_items = [self.file_list.item(i).text() for i in range(self.file_list.count())]
            matching_items = [item for item in existing_items if item.startswith(f"{file_label}:")]
            
            if matching_items:
                # 替換已存在的項目
                for item in matching_items:
                    item_idx = existing_items.index(item)
                    self.file_list.takeItem(item_idx)
            
            self.file_list.addItem(list_item)
            
            # 更新狀態
            self.status_label.setText(STRINGS["SPX_STATUS_FILE_SELECTED_PREFIX"] + file_label)
            
            # 如果是主文件，嘗試填充年月字段
            if file_key == "po_file" and not self.period_input.text(): 
                try:
                    # 從文件名提取年月 (假設格式為 YYYYMM_其他內容.csv)
                    year_month = file_name[:6]
                    if year_month.isdigit() and len(year_month) == 6:
                        self.period_input.setText(year_month)
                except Exception:
                    pass # Dev: Log this?
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.info(f"已選擇 {file_label}: {file_name}") # Dev log
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText(STRINGS["SPX_STATUS_ERR_SELECTING_FILE"])
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"選擇文件時出錯: {str(e)}", exc_info=True) # Dev log
            
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['SPX_STATUS_ERR_SELECTING_FILE']}:\n{str(e)}")
    
    def process_files(self):
        """處理文件"""
        try:
            # 檢查必須文件
            required_files = ["po_file", "ap_invoice"] # Internal keys
            missing_files = [file_key for file_key in required_files if file_key not in self.file_paths]
            
            if missing_files:
                file_type_dict = dict(self.file_types) 
                missing_labels = [file_type_dict.get(file_key, file_key) for file_key in missing_files]

                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_MISSING_FILES_PREFIX"] + ', '.join(missing_labels))
                return
            
            # 檢查處理參數
            period = self.period_input.text()
            user = self.user_input.text()
            
            if not period or not period.isdigit() or len(period) != 6:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_PERIOD_FORMAT"])
                return
            
            if not user:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_USER"])
                return
            
            # 更新狀態
            self.status_label.setText(STRINGS["SPX_STATUS_PROCESSING"])
            self.status_label.setStyleSheet("font-size:10pt; color: blue;")
            
            # 準備文件路徑參數
            po_file_path = self.file_paths.get("po_file") 
            po_file_name = os.path.basename(po_file_path)
            
            # 構建處理參數字典
            processing_params = {
                'po_file': po_file_path,
                'po_file_name': po_file_name,
                'previous_wp': self.file_paths.get("previous_wp"),
                'procurement': self.file_paths.get("procurement"),
                'ap_invoice': self.file_paths.get("ap_invoice"),
                'previous_wp_pr': self.file_paths.get("previous_wp_pr"),
                'procurement_pr': self.file_paths.get("procurement_pr"),
                'period': period,
                'user': user
            }
            
            # 調用處理函數 - 這裡連接到主程序的SPX處理邏輯
            self._process_spx_data(processing_params)
            
            # 更新狀態
            self.status_label.setText(STRINGS["SPX_STATUS_PROCESS_COMPLETE"])
            self.status_label.setStyleSheet("font-size:10pt; color: green;")
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText(STRINGS["SPX_STATUS_PROCESS_ERROR"])
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"處理文件時出錯: {str(e)}", exc_info=True) # Dev Log
            
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['SPX_STATUS_PROCESS_ERROR']}:\n{str(e)}")
    
    def _process_spx_data(self, params):
        """處理SPX數據
        
        Args:
            params: 處理參數字典
        """
        # 這個方法將連接到主程序的SPX處理邏輯
        # 在實際整合時，這裡會調用SPXTW_PO的處理方法
        
        try:
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.info(f"開始處理SPX數據: {params['po_file_name']}") # Dev log
            
            # 實例化SPXTW_PO並處理數據
            from .spxtwpo import SPXTW_PO 
            processor = SPXTW_PO()
            
            # 使用並發處理方法
            processor.process(
                fileUrl=params['po_file'],
                file_name=params['po_file_name'],
                fileUrl_previwp=params['previous_wp'],
                fileUrl_p=params['procurement'],
                fileUrl_ap=params['ap_invoice'],
                fileUrl_previwp_pr=params['previous_wp_pr'],
                fileUrl_p_pr=params['procurement_pr']
            )
            
            # 顯示成功訊息
            QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["SPX_INFO_PROCESS_COMPLETE"])
            
        except Exception as e:
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"處理SPX數據時出錯: {str(e)}", exc_info=True) # Dev log
            raise
    
    def clear_all_files(self):
        """清除所有已選文件"""
        try:
            # 清除文件路徑
            self.file_paths = {}
            
            # 清除標籤
            for label in self.labels.values():
                label.setText(STRINGS["SPX_LBL_FILENAME_NOT_SELECTED"])
                label.setStyleSheet("color: gray;")
            
            # 清除文件列表
            self.file_list.clear()
            
            # 更新狀態
            self.status_label.setText(STRINGS["SPX_STATUS_FILES_CLEARED"])
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.info("已清除所有SPX文件") # Dev log
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText(STRINGS["SPX_STATUS_ERR_CLEARING_FILES"])
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"清除文件時出錯: {str(e)}", exc_info=True) # Dev log
            
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['SPX_STATUS_ERR_CLEARING_FILES']}:\n{str(e)}")
    
    def export_upload_form(self):
        """匯出上傳表單"""
        try:
            # 檢查必須文件
            if "po_file" not in self.file_paths: 
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_NO_PO_FILE_FOR_UPLOAD_FORM"])
                return
            
            # 檢查處理參數
            period = self.period_input.text()
            user = self.user_input.text()
            
            if not period or not period.isdigit() or len(period) != 6:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_PERIOD_FORMAT"])
                return
            
            if not user:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_USER"])
                return
            
            # 更新狀態
            self.status_label.setText(STRINGS["SPX_STATUS_EXPORTING_UPLOAD_FORM"])
            self.status_label.setStyleSheet("font-size:10pt; color: blue;")
            
            # 構建參數
            entity = STRINGS["SPX_ENTITY_NAME_INTERNAL"] 
            year = period[:4]
            month = period[4:6]
            period_str = f"{year}-{month}"  
            month_abbr = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",  
                          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"][int(month) - 1] 
            period_display = f"{month_abbr}-{year[2:]}"  
            accounting_date = f"{year}/{month}/25"  
            category = STRINGS["SPX_CATEGORY_DEFAULT"] 
            
            # 調用上傳表單處理函數
            self._generate_upload_form(
                po_file_path=self.file_paths.get("po_file"), 
                entity=entity,
                period=period_display,
                period_str=period_str,
                accounting_date=accounting_date,
                category=category,
                user=user
            )
            
            # 更新狀態
            self.status_label.setText(STRINGS["SPX_STATUS_UPLOAD_FORM_EXPORTED"])
            self.status_label.setStyleSheet("font-size:10pt; color: green;")
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText(STRINGS["SPX_STATUS_ERR_EXPORTING_UPLOAD_FORM"])
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"匯出上傳表單時出錯: {str(e)}", exc_info=True) # Dev log
            
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['SPX_STATUS_ERR_EXPORTING_UPLOAD_FORM']}:\n{str(e)}")
    
    def _generate_upload_form(self, po_file_path, entity, period, period_str, accounting_date, category, user):
        """生成上傳表單
        
        Args:
            po_file_path: PO文件路徑
            entity: 實體名稱
            period: 期間顯示格式 (如 "APR-25")
            period_str: 期間字符串格式 (如 "2025-04")
            accounting_date: 會計日期 (如 "2025/04/25")
            category: 類別
            user: 處理人員
        """
        try:
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.info(f"開始生成上傳表單: {entity}, {period}") # Dev log
            
            from .upload_form import get_aggregation_twd, get_entries 
            
            # 獲取聚合數據
            dfs = get_aggregation_twd(po_file_path, period_str, is_mob=False)
            
            # 生成上傳表單
            result = get_entries(dfs, entity, period, accounting_date, category, user, STRINGS["SPX_CURRENCY_DEFAULT"]) 
            
            # 保存結果
            output_file = f'Upload Form-{entity}-{period}-{STRINGS["SPX_CURRENCY_DEFAULT"]}.xlsx' # Filename
            result.to_excel(output_file, index=False)
            
            # 顯示成功訊息
            QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], f"{STRINGS['SPX_INFO_UPLOAD_FORM_GENERATED_PREFIX']}{output_file}")
            
        except Exception as e:
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"生成上傳表單時出錯: {str(e)}", exc_info=True) # Dev log
            raise

def main():
    """主函數"""
    try:
        # 創建應用
        APP = QApplication(sys.argv)
        window = Main()
        
        # 應用樣式
        apply_stylesheet(APP, theme='dark_lightgreen.xml')
        
        # 運行應用
        sys.exit(APP.exec_())
        
    except Exception as e:
        logging.error(f"啟動應用時出錯: {str(e)}", exc_info=True)
        print(f"啟動應用時出錯: {str(e)}")


# 程序入口
if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        print('應用正常退出') # Dev message
    except Exception as e:
        # Using a generic error message here as STRINGS might not be available if an error occurs very early.
        # Fallback to a literal string if STRINGS itself is the cause of an early error.
        err_msg_template = "Application Error: {}"
        try:
            err_msg_template = STRINGS.get("MSGBOX_ERR_PROCESSING_FAILED_DETAILED", "Application Error: {}")
        except NameError: # If STRINGS is not defined at all
            pass
        err_msg = err_msg_template.format(str(e))
        print(err_msg)
        logging.error(err_msg, exc_info=True) # Dev log
