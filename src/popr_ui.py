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
from PyQt5.QtGui import QPixmap, QFont, QTextCursor
from qt_material import apply_stylesheet

# 導入處理模塊
from spttwpo import SPTTW_PO
from spttwpr import SPTTW_PR
from mobtwpr import MOBTW_PR
from mobtwpo import MOBTW_PO
from hris_dup import HRISDuplicateChecker
from upload_form import get_aggregation_twd, get_aggregation_foreign, get_entries
from upload_form_spx import get_aggregation_spx, get_spx_entries
from utils import Logger, ReconEntryAmt


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
            self.logger.info("開始HRIS重複檢查")
            self.updateStatus("處理HRIS重複檢查...")
            
            # 選擇文件
            urls = QFileDialog.getOpenFileUrls(self, '產生的PR/PO/AP invoice')
            
            if not urls[0]:
                self.logger.info("未選擇文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            # 提取文件路徑
            file_urls = [url.url() for url in urls[0]]
            
            # 讀取PR、PO和AP文件
            try:
                df_pr = pd.read_excel([url for url in file_urls if 'PR' in os.path.basename(url)][0], dtype=str)
                df_po = pd.read_excel([url for url in file_urls if 'PO' in os.path.basename(url)][0], dtype=str)
                df_ap = pd.read_excel([url for url in file_urls if 'AP' in os.path.basename(url)][0],
                                      dtype=str, header=1, sheet_name=1)
                                      
                self.logger.info(f"成功讀取文件 PR:{df_pr.shape}, PO:{df_po.shape}, AP:{df_ap.shape}")
            except Exception as err:
                self.logger.error(f"讀取文件失敗: {str(err)}", exc_info=True)
                self.updateStatus("錯誤: 文件讀取失敗", error=True)
                QMessageBox.critical(self, "錯誤", f"文件讀取失敗:\n{str(err)}")
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
                    self.updateStatus("處理過程中發生錯誤，停止操作", error=True)
                    return
                
                # 保存文件
                checker.save_files(df_pr_p, df_po_p)
                
                self.updateStatus("HRIS重複檢查完成")
                QMessageBox.information(self, "完成", "HRIS重複檢查完成")
                self.logger.info("HRIS重複檢查完成")
            except Exception as err:
                self.logger.error(f"HRIS重複檢查失敗: {str(err)}", exc_info=True)
                self.updateStatus("錯誤: 重複檢查失敗", error=True)
                QMessageBox.critical(self, "錯誤", f"重複檢查失敗:\n{str(err)}")
                
        except Exception as err:
            self.logger.error(f"HRIS處理過程中出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 處理過程中出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"處理過程中出錯:\n{str(err)}")
    
    def import_raw(self):
        """導入原始數據文件"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus("導入原始數據...")
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(
                self, '原始數據',
                "",
                "Files(*.csv *.xlsx);;CSV(*.csv);;EXCEL(*.xlsx)"
            )
            
            if not url[0]:
                self.logger.info("未選擇原始數據文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            self.fileUrl = url[0]
            self.file_name = os.path.basename(self.fileUrl)
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if '原始資料' in items:
                self.logger.warning("原始資料已存在")
                self.updateStatus("警告: 已導入原始資料")
                QMessageBox.warning(self, "警告", "已導入原始資料")
                return
            
            # 檢查文件名格式
            ym = self.file_name[0:6]
            if re.match(r'[0-9]{6}', str(ym)) is None:
                self.logger.warning("文件名格式錯誤，需要包含年月")
                self.updateStatus("錯誤: 文件名格式不正確", error=True)
                QMessageBox.warning(self, "警告", "請檢查文件名是否包含年月(YYYYMM)")
                return
                
            # 添加到列表
            self.importedList.addItem('原始資料')
            self.logger.info(f"成功導入原始資料: {self.file_name}")
            self.updateStatus(f"已導入: {self.file_name}")
            
        except Exception as err:
            self.logger.error(f"導入原始數據時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 導入原始數據時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"導入原始數據時出錯:\n{str(err)}")
    
    def import_closing(self):
        """導入關單清單"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus("導入關單清單...")
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(
                self, '關單清單',
                "",
                "Files(*.csv *.xlsx);;CSV(*.csv);;EXCEL(*.xlsx)"
            )
            
            if not url[0]:
                self.logger.info("未選擇關單清單文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            self.fileUrl_c = url[0]
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if '關單清單' in items:
                self.logger.warning("關單清單已存在")
                self.updateStatus("警告: 已導入關單清單")
                QMessageBox.warning(self, "警告", "已導入關單清單")
                return
                
            # 添加到列表
            self.importedList.addItem('關單清單')
            self.logger.info(f"成功導入關單清單: {os.path.basename(self.fileUrl_c)}")
            self.updateStatus(f"已導入: {os.path.basename(self.fileUrl_c)}")
            
        except Exception as err:
            self.logger.error(f"導入關單清單時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 導入關單清單時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"導入關單清單時出錯:\n{str(err)}")
    
    def import_previousup(self):
        """導入前期底稿"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus("導入前期底稿...")
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(self, '前期底稿', "", "EXCEL(*.xlsx)")
            
            if not url[0]:
                self.logger.info("未選擇前期底稿文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            self.fileUrl_previwp = url[0]
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if '前期底稿' in items:
                self.logger.warning("前期底稿已存在")
                self.updateStatus("警告: 已導入前期底稿")
                QMessageBox.warning(self, "警告", "已導入前期底稿")
                return
            
            # 驗證文件格式
            try:
                column_checking = pd.read_excel(
                    self.fileUrl_previwp, dtype=str, nrows=3, engine='openpyxl').columns.tolist()
                important_col = ['Remarked by Procurement', 'Noted by Procurement', 'Line#']
                
                if not set(important_col).issubset(column_checking):
                    self.logger.warning("前期底稿格式錯誤，缺少必要列")
                    self.updateStatus("錯誤: 前期底稿格式不正確", error=True)
                    warning_text = "請檢查前期底稿是否包含必要欄位: Remarked by Procurement, Noted by Procurement, Line#"
                    QMessageBox.warning(self, "警告", warning_text)
                    return
                    
                # 添加到列表
                self.importedList.addItem('前期底稿')
                self.logger.info(f"成功導入前期底稿: {os.path.basename(self.fileUrl_previwp)}")
                self.updateStatus(f"已導入: {os.path.basename(self.fileUrl_previwp)}")
                
            except Exception as e:
                self.logger.error(f"驗證前期底稿時出錯: {str(e)}", exc_info=True)
                self.updateStatus("錯誤: 驗證前期底稿時出錯", error=True)
                QMessageBox.critical(self, "錯誤", f"驗證前期底稿時出錯:\n{str(e)}")
                
        except Exception as err:
            self.logger.error(f"導入前期底稿時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 導入前期底稿時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"導入前期底稿時出錯:\n{str(err)}")
    
    def import_procurement(self):
        """導入採購底稿"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus("導入採購底稿...")
            
            # 選擇文件
            url = QFileDialog.getOpenFileName(self, '採購底稿', "", "EXCEL(*.xlsx)")
            
            if not url[0]:
                self.logger.info("未選擇採購底稿文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            self.fileUrl_p = url[0]
            
            # 檢查是否已經導入
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            
            if '採購底稿' in items:
                self.logger.warning("採購底稿已存在")
                self.updateStatus("警告: 已導入採購底稿")
                QMessageBox.warning(self, "警告", "已導入採購底稿")
                return
            
            # 驗證文件格式
            try:
                column_checking = pd.read_excel(self.fileUrl_p, dtype=str, nrows=3, engine='openpyxl').columns.tolist()
                important_col = ['Remarked by Procurement', 'Noted by Procurement', 'Line#']
                
                if not set(important_col).issubset(column_checking):
                    self.logger.warning("採購底稿格式錯誤，缺少必要列")
                    self.updateStatus("錯誤: 採購底稿格式不正確", error=True)
                    warning_text = "請檢查採購底稿是否包含必要欄位: Remarked by Procurement, Noted by Procurement, Line#"
                    QMessageBox.warning(self, "警告", warning_text)
                    return
                    
                # 添加到列表
                self.importedList.addItem('採購底稿')
                self.logger.info(f"成功導入採購底稿: {os.path.basename(self.fileUrl_p)}")
                self.updateStatus(f"已導入: {os.path.basename(self.fileUrl_p)}")
                
            except Exception as e:
                self.logger.error(f"驗證採購底稿時出錯: {str(e)}", exc_info=True)
                self.updateStatus("錯誤: 驗證採購底稿時出錯", error=True)
                QMessageBox.critical(self, "錯誤", f"驗證採購底稿時出錯:\n{str(e)}")
                
        except Exception as err:
            self.logger.error(f"導入採購底稿時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 導入採購底稿時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"導入採購底稿時出錯:\n{str(err)}")
    
    def process(self):
        """處理數據"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus("開始處理數據...")
            
            # 獲取導入的文件列表
            items = []
            for x in range(self.importedList.count()):
                items.append(self.importedList.item(x).text())
                
            self.logger.info(f"處理數據，已導入項目: {items}")
            
            # 獲取選擇的實體類型
            module_type = self.entitySelect.currentText()
            
            # 檢查是否已導入原始數據
            if '原始資料' not in items:
                self.logger.warning("未導入原始數據")
                self.updateStatus("錯誤: 未導入原始數據", error=True)
                QMessageBox.warning(self, "警告", "請先導入原始數據")
                return
            
            # 根據實體類型和導入文件選擇處理模式
            if module_type == 'MOBA_PR' and '原始資料' in items:
                self._process_moba_pr(items)
                if not self.had_error:
                    self.updateStatus("MOBA PR處理完成")
                    QMessageBox.information(self, "完成", "MOBA PR處理完成")
            elif module_type == 'MOBA_PO' and '原始資料' in items:
                self._process_moba_po(items)
                if not self.had_error:
                    self.updateStatus("MOBA PO處理完成")
                    QMessageBox.information(self, "完成", "MOBA PO處理完成")
            elif module_type == 'SPT_PR' and '原始資料' in items:
                self._process_spt_pr(items)
                if not self.had_error:
                    self.updateStatus("SPT PR處理完成")
                    QMessageBox.information(self, "完成", "SPT PR處理完成")
            elif module_type == 'SPT_PO' and '原始資料' in items:
                self._process_spt_po(items)
                if not self.had_error:
                    self.updateStatus("SPT PO處理完成")
                    QMessageBox.information(self, "完成", "SPT PO處理完成")
            else:
                self.logger.warning("無法確定處理模式")
                self.updateStatus("錯誤: 無法確定處理模式", error=True)
                QMessageBox.warning(self, "警告", "無法確定處理模式，請檢查選擇類型和導入文件")
                
        except Exception as err:
            self.logger.error(f"處理數據時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 處理數據時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"處理數據時出錯:\n{str(err)}")
    
    def _process_moba_pr(self, items):
        """處理MOBA PR數據"""
        try:
            processor = MOBTW_PR()
            
            if '前期底稿' in items:
                if len(items) == 4:  # 原始資料 + 前期底稿 + 採購底稿 + 關單清單
                    QMessageBox.warning(self, "警告", "Locked")
                    self.updateStatus("錯誤: Locked", error=True)
                    self.had_error = True
                    return
                elif len(items) == 3 and '採購底稿' in items:  # 原始資料 + 前期底稿 + 採購底稿
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_previwp)
                elif len(items) == 3 and '關單清單' in items:  # 原始資料 + 前期底稿 + 關單清單
                    QMessageBox.warning(self, "警告", "Locked")
                    self.updateStatus("錯誤: Locked", error=True)
                    self.had_error = True
                    return
                elif len(items) == 2:  # 原始資料 + 前期底稿
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif len(items) == 1:  # 只有原始資料
                processor.mode_4(self.fileUrl, self.file_name)
            elif set(['原始資料', '關單清單', '採購底稿']).issubset(items):  # 模式1
                processor.mode_1(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set(['原始資料', '採購底稿']).issubset(items):  # 模式3
                processor.mode_3(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set(['原始資料', '關單清單']).issubset(items):  # 模式2
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_c)
                
            self.logger.info("MOBA PR處理完成")
            
        except Exception as e:
            self.logger.error(f"處理MOBA PR時出錯: {str(e)}", exc_info=True)
            self.updateStatus("錯誤: 處理MOBA PR時出錯", error=True)
            self.had_error = True
            QMessageBox.critical(self, "錯誤", f"處理MOBA PR時出錯:\n{str(e)}")
    
    def _process_moba_po(self, items):
        """處理MOBA PO數據"""
        try:
            processor = MOBTW_PO()
            
            if set(['原始資料', '關單清單', '採購底稿', '前期底稿']).issubset(items):  # 模式1
                QMessageBox.warning(self, "警告", "Locked")
                self.updateStatus("錯誤: Locked", error=True)
                self.had_error = True
                return
            elif set(['原始資料', '採購底稿', '前期底稿']).issubset(items):  # 模式2
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_previwp, self.fileUrl_p)
            elif set(['原始資料', '關單清單', '前期底稿']).issubset(items):  # 模式3
                QMessageBox.warning(self, "警告", "Locked")
                self.updateStatus("錯誤: Locked", error=True)
                self.had_error = True
                return
            elif set(['原始資料', '關單清單', '採購底稿']).issubset(items):  # 模式4
                processor.mode_4(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set(['原始資料', '採購底稿']).issubset(items):  # 模式5
                processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set(['原始資料', '前期底稿']).issubset(items):  # 模式6
                processor.mode_6(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif set(['原始資料', '關單清單']).issubset(items):  # 模式7
                processor.mode_7(self.fileUrl, self.file_name, self.fileUrl_c)
            else:  # 模式8
                processor.mode_8(self.fileUrl, self.file_name)
                
            self.logger.info("MOBA PO處理完成")
            
        except Exception as e:
            self.logger.error(f"處理MOBA PO時出錯: {str(e)}", exc_info=True)
            self.updateStatus("錯誤: 處理MOBA PO時出錯", error=True)
            self.had_error = True
            QMessageBox.critical(self, "錯誤", f"處理MOBA PO時出錯:\n{str(e)}")
    
    def _process_spt_pr(self, items):
        """處理SPT PR數據"""
        try:
            processor = SPTTW_PR()
            
            if '前期底稿' in items:
                if len(items) == 4:  # 原始資料 + 前期底稿 + 採購底稿 + 關單清單
                    QMessageBox.warning(self, "警告", "Locked")
                    self.updateStatus("錯誤: Locked", error=True)
                    self.had_error = True
                    return
                elif len(items) == 3 and '採購底稿' in items:  # 原始資料 + 前期底稿 + 採購底稿
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_previwp)
                elif len(items) == 3 and '關單清單' in items:  # 原始資料 + 前期底稿 + 關單清單
                    QMessageBox.warning(self, "警告", "Locked")
                    self.updateStatus("錯誤: Locked", error=True)
                    self.had_error = True
                    return
                elif len(items) == 2:  # 原始資料 + 前期底稿
                    processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif len(items) == 1:  # 只有原始資料
                processor.mode_4(self.fileUrl, self.file_name)
            elif set(['原始資料', '關單清單', '採購底稿']).issubset(items):  # 模式1
                processor.mode_1(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set(['原始資料', '採購底稿']).issubset(items):  # 模式3
                processor.mode_3(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set(['原始資料', '關單清單']).issubset(items):  # 模式2
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_c)
                
            self.logger.info("SPT PR處理完成")
            
        except Exception as e:
            self.logger.error(f"處理SPT PR時出錯: {str(e)}", exc_info=True)
            self.updateStatus("錯誤: 處理SPT PR時出錯", error=True)
            self.had_error = True
            QMessageBox.critical(self, "錯誤", f"處理SPT PR時出錯:\n{str(e)}")
    
    def _process_spt_po(self, items):
        """處理SPT PO數據"""
        try:
            processor = SPTTW_PO()
            
            if set(['原始資料', '關單清單', '採購底稿', '前期底稿']).issubset(items):  # 模式1
                QMessageBox.warning(self, "警告", "Locked")
                self.updateStatus("錯誤: Locked", error=True)
                self.had_error = True
                return
            elif set(['原始資料', '採購底稿', '前期底稿']).issubset(items):  # 模式2
                processor.mode_2(self.fileUrl, self.file_name, self.fileUrl_previwp, self.fileUrl_p)
            elif set(['原始資料', '關單清單', '前期底稿']).issubset(items):  # 模式3
                QMessageBox.warning(self, "警告", "Locked")
                self.updateStatus("錯誤: Locked", error=True)
                self.had_error = True
                return
            elif set(['原始資料', '關單清單', '採購底稿']).issubset(items):  # 模式4
                processor.mode_4(self.fileUrl, self.file_name, self.fileUrl_p, self.fileUrl_c)
            elif set(['原始資料', '採購底稿']).issubset(items):  # 模式5
                processor.mode_5(self.fileUrl, self.file_name, self.fileUrl_p)
            elif set(['原始資料', '前期底稿']).issubset(items):  # 模式6
                processor.mode_6(self.fileUrl, self.file_name, self.fileUrl_previwp)
            elif set(['原始資料', '關單清單']).issubset(items):  # 模式7
                processor.mode_7(self.fileUrl, self.file_name, self.fileUrl_c)
            else:  # 模式8
                processor.mode_8(self.fileUrl, self.file_name)
                
            self.logger.info("SPT PO處理完成")
            
        except Exception as e:
            self.logger.error(f"處理SPT PO時出錯: {str(e)}", exc_info=True)
            self.updateStatus("錯誤: 處理SPT PO時出錯", error=True)
            self.had_error = True
            QMessageBox.critical(self, "錯誤", f"處理SPT PO時出錯:\n{str(e)}")

    def deleteImportFile(self):
        """刪除已導入的文件"""
        try:
            # 檢查是否選擇了要刪除的項目
            if not self.importedList.selectedItems():
                self.logger.warning("未選擇要刪除的項目")
                self.updateStatus("警告: 未選擇要刪除的項目")
                QMessageBox.warning(self, "警告", "請選擇要刪除的項目")
                return
                
            # 獲取選中的項目
            index = self.importedList.currentRow()
            item = self.importedList.currentItem()
            
            # 確認刪除
            mbox = QMessageBox.question(
                self, "警告", f"確定要刪除 {item.text()} 嗎?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            
            if mbox == QMessageBox.Yes:
                try:
                    self.importedList.takeItem(index)
                    self.logger.info(f"已刪除項目: {item.text()}")
                    self.updateStatus(f"已刪除: {item.text()}")
                except Exception as err:
                    self.logger.error(f"刪除項目時出錯: {str(err)}", exc_info=True)
                    self.updateStatus("錯誤: 刪除項目時出錯", error=True)
                    QMessageBox.critical(self, "錯誤", "刪除項目時出錯")
                    
        except Exception as err:
            self.logger.error(f"刪除文件時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 刪除文件時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"刪除文件時出錯:\n{str(err)}")
    
    def uploadFormWidget(self):
        """打開Upload Form對話框"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus("打開Upload Form對話框...")
            
            sub_widget = UploadFormWidget(self)
            sub_widget.exec_()
            
            self.updateStatus("準備就緒")
            
        except Exception as err:
            self.logger.error(f"打開Upload Form對話框時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 打開Upload Form對話框時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"打開Upload Form對話框時出錯:\n{str(err)}")
    
    def check2(self):
        """執行兩期檢查"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.logger.info("開始兩期檢查")
            self.updateStatus("執行兩期檢查...")
            
            # 選擇文件
            url_pr = QFileDialog.getOpenFileName(self, '採購PR', "", "EXCEL(*.xlsx)")
            if not url_pr[0]:
                self.logger.info("未選擇採購PR文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            url_po = QFileDialog.getOpenFileName(self, '採購PO', "", "EXCEL(*.xlsx)")
            if not url_po[0]:
                self.logger.info("未選擇採購PO文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            url_ac = QFileDialog.getOpenFileName(self, '前期底稿', "", "EXCEL(*.xlsm)")
            if not url_ac[0]:
                self.logger.info("未選擇前期底稿文件，取消操作")
                self.updateStatus("準備就緒")
                return
            
            # 執行差異比較
            try:
                a, b, c = ReconEntryAmt.get_difference(url_ac[0], url_pr[0], url_po[0])
                
                # 檢查是否有錯誤
                if self.checkHadError():
                    self.updateStatus("處理過程中發生錯誤，停止操作", error=True)
                    return
                
                # 輸出結果
                pd.DataFrame({**a, **b, **c}, index=[0]).T.to_excel('check_dif_amount.xlsx')
                
                self.updateStatus("兩期檢查完成")
                QMessageBox.information(self, '完成', '兩期檢查完成，結果已保存到 check_dif_amount.xlsx')
                self.logger.info("兩期檢查完成")
            except Exception as err:
                self.logger.error(f"執行差異比較時出錯: {str(err)}", exc_info=True)
                self.updateStatus("錯誤: 執行差異比較時出錯", error=True)
                QMessageBox.critical(self, "錯誤", f"執行差異比較時出錯:\n{str(err)}")
                return
            
        except Exception as err:
            self.logger.error(f"兩期檢查時出錯: {str(err)}", exc_info=True)
            self.updateStatus("錯誤: 兩期檢查時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"兩期檢查時出錯:\n{str(err)}")

    def add_spx_tab_to_main_ui(self):
        """將SPX tab添加到主UI
        
        這個函數應該添加到Main類中，用於初始化SPX Tab
        """
        # 創建SPX Tab
        self.spxTab = SPXTabWidget(self)
        
        # 將SPX Tab添加到tabWidget
        self.tabWidget.addTab(self.spxTab, "SPX模組")
        
        # 日誌記錄 - 增加檢查以防 logger 尚未初始化
        if hasattr(self, 'logger') and self.logger is not None:
            self.logger.info("SPX模組Tab已初始化")


class SPXUploadFormWidget(QDialog):
    """SPX Upload Form對話框"""
    
    def __init__(self, parent=None):
        """初始化SPX Upload Form對話框"""
        super().__init__(parent)
        self.logger = Logger().get_logger(__name__)
        self.had_error = False  # 標記是否有錯誤發生
        
        self.setWindowTitle("SPX Upload Form")
        self.setFixedSize(400, 350)  # 設置固定大小
        self.setupUI()
        self.logger.info("SPX Upload Form對話框已打開")
    
    def updateStatus(self, message, error=False):
        """更新狀態"""
        if error:
            self.had_error = True
            self.statusLabel.setStyleSheet("font-size:10pt; color: red;")
        else:
            self.statusLabel.setStyleSheet("font-size:10pt; color: black;")
            
        self.statusLabel.setText(f"狀態: {message}")
    
    def setupUI(self):
        """設置UI組件"""
        # 創建主布局
        main_layout = QVBoxLayout()
        
        # 創建表單布局
        form_layout = QGridLayout()
        
        # Entity (實體)
        form_layout.addWidget(QLabel("Entity"), 0, 0)
        self.combo_entity = QComboBox()
        self.combo_entity.addItem("SPTTW")
        form_layout.addWidget(self.combo_entity, 0, 1)
        
        # Period (期間)
        form_layout.addWidget(QLabel("Period"), 1, 0)
        self.line_period = QLineEdit()
        self.line_period.setPlaceholderText("MAR-25")
        form_layout.addWidget(self.line_period, 1, 1)
        
        # Accounting Date (會計日期)
        form_layout.addWidget(QLabel("Accounting Date"), 2, 0)
        self.line_ac_date = QLineEdit()
        self.line_ac_date.setPlaceholderText("2025/03/31")
        form_layout.addWidget(self.line_ac_date, 2, 1)
        
        # Category (類別)
        form_layout.addWidget(QLabel("Category"), 3, 0)
        self.line_cate = QLineEdit()
        self.line_cate.setPlaceholderText("01 SEA Accrual Expense")
        self.line_cate.setText('01 SEA Accrual Expense')
        form_layout.addWidget(self.line_cate, 3, 1)
        
        # Accountant (會計人員)
        form_layout.addWidget(QLabel("Accountant"), 4, 0)
        self.line_accountant = QLineEdit()
        self.line_accountant.setPlaceholderText("Blaire")
        self.line_accountant.setText('Blaire')
        form_layout.addWidget(self.line_accountant, 4, 1)
        
        # Currency (幣別)
        form_layout.addWidget(QLabel("Currency"), 5, 0)
        self.combo_currency = QComboBox()
        currencies = ['TWD', 'USD', 'SGD']
        for c in currencies:
            self.combo_currency.addItem(c)
        form_layout.addWidget(self.combo_currency, 5, 1)
        
        # Working Paper (工作底稿)
        form_layout.addWidget(QLabel("Working Paper"), 6, 0)
        self.button_get_wp = QPushButton("SELECT WORKING PAPER")
        self.button_get_wp.clicked.connect(self.get_wp)
        self.button_get_wp.setStyleSheet("text-align: left; padding: 5px;")
        form_layout.addWidget(self.button_get_wp, 6, 1)
        
        # Process (處理)
        form_layout.addWidget(QLabel("Process"), 7, 0)
        self.button_do_upload = QPushButton("GENERATE UPLOAD FORM")
        self.button_do_upload.clicked.connect(self.process_upload_form)
        self.button_do_upload.setStyleSheet(
            "background-color: #4CAF50; color: white; font-weight: bold; "
            "text-align: left; padding: 8px;"
        )
        form_layout.addWidget(self.button_do_upload, 7, 1)
        
        # 添加表單布局到主布局
        main_layout.addLayout(form_layout)
        
        # 添加分隔線
        main_layout.addSpacing(10)
        
        # 狀態標籤
        self.statusLabel = QLabel("狀態: 準備就緒")
        self.statusLabel.setStyleSheet("font-size:10pt; border-top: 1px solid gray; padding-top: 5px;")
        main_layout.addWidget(self.statusLabel)
        
        # 設置主布局
        self.setLayout(main_layout)
    
    def get_wp(self):
        """選擇工作底稿文件"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.updateStatus("選擇工作底稿...")
            
            url = QFileDialog.getOpenFileName(
                self, 'SPX工作底稿',
                "",
                "Files(*.xlsm *.xlsx);;EXCEL(*.xlsx *.xlsm)"
            )
            
            if not url[0]:
                self.logger.info("未選擇工作底稿文件，取消操作")
                self.updateStatus("準備就緒")
                return
                
            self.fileUrl = url[0]
            file_name = os.path.basename(self.fileUrl)
            self.logger.info(f"已選擇SPX工作底稿: {file_name}")
            
            # 更新按鈕文字，顯示已選擇文件
            display_name = file_name if len(file_name) <= 25 else f"{file_name[:22]}..."
            self.button_get_wp.setText(f"✓ {display_name}")
            self.button_get_wp.setStyleSheet(
                "text-align: left; padding: 5px; color: blue; font-weight: bold;"
            )
            self.updateStatus(f"已選擇 {file_name}")
            
            # 嘗試從文件名自動填充期間
            self._auto_fill_period_from_filename(file_name)
            
        except Exception as err:
            self.logger.error(f"選擇工作底稿時出錯: {str(err)}", exc_info=True)
            self.updateStatus("選擇工作底稿時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"選擇工作底稿時出錯:\n{str(err)}")
    
    def _auto_fill_period_from_filename(self, filename):
        """從文件名自動填充期間"""
        try:
            # 嘗試從文件名提取年月 (例如: 202503-SPX-PO-template.xlsm)
            import re
            
            # 查找6位數字 (YYYYMM)
            pattern = r'(20[2-9][0-9])([0-1][0-9])'
            match = re.search(pattern, filename)
            
            if match and not self.line_period.text():
                year = match.group(1)
                month = match.group(2)
                
                # 轉換為期間格式
                month_abbr = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", 
                              "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
                month_index = int(month) - 1
                
                if 0 <= month_index < 12:
                    period_display = f"{month_abbr[month_index]}-{year[2:]}"
                    self.line_period.setText(period_display)
                    
                    # 同時填充會計日期
                    if not self.line_ac_date.text():
                        import calendar
                        last_day = calendar.monthrange(int(year), int(month))[1]
                        ac_date = f"{year}/{month}/{last_day:02d}"
                        self.line_ac_date.setText(ac_date)
                        
                    self.logger.info(f"自動填充期間: {period_display}, 會計日期: {ac_date}")
                    
        except Exception as e:
            # 如果自動填充失敗，不顯示錯誤，只記錄日誌
            self.logger.debug(f"自動填充期間失敗: {str(e)}")
    
    def process_upload_form(self):
        """處理並生成Upload Form"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.logger.info("開始生成SPX Upload Form")
            self.updateStatus("正在生成SPX Upload Form...")
            
            # 獲取輸入參數
            entity = self.combo_entity.currentText()
            period = self.line_period.text().strip()
            ac_date = self.line_ac_date.text().strip()
            cate = self.line_cate.text().strip()
            accountant = self.line_accountant.text().strip()
            currency = self.combo_currency.currentText()
            
            # 驗證輸入
            if not hasattr(self, 'fileUrl') or not self.fileUrl:
                self.updateStatus("未選擇工作底稿", error=True)
                QMessageBox.warning(self, "警告", "請選擇SPX工作底稿")
                return
                
            if not period:
                self.updateStatus("未輸入期間", error=True)
                QMessageBox.warning(self, "警告", "請輸入期間 (例如: MAR-25)")
                return
                
            if not ac_date:
                self.updateStatus("未輸入會計日期", error=True)
                QMessageBox.warning(self, "警告", "請輸入會計日期 (例如: 2025/03/31)")
                return
            
            if not accountant:
                self.updateStatus("未輸入會計人員", error=True)
                QMessageBox.warning(self, "警告", "請輸入會計人員名稱")
                return
            
            # 解析會計日期
            try:
                from datetime import datetime
                m_date = datetime.strptime(ac_date, '%Y/%m/%d').date()
                ac_period = datetime.strftime(m_date, '%Y/%m')
            except ValueError:
                self.updateStatus("會計日期格式錯誤", error=True)
                QMessageBox.warning(self, "警告", "會計日期格式錯誤，應為 YYYY/MM/DD")
                return
            
            # 生成SPX Upload Form
            try:
                self.logger.info(f"處理 {entity} {currency} SPX表單")
                
                # 獲取SPX聚合數據
                dfs = get_aggregation_spx(self.fileUrl, ac_period, currency)
                
                # 檢查聚合數據是否有效
                if dfs is None or all(df is None for df in dfs):
                    raise ValueError("聚合數據處理失敗，請檢查輸入文件格式")
                
                # 生成條目
                result = get_spx_entries(dfs, entity, period, ac_date, cate, accountant, currency)
                
                # 檢查結果是否有效
                if result is None or result.empty:
                    raise ValueError("生成的上傳表單為空，請檢查數據是否符合條件")
                
                # 保存結果
                output_file = f'Upload Form-{entity}-{period}-{currency}.xlsx'
                result.to_excel(output_file, index=False)
                
                self.logger.info(f"已成功生成SPX Upload Form: {output_file}, 總行數: {len(result)}")
                self.updateStatus("已成功生成SPX Upload Form")
                
                # 顯示成功對話框
                QMessageBox.information(
                    self, "完成", 
                    f"SPX Upload Form已生成！\n\n"
                    f"檔案名稱: {output_file}\n"
                    f"總行數: {len(result)}\n"
                    f"幣別: {currency}\n\n"
                    f"檔案已保存到當前目錄。"
                )
                
            except Exception as e:
                self.logger.error(f"生成SPX Upload Form時出錯: {str(e)}", exc_info=True)
                self.updateStatus("生成SPX Upload Form時出錯", error=True)
                QMessageBox.critical(self, "錯誤", f"生成SPX Upload Form時出錯:\n{str(e)}")
                return
                
        except Exception as err:
            self.logger.error(f"處理SPX Upload Form時出錯: {str(err)}", exc_info=True)
            self.updateStatus("處理SPX Upload Form時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"處理SPX Upload Form時出錯:\n{str(err)}")


class UploadFormWidget(QDialog):
    """Upload Form對話框"""
    
    def __init__(self, parent=None):
        """初始化Upload Form對話框"""
        super().__init__(parent)
        self.logger = Logger().get_logger(__name__)
        self.had_error = False  # 標記是否有錯誤發生
        
        self.setWindowTitle("Upload Form")
        self.setupUI()
        self.logger.info("Upload Form對話框已打開")
    
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
        self.label_entity = QLabel("Entity")
        self.combo_entity = QComboBox(self)
        entities = ['MOBTW', 'SPTTW']
        for name in entities:
            self.combo_entity.addItem(name)
        
        self.label_period = QLabel("Period")
        self.line_period = QLineEdit()
        self.line_period.setPlaceholderText("JAN-24")
        
        self.label_ac_date = QLabel("Accounting Date")
        self.line_ac_date = QLineEdit()
        self.line_ac_date.setPlaceholderText("2024/01/31")
        
        self.label_cate = QLabel("Category")
        self.line_cate = QLineEdit()
        self.line_cate.setPlaceholderText("01 SEA Accrual Expense")
        self.line_cate.setText('01 SEA Accrual Expense')
        
        self.label_accountant = QLabel("Accountant")
        self.line_accountant = QLineEdit()
        self.line_accountant.setPlaceholderText("Lynn")
        self.line_accountant.setText('Lynn')
        
        self.label_currency = QLabel("Currency")
        self.combo_currency = QComboBox(self)
        currencies = ['TWD', 'USD', 'HKD']
        for c in currencies:
            self.combo_currency.addItem(c)
        
        self.label_wp = QLabel("Working Paper")
        self.button_get_wp = QPushButton("Select Working Paper")
        self.button_get_wp.clicked.connect(self.get_wp)
        
        self.label_start = QLabel("Process")
        self.button_do_upload = QPushButton("Generate Upload Form")
        self.button_do_upload.clicked.connect(self.process_upload_form)
        
        # 狀態標籤
        self.statusLabel = QLabel("狀態: 準備就緒")
        
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
            self.statusLabel.setText("狀態: 選擇工作底稿...")
            
            url = QFileDialog.getOpenFileName(
                self, '工作底稿',
                "",
                "Files(*.xlsm *.xlsx);;EXCEL(*.xlsx *.xlsm)"
            )
            
            if not url[0]:
                self.logger.info("未選擇工作底稿文件，取消操作")
                self.statusLabel.setText("狀態: 準備就緒")
                return
                
            self.fileUrl = url[0]
            self.logger.info(f"已選擇工作底稿: {os.path.basename(self.fileUrl)}")
            
            # 更新按鈕文字，顯示已選擇文件
            self.button_get_wp.setText(f"Selected: {os.path.basename(self.fileUrl)}")
            self.statusLabel.setText(f"狀態: 已選擇 {os.path.basename(self.fileUrl)}")
            
        except Exception as err:
            self.logger.error(f"選擇工作底稿時出錯: {str(err)}", exc_info=True)
            self.statusLabel.setText("狀態: 錯誤 - 選擇工作底稿時出錯")
            self.updateStatus("錯誤: 選擇工作底稿時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"選擇工作底稿時出錯:\n{str(err)}")
    
    def process_upload_form(self):
        """處理並生成Upload Form"""
        try:
            self.had_error = False  # 重置錯誤狀態
            self.logger.info("開始生成Upload Form")
            self.statusLabel.setText("狀態: 正在生成Upload Form...")
            
            # 獲取輸入參數
            entity = self.combo_entity.currentText()
            period = self.line_period.text()
            ac_date = self.line_ac_date.text()
            cate = self.line_cate.text()
            accountant = self.line_accountant.text()
            currency = self.combo_currency.currentText()
            
            # 驗證輸入
            if not hasattr(self, 'fileUrl') or not self.fileUrl:
                self.logger.warning("未選擇工作底稿")
                self.statusLabel.setText("狀態: 錯誤 - 未選擇工作底稿")
                self.updateStatus("錯誤: 未選擇工作底稿", error=True)
                QMessageBox.warning(self, "警告", "請選擇工作底稿")
                return
                
            if not period:
                self.logger.warning("未輸入期間")
                self.statusLabel.setText("狀態: 錯誤 - 未輸入期間")
                self.updateStatus("錯誤: 未輸入期間", error=True)
                QMessageBox.warning(self, "警告", "請輸入期間 (例如: JAN-24)")
                return
                
            if not ac_date:
                self.logger.warning("未輸入會計日期")
                self.statusLabel.setText("狀態: 錯誤 - 未輸入會計日期")
                self.updateStatus("錯誤: 未輸入會計日期", error=True)
                QMessageBox.warning(self, "警告", "請輸入會計日期 (例如: 2024/01/31)")
                return
            
            # 解析會計日期
            try:
                m_date = datetime.strptime(ac_date, '%Y/%m/%d').date()
                ac_period = datetime.strftime(m_date, '%Y/%m')
            except ValueError:
                self.logger.warning("會計日期格式錯誤")
                self.statusLabel.setText("狀態: 錯誤 - 會計日期格式錯誤")
                self.updateStatus("錯誤: 會計日期格式錯誤", error=True)
                QMessageBox.warning(self, "警告", "會計日期格式錯誤，應為 YYYY/MM/DD")
                return
            
            # 根據實體和貨幣選擇處理方法
            try:
                if entity == 'MOBTW':
                    if currency == 'TWD':
                        self.logger.info(f"處理 {entity} {currency} 表單")
                        dfs = get_aggregation_twd(self.fileUrl, ac_period)
                    else:
                        self.logger.info(f"處理 {entity} {currency} 表單")
                        dfs = get_aggregation_foreign(self.fileUrl, ac_period, currency=currency)
                else:  # SPTTW
                    if currency == 'TWD':
                        self.logger.info(f"處理 {entity} {currency} 表單")
                        dfs = get_aggregation_twd(self.fileUrl, ac_period, is_mob=False)
                    else:
                        self.logger.info(f"處理 {entity} {currency} 表單")
                        dfs = get_aggregation_foreign(self.fileUrl, ac_period, is_mob=False, currency=currency)
                
                # 檢查是否有錯誤
                if self.had_error:
                    self.statusLabel.setText("狀態: 處理過程中發生錯誤，停止操作")
                    return
                
                # 生成條目
                result = get_entries(dfs, entity, period, ac_date, cate, accountant, currency)
                
                # 保存結果
                output_file = f'Upload Form-{entity}-{period[:3]}-{currency}.xlsx'
                result.to_excel(output_file, index=False)
                
                self.logger.info(f"已成功生成Upload Form: {output_file}")
                self.statusLabel.setText("狀態: 已成功生成Upload Form")
                QMessageBox.information(self, "完成", f"Upload Form已生成: {output_file}")
                
            except Exception as e:
                self.logger.error(f"生成Upload Form時出錯: {str(e)}", exc_info=True)
                self.statusLabel.setText("狀態: 錯誤 - 生成Upload Form時出錯")
                self.updateStatus("錯誤: 生成Upload Form時出錯", error=True)
                QMessageBox.critical(self, "錯誤", f"生成Upload Form時出錯:\n{str(e)}")
                return
                
        except Exception as err:
            self.logger.error(f"處理Upload Form時出錯: {str(err)}", exc_info=True)
            self.statusLabel.setText("狀態: 錯誤 - 處理Upload Form時出錯")
            self.updateStatus("錯誤: 處理Upload Form時出錯", error=True)
            QMessageBox.critical(self, "錯誤", f"處理Upload Form時出錯:\n{str(err)}")


class SPXTabWidget(QWidget):
    """SPX模組專用的tab介面"""
    
    def __init__(self, parent=None):
        """初始化SPX模組tab介面"""
        super(SPXTabWidget, self).__init__(parent)
        self.parent = parent  # 存儲主窗口引用(Main class)，用於訪問日誌等功能(調用Main.logger.info運用)
        self.file_paths = {}  # 存儲所有文件路徑
        # 定義文件類型映射，作為類屬性以便在所有方法中使用
        self.file_types = [
            ("po_file", "原始PO數據"),
            ("previous_wp", "前期底稿(PO)"),
            ("procurement", "採購底稿(PO)"),
            ("ap_invoice", "AP發票文件"),
            ("previous_wp_pr", "前期PR底稿"),
            ("procurement_pr", "採購PR底稿")
        ]
        self.setupUI()
    
    def setupUI(self):
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
        upload_group = QGroupBox("文件上傳")
        grid_layout = QGridLayout()
        
        # 上傳按鈕和標籤
        self.buttons = {}
        self.labels = {}
        
        # 使用類屬性 self.file_types
        for row, (file_key, file_label) in enumerate(self.file_types):
            # 標籤
            label = QLabel(f"{file_label}:")
            grid_layout.addWidget(label, row, 0)
            
            # 顯示文件名的標籤
            file_name_label = QLabel("未選擇文件")
            file_name_label.setStyleSheet("color: gray;")
            grid_layout.addWidget(file_name_label, row, 1)
            self.labels[file_key] = file_name_label
            
            # 上傳按鈕
            upload_btn = QPushButton("選擇文件")
            upload_btn.clicked.connect(lambda checked, k=file_key, label=file_label: self.select_file(k, label))
            grid_layout.addWidget(upload_btn, row, 2)
            self.buttons[file_key] = upload_btn
        
        upload_group.setLayout(grid_layout)
        
        # 批次處理參數
        process_group = QGroupBox("處理參數")
        process_layout = QGridLayout()
        
        # 財務年月
        process_layout.addWidget(QLabel("財務年月 (YYYYMM):"), 0, 0)
        self.period_input = QLineEdit()
        self.period_input.setPlaceholderText("例如: 202504")
        process_layout.addWidget(self.period_input, 0, 1)
        
        # 處理人員
        process_layout.addWidget(QLabel("處理人員:"), 1, 0)
        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText("例如: Blaire")
        process_layout.addWidget(self.user_input, 1, 1)
        
        process_group.setLayout(process_layout)
        
        # 已上傳文件列表
        self.file_list = QListWidget()
        file_list_label = QLabel("已上傳文件")
        file_list_label.setStyleSheet("font-size:12pt;background-color:#93FF93;color:#000080;font:Bold")
        file_list_label.setAlignment(Qt.AlignCenter)
        
        # 將元素添加到左側布局
        left_layout.addWidget(upload_group)
        left_layout.addWidget(process_group)
        left_layout.addWidget(file_list_label)
        left_layout.addWidget(self.file_list)
        
        # === 右側部分 ===
        # 操作按鈕
        self.process_btn = QPushButton("處理並產生結果")
        self.process_btn.clicked.connect(self.process_files)
        self.process_btn.setStyleSheet("font-size:12pt;font:Bold;padding:10px;")
        
        self.clear_btn = QPushButton("清除所有文件")
        self.clear_btn.clicked.connect(self.clear_all_files)
        
        self.export_btn = QPushButton("匯出上傳表單")
        self.export_btn.clicked.connect(self.export_upload_form)
        self.export_btn.setStyleSheet("font-size:11pt;font:Bold;padding:8px;")
        
        # 說明文字
        tips_label = QLabel("SPX模組說明:")
        tips_label.setStyleSheet("font-size:11pt;font:Bold;")
        
        tips_content = QLabel(
            "此模組用於處理SPX相關的PO/PR數據。\n\n"
            "使用步驟:\n"
            "1. 上傳各項必要文件(原始PO數據*、AP發票文件*)\n"
            "2. 填寫處理參數(財務年月*、處理人員*)\n"
            "3. 點擊「處理並產生結果」進行數據處理\n"
            "4. 點擊「匯出上傳表單」生成Upload Form\n"
            "5. 結果將自動保存\n\n"
            "註: 標*為必要項目\n"
            "TBC:PR, Upload Form(外幣)\n"
        )
        tips_content.setWordWrap(True)
        # tips_content.setStyleSheet("background-color:#93FF93;color:#000080;padding:10px;border-radius:5px;")
        tips_content.setStyleSheet("color:#000080;padding:10px;border-radius:5px;")
        
        # 狀態欄
        self.status_label = QLabel("狀態: 準備就緒")
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
            file_label: 文件類型標籤
        """
        try:
            # 根據文件類型選擇過濾器
            if file_key == "ap_invoice":
                file_filter = "Excel Files (*.xlsx *.xlsm)"
            else:
                file_filter = "Files (*.csv *.xlsx);;CSV (*.csv);;Excel (*.xlsx *.xlsm)"
            
            # 打開文件選擇對話框
            file_path, _ = QFileDialog.getOpenFileName(
                self, f'選擇{file_label}', "", file_filter
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
            self.status_label.setText(f"狀態: 已選擇 {file_label}")
            
            # 如果是主文件，嘗試填充年月字段
            if file_key == "po_file" and not self.period_input.text():
                try:
                    # 從文件名提取年月 (假設格式為 YYYYMM_其他內容.csv)
                    year_month = file_name[:6]
                    if year_month.isdigit() and len(year_month) == 6:
                        self.period_input.setText(year_month)
                except Exception:
                    pass
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.info(f"已選擇 {file_label}: {file_name}")
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText("狀態: 選擇文件出錯")
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"選擇文件時出錯: {str(e)}", exc_info=True)
            
            QMessageBox.critical(self, "錯誤", f"選擇文件時出錯:\n{str(e)}")
    
    def process_files(self):
        """處理文件"""
        try:
            # 檢查必須文件
            required_files = ["po_file", "ap_invoice"]
            missing_files = [file_key for file_key in required_files if file_key not in self.file_paths]
            
            if missing_files:
                # 建立文件類型映射字典
                file_type_dict = dict(self.file_types)
                missing_labels = [file_type_dict.get(file_key, file_key) for file_key in missing_files]

                QMessageBox.warning(self, "警告", f"缺少必要文件: {', '.join(missing_labels)}")
                return
            
            # 檢查處理參數
            period = self.period_input.text()
            user = self.user_input.text()
            
            if not period or not period.isdigit() or len(period) != 6:
                QMessageBox.warning(self, "警告", "請輸入有效的財務年月 (格式: YYYYMM)")
                return
            
            if not user:
                QMessageBox.warning(self, "警告", "請輸入處理人員名稱")
                return
            
            # 更新狀態
            self.status_label.setText("狀態: 處理中...")
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
            self.status_label.setText("狀態: 處理完成!")
            self.status_label.setStyleSheet("font-size:10pt; color: green;")
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText("狀態: 處理出錯")
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"處理文件時出錯: {str(e)}", exc_info=True)
            
            QMessageBox.critical(self, "錯誤", f"處理文件時出錯:\n{str(e)}")
    
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
                self.parent.logger.info(f"開始處理SPX數據: {params['po_file_name']}")
            
            # 實例化SPXTW_PO並處理數據
            from spxtwpo import SPXTW_PO
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
            QMessageBox.information(self, "完成", "SPX數據處理完成！")
            
        except Exception as e:
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"處理SPX數據時出錯: {str(e)}", exc_info=True)
            raise
    
    def clear_all_files(self):
        """清除所有已選文件"""
        try:
            # 清除文件路徑
            self.file_paths = {}
            
            # 清除標籤
            for label in self.labels.values():
                label.setText("未選擇文件")
                label.setStyleSheet("color: gray;")
            
            # 清除文件列表
            self.file_list.clear()
            
            # 更新狀態
            self.status_label.setText("狀態: 已清除所有文件")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.info("已清除所有SPX文件")
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText("狀態: 清除文件出錯")
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"清除文件時出錯: {str(e)}", exc_info=True)
            
            QMessageBox.critical(self, "錯誤", f"清除文件時出錯:\n{str(e)}")
    
    def export_upload_form(self):
        """匯出上傳表單"""
        try:
            # 打開SPX Upload Form對話框
            sub_widget = SPXUploadFormWidget(self)
            sub_widget.exec_()
            
            # 更新狀態
            self.status_label.setText("狀態: 準備就緒")
            
        except Exception as e:
            # 更新狀態並顯示錯誤訊息
            self.status_label.setText("狀態: 打開對話框出錯")
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            
            # 通知父窗口進行日誌記錄
            if hasattr(self.parent, 'logger'):
                self.parent.logger.error(f"打開SPX Upload Form對話框時出錯: {str(e)}", exc_info=True)
            
            QMessageBox.critical(self, "錯誤", f"打開SPX Upload Form對話框時出錯:\n{str(e)}")


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
        print('應用正常退出')
    except Exception as e:
        print(f"應用出錯: {str(e)}")
        logging.error("應用出錯", exc_info=True)
