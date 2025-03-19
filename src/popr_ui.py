import sys
import os
import time
import re
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any, Union

import pandas as pd
import numpy as np
from PyQt5.QtCore import Qt, QObject, pyqtSignal, pyqtSlot
from PyQt5.QtWidgets import (
    QApplication, QWidget, QMainWindow, QVBoxLayout, QHBoxLayout, 
    QLabel, QPushButton, QComboBox, QListWidget, QFileDialog, 
    QMessageBox, QDialog, QLineEdit, QGroupBox, QTextEdit, QSplitter,
    QTabWidget
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
from utils import Logger, ReconEntryAmt


# 自定義日誌處理器，用於捕獲日誌並將其顯示在UI上
class QTextEditLogger(logging.Handler):
    def __init__(self, widget):
        super().__init__()
        self.widget = widget
        self.widget.setReadOnly(True)
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
    def emit(self, record):
        msg = self.format(record)
        self.widget.append(msg)
        # 自動滾動到底部
        self.widget.moveCursor(QTextCursor.End)


class Main(QWidget):
    """主界面類"""
    
    def __init__(self):
        """初始化主界面"""
        super().__init__()
        self.had_error = False  # 標記是否有錯誤發生
        self.logger = None  # 將在setupLogger中初始化
        
        # 初始化UI
        self.setWindowTitle("POPR BOT")
        self.setGeometry(450, 150, 600, 400)  # 擴大窗口尺寸
        self.UI()
        self.setupLogger()
        self.show()
    
    def setupLogger(self):
        """設置日誌系統"""
        self.logger = Logger().get_logger(__name__)
        
        # 創建自定義處理器，將日誌輸出到UI
        log_handler = QTextEditLogger(self.logTextEdit)
        log_handler.setLevel(logging.INFO)
        
        # 獲取根記錄器，添加我們的處理器
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)
        
        self.logger.info("POPR BOT 啟動")
    
    def UI(self):
        """設置UI組件"""
        self.mainDesign()
        self.layouts()
    
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
        entities = ['MOBA_PR', 'MOBA_PO', 'SPT_PR', 'SPT_PO', 'SPX_PR', 'SPX_PO']
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
            # TODO: 新增SPX PO PR模組
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
