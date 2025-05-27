import sys
import os
import re
import logging
# import traceback # Not used
from datetime import datetime

import pandas as pd
from PyQt5.QtCore import Qt, QObject, pyqtSignal, pyqtSlot
from PyQt5.QtWidgets import (
    QApplication, QWidget, # QMainWindow, # Not used
    QVBoxLayout, QHBoxLayout, 
    QLabel, QPushButton, QComboBox, QListWidget, QFileDialog, 
    QMessageBox, QDialog, QLineEdit, QGroupBox, QTextEdit, # QSplitter, # Not used
    QTabWidget, QGridLayout
)
from PyQt5.QtGui import QFont, QTextCursor # QPixmap not used
from qt_material import apply_stylesheet

from app_controller import AppController
# utils.Logger is not directly used in Main anymore, logging is handled by root logger + LogHandler
# from utils import Logger 

class LogHandler(QObject, logging.Handler):
    new_log_signal = pyqtSignal(str)
    def __init__(self):
        QObject.__init__(self)
        logging.Handler.__init__(self)
        # Ensure formatter is applied to the handler itself
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    def emit(self, record):
        try:
            msg = self.format(record)
            self.new_log_signal.emit(msg)
        except Exception as e:
            # Fallback to console print if UI logging fails
            print(f"LogHandler emit error: {e}\nOriginal log message: {record.getMessage()}")


class Main(QWidget):
    def __init__(self):
        super().__init__()
        # self.had_error = False # This can be managed by AppController or through status levels
        self.app_controller = AppController(ui_callback=self.handle_app_controller_callback)
        self.setWindowTitle("POPR BOT (Controller UI Feedback V1)") # Updated title
        self.setGeometry(450, 150, 600, 400) # Default size for main tab
        
        self.createBasicComponents()
        self.setupLogger() 
        self.createCompleteUI()
        self.setupTabEvents()
        self.show()

    def handle_app_controller_callback(self, type: str, message: str, level: str = "info", module: str = "main", files: Optional[List[str]] = None, **details):
        """處理來自 AppController 的回調"""
        if type == "status":
            is_error = (level == "error")
            target_status_label = self.statusLabel # Default to main status label
            if module == "spx" and hasattr(self, 'spxTab') and hasattr(self.spxTab, 'status_label'):
                target_status_label = self.spxTab.status_label
            
            target_status_label.setText(f"狀態: {message}")
            if is_error:
                target_status_label.setStyleSheet("font-size:10pt; color: red;")
            else:
                target_status_label.setStyleSheet("font-size:10pt; color: black;")
            QApplication.processEvents() # Ensure status update is immediate

            # 處理彈窗請求
            if details.get("show_success_popup"):
                QMessageBox.information(self, details.get("title", "完成"), details.get("message", message))
            elif details.get("show_error_popup"):
                QMessageBox.critical(self, details.get("title", "錯誤"), details.get("message", message))
            elif details.get("show_warning_popup"):
                 QMessageBox.warning(self, details.get("title", "警告"), details.get("message", message))
        
        elif type == "log":
            # Log messages are now primarily handled by the LogHandler connected to the root logger.
            # AppController's _log_message uses standard logging, which LogHandler picks up.
            # So, direct appending here might be redundant if LogHandler is working correctly.
            # If specific formatting for UI log is needed different from standard log, this could be used.
            # For now, relying on LogHandler.
            pass 
        
        elif type == "file_list_update":
            target_list_widget = None
            if module == "main":
                target_list_widget = self.importedList
            elif module == "spx" and hasattr(self, 'spxTab'):
                target_list_widget = self.spxTab.file_list
            
            if target_list_widget is not None and files is not None:
                target_list_widget.clear()
                for file_display_text in files:
                    target_list_widget.addItem(file_display_text)
                self.logger.info(f"UI File list for module '{module}' updated with {len(files)} items.")


    def createBasicComponents(self): 
        self.tabWidget = QTabWidget()
        self.mainTab = QWidget()
        self.logTab = QWidget()
        self.logTextEdit = QTextEdit()
        self.logTextEdit.setReadOnly(True)
        self.logTabLayout = QVBoxLayout(self.logTab)
        self.logTabLayout.addWidget(self.logTextEdit)

    def createCompleteUI(self): 
        self.mainDesign()
        self.layouts()
        self.add_spx_tab_to_main_ui()

    def setupLogger(self): 
        # self.logger = Logger().get_logger(__name__) # UI specific logger, AppController uses its own or root
        self.logger = logging.getLogger(self.__class__.__name__) # Use standard logging for UI actions too
        self.logger.setLevel(logging.INFO) # Set level for this specific logger instance

        if not hasattr(self, 'logTextEdit'): 
            self.logTextEdit = QTextEdit() 
            self.logTextEdit.setReadOnly(True)
        
        self.log_handler_qt = LogHandler() # Renamed to avoid confusion if there was another self.log_handler
        self.log_handler_qt.setLevel(logging.INFO)
        self.log_handler_qt.new_log_signal.connect(self.append_log_from_signal)
        
        root_logger = logging.getLogger() 
        root_logger.setLevel(logging.INFO) # Ensure root logger level is also appropriate
        if not any(isinstance(h, LogHandler) for h in root_logger.handlers):
            root_logger.addHandler(self.log_handler_qt)
        
        self.logger.info("POPR BOT UI Logger initialized.")

    @pyqtSlot(str)
    def append_log_from_signal(self, message): 
        if hasattr(self, 'logTextEdit') and self.logTextEdit:
            try:
                self.logTextEdit.append(message)
                self.logTextEdit.moveCursor(QTextCursor.End)
                # QApplication.processEvents() # Process events less aggressively
            except Exception as e:
                print(f"日誌顯示錯誤: {e}")
    
    def mainDesign(self): 
        self.checkList = QLabel('已導入檔案 (主模塊)') 
        self.checkList.setStyleSheet("font-size:12pt;background-color:#93FF93;color:#000080;font:Bold")
        self.checkList.setAlignment(Qt.AlignCenter)
        self.importedList = QListWidget()
        self.entitySelect = QComboBox(self)
        entities = ['MOBA_PR', 'MOBA_PO', 'SPT_PR', 'SPT_PO']; 
        for name in entities: self.entitySelect.addItem(name)
        
        self.btnImport_raw = QPushButton('原始資料 (POPR)', self); self.btnImport_raw.clicked.connect(self.import_raw_data)
        self.btnImport_closinglist = QPushButton('關單清單 (Closing List)', self); self.btnImport_closinglist.clicked.connect(self.import_closing_list_data)
        self.btnImport_previouswp = QPushButton('前期底稿 (Previous WP)', self); self.btnImport_previouswp.clicked.connect(self.import_previous_wp_data)
        self.btnImpot_procuremant = QPushButton('採購底稿 (Procurement WP)', self); self.btnImpot_procuremant.clicked.connect(self.import_procurement_wp_data)
        
        self.btnProcess = QPushButton('開始處理 (主流程)', self); self.btnProcess.clicked.connect(self.process_data_via_controller)
        self.btnDelete = QPushButton('清除所有已導入文件', self); self.btnDelete.clicked.connect(self.clear_all_imported_files_ui)
        
        self.btnHRIS = QPushButton('HRIS重複檢查', self); self.btnHRIS.clicked.connect(self.trigger_hris_check) 
        self.btnUploadForm = QPushButton('Upload Form', self); self.btnUploadForm.clicked.connect(self.open_upload_form_dialog)
        self.btncheck2 = QPushButton('兩期檢查', self); self.btncheck2.clicked.connect(self.trigger_two_period_check) 

        self.statusLabel = QLabel('狀態: 準備就緒'); self.statusLabel.setStyleSheet("font-size:10pt;")
    
    def layouts(self): 
        self.mainLayout = QVBoxLayout()
        self.mainTabLayout = QHBoxLayout(self.mainTab)
        self.leftLayout = QVBoxLayout(); self.rightLayout = QVBoxLayout()
        self.mainTabLayout.addLayout(self.leftLayout, 60); self.mainTabLayout.addLayout(self.rightLayout, 40)
        self.leftLayout.addWidget(self.checkList); self.leftLayout.addWidget(self.importedList)
        self.rightLayout.addWidget(self.entitySelect); self.rightLayout.addWidget(self.btnImport_raw)
        self.rightLayout.addWidget(self.btnImport_closinglist); self.rightLayout.addWidget(self.btnImport_previouswp)
        self.rightLayout.addWidget(self.btnImpot_procuremant); self.rightLayout.addWidget(self.btnProcess)
        self.rightLayout.addWidget(self.btnDelete); self.rightLayout.addWidget(self.btnHRIS)
        self.rightLayout.addWidget(self.btnUploadForm); self.rightLayout.addWidget(self.btncheck2)
        self.rightLayout.addStretch(); self.rightLayout.addWidget(self.statusLabel)
        # self.logTabLayout is initialized in createBasicComponents and logTextEdit added there
        self.tabWidget.addTab(self.mainTab, "主功能"); self.tabWidget.addTab(self.logTab, "日誌")
        self.mainLayout.addWidget(self.tabWidget); self.setLayout(self.mainLayout)

    def setupTabEvents(self): 
        self.tabWidget.currentChanged.connect(self.onTabChanged)
        # Capture initial geometry for mainTabSize after window is shown or at least fully constructed
        # self.mainTabSize = (self.geometry().x(), self.geometry().y(), self.width(), self.height())
        # A more robust way if called before show():
        self.mainTabSize = (100, 100, 600, 400) # Default, can be refined
        self.spxTabSize = (self.mainTabSize[0], self.mainTabSize[1], 800, 600) 

    def onTabChanged(self, index): 
        try:
            tab_name = self.tabWidget.tabText(index)
            self.logger.info(f"切換到標籤頁: {tab_name}") # Use self.logger
            center = self.frameGeometry().center()
            if tab_name == "主功能": self.setGeometry(self.mainTabSize[0], self.mainTabSize[1], self.mainTabSize[2], self.mainTabSize[3])
            elif tab_name == "SPX模組": self.setGeometry(self.spxTabSize[0], self.spxTabSize[1], self.spxTabSize[2], self.spxTabSize[3])
            elif tab_name == "日誌": self.setGeometry(self.mainTabSize[0], self.mainTabSize[1], self.mainTabSize[2], self.mainTabSize[3])
            new_rect = self.frameGeometry(); new_rect.moveCenter(center); self.move(new_rect.topLeft())
        except Exception as e: logging.error(f"切換標籤頁時出錯: {str(e)}", exc_info=True)

    def updateStatus(self, message, error=False):
        # This method is now primarily called by handle_app_controller_callback for the main status label
        if error: self.statusLabel.setStyleSheet("font-size:10pt; color: red;")
        else: self.statusLabel.setStyleSheet("font-size:10pt; color: black;")
        self.statusLabel.setText(f"狀態: {message}"); # QApplication.processEvents() # Less aggressive processing

    def _get_file_path_from_dialog(self, title: str, file_filter: str) -> Optional[str]: 
        url, _ = QFileDialog.getOpenFileName(self, title, "", file_filter)
        if not url: 
            self.logger.info(f"操作取消: 未選擇文件 ({title})") # Use self.logger
            self.updateStatus("準備就緒") # Update main status
            return None
        return url

    def _register_and_update_ui_list(self, ui_file_type_display: str, internal_file_key: str, file_path: str, module: str = "main"):
        """輔助方法：調用controller註冊文件。UI列表更新將通過回調處理。"""
        if module == "main":
            self.app_controller.register_imported_file(ui_file_type_display, internal_file_key, file_path)
        elif module == "spx":
            self.app_controller.spx_register_file(ui_file_type_display, internal_file_key, file_path)


    def import_raw_data(self): 
        file_path = self._get_file_path_from_dialog('原始數據 (POPR)', "Files(*.csv *.xlsx);;CSV(*.csv);;EXCEL(*.xlsx)")
        if file_path:
            file_name = os.path.basename(file_path)
            if re.match(r'[0-9]{6}', str(file_name[0:6])) is None:
                self.logger.warning("文件名格式錯誤，需要包含年月 (YYYYMM)")
                self.updateStatus("錯誤: 文件名格式不正確 (YYYYMM)", error=True) # Update main status
                QMessageBox.warning(self, "警告", "文件名需要以YYYYMM開頭 (例如 202312_raw_data.csv)"); return
            self._register_and_update_ui_list("原始資料 (POPR)", "raw_data", file_path, module="main")

    def import_closing_list_data(self): 
        file_path = self._get_file_path_from_dialog('關單清單 (Closing List)', "Files(*.csv *.xlsx);;CSV(*.csv);;EXCEL(*.xlsx)")
        if file_path: self._register_and_update_ui_list("關單清單", "closing_list", file_path, module="main")

    def import_previous_wp_data(self): 
        file_path = self._get_file_path_from_dialog('前期底稿 (Previous WP)', "EXCEL(*.xlsx)")
        if file_path: self._register_and_update_ui_list("前期底稿", "previous_wp", file_path, module="main")

    def import_procurement_wp_data(self): 
        file_path = self._get_file_path_from_dialog('採購底稿 (Procurement WP)', "EXCEL(*.xlsx)")
        if file_path: self._register_and_update_ui_list("採購底稿", "procurement_wp", file_path, module="main")
            
    def clear_all_imported_files_ui(self): 
        confirm = QMessageBox.question(self, "確認", "確定要清除所有已導入的文件記錄嗎？\n(主模塊和SPX模塊的文件記錄都將被清除)", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if confirm == QMessageBox.Yes:
            # UI lists will be cleared via callback from AppController
            self.app_controller.clear_imported_files() 
            self.app_controller.spx_clear_all_files() 
            self.logger.info("UI層請求清除所有模塊的文件。")

    def process_data_via_controller(self): 
        module_type = self.entitySelect.currentText()
        # DAL will be checked by AppController now
        params = {"entity_type": module_type}
        try: self.app_controller.start_main_processing(params)
        except Exception as e:
            self.logger.critical(f"調用 AppController (主流程) 時發生未預期錯誤: {str(e)}", exc_info=True) # Use self.logger
            self.updateStatus(f"嚴重錯誤: {str(e)}", error=True)

    def trigger_hris_check(self):
        # self.updateStatus("準備HRIS重複檢查...", "info") # AppController will send this
        urls, _ = QFileDialog.getOpenFileUrls(self, '選擇PR, PO, AP發票文件 (按此順序選擇或確保文件名包含PR/PO/AP)', "", "Excel Files (*.xlsx *.xls)")
        if not urls or len(urls) < 3: 
            QMessageBox.warning(self, "警告", "請至少選擇三個文件 (PR, PO, AP)。建議文件名包含PR/PO/AP以更好識別。")
            # self.updateStatus("HRIS檢查已取消。", "info") # AppController will send status if needed
            return
        pr_path, po_path, ap_path = None, None, None; temp_files = list(urls)
        for i in range(len(temp_files)):
            url_str = temp_files[i].toLocalFile()
            if not pr_path and "PR" in os.path.basename(url_str).upper(): pr_path = url_str; continue
            if not po_path and "PO" in os.path.basename(url_str).upper(): po_path = url_str; continue
            if not ap_path and "AP" in os.path.basename(url_str).upper(): ap_path = url_str; continue
        if not (pr_path and po_path and ap_path):
            if len(urls) >= 3:
                 self.logger.warning("無法通過文件名識別所有HRIS文件類型，將按選擇順序假定：1.PR, 2.PO, 3.AP")
                 pr_path = urls[0].toLocalFile() if not pr_path and len(urls)>0 else pr_path
                 po_path = urls[1].toLocalFile() if not po_path and len(urls)>1 else po_path
                 ap_path = urls[2].toLocalFile() if not ap_path and len(urls)>2 else ap_path
            else: 
                QMessageBox.critical(self, "錯誤", "無法識別所有必要的文件類型 (PR, PO, AP)。"); return
        if not all([pr_path, po_path, ap_path]):
            QMessageBox.critical(self, "錯誤", "未能成功匹配所有HRIS所需文件 (PR, PO, AP)。"); return
        self.app_controller.start_hris_check(pr_path, po_path, ap_path)

    def open_upload_form_dialog(self):
        # self.updateStatus("打開Upload Form對話框...", "info") # AppController will handle status
        dialog = UploadFormWidget(self) 
        dialog.generation_requested.connect(self.trigger_upload_form_generation_from_dialog)
        dialog.exec_() 

    @pyqtSlot(dict) 
    def trigger_upload_form_generation_from_dialog(self, params: dict):
        self.logger.info(f"從UploadFormWidget接收到生成請求: {params}") # Use self.logger
        self.app_controller.generate_upload_form(params)

    def trigger_two_period_check(self):
        # self.updateStatus("準備兩期檢查...", "info") # AppController will handle status
        url_pr_path = self._get_file_path_from_dialog('選擇採購PR文件 (當期)', "EXCEL(*.xlsx)")
        if not url_pr_path: return
        url_po_path = self._get_file_path_from_dialog('選擇採購PO文件 (當期)', "EXCEL(*.xlsx)")
        if not url_po_path: return
        url_ac_path = self._get_file_path_from_dialog('選擇前期底稿 (Accrual Schedule)', "EXCEL(*.xlsm)")
        if not url_ac_path: return
        self.app_controller.start_two_period_check(url_pr_path, url_po_path, url_ac_path)

    def add_spx_tab_to_main_ui(self): 
        self.spxTab = SPXTabWidget(self) 
        self.tabWidget.addTab(self.spxTab, "SPX模組")
        self.logger.info("SPX模組Tab已初始化") # Use self.logger


class UploadFormWidget(QDialog):
    generation_requested = pyqtSignal(dict)
    def __init__(self, parent=None): 
        super().__init__(parent)
        self.logger = logging.getLogger(self.__class__.__name__) # Use standard logging
        self.setWindowTitle("Upload Form")
        self.setupUI()
        self.logger.info("Upload Form對話框已打開")
            
    def setupUI(self): 
        self.mainLayout = QHBoxLayout(); self.leftLayout = QVBoxLayout(); self.rightLayout = QVBoxLayout()
        self.mainLayout.addLayout(self.leftLayout, 60); self.mainLayout.addLayout(self.rightLayout, 40)
        self.label_entity = QLabel("Entity"); self.combo_entity = QComboBox(self); entities = ['MOBTW', 'SPTTW']; 
        for name in entities: self.combo_entity.addItem(name)
        self.label_period = QLabel("Period"); self.line_period = QLineEdit(); self.line_period.setPlaceholderText("JAN-24")
        self.label_ac_date = QLabel("Accounting Date"); self.line_ac_date = QLineEdit(); self.line_ac_date.setPlaceholderText("2024/01/31")
        self.label_cate = QLabel("Category"); self.line_cate = QLineEdit(); self.line_cate.setPlaceholderText("01 SEA Accrual Expense"); self.line_cate.setText('01 SEA Accrual Expense')
        self.label_accountant = QLabel("Accountant"); self.line_accountant = QLineEdit(); self.line_accountant.setPlaceholderText("Lynn"); self.line_accountant.setText('Lynn')
        self.label_currency = QLabel("Currency"); self.combo_currency = QComboBox(self); currencies = ['TWD', 'USD', 'HKD']; 
        for c in currencies: self.combo_currency.addItem(c)
        self.label_wp = QLabel("Working Paper"); self.button_get_wp = QPushButton("Select Working Paper"); self.button_get_wp.clicked.connect(self.get_wp)
        self.label_start = QLabel("Process"); self.button_do_upload = QPushButton("Generate Upload Form"); self.button_do_upload.clicked.connect(self.request_form_generation)
        self.statusLabel = QLabel("狀態: 準備就緒") 
        self.leftLayout.addWidget(self.label_entity); self.leftLayout.addWidget(self.label_period); self.leftLayout.addWidget(self.label_ac_date); self.leftLayout.addWidget(self.label_cate); self.leftLayout.addWidget(self.label_accountant); self.leftLayout.addWidget(self.label_currency); self.leftLayout.addWidget(self.label_wp); self.leftLayout.addWidget(self.label_start)
        self.rightLayout.addWidget(self.combo_entity); self.rightLayout.addWidget(self.line_period); self.rightLayout.addWidget(self.line_ac_date); self.rightLayout.addWidget(self.line_cate); self.rightLayout.addWidget(self.line_accountant); self.rightLayout.addWidget(self.combo_currency); self.rightLayout.addWidget(self.button_get_wp); self.rightLayout.addWidget(self.button_do_upload)
        statusLayout = QHBoxLayout(); statusLayout.addWidget(self.statusLabel)
        mainContainer = QVBoxLayout(); mainContainer.addLayout(self.mainLayout); mainContainer.addStretch(); mainContainer.addLayout(statusLayout)
        self.setLayout(mainContainer)

    def get_wp(self): 
        try:
            self.statusLabel.setText("狀態: 選擇工作底稿...")
            url, _ = QFileDialog.getOpenFileName(self, '工作底稿', "", "Files(*.xlsm *.xlsx);;EXCEL(*.xlsx *.xlsm)")
            if not url: self.logger.info("未選擇工作底稿文件，取消操作"); self.statusLabel.setText("狀態: 準備就緒"); return
            self.fileUrl = url; self.logger.info(f"已選擇工作底稿: {os.path.basename(self.fileUrl)}")
            self.button_get_wp.setText(f"Selected: {os.path.basename(self.fileUrl)}"); self.statusLabel.setText(f"狀態: 已選擇 {os.path.basename(self.fileUrl)}")
        except Exception as err:
            self.logger.error(f"選擇工作底稿時出錯: {str(err)}", exc_info=True); self.statusLabel.setText("狀態: 錯誤 - 選擇工作底稿時出錯"); 
            QMessageBox.critical(self, "錯誤", f"選擇工作底稿時出錯:\n{str(err)}")
    
    def request_form_generation(self): 
        try:
            self.logger.info("請求生成Upload Form...")
            self.statusLabel.setText("狀態: 驗證參數...")
            if not hasattr(self, 'fileUrl') or not self.fileUrl:
                self.logger.warning("未選擇工作底稿"); self.statusLabel.setText("狀態: 錯誤 - 未選擇工作底稿"); 
                QMessageBox.warning(self, "警告", "請選擇工作底稿"); return
            params = {"wp_file_path": self.fileUrl, "entity": self.combo_entity.currentText(), "period": self.line_period.text(), "ac_date": self.line_ac_date.text(), "category": self.line_cate.text(), "accountant": self.line_accountant.text(), "currency": self.combo_currency.currentText()}
            if not params["period"]: self.logger.warning("未輸入期間"); self.statusLabel.setText("狀態: 錯誤 - 未輸入期間"); QMessageBox.warning(self, "警告", "請輸入期間 (例如: JAN-24)"); return
            if not params["ac_date"]: self.logger.warning("未輸入會計日期"); self.statusLabel.setText("狀態: 錯誤 - 未輸入會計日期"); QMessageBox.warning(self, "警告", "請輸入會計日期 (例如: 2024/01/31)"); return
            try: datetime.strptime(params["ac_date"], '%Y/%m/%d')
            except ValueError: self.logger.warning("會計日期格式錯誤"); self.statusLabel.setText("狀態: 錯誤 - 會計日期格式錯誤"); QMessageBox.warning(self, "警告", "會計日期格式錯誤，應為 YYYY/MM/DD"); return
            self.statusLabel.setText("狀態: 參數驗證通過，請求主程序處理...")
            self.generation_requested.emit(params); self.accept()
        except Exception as err:
            self.logger.error(f"請求生成Upload Form時出錯: {str(err)}", exc_info=True)
            self.statusLabel.setText("狀態: 錯誤 - 請求生成時出錯"); QMessageBox.critical(self, "錯誤", f"請求生成Upload Form時出錯:\n{str(err)}")


class SPXTabWidget(QWidget): 
    def __init__(self, parent_main_window=None): 
        super(SPXTabWidget, self).__init__(parent_main_window)
        self.parent_main = parent_main_window 
        # self.file_paths_ui_cache = {} # No longer needed, AppController is source of truth via DAL
        self.file_types = [("po_file", "原始PO數據"), ("previous_wp", "前期底稿(PO)"), ("procurement", "採購底稿(PO)"), ("ap_invoice", "AP發票文件"), ("previous_wp_pr", "前期PR底稿"), ("procurement_pr", "採購PR底稿")]; 
        self.setupUI()
    
    def setupUI(self):
        main_layout = QHBoxLayout(self); left_layout = QVBoxLayout(); right_layout = QVBoxLayout()
        main_layout.addLayout(left_layout, 60); main_layout.addLayout(right_layout, 40)
        upload_group = QGroupBox("文件上傳"); grid_layout = QGridLayout(); self.buttons = {}; self.labels = {}
        for row, (file_key, file_label) in enumerate(self.file_types):
            label = QLabel(f"{file_label}:"); grid_layout.addWidget(label, row, 0)
            file_name_label = QLabel("未選擇文件"); file_name_label.setStyleSheet("color: gray;"); grid_layout.addWidget(file_name_label, row, 1); self.labels[file_key] = file_name_label
            upload_btn = QPushButton("選擇文件"); upload_btn.clicked.connect(lambda checked, k=file_key, label=file_label: self.select_file(k, label)); grid_layout.addWidget(upload_btn, row, 2); self.buttons[file_key] = upload_btn
        upload_group.setLayout(grid_layout)
        process_group = QGroupBox("處理參數"); process_layout = QGridLayout()
        process_layout.addWidget(QLabel("財務年月 (YYYYMM):"), 0, 0); self.period_input = QLineEdit(); self.period_input.setPlaceholderText("例如: 202504"); process_layout.addWidget(self.period_input, 0, 1)
        process_layout.addWidget(QLabel("處理人員:"), 1, 0); self.user_input = QLineEdit(); self.user_input.setPlaceholderText("例如: Blaire"); process_layout.addWidget(self.user_input, 1, 1)
        process_group.setLayout(process_layout)
        self.file_list = QListWidget(); file_list_label = QLabel("已上傳文件 (SPX)"); file_list_label.setStyleSheet("font-size:12pt;background-color:#ADD8E6;color:#000080;font:Bold"); file_list_label.setAlignment(Qt.AlignCenter) 
        left_layout.addWidget(upload_group); left_layout.addWidget(process_group); left_layout.addWidget(file_list_label); left_layout.addWidget(self.file_list)
        self.process_btn = QPushButton("處理SPX並產生結果"); self.process_btn.clicked.connect(self.process_spx_files_via_controller); self.process_btn.setStyleSheet("font-size:12pt;font:Bold;padding:10px;")
        self.clear_btn = QPushButton("清除所有SPX文件"); self.clear_btn.clicked.connect(self.clear_all_spx_files_ui_and_controller)
        self.export_btn = QPushButton("匯出SPX上傳表單"); self.export_btn.clicked.connect(self.export_spx_upload_form_via_controller)
        tips_label = QLabel("SPX模組說明:"); tips_label.setStyleSheet("font-size:11pt;font:Bold;")
        tips_content = QLabel("此模組用於處理SPX相關的PO/PR數據。\n\n使用步驟:\n1. 上傳各項必要文件\n2. 填寫處理參數 (財務年月, 處理人員)\n3. 點擊「處理SPX並產生結果」\n4. 或點擊「匯出SPX上傳表單」"); tips_content.setWordWrap(True); tips_content.setStyleSheet("color:#000080;padding:10px;border-radius:5px;")
        self.status_label = QLabel("狀態: 準備就緒"); self.status_label.setStyleSheet("font-size:10pt;") 
        right_layout.addWidget(self.process_btn); right_layout.addWidget(self.clear_btn); right_layout.addWidget(self.export_btn); right_layout.addSpacing(20); right_layout.addWidget(tips_label); right_layout.addWidget(tips_content); right_layout.addStretch(); right_layout.addWidget(self.status_label)

    def select_file(self, file_key, file_label):
        try:
            file_filter = "Excel Files (*.xlsx *.xlsm)" if file_key == "ap_invoice" else "Files (*.csv *.xlsx);;CSV (*.csv);;Excel (*.xlsx *.xlsm)"
            file_path, _ = QFileDialog.getOpenFileName(self, f'選擇SPX {file_label}', "", file_filter)
            if not file_path: return

            if self.parent_main and self.parent_main.app_controller:
                # Pass the display label (file_label) and internal key (file_key)
                if self.parent_main.app_controller.spx_register_file(file_label, file_key, file_path):
                    # UI update for self.labels[file_key] and self.file_list will now be handled by callback
                    # self.labels[file_key].setText(os.path.basename(file_path))
                    # self.labels[file_key].setStyleSheet("color: blue;")
                    # list_item_text = f"{file_label}: {os.path.basename(file_path)}"
                    # ... (logic to update/add to self.file_list was here) ...
                    if file_key == "po_file" and not self.period_input.text():
                        try: 
                            year_month = os.path.basename(file_path)[:6]
                            if year_month.isdigit() and len(year_month) == 6: self.period_input.setText(year_month)
                        except Exception: pass
            else:
                QMessageBox.critical(self, "錯誤", "無法訪問 AppController 實例。")
                logging.error("SPXTab: AppController not accessible via parent_main.")
        except Exception as e:
            self.status_label.setText("狀態: 選擇文件出錯"); self.status_label.setStyleSheet("font-size:10pt; color: red;")
            logging.error(f"SPXTab: 選擇文件時出錯: {str(e)}", exc_info=True)
            QMessageBox.critical(self, "錯誤", f"選擇SPX文件時出錯:\n{str(e)}")

    def clear_all_spx_files_ui_and_controller(self):
        confirm = QMessageBox.question(self, "確認", "確定要清除所有SPX模塊已導入的文件記錄嗎？", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if confirm == QMessageBox.Yes:
            # self.file_list.clear() # UI list will be cleared by callback
            # for label in self.labels.values(): label.setText("未選擇文件"); label.setStyleSheet("color: gray;")
            # self.period_input.clear(); self.user_input.clear()
            if self.parent_main and self.parent_main.app_controller:
                self.parent_main.app_controller.spx_clear_all_files() 
            else:
                QMessageBox.critical(self, "錯誤", "無法訪問 AppController 實例。")
                logging.error("SPXTab: AppController not accessible for clearing SPX files.")
    
    def process_spx_files_via_controller(self):
        if not (self.parent_main and self.parent_main.app_controller):
            QMessageBox.critical(self, "錯誤", "無法訪問 AppController 實例。"); return
        period = self.period_input.text(); user = self.user_input.text()
        if not period or not period.isdigit() or len(period) != 6:
            QMessageBox.warning(self, "警告", "請輸入有效的財務年月 (格式: YYYYMM)"); return
        if not user:
            QMessageBox.warning(self, "警告", "請輸入處理人員名稱"); return
        params = {"period": period, "user": user} # File paths will be retrieved by AppController from DAL
        self.parent_main.app_controller.spx_start_processing(params)

    def export_spx_upload_form_via_controller(self):
        if not (self.parent_main and self.parent_main.app_controller):
            QMessageBox.critical(self, "錯誤", "無法訪問 AppController 實例。"); return
        period = self.period_input.text(); user = self.user_input.text()
        if not period or not period.isdigit() or len(period) != 6:
            QMessageBox.warning(self, "警告", "請輸入有效的財務年月 (格式: YYYYMM)"); return
        if not user:
            QMessageBox.warning(self, "警告", "請輸入處理人員名稱"); return
        year = period[:4]; month = period[4:6]
        params = {
            # "po_file_path" will be retrieved by AppController from DAL using "po_file" key for "spx" entity
            "entity": "SPXTW", 
            "period_display": f"{datetime(int(year), int(month), 1).strftime('%b').upper()}-{year[2:]}",
            "period_str": f"{year}-{month}", 
            "accounting_date": f"{year}/{month}/25", 
            "category": "01 SEA Accrual Expense", 
            "user": user
        }
        self.parent_main.app_controller.spx_start_export_upload_form(params)

def main(): 
    try:
        APP = QApplication(sys.argv)
        window = Main()
        apply_stylesheet(APP, theme='dark_lightgreen.xml')
        sys.exit(APP.exec_())
    except Exception as e:
        logging.critical(f"啟動應用時發生致命錯誤: {str(e)}", exc_info=True) # Use critical for startup errors
        print(f"啟動應用時發生致命錯誤: {str(e)}")

if __name__ == '__main__': 
    try: main()
    except SystemExit:
        logging.info('應用正常退出') # Log normal exit
        print('應用正常退出')
    except Exception as e:
        print(f"應用主程序出錯: {str(e)}")
        logging.critical("應用主程序未捕獲的異常", exc_info=True)
```
