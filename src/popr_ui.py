# src/popr_ui.py
import sys
import os
import re
import logging
import traceback
from datetime import datetime

import pandas as pd
from PyQt5.QtCore import Qt, QObject, pyqtSignal, pyqtSlot, QThread 
from PyQt5.QtWidgets import (
    QApplication, QWidget, QMainWindow, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QComboBox, QListWidget, QFileDialog,
    QMessageBox, QDialog, QLineEdit, QGroupBox, QTextEdit, QSplitter,
    QTabWidget, QGridLayout
)
from PyQt5.QtGui import QFontDatabase, QPixmap, QFont, QTextCursor
# apply_stylesheet line is currently commented out due to previous launch issues.
# from qt_material import apply_stylesheet

from .ui_strings import STRINGS
from .processing_worker import ProcessingWorker 
from .spttwpo import SPTTW_PO
from .spttwpr import SPTTW_PR
from .mobtwpr import MOBTW_PR
from .mobtwpo import MOBTW_PO
from .hris_dup import HRISDuplicateChecker
from .upload_form import get_aggregation_twd, get_aggregation_foreign, get_entries
from .utils import Logger, ReconEntryAmt


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
            self.widget.moveCursor(QTextCursor.End)
            QApplication.processEvents()
        except Exception as e:
            print(f"日誌輸出錯誤: {e}")

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
    def __init__(self):
        super().__init__()
        self.had_error = False
        self.setWindowTitle(STRINGS["WINDOW_TITLE_MAIN"])
        self.setGeometry(450, 150, 600, 400)
        self.createBasicComponents()
        self.setupLogger()
        self.createCompleteUI()
        self.setupTabEvents()

        self.thread = None 
        self.worker = None 
        self.show()

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
        self.logger = Logger().get_logger(__name__)
        if not hasattr(self, 'logTextEdit'):
            self.logTextEdit = QTextEdit()
            self.logTextEdit.setReadOnly(True)
        self.log_handler = LogHandler()
        self.log_handler.setLevel(logging.INFO)
        self.log_handler.new_log_signal.connect(self.append_log)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, (QTextEditLogger, LogHandler)):
                root_logger.removeHandler(handler)
        root_logger.addHandler(self.log_handler)
        self.logger.info(STRINGS.get("LOG_POPR_BOT_STARTED", "POPR BOT 啟動 (Fallback)"))
        self.logger.info(STRINGS.get("LOG_LOGGER_INIT_COMPLETE", "日誌系統初始化完成 (Fallback)"))

    @pyqtSlot(str)
    def append_log(self, message):
        if hasattr(self, 'logTextEdit') and self.logTextEdit:
            try:
                self.logTextEdit.append(message)
                self.logTextEdit.moveCursor(QTextCursor.End)
                QApplication.processEvents()
            except Exception as e:
                print(f"日誌顯示錯誤: {e}")

    def mainDesign(self):
        self.tabWidget = QTabWidget()
        self.mainTab = QWidget()
        self.logTab = QWidget()
        self.logTextEdit = QTextEdit()
        self.checkList = QLabel(STRINGS["MAIN_LBL_IMPORTED_FILES_TITLE"])
        self.checkList.setStyleSheet("font-size:12pt;background-color:#93FF93;color:#000080;font:Bold")
        self.checkList.setAlignment(Qt.AlignCenter)
        self.importedList = QListWidget()
        self.entitySelect = QComboBox(self)
        entities = [STRINGS["COMBO_ENTITY_MOBAPR"], STRINGS["COMBO_ENTITY_MOBAPO"], STRINGS["COMBO_ENTITY_SPTPR"], STRINGS["COMBO_ENTITY_SPTPO"]]
        for name in entities:
            self.entitySelect.addItem(name)
        self.btnImport_raw = QPushButton(STRINGS["MAIN_BTN_IMPORT_RAW"], self)
        self.btnImport_raw.clicked.connect(self.import_raw)
        self.btnImport_closinglist = QPushButton(STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"], self)
        self.btnImport_closinglist.clicked.connect(self.import_closing)
        self.btnImport_previouswp = QPushButton(STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"], self)
        self.btnImport_previouswp.clicked.connect(self.import_previousup)
        self.btnImpot_procuremant = QPushButton(STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"], self)
        self.btnImpot_procuremant.clicked.connect(self.import_procurement)
        self.btnProcess = QPushButton(STRINGS["MAIN_BTN_PROCESS"], self)
        self.btnProcess.clicked.connect(self.process)
        self.btnDelete = QPushButton(STRINGS["MAIN_BTN_DELETE_IMPORTED"], self)
        self.btnDelete.clicked.connect(self.deleteImportFile)
        self.btnHRIS = QPushButton(STRINGS["MAIN_BTN_HRIS_CHECK"], self)
        self.btnHRIS.clicked.connect(self.process_hris)
        self.btnUploadForm = QPushButton(STRINGS["MAIN_BTN_UPLOAD_FORM"], self)
        self.btnUploadForm.clicked.connect(self.uploadFormWidget)
        self.btncheck2 = QPushButton(STRINGS["MAIN_BTN_CHECK2"], self)
        self.btncheck2.clicked.connect(self.check2)
        self.statusLabel = QLabel(STRINGS["MAIN_LBL_STATUS_PREFIX"] + STRINGS["STATUS_READY"])
        self.statusLabel.setStyleSheet("font-size:10pt;")

    def layouts(self):
        self.mainLayout = QVBoxLayout()
        self.mainTabLayout = QHBoxLayout(self.mainTab)
        self.leftLayout = QVBoxLayout()
        self.rightLayout = QVBoxLayout()
        self.mainTabLayout.addLayout(self.leftLayout, 60)
        self.mainTabLayout.addLayout(self.rightLayout, 40)
        self.leftLayout.addWidget(self.checkList)
        self.leftLayout.addWidget(self.importedList)
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
        self.logTabLayout = QVBoxLayout(self.logTab)
        self.logTabLayout.addWidget(self.logTextEdit)
        self.tabWidget.addTab(self.mainTab, STRINGS["TAB_MAIN"])
        self.tabWidget.addTab(self.logTab, STRINGS["TAB_LOG"])
        self.mainLayout.addWidget(self.tabWidget)
        self.setLayout(self.mainLayout)

    def setupTabEvents(self):
        self.tabWidget.currentChanged.connect(self.onTabChanged)
        self.mainTabSize = (450, 150, 600, 400)
        self.spxTabSize = (450, 150, 800, 600)

    def onTabChanged(self, index):
        try:
            tab_name = self.tabWidget.tabText(index)
            self.logger.info(f"切換到標籤頁: {tab_name}")
            center = self.frameGeometry().center()
            if tab_name == STRINGS["TAB_MAIN"]: self.setGeometry(*self.mainTabSize)
            elif tab_name == STRINGS["TAB_SPX"]: self.setGeometry(*self.spxTabSize)
            elif tab_name == STRINGS["TAB_LOG"]: self.setGeometry(*self.mainTabSize)
            new_rect = self.frameGeometry()
            new_rect.moveCenter(center)
            self.move(new_rect.topLeft())
        except Exception as e:
            self.logger.error(f"切換標籤頁時出錯: {str(e)}", exc_info=True)

    def updateStatus(self, message, error=False):
        if error:
            self.statusLabel.setStyleSheet("font-size:10pt; color: red;")
            self.had_error = True
        else:
            self.statusLabel.setStyleSheet("font-size:10pt; color: black;")
        status_prefix = STRINGS.get("MAIN_LBL_STATUS_PREFIX", "狀態: ")
        self.statusLabel.setText(status_prefix + str(message))

    def checkHadError(self):
        if self.had_error:
            self.had_error = False
            return True
        return False

    def process_hris(self):
        try:
            self.had_error = False
            self.logger.info("開始HRIS重複檢查")
            self.updateStatus(STRINGS["STATUS_PROCESSING_HRIS"])
            urls, _ = QFileDialog.getOpenFileUrls(self, STRINGS["WINDOW_TITLE_HRIS_DIALOG"]) 
            if not urls: 
                self.logger.info("未選擇文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            file_urls = [url.toLocalFile() for url in urls] 
            
            df_pr_path = next((url for url in file_urls if 'PR' in os.path.basename(url)), None)
            df_po_path = next((url for url in file_urls if 'PO' in os.path.basename(url)), None)
            df_ap_path = next((url for url in file_urls if 'AP' in os.path.basename(url)), None)

            if not all([df_pr_path, df_po_path, df_ap_path]):
                self.logger.error("HRIS檢查缺少必要文件 (PR, PO, or AP).")
                self.updateStatus(STRINGS["STATUS_HRIS_ERROR_FILE_MISSING"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS.get("MSGBOX_ERR_HRIS_MISSING_FILES", "Missing one or more required files for HRIS check (PR, PO, AP)."))
                return

            try:
                df_pr = pd.read_excel(df_pr_path, dtype=str)
                df_po = pd.read_excel(df_po_path, dtype=str)
                df_ap = pd.read_excel(df_ap_path, dtype=str, header=1, sheet_name=1)
                self.logger.info(f"成功讀取文件 PR:{df_pr.shape}, PO:{df_po.shape}, AP:{df_ap.shape}")
            except Exception as err:
                self.logger.error(f"讀取文件失敗: {str(err)}", exc_info=True)
                self.updateStatus(STRINGS["STATUS_HRIS_ERROR_FILE_READ_FAILED"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_FILE_READ_FAILED_DETAILED"].format(str(err)))
                return
            try:
                checker = HRISDuplicateChecker()
                df_pr_p = checker.check_duplicates_in_po(df_pr, df_po)
                df_pr_p, df_po_p = checker.check_duplicates_in_ap(df_pr_p, df_po, df_ap)
                df_pr_p, df_po_p = checker.relocate_columns(df_pr_p, df_po_p)
                if self.checkHadError():
                    self.updateStatus(STRINGS["STATUS_PROCESSING_ERROR_STOP"], error=True)
                    return
                checker.save_files(df_pr_p, df_po_p)
                self.updateStatus(STRINGS["STATUS_HRIS_COMPLETE"])
                QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_HRIS_COMPLETE"])
                self.logger.info("HRIS重複檢查完成")
            except Exception as err:
                self.logger.error(f"HRIS重複檢查失敗: {str(err)}", exc_info=True)
                self.updateStatus(STRINGS["STATUS_HRIS_ERROR_PROCESSING_FAILED"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_HRIS_FAILED_DETAILED"].format(str(err)))
        except Exception as err:
            self.logger.error(f"HRIS處理過程中出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_HRIS_ERROR_DURING_PROCESSING"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_PROCESSING_FAILED_DETAILED"].format(str(err)))

    def import_raw(self):
        try:
            self.had_error = False
            self.updateStatus(STRINGS["STATUS_IMPORTING_RAW_DATA"])
            url, _ = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_RAW_DATA_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_CSV"])
            if not url:
                self.logger.info("未選擇原始數據文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            self.fileUrl = url
            self.file_name = os.path.basename(self.fileUrl)
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            if STRINGS["MAIN_BTN_IMPORT_RAW"] in items:
                self.logger.warning("原始資料已存在")
                self.updateStatus(STRINGS["STATUS_WARN_RAW_DATA_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_RAW_DATA_EXISTS"])
                return
            ym = self.file_name[0:6]
            if re.match(r'[0-9]{6}', str(ym)) is None:
                self.logger.warning("文件名格式錯誤，需要包含年月")
                self.updateStatus(STRINGS["STATUS_ERR_FILENAME_FORMAT_YYYYMM"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_FILENAME_FORMAT_YYYYMM"])
                return
            self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_RAW"])
            self.logger.info(f"成功導入原始資料: {self.file_name}")
            self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + self.file_name)
        except Exception as err:
            self.logger.error(f"導入原始數據時出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_RAW_DATA"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_RAW_DATA_DETAILED"].format(str(err)))

    def import_closing(self):
        try:
            self.had_error = False
            self.updateStatus(STRINGS["STATUS_IMPORTING_CLOSING_LIST"])
            url, _ = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_CLOSING_LIST_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_CSV"])
            if not url:
                self.logger.info("未選擇關單清單文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            self.fileUrl_c = url
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            if STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"] in items:
                self.logger.warning("關單清單已存在")
                self.updateStatus(STRINGS["STATUS_WARN_CLOSING_LIST_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_CLOSING_LIST_EXISTS"])
                return
            self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"])
            self.logger.info(f"成功導入關單清單: {os.path.basename(self.fileUrl_c)}")
            self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + os.path.basename(self.fileUrl_c))
        except Exception as err:
            self.logger.error(f"導入關單清單時出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_CLOSING_LIST"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_CLOSING_LIST_DETAILED"].format(str(err)))

    def import_previousup(self):
        try:
            self.had_error = False
            self.updateStatus(STRINGS["STATUS_IMPORTING_PREVIOUS_WP"])
            url, _ = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PREVIOUS_WP_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            if not url:
                self.logger.info("未選擇前期底稿文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            self.fileUrl_previwp = url
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            if STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"] in items:
                self.logger.warning("前期底稿已存在")
                self.updateStatus(STRINGS["STATUS_WARN_PREVIOUS_WP_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_PREVIOUS_WP_EXISTS"])
                return
            try:
                column_checking = pd.read_excel(self.fileUrl_previwp, dtype=str, nrows=3, engine='openpyxl').columns.tolist()
                important_col = ['Remarked by Procurement', 'Noted by Procurement', 'Line#']
                if not set(important_col).issubset(column_checking):
                    self.logger.warning("前期底稿格式錯誤，缺少必要列")
                    self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PREVIOUS_WP"], error=True)
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_PREVIOUS_WP_FORMAT_ERROR"])
                    return
                self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"])
                self.logger.info(f"成功導入前期底稿: {os.path.basename(self.fileUrl_previwp)}")
                self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + os.path.basename(self.fileUrl_previwp))
            except Exception as e:
                self.logger.error(f"驗證前期底稿時出錯: {str(e)}", exc_info=True)
                self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PREVIOUS_WP"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_VALIDATING_PREVIOUS_WP_DETAILED"].format(str(e)))
        except Exception as err:
            self.logger.error(f"導入前期底稿時出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_PREVIOUS_WP"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_PREVIOUS_WP_DETAILED"].format(str(err)))

    def import_procurement(self):
        try:
            self.had_error = False
            self.updateStatus(STRINGS["STATUS_IMPORTING_PROCUREMENT_WP"])
            url, _ = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PROCUREMENT_WP_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            if not url:
                self.logger.info("未選擇採購底稿文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            self.fileUrl_p = url
            items = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            if STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"] in items:
                self.logger.warning("採購底稿已存在")
                self.updateStatus(STRINGS["STATUS_WARN_PROCUREMENT_WP_EXISTS"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_PROCUREMENT_WP_EXISTS"])
                return
            try:
                column_checking = pd.read_excel(self.fileUrl_p, dtype=str, nrows=3, engine='openpyxl').columns.tolist()
                important_col = ['Remarked by Procurement', 'Noted by Procurement', 'Line#']
                if not set(important_col).issubset(column_checking):
                    self.logger.warning("採購底稿格式錯誤，缺少必要列")
                    self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PROCUREMENT_WP"], error=True)
                    QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_PROCUREMENT_WP_FORMAT_ERROR"])
                    return
                self.importedList.addItem(STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"])
                self.logger.info(f"成功導入採購底稿: {os.path.basename(self.fileUrl_p)}")
                self.updateStatus(STRINGS["STATUS_FILE_IMPORTED_PREFIX"] + os.path.basename(self.fileUrl_p))
            except Exception as e:
                self.logger.error(f"驗證採購底稿時出錯: {str(e)}", exc_info=True)
                self.updateStatus(STRINGS["STATUS_ERR_VALIDATING_PROCUREMENT_WP"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_VALIDATING_PROCUREMENT_WP_DETAILED"].format(str(e)))
        except Exception as err:
            self.logger.error(f"導入採購底稿時出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_ERR_IMPORTING_PROCUREMENT_WP"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_IMPORTING_PROCUREMENT_WP_DETAILED"].format(str(err)))
    
    def process(self):
        try:
            self.had_error = False
            items_text_list = [self.importedList.item(x).text() for x in range(self.importedList.count())]
            module_type_str_key = self.entitySelect.currentText() 

            if STRINGS["MAIN_BTN_IMPORT_RAW"] not in items_text_list:
                self.logger.warning("Raw data not imported for Main.process.")
                self.updateStatus(STRINGS["STATUS_ERR_NO_RAW_DATA_TO_PROCESS"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_NO_RAW_DATA"])
                return

            self.updateStatus(STRINGS["STATUS_PROCESSING_DATA"])
            self.btnProcess.setEnabled(False)
            
            files_data = {'imported_item_names': items_text_list}
            if hasattr(self, 'fileUrl') and self.fileUrl: files_data['raw'] = self.fileUrl
            if hasattr(self, 'file_name') and self.file_name: files_data['raw_name'] = self.file_name
            
            if STRINGS["MAIN_BTN_IMPORT_CLOSING_LIST"] in items_text_list and hasattr(self, 'fileUrl_c') and self.fileUrl_c:
                files_data['closing'] = self.fileUrl_c
            if STRINGS["MAIN_BTN_IMPORT_PREVIOUS_WP"] in items_text_list and hasattr(self, 'fileUrl_previwp') and self.fileUrl_previwp:
                files_data['previous_wp'] = self.fileUrl_previwp
            if STRINGS["MAIN_BTN_IMPORT_PROCUREMENT"] in items_text_list and hasattr(self, 'fileUrl_p') and self.fileUrl_p:
                files_data['procurement'] = self.fileUrl_p
            
            if self.thread is not None and self.thread.isRunning():
                self.logger.warning("Processing is already in progress.")
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS.get("STATUS_PROCESSING_ALREADY_RUNNING", "Processing is already in progress."))
                self.btnProcess.setEnabled(True) 
                return

            self.thread = QThread(self) 
            self.worker = ProcessingWorker(module_type_str_key, files_data)
            self.worker.moveToThread(self.thread)

            self.thread.started.connect(self.worker.process_data)
            self.worker.finished.connect(self.on_processing_finished)
            self.worker.error.connect(self.on_processing_error)
            
            self.thread.finished.connect(self.worker.deleteLater)
            self.thread.finished.connect(self.thread.deleteLater)
            self.worker.error.connect(self.thread.quit) 

            self.thread.start()

        except Exception as err:
            self.logger.error(f"Error setting up processing thread in Main.process: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS.get("STATUS_ERR_PROCESSING_DATA", "Error: Processing data failed"), error=True)
            QMessageBox.critical(self, STRINGS.get("MSGBOX_TITLE_ERROR", "Error"), STRINGS.get("MSGBOX_ERR_PROCESSING_DATA_DETAILED", "Processing data error: {}").format(str(err)))
            self.btnProcess.setEnabled(True) 

    @pyqtSlot(str, str) 
    def on_processing_finished(self, entity_type, output_file_path):
        self.logger.info(f"Main.on_processing_finished for {entity_type}. Output: {output_file_path}")
        if not hasattr(self, 'btnProcess') or self.btnProcess is None: 
            self.logger.warning("btnProcess not found in on_processing_finished, UI might be closing.")
            return
            
        self.btnProcess.setEnabled(True)
       
        entity_code = "UNKNOWN_ENTITY"
        if entity_type == STRINGS.get("COMBO_ENTITY_MOBAPR"): entity_code = "MOBA_PR"
        elif entity_type == STRINGS.get("COMBO_ENTITY_MOBAPO"): entity_code = "MOBA_PO"
        elif entity_type == STRINGS.get("COMBO_ENTITY_SPTPR"): entity_code = "SPT_PR"
        elif entity_type == STRINGS.get("COMBO_ENTITY_SPTPO"): entity_code = "SPT_PO"
       
        status_msg_key = f"STATUS_PROCESS_{entity_code}_COMPLETE"
        infobox_msg_key = f"MSGBOX_INFO_{entity_code}_COMPLETE"

        if output_file_path == "LOCKED_MODE_ERROR":
            QMessageBox.warning(self, STRINGS.get("MSGBOX_TITLE_WARNING", "Warning"), STRINGS.get("MSGBOX_WARN_LOCKED_MODE", "Locked Mode"))
            self.updateStatus(STRINGS.get("STATUS_ERR_LOCKED_MODE", "Error: Mode is locked"), error=True)
        elif output_file_path == "UNKNOWN_ENTITY_TYPE_ERROR" or output_file_path == "NO_MATCHING_MODE":
            self.updateStatus(STRINGS.get("STATUS_ERR_CANNOT_DETERMINE_MODE", "Error: Cannot determine processing mode."), error=True)
            QMessageBox.warning(self, STRINGS.get("MSGBOX_TITLE_WARNING", "Warning"), STRINGS.get("MSGBOX_WARN_CANNOT_DETERMINE_MODE", "Cannot determine processing mode."))
        elif output_file_path is not None and (output_file_path.endswith(("_ERROR", "_EXCEPTION")) or output_file_path.startswith("EXECUTION_ERROR:")): 
            self.updateStatus(STRINGS.get(f"STATUS_ERR_PROCESSING_{entity_code}", f"Error processing {entity_type}."), error=True)
            QMessageBox.critical(self, STRINGS.get("MSGBOX_TITLE_ERROR", "Error"), f"Processing failed for {entity_type}: {output_file_path}")
        else: 
            status_text = STRINGS.get(status_msg_key, f"{entity_type} processing complete.")
            self.updateStatus(status_text) 
            
            actual_infobox_message = STRINGS.get(infobox_msg_key, f"Processing for {entity_type} complete. Output: {output_file_path}")
            if "{output_file_path}" in actual_infobox_message: actual_infobox_message = actual_infobox_message.format(output_file_path=output_file_path)
            elif "{entity_type}" in actual_infobox_message: actual_infobox_message = actual_infobox_message.format(entity_type=entity_type)
            QMessageBox.information(self, STRINGS.get("MSGBOX_TITLE_INFO", "Info"), actual_infobox_message)
       
        if self.thread is not None:
            if self.thread.isRunning(): self.thread.quit(); self.thread.wait(500) 
            # self.thread.deleteLater() # Connected to thread.finished
            self.thread = None 
        if self.worker is not None: 
            # self.worker.deleteLater() # Connected to thread.finished
            self.worker = None

    @pyqtSlot(str, str) 
    def on_processing_error(self, entity_type, error_message):
        self.logger.error(f"Main.on_processing_error for {entity_type}: {error_message}")
        if not hasattr(self, 'btnProcess') or self.btnProcess is None:
            self.logger.warning("btnProcess not found in on_processing_error, UI might be closing.")
            return
        self.btnProcess.setEnabled(True)

        entity_code = "UNKNOWN_ENTITY"
        if entity_type == STRINGS.get("COMBO_ENTITY_MOBAPR"): entity_code = "MOBA_PR"
        elif entity_type == STRINGS.get("COMBO_ENTITY_MOBAPO"): entity_code = "MOBA_PO"
        elif entity_type == STRINGS.get("COMBO_ENTITY_SPTPR"): entity_code = "SPT_PR"
        elif entity_type == STRINGS.get("COMBO_ENTITY_SPTPO"): entity_code = "SPT_PO"
        
        status_err_msg_key = f"STATUS_ERR_PROCESSING_{entity_code}"
        default_err_status = f"Error processing {entity_type}."
        self.updateStatus(STRINGS.get(status_err_msg_key, default_err_status), error=True)
        QMessageBox.critical(self, STRINGS.get("MSGBOX_TITLE_ERROR", "Error"), error_message)
       
        if self.thread is not None:
            if self.thread.isRunning(): self.thread.quit(); self.thread.wait(500)
            # self.thread.deleteLater() # Connected to thread.finished
            self.thread = None
        if self.worker is not None: 
            # self.worker.deleteLater() # Connected to thread.finished
            self.worker = None

    def deleteImportFile(self):
        try:
            if not self.importedList.selectedItems():
                self.logger.warning("未選擇要刪除的項目")
                self.updateStatus(STRINGS["STATUS_WARN_NO_ITEM_TO_DELETE"])
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["MSGBOX_WARN_SELECT_ITEM_TO_DELETE"])
                return
            index = self.importedList.currentRow()
            item = self.importedList.currentItem()
            mbox = QMessageBox.question(self, STRINGS["MSGBOX_TITLE_CONFIRM"], STRINGS["MSGBOX_CONFIRM_DELETE_ITEM_PROMPT"].format(item.text()), QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if mbox == QMessageBox.Yes:
                try:
                    self.importedList.takeItem(index)
                    self.logger.info(f"已刪除項目: {item.text()}")
                    self.updateStatus(STRINGS["STATUS_ITEM_DELETED_PREFIX"] + item.text())
                except Exception as err:
                    self.logger.error(f"刪除項目時出錯: {str(err)}", exc_info=True)
                    self.updateStatus(STRINGS["STATUS_ERR_DELETING_ITEM"], error=True)
                    QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_DELETING_ITEM_FAILED"])
        except Exception as err:
            self.logger.error(f"刪除文件時出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_ERR_DELETING_FILE"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_DELETING_FILE_FAILED_DETAILED"].format(str(err)))

    def uploadFormWidget(self):
        try:
            self.had_error = False
            self.updateStatus(STRINGS["STATUS_OPENING_UPLOAD_FORM_DIALOG"])
            sub_widget = UploadFormWidget(self)
            sub_widget.exec_()
            self.updateStatus(STRINGS["STATUS_READY"])
        except Exception as err:
            self.logger.error(f"打開Upload Form對話框時出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_ERR_OPENING_UPLOAD_FORM_DIALOG"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_OPENING_UPLOAD_FORM_DIALOG_DETAILED"].format(str(err)))

    def check2(self):
        try:
            self.had_error = False
            self.logger.info("開始兩期檢查")
            self.updateStatus(STRINGS["STATUS_PERFORMING_CHECK2"])
            url_pr, _ = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PURCHASE_PR_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            if not url_pr:
                self.logger.info("未選擇採購PR文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            url_po, _ = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PURCHASE_PO_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ONLY"])
            if not url_po:
                self.logger.info("未選擇採購PO文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            url_ac, _ = QFileDialog.getOpenFileName(self, STRINGS["WINDOW_TITLE_PREVIOUS_WP_XLSM_DIALOG"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_MACRO_ENABLED"])
            if not url_ac:
                self.logger.info("未選擇前期底稿文件，取消操作")
                self.updateStatus(STRINGS["STATUS_READY"])
                return
            try:
                a, b, c = ReconEntryAmt.get_difference(url_ac, url_pr, url_po)
                if self.checkHadError():
                    self.updateStatus(STRINGS["STATUS_PROCESSING_ERROR_STOP"], error=True)
                    return
                pd.DataFrame({**a, **b, **c}, index=[0]).T.to_excel('check_dif_amount.xlsx')
                self.updateStatus(STRINGS["STATUS_CHECK2_COMPLETE_FILE_SAVED"])
                QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], STRINGS["MSGBOX_INFO_CHECK2_COMPLETE"])
                self.logger.info("兩期檢查完成")
            except Exception as err:
                self.logger.error(f"執行差異比較時出錯: {str(err)}", exc_info=True)
                self.updateStatus(STRINGS["STATUS_ERR_PERFORMING_CHECK2_COMPARE"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_CHECK2_COMPARE_FAILED_DETAILED"].format(str(err)))
                return
        except Exception as err:
            self.logger.error(f"兩期檢查時出錯: {str(err)}", exc_info=True)
            self.updateStatus(STRINGS["STATUS_ERR_PERFORMING_CHECK2"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], STRINGS["MSGBOX_ERR_CHECK2_FAILED_DETAILED"].format(str(err)))

    def add_spx_tab_to_main_ui(self):
        self.spxTab = SPXTabWidget(self)
        self.tabWidget.addTab(self.spxTab, STRINGS["TAB_SPX"])
        if hasattr(self, 'logger') and self.logger is not None:
            self.logger.info("SPX模組Tab已初始化")

class UploadFormWidget(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.logger = Logger().get_logger(__name__)
        self.had_error = False
        self.setWindowTitle(STRINGS["WINDOW_TITLE_UPLOAD_FORM"])
        self.setupUI()
        self.logger.info("Upload Form對話框已打開")
    def updateStatus(self, message, error=False):
        if error: self.had_error = True
    def setupUI(self):
        self.mainLayout = QHBoxLayout()
        self.leftLayout = QVBoxLayout()
        self.rightLayout = QVBoxLayout()
        self.mainLayout.addLayout(self.leftLayout, 60)
        self.mainLayout.addLayout(self.rightLayout, 40)
        self.label_entity = QLabel(STRINGS["UPLOAD_FORM_LBL_ENTITY"])
        self.combo_entity = QComboBox(self)
        entities = [STRINGS["COMBO_ENTITY_MOBTW"], STRINGS["COMBO_ENTITY_SPTTW"]]
        for name in entities: self.combo_entity.addItem(name)
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
        for c in currencies: self.combo_currency.addItem(c)
        self.label_wp = QLabel(STRINGS["UPLOAD_FORM_LBL_WP"])
        self.button_get_wp = QPushButton(STRINGS["UPLOAD_FORM_BTN_SELECT_WP"])
        self.button_get_wp.clicked.connect(self.get_wp)
        self.label_start = QLabel(STRINGS["UPLOAD_FORM_LBL_PROCESS"])
        self.button_do_upload = QPushButton(STRINGS["UPLOAD_FORM_BTN_GENERATE_UPLOAD_FORM"])
        self.button_do_upload.clicked.connect(self.process_upload_form)
        self.statusLabel = QLabel(STRINGS["MAIN_LBL_STATUS_PREFIX"] + STRINGS["STATUS_READY"])
        self.leftLayout.addWidget(self.label_entity)
        self.leftLayout.addWidget(self.label_period)
        self.leftLayout.addWidget(self.label_ac_date)
        self.leftLayout.addWidget(self.label_cate)
        self.leftLayout.addWidget(self.label_accountant)
        self.leftLayout.addWidget(self.label_currency)
        self.leftLayout.addWidget(self.label_wp)
        self.leftLayout.addWidget(self.label_start)
        self.rightLayout.addWidget(self.combo_entity)
        self.rightLayout.addWidget(self.line_period)
        self.rightLayout.addWidget(self.line_ac_date)
        self.rightLayout.addWidget(self.line_cate)
        self.rightLayout.addWidget(self.line_accountant)
        self.rightLayout.addWidget(self.combo_currency)
        self.rightLayout.addWidget(self.button_get_wp)
        self.rightLayout.addWidget(self.button_do_upload)
        statusLayout = QHBoxLayout()
        statusLayout.addWidget(self.statusLabel)
        mainContainer = QVBoxLayout()
        mainContainer.addLayout(self.mainLayout)
        mainContainer.addStretch()
        mainContainer.addLayout(statusLayout)
        self.setLayout(mainContainer)
    def get_wp(self):
        try:
            self.had_error = False
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_SELECTING_WP"])
            url, _ = QFileDialog.getOpenFileName(self, STRINGS["UPLOAD_FORM_LBL_WP"], "", STRINGS["FILE_DIALOG_FILTER_EXCEL_ALL_TYPES"])
            if not url:
                self.logger.info("未選擇工作底稿文件，取消操作")
                self.statusLabel.setText(STRINGS["MAIN_LBL_STATUS_PREFIX"] + STRINGS["STATUS_READY"])
                return
            self.fileUrl = url
            self.logger.info(f"已選擇工作底稿: {os.path.basename(self.fileUrl)}")
            self.button_get_wp.setText(f"{STRINGS['UPLOAD_FORM_STATUS_SELECTED_WP_PREFIX']}{os.path.basename(self.fileUrl)}")
            self.statusLabel.setText(f"{STRINGS['UPLOAD_FORM_STATUS_SELECTED_WP_PREFIX']}{os.path.basename(self.fileUrl)}")
        except Exception as err:
            self.logger.error(f"選擇工作底稿時出錯: {str(err)}", exc_info=True)
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_SELECTING_WP"])
            self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_SELECTING_WP"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['UPLOAD_FORM_STATUS_ERR_SELECTING_WP']}:\n{str(err)}")
    def process_upload_form(self):
        try:
            self.had_error = False
            self.logger.info("開始生成Upload Form")
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_GENERATING"])
            entity = self.combo_entity.currentText()
            period = self.line_period.text()
            ac_date = self.line_ac_date.text()
            cate = self.line_cate.text()
            accountant = self.line_accountant.text()
            currency = self.combo_currency.currentText()
            if not hasattr(self, 'fileUrl') or not self.fileUrl:
                self.logger.warning("未選擇工作底稿")
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_WP_SELECTED"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_WP_SELECTED"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_NO_WP_SELECTED"])
                return
            if not period:
                self.logger.warning("未輸入期間")
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_PERIOD"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_PERIOD"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_NO_PERIOD"])
                return
            if not ac_date:
                self.logger.warning("未輸入會計日期")
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_AC_DATE"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_NO_AC_DATE"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_NO_AC_DATE"])
                return
            try:
                m_date = datetime.strptime(ac_date, '%Y/%m/%d').date()
                ac_period = datetime.strftime(m_date, '%Y/%m')
            except ValueError:
                self.logger.warning("會計日期格式錯誤")
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_AC_DATE_FORMAT"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_AC_DATE_FORMAT"], error=True)
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["UPLOAD_FORM_WARN_AC_DATE_FORMAT"])
                return
            try:
                if entity == STRINGS["COMBO_ENTITY_MOBTW"]:
                    if currency == STRINGS["COMBO_CURRENCY_TWD"]:
                        dfs = get_aggregation_twd(self.fileUrl, ac_period)
                    else:
                        dfs = get_aggregation_foreign(self.fileUrl, ac_period, currency=currency)
                else:
                    if currency == STRINGS["COMBO_CURRENCY_TWD"]:
                        dfs = get_aggregation_twd(self.fileUrl, ac_period, is_mob=False)
                    else:
                        dfs = get_aggregation_foreign(self.fileUrl, ac_period, is_mob=False, currency=currency)
                if self.had_error:
                    self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_PROCESSING_STOPPED"])
                    return
                result = get_entries(dfs, entity, period, ac_date, cate, accountant, currency)
                output_file = f'Upload Form-{entity}-{period[:3]}-{currency}.xlsx'
                result.to_excel(output_file, index=False)
                self.logger.info(f"已成功生成Upload Form: {output_file}")
                self.statusLabel.setText(f"{STRINGS['UPLOAD_FORM_STATUS_SUCCESS_PREFIX']} {output_file}")
                QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], f"{STRINGS['UPLOAD_FORM_INFO_GENERATED_PREFIX']}{output_file}")
            except Exception as e:
                self.logger.error(f"生成Upload Form時出錯: {str(e)}", exc_info=True)
                self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_GENERATING_DETAILED"])
                self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_GENERATING_DETAILED"], error=True)
                QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['UPLOAD_FORM_STATUS_ERR_GENERATING_DETAILED']}:\n{str(e)}")
                return
        except Exception as err:
            self.logger.error(f"處理Upload Form時出錯: {str(err)}", exc_info=True)
            self.statusLabel.setText(STRINGS["UPLOAD_FORM_STATUS_ERR_PROCESSING_DETAILED"])
            self.updateStatus(STRINGS["UPLOAD_FORM_STATUS_ERR_PROCESSING_DETAILED"], error=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['UPLOAD_FORM_STATUS_ERR_PROCESSING_DETAILED']}:\n{str(err)}")

class SPXTabWidget(QWidget):
    def __init__(self, parent=None):
        super(SPXTabWidget, self).__init__(parent)
        self.parent = parent
        self.file_paths = {}
        self.file_types = [] 
        self.setupUI()
        self.thread = None 
        self.worker = None 
    def setupUI(self):
        self.file_types = [
            ("po_file", STRINGS["SPX_TAB_FILE_TYPES_PO_FILE"]),
            ("previous_wp", STRINGS["SPX_TAB_FILE_TYPES_PREVIOUS_WP_PO"]),
            ("procurement", STRINGS["SPX_TAB_FILE_TYPES_PROCUREMENT_PO"]),
            ("ap_invoice", STRINGS["SPX_TAB_FILE_TYPES_AP_INVOICE"]),
            ("previous_wp_pr", STRINGS["SPX_TAB_FILE_TYPES_PREVIOUS_WP_PR"]),
            ("procurement_pr", STRINGS["SPX_TAB_FILE_TYPES_PROCUREMENT_PR"])
        ]
        main_layout = QHBoxLayout(self)
        left_layout = QVBoxLayout()
        right_layout = QVBoxLayout()
        main_layout.addLayout(left_layout, 60)
        main_layout.addLayout(right_layout, 40)
        upload_group = QGroupBox(STRINGS["SPX_GRP_FILE_UPLOAD"])
        grid_layout = QGridLayout()
        self.buttons = {}
        self.labels = {}
        for row, (file_key, file_label_from_strings) in enumerate(self.file_types):
            label = QLabel(f"{file_label_from_strings}:")
            grid_layout.addWidget(label, row, 0)
            file_name_label = QLabel(STRINGS["SPX_LBL_FILENAME_NOT_SELECTED"])
            file_name_label.setStyleSheet("color: gray;")
            grid_layout.addWidget(file_name_label, row, 1)
            self.labels[file_key] = file_name_label
            upload_btn = QPushButton(STRINGS["SPX_BTN_SELECT_FILE"])
            upload_btn.clicked.connect(lambda checked, k=file_key, fl=file_label_from_strings: self.select_file(k, fl))
            grid_layout.addWidget(upload_btn, row, 2)
            self.buttons[file_key] = upload_btn
        upload_group.setLayout(grid_layout)
        process_group = QGroupBox(STRINGS["SPX_GRP_PROCESS_PARAMS"])
        process_layout = QGridLayout()
        process_layout.addWidget(QLabel(STRINGS["SPX_LBL_PERIOD_YYYYMM"]), 0, 0)
        self.period_input = QLineEdit()
        self.period_input.setPlaceholderText(STRINGS["SPX_PLACEHOLDER_PERIOD_YYYYMM"])
        process_layout.addWidget(self.period_input, 0, 1)
        process_layout.addWidget(QLabel(STRINGS["SPX_LBL_USER"]), 1, 0)
        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText(STRINGS["SPX_PLACEHOLDER_USER"])
        process_layout.addWidget(self.user_input, 1, 1)
        process_group.setLayout(process_layout)
        self.file_list = QListWidget()
        file_list_label = QLabel(STRINGS["SPX_LBL_UPLOADED_FILES_TITLE"])
        file_list_label.setStyleSheet("font-size:12pt;background-color:#93FF93;color:#000080;font:Bold")
        file_list_label.setAlignment(Qt.AlignCenter)
        left_layout.addWidget(upload_group)
        left_layout.addWidget(process_group)
        left_layout.addWidget(file_list_label)
        left_layout.addWidget(self.file_list)
        self.process_btn = QPushButton(STRINGS["SPX_BTN_PROCESS_GENERATE"])
        self.process_btn.clicked.connect(self.process_files)
        self.process_btn.setStyleSheet("font-size:12pt;font:Bold;padding:10px;")
        self.clear_btn = QPushButton(STRINGS["SPX_BTN_CLEAR_ALL_FILES"])
        self.clear_btn.clicked.connect(self.clear_all_files)
        self.export_btn = QPushButton(STRINGS["SPX_BTN_EXPORT_UPLOAD_FORM"])
        self.export_btn.clicked.connect(self.export_upload_form)
        tips_label = QLabel(STRINGS["SPX_LBL_MODULE_DESCRIPTION_TITLE"])
        tips_label.setStyleSheet("font-size:11pt;font:Bold;")
        tips_content = QLabel(STRINGS["SPX_LBL_MODULE_DESCRIPTION_CONTENT"])
        tips_content.setWordWrap(True)
        tips_content.setStyleSheet("color:#000080;padding:10px;border-radius:5px;")
        self.status_label = QLabel(STRINGS["MAIN_LBL_STATUS_PREFIX"] + STRINGS["STATUS_READY"])
        self.status_label.setStyleSheet("font-size:10pt;")
        right_layout.addWidget(self.process_btn)
        right_layout.addWidget(self.clear_btn)
        right_layout.addWidget(self.export_btn)
        right_layout.addSpacing(20)
        right_layout.addWidget(tips_label)
        right_layout.addWidget(tips_content)
        right_layout.addStretch()
        right_layout.addWidget(self.status_label)
        self.setLayout(main_layout)

    def select_file(self, file_key, file_label):
        try:
            if file_key == "ap_invoice": file_filter = STRINGS["SPX_FILE_DIALOG_FILTER_EXCEL_ALL"]
            else: file_filter = STRINGS["SPX_FILE_DIALOG_FILTER_DEFAULT"]
            file_path, _ = QFileDialog.getOpenFileName(self, STRINGS["SPX_FILE_DIALOG_SELECT_PREFIX"] + file_label, "", file_filter)
            if not file_path: return
            self.file_paths[file_key] = file_path
            file_name = os.path.basename(file_path)
            self.labels[file_key].setText(file_name)
            self.labels[file_key].setStyleSheet("color: blue;")
            list_item = f"{file_label}: {file_name}"
            existing_items = [self.file_list.item(i).text() for i in range(self.file_list.count())]
            matching_items = [item for item in existing_items if item.startswith(f"{file_label}:")]
            if matching_items:
                for item in matching_items:
                    item_idx = existing_items.index(item)
                    self.file_list.takeItem(item_idx)
            self.file_list.addItem(list_item)
            self.status_label.setText(STRINGS["SPX_STATUS_FILE_SELECTED_PREFIX"] + file_label)
            if file_key == "po_file" and not self.period_input.text():
                try:
                    year_month = file_name[:6]
                    if year_month.isdigit() and len(year_month) == 6: self.period_input.setText(year_month)
                except Exception: pass
            if hasattr(self.parent, 'logger'): self.parent.logger.info(f"已選擇 {file_label}: {file_name}")
        except Exception as e:
            self.status_label.setText(STRINGS["SPX_STATUS_ERR_SELECTING_FILE"])
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            if hasattr(self.parent, 'logger'): self.parent.logger.error(f"選擇文件時出錯: {str(e)}", exc_info=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['SPX_STATUS_ERR_SELECTING_FILE']}:\n{str(e)}")
    
    def process_files(self):
        try:
            required_files = ["po_file", "ap_invoice"]
            missing_files_keys = [key for key in required_files if key not in self.file_paths or not self.file_paths[key]]
            if missing_files_keys:
                file_type_dict = dict(self.file_types)
                missing_labels = [file_type_dict.get(key, key) for key in missing_files_keys]
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_MISSING_FILES_PREFIX"] + ', '.join(missing_labels))
                return

            period_text = self.period_input.text()
            user_text = self.user_input.text()

            if not period_text or not period_text.isdigit() or len(period_text) != 6:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_PERIOD_FORMAT"])
                return
            if not user_text:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_USER"])
                return

            self.status_label.setText(STRINGS.get("SPX_STATUS_PROCESSING", "Processing..."))
            self.status_label.setStyleSheet("font-size:10pt; color: blue;")
            self.process_btn.setEnabled(False)
            
            worker_files_data = self.file_paths.copy() 
            if self.file_paths.get("po_file"):
                worker_files_data['po_file_name'] = os.path.basename(self.file_paths.get("po_file"))
           
            worker_processing_params = {
                'period': period_text,
                'user': user_text
            }
           
            if self.thread is not None and self.thread.isRunning():
                self.parent.logger.warning("SPX processing already in progress. Please wait.") 
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS.get("STATUS_PROCESSING_ALREADY_RUNNING", "Processing is already in progress."))
                self.process_btn.setEnabled(True)
                return

            self.thread = QThread(self) 
            self.worker = ProcessingWorker("SPX_PO_PROCESSOR_KEY", worker_files_data, worker_processing_params)
            self.worker.moveToThread(self.thread)

            self.thread.started.connect(self.worker.process_data)
            self.worker.finished.connect(self.on_spx_processing_finished)
            self.worker.error.connect(self.on_spx_processing_error)

            self.thread.finished.connect(self.worker.deleteLater) 
            self.thread.finished.connect(self.thread.deleteLater)
            self.worker.error.connect(self.thread.quit)

            self.thread.start()

        except Exception as e:
            self.parent.logger.error(f"Error setting up SPX processing thread: {str(e)}", exc_info=True)
            self.status_label.setText(STRINGS.get("SPX_STATUS_PROCESS_ERROR", "SPX Processing Error"))
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            QMessageBox.critical(self, STRINGS.get("MSGBOX_TITLE_ERROR", "Error"), f"SPX Setup Error: {str(e)}")
            self.process_btn.setEnabled(True)
            
    def clear_all_files(self):
        try:
            self.file_paths = {}
            for label in self.labels.values():
                label.setText(STRINGS["SPX_LBL_FILENAME_NOT_SELECTED"])
                label.setStyleSheet("color: gray;")
            self.file_list.clear()
            self.status_label.setText(STRINGS["SPX_STATUS_FILES_CLEARED"])
            if hasattr(self.parent, 'logger'): self.parent.logger.info("已清除所有SPX文件")
        except Exception as e:
            self.status_label.setText(STRINGS["SPX_STATUS_ERR_CLEARING_FILES"])
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            if hasattr(self.parent, 'logger'): self.parent.logger.error(f"清除文件時出錯: {str(e)}", exc_info=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['SPX_STATUS_ERR_CLEARING_FILES']}:\n{str(e)}")
    def export_upload_form(self):
        try:
            if "po_file" not in self.file_paths:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_NO_PO_FILE_FOR_UPLOAD_FORM"])
                return
            period = self.period_input.text()
            user = self.user_input.text()
            if not period or not period.isdigit() or len(period) != 6:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_PERIOD_FORMAT"])
                return
            if not user:
                QMessageBox.warning(self, STRINGS["MSGBOX_TITLE_WARNING"], STRINGS["SPX_WARN_INVALID_USER"])
                return
            self.status_label.setText(STRINGS["SPX_STATUS_EXPORTING_UPLOAD_FORM"])
            self.status_label.setStyleSheet("font-size:10pt; color: blue;")
            entity = STRINGS["SPX_ENTITY_NAME_INTERNAL"]
            year = period[:4]; month = period[4:6]
            period_str = f"{year}-{month}"
            month_abbr = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"][int(month) - 1]
            period_display = f"{month_abbr}-{year[2:]}"
            accounting_date = f"{year}/{month}/25"
            category = STRINGS["SPX_CATEGORY_DEFAULT"]
            self._generate_upload_form(
                po_file_path=self.file_paths.get("po_file"), entity=entity, period=period_display,
                period_str=period_str, accounting_date=accounting_date, category=category, user=user
            )
            self.status_label.setText(STRINGS["SPX_STATUS_UPLOAD_FORM_EXPORTED"])
            self.status_label.setStyleSheet("font-size:10pt; color: green;")
        except Exception as e:
            self.status_label.setText(STRINGS["SPX_STATUS_ERR_EXPORTING_UPLOAD_FORM"])
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            if hasattr(self.parent, 'logger'): self.parent.logger.error(f"匯出上傳表單時出錯: {str(e)}", exc_info=True)
            QMessageBox.critical(self, STRINGS["MSGBOX_TITLE_ERROR"], f"{STRINGS['SPX_STATUS_ERR_EXPORTING_UPLOAD_FORM']}:\n{str(e)}")
    def _generate_upload_form(self, po_file_path, entity, period, period_str, accounting_date, category, user):
        try:
            if hasattr(self.parent, 'logger'): self.parent.logger.info(f"開始生成上傳表單: {entity}, {period}")
            from .upload_form import get_aggregation_twd, get_entries
            dfs = get_aggregation_twd(po_file_path, period_str, is_mob=False)
            result = get_entries(dfs, entity, period, accounting_date, category, user, STRINGS["SPX_CURRENCY_DEFAULT"])
            output_file = f'Upload Form-{entity}-{period}-{STRINGS["SPX_CURRENCY_DEFAULT"]}.xlsx'
            result.to_excel(output_file, index=False)
            QMessageBox.information(self, STRINGS["MSGBOX_TITLE_INFO"], f"{STRINGS['SPX_INFO_UPLOAD_FORM_GENERATED_PREFIX']}{output_file}")
        except Exception as e:
            if hasattr(self.parent, 'logger'): self.parent.logger.error(f"生成上傳表單時出錯: {str(e)}", exc_info=True)
            raise

    @pyqtSlot(str, str) 
    def on_spx_processing_finished(self, entity_type, output_file_path):
        self.parent.logger.info(f"SPX Processing finished via worker. Output: {output_file_path}") 
        if not hasattr(self, 'process_btn') or self.process_btn is None:
            self.parent.logger.warning("SPXTabWidget.process_btn not found, UI might be closing.")
            return

        self.process_btn.setEnabled(True)
        
        if output_file_path == "LOCKED_MODE_ERROR" or \
           output_file_path == "UNKNOWN_ENTITY_TYPE_ERROR" or \
           output_file_path == "NO_MATCHING_MODE" or \
           (output_file_path is not None and output_file_path.endswith(("_ERROR", "_EXCEPTION"))):
            self.status_label.setText(STRINGS.get("SPX_STATUS_PROCESS_ERROR", "Error processing SPX data."))
            self.status_label.setStyleSheet("font-size:10pt; color: red;")
            QMessageBox.critical(self, STRINGS.get("MSGBOX_TITLE_ERROR", "Error"), f"SPX Processing Error: {output_file_path}")
        else:
            self.status_label.setText(STRINGS.get("SPX_STATUS_PROCESS_COMPLETE", "SPX Processing Complete!"))
            self.status_label.setStyleSheet("font-size:10pt; color: green;")
            
            QMessageBox.information(self, STRINGS.get("MSGBOX_TITLE_INFO", "Info"), STRINGS.get("SPX_INFO_PROCESS_COMPLETE_FROM_UI", "SPX data processing initiated by worker has finished."))


        if self.thread is not None:
            if self.thread.isRunning(): self.thread.quit(); self.thread.wait(500)
            self.thread.deleteLater() 
            self.thread = None
        if self.worker is not None: 
            self.worker.deleteLater() 
            self.worker = None

    @pyqtSlot(str, str) 
    def on_spx_processing_error(self, entity_type, error_message):
        self.parent.logger.error(f"SPX Processing error via worker: {error_message}")
        if not hasattr(self, 'process_btn') or self.process_btn is None:
            self.parent.logger.warning("SPXTabWidget.process_btn not found, UI might be closing.")
            return
            
        self.process_btn.setEnabled(True)
        self.status_label.setText(STRINGS.get("SPX_STATUS_PROCESS_ERROR", "SPX Processing Error"))
        self.status_label.setStyleSheet("font-size:10pt; color: red;")
        QMessageBox.critical(self, STRINGS.get("MSGBOX_TITLE_ERROR", "Error"), error_message)

        if self.thread is not None:
            if self.thread.isRunning(): self.thread.quit(); self.thread.wait(500)
            self.thread.deleteLater()
            self.thread = None
        if self.worker is not None: 
            self.worker.deleteLater()
            self.worker = None

def main():
    try:
        APP = QApplication(sys.argv)
        window = Main()
        # apply_stylesheet(APP, theme='dark_lightgreen.xml') # Kept commented out
        sys.exit(APP.exec_())
    except Exception as e:
        logging.error(f"啟動應用時出錯: {str(e)}", exc_info=True)
        print(f"啟動應用時出錯: {str(e)}")

if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        print('應用正常退出')
    except Exception as e:
        err_msg_template = "Application Error: {}"
        try:
            if 'STRINGS' in globals() and isinstance(STRINGS, dict):
                 err_msg_template = STRINGS.get("MSGBOX_ERR_PROCESSING_FAILED_DETAILED", "Application Error: {}")
        except NameError:
            pass 
        err_msg = err_msg_template.format(str(e))
        print(err_msg)
        logging.error(err_msg, exc_info=True)

```
