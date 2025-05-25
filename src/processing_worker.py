# src/processing_worker.py

import time 
import traceback
import os
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot

from .utils import Logger
from .ui_strings import STRINGS

class ProcessingWorker(QObject):
    finished = pyqtSignal(str, str)  # entity_type, output_file_path
    error = pyqtSignal(str, str)     # entity_type, error_message

    def __init__(self, entity_type, files_data, processing_params=None):
        super().__init__()
        self.entity_type = entity_type
        self.files_data = files_data 
        self.processing_params = processing_params if processing_params else {}
        self.logger = Logger().get_logger(self.__class__.__name__)

    @pyqtSlot()
    def process_data(self):
        try:
            self.logger.info(f"Worker started for entity: {self.entity_type} with files: {self.files_data.get('imported_item_names', 'N/A')}")
            output_file_path = self._execute_entity_processing() 
            
            if output_file_path and not output_file_path.endswith( ("_ERROR", "_EXCEPTION")) and \
               output_file_path not in ["LOCKED_MODE_ERROR", "UNKNOWN_ENTITY_TYPE_ERROR", "NO_MATCHING_MODE", "PROCESSOR_LOGIC_INCOMPLETE_OR_ERROR"]:
                self.finished.emit(self.entity_type, output_file_path)
            else:
                error_msg = output_file_path if output_file_path else "Unknown error in worker's _execute_entity_processing."
                if output_file_path in ["LOCKED_MODE_ERROR", "UNKNOWN_ENTITY_TYPE_ERROR", "NO_MATCHING_MODE", "PROCESSOR_LOGIC_INCOMPLETE_OR_ERROR"]:
                    self.logger.warning(f"Worker emitting specific non-success 'finished' state for {self.entity_type}: {output_file_path}")
                    self.finished.emit(self.entity_type, output_file_path) 
                else: 
                    self.logger.error(f"Worker emitting error for {self.entity_type}: {error_msg}")
                    self.error.emit(self.entity_type, error_msg)
        except Exception as e:
            error_details = traceback.format_exc()
            self.logger.error(f"Critical Error in ProcessingWorker for {self.entity_type}: {str(e)}\n{error_details}")
            self.error.emit(self.entity_type, f"Critical Error during processing for {self.entity_type}: {str(e)}\nDetails:\n{error_details}")

    def _execute_entity_processing(self) -> str:
        self.logger.info(f"Worker._execute_entity_processing called for {self.entity_type}")
        self.logger.info(f"File data: {self.files_data}")
        self.logger.info(f"Processing params: {self.processing_params}")

        from .mobtwpr import MOBTW_PR
        from .mobtwpo import MOBTW_PO
        from .spttwpr import SPTTW_PR
        from .spttwpo import SPTTW_PO
        from .spxtwpo import SPXTW_PO 

        processor = None # For MOB/SPT
        processor_spx = None # For SPX
        output_file_path = "" 
        
        items_text_list = self.files_data.get('imported_item_names', [])
        raw_file_url = self.files_data.get('raw')
        raw_file_name = self.files_data.get('raw_name')
        procurement_file_url = self.files_data.get('procurement')
        closing_list_file_url = self.files_data.get('closing')
        previous_wp_file_url = self.files_data.get('previous_wp')

        date_from_filename_str = "000000"
        filename_for_date_calc = raw_file_name
        if self.entity_type == "SPX_PO_PROCESSOR_KEY": # SPX uses a different file for date info
            filename_for_date_calc = self.files_data.get('po_file_name', '000000')
        
        if filename_for_date_calc and len(filename_for_date_calc) >=6 and filename_for_date_calc[:6].isdigit():
            date_from_filename_str = filename_for_date_calc[:6]
        else:
            self.logger.warning(f"Could not determine 6-digit date from filename '{filename_for_date_calc}' for output path construction.")

        main_btn_raw = STRINGS.get("MAIN_BTN_IMPORT_RAW", "原始資料")
        main_btn_prev_wp = STRINGS.get("MAIN_BTN_IMPORT_PREVIOUS_WP", "前期底稿")
        main_btn_proc_wp = STRINGS.get("MAIN_BTN_IMPORT_PROCUREMENT", "採購底稿")
        main_btn_closing = STRINGS.get("MAIN_BTN_IMPORT_CLOSING_LIST", "關單清單")

        if self.entity_type == STRINGS["COMBO_ENTITY_MOBAPR"]:
            processor = MOBTW_PR()
            self.logger.info(f"Processing MOBA_PR with items: {items_text_list}")
            if main_btn_prev_wp in items_text_list:
                if len(items_text_list) == 4: return "LOCKED_MODE_ERROR"
                elif len(items_text_list) == 3 and main_btn_proc_wp in items_text_list:
                    processor.mode_5(raw_file_url, raw_file_name, procurement_file_url, previous_wp_file_url)
                elif len(items_text_list) == 3 and main_btn_closing in items_text_list:
                    return "LOCKED_MODE_ERROR"
                elif len(items_text_list) == 2: 
                    processor.mode_5(raw_file_url, raw_file_name, previous_wp_file_url)
                else: return "NO_MATCHING_MODE"
            elif len(items_text_list) == 1 and main_btn_raw in items_text_list: 
                processor.mode_4(raw_file_url, raw_file_name)
            elif {main_btn_raw, main_btn_closing, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_1(raw_file_url, raw_file_name, procurement_file_url, closing_list_file_url)
            elif {main_btn_raw, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_3(raw_file_url, raw_file_name, procurement_file_url)
            elif {main_btn_raw, main_btn_closing}.issubset(items_text_list):
                processor.mode_2(raw_file_url, raw_file_name, closing_list_file_url)
            else: return "NO_MATCHING_MODE"
            output_file_path = f"{date_from_filename_str}-MOB-PR Compare Result.xlsx"

        elif self.entity_type == STRINGS["COMBO_ENTITY_MOBAPO"]:
            processor = MOBTW_PO()
            self.logger.info(f"Processing MOBA_PO with items: {items_text_list}")
            if {main_btn_raw, main_btn_closing, main_btn_proc_wp, main_btn_prev_wp}.issubset(items_text_list):
                return "LOCKED_MODE_ERROR"
            elif {main_btn_raw, main_btn_proc_wp, main_btn_prev_wp}.issubset(items_text_list):
                processor.mode_2(raw_file_url, raw_file_name, previous_wp_file_url, procurement_file_url)
            elif {main_btn_raw, main_btn_closing, main_btn_prev_wp}.issubset(items_text_list):
                return "LOCKED_MODE_ERROR"
            elif {main_btn_raw, main_btn_closing, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_4(raw_file_url, raw_file_name, procurement_file_url, closing_list_file_url)
            elif {main_btn_raw, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_5(raw_file_url, raw_file_name, procurement_file_url)
            elif {main_btn_raw, main_btn_prev_wp}.issubset(items_text_list):
                processor.mode_6(raw_file_url, raw_file_name, previous_wp_file_url)
            elif {main_btn_raw, main_btn_closing}.issubset(items_text_list):
                processor.mode_7(raw_file_url, raw_file_name, closing_list_file_url)
            elif main_btn_raw in items_text_list and len(items_text_list) == 1 :
                processor.mode_8(raw_file_url, raw_file_name)
            else: return "NO_MATCHING_MODE"
            output_file_path = f"{date_from_filename_str}-MOB-PO Compare Result.xlsx"

        elif self.entity_type == STRINGS["COMBO_ENTITY_SPTPR"]:
            processor = SPTTW_PR()
            self.logger.info(f"Processing SPT_PR with items: {items_text_list}")
            if main_btn_prev_wp in items_text_list:
                if len(items_text_list) == 4: return "LOCKED_MODE_ERROR"
                elif len(items_text_list) == 3 and main_btn_proc_wp in items_text_list:
                    processor.mode_5(raw_file_url, raw_file_name, procurement_file_url, previous_wp_file_url)
                elif len(items_text_list) == 3 and main_btn_closing in items_text_list:
                    return "LOCKED_MODE_ERROR"
                elif len(items_text_list) == 2: 
                    processor.mode_5(raw_file_url, raw_file_name, previous_wp_file_url)
                else: return "NO_MATCHING_MODE"
            elif len(items_text_list) == 1 and main_btn_raw in items_text_list:
                processor.mode_4(raw_file_url, raw_file_name)
            elif {main_btn_raw, main_btn_closing, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_1(raw_file_url, raw_file_name, procurement_file_url, closing_list_file_url)
            elif {main_btn_raw, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_3(raw_file_url, raw_file_name, procurement_file_url)
            elif {main_btn_raw, main_btn_closing}.issubset(items_text_list):
                processor.mode_2(raw_file_url, raw_file_name, closing_list_file_url)
            else: return "NO_MATCHING_MODE"
            output_file_path = f"{date_from_filename_str}-SPT-PR Compare Result.xlsx"

        elif self.entity_type == STRINGS["COMBO_ENTITY_SPTPO"]:
            processor = SPTTW_PO()
            self.logger.info(f"Processing SPT_PO with items: {items_text_list}")
            if {main_btn_raw, main_btn_closing, main_btn_proc_wp, main_btn_prev_wp}.issubset(items_text_list):
                return "LOCKED_MODE_ERROR"
            elif {main_btn_raw, main_btn_proc_wp, main_btn_prev_wp}.issubset(items_text_list):
                processor.mode_2(raw_file_url, raw_file_name, previous_wp_file_url, procurement_file_url)
            elif {main_btn_raw, main_btn_closing, main_btn_prev_wp}.issubset(items_text_list):
                return "LOCKED_MODE_ERROR"
            elif {main_btn_raw, main_btn_closing, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_4(raw_file_url, raw_file_name, procurement_file_url, closing_list_file_url)
            elif {main_btn_raw, main_btn_proc_wp}.issubset(items_text_list):
                processor.mode_5(raw_file_url, raw_file_name, procurement_file_url)
            elif {main_btn_raw, main_btn_prev_wp}.issubset(items_text_list):
                processor.mode_6(raw_file_url, raw_file_name, previous_wp_file_url)
            elif {main_btn_raw, main_btn_closing}.issubset(items_text_list):
                processor.mode_7(raw_file_url, raw_file_name, closing_list_file_url)
            elif main_btn_raw in items_text_list and len(items_text_list) == 1:
                processor.mode_8(raw_file_url, raw_file_name)
            else: return "NO_MATCHING_MODE"
            output_file_path = f"{date_from_filename_str}-SPT-PO Compare Result.xlsx"
        
        elif self.entity_type == "SPX_PO_PROCESSOR_KEY": 
            processor_spx = SPXTW_PO() 
            self.logger.info(f"Processing SPX with data: {self.files_data} and params: {self.processing_params}")
            processor_spx.process(
                fileUrl=self.files_data.get("po_file"),
                file_name=self.files_data.get("po_file_name"), 
                fileUrl_previwp=self.files_data.get("previous_wp"),
                fileUrl_p=self.files_data.get("procurement"),
                fileUrl_ap=self.files_data.get("ap_invoice"),
                fileUrl_previwp_pr=self.files_data.get("previous_wp_pr"),
                fileUrl_p_pr=self.files_data.get("procurement_pr")
            )
            period = self.processing_params.get('period', date_from_filename_str if date_from_filename_str != "000000" else 'NO_PERIOD')
            output_file_path = f"{period}-SPX-PO Compare Result.xlsx"
        else:
            self.logger.error(f"Unknown entity type in _execute_entity_processing: {self.entity_type}")
            return "UNKNOWN_ENTITY_TYPE_ERROR"

        if output_file_path: 
            self.logger.info(f"Processing for {self.entity_type} completed by worker, returning placeholder path: {output_file_path}")
            return output_file_path
        else:
            # This case implies a mode was matched but the processor didn't execute or no output_file_path was set.
            # Or a specific error string like "LOCKED_MODE_ERROR" or "NO_MATCHING_MODE" was already returned.
            # The previous specific returns handle explicit error strings. This is a fallback.
            self.logger.error(f"Output file path not set for entity: {self.entity_type}. This may indicate a mode was not matched or processor failed internally without setting an output path.")
            return "PROCESSOR_LOGIC_INCOMPLETE_OR_ERROR"

```
