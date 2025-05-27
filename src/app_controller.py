import logging
import os
import re
from typing import List, Dict, Any, Optional # Added Optional

from task_processor import TaskProcessor
from data_access_layer import DAL # 導入DAL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AppController:
    def __init__(self, ui_callback=None, db_path: Optional[str] = None): # Added db_path for DAL
        # 移除 self.imported_files 和 self.spx_imported_files
        # self.imported_files: Dict[str, Dict[str, str]] = {}
        # self.spx_imported_files: Dict[str, Dict[str, str]] = {}
        self.ui_callback = ui_callback
        self.dal = DAL(db_path=db_path) # 實例化DAL
        self._initialize_dependencies()
        logging.info("AppController initialized with DAL.")

    def _initialize_dependencies(self):
        self.task_processor = TaskProcessor(self)
        logging.info("TaskProcessor initialized within AppController.")

    def _format_files_for_ui(self, db_files: List[Dict[str, Any]]) -> List[str]:
        """將從DAL獲取的文件記錄列表格式化為UI列表框期望的字符串列表。"""
        formatted_list = []
        for record in db_files:
            # 例如: "原始資料 (POPR): popr_202312.csv"
            # DAL 中的 'ui_file_type' 存儲的是類似 "原始資料 (POPR)" 的文本
            # 'internal_file_key' 存儲的是 "raw_data"
            # 'original_file_name' 存儲的是 "popr_202312.csv"
            formatted_list.append(f"{record['ui_file_type']}: {record['original_file_name']}")
        return formatted_list

    # --- File Handling Methods (Modified to use DAL) ---
    def register_imported_file(self, ui_file_type_display: str, internal_file_key: str, file_path: str) -> bool:
        """
        註冊主模塊文件到資料庫。
        :param ui_file_type_display: UI上顯示的文件類型描述 (e.g., "原始資料 (POPR)")
        :param internal_file_key: 程序內部使用的唯一鍵 (e.g., "raw_data")
        :param file_path: 文件實際路徑
        """
        if not file_path or not os.path.exists(file_path):
            self._log_message(f"無效或不存在的文件路徑: {file_path} (類型: {internal_file_key})", "error")
            self._update_ui_status(f"錯誤：文件 '{os.path.basename(file_path)}' 不存在。", "error", module="main",
                                 details={"show_error_popup": True, "title": "導入失敗", "message": f"文件 '{os.path.basename(file_path)}' 不存在。"})
            return False
        
        original_file_name = os.path.basename(file_path)
        
        # 將文件信息存儲到DAL
        file_id = self.dal.add_imported_file(
            ui_file_type=ui_file_type_display, # UI顯示的類型
            internal_file_key=internal_file_key, # 內部鍵
            file_path=file_path,
            original_file_name=original_file_name,
            related_entity='main' # 標記為'main'實體
        )

        if file_id is not None:
            self._log_message(f"主模塊文件已存儲到DB: Key='{internal_file_key}', Path='{file_path}'", "info")
            self._update_ui_status(f"已成功導入 '{ui_file_type_display}' 文件: {original_file_name}", "info", module="main")
            # 更新UI列表
            main_files_from_db = self.dal.get_imported_files_by_entity('main')
            if self.ui_callback:
                self.ui_callback(type="file_list_update", module="main", files=self._format_files_for_ui(main_files_from_db))
            return True
        else:
            self._log_message(f"主模塊文件存儲到DB失敗: Key='{internal_file_key}'", "error")
            self._update_ui_status(f"導入 '{ui_file_type_display}' 文件失敗。", "error", module="main",
                                 details={"show_error_popup": True, "title": "導入失敗", "message": f"無法將文件 '{original_file_name}' 信息存入資料庫。"})
            return False

    def clear_imported_files(self): # Renamed from clear_all_imported_files
        if self.dal.clear_imported_files_by_entity('main'):
            self._log_message("所有主模塊已導入文件記錄已從DB清除。", "info")
            self._update_ui_status("已清除所有主模塊的導入文件記錄。", "info", module="main")
            if self.ui_callback:
                self.ui_callback(type="file_list_update", module="main", files=[]) # 發送空列表以清除UI列表
        else:
            self._log_message("從DB清除主模塊文件記錄失敗。", "error")
            self._update_ui_status("清除主模塊文件記錄失敗。", "error", module="main")


    def get_file_record(self, internal_file_key: str, related_entity: str = 'main') -> Optional[Dict[str, Any]]:
        """從DAL獲取單個文件記錄（字典）。"""
        # DAL的get_imported_file_path只返回路徑，我們需要更完整的記錄或多個記錄
        # 假設我們需要根據 internal_file_key 和 related_entity 獲取唯一記錄
        files = self.dal.get_imported_files_by_entity(related_entity)
        for f in files:
            if f['internal_file_key'] == internal_file_key:
                return f
        return None

    def get_file_path(self, internal_file_key: str) -> Optional[str]: # related_entity 默認為 'main'
        record = self.get_file_record(internal_file_key, 'main')
        return record['file_path'] if record else None

    def get_file_name(self, internal_file_key: str) -> Optional[str]: # related_entity 默認為 'main'
        record = self.get_file_record(internal_file_key, 'main')
        return record['original_file_name'] if record else None

    # --- SPX Tab File Handling (Modified to use DAL) ---
    def spx_register_file(self, ui_file_type_display: str, internal_file_key: str, file_path: str) -> bool:
        if not file_path or not os.path.exists(file_path):
            self._log_message(f"SPX：無效或不存在的文件路徑: {file_path} (類型: {internal_file_key})", "error")
            self._update_ui_status(f"SPX錯誤：文件 '{os.path.basename(file_path)}' 不存在。", "error", module="spx",
                                 details={"show_error_popup": True, "title": "SPX導入失敗", "message": f"文件 '{os.path.basename(file_path)}' 不存在。"})
            return False
        
        original_file_name = os.path.basename(file_path)
        file_id = self.dal.add_imported_file(
            ui_file_type=ui_file_type_display,
            internal_file_key=internal_file_key, # SPX 文件也需要 internal_file_key
            file_path=file_path,
            original_file_name=original_file_name,
            related_entity='spx'
        )
        if file_id is not None:
            self._log_message(f"SPX文件已存儲到DB: Key='{internal_file_key}', Path='{file_path}'", "info")
            self._update_ui_status(f"SPX已導入 '{ui_file_type_display}' 文件: {original_file_name}", "info", module="spx")
            spx_files_from_db = self.dal.get_imported_files_by_entity('spx')
            if self.ui_callback:
                self.ui_callback(type="file_list_update", module="spx", files=self._format_files_for_ui(spx_files_from_db))
            return True
        else:
            self._log_message(f"SPX文件存儲到DB失敗: Key='{internal_file_key}'", "error")
            self._update_ui_status(f"SPX導入 '{ui_file_type_display}' 文件失敗。", "error", module="spx",
                                 details={"show_error_popup": True, "title": "SPX導入失敗", "message": f"無法將SPX文件 '{original_file_name}' 信息存入資料庫。"})
            return False

    def spx_clear_all_files(self):
        if self.dal.clear_imported_files_by_entity('spx'):
            self._log_message("所有SPX模塊已導入文件記錄已從DB清除。", "info")
            self._update_ui_status("已清除所有SPX模塊的導入文件記錄。", "info", module="spx")
            if self.ui_callback:
                self.ui_callback(type="file_list_update", module="spx", files=[])
        else:
            self._log_message("從DB清除SPX模塊文件記錄失敗。", "error")
            self._update_ui_status("清除SPX模塊文件記錄失敗。", "error", module="spx")
    
    def get_spx_file_path(self, internal_file_key: str) -> Optional[str]:
        record = self.get_file_record(internal_file_key, 'spx')
        return record['file_path'] if record else None

    def get_spx_file_name(self, internal_file_key: str) -> Optional[str]:
        record = self.get_file_record(internal_file_key, 'spx')
        return record['original_file_name'] if record else None

    # --- Processing Methods (Adjusted to use DAL for file info) ---
    def start_main_processing(self, params: Dict[str, Any]):
        entity_type = params.get("entity_type")
        self._log_message(f"AppController: 請求主處理流程，實體類型: {entity_type}", "info")
        self._update_ui_status(f"主要處理流程已啟動 ({entity_type})...", "info", module="main")

        raw_data_record = self.get_file_record("raw_data", "main")

        if not raw_data_record or not raw_data_record.get("path"):
            msg = "錯誤：未從資料庫中找到已導入的原始數據文件。處理中止。"
            self._log_message(msg, "error")
            self._update_ui_status(msg, "error", module="main", details={"show_error_popup": True, "title": "主流程錯誤", "message": msg})
            return

        raw_data_path = raw_data_record["path"]
        raw_data_filename = raw_data_record["original_file_name"]
        year_month = None
        if raw_data_filename:
            match = re.match(r'(\d{6})', raw_data_filename)
            if match: year_month = match.group(1)
        
        if not year_month:
            msg = f"錯誤：無法從原始數據文件名 '{raw_data_filename}' 中提取年月 (YYYYMM)。"
            self._log_message(msg, "error")
            self._update_ui_status(msg, "error", module="main", details={"show_error_popup": True, "title": "主流程錯誤", "message": msg})
            return

        # 獲取所有主模塊相關文件，以確定模式
        all_main_files = self.dal.get_imported_files_by_entity('main')
        main_files_dict = {f['internal_file_key']: f for f in all_main_files} # For easy lookup
        items_present = set(main_files_dict.keys())

        specific_task_type = None
        task_specific_params = {
            "raw_data_path": raw_data_path,
            "raw_data_filename": raw_data_filename,
            "year_month": year_month,
            "module": "main"
        }

        # --- Logic to determine specific_task_type and its params ---
        # This logic now uses main_files_dict to get paths for other files
        if entity_type == 'MOBA_PR' or entity_type == 'SPT_PR':
            prefix = "PROCESS_" + entity_type
            if "previous_wp" in items_present:
                task_specific_params["previous_wp_path"] = main_files_dict["previous_wp"]["path"]
                if "procurement_wp" in items_present and "closing_list" in items_present:
                    self._update_ui_status(f"錯誤: {entity_type} - 提供了過多文件組合", "error", module="main", details={"show_error_popup": True, "title": f"{entity_type}錯誤"}); return
                elif "procurement_wp" in items_present:
                    specific_task_type = f"{prefix}_MODE_5_PREV_PROC"
                    task_specific_params["procurement_wp_path"] = main_files_dict["procurement_wp"]["path"]
                elif "closing_list" in items_present:
                    self._update_ui_status(f"錯誤: {entity_type} - 不支持的組合 (原始, 前期, 關單)", "error", module="main", details={"show_error_popup": True, "title": f"{entity_type}錯誤"}); return
                else: specific_task_type = f"{prefix}_MODE_5_PREV_ONLY"
            elif "closing_list" in items_present and "procurement_wp" in items_present:
                specific_task_type = f"{prefix}_MODE_1"
                task_specific_params["procurement_wp_path"] = main_files_dict["procurement_wp"]["path"]
                task_specific_params["closing_list_path"] = main_files_dict["closing_list"]["path"]
            elif "procurement_wp" in items_present:
                specific_task_type = f"{prefix}_MODE_3"
                task_specific_params["procurement_wp_path"] = main_files_dict["procurement_wp"]["path"]
            elif "closing_list" in items_present:
                specific_task_type = f"{prefix}_MODE_2"
                task_specific_params["closing_list_path"] = main_files_dict["closing_list"]["path"]
            else: specific_task_type = f"{prefix}_MODE_4"
        # ... (similar logic for MOBA_PO, SPT_PO) ...
        elif entity_type == 'MOBA_PO' or entity_type == 'SPT_PO':
            prefix = "PROCESS_" + entity_type 
            if "previous_wp" in items_present and "procurement_wp" in items_present and "closing_list" in items_present:
                self._update_ui_status(f"錯誤: {entity_type} - 過多文件組合", "error", module="main", details={"show_error_popup": True, "title": f"{entity_type}錯誤"}); return
            elif "previous_wp" in items_present and "procurement_wp" in items_present:
                specific_task_type = f"{prefix}_MODE_2"
                task_specific_params["previous_wp_path"] = main_files_dict["previous_wp"]["path"]
                task_specific_params["procurement_wp_path"] = main_files_dict["procurement_wp"]["path"]
            elif "previous_wp" in items_present and "closing_list" in items_present:
                 self._update_ui_status(f"錯誤: {entity_type} - 不支持的組合 (原始, 前期, 關單)", "error", module="main", details={"show_error_popup": True, "title": f"{entity_type}錯誤"}); return
            elif "procurement_wp" in items_present and "closing_list" in items_present:
                specific_task_type = f"{prefix}_MODE_4"
                task_specific_params["procurement_wp_path"] = main_files_dict["procurement_wp"]["path"]
                task_specific_params["closing_list_path"] = main_files_dict["closing_list"]["path"]
            elif "procurement_wp" in items_present:
                specific_task_type = f"{prefix}_MODE_5"
                task_specific_params["procurement_wp_path"] = main_files_dict["procurement_wp"]["path"]
            elif "previous_wp" in items_present:
                specific_task_type = f"{prefix}_MODE_6"
                task_specific_params["previous_wp_path"] = main_files_dict["previous_wp"]["path"]
            elif "closing_list" in items_present:
                specific_task_type = f"{prefix}_MODE_7"
                task_specific_params["closing_list_path"] = main_files_dict["closing_list"]["path"]
            else: specific_task_type = f"{prefix}_MODE_8"
        else:
            self._log_message(f"錯誤: AppController 無法識別的實體類型 '{entity_type}'", "error")
            self._update_ui_status(f"錯誤: 無法識別的實體類型 '{entity_type}'", "error", module="main", details={"show_error_popup": True, "title": "主流程錯誤"}); return

        if not specific_task_type:
            self._log_message(f"錯誤: 未能為實體 '{entity_type}' 和文件 '{items_present}' 確定具體的任務類型。", "error")
            self._update_ui_status(f"錯誤: 未能確定 '{entity_type}' 的具體處理模式。", "error", module="main", details={"show_error_popup": True, "title": "主流程錯誤"}); return

        task_specific_params["success_popup_details"] = {"show_success_popup": True, "title": f"{entity_type} 處理完成", "message": f"{specific_task_type} 成功完成。"}
        task_specific_params["error_popup_details"] = {"show_error_popup": True, "title": f"{entity_type} 處理失敗"}

        try:
            self.task_processor.execute_task(specific_task_type, task_specific_params)
        except Exception as e:
            self._log_message(f"AppController: 主處理任務 '{specific_task_type}' 執行時發生意外: {str(e)}", "error")
            self._update_ui_status(f"主處理過程中發生意外錯誤: {str(e)}", "error", module="main", details=task_specific_params["error_popup_details"])
            logging.exception(f"AppController: 主處理詳細錯誤 ({specific_task_type}):")

    # --- Other processing methods (start_hris_check, etc.) remain structurally similar, ---
    # --- they already delegate to TaskProcessor with specific task types and params. ---
    # --- The main change is that TaskProcessor will use AppController's DAL-backed get_file_path if needed, ---
    # --- or AppController can pass all necessary paths in params directly. ---
    # --- For now, these methods pass paths directly as they did before. ---
    def start_hris_check(self, pr_path: str, po_path: str, ap_path: str):
        self._log_message("AppController: 請求 HRIS 重複檢查...", "info")
        self._update_ui_status("HRIS 重複檢查處理中...", "info", module="main")
        task_params = {
            "pr_path": pr_path, "po_path": po_path, "ap_path": ap_path, "module": "main",
            "success_popup_details": {"show_success_popup": True, "title": "HRIS完成", "message": "HRIS重複檢查完成,結果已保存"},
            "error_popup_details": {"show_error_popup": True, "title": "HRIS檢查失敗"}
        }
        try: self.task_processor.execute_task("hris_check", task_params)
        except Exception as e: self._log_message(f"AppController: HRIS 檢查失敗: {str(e)}", "error"); logging.exception("AppController: HRIS 檢查詳細錯誤:")

    def generate_upload_form(self, params: Dict[str, Any]):
        self._log_message("AppController: 請求生成 Upload Form...", "info")
        self._update_ui_status("生成 Upload Form 處理中...", "info", module="main")
        task_params = params.copy()
        task_params["module"] = params.get("module", "main")
        task_params["success_popup_details"] = {"show_success_popup": True, "title": "Upload Form 完成", "message": f"Upload Form 已成功生成: Upload Form-{params.get('entity')}-{params.get('period', '')[:3]}-{params.get('currency')}.xlsx"}
        task_params["error_popup_details"] = {"show_error_popup": True, "title": "Upload Form 生成失敗"}
        try: self.task_processor.execute_task("upload_form_main", task_params)
        except Exception as e: self._log_message(f"AppController: Upload Form 生成失敗: {str(e)}", "error"); logging.exception("AppController: Upload Form 生成詳細錯誤:")

    def start_two_period_check(self, pr_file_path: str, po_file_path: str, ac_file_path: str):
        self._log_message("AppController: 請求兩期檢查...", "info")
        self._update_ui_status("兩期檢查處理中...", "info", module="main")
        task_params = {
            "pr_file_path": pr_file_path, "po_file_path": po_file_path, "ac_file_path": ac_file_path, "module": "main",
            "success_popup_details": {"show_success_popup": True, "title": "兩期檢查完成", "message": "兩期檢查完成，結果已保存到 check_dif_amount.xlsx"},
            "error_popup_details": {"show_error_popup": True, "title": "兩期檢查失敗"}
        }
        try: self.task_processor.execute_task("two_period_check", task_params)
        except Exception as e: self._log_message(f"AppController: 兩期檢查失敗: {str(e)}", "error"); logging.exception("AppController: 兩期檢查詳細錯誤:")

    def spx_start_processing(self, params: Dict[str, Any]):
        self._log_message("AppController: 請求 SPX 處理流程...", "info")
        self._update_ui_status("SPX 處理中...", "info", module="spx")
        task_params = params.copy()
        task_params["module"] = "spx"
        
        # Populate file paths from DAL for SPX
        task_params["po_file"] = self.get_spx_file_path("po_file")
        task_params["po_file_name"] = self.get_spx_file_name("po_file") # TaskProcessor expects this
        task_params["ap_invoice"] = self.get_spx_file_path("ap_invoice")
        task_params["previous_wp"] = self.get_spx_file_path("previous_wp")
        task_params["procurement"] = self.get_spx_file_path("procurement")
        task_params["previous_wp_pr"] = self.get_spx_file_path("previous_wp_pr")
        task_params["procurement_pr"] = self.get_spx_file_path("procurement_pr")

        # Check if essential files for SPX processing are present
        if not task_params["po_file"] or not task_params["ap_invoice"]:
            msg = "SPX錯誤：PO文件或AP發票文件未導入。"
            self._log_message(msg, "error")
            self._update_ui_status(msg, "error", module="spx", details={"show_error_popup": True, "title": "SPX錯誤", "message": msg})
            return

        task_params["success_popup_details"] = {"show_success_popup": True, "title": "SPX處理完成", "message": "SPX數據處理完成！"}
        task_params["error_popup_details"] = {"show_error_popup": True, "title": "SPX處理失敗"}
        try: self.task_processor.execute_task("spx_process", task_params)
        except Exception as e: self._log_message(f"AppController: SPX 處理失敗: {str(e)}", "error"); logging.exception("AppController: SPX 處理詳細錯誤:")

    def spx_start_export_upload_form(self, params: Dict[str, Any]):
        self._log_message("AppController: 請求 SPX Upload Form 生成...", "info")
        self._update_ui_status("SPX Upload Form 生成中...", "info", module="spx")
        task_params = params.copy()
        task_params["module"] = "spx"
        
        # Ensure po_file_path is correctly sourced from DAL if not already in params from UI
        if "po_file_path" not in task_params or not task_params["po_file_path"]:
            task_params["po_file_path"] = self.get_spx_file_path("po_file")

        if not task_params["po_file_path"]:
            msg = "SPX Upload Form錯誤：PO文件未導入。"
            self._log_message(msg, "error")
            self._update_ui_status(msg, "error", module="spx", details={"show_error_popup": True, "title": "SPX Upload Form錯誤", "message": msg})
            return

        task_params["success_popup_details"] = {"show_success_popup": True, "title": "SPX Upload Form 完成", "message": f"SPX Upload Form 已成功生成: Upload Form-{params.get('entity')}-{params.get('period_display')}-TWD.xlsx"}
        task_params["error_popup_details"] = {"show_error_popup": True, "title": "SPX Upload Form 生成失敗"}
        try: self.task_processor.execute_task("spx_upload_form", task_params)
        except Exception as e: self._log_message(f"AppController: SPX Upload Form 生成失敗: {str(e)}", "error"); logging.exception("AppController: SPX Upload Form 生成詳細錯誤:")

    # --- UI Callback Helper Methods ---
    def _update_ui_status(self, message: str, level: str = "info", module: str = "main", details: Optional[Dict] = None):
        payload = {"type": "status", "message": message, "level": level, "module": module}
        if details: payload.update(details)
        if self.ui_callback:
            try: self.ui_callback(**payload)
            except Exception as e: logging.error(f"Failed to update UI status: {e}")
        log_level_map = {"error": logging.ERROR, "warning": logging.WARNING, "info": logging.INFO} # Python 3.10: use logging.getLevelNameMapping()
        actual_log_level = log_level_map.get(level.lower(), logging.INFO)
        logging.log(actual_log_level, f"UI Status ({module}): {message}")


    def _log_message(self, message: str, level: str = "info"):
        log_level_map = {"error": logging.ERROR, "warning": logging.WARNING, "info": logging.INFO, "debug": logging.DEBUG}
        actual_log_level = log_level_map.get(level.lower(), logging.INFO)
        logging.log(actual_log_level, message)
        if self.ui_callback:
            try: self.ui_callback(type="log", message=message, level=level)
            except Exception as e: logging.error(f"Failed to log to UI: {e}")
```
