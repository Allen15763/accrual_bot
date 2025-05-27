import logging
import os
import re
import pandas as pd
from typing import Dict, Any, TYPE_CHECKING, List, Tuple, Optional 
from datetime import datetime

if TYPE_CHECKING:
    from app_controller import AppController

from spttwpo import SPTTW_PO
from spttwpr import SPTTW_PR
from mobtwpr import MOBTW_PR
from mobtwpo import MOBTW_PO
from hris_dup import HRISDuplicateChecker
from upload_form import get_aggregation_twd, get_aggregation_foreign, get_entries
from utils import ReconEntryAmt , DataImporter 
from spxtwpo import SPXTW_PO


class MissingCriticalColumnError(ValueError): 
    def __init__(self, message, missing_columns: Optional[List[str]] = None):
        super().__init__(message)
        self.missing_columns = missing_columns if missing_columns else []


class TaskProcessor:
    def __init__(self, app_controller: 'AppController'):
        self.app_controller = app_controller
        self.logger = logging.getLogger(__name__)
        self.importer = DataImporter() 
        self.CORE_COLUMNS = { # 定義移到 __init__ 外部，作為類屬性
            "raw_data_po": ['PO#', 'Line#', 'PR#', 'Currency', 'Entry Amount', 'Entry Billed Amount', 'GL#', 'Item Description', 'Expected Receive Month', 'Closed For Invoice', 'Entry Quantity', 'Billed Quantity', 'PO Entry full invoiced status', 'Received Quantity'],
            "raw_data_pr": ['PR#', 'Line#', 'EBS Task', 'Item Description', 'GL#', 'Expected Receive Month', 'Product Code'],
            "previous_wp_po": ['PO Line', 'Remarked by FN_l', 'Remark by PR Team_l'],
            "previous_wp_pr": ['PR Line', 'Remarked by FN_l'],
            "procurement_wp_po": ['PO Line', 'PR Line', 'Remark by PR Team', 'Noted by PR'],
            "procurement_wp_pr": ['PR Line', 'Remarked by Procurement', 'Noted by Procurement'],
            "closing_list": ['PO#'], 
            "hris_pr": ["PR Number", "PR Line Number", "PO Number"],
            "hris_po": ["PO Number", "Line Number", "Quantity", "Unit Price"],
            "hris_ap": ["Invoice Number", "PO Number", "Invoice Amount"],
            "upload_form_wp": ['Subsidiary Ledger Account Code', 'Transaction Amount(Transaction Currency)', 'Transaction Currency Code', 'Account Description (Eng)', 'Company Code', 'Cost Centre', 'Product Code', 'Intercompany Code', 'Reserved Field 1 (Order number)', 'Text', 'Accounting Period'],
            "two_period_ac": ["PO No.", "PO Line No.", "Accrual Amount"], 
            "two_period_pr": ["PR No.", "PR Line No.", "PR Amount"], 
            "two_period_po": ["PO No.", "PO Line No.", "PO Amount"], 
            "spx_po_file": ['PO#', 'Line#', 'Currency', 'Entry Amount', 'GL#', 'Item Description', 'Expected Receive Month'],
            "spx_ap_invoice": ['PO Number', 'PO_LINE_NUMBER', 'Period'], 
        }
        self.logger.info("TaskProcessor initialized with core column definitions.")

    def _validate_df_columns(self, df: Optional[pd.DataFrame], required_columns: List[str], file_description: str) -> Tuple[bool, Optional[str]]:
        if df is None:
            msg = f"文件 '{file_description}' 未能加載或為空。"
            self.logger.error(msg)
            return False, msg
        if df.empty and required_columns:
             msg = f"文件 '{file_description}' 為空，但期望的欄位為: {', '.join(required_columns)}。"
             self.logger.error(msg)
             return False, msg
        actual_columns = df.columns.tolist()
        missing_cols = [col for col in required_columns if col not in actual_columns]
        if missing_cols:
            msg = f"文件 '{file_description}' 缺少核心欄位: {', '.join(missing_cols)}。"
            self.logger.error(msg)
            return False, msg
        self.logger.info(f"文件 '{file_description}' 的核心欄位驗證通過。")
        return True, None

    def _extract_user_friendly_error(self, e: Exception) -> str:
        """從異常中提取用戶友好的錯誤信息。"""
        if isinstance(e, MissingCriticalColumnError):
            return f"文件欄位錯誤: {str(e)}"
        elif isinstance(e, FileNotFoundError):
            return f"文件未找到: {str(e)}"
        elif isinstance(e, pd.errors.ParserError):
            return f"文件解析錯誤，請檢查文件格式是否正確 (例如CSV是否逗號分隔，Excel是否未損壞): {str(e)}"
        elif isinstance(e, KeyError):
             return f"處理數據時發生錯誤：找不到預期的欄位名 '{str(e)}'。請檢查輸入文件是否包含此欄位或欄位名是否正確。"
        # 可以根據需要添加更多特定異常的處理
        return f"發生未知錯誤: {str(e)}"


    def execute_task(self, task_type: str, params: Dict[str, Any]) -> Any:
        self.logger.info(f"開始執行任務: {task_type}，參數鍵值: {list(params.keys())}")
        module_for_ui = params.get("module", "main")
        
        # 從params中提取彈窗信息，如果不存在則使用默認值
        success_popup_details = params.get("success_popup_details", {"show_success_popup": True, "title": f"任務 '{task_type}' 完成"})
        error_popup_details = params.get("error_popup_details", {"show_error_popup": True, "title": f"任務 '{task_type}' 失敗"})

        result = None
        status_message = ""
        level = "info"

        try:
            # --- 主流程處理 (細化 task_type) ---
            if task_type.startswith("PROCESS_"):
                self.logger.info(f"進入主流程處理分支: {task_type}")
                result = self._process_main_entity_refined(task_type, params)
            # --- 其他特定任務 ---
            elif task_type == "hris_check":
                result = self._process_hris_check(params)
            elif task_type == "upload_form_main":
                result = self._process_upload_form_main(params)
            elif task_type == "two_period_check":
                result = self._process_two_period_check(params)
            elif task_type == "spx_process":
                result = self._process_spx_main(params)
            elif task_type == "spx_upload_form":
                result = self._process_spx_upload_form(params)
            else:
                self.logger.error(f"未知的任務類型: {task_type}")
                raise ValueError(f"未知的任務類型: {task_type}") # 由外層try-except捕獲

            # 成功處理
            status_message = result.get("message", f"任務 '{task_type}' 成功完成。") if isinstance(result, dict) else f"任務 '{task_type}' 成功完成。"
            level = "info"
            if success_popup_details and ("message" not in success_popup_details or not success_popup_details["message"]):
                success_popup_details["message"] = status_message
            self.app_controller._update_ui_status(status_message, level, module=module_for_ui, details=success_popup_details)
            self.logger.info(f"任務 '{task_type}' 成功完成。結果: {result}")

        except (MissingCriticalColumnError, ValueError, FileNotFoundError, pd.errors.ParserError) as e: #捕獲已知類型的業務或文件錯誤
            self.logger.error(f"執行任務 '{task_type}' 時發生錯誤: {str(e)}", exc_info=True) # 記錄完整堆棧
            status_message = self._extract_user_friendly_error(e)
            level = "error"
            if error_popup_details: error_popup_details["message"] = status_message
            self.app_controller._update_ui_status(status_message, level, module=module_for_ui, details=error_popup_details)
        except Exception as e: # 捕獲所有其他意外錯誤
            self.logger.critical(f"執行任務 '{task_type}' 時發生意外嚴重錯誤: {str(e)}", exc_info=True)
            status_message = self._extract_user_friendly_error(e) # 嘗試提取友好信息
            level = "error"
            if error_popup_details: error_popup_details["message"] = f"意外錯誤，請聯繫技術支持: {status_message}"
            self.app_controller._update_ui_status(f"任務 '{task_type}' 發生意外嚴重錯誤。", level, module=module_for_ui, details=error_popup_details)
        
        self.logger.info(f"任務 '{task_type}' 執行結束，最終狀態: {level}, 消息: {status_message}")
        return result


    def _process_main_entity_refined(self, task_type: str, params: Dict[str, Any]):
        self.logger.info(f"TaskProcessor: Refined main entity processing for {task_type}")
        raw_path = params["raw_data_path"]
        raw_fn = params["raw_data_filename"]
        
        # 這裡假設文件路徑已經在params中，並且是有效的
        # 實際的數據加載和驗證應該發生在調用此方法之前，或者在此方法開始時
        # 根據目前的設計，AppController準備了這些參數，TaskProcessor直接使用。

        processor_instance = None
        mode_to_call = None
        mode_params = [raw_path, raw_fn]

        try:
            if task_type.startswith("PROCESS_MOBA_PR_"):
                processor_instance = MOBTW_PR()
                mode = task_type.split('_')[-1]
                if mode == "1": mode_to_call = processor_instance.mode_1; mode_params.extend([params["procurement_wp_path"], params["closing_list_path"]])
                elif mode == "2": mode_to_call = processor_instance.mode_2; mode_params.append(params["closing_list_path"])
                elif mode == "3": mode_to_call = processor_instance.mode_3; mode_params.append(params["procurement_wp_path"])
                elif mode == "4": mode_to_call = processor_instance.mode_4
                elif mode == "5_PREV_PROC": mode_to_call = processor_instance.mode_5; mode_params.extend([params["procurement_wp_path"], params["previous_wp_path"]])
                elif mode == "5_PREV_ONLY": mode_to_call = processor_instance.mode_5; mode_params.append(params["previous_wp_path"])
                else: raise ValueError(f"未知的 MOBA_PR 模式: {mode}")
            
            elif task_type.startswith("PROCESS_MOBA_PO_"):
                processor_instance = MOBTW_PO()
                mode = task_type.split('_')[-1]
                if mode == "2": mode_to_call = processor_instance.mode_2; mode_params.extend([params["previous_wp_path"], params["procurement_wp_path"]])
                elif mode == "4": mode_to_call = processor_instance.mode_4; mode_params.extend([params["procurement_wp_path"], params["closing_list_path"]])
                elif mode == "5": mode_to_call = processor_instance.mode_5; mode_params.append(params["procurement_wp_path"])
                elif mode == "6": mode_to_call = processor_instance.mode_6; mode_params.append(params["previous_wp_path"])
                elif mode == "7": mode_to_call = processor_instance.mode_7; mode_params.append(params["closing_list_path"])
                elif mode == "8": mode_to_call = processor_instance.mode_8
                else: raise ValueError(f"未知的 MOBA_PO 模式: {mode}")

            elif task_type.startswith("PROCESS_SPT_PR_"):
                processor_instance = SPTTW_PR()
                mode = task_type.split('_')[-1]
                if mode == "1": mode_to_call = processor_instance.mode_1; mode_params.extend([params["procurement_wp_path"], params["closing_list_path"]])
                elif mode == "2": mode_to_call = processor_instance.mode_2; mode_params.append(params["closing_list_path"])
                elif mode == "3": mode_to_call = processor_instance.mode_3; mode_params.append(params["procurement_wp_path"])
                elif mode == "4": mode_to_call = processor_instance.mode_4
                elif mode == "5_PREV_PROC": mode_to_call = processor_instance.mode_5; mode_params.extend([params["procurement_wp_path"], params["previous_wp_path"]])
                elif mode == "5_PREV_ONLY": mode_to_call = processor_instance.mode_5; mode_params.append(params["previous_wp_path"])
                else: raise ValueError(f"未知的 SPT_PR 模式: {mode}")

            elif task_type.startswith("PROCESS_SPT_PO_"):
                processor_instance = SPTTW_PO()
                mode = task_type.split('_')[-1]
                if mode == "2": mode_to_call = processor_instance.mode_2; mode_params.extend([params["previous_wp_path"], params["procurement_wp_path"]])
                elif mode == "4": mode_to_call = processor_instance.mode_4; mode_params.extend([params["procurement_wp_path"], params["closing_list_path"]])
                elif mode == "5": mode_to_call = processor_instance.mode_5; mode_params.append(params["procurement_wp_path"])
                elif mode == "6": mode_to_call = processor_instance.mode_6; mode_params.append(params["previous_wp_path"])
                elif mode == "7": mode_to_call = processor_instance.mode_7; mode_params.append(params["closing_list_path"])
                elif mode == "8": mode_to_call = processor_instance.mode_8
                else: raise ValueError(f"未知的 SPT_PO 模式: {mode}")
            else:
                raise ValueError(f"TaskProcessor: 無法處理的主流程任務類型 '{task_type}'")

            self.logger.info(f"調用處理器 {processor_instance.__class__.__name__} 的模式 {mode_to_call.__name__ if mode_to_call else 'N/A'} 參數數量: {len(mode_params)}")
            if mode_to_call:
                mode_to_call(*mode_params) # 調用選定的模式方法
            else: # Should not happen if logic is correct
                 raise ValueError(f"未能為 {task_type} 確定有效的處理方法。")

            self.logger.info(f"TaskProcessor: {task_type} 處理邏輯已執行。")
            return {"status": "success", "message": f"{task_type} 處理完成"}
        except Exception as e_proc: # Capture errors from processor.mode_X calls
            self.logger.error(f"執行 {task_type} 內部處理時出錯: {str(e_proc)}", exc_info=True)
            raise # Re-throw to be caught by execute_task's main try-except


    def _process_hris_check(self, params: Dict[str, Any]):
        self.logger.info(f"TaskProcessor: HRIS檢查，參數: {list(params.keys())}")
        pr_path = params.get("pr_path"); po_path = params.get("po_path"); ap_path = params.get("ap_path")
        
        # File existence checks (can be enhanced with _validate_df_columns after loading)
        if not all(os.path.exists(p) for p in [pr_path, po_path, ap_path] if p):
             missing = [os.path.basename(p or "路徑未提供") for p in [pr_path, po_path, ap_path] if not p or not os.path.exists(p or "")]
             raise FileNotFoundError(f"HRIS檢查：一個或多個文件不存在: {', '.join(missing)}")

        try:
            df_pr = pd.read_excel(pr_path, dtype=str)
            is_valid, err = self._validate_df_columns(df_pr, self.CORE_COLUMNS["hris_pr"], os.path.basename(pr_path)); 
            if not is_valid: raise MissingCriticalColumnError(err)
            
            df_po = pd.read_excel(po_path, dtype=str)
            is_valid, err = self._validate_df_columns(df_po, self.CORE_COLUMNS["hris_po"], os.path.basename(po_path)); 
            if not is_valid: raise MissingCriticalColumnError(err)

            df_ap = pd.read_excel(ap_path, dtype=str, header=1, sheet_name=1)
            is_valid, err = self._validate_df_columns(df_ap, self.CORE_COLUMNS["hris_ap"], os.path.basename(ap_path)); 
            if not is_valid: raise MissingCriticalColumnError(err)

            self.logger.info(f"HRIS 檢查：成功讀取並驗證文件。")
        except Exception as e_read:
            self.logger.error(f"HRIS 檢查讀取文件時出錯: {str(e_read)}", exc_info=True)
            raise # Re-throw to be caught by execute_task

        try:
            checker = HRISDuplicateChecker()
            df_pr_p = checker.check_duplicates_in_po(df_pr, df_po)
            df_pr_p, df_po_p = checker.check_duplicates_in_ap(df_pr_p, df_po, df_ap)
            df_pr_p, df_po_p = checker.relocate_columns(df_pr_p, df_po_p)
            checker.save_files(df_pr_p, df_po_p)
            return {"status": "success", "message": "HRIS重複檢查完成,結果已保存"}
        except Exception as e_hris:
            self.logger.error(f"HRIS 核心邏輯執行失敗: {str(e_hris)}", exc_info=True)
            raise # Re-throw


    def _process_upload_form_main(self, params: Dict[str, Any]):
        self.logger.info(f"TaskProcessor: 生成主模塊Upload Form，參數: {list(params.keys())}")
        wp_file_path = params.get("wp_file_path"); entity = params.get("entity"); period = params.get("period") 
        ac_date_str = params.get("ac_date"); category = params.get("category"); accountant = params.get("accountant")
        currency = params.get("currency")
        if not wp_file_path or not os.path.exists(wp_file_path): raise FileNotFoundError(f"工作底稿文件 '{os.path.basename(wp_file_path or 'N/A')}' 不存在。")
        
        # Validate working paper columns before processing
        # This assumes get_aggregation_twd/foreign does not read specific sheets but works on a pre-loaded df.
        # If they read sheets, validation must happen after that. For now, assume path is used directly.
        # No, get_aggregation_twd takes path, so validation of resulting df is needed.

        try:
            m_date = datetime.strptime(ac_date_str, '%Y/%m/%d').date(); ac_period = datetime.strftime(m_date, '%Y/%m')
            self.logger.debug(f"Upload Form (Main) 參數: Entity={entity}, Period={period}, AcDate={ac_date_str}, Currency={currency}, WP={wp_file_path}")
            if entity == 'MOBTW':
                dfs = get_aggregation_twd(wp_file_path, ac_period) 
            elif entity == 'SPTTW':
                dfs = get_aggregation_twd(wp_file_path, ac_period, is_mob=False)
            else: raise ValueError(f"不支持的實體類型: {entity} for Upload Form (Main)")

            is_valid, err_msg = self._validate_df_columns(dfs, self.CORE_COLUMNS["upload_form_wp"], f"聚合數據 ({entity} - {currency})")
            if not is_valid: raise MissingCriticalColumnError(err_msg)

            result_df = get_entries(dfs, entity, period, ac_date_str, category, accountant, currency)
            output_file = f'Upload Form-{entity}-{period[:3]}-{currency}.xlsx'
            result_df.to_excel(output_file, index=False)
            return {"status": "success", "message": f"Upload Form已生成: {output_file}", "output_file": output_file}
        except Exception as e_upload:
            self.logger.error(f"生成主模塊 Upload Form 核心邏輯失敗: {str(e_upload)}", exc_info=True)
            raise


    def _process_two_period_check(self, params: Dict[str, Any]):
        self.logger.info(f"TaskProcessor: 兩期檢查，參數: {list(params.keys())}")
        pr_file_path = params.get("pr_file_path"); po_file_path = params.get("po_file_path"); ac_file_path = params.get("ac_file_path")
        if not all(os.path.exists(p) for p in [pr_file_path, po_file_path, ac_file_path] if p):
            missing = [os.path.basename(p or "") for p in [pr_file_path, po_file_path, ac_file_path] if not p or not os.path.exists(p or "")]
            raise FileNotFoundError(f"兩期檢查：一個或多個文件不存在: {', '.join(missing)}")
        
        # Add validation for dataframes loaded by ReconEntryAmt if possible, or ensure ReconEntryAmt handles it.
        # For now, assume ReconEntryAmt.get_difference is robust or handles its own validation.
        try:
            a, b, c = ReconEntryAmt.get_difference(ac_file_path, pr_file_path, po_file_path)
            output_df = pd.DataFrame({**a, **b, **c}, index=[0]).T
            output_file = 'check_dif_amount.xlsx'
            output_df.to_excel(output_file)
            return {"status": "success", "message": f"兩期檢查完成，結果已保存到 {output_file}", "output_file": output_file}
        except Exception as e_check2:
            self.logger.error(f"兩期檢查核心邏輯失敗: {str(e_check2)}", exc_info=True)
            raise

    def _process_spx_main(self, params: Dict[str, Any]):
        self.logger.info(f"TaskProcessor: SPX主流程處理，參數: {list(params.keys())}")
        po_file_path = params.get("po_file"); po_file_name = params.get("po_file_name") 
        ap_invoice_path = params.get("ap_invoice") # This is passed from AppController based on DAL
        
        if not po_file_path or not os.path.exists(po_file_path): raise FileNotFoundError(f"SPX PO 文件 '{os.path.basename(po_file_path or 'N/A')}' 不存在。")
        if not ap_invoice_path or not os.path.exists(ap_invoice_path): raise FileNotFoundError(f"SPX AP Invoice 文件 '{os.path.basename(ap_invoice_path or 'N/A')}' 不存在。")
        
        # Validation of data loaded by SPXTW_PO().process would ideally be inside SPXTW_PO or after each load step.
        # For now, assume SPXTW_PO().process handles its internal data loading and validation.
        try:
            processor = SPXTW_PO()
            processor.process(
                fileUrl=po_file_path, file_name=po_file_name,
                fileUrl_previwp=params.get("previous_wp"), fileUrl_p=params.get("procurement"),       
                fileUrl_ap=ap_invoice_path, fileUrl_previwp_pr=params.get("previous_wp_pr"), 
                fileUrl_p_pr=params.get("procurement_pr")        
            )
            return {"status": "success", "message": "SPX數據處理完成！"}
        except Exception as e_spx_proc:
            self.logger.error(f"SPX 主流程核心邏輯失敗: {str(e_spx_proc)}", exc_info=True)
            raise

    def _process_spx_upload_form(self, params: Dict[str, Any]):
        self.logger.info(f"TaskProcessor: SPX Upload Form 生成，參數: {list(params.keys())}")
        po_file_path = params.get("po_file_path"); entity = params.get("entity", "SPXTW")
        period_display = params.get("period_display"); period_str = params.get("period_str")
        accounting_date = params.get("accounting_date"); category = params.get("category"); user = params.get("user")
        if not po_file_path or not os.path.exists(po_file_path): raise FileNotFoundError(f"SPX PO 文件 (用於Upload Form) '{os.path.basename(po_file_path or 'N/A')}' 不存在。")

        try:
            dfs = get_aggregation_twd(po_file_path, period_str, is_mob=False) 
            is_valid, err_msg = self._validate_df_columns(dfs, self.CORE_COLUMNS["upload_form_wp"], f"{entity} Aggregated Data for SPX Upload")
            if not is_valid: raise MissingCriticalColumnError(err_msg)
            
            result_df = get_entries(dfs, entity, period_display, accounting_date, category, user, "TWD")
            output_file = f'Upload Form-{entity}-{period_display}-TWD.xlsx'
            result_df.to_excel(output_file, index=False)
            return {"status": "success", "message": f"SPX Upload Form已生成: {output_file}", "output_file": output_file}
        except Exception as e_spx_upload:
            self.logger.error(f"SPX Upload Form 核心邏輯失敗: {str(e_spx_upload)}", exc_info=True)
            raise

# __main__ block (remains for testing, might need adjustments)
if __name__ == '__main__':
    class MockAppController: 
        def __init__(self):
            self.imported_files = {}; self.spx_imported_files = {}
            self.logger = logging.getLogger("MockAppControllerForTaskProc")
            self.config = ConfigManager()
        def _update_ui_status(self, message: str, level: str = "info", module: str = "main", details: Dict = None):
            self.logger.info(f"MockUI Status ({module}): [{level.upper()}] {message} {details or ''}")
        def _log_message(self, message: str, level: str = "info", module: str = "main"): 
            self.logger.info(f"MockUI Log ({module}): [{level.upper()}] {message}")
        def get_file_path(self, file_type: str) -> str | None: return self.imported_files.get(file_type, {}).get("path")
        def get_file_name(self, file_type: str) -> str | None: return self.imported_files.get(file_type, {}).get("name")
        def get_spx_file_path(self, file_key: str) -> str | None: return self.spx_imported_files.get(file_key, {}).get("path")
        def get_spx_file_name(self, file_key: str) -> str | None: return self.spx_imported_files.get(file_key, {}).get("name")

    mock_controller_tp = MockAppController()
    processor_tp = TaskProcessor(mock_controller_tp)
    # ... (rest of the __main__ block for testing) ...
```
