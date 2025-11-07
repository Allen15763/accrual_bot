"""
build pipeline odj for main_pipeline.py
"""

from typing import Optional, Dict, List

from accrual_bot.utils.config import config_manager
from accrual_bot.core.pipeline import PipelineBuilder, Pipeline
from accrual_bot.core.pipeline import steps


def create_spt_po_complete_pipeline(file_paths: Dict[str, str]) -> Pipeline:
    """
    創建完整的 SPT PO 處理 Pipeline
    
    
    Args:
        file_paths: 文件路徑字典，包含:
            - raw_po: 原始 PO 文件
            - previous: 前期底稿 (PO)
            - procurement_po: 採購底稿 (PO)
            - ap_invoice: AP Invoice
            - previous_pr: 前期底稿 (PR)
            - procurement_pr: 採購底稿 (PR)
            - media系列
            
    Returns:
        Pipeline: 完整配置的 SPT PO Pipeline
    """
    
    # 創建 Pipeline Builder
    builder = PipelineBuilder("SPT_PO_Complete", "SPT")
    
    # 配置 Pipeline
    pipeline = (builder
                .with_description("Complete SPT PO processing pipeline - refactored from process()")
                .with_stop_on_error(False)  # 不要遇錯即停，收集所有錯誤
                #  ========== 階段 1: 數據載入 ==========
                .add_step(
                    steps.SPTDataLoadingStep(
                        name="Load_All_Data",
                        file_paths=file_paths,
                        required=True,
                        retry_count=1,  # 載入失敗重試1次
                        timeout=300.0   # 5分鐘超時
                    )
                )
        
                # ========== 階段 2: 數據準備與整合 ==========
                .add_step(
                    steps.ProductFilterStep(
                        name="Filter_Products",
                        product_pattern='(?i)SPX',
                        exclude=True,
                        required=True
                    )
                )
                .add_step(steps.ColumnAdditionStep(name="Add_Columns", required=True))
                .add_step(steps.APInvoiceIntegrationStep(name="Integrate_AP_Invoice", required=True))
                .add_step(steps.PreviousWorkpaperIntegrationStep(name="Integrate_Previous_WP", required=True))
                .add_step(steps.ProcurementIntegrationStep(name="Integrate_Procurement", required=True))
                
                # ========== 階段3: 業務邏輯 ==========
                .add_step(steps.CommissionDataUpdateStep(name="Update_Commission_Data", required=True))
                .add_step(steps.PayrollDetectionStep(name="Detect_Payroll_Records", required=True))
                .add_step(steps.DateLogicStep(name="Process_Dates", required=True))
                .add_step(steps.SPTERMLogicStep(name="Apply_ERM_Logic", required=True, retry_count=0))

                # ========== 階段4: 後處理 ==========
                .add_step(steps.SPTPostProcessingStep(
                    name="Post_Processing",
                    enable_statistics=True,
                    enable_validation=True,
                    required=True
                ))
                
                )
    
    return pipeline.build()

def create_spt_pr_complete_pipeline(file_paths: Dict[str, str]) -> Pipeline:
    """
    創建完整的 SPT PR 處理 Pipeline
    
    這是將原始 SPTPRProcessor.process() 方法完全重構後的版本
    
    Args:
        file_paths: 文件路徑字典，包含:
            - raw_po: 原始 PR 文件
            - previous_pr: 前期底稿 (PR)
            - procurement_pr: 採購底稿 (PR)
            - media系列
            
    Returns:
        Pipeline: 完整配置的 SPT PR Pipeline
    """
    
    # 創建 Pipeline Builder
    builder = PipelineBuilder("SPT_PR_Complete", "SPT")
    
    # 配置 Pipeline
    pipeline = (builder
                .with_description("Complete SPX PR processing pipeline - refactored from process()")
                .with_stop_on_error(False)  # 不要遇錯即停，收集所有錯誤
                #  ========== 階段 1: 數據載入 ==========
                .add_step(
                    steps.SPTPRDataLoadingStep(
                        name="Load_All_Data",
                        file_paths=file_paths,
                        required=True,
                        retry_count=2,  # 載入失敗重試2次
                        timeout=300.0   # 5分鐘超時
                    )
                )
        
                # ========== 階段 2: 數據準備與整合 ==========
                .add_step(
                    steps.ProductFilterStep(
                        name="Filter_Products",
                        product_pattern='(?i)SPX',
                        exclude=True,
                        required=True
                    )
                )
                .add_step(steps.ColumnAdditionStep(name="Add_Columns", required=True))
                .add_step(steps.PreviousWorkpaperIntegrationStep(name="Integrate_Previous_WP", required=True))
                .add_step(steps.ProcurementIntegrationStep(name="Integrate_Procurement", required=True))
                
                # # ========== 階段3: 業務邏輯 ==========
                .add_step(steps.CommissionDataUpdateStep(name="Update_Commission_Data", 
                                                         status_column="PR狀態", required=True))
                .add_step(steps.PayrollDetectionStep(name="Detect_Payroll_Records", required=True))
                .add_step(steps.DateLogicStep(name="Process_Dates", required=True))
                .add_step(steps.SPXPRERMLogicStep(name="Apply_ERM_Logic", required=True, retry_count=0))
                
                # # ========== 階段4: 後處理 ==========
                .add_step(steps.SPTPostProcessingStep(name="Reformat_Data", required=True))
                # ========== 階段 5: 導出結果 ==========
                .add_step(
                    steps.SPXPRExportStep(
                        name="Export_Results",
                        output_dir="output",              # 輸出目錄
                        sheet_name="PR",                  # Sheet 名稱
                        include_index=False,              # 不包含索引
                        required=True,                    # 必需步驟
                        retry_count=0                     # 失敗重試0次
                    )
                )
                )

    return pipeline.build()


# =============================================================================
# SPX組裝管道與執行
# =============================================================================
def create_spx_po_complete_pipeline(file_paths: Dict[str, str]) -> Pipeline:
    """
    創建完整的 SPX PO 處理 Pipeline
    
    這是將原始 SPXPOProcessor.process() 方法完全重構後的版本
    
    Args:
        file_paths: 文件路徑字典，包含:
            - raw_po: 原始 PO 文件
            - previous: 前期底稿 (PO)
            - procurement_po: 採購底稿 (PO)
            - ap_invoice: AP Invoice
            - previous_pr: 前期底稿 (PR)
            - procurement_pr: 採購底稿 (PR)
            - validation: OPS 驗收文件
            
    Returns:
        Pipeline: 完整配置的 SPX PO Pipeline
    """
    
    # 創建 Pipeline Builder
    builder = PipelineBuilder("SPX_PO_Complete", "SPX")
    
    # 配置 Pipeline
    pipeline = (builder
                .with_description("Complete SPX PO processing pipeline - refactored from process()")
                .with_stop_on_error(False)  # 不要遇錯即停，收集所有錯誤
                #  ========== 階段 1: 數據載入 ==========
                .add_step(
                    steps.SPXDataLoadingStep(
                        name="Load_All_Data",
                        file_paths=file_paths,
                        required=True,
                        retry_count=2,  # 載入失敗重試2次
                        timeout=300.0   # 5分鐘超時
                    )
                )
        
                # ========== 階段 2: 數據準備與整合 ==========
                .add_step(
                    steps.ProductFilterStep(
                        name="Filter_Products",
                        product_pattern='(?i)LG_SPX',
                        required=True
                    )
                )
                .add_step(steps.ColumnAdditionStep(name="Add_Columns", required=True))
                .add_step(steps.APInvoiceIntegrationStep(name="Integrate_AP_Invoice", required=True))
                .add_step(steps.PreviousWorkpaperIntegrationStep(name="Integrate_Previous_WP", required=True))
                .add_step(steps.ProcurementIntegrationStep(name="Integrate_Procurement", required=True))
                
                # ========== 階段3: 業務邏輯 ==========
                .add_step(steps.DateLogicStep(name="Process_Dates", required=True))
                .add_step(steps.ClosingListIntegrationStep(name="Integrate_Closing_List", required=True))
                .add_step(steps.StatusStage1Step(name="Evaluate_Status_Stage1", required=True))
                .add_step(steps.SPXERMLogicStep(name="Apply_ERM_Logic", required=True, retry_count=0))
                .add_step(steps.ValidationDataProcessingStep(name="Process_Validation", required=False))

                .add_step(steps.DepositStatusUpdateStep(
                    name="Update_Deposit_Status",
                    description_column="Item Description",
                    po_column="PO#",
                    date_column="Expected Received Month_轉換格式",
                    status_column="整單訂金與最大ERM核對",
                    deposit_keyword="訂金",
                    completed_status="該PO含訂金且ERM(最大)小於等於當期",
                    required=True
                ))

                # ========== 階段4: 後處理 ==========
                .add_step(steps.DataReformattingStep(name="Reformat_Data", required=True))
                .add_step(steps.SPXExportStep(name="Export_Results", output_dir="output", required=True))

                )
    
    return pipeline.build()

def create_spx_pr_complete_pipeline(file_paths: Dict[str, str]) -> Pipeline:
    """
    創建完整的 SPX PR 處理 Pipeline
    
    這是將原始 SPXPRProcessor.process() 方法完全重構後的版本
    
    Args:
        file_paths: 文件路徑字典，包含:
            - raw_po: 原始 PR 文件
            - previous_pr: 前期底稿 (PR)
            - procurement_pr: 採購底稿 (PR)
            
    Returns:
        Pipeline: 完整配置的 SPX PR Pipeline
    """
    
    # 創建 Pipeline Builder
    builder = PipelineBuilder("SPX_PR_Complete", "SPX")
    
    # 配置 Pipeline
    pipeline = (builder
                .with_description("Complete SPX PR processing pipeline - refactored from process()")
                .with_stop_on_error(False)  # 不要遇錯即停，收集所有錯誤
                #  ========== 階段 1: 數據載入 ==========
                .add_step(
                    steps.SPXPRDataLoadingStep(
                        name="Load_All_Data",
                        file_paths=file_paths,
                        required=True,
                        retry_count=2,  # 載入失敗重試2次
                        timeout=300.0   # 5分鐘超時
                    )
                )
        
                # ========== 階段 2: 數據準備與整合 ==========
                .add_step(
                    steps.ProductFilterStep(
                        name="Filter_Products",
                        product_pattern='(?i)LG_SPX',
                        required=True
                    )
                )
                .add_step(steps.ColumnAdditionStep(name="Add_Columns", required=True))
                .add_step(steps.PreviousWorkpaperIntegrationStep(name="Integrate_Previous_WP", required=True))
                .add_step(steps.ProcurementIntegrationStep(name="Integrate_Procurement", required=True))
                
                # # ========== 階段3: 業務邏輯 ==========
                .add_step(steps.DateLogicStep(name="Process_Dates", required=True))
                .add_step(steps.ClosingListIntegrationStep(name="Integrate_Closing_List", required=True))
                .add_step(steps.StatusStage1Step(name="Evaluate_Status_Stage1", required=True))
                .add_step(steps.SPXPRERMLogicStep(name="Apply_ERM_Logic", required=True, retry_count=0))
                
                # # ========== 階段4: 後處理 ==========
                .add_step(steps.PRDataReformattingStep(name="Reformat_Data", required=True))
                # ========== 階段 5: 導出結果 ========== (新增！)
                .add_step(
                    steps.SPXPRExportStep(
                        name="Export_Results",
                        output_dir="output",              # 輸出目錄
                        sheet_name="PR",                  # Sheet 名稱
                        include_index=False,              # 不包含索引
                        required=True,                    # 必需步驟
                        retry_count=0                     # 失敗重試0次
                    )
                )
                )

    return pipeline.build()

"""
PPE (Property, Plant & Equipment) 折舊期間計算步驟
用於 SPX 租金合約的折舊期間計算
"""
# =============================================================================
# 創建完整的 PPE Pipeline
# =============================================================================

def create_ppe_pipeline(contract_filing_list_url: str,
                        current_month: int) -> 'Pipeline':
    """
    創建完整的 PPE 處理 Pipeline
    
    Args:
        contract_filing_list_url: 合約歸檔清單檔案路徑
        current_month: 當前月份 (YYYYMM)
        
    Returns:
        Pipeline: 配置好的 Pipeline 實例
    """
    
    pipeline = (PipelineBuilder("PPE_Processing")
                .add_step(steps.PPEDataLoadingStep(
                    contract_filing_list_url=contract_filing_list_url
                ))
                .add_step(steps.PPEDataCleaningStep())
                .add_step(steps.PPEDataMergeStep(
                    merge_keys=config_manager.get_list(
                        'SPX', 
                        'key_for_merging_origin_and_renew_contract'
                    )
                ))
                .add_step(steps.PPEContractDateUpdateStep())
                .add_step(steps.PPEMonthDifferenceStep(
                    current_month=current_month
                ))
                .build())
    
    return pipeline
