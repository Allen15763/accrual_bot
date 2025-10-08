"""
改進版 SPX 數據載入步驟
使用統一的 datasources 模組，提供更好的架構設計

優勢:
1. 統一的數據源接口，易於擴展
2. 線程安全的並發讀取
3. 自動資源管理
4. 更好的錯誤處理
5. 支援多種數據格式
6. 支援每個文件獨立參數配置

文件位置: core/pipeline/steps/spx_po_steps.py
"""

import pandas as pd
import asyncio

from typing import Dict

from accrual_bot.core.pipeline import PipelineBuilder, Pipeline
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager


from accrual_bot.core.pipeline.steps.spx_loading import SPXDataLoadingStep

# =============================================================================
# 步驟 2: SPX 產品過濾步驟
# 替代原始: filter_spx_product_code()
# =============================================================================
from accrual_bot.core.pipeline.steps.common import ProductFilterStep
    
# =============================================================================
# 步驟 3: 添加欄位步驟
# 替代原始: add_cols()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_integration import ColumnAdditionStep

# =============================================================================
# 步驟 4: AP Invoice 整合步驟
# 替代原始: get_period_from_ap_invoice()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_integration import APInvoiceIntegrationStep

# =============================================================================
# 步驟 5: 前期底稿整合步驟
# 替代原始: judge_previous()
# =============================================================================
from accrual_bot.core.pipeline.steps.common import PreviousWorkpaperIntegrationStep

# =============================================================================
# 步驟 6: 採購底稿整合步驟
# 替代原始: judge_procurement()
# =============================================================================
from accrual_bot.core.pipeline.steps.common import ProcurementIntegrationStep

# =============================================================================
# 步驟 7: 日期邏輯處理步驟
# 替代原始: apply_date_logic()
# =============================================================================
from accrual_bot.core.pipeline.steps.common import DateLogicStep

# =============================================================================
# 步驟 8: 關單清單整合步驟
# 替代原始: get_closing_note() + partial give_status_stage_1()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_integration import ClosingListIntegrationStep
    
# =============================================================================
# 步驟 9: 第一階段狀態判斷步驟
# 替代原始: give_status_stage_1()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_evaluation import StatusStage1Step

# =============================================================================
# 步驟 10: ERM 邏輯步驟 (核心業務邏輯)
# 替代原始: erm()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_evaluation import SPXERMLogicStep

# =============================================================================
# 步驟 11: 驗收數據處理步驟
# 替代原始: process_validation_data() + apply_validation_data_to_po()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_integration import ValidationDataProcessingStep

# =============================================================================
# 步驟 12: 數據格式化和重組步驟
# 替代原始: reformate()、give_account_by_keyword()、is_installment()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_integration import DataReformattingStep

# =============================================================================
# 步驟 13: 導出步驟
# 替代原始: _save_output()
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_exporting import SPXExportStep

# =============================================================================
# 載入步驟使用範例
# =============================================================================

async def example_usage_for_loading():
    """範例：如何使用改進的 SPXDataLoadingStep (新格式 - 支援參數)"""
    import warnings
    warnings.filterwarnings('ignore')
    
    # 新格式：每個文件可以指定獨立參數
    file_paths = {
        'raw_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_purchase_order.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PO_FN.xlsx",
            'params': {'sheet_name': 0, 'header': 0, 'dtype': str, }
        },
        'procurement_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PO_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ap_invoice': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_AP_Invoice_Match_Monitoring_Ext.xlsx",
            'params': {}
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PR_FN.xlsx",
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ops_validation': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\SPX智取櫃及繳費機驗收明細(For FN)_2509.xlsx",
            'params': {
                'sheet_name': '智取櫃驗收明細',
                'header': 1,  # 第二行作為表頭
                'usecols': 'A:AE',
                # 'dtype': str, 
            }
        }
    }
    
    # 創建步驟
    step = SPXDataLoadingStep(
        name="Load_SPX_Data",
        file_paths=file_paths,
        required=True,
        timeout=300.0
    )
    
    # 創建上下文
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPX',
        processing_date=202509,
        processing_type='PO'
    )
    
    # 執行步驟
    result = await step.execute(context)
    
    # 檢查結果
    if result.is_success:
        print("✅ 載入成功！")
        print(f"   主數據: {len(context.data)} 行")
        print(f"   輔助數據: {len(context.list_auxiliary_data())} 個")
        print(f"   處理日期: {context.get_variable('processing_date')}")
        print("\n輔助數據列表:")
        for aux_name in context.list_auxiliary_data():
            aux_data = context.get_auxiliary_data(aux_name)
            if aux_data is not None and not aux_data.empty:
                print(f"   - {aux_name}: {len(aux_data)} 行")
            else:
                print(f"   - {aux_name}: 空")
        """
        How to get loaded dataset(DataFrame):
            - raw po csv: result.data

            - Others:
                - context.list_auxiliary_data()
                    - ['previous', 'procurement_po', 'ap_invoice', 'previous_pr', 
                    'procurement_pr', 'ops_validation', 'reference_account', 'reference_liability']
                - context.get_auxiliary_data('${name}')
        """
    else:
        print(f"❌ 載入失敗: {result.message}")


async def example_old_format():
    """範例：使用舊格式 (向後兼容)"""
    import warnings
    warnings.filterwarnings('ignore')
    
    # 舊格式：直接提供路徑字符串 (仍然支援)
    file_paths = {
        'raw_po': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_purchase_order.csv",
        'previous': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PO_FN.xlsx",
        'procurement_po': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PO_PQ.xlsx"
    }
    
    step = SPXDataLoadingStep(
        name="Load_SPX_Data",
        file_paths=file_paths
    )
    
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPX',
        processing_date=202509,
        processing_type='PO'
    )
    
    result = await step.execute(context)
    
    if result.is_success:
        print("✅ 舊格式載入成功！")

# =============================================================================
# 組裝管道與執行
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
                    SPXDataLoadingStep(
                        name="Load_All_Data",
                        file_paths=file_paths,
                        required=True,
                        retry_count=2,  # 載入失敗重試2次
                        timeout=300.0   # 5分鐘超時
                    )
                )
        
                # ========== 階段 2: 數據準備與整合 ==========
                .add_step(
                    ProductFilterStep(
                        name="Filter_Products",
                        product_pattern='(?i)LG_SPX',
                        required=True
                    )
                )
                .add_step(ColumnAdditionStep(name="Add_Columns", required=True))
                .add_step(APInvoiceIntegrationStep(name="Integrate_AP_Invoice", required=True))
                .add_step(PreviousWorkpaperIntegrationStep(name="Integrate_Previous_WP", required=True))
                .add_step(ProcurementIntegrationStep(name="Integrate_Procurement", required=True))
                
                # ========== 階段3: 業務邏輯 ==========
                .add_step(DateLogicStep(name="Process_Dates", required=True))
                .add_step(ClosingListIntegrationStep(name="Integrate_Closing_List", required=True))
                .add_step(StatusStage1Step(name="Evaluate_Status_Stage1", required=True))
                .add_step(SPXERMLogicStep(name="Apply_ERM_Logic", required=True, retry_count=0))
                .add_step(ValidationDataProcessingStep(name="Process_Validation", required=False))

                # ========== 階段4: 後處理 ==========
                .add_step(DataReformattingStep(name="Reformat_Data", required=True))
                .add_step(SPXExportStep(name="Export_Results", output_dir="output", required=True))

                )
    
    return pipeline.build()

async def execute_spx_po_pipeline(
    file_paths: Dict[str, str],
    processing_date: int,
    mode: str = "complete"
) -> Dict[str, any]:
    """
    執行 SPX PO Pipeline
    
    Args:
        file_paths: 文件路徑字典
        processing_date: 處理日期 (YYYYMM)
        mode: 執行模式 ("complete", "quick", "debug")
        
    Returns:
        Dict: 執行結果
    """
    
    # 根據模式創建 Pipeline
    if mode == "complete":
        pipeline = create_spx_po_complete_pipeline(file_paths)
    # elif mode == "quick":
    #     pipeline = create_spx_po_quick_pipeline(file_paths)
    # elif mode == "debug":
    #     pipeline = create_spx_po_debug_pipeline(file_paths)
    else:
        raise ValueError(f"Unknown mode: {mode}")
    
    # 創建處理上下文
    # 注意: data 為空 DataFrame，會由第一步載入
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPX",
        processing_date=processing_date,
        processing_type="PO"
    )
    
    # 執行 Pipeline
    result = await pipeline.execute(context)
    
    # 添加處理後的數據到結果
    if result['success']:
        
        result['output_data'] = context.data
        result['output_path'] = context.get_variable('output_path')
        if (result.get('successful_steps') - result.get('failed_steps') - result.get('skipped_steps') == 
           result.get('total_steps')):
            print("All successfully")
    
    return result


"""
PPE (Property, Plant & Equipment) 折舊期間計算步驟
用於 SPX 租金合約的折舊期間計算
"""
# =============================================================================
# 步驟 1: PPE 數據載入步驟
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_loading import PPEDataLoadingStep

# =============================================================================
# 步驟 2: PPE 數據清理與標準化步驟
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_integration import PPEDataCleaningStep

# =============================================================================
# 步驟 3: PPE 數據合併步驟
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_integration import PPEDataMergeStep

# =============================================================================
# 步驟 4: PPE 合約日期更新步驟
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_evaluation import PPEContractDateUpdateStep

# =============================================================================
# 步驟 5: PPE 月份差異計算步驟
# =============================================================================
from accrual_bot.core.pipeline.steps.spx_evaluation import PPEMonthDifferenceStep

# =============================================================================
# 快捷函數：創建完整的 PPE Pipeline
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
                .add_step(PPEDataLoadingStep(
                    contract_filing_list_url=contract_filing_list_url
                ))
                .add_step(PPEDataCleaningStep())
                .add_step(PPEDataMergeStep(
                    merge_keys=config_manager.get_list(
                        'SPX', 
                        'key_for_merging_origin_and_renew_contract'
                    )
                ))
                .add_step(PPEContractDateUpdateStep())
                .add_step(PPEMonthDifferenceStep(
                    current_month=current_month
                ))
                .build())
    
    return pipeline


if __name__ == "__main__":
    # 運行範例
    # print("=== 新格式 (支援參數) ===")
    # asyncio.run(example_usage_for_loading())
    
    # print("\n=== 舊格式 (向後兼容) ===")
    # asyncio.run(example_old_format())

    file_paths = {
        'raw_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_purchase_order.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PO_FN.xlsx",
            'params': {'sheet_name': 0, 'header': 0, 'dtype': str, }
        },
        'procurement_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PO_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ap_invoice': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_AP_Invoice_Match_Monitoring_Ext.xlsx",
            'params': {}
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PR_FN.xlsx",
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ops_validation': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\SPX智取櫃及繳費機驗收明細(For FN)_2509.xlsx",
            'params': {
                'sheet_name': '智取櫃驗收明細',
                'header': 1,  # 第二行作為表頭
                'usecols': 'A:AE',
                # 'dtype': str, 
            }
        }
    }
    
    # asyncio.run(execute_spx_po_pipeline(file_paths, 202509))
