import sys
import os
from pathlib import Path

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from pathlib import Path
# TEST NEW MODULE
import asyncio
import pandas as pd

from accrual_bot.core.pipeline import (
    execute_pipeline_with_checkpoint,
    resume_from_step
)

    
async def run_spx_pr_full_pipeline():
    
    file_paths_pr = {
        'raw_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_purchase_request_20260201_092128.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\前期底稿\SPX\202511_PR_FN.xlsx",  # xxx_改欄名，暫不需要
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },

    }
    
    from accrual_bot.core.pipeline.build_pipelines import create_spx_pr_complete_pipeline

    result = await execute_pipeline_with_checkpoint(
        file_paths=file_paths_pr,
        processing_date=202512,
        pipeline_func=create_spx_pr_complete_pipeline,
        entity='SPX',
        processing_type="PR",
        save_checkpoints=False
    )

    # timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    # result.get('context').data.to_excel(f'./output/SPX_PR_202510_processed_{timestamp}.xlsx', index=False)
    # result.get('context').get_auxiliary_data('result_with_temp_cols')
    
    print(f"導出狀態: {result['success']}")
    print(f"輸出路徑: {result['context'].get_variable('pr_export_output_path')}")
    return result


async def run_spx_po_full_pipeline():
    from accrual_bot.core.pipeline.build_pipelines import create_spx_po_complete_pipeline
    
    file_paths = {
        'raw_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_purchase_order_20260201_092408.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\前期底稿\SPX\202511_PO_FN.xlsx",  # ..._改小寫，也行
            'params': {'sheet_name': 0, 'header': 0, 'dtype': str, }
        },
        'procurement_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_PO_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ap_invoice': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_AP_Invoice_Match_Monitoring_Ext.xlsx",
            'params': {}
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\前期底稿\SPX\202511_PR_FN.xlsx",
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ops_validation': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\SPX智取櫃及繳費機驗收明細(For FN_2512).xlsx",
            'params': {
                'sheet_name': '智取櫃驗收明細',
                'header': 3,  # 第二行作為表頭
                'usecols': 'A:AH',  # 同上
                # 'dtype': str, 
                'kiosk_sheet_name': '繳費機驗收明細',
                'kiosk_usecols': 'A:G',

            }
        }
    }
    
    result = await execute_pipeline_with_checkpoint(
        file_paths=file_paths,
        processing_date=202512,
        pipeline_func=create_spx_po_complete_pipeline,
        entity='SPX',
        processing_type="PO",
        save_checkpoints=True
    )

    return result


async def run_spx_ppe_full_pipeline():
    from accrual_bot.core.pipeline.build_pipelines import create_ppe_pipeline

    result = await execute_pipeline_with_checkpoint(
        file_paths=r'G:\共用雲端硬碟\INT_TWN_SEA_FN_Shared_Resources\00_Temp_Internal_share\SPX\租金\SPX租金合約歸檔清單及匯款狀態_marge1.xlsx',
        processing_date=202512,
        pipeline_func=create_ppe_pipeline,
        entity='SPX',
        processing_type="PPE",
        save_checkpoints=True
    )

    return result


async def run_spt_po_full_pipeline():
    from accrual_bot.core.pipeline.build_pipelines import create_spt_po_complete_pipeline
    file_paths = {
        'raw_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_purchase_order_20260201_092408.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\前期底稿\202511_PO_FN.xlsx",
            'params': {'sheet_name': 0, 'header': 0, 'dtype': str, }
        },
        'procurement_po': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_PO_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'ap_invoice': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_AP_Invoice_Match_Monitoring_Ext.xlsx",
            'params': {}
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\前期底稿\202511_PR_FN.xlsx",
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'media_finished': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202511\SPT\Sea TV program coverage list-2025.xlsx",
            'params': {'dtype': str, 'sheet_name': '2024-2025使用完畢'}
        },
        'media_left': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202511\SPT\Sea TV program coverage list-2025.xlsx",
            'params': {'dtype': str, 'sheet_name': '2024-25新聞剩餘量'}
        },
        'media_summary': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202511\SPT\Sea TV program coverage list-2025.xlsx",
            'params': {'dtype': str, 'sheet_name': '2024-2025新聞總表(已用記錄)'}
        },
    }

    result: dict = await execute_pipeline_with_checkpoint(
        file_paths=file_paths,
        processing_date=202512,
        pipeline_func=create_spt_po_complete_pipeline,
        entity='SPT',
        save_checkpoints=True,
        processing_type='PO'
    )
    return result

async def run_spt_pr_full_pipeline():
    from accrual_bot.core.pipeline.build_pipelines import create_spt_pr_complete_pipeline
    file_paths = {
        'raw_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_purchase_request_20260201_092128.csv",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\前期底稿\202511_PR_FN.xlsx",
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\Original Data\202512_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },
        'media_finished': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202511\SPT\Sea TV program coverage list-2025.xlsx",
            'params': {'dtype': str, 'sheet_name': '2024-2025使用完畢'}
        },
        'media_left': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202511\SPT\Sea TV program coverage list-2025.xlsx",
            'params': {'dtype': str, 'sheet_name': '2024-25新聞剩餘量'}
        },
        'media_summary': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202511\SPT\Sea TV program coverage list-2025.xlsx",
            'params': {'dtype': str, 'sheet_name': '2024-2025新聞總表(已用記錄)'}
        },
    }

    result: dict = await execute_pipeline_with_checkpoint(
        file_paths=file_paths,
        processing_date=202512,
        pipeline_func=create_spt_pr_complete_pipeline,
        entity='SPT',
        save_checkpoints=False,
        processing_type='PR'
    )
    return result


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    # asyncio.run(example_usage())

    # Run all steps
    result = asyncio.run(run_spx_po_full_pipeline())
    
    # Start from specific point
    # from accrual_bot.core.pipeline.build_pipelines import create_spx_po_complete_pipeline  # 替換成實際路徑
    # result = asyncio.run(resume_from_step(
    #     checkpoint_name="SPX_PO_202511_after_Filter_Products",    # checkpoint資料夾路徑名稱
    #     start_from_step="Add_Columns",
    #     pipeline_func=create_spx_po_complete_pipeline,
    #     save_checkpoints=False
    # ))

    # 從特定步驟開始，跟resume_from_step類似
    # result = asyncio.run(quick_test_step(
    #     checkpoint_name="SPX_202509_after_Add_Columns",
    #     step_to_test="Integrate_AP_Invoice"
    # ))

    # Run PPE steps
    result = asyncio.run(run_spx_ppe_full_pipeline())
    result.get('context').data.to_excel(r'C:\SEA\Accrual\prpo_bot\resources\頂一下\202512\PPE\年限表_202512.xlsx', index=False)

    # Run PR
    # result = asyncio.run(run_spx_pr_full_pipeline())

    # Run SPT
    # result = asyncio.run(run_spt_po_full_pipeline())

    # from accrual_bot.core.pipeline.build_pipelines import create_spt_po_complete_pipeline  # 替換成實際路徑
    # result = asyncio.run(resume_from_step(
    #     checkpoint_name="SPT_PO_202511_after_Filter_Products",    # checkpoint資料夾路徑名稱
    #     start_from_step="Add_Columns",                            # 下一步的名稱(在pipeline物件中的)
    #     pipeline_func=create_spt_po_complete_pipeline,
    #     save_checkpoints=False
    # ))

    # result = asyncio.run(run_spt_pr_full_pipeline())

    print(1)
