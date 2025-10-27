"""
SPX Pipeline Checkpoint 系統
解決測試時每次都要重跑耗時步驟的問題

功能:
1. 儲存 pipeline 執行的中間狀態
2. 從指定步驟恢復執行
3. 快速測試後續步驟

使用方式:
    # 首次執行 - 自動儲存 checkpoint
    result = await execute_with_checkpoint(file_paths, 202509)
    
    # 從特定步驟恢復
    result = await resume_from_step(
        checkpoint_name="SPX_202509_after_Filter_SPX_Products",
        start_from="Add_Columns"
    )
"""
import sys
import os
import time
from pathlib import Path

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
# TEST NEW MODULE
import asyncio
import pandas as pd

from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline import Pipeline


class CheckpointManager:
    """Pipeline Checkpoint 管理器"""
    
    def __init__(self, checkpoint_dir: str = "./checkpoints"):
        """
        初始化 Checkpoint 管理器
        
        Args:
            checkpoint_dir: checkpoint 儲存目錄
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def save_checkpoint(
        self,
        context: ProcessingContext,
        step_name: str,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        儲存 checkpoint
        
        Args:
            context: 處理上下文
            step_name: 步驟名稱
            metadata: 額外的元數據
            
        Returns:
            str: checkpoint 名稱
        """
        # 生成 checkpoint 名稱
        entity_type = context.metadata.entity_type or "unknown"
        processing_date = context.metadata.processing_date or "unknown"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        checkpoint_name = f"{entity_type}_{processing_date}_after_{step_name}"
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        checkpoint_path.mkdir(parents=True, exist_ok=True)
        
        # 儲存主數據
        if context.data is not None and not context.data.empty:
            data_path = checkpoint_path / "data.parquet"
            context.data.to_parquet(data_path, index=False)
        
        # 儲存輔助數據
        aux_data_dir = checkpoint_path / "auxiliary_data"
        aux_data_dir.mkdir(exist_ok=True)
        
        for aux_name in context.list_auxiliary_data():
            aux_data = context.get_auxiliary_data(aux_name)
            if aux_data is not None and not aux_data.empty:
                aux_path = aux_data_dir / f"{aux_name}.parquet"
                try:
                    if 'ops_validation' in aux_name:
                        aux_data['discount'] = aux_data['discount'].astype(str)
                    else:
                        aux_data.to_parquet(aux_path, index=False)
                except Exception as err:
                    print(f"ERROR exporting parquet on {aux_name}")
        
        # 儲存變數和元數據
        checkpoint_info = {
            'step_name': step_name,
            'entity_type': context.metadata.entity_type,
            'processing_date': context.metadata.processing_date,
            'processing_type': context.metadata.processing_type,
            'variables': context._variables,
            'warnings': context.warnings,
            'errors': context.errors,
            'timestamp': timestamp,
            'auxiliary_data_list': context.list_auxiliary_data(),
            'metadata': metadata or {}
        }
        
        with open(checkpoint_path / "checkpoint_info.json", 'w', encoding='utf-8') as f:
            json.dump(checkpoint_info, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"✅ Checkpoint 已儲存: {checkpoint_name}")
        return checkpoint_name
    
    def load_checkpoint(self, checkpoint_name: str) -> ProcessingContext:
        """
        載入 checkpoint
        
        Args:
            checkpoint_name: checkpoint 名稱
            
        Returns:
            ProcessingContext: 恢復的上下文
        """
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Checkpoint 不存在: {checkpoint_name}")
        
        # 載入元數據
        with open(checkpoint_path / "checkpoint_info.json", 'r', encoding='utf-8') as f:
            info = json.load(f)
        
        # 載入主數據
        data_path = checkpoint_path / "data.parquet"
        if data_path.exists():
            data = pd.read_parquet(data_path)
        else:
            data = pd.DataFrame()
        
        # 創建上下文
        context = ProcessingContext(
            data=data,
            entity_type=info['entity_type'],
            processing_date=info['processing_date'],
            processing_type=info['processing_type']
        )
        
        # 恢復變數
        for key, value in info['variables'].items():
            context.set_variable(key, value)
        
        # 恢復輔助數據
        aux_data_dir = checkpoint_path / "auxiliary_data"
        if aux_data_dir.exists():
            for aux_file in aux_data_dir.glob("*.parquet"):
                aux_name = aux_file.stem
                aux_data = pd.read_parquet(aux_file)
                context.add_auxiliary_data(aux_name, aux_data)
        
        print(f"✅ Checkpoint 已載入: {checkpoint_name}")
        print(f"   - 主數據: {len(context.data)} 行")
        print(f"   - 輔助數據: {len(context.list_auxiliary_data())} 個")
        print(f"   - 變數: {len(context._variables)} 個")
        
        return context
    
    def list_checkpoints(self) -> List[Dict]:
        """列出所有可用的 checkpoint"""
        checkpoints = []
        
        for checkpoint_path in self.checkpoint_dir.iterdir():
            if checkpoint_path.is_dir():
                info_file = checkpoint_path / "checkpoint_info.json"
                if info_file.exists():
                    with open(info_file, 'r', encoding='utf-8') as f:
                        info = json.load(f)
                    checkpoints.append({
                        'name': checkpoint_path.name,
                        'step': info['step_name'],
                        'date': info['processing_date'],
                        'timestamp': info['timestamp']
                    })
        
        return sorted(checkpoints, key=lambda x: x['timestamp'], reverse=True)
    
    def delete_checkpoint(self, checkpoint_name: str):
        """刪除指定的 checkpoint"""
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        if checkpoint_path.exists():
            import shutil
            shutil.rmtree(checkpoint_path)
            print(f"✅ Checkpoint 已刪除: {checkpoint_name}")


class PipelineWithCheckpoint:
    """
    帶 Checkpoint 功能的 Pipeline 執行器
    """
    
    def __init__(self, pipeline: Pipeline, checkpoint_manager: CheckpointManager):
        self.pipeline = pipeline
        self.checkpoint_manager = checkpoint_manager
    
    async def execute_with_checkpoint(
        self,
        context: ProcessingContext,
        save_after_each_step: bool = True,
        start_from_step: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        執行 Pipeline 並自動儲存 checkpoint
        
        Args:
            context: 處理上下文
            save_after_each_step: 是否在每個步驟後儲存 checkpoint
            start_from_step: 從哪個步驟開始執行 (None = 從頭開始)
            
        Returns:
            Dict: 執行結果
        """
        # 找到起始步驟的索引
        start_index = 0
        if start_from_step:
            for i, step in enumerate(self.pipeline.steps):
                if step.name == start_from_step:
                    start_index = i
                    print(f"🔄 從步驟 '{start_from_step}' 開始執行 (跳過前 {i} 個步驟)")
                    break
            else:
                raise ValueError(f"找不到步驟: {start_from_step}")
        
        # 執行步驟
        results = []
        for i, step in enumerate(self.pipeline.steps[start_index:], start=start_index):
            print(f"\n{'='*60}")
            print(f"執行步驟 {i+1}/{len(self.pipeline.steps)}: {step.name}")
            print(f"{'='*60}")
            
            # 執行步驟
            result = await step.execute(context)
            results.append(result)
            
            # 儲存 checkpoint
            if save_after_each_step and result.is_success:
                self.checkpoint_manager.save_checkpoint(
                    context=context,
                    step_name=step.name,
                    metadata={
                        'step_index': i,
                        'step_status': result.status.value,
                        'step_message': result.message
                    }
                )
            
            # 如果失敗且設定為遇錯即停
            if not result.is_success and self.pipeline.config.stop_on_error:
                print(f"❌ 步驟失敗,停止執行: {result.message}")
                break
        
        # 彙總結果
        successful = sum(1 for r in results if r.is_success)
        failed = sum(1 for r in results if not r.is_success and not r.is_skipped)
        # skipped = sum(1 for r in results if r.is_skipped)
        
        return {
            'success': failed == 0,
            'total_steps': len(results),
            'successful_steps': successful,
            'failed_steps': failed,
            # 'skipped_steps': skipped,
            'results': results,  # List[StepResult]
            'context': context
        }


# =============================================================================
# 便捷函數
# =============================================================================

async def execute_with_checkpoint(
    file_paths: Dict[str, str],
    processing_date: int,
    checkpoint_dir: str = "./checkpoints",
    save_checkpoints: bool = True,
    processing_type: str = 'PO'
) -> Dict[str, Any]:
    """
    執行完整 pipeline 並自動儲存 checkpoint
    
    Args:
        file_paths: 文件路徑字典
        processing_date: 處理日期
        checkpoint_dir: checkpoint 儲存目錄
        save_checkpoints: 是否儲存 checkpoint
        
    Returns:
        Dict: 執行結果
    """
    from accrual_bot.core.pipeline.steps.spx_po_steps import create_spx_po_complete_pipeline  # 替換成實際路徑
    
    # 創建 pipeline 和 checkpoint manager
    pipeline = create_spx_po_complete_pipeline(file_paths)
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    
    # 創建上下文
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPX",
        processing_date=processing_date,
        processing_type=processing_type
    )
    
    # 執行
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    result = await executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints
    )
    
    return result

async def execute_pr_with_checkpoint(
    file_paths: Dict[str, str],
    processing_date: int,
    checkpoint_dir: str = "./checkpoints",
    save_checkpoints: bool = True,
    processing_type: str = 'PR'
) -> Dict[str, Any]:
    """
    執行完整 pipeline 並自動儲存 checkpoint
    
    Args:
        file_paths: 文件路徑字典
        processing_date: 處理日期
        checkpoint_dir: checkpoint 儲存目錄
        save_checkpoints: 是否儲存 checkpoint
        
    Returns:
        Dict: 執行結果
    """
    from accrual_bot.core.pipeline.steps.spx_steps import create_spx_pr_complete_pipeline  # 替換成實際路徑
    
    # 創建 pipeline 和 checkpoint manager
    pipeline = create_spx_pr_complete_pipeline(file_paths)
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    
    # 創建上下文
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPX",
        processing_date=processing_date,
        processing_type=processing_type
    )
    
    # 執行
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    result = await executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints
    )
    
    return result

async def execute_ppe_with_checkpoint(
    file_paths: str,
    processing_date: int,
    checkpoint_dir: str = "./checkpoints",
    save_checkpoints: bool = True
) -> Dict[str, Any]:
    """
    執行完整 pipeline 並自動儲存 checkpoint
    
    Args:
        file_paths: 文件路徑字串
        processing_date: 處理日期
        checkpoint_dir: checkpoint 儲存目錄
        save_checkpoints: 是否儲存 checkpoint
        
    Returns:
        Dict: 執行結果
    """
    from accrual_bot.core.pipeline.steps.spx_po_steps import create_ppe_pipeline  # 替換成實際路徑
    
    # 創建 pipeline 和 checkpoint manager
    pipeline = create_ppe_pipeline(file_paths, processing_date)
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    
    # 創建上下文
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPX",
        processing_date=processing_date,
        processing_type="PO"
    )
    
    # 執行
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    result = await executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints
    )
    
    return result


async def resume_from_step(
    checkpoint_name: str,
    start_from_step: str,
    file_paths: Optional[Dict[str, str]] = None,
    checkpoint_dir: str = "./checkpoints"
) -> Dict[str, Any]:
    """
    從 checkpoint 恢復並從指定步驟開始執行
    
    Args:
        checkpoint_name: checkpoint 名稱
        start_from_step: 從哪個步驟開始
        file_paths: 文件路徑 (如果需要重建 pipeline)
        checkpoint_dir: checkpoint 目錄
        
    Returns:
        Dict: 執行結果
    """
    from accrual_bot.core.pipeline.steps.spx_po_steps import create_spx_po_complete_pipeline  # 替換成實際路徑
    
    # 載入 checkpoint
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    context = checkpoint_manager.load_checkpoint(checkpoint_name)
    
    # 重建 pipeline (使用原始 file_paths 或從 context 獲取)
    if file_paths is None:
        # 嘗試從 context 中獲取文件路徑
        file_paths = context.get_variable('file_paths', {})
        if not file_paths:
            raise ValueError("無法獲取文件路徑,請提供 file_paths 參數")
    
    pipeline = create_spx_po_complete_pipeline(file_paths)
    
    # 執行
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    result = await executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=True,
        start_from_step=start_from_step
    )
    
    return result


async def quick_test_step(
    checkpoint_name: str,
    step_to_test: str,
    checkpoint_dir: str = "./checkpoints"
) -> Dict[str, Any]:
    """
    快速測試單一步驟 (從上一個 checkpoint 恢復)
    
    Args:
        checkpoint_name: checkpoint 名稱
        step_to_test: 要測試的步驟名稱
        checkpoint_dir: checkpoint 目錄
        
    Returns:
        Dict: 執行結果
    """
    return await resume_from_step(
        checkpoint_name=checkpoint_name,
        start_from_step=step_to_test,
        checkpoint_dir=checkpoint_dir
    )


# =============================================================================
# 使用範例
# =============================================================================

async def example_usage():
    """使用範例"""
    
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
    
    # ========== 情境 1: 首次執行,自動儲存 checkpoint ==========
    print("情境 1: 首次執行")
    result = await execute_with_checkpoint(
        file_paths=file_paths,
        processing_date=202509,
        save_checkpoints=True
    )
    
    # ========== 情境 2: 查看可用的 checkpoint ==========
    print("\n情境 2: 查看 checkpoints")
    checkpoint_manager = CheckpointManager()
    checkpoints = checkpoint_manager.list_checkpoints()
    for cp in checkpoints:
        print(f"  - {cp['name']} (步驟: {cp['step']}, 時間: {cp['timestamp']})")
    
    # ========== 情境 3: 從特定步驟恢復 (前面步驟已測試完成) ==========
    print("\n情境 3: 從 Add_Columns 步驟開始")
    result = await resume_from_step(
        checkpoint_name="SPX_202509_after_Filter_SPX_Products",
        start_from_step="Add_Columns",
        file_paths=file_paths  # 可選,如果 checkpoint 中沒有
    )
    
    # ========== 情境 4: 快速測試某個步驟 ==========
    print("\n情境 4: 快速測試 AP Invoice Integration")
    result = await quick_test_step(
        checkpoint_name="SPX_202509_after_Add_Columns",
        step_to_test="Integrate_AP_Invoice"
    )
    
    # ========== 情境 5: 刪除舊的 checkpoint ==========
    print("\n情境 5: 清理舊 checkpoints")
    checkpoint_manager.delete_checkpoint("SPX_202509_after_Load_All_Data")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    # asyncio.run(example_usage())

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

    # file_paths = {
    #     'raw_po': {
    #         'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_purchase_order.csv",
    #         'params': {'encoding': 'utf-8', 
    #                    'sep': ',', 
    #                    'dtype': str, 
    #                    'keep_default_na': False, 
    #                    'na_values': ['']
    #                    }
    #     },
    #     'previous': {
    #         'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202507_PO_FN.xlsx",
    #         'params': {'sheet_name': 0, 'header': 0, 'dtype': str, }
    #     },
    #     'procurement_po': {
    #         'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_PO_PQ.xlsx",
    #         'params': {'dtype': str, }
    #     },
    #     'ap_invoice': {
    #         'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\AP_Invoice_Match_Monitoring_Ext (NEW).xlsx",
    #         'params': {}
    #     },
    #     'previous_pr': {
    #         'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202507_PR_FN.xlsx",
    #         'params': {'dtype': str, }
    #     },
    #     'procurement_pr': {
    #         'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_PR_PQ.xlsx",
    #         'params': {'dtype': str, }
    #     },
    #     'ops_validation': {
    #         'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\SPX智取櫃及繳費機驗收明細(For FN)_2508_修復.xlsx",
    #         'params': {
    #             'sheet_name': '智取櫃驗收明細',
    #             'header': 1,  # 第二行作為表頭
    #             'usecols': 'A:AE',
    #             # 'dtype': str, 
    #         }
    #     }
    # }
    
    # Run all steps
    # result = asyncio.run(execute_with_checkpoint(
    #     file_paths=file_paths,
    #     processing_date=202509,
    #     save_checkpoints=True
    # ))

    # Start from specific point
    # result = asyncio.run(resume_from_step(
    #     checkpoint_name="SPX_202509_after_Filter_SPX_Products",    # checkpoint資料夾路徑名稱
    #     start_from_step="Add_Columns",
    #     # checkpoint_name="SPX_202509_after_Process_Dates",    # checkpoint資料夾路徑名稱
    #     # start_from_step="Integrate_Closing_List",
    #     file_paths=file_paths  # 可選,如果 checkpoint 中沒有
    # ))

    # 從特定步驟開始，跟resume_from_step類似
    # result = asyncio.run(quick_test_step(
    #     checkpoint_name="SPX_202509_after_Add_Columns",
    #     step_to_test="Integrate_AP_Invoice"
    # ))

    # Run PPE steps
    # result = asyncio.run(execute_ppe_with_checkpoint(
    #     file_paths=r'G:\共用雲端硬碟\INT_TWN_SEA_FN_Shared_Resources\00_Temp_Internal_share\SPX\租金\SPX租金合約歸檔清單及匯款狀態_marge1.xlsx',
    #     processing_date=202509,
    #     save_checkpoints=True
    # ))

    # Run PR
    file_paths_pr = {
        'raw_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_purchase_request.xlsx",
            'params': {'encoding': 'utf-8', 
                       'sep': ',', 
                       'dtype': str, 
                       'keep_default_na': False, 
                       'na_values': ['']
                       }
        },
        'previous_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202508_PR_FN.xlsx",  # xxx_改欄名，暫不需要
            'params': {'dtype': str, }
        },
        'procurement_pr': {
            'path': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202509\202509_PR_PQ.xlsx",
            'params': {'dtype': str, }
        },

    }
    result = asyncio.run(execute_pr_with_checkpoint(
        file_paths=file_paths_pr,
        processing_date=202509,
        save_checkpoints=False
    ))
    
    print(1)
