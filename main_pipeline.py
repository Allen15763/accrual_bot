"""
main_pipeline.py - Accrual Bot 主入口點

使用方式:
1. 修改 config/run_config.toml 設定要執行的 pipeline
2. 按 F5 (VS Code) 或執行 python main_pipeline.py

支援功能:
- 逐步執行模式 (step_by_step = true)
- Checkpoint 儲存與恢復
- 多實體/多處理類型切換
"""

import sys
import asyncio
import warnings
from pathlib import Path
from typing import Any, Dict

import pandas as pd

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from accrual_bot.core.pipeline import ProcessingContext
from accrual_bot.runner import load_run_config, load_file_paths, StepByStepExecutor
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)


def get_orchestrator(entity: str):
    """
    根據實體類型取得對應的 Orchestrator

    Args:
        entity: 業務實體類型 (SPX, SPT, MOB)

    Returns:
        Pipeline Orchestrator 實例

    Raises:
        ValueError: 不支援的實體類型
    """
    orchestrators = {
        "SPT": SPTPipelineOrchestrator,
        "SPX": SPXPipelineOrchestrator,
        # "MOB": MOBPipelineOrchestrator,  # TODO: 實作 MOB orchestrator
    }

    if entity not in orchestrators:
        raise ValueError(f"不支援的實體類型: {entity}. 支援: {list(orchestrators.keys())}")

    return orchestrators[entity]()


async def main() -> Dict[str, Any]:
    """
    主執行函數

    讀取 run_config.toml 配置，執行對應的 pipeline

    Returns:
        Dict[str, Any]: 執行結果
    """
    # 1. 載入執行配置
    config = load_run_config()
    logger.info(f"執行配置: entity={config.entity}, type={config.processing_type}, date={config.processing_date}")

    # 2. 檢查是否從 checkpoint 恢復執行
    if config.resume_enabled:
        logger.info("從 checkpoint 恢復執行模式")
        if not config.checkpoint_name or not config.from_step:
            raise ValueError("Resume 模式需要設定 checkpoint_name 和 from_step")

        result = await resume_from_checkpoint(
            checkpoint_name=config.checkpoint_name,
            from_step=config.from_step,
            entity=config.entity,
            processing_type=config.processing_type,
            processing_date=config.processing_date,
            save_checkpoints=config.save_checkpoints
        )
        _print_result_summary(result)
        return result

    # 3. 一般執行模式 - 載入檔案路徑
    file_paths = load_file_paths(
        entity=config.entity,
        processing_type=config.processing_type,
        processing_date=config.processing_date
    )

    # 4. 取得 Orchestrator 並建立 Pipeline
    orchestrator = get_orchestrator(config.entity)

    if config.processing_type == "PO":
        pipeline = orchestrator.build_po_pipeline(file_paths)
    elif config.processing_type == "PR":
        pipeline = orchestrator.build_pr_pipeline(file_paths)
    elif config.processing_type == "PPE":
        # PPE 處理
        if hasattr(orchestrator, 'build_ppe_pipeline'):
            pipeline = orchestrator.build_ppe_pipeline(file_paths, config.processing_date)
            logger.info("使用 PPE pipeline")
        else:
            raise ValueError(f"{config.entity} 不支援 PPE 處理類型")
    elif config.processing_type == "PROCUREMENT":
        # PROCUREMENT 處理 (僅限 SPT)
        if config.entity != "SPT":
            raise ValueError(f"{config.entity} 不支援 PROCUREMENT 處理類型")
        if not config.source_type:
            raise ValueError("PROCUREMENT 處理類型需要設定 source_type (PO/PR/COMBINED)")

        pipeline = orchestrator.build_procurement_pipeline(
            file_paths=file_paths,
            source_type=config.source_type
        )
        logger.info(f"使用 PROCUREMENT pipeline, source_type={config.source_type}")
    else:
        raise ValueError(f"不支援的處理類型: {config.processing_type}")

    logger.info(f"Pipeline 建立完成: {pipeline.config.name}, 共 {len(pipeline.steps)} 個步驟")

    # 5. 建立處理上下文
    # 初始化空 DataFrame，由第一個步驟載入資料
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type=config.entity,
        processing_date=config.processing_date,
        processing_type=config.processing_type
    )

    # 將 file_paths 儲存到 context 中，供步驟存取參數
    context.set_variable('file_paths', file_paths)

    # PROCUREMENT 專用: 儲存 source_type 供 resume 使用
    if config.processing_type == "PROCUREMENT":
        context.set_variable('source_type', config.source_type)

    # 6. 執行 Pipeline
    if config.step_by_step:
        # 逐步執行模式
        logger.info("啟用逐步執行模式")
        executor = StepByStepExecutor(
            pipeline=pipeline,
            context=context,
            save_checkpoints=config.save_checkpoints
        )
        result = await executor.run()
    else:
        # 一般執行模式
        logger.info("執行 Pipeline...")
        result = await pipeline.execute(context)
        # 將 context 加入結果
        result["context"] = context

    # 7. 顯示結果摘要
    _print_result_summary(result)

    return result


def _print_result_summary(result: Dict[str, Any]):
    """顯示執行結果摘要"""
    success = result.get("success", False)
    duration = result.get("duration", 0)

    print("\n" + "=" * 60)
    print(f"執行結果: {'成功' if success else '失敗'}")
    print(f"耗時: {duration:.2f} 秒")

    if "successful_steps" in result:
        print(f"成功步驟: {result['successful_steps']}")
        print(f"跳過步驟: {result['skipped_steps']}")
        print(f"失敗步驟: {result['failed_steps']}")

    if result.get("errors"):
        print(f"錯誤數: {len(result['errors'])}")

    # 顯示輸出路徑
    context = result.get("context")
    if context:
        output_path = context.get_variable("po_export_output_path") or context.get_variable("pr_export_output_path")
        if output_path:
            print(f"輸出路徑: {output_path}")

    print("=" * 60 + "\n")


async def resume_from_checkpoint(
    checkpoint_name: str,
    from_step: str,
    entity: str,
    processing_type: str,
    processing_date: int,
    save_checkpoints: bool = True
) -> Dict[str, Any]:
    """
    從 checkpoint 恢復執行

    Args:
        checkpoint_name: checkpoint 名稱 (例如: "SPX_PO_202512_after_Load_All_Data")
        from_step: 從哪個步驟開始 (例如: "Add_Columns")
        entity: 業務實體 (SPX/SPT/MOB)
        processing_type: 處理類型 (PO/PR/PPE/PROCUREMENT)
        processing_date: 處理日期 (YYYYMM)
        save_checkpoints: 是否儲存 checkpoint

    Returns:
        Dict[str, Any]: 執行結果

    範例:
        result = await resume_from_checkpoint(
            checkpoint_name="SPX_PO_202511_after_Filter_Products",
            from_step="Add_Columns",
            entity="SPX",
            processing_type="PO",
            processing_date=202511,
            save_checkpoints=False
        )

        or

        result = asyncio.run(resume_from_checkpoint(...))

    Note:
        PROCUREMENT 類型會從 checkpoint 的 context 中讀取 source_type
    """
    from accrual_bot.core.pipeline.checkpoint import CheckpointManager, PipelineWithCheckpoint

    logger.info(f"從 checkpoint 恢復執行: {checkpoint_name}")
    logger.info(f"起始步驟: {from_step}")

    # 1. 載入 checkpoint
    checkpoint_manager = CheckpointManager("./checkpoints")
    context = checkpoint_manager.load_checkpoint(checkpoint_name)

    # 2. 載入檔案路徑
    file_paths = load_file_paths(entity, processing_type, processing_date)

    # 3. 將 file_paths 儲存到 context（如果 checkpoint 中沒有）
    if not context.get_variable('file_paths'):
        context.set_variable('file_paths', file_paths)

    # 4. 建立 pipeline
    orchestrator = get_orchestrator(entity)

    if processing_type == 'PO':
        pipeline = orchestrator.build_po_pipeline(file_paths)
    elif processing_type == 'PR':
        pipeline = orchestrator.build_pr_pipeline(file_paths)
    elif processing_type == 'PPE':
        if hasattr(orchestrator, 'build_ppe_pipeline'):
            pipeline = orchestrator.build_ppe_pipeline(file_paths, processing_date)
        else:
            raise ValueError(f"{entity} 不支援 PPE 處理類型")
    elif processing_type == 'PROCUREMENT':
        # 從 context 取得 source_type (由 main() 儲存)
        source_type = context.get_variable('source_type') or 'PO'
        if hasattr(orchestrator, 'build_procurement_pipeline'):
            pipeline = orchestrator.build_procurement_pipeline(file_paths, source_type)
        else:
            raise ValueError(f"{entity} 不支援 PROCUREMENT 處理類型")
    else:
        raise ValueError(f"不支援的處理類型: {processing_type}")

    # 5. 執行 pipeline（從指定步驟開始）
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    result = await executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints,
        start_from_step=from_step
    )

    logger.info("Resume 執行完成")
    return result


# =============================================================================
# 向後相容的舊函數 (保留給現有代碼調用)
# =============================================================================

async def run_spx_po_full_pipeline() -> Dict[str, Any]:
    """執行 SPX PO Pipeline (向後相容)"""
    file_paths = load_file_paths("SPX", "PO", 202512)
    orchestrator = SPXPipelineOrchestrator()
    pipeline = orchestrator.build_po_pipeline(file_paths)

    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPX",
        processing_date=202512,
        processing_type="PO"
    )
    context.set_variable('file_paths', file_paths)

    result = await pipeline.execute(context)
    result["context"] = context
    return result


async def run_spx_pr_full_pipeline() -> Dict[str, Any]:
    """執行 SPX PR Pipeline (向後相容)"""
    file_paths = load_file_paths("SPX", "PR", 202512)
    orchestrator = SPXPipelineOrchestrator()
    pipeline = orchestrator.build_pr_pipeline(file_paths)

    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPX",
        processing_date=202512,
        processing_type="PR"
    )
    context.set_variable('file_paths', file_paths)

    result = await pipeline.execute(context)
    result["context"] = context
    return result


async def run_spt_po_full_pipeline() -> Dict[str, Any]:
    """執行 SPT PO Pipeline (向後相容)"""
    file_paths = load_file_paths("SPT", "PO", 202512)
    orchestrator = SPTPipelineOrchestrator()
    pipeline = orchestrator.build_po_pipeline(file_paths)

    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPT",
        processing_date=202512,
        processing_type="PO"
    )
    context.set_variable('file_paths', file_paths)

    result = await pipeline.execute(context)
    result["context"] = context
    return result


async def run_spt_pr_full_pipeline() -> Dict[str, Any]:
    """執行 SPT PR Pipeline (向後相容)"""
    file_paths = load_file_paths("SPT", "PR", 202512)
    orchestrator = SPTPipelineOrchestrator()
    pipeline = orchestrator.build_pr_pipeline(file_paths)

    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type="SPT",
        processing_date=202512,
        processing_type="PR"
    )
    context.set_variable('file_paths', file_paths)

    result = await pipeline.execute(context)
    result["context"] = context
    return result

# =============================================================================
# 入口點
# =============================================================================

if __name__ == "__main__":
    warnings.filterwarnings("ignore")

    # 執行 main 函數 (根據 run_config.toml 配置)
    result = asyncio.run(main())

    # 設定退出碼
    sys.exit(0 if result.get("success", False) else 1)