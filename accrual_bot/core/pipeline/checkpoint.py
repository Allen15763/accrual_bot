"""
Pipeline Checkpoint 系統
提供 pipeline 執行狀態的儲存和恢復功能

整合來源：
  - accrual_bot/core/pipeline/checkpoint.py
    （async 執行、checkpoint 名含 processing_date、execute_pipeline_with_checkpoint）
  - spe_bank_recon/src/core/pipeline/checkpoint.py
    （logger、Parquet→Pickle fallback、非 DataFrame aux_data、JSON 安全序列化、
     data_shape、warnings/errors 恢復、cleanup_old_checkpoints、filter 參數）

功能：
1. 儲存 pipeline 執行的中間狀態（主數據 + 輔助數據 + 變數）
2. 從指定步驟恢復執行
3. 快速測試後續步驟
4. 自動清理舊 checkpoint

使用方式：
    # 首次執行 - 自動儲存 checkpoint（orchestrator 整合）
    result = await execute_pipeline_with_checkpoint(
        file_paths=file_paths,
        processing_date=202509,
        pipeline_func=create_spx_pipeline,
        entity='SPX',
        processing_type='PO',
        save_checkpoints=True
    )

    # 從特定步驟恢復（便捷函數）
    result = await resume_from_step(
        checkpoint_name="SPX_PO_202509_after_Filter_SPX_Products",
        start_from_step="Add_Columns",
        pipeline_func=create_spx_pipeline,
        file_paths=file_paths
    )

    # 快速測試單一步驟
    result = await quick_test_step(
        checkpoint_name="SPX_PO_202509_after_Add_Columns",
        step_to_test="Integrate_AP_Invoice",
        pipeline_func=create_spx_pipeline,
        file_paths=file_paths
    )
"""

import json
import pickle
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

import pandas as pd

from .context import ProcessingContext
from .pipeline import Pipeline
from .base import StepResult, StepStatus
from accrual_bot.utils.logging import get_logger


class CheckpointManager:
    """Pipeline Checkpoint 管理器"""

    def __init__(self, checkpoint_dir: str = "./checkpoints"):
        """
        初始化 Checkpoint 管理器

        Args:
            checkpoint_dir: checkpoint 儲存目錄（預設 ./checkpoints）
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("pipeline.checkpoint")

    # ────────────────────────────────────────────────
    # 儲存
    # ────────────────────────────────────────────────

    def save_checkpoint(
        self,
        context: ProcessingContext,
        step_name: str,
        metadata: Optional[Dict] = None,
    ) -> str:
        """
        儲存 checkpoint

        儲存策略：
          - 主數據優先 Parquet，失敗時 fallback 至 Pickle
          - DataFrame 型輔助數據：Parquet → Pickle fallback
          - 非 DataFrame 型輔助數據：直接 Pickle
          - 變數：JSON 安全序列化（不可序列化值轉為 str）

        Args:
            context: 處理上下文
            step_name: 步驟名稱
            metadata: 額外的元數據

        Returns:
            str: checkpoint 名稱
        """
        entity_type = context.metadata.entity_type or "unknown"
        processing_type = context.metadata.processing_type or "unknown"
        processing_date = context.metadata.processing_date or "unknown"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        checkpoint_name = f"{entity_type}_{processing_type}_{processing_date}_after_{step_name}"
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        checkpoint_path.mkdir(parents=True, exist_ok=True)

        # --- 儲存主數據 ---
        if context.data is not None and not context.data.empty:
            self._save_dataframe(
                context.data,
                checkpoint_path / "data.parquet",
                checkpoint_path / "data.pkl",
                label="主數據",
            )

        # --- 儲存輔助數據 ---
        aux_data_dir = checkpoint_path / "auxiliary_data"
        aux_data_dir.mkdir(exist_ok=True)

        for aux_name in context.list_auxiliary_data():
            aux_data = context.get_auxiliary_data(aux_name)
            if aux_data is None:
                continue
            if isinstance(aux_data, pd.DataFrame):
                if aux_data.empty:
                    continue
                # 特定欄位型別修正（如 ops_validation 的 discount 欄）
                aux_data_to_save = aux_data.copy()
                if 'discount' in aux_data_to_save.columns:
                    aux_data_to_save['discount'] = aux_data_to_save['discount'].astype(str)
                self._save_dataframe(
                    aux_data_to_save,
                    aux_data_dir / f"{aux_name}.parquet",
                    aux_data_dir / f"{aux_name}.pkl",
                    label=f"輔助數據 {aux_name}",
                )
            else:
                # 非 DataFrame：直接用 pickle
                try:
                    with open(aux_data_dir / f"{aux_name}.pkl", 'wb') as f:
                        pickle.dump(aux_data, f)
                except Exception as e:
                    self.logger.error(f"輔助數據（非 DataFrame）{aux_name} 儲存失敗: {e}")

        # --- 儲存變數與元數據（JSON 安全序列化）---
        safe_variables = {}
        for k, v in context._variables.items():
            try:
                json.dumps(v)
                safe_variables[k] = v
            except (TypeError, ValueError):
                safe_variables[k] = str(v)

        checkpoint_info = {
            'step_name': step_name,
            'entity_type': entity_type,
            'processing_date': processing_date,
            'processing_type': processing_type,
            'variables': safe_variables,
            'warnings': context.warnings,
            'errors': context.errors,
            'timestamp': timestamp,
            'auxiliary_data_list': context.list_auxiliary_data(),
            'data_shape': list(context.data.shape) if context.data is not None else [0, 0],
            'metadata': metadata or {},
        }

        with open(checkpoint_path / "checkpoint_info.json", 'w', encoding='utf-8') as f:
            json.dump(checkpoint_info, f, indent=2, ensure_ascii=False, default=str)

        self.logger.info(
            f"Checkpoint 已儲存: {checkpoint_name} "
            f"（主數據 {checkpoint_info['data_shape'][0]} 行，"
            f"輔助數據 {len(checkpoint_info['auxiliary_data_list'])} 個）"
        )
        return checkpoint_name

    def _save_dataframe(
        self,
        df: pd.DataFrame,
        parquet_path: Path,
        pkl_path: Path,
        label: str = "DataFrame",
    ) -> None:
        """嘗試 Parquet 儲存，失敗時 fallback 至 Pickle"""
        try:
            df.to_parquet(parquet_path, index=False)
        except Exception as e:
            self.logger.warning(f"{label} Parquet 儲存失敗，改用 Pickle: {e}")
            try:
                df.to_pickle(pkl_path)
            except Exception as e2:
                self.logger.error(f"{label} Pickle 儲存亦失敗: {e2}")

    # ────────────────────────────────────────────────
    # 載入
    # ────────────────────────────────────────────────

    def load_checkpoint(self, checkpoint_name: str) -> ProcessingContext:
        """
        載入 checkpoint，恢復完整的 ProcessingContext

        載入策略：
          - 主數據：優先 Parquet，不存在則 Pickle
          - 輔助數據：先載入所有 Parquet，再補載 Pickle（不覆蓋已存在的）
          - 恢復 variables、warnings、errors

        Args:
            checkpoint_name: checkpoint 名稱

        Returns:
            ProcessingContext: 恢復的上下文
        """
        checkpoint_path = self.checkpoint_dir / checkpoint_name

        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Checkpoint 不存在: {checkpoint_name}")

        # --- 載入元數據 ---
        with open(checkpoint_path / "checkpoint_info.json", 'r', encoding='utf-8') as f:
            info = json.load(f)

        # --- 載入主數據 ---
        data_parquet = checkpoint_path / "data.parquet"
        data_pkl = checkpoint_path / "data.pkl"

        if data_parquet.exists():
            data = pd.read_parquet(data_parquet)
        elif data_pkl.exists():
            data = pd.read_pickle(data_pkl)
        else:
            data = pd.DataFrame()

        # --- 建立上下文 ---
        context = ProcessingContext(
            data=data,
            entity_type=info['entity_type'],
            processing_date=info['processing_date'],
            processing_type=info['processing_type'],
        )

        # --- 恢復變數 ---
        for key, value in info['variables'].items():
            context.set_variable(key, value)

        # --- 恢復 warnings / errors ---
        context.warnings = info.get('warnings', [])
        context.errors = info.get('errors', [])

        # --- 恢復輔助數據 ---
        aux_data_dir = checkpoint_path / "auxiliary_data"
        if aux_data_dir.exists():
            # 先載 Parquet
            for aux_file in sorted(aux_data_dir.glob("*.parquet")):
                aux_name = aux_file.stem
                try:
                    context.add_auxiliary_data(aux_name, pd.read_parquet(aux_file))
                except Exception as e:
                    self.logger.warning(f"輔助數據 {aux_name}.parquet 載入失敗: {e}")

            # 再補 Pickle（不覆蓋已存在的）
            for aux_file in sorted(aux_data_dir.glob("*.pkl")):
                aux_name = aux_file.stem
                if context.has_auxiliary_data(aux_name):
                    continue
                try:
                    with open(aux_file, 'rb') as f:
                        context.add_auxiliary_data(aux_name, pickle.load(f))
                except Exception as e:
                    self.logger.warning(f"輔助數據 {aux_name}.pkl 載入失敗: {e}")

        self.logger.info(
            f"Checkpoint 已載入: {checkpoint_name} | "
            f"主數據 {len(context.data)} 行 | "
            f"輔助數據 {len(context.list_auxiliary_data())} 個 | "
            f"變數 {len(context._variables)} 個"
        )
        return context

    # ────────────────────────────────────────────────
    # 查詢 / 管理
    # ────────────────────────────────────────────────

    def list_checkpoints(
        self, filter_by_entity: Optional[str] = None
    ) -> List[Dict]:
        """
        列出所有可用的 checkpoint

        Args:
            filter_by_entity: 過濾指定 entity_type（如 'SPX'），None 表示全部

        Returns:
            List[Dict]: checkpoint 資訊列表（按時間戳降序）
        """
        checkpoints = []

        for checkpoint_path in self.checkpoint_dir.iterdir():
            if not checkpoint_path.is_dir():
                continue
            info_file = checkpoint_path / "checkpoint_info.json"
            if not info_file.exists():
                continue
            try:
                with open(info_file, 'r', encoding='utf-8') as f:
                    info = json.load(f)

                if filter_by_entity and info.get('entity_type') != filter_by_entity:
                    continue

                checkpoints.append({
                    'name': checkpoint_path.name,
                    'step': info['step_name'],
                    'entity_type': info.get('entity_type', 'unknown'),
                    'processing_type': info.get('processing_type', 'unknown'),
                    'processing_date': info.get('processing_date', 'unknown'),
                    'timestamp': info['timestamp'],
                    'data_shape': info.get('data_shape', [0, 0]),
                })
            except Exception as e:
                self.logger.warning(f"讀取 checkpoint 資訊失敗: {checkpoint_path.name} — {e}")

        return sorted(checkpoints, key=lambda x: x['timestamp'], reverse=True)

    def delete_checkpoint(self, checkpoint_name: str) -> bool:
        """
        刪除指定的 checkpoint

        Args:
            checkpoint_name: checkpoint 名稱

        Returns:
            bool: 是否成功刪除
        """
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        if checkpoint_path.exists():
            shutil.rmtree(checkpoint_path)
            self.logger.info(f"Checkpoint 已刪除: {checkpoint_name}")
            return True
        self.logger.warning(f"Checkpoint 不存在，無法刪除: {checkpoint_name}")
        return False

    def cleanup_old_checkpoints(
        self,
        keep_last: int = 5,
        filter_by_entity: Optional[str] = None,
    ) -> int:
        """
        清理舊的 checkpoint，保留最近的 N 個

        Args:
            keep_last: 保留最近的數量（預設 5）
            filter_by_entity: 限定 entity_type 清理，None 表示全部

        Returns:
            int: 已刪除的 checkpoint 數量
        """
        checkpoints = self.list_checkpoints(filter_by_entity=filter_by_entity)

        deleted = 0
        if len(checkpoints) > keep_last:
            to_delete = checkpoints[keep_last:]
            for cp in to_delete:
                if self.delete_checkpoint(cp['name']):
                    deleted += 1
            self.logger.info(
                f"清理完成：刪除 {deleted} 個舊 checkpoint"
                + (f"（entity={filter_by_entity}）" if filter_by_entity else "")
            )
        return deleted


# =============================================================================
# PipelineWithCheckpoint — async 執行器
# =============================================================================

class PipelineWithCheckpoint:
    """帶 Checkpoint 功能的非同步 Pipeline 執行器"""

    def __init__(
        self,
        pipeline: Pipeline,
        checkpoint_manager: Optional[CheckpointManager] = None,
    ):
        """
        Args:
            pipeline: Pipeline 實例
            checkpoint_manager: Checkpoint 管理器，None 則自動建立
        """
        self.pipeline = pipeline
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.logger = get_logger("pipeline.checkpoint.executor")

    async def execute_with_checkpoint(
        self,
        context: ProcessingContext,
        save_after_each_step: bool = True,
        start_from_step: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        非同步執行 Pipeline 並自動儲存 checkpoint

        Args:
            context: 處理上下文
            save_after_each_step: 是否在每個成功步驟後儲存 checkpoint
            start_from_step: 從哪個步驟開始執行（None = 從頭）

        Returns:
            Dict: 執行結果，含 success、duration、results 等欄位
        """
        start_time = datetime.now()

        # --- 找起始步驟索引 ---
        start_index = 0
        if start_from_step:
            for i, step in enumerate(self.pipeline.steps):
                if step.name == start_from_step:
                    start_index = i
                    self.logger.info(
                        f"從步驟 '{start_from_step}' 開始執行（跳過前 {i} 個步驟）"
                    )
                    break
            else:
                raise ValueError(f"找不到步驟: {start_from_step}")

        # --- 執行步驟 ---
        results = []
        for i, step in enumerate(self.pipeline.steps[start_index:], start=start_index):
            self.logger.info(
                f"{'='*60}\n"
                f"執行步驟 {i+1}/{len(self.pipeline.steps)}: {step.name}\n"
                f"{'='*60}"
            )

            result = await step.execute(context)
            results.append(result)

            # 記錄到上下文歷史
            context.add_history(step.name, result.status.value)

            # 儲存 checkpoint（只在成功時）
            if save_after_each_step and result.is_success:
                self.checkpoint_manager.save_checkpoint(
                    context=context,
                    step_name=step.name,
                    metadata={
                        'step_index': i,
                        'step_status': result.status.value,
                        'step_message': result.message,
                    },
                )

            # 遇錯即停
            if not result.is_success and self.pipeline.config.stop_on_error:
                self.logger.error(f"步驟失敗，停止執行: {result.message}")
                break

        # --- 彙總結果 ---
        end_time = datetime.now()
        successful = sum(1 for r in results if r.is_success)
        failed = sum(1 for r in results if r.is_failed)
        skipped = sum(1 for r in results if r.status == StepStatus.SKIPPED)

        return {
            'success': failed == 0,
            'pipeline': self.pipeline.config.name,
            'start_time': start_time,
            'end_time': end_time,
            'duration': (end_time - start_time).total_seconds(),
            'total_steps': len(self.pipeline.steps),
            'executed_steps': len(results),
            'successful_steps': successful,
            'failed_steps': failed,
            'skipped_steps': skipped,
            'results': results,
            'context': context,
        }


# =============================================================================
# 便捷函數
# =============================================================================

async def execute_pipeline_with_checkpoint(
    file_paths: Dict[str, Any],
    processing_date: int,
    pipeline_func,
    entity: str,
    checkpoint_dir: str = "./checkpoints",
    save_checkpoints: bool = True,
    processing_type: str = 'PO',
) -> Dict[str, Any]:
    """
    Orchestrator 整合用：建立 pipeline 並執行，自動儲存 checkpoint

    Args:
        file_paths: 檔案路徑字典
        processing_date: 處理日期（YYYYMM）
        pipeline_func: 建立 pipeline 的函數（create_xxx_pipeline）
        entity: 業務實體（SPX/SPT）
        checkpoint_dir: checkpoint 儲存目錄
        save_checkpoints: 是否儲存 checkpoint
        processing_type: 處理類型（PO/PR/PPE）

    Returns:
        Dict: 執行結果
    """
    if processing_type == "PPE":
        pipeline = pipeline_func(file_paths, processing_date)
    else:
        pipeline = pipeline_func(file_paths)

    checkpoint_manager = CheckpointManager(checkpoint_dir)
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type=entity,
        processing_date=processing_date,
        processing_type=processing_type,
    )

    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    return await executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints,
    )


async def resume_from_step(
    checkpoint_name: str,
    start_from_step: str,
    pipeline_func,
    file_paths: Optional[Dict[str, Any]] = None,
    checkpoint_dir: str = "./checkpoints",
    save_checkpoints: bool = True,
) -> Dict[str, Any]:
    """
    從 checkpoint 恢復並從指定步驟開始執行

    Args:
        checkpoint_name: checkpoint 名稱
        start_from_step: 起始步驟名稱
        pipeline_func: 建立 pipeline 的函數
        file_paths: 檔案路徑（若 checkpoint 中未保存則必填）
        checkpoint_dir: checkpoint 目錄
        save_checkpoints: 是否儲存新的 checkpoint

    Returns:
        Dict: 執行結果
    """
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    context = checkpoint_manager.load_checkpoint(checkpoint_name)

    if file_paths is None:
        file_paths = context.get_variable('file_paths', {})
        if not file_paths:
            raise ValueError("無法取得檔案路徑，請提供 file_paths 參數")

    pipeline = pipeline_func(file_paths)
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)

    return await executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints,
        start_from_step=start_from_step,
    )


async def quick_test_step(
    checkpoint_name: str,
    step_to_test: str,
    pipeline_func,
    file_paths: Optional[Dict[str, Any]] = None,
    checkpoint_dir: str = "./checkpoints",
) -> Dict[str, Any]:
    """
    快速測試單一步驟（從上一個 checkpoint 恢復，不儲存新 checkpoint）

    Args:
        checkpoint_name: checkpoint 名稱
        step_to_test: 要測試的步驟名稱
        pipeline_func: 建立 pipeline 的函數
        file_paths: 檔案路徑（選填）
        checkpoint_dir: checkpoint 目錄

    Returns:
        Dict: 執行結果
    """
    return await resume_from_step(
        checkpoint_name=checkpoint_name,
        start_from_step=step_to_test,
        pipeline_func=pipeline_func,
        file_paths=file_paths,
        checkpoint_dir=checkpoint_dir,
        save_checkpoints=False,
    )


def list_available_checkpoints(
    checkpoint_dir: str = "./checkpoints",
    filter_by_entity: Optional[str] = None,
) -> List[Dict]:
    """
    列出可用的 checkpoint（同步便捷函數）

    Args:
        checkpoint_dir: checkpoint 目錄
        filter_by_entity: 過濾指定 entity_type

    Returns:
        List[Dict]: checkpoint 資訊列表
    """
    return CheckpointManager(checkpoint_dir).list_checkpoints(
        filter_by_entity=filter_by_entity
    )
