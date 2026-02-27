"""
Unified Pipeline Service

統一的 pipeline 服務層，解耦 UI 與具體實作。
提供 entity、step 的動態查詢介面。
"""

from typing import Dict, List, Optional, Any
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.core.pipeline import Pipeline
from accrual_bot.ui.config import ENTITY_CONFIG
from accrual_bot.utils.config import ConfigManager


class UnifiedPipelineService:
    """統一的 pipeline 服務層"""

    def get_available_entities(self) -> List[str]:
        """
        獲取可用的 entity 清單 (排除 MOB)

        Returns:
            Entity 清單，例如 ['SPT', 'SPX']
        """
        return list(ENTITY_CONFIG.keys())

    def get_entity_config(self, entity: str) -> Dict[str, Any]:
        """
        獲取 entity 的設定資訊

        Args:
            entity: Entity 名稱

        Returns:
            Entity 設定字典
        """
        return ENTITY_CONFIG.get(entity, {})

    def get_entity_types(self, entity: str) -> List[str]:
        """
        獲取 entity 支援的處理類型

        Args:
            entity: Entity 名稱

        Returns:
            支援的處理類型清單，例如 ['PO', 'PR']
        """
        return ENTITY_CONFIG.get(entity, {}).get('types', [])

    def get_enabled_steps(self, entity: str, proc_type: str, source_type: Optional[str] = None) -> List[str]:
        """
        獲取已啟用的步驟清單 (唯讀，從配置檔讀取)

        Args:
            entity: Entity 名稱
            proc_type: 處理類型
            source_type: 子類型 (僅 PROCUREMENT 使用: PO/PR/COMBINED)

        Returns:
            步驟名稱清單
        """
        orchestrator = self._get_orchestrator(entity)

        # 如果是 PROCUREMENT 且有 source_type，傳入 source_type
        if proc_type == 'PROCUREMENT' and source_type and hasattr(orchestrator, 'get_enabled_steps'):
            return orchestrator.get_enabled_steps(proc_type, source_type=source_type)

        return orchestrator.get_enabled_steps(proc_type)

    def build_pipeline(
        self,
        entity: str,
        proc_type: str,
        file_paths: Dict[str, str],
        processing_date: Optional[int] = None,
        source_type: Optional[str] = None
    ) -> Pipeline:
        """
        建立 pipeline

        Args:
            entity: Entity 名稱
            proc_type: 處理類型 (PO, PR, PPE, PROCUREMENT)
            file_paths: 檔案路徑字典（可能只包含路徑字符串）
            processing_date: 處理日期 (YYYYMM，PPE 必填)
            source_type: 子類型 (僅 PROCUREMENT 使用: PO/PR/COMBINED)

        Returns:
            Pipeline 物件

        Raises:
            ValueError: 當 proc_type 無效時
        """
        # 整合配置文件中的參數到 file_paths
        enriched_file_paths = self._enrich_file_paths(file_paths, entity, proc_type)

        orchestrator = self._get_orchestrator(entity)

        if proc_type == 'PO':
            return orchestrator.build_po_pipeline(enriched_file_paths)
        elif proc_type == 'PR':
            return orchestrator.build_pr_pipeline(enriched_file_paths)
        elif proc_type == 'PROCUREMENT' and entity == 'SPT':
            # 使用明確傳入的 source_type，不再自動推測
            if not source_type:
                raise ValueError("PROCUREMENT 需要指定 source_type (PO/PR/COMBINED)")

            return orchestrator.build_procurement_pipeline(
                enriched_file_paths,
                source_type=source_type
            )
        elif proc_type == 'PPE' and entity == 'SPX':
            if not processing_date:
                raise ValueError("PPE 處理需要提供 processing_date")
            return orchestrator.build_ppe_pipeline(enriched_file_paths, processing_date)
        elif proc_type == 'PPE_DESC' and entity == 'SPX':
            if not processing_date:
                raise ValueError("PPE_DESC 處理需要提供 processing_date")
            return orchestrator.build_ppe_desc_pipeline(enriched_file_paths, processing_date)
        else:
            raise ValueError(f"不支援的處理類型: {entity}/{proc_type}")

    def _get_orchestrator(self, entity: str):
        """
        獲取對應的 orchestrator

        Args:
            entity: Entity 名稱

        Returns:
            Orchestrator 實例

        Raises:
            ValueError: 當 entity 不存在時
        """
        orchestrators = {
            'SPT': SPTPipelineOrchestrator,
            'SPX': SPXPipelineOrchestrator,
        }

        orchestrator_class = orchestrators.get(entity)
        if not orchestrator_class:
            raise ValueError(f"不支援的 entity: {entity}")

        return orchestrator_class()

    def _enrich_file_paths(
        self,
        file_paths: Dict[str, str],
        entity: str,
        proc_type: str
    ) -> Dict[str, Any]:
        """
        整合配置文件中的檔案參數到 file_paths

        從 config/paths.toml 讀取 [entity.proc_type.params] 區段的參數，
        將簡單的字符串路徑轉換為包含 params 的字典結構。

        Args:
            file_paths: UI 提供的檔案路徑字典（字符串格式）
            entity: Entity 名稱（如 'SPX'）
            proc_type: 處理類型（如 'PO'）

        Returns:
            整合參數後的 file_paths 字典

        Example:
            Input:  {'ops_validation': '/path/to/file.xlsx'}
            Output: {'ops_validation': {'path': '/path/to/file.xlsx', 'params': {...}}}
        """
        try:
            config_manager = ConfigManager()

            # 使用公開 API 獲取參數配置
            # 路徑: [entity.proc_type.params]
            params_config = config_manager.get_paths_config(
                entity.lower(),
                proc_type.lower(),
                'params'
            )

            if params_config and isinstance(params_config, dict):
                enriched_paths = {}
                for file_key, file_path in file_paths.items():
                    if file_key in params_config:
                        enriched_paths[file_key] = {
                            'path': file_path,
                            'params': params_config[file_key]
                        }
                    else:
                        enriched_paths[file_key] = file_path
                return enriched_paths

        except Exception as e:
            import traceback
            print(f"[ERROR] Failed to enrich file_paths: {e}")
            traceback.print_exc()

        return file_paths
