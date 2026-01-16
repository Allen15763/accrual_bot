"""
Unified Pipeline Service

統一的 pipeline 服務層，解耦 UI 與具體實作。
提供 entity、template、step 的動態查詢介面。
"""

from typing import Dict, List, Optional, Any
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.core.pipeline import PipelineTemplateManager, Pipeline
from accrual_bot.ui.config import ENTITY_CONFIG
from accrual_bot.utils.config import ConfigManager


class UnifiedPipelineService:
    """統一的 pipeline 服務層"""

    def __init__(self):
        self.template_manager = PipelineTemplateManager()

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

    def get_templates(self, entity: str, proc_type: str) -> Dict[str, Any]:
        """
        獲取可用範本（已棄用，保留向後兼容）

        Args:
            entity: Entity 名稱
            proc_type: 處理類型

        Returns:
            空字典，使用 orchestrator 配置代替範本
        """
        # 不再使用 PipelineTemplateManager，直接使用 orchestrator 配置
        return {
            'recommended': f'{entity}_配置檔案',
            'all': []
        }

    def get_enabled_steps(self, entity: str, proc_type: str) -> List[str]:
        """
        獲取已啟用的步驟清單 (唯讀，從配置檔讀取)

        Args:
            entity: Entity 名稱
            proc_type: 處理類型

        Returns:
            步驟名稱清單
        """
        orchestrator = self._get_orchestrator(entity)
        return orchestrator.get_enabled_steps(proc_type)

    def build_pipeline(
        self,
        entity: str,
        proc_type: str,
        file_paths: Dict[str, str],
        processing_date: Optional[int] = None
    ) -> Pipeline:
        """
        建立 pipeline

        Args:
            entity: Entity 名稱
            proc_type: 處理類型 (PO, PR, PPE)
            file_paths: 檔案路徑字典（可能只包含路徑字符串）
            processing_date: 處理日期 (YYYYMM，PPE 必填)

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
        elif proc_type == 'PPE' and entity == 'SPX':
            if not processing_date:
                raise ValueError("PPE 處理需要提供 processing_date")
            return orchestrator.build_ppe_pipeline(enriched_file_paths, processing_date)
        else:
            raise ValueError(f"不支援的處理類型: {entity}/{proc_type}")

    def build_pipeline_from_template(
        self,
        template_name: str,
        entity: str,
        proc_type: str,
        file_paths: Dict[str, str]
    ) -> Pipeline:
        """
        從範本建立 pipeline（已棄用，直接調用 build_pipeline）

        Args:
            template_name: 範本名稱（忽略）
            entity: Entity 名稱
            proc_type: 處理類型
            file_paths: 檔案路徑字典

        Returns:
            Pipeline 物件
        """
        # 忽略 template_name，直接使用 orchestrator 配置
        return self.build_pipeline(entity, proc_type, file_paths)

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
            # 獲取 ConfigManager 實例
            config_manager = ConfigManager()

            # 從 paths.toml 讀取參數
            # 配置路徑: [entity.proc_type.params]
            # 例如: [spx.po.params]
            config_section = f"{entity.lower()}.{proc_type.lower()}.params"

            print(f"[DEBUG] Enriching file_paths for: {config_section}")
            print(f"[DEBUG] Has _config_toml: {hasattr(config_manager, '_config_toml')}")

            # 訪問 TOML 配置（雖然是私有屬性，但暫時沒有公開 API）
            if hasattr(config_manager, '_config_toml'):
                toml_config = config_manager._config_toml
                print(f"[DEBUG] TOML keys: {list(toml_config.keys()) if isinstance(toml_config, dict) else 'not a dict'}")

                # 嘗試從配置中獲取參數
                params_config = toml_config
                for key in config_section.split('.'):
                    if isinstance(params_config, dict) and key in params_config:
                        params_config = params_config[key]
                        print(f"[DEBUG] Found key '{key}', type: {type(params_config)}")
                    else:
                        print(f"[DEBUG] Key '{key}' not found or not a dict")
                        params_config = None
                        break

                # 如果成功獲取配置，整合到 file_paths
                if isinstance(params_config, dict):
                    print(f"[DEBUG] Params config keys: {list(params_config.keys())}")
                    enriched_paths = {}
                    for file_key, file_path in file_paths.items():
                        # 如果配置中有此檔案的參數，則整合
                        if file_key in params_config:
                            print(f"[DEBUG] Enriching '{file_key}' with params: {params_config[file_key]}")
                            enriched_paths[file_key] = {
                                'path': file_path,
                                'params': params_config[file_key]
                            }
                        else:
                            print(f"[DEBUG] No params for '{file_key}', keeping as string")
                            # 沒有參數，保持原樣
                            enriched_paths[file_key] = file_path

                    print(f"[DEBUG] Enriched paths: {enriched_paths}")
                    return enriched_paths
                else:
                    print(f"[DEBUG] params_config is not a dict: {type(params_config)}")

        except Exception as e:
            # 讀取配置失敗，返回原始 file_paths
            import traceback
            print(f"[ERROR] Failed to enrich file_paths from config: {e}")
            traceback.print_exc()

        # 如果無法讀取配置，返回原始 file_paths
        print(f"[DEBUG] Returning original file_paths (enrich failed)")
        return file_paths

    def _template_matches(self, template: Dict[str, str], entity: str, proc_type: str) -> bool:
        """
        判斷範本是否適用於指定的 entity/type

        Args:
            template: 範本資訊
            entity: Entity 名稱
            proc_type: 處理類型

        Returns:
            是否匹配
        """
        # 簡單的名稱匹配邏輯
        # 可以根據需要擴展更複雜的匹配規則
        template_name = template.get('name', '').upper()
        return entity.upper() in template_name or proc_type.upper() in template_name
