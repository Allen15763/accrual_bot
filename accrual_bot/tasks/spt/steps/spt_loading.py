"""
SPT Data Loading Steps

SPT PO 和 PR 數據載入步驟，基於 BaseLoadingStep 模板方法模式。
消除了原 SPTDataLoadingStep 與 SPTPRDataLoadingStep 之間的約 600 行重複程式碼（原 1164 行）。

共用邏輯（Line#/GL#/Project Number 欄位處理、並發載入、參考資料載入）
全部集中於 SPTBaseDataLoadingStep，子類只需宣告 get_required_file_type()。
processing_date 由 context.metadata.processing_date 提供（UI/CLI），不從檔名擷取。
"""

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Dict, Tuple, Any, Union
import pandas as pd

from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.base_loading import BaseLoadingStep
from accrual_bot.core.datasources import DataSourceFactory
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.helpers import get_ref_on_colab


class SPTBaseDataLoadingStep(BaseLoadingStep):
    """
    SPT PO/PR 通用數據載入基類

    使用 BaseLoadingStep 模板方法模式，消除 SPTDataLoadingStep 與
    SPTPRDataLoadingStep 之間的重複程式碼。

    子類只需覆寫 get_required_file_type()：
    - SPTDataLoadingStep  → 'raw_po'
    - SPTPRDataLoadingStep → 'raw_pr'
    """

    @abstractmethod
    def get_required_file_type(self) -> str:
        """返回必要的文件類型（'raw_po' 或 'raw_pr'）"""
        ...

    # ========== BaseLoadingStep 抽象鉤子實作 ==========

    async def _load_primary_file(
        self,
        source,
        file_path: str
    ) -> pd.DataFrame:
        """
        載入主要數據文件。

        使用 BaseLoadingStep._process_common_columns() 統一處理
        Line#、GL#、Project Number → Project 欄位轉換。
        processing_date 由 context.metadata 提供，不從檔名擷取。
        """
        df = await source.read()
        df = self._process_common_columns(df)
        self.logger.debug(
            f"成功導入{self.get_required_file_type().upper()}數據, 數據維度: {df.shape}"
        )
        return df

    def _extract_primary_data(
        self,
        primary_result: pd.DataFrame
    ) -> pd.DataFrame:
        """驗證主數據（非空、必要欄位）"""
        df = primary_result
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            raise ValueError(
                f"Raw {self.get_required_file_type()} data is empty"
            )
        required_columns = ['Product Code', 'Item Description', 'GL#']
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        self.logger.info(
            f"Raw {self.get_required_file_type()} data validated: {df.shape}"
        )
        return df

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """
        載入科目映射等參考數據。

        PO 與 PR 使用相同的參考數據路徑（ref_path_spt）。
        """
        try:
            from accrual_bot.utils.helpers.file_utils import resolve_config_ref_path
            ref_data_path = config_manager._config_data.get('PATHS').get('ref_path_spt')
            ref_data_path = resolve_config_ref_path(ref_data_path)
            count = 0

            # Colab 環境：從 ZIP 載入
            ref_ac = get_ref_on_colab(ref_data_path)
            if ref_ac is not None and isinstance(ref_ac, pd.DataFrame):
                context.add_auxiliary_data('reference_account', ref_ac.iloc[:, 1:3].copy())
                context.add_auxiliary_data(
                    'reference_liability', ref_ac.loc[:, ['Account', 'Liability']].copy()
                )
                count += 2
                self.logger.info(f"Loaded account mapping from zip: {len(ref_ac)} records")
                return count

            # 一般環境：從檔案載入
            if Path(ref_data_path).exists():
                source = DataSourceFactory.create_from_file(str(ref_data_path))
                ref_ac = await source.read(dtype=str)
                context.add_auxiliary_data('reference_account', ref_ac.iloc[:, 1:3].copy())
                context.add_auxiliary_data(
                    'reference_liability', ref_ac.loc[:, ['Account', 'Liability']].copy()
                )
                await source.close()
                count += 2
                self.logger.info(f"Loaded account mapping: {len(ref_ac)} records")
            else:
                self.logger.warning(f"Account mapping file not found: {ref_data_path}")
                context.add_auxiliary_data('reference_account', pd.DataFrame())
                context.add_auxiliary_data('reference_liability', pd.DataFrame())

            return count

        except Exception as e:
            self.logger.error(f"Failed to load reference data: {str(e)}")
            context.add_auxiliary_data('reference_account', pd.DataFrame())
            context.add_auxiliary_data('reference_liability', pd.DataFrame())
            return 0

    # ========== 可選覆寫方法 ==========

    def _get_custom_file_loader(self, file_type: str):
        """為 ap_invoice 提供自定義載入邏輯"""
        if file_type == 'ap_invoice':
            return self._load_ap_invoice
        return None

    async def _load_ap_invoice(self, source, config: Dict[str, Any]) -> pd.DataFrame:
        """
        載入 AP Invoice，覆寫讀取參數。

        注意：ap_columns 目前仍使用 'SPX' config key（已知 BUG-1，待後續修復）。
        """
        df = await source.read(
            usecols=config_manager.get_list('SPX', 'ap_columns'),
            header=1,
            sheet_name=1,
            dtype=str
        )
        self.logger.debug(f"成功導入AP數據, 數據維度: {df.shape}")
        return df

    def _set_additional_context_variables(
        self,
        context: ProcessingContext,
        validated_configs: Dict[str, Any],
        loaded_data: Dict[str, Any]
    ) -> None:
        """添加 raw_data_snapshot 供 DataShapeSummaryStep 使用"""
        shape_summary_cfg = config_manager._config_toml.get('data_shape_summary', {})
        if shape_summary_cfg.get('enabled', False):
            context.add_auxiliary_data('raw_data_snapshot', context.data.copy())


# ========== 具體子類（公開 API） ==========

class SPTDataLoadingStep(SPTBaseDataLoadingStep):
    """
    SPT PO 數據載入步驟

    載入 raw_po 主檔及所有輔助檔案（previous、procurement_po、ap_invoice 等）。
    """

    def __init__(
        self,
        name: str = "SPTDataLoading",
        file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
        **kwargs
    ):
        super().__init__(
            name=name,
            file_paths=file_paths,
            description="Load all SPT PO files using datasources module",
            **kwargs
        )

    def get_required_file_type(self) -> str:
        return 'raw_po'


class SPTPRDataLoadingStep(SPTBaseDataLoadingStep):
    """
    SPT PR 數據載入步驟

    載入 raw_pr 主檔及所有輔助檔案（previous_pr、procurement_pr 等）。
    """

    def __init__(
        self,
        name: str = "SPTPRDataLoading",
        file_paths: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None,
        **kwargs
    ):
        super().__init__(
            name=name,
            file_paths=file_paths,
            description="Load all SPT PR files using datasources module",
            **kwargs
        )

    def get_required_file_type(self) -> str:
        return 'raw_pr'
