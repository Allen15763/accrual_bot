"""
SCT 差異分析 - 預處理步驟

負責：
1. 欄位正規化（小寫 + 空白轉底線）
2. 欄位別名解析（兼容原始/已清理的欄位名稱）
3. 前期特有：公式合成欄位（如 po# + line# → po_line）
4. 篩選 + 選取標準化欄位

所有配置均從 TOML [sct.variance] 讀取。
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.logging import get_logger


class SCTVariancePreprocessingStep(PipelineStep):
    """
    SCT 差異分析預處理步驟

    處理流程（當期/前期共用）：
    1. 欄位正規化：小寫 + 空白轉底線
    2. 欄位別名解析：根據 column_aliases 將各種來源名稱統一為標準名
    3. （前期限定）公式合成：根據 previous_column_formulas 合成缺失欄位
    4. 篩選：依配置條件過濾列
    5. 選取 standard_columns 輸出

    無論輸入是原始底稿或已清理過的底稿，最終輸出欄位一致。
    """

    def __init__(self, name: str = "SCTVariancePreprocessing", **kwargs):
        super().__init__(name=name, **kwargs)
        self.logger = get_logger(__name__)
        self.config = config_manager._config_toml.get('sct', {}).get('variance', {})

    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        預處理當期與前期底稿
        """
        try:
            current_df = context.data
            previous_df = context.get_auxiliary_data('previous_worksheet')

            if current_df is None or previous_df is None:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="當期或前期底稿未載入",
                )

            standard_columns = self.config.get('standard_columns', [
                'item_description', 'po_line', 'account_code', 'currency_c', 'amount',
            ])
            aliases = self.config.get('column_aliases', {})

            # 處理當期底稿
            current_processed = self._process_worksheet(
                df=current_df,
                label="當期",
                filter_col=self.config.get('current_filter_column', '是否估計入帳'),
                filter_val=self.config.get('current_filter_value', 'Y'),
                aliases=aliases,
                standard_columns=standard_columns,
            )
            self.logger.info(
                f"當期底稿預處理完成: {current_df.shape} → {current_processed.shape}"
            )

            # 處理前期底稿（額外支援公式合成）
            previous_processed = self._process_worksheet(
                df=previous_df,
                label="前期",
                filter_col=self.config.get('previous_filter_column', '是否需要估計入帳'),
                filter_val=self.config.get('previous_filter_value', 'Y'),
                aliases=aliases,
                standard_columns=standard_columns,
                formulas=self.config.get('previous_column_formulas', {}),
            )
            self.logger.info(
                f"前期底稿預處理完成: {previous_df.shape} → {previous_processed.shape}"
            )

            # 更新 context
            context.data = current_processed
            context.set_auxiliary_data('previous_worksheet', previous_processed)

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=(
                    f"預處理完成: 當期 {current_processed.shape}, "
                    f"前期 {previous_processed.shape}"
                ),
                metadata={
                    'current_rows_after_filter': len(current_processed),
                    'previous_rows_after_filter': len(previous_processed),
                },
            )

        except Exception as e:
            self.logger.error(f"預處理失敗: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"預處理失敗: {e}",
            )

    def _process_worksheet(
        self,
        df: pd.DataFrame,
        label: str,
        filter_col: str,
        filter_val: str,
        aliases: Dict[str, List[str]],
        standard_columns: List[str],
        formulas: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> pd.DataFrame:
        """
        統一處理流程：正規化 → 別名解析 → 公式合成 → 篩選 → 選取標準欄位

        Args:
            df: 原始 DataFrame
            label: 標籤（"當期"/"前期"），用於 log
            filter_col: 篩選欄位名
            filter_val: 篩選值
            aliases: 欄位別名表 {標準名: [可能來源名...]}
            standard_columns: 最終輸出欄位清單
            formulas: 公式合成規則（前期專用）
        """
        df = df.copy()
        self.logger.debug(f"{label}原始欄位: {list(df.columns)}")

        # 1. 欄位正規化：小寫 + 空白轉底線
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        self.logger.debug(f"{label}正規化後欄位: {list(df.columns)}")

        # 2. 欄位別名解析：將各種來源名統一為標準名
        rename_map = self._resolve_aliases(df, aliases)
        if rename_map:
            df = df.rename(columns=rename_map)
            self.logger.debug(f"{label}別名解析: {rename_map}")

        # 3. 公式合成（前期專用）：當別名解析後仍缺少的欄位，嘗試用公式合成
        if formulas:
            for target_col, formula_info in formulas.items():
                if target_col not in df.columns:
                    self._apply_formula(df, target_col, formula_info, label)

        # 4. 篩選
        if filter_col in df.columns:
            df = df.query(f"`{filter_col}` == @filter_val")
            self.logger.debug(f"{label}篩選 {filter_col}=={filter_val}: {len(df)} 筆")

        # 5. 選取標準欄位
        available = [c for c in standard_columns if c in df.columns]
        missing = [c for c in standard_columns if c not in df.columns]
        if missing:
            self.logger.warning(
                f"{label}底稿缺少標準欄位: {missing}（正規化後可用: {list(df.columns)}）"
            )
        df = df[available]

        return df.reset_index(drop=True)

    @staticmethod
    def _resolve_aliases(
        df: pd.DataFrame, aliases: Dict[str, List[str]]
    ) -> Dict[str, str]:
        """
        根據別名表解析欄位名稱

        對每個標準欄位，如果 df 中不存在該名稱，
        則從別名清單中找第一個存在的來源欄位，建立 rename mapping。

        Returns:
            {來源欄位名: 標準欄位名} 的 rename dict
        """
        rename_map: Dict[str, str] = {}
        for target, source_names in aliases.items():
            if target in df.columns:
                continue  # 已存在，不需解析
            for source in source_names:
                if source in df.columns and source != target:
                    rename_map[source] = target
                    break
        return rename_map

    def _apply_formula(
        self,
        df: pd.DataFrame,
        target_col: str,
        formula_info: Dict[str, Any],
        label: str,
    ) -> None:
        """
        根據公式合成缺失欄位

        支援：
        - type="concat", formula="col1 + col2": 串接多個欄位
        """
        if not isinstance(formula_info, dict):
            return

        if formula_info.get('type') == 'concat' and 'formula' in formula_info:
            parts = [p.strip() for p in formula_info['formula'].split('+')]
            if all(p in df.columns for p in parts):
                df[target_col] = df[parts[0]].astype(str)
                for part in parts[1:]:
                    df[target_col] = df[target_col] + df[part].astype(str)
                self.logger.debug(f"{label}公式合成 {target_col} ← concat({parts})")
            else:
                missing_parts = [p for p in parts if p not in df.columns]
                self.logger.warning(
                    f"{label}公式合成 {target_col} 失敗: 缺少欄位 {missing_parts}"
                )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證 context 中有當期和前期資料"""
        if context.data is None or context.data.empty:
            self.logger.error("當期底稿為空")
            return False
        if context.get_auxiliary_data('previous_worksheet') is None:
            self.logger.error("前期底稿未載入")
            return False
        return True
