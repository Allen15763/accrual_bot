"""
SCT PPE 資產狀態更新步驟

針對含有 PPE 相關品項（訂金、安裝款、驗收款、保固）的 PO，
以驗收款品項的 ERM 日期為基準判斷整筆 PO 狀態。

與 SPX DepositStatusUpdateStep 的差異：
1. 不使用「最大 ERM」策略，改用「驗收款 ERM」策略
2. 保固品項明確排除於驗收判定外
3. 無驗收品項時 fallback 到採購備註
4. PPE PO 辨識：FA 科目 OR 關鍵字（雙重觸發）
"""

import time
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager


class SCTAssetStatusUpdateStep(PipelineStep):
    """
    SCT PPE PO 狀態更新步驟

    業務邏輯：
    1. 辨識 PPE 品項：GL# 為 FA 科目 OR Item Description 含觸發關鍵字
    2. 收集含 PPE 品項的 PO# → PPE PO 群組
    3. 每個 PPE PO 內找驗收品項（含「驗收」但不含「保固」）
    4. 有驗收品項：以驗收 ERM 判定狀態
       - ERM ≤ processing_date → 已完成(PPE)
       - ERM > processing_date → 未完成(PPE)
    5. 無驗收品項：fallback 到採購備註
    6. 保護狀態（已入帳、上期已入PPE 等）永不覆蓋
    """

    def __init__(self, name: str = "SCTAssetStatusUpdate", **kwargs):
        super().__init__(
            name=name,
            description="Update PPE PO status based on acceptance item ERM",
            **kwargs
        )

        # 從配置讀取 PPE 參數
        ppe_config = config_manager._config_toml.get('sct', {}).get('ppe', {})
        self.trigger_keywords: List[str] = ppe_config.get(
            'trigger_keywords', ['訂金', '安裝款', '驗收款', '保固']
        )
        self.acceptance_keyword: str = ppe_config.get('acceptance_keyword', '驗收')
        self.warranty_keyword: str = ppe_config.get('warranty_keyword', '保固')
        self.completed_status: str = ppe_config.get('completed_status', '已完成(PPE)')
        self.incomplete_status: str = ppe_config.get('incomplete_status', '未完成(PPE)')
        self.protected_statuses: List[str] = ppe_config.get('protected_statuses', [
            '已入帳', '上期已入PPE', '上期FN備註已完成或Voucher number',
            'Outright', 'Consignment', 'Outsourcing',
        ])

        # FA 科目
        self.fa_accounts = config_manager.get_list('FA_ACCOUNTS', 'sct', ['199999'])

        # 欄位名稱
        self.po_column = 'PO#'
        self.description_column = 'Item Description'
        self.date_column = 'Expected Received Month_轉換格式'
        self.status_column = 'PO狀態'

        self.logger.info(
            f"Initialized {name}: trigger={self.trigger_keywords}, "
            f"acceptance='{self.acceptance_keyword}', "
            f"fa_accounts={self.fa_accounts}"
        )

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 PPE 資產狀態更新"""
        start_time = time.time()

        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date

            self.logger.info(
                f"開始 SCT PPE 資產狀態更新，處理日期：{processing_date}，"
                f"總記錄數：{len(df):,}"
            )

            # 階段 1: 辨識 PPE PO
            ppe_pos = self._identify_ppe_pos(df)

            if not ppe_pos:
                self.logger.info("無 PPE PO，跳過狀態更新")
                duration = time.time() - start_time
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="無 PPE PO，已跳過",
                    duration=duration,
                    metadata={'ppe_po_count': 0}
                )

            self.logger.info(f"辨識出 {len(ppe_pos):,} 個 PPE PO")

            # 階段 2: 逐 PO 判定狀態
            update_stats = self._process_ppe_pos(df, ppe_pos, processing_date)

            # 階段 3: 更新估列標記
            self._update_accrual_flag(df, update_stats['updated_mask'])

            # 更新上下文
            context.update_data(df)

            # 生成統計
            stats = self._generate_statistics(df, ppe_pos, update_stats)

            self.logger.info(
                f"SCT PPE 狀態更新完成 - "
                f"PPE PO: {len(ppe_pos)} 個, "
                f"更新: {update_stats['updated_count']:,} 筆"
            )
            duration = time.time() - start_time

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=(
                    f"PPE PO {len(ppe_pos)} 個，"
                    f"更新 {update_stats['updated_count']:,} 筆"
                ),
                duration=duration,
                metadata=stats
            )

        except Exception as e:
            self.logger.error(f"SCT PPE 狀態更新失敗: {str(e)}", exc_info=True)
            context.add_error(f"SCT PPE 狀態更新失敗: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    # ========== 階段 1: 辨識 PPE PO ==========

    def _identify_ppe_pos(self, df: pd.DataFrame) -> List[str]:
        """
        辨識包含 PPE 品項的 PO#

        觸發條件（OR）：
        - GL# 為 FA 科目
        - Item Description 含任一觸發關鍵字
        """
        if self.po_column not in df.columns:
            self.logger.warning(f"缺少 {self.po_column} 欄位")
            return []

        # 條件 1: FA 科目
        fa_mask = df['GL#'].astype('string').isin(
            [str(x) for x in self.fa_accounts]
        )

        # 條件 2: 觸發關鍵字
        keyword_pattern = '|'.join(self.trigger_keywords)
        keyword_mask = df[self.description_column].str.contains(
            keyword_pattern, case=False, na=False
        )

        ppe_items = fa_mask | keyword_mask
        ppe_pos = df.loc[ppe_items, self.po_column].unique().tolist()

        self.logger.debug(
            f"PPE 辨識: FA科目 {fa_mask.sum()} 筆, "
            f"關鍵字 {keyword_mask.sum()} 筆, "
            f"涉及 PO {len(ppe_pos)} 個"
        )

        return ppe_pos

    # ========== 階段 2: 逐 PO 判定狀態 ==========

    def _process_ppe_pos(
        self,
        df: pd.DataFrame,
        ppe_pos: List[str],
        processing_date: int
    ) -> Dict[str, Any]:
        """處理每個 PPE PO 的狀態判定"""
        updated_count = 0
        completed_pos = 0
        incomplete_pos = 0
        fallback_pos = 0
        skipped_pos = 0
        updated_mask = pd.Series(False, index=df.index)

        for po_num in ppe_pos:
            po_mask = df[self.po_column] == po_num

            # 排除保護狀態的品項
            protected = df[self.status_column].isin(self.protected_statuses)
            updatable = po_mask & ~protected

            if not updatable.any():
                skipped_pos += 1
                continue

            # 找驗收品項（含「驗收」但不含「保固」）
            acceptance_erm = self._find_acceptance_erm(df, po_mask)

            if acceptance_erm is not None:
                # 有驗收品項：以驗收 ERM + 採購備註雙重判定
                procurement_completed = df.loc[
                    po_mask, 'Remarked by Procurement'
                ].str.contains('(?i)已完成', na=False).any()

                if acceptance_erm <= processing_date and procurement_completed:
                    status = self.completed_status
                    note = (
                        f"PPE PO 以驗收款 ERM({acceptance_erm}) "
                        f"≤ 處理日期({processing_date}) + 採購備註已完成，判定已完成"
                    )
                    completed_pos += 1
                else:
                    status = self.incomplete_status
                    if acceptance_erm > processing_date:
                        note = (
                            f"PPE PO 以驗收款 ERM({acceptance_erm}) "
                            f"> 處理日期({processing_date})，判定未完成"
                        )
                    else:
                        note = (
                            f"PPE PO 以驗收款 ERM({acceptance_erm}) "
                            f"≤ 處理日期({processing_date})，"
                            f"但採購備註未確認已完成，判定未完成"
                        )
                    incomplete_pos += 1

                df.loc[updatable, self.status_column] = status
                df.loc[updatable, 'matched_condition_on_status'] = note
                updated_mask = updated_mask | updatable
                updated_count += updatable.sum()

            else:
                # 無驗收品項：fallback 到採購備註
                procurement_completed = df.loc[
                    po_mask, 'Remarked by Procurement'
                ].str.contains('(?i)已完成', na=False).any()

                if procurement_completed:
                    df.loc[updatable, self.status_column] = self.completed_status
                    df.loc[updatable, 'matched_condition_on_status'] = (
                        "PPE PO 無驗收品項，依採購備註「已完成」判定"
                    )
                    updated_mask = updated_mask | updatable
                    updated_count += updatable.sum()
                    fallback_pos += 1
                else:
                    # 無驗收 + 無採購備註 → 維持原狀態
                    skipped_pos += 1
                    self.logger.debug(
                        f"PO {po_num}: 無驗收品項且無採購備註，維持原狀態"
                    )

        self.logger.info(
            f"PPE PO 處理: 完成 {completed_pos}, 未完成 {incomplete_pos}, "
            f"fallback {fallback_pos}, 跳過 {skipped_pos}"
        )

        return {
            'updated_count': int(updated_count),
            'completed_pos': completed_pos,
            'incomplete_pos': incomplete_pos,
            'fallback_pos': fallback_pos,
            'skipped_pos': skipped_pos,
            'updated_mask': updated_mask,
        }

    def _find_acceptance_erm(
        self, df: pd.DataFrame, po_mask: pd.Series
    ) -> Optional[int]:
        """
        找 PO 中驗收品項的 max ERM

        驗收品項：Item Description 含 acceptance_keyword 且不含 warranty_keyword
        """
        po_items = df.loc[po_mask]

        acceptance_mask = (
            po_items[self.description_column].str.contains(
                self.acceptance_keyword, case=False, na=False
            )
            & ~po_items[self.description_column].str.contains(
                self.warranty_keyword, case=False, na=False
            )
        )

        acceptance_items = po_items.loc[acceptance_mask]

        if acceptance_items.empty:
            return None

        # 取非 NaN 的 max ERM
        erm_values = acceptance_items[self.date_column].dropna()

        if erm_values.empty:
            return None

        return erm_values.max()

    # ========== 階段 3: 更新估列標記 ==========

    def _update_accrual_flag(
        self, df: pd.DataFrame, updated_mask: pd.Series
    ) -> None:
        """重新設定受影響列的估列標記"""
        if not updated_mask.any():
            return

        # 對更新的列重新判定
        need_accrual = updated_mask & df[self.status_column].str.contains(
            '已完成', na=False
        )
        no_accrual = updated_mask & ~df[self.status_column].str.contains(
            '已完成', na=False
        )

        df.loc[need_accrual, '是否估計入帳'] = 'Y'
        df.loc[no_accrual, '是否估計入帳'] = 'N'

        accrual_count = need_accrual.sum()
        self.logger.info(f"PPE 估列標記更新：{accrual_count:,} 筆需估列")

    # ========== 輔助方法 ==========

    def _generate_statistics(
        self,
        df: pd.DataFrame,
        ppe_pos: List[str],
        update_stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """生成統計資訊"""
        return {
            'total_count': len(df),
            'ppe_po_count': len(ppe_pos),
            'updated_count': update_stats['updated_count'],
            'completed_pos': update_stats['completed_pos'],
            'incomplete_pos': update_stats['incomplete_pos'],
            'fallback_pos': update_stats['fallback_pos'],
            'skipped_pos': update_stats['skipped_pos'],
        }

    # ========== 驗證方法 ==========

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        df = context.data

        if df is None or df.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False

        required_columns = [
            self.po_column,
            self.description_column,
            self.date_column,
            self.status_column,
            'GL#',
            'Remarked by Procurement',
            '是否估計入帳',
        ]

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            self.logger.error(f"缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False

        processing_date = context.metadata.processing_date
        if not processing_date:
            self.logger.error("缺少處理日期")
            context.add_error("缺少處理日期")
            return False

        self.logger.info("PPE 步驟輸入驗證通過")
        return True

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作"""
        self.logger.warning(f"回滾 SCT PPE 狀態更新：{str(error)}")
