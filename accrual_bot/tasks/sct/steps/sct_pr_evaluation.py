"""
SCT PR ERM 邏輯判斷與評估 - 配置驅動版本
專門處理 PR (Purchase Request) 的狀態評估和會計欄位設置

與 PO 的核心差異：
1. 不判斷收貨狀態（無 Received Quantity 相關邏輯）
2. 不判斷入賬狀態（無 Billed Amount 相關邏輯）
3. 簡化的會計欄位設置（不處理預付款和負債科目）
4. Accr. Amount 直接使用 Entry Amount

狀態條件從 [sct_pr_erm_status_rules] 配置讀取，
由 ConditionEngine 依 priority 順序執行。
"""

import time
from typing import Dict, Any
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.engines import ConditionEngine
from accrual_bot.utils.config import config_manager


class SCTPRERMLogicStep(PipelineStep):
    """
    SCT PR ERM 邏輯步驟 - 配置驅動版本

    功能：
    1. 設置檔案日期
    2. 判斷 多 種 PR 狀態（從 [sct_pr_erm_status_rules] 配置讀取）
    3. 根據狀態設置是否估計入帳
    4. 設置會計相關欄位（簡化版本）

    與 PO 的主要差異：
    - 移除收貨相關判斷（無 Received Quantity）
    - 移除入賬相關判斷（無 Billed Amount）
    - 移除預付款處理
    - 移除負債科目設置
    - Accr. Amount 直接使用 Entry Amount
    """

    def __init__(self, name: str = "SCTPRERMLogic", **kwargs):
        super().__init__(
            name=name,
            description="Apply SCT PR ERM logic with config-driven status conditions",
            **kwargs
        )

        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list('FA_ACCOUNTS', 'sct', ['199999'])
        self.dept_accounts = config_manager.get_list('SCT', 'dept_accounts', [])

        # 初始化配置驅動引擎
        self.engine = ConditionEngine('sct_pr_erm_status_rules', entity_type='SCT')

        # 讀取 SCT 欄位預設值
        self.col_defaults = config_manager._config_toml.get('sct_column_defaults', {})

        self.logger.info(f"Initialized {name} for PR processing (config-driven)")

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 PR ERM 邏輯"""
        start_time = time.time()

        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date

            # 獲取參考數據
            ref_account = context.get_auxiliary_data('reference_account')

            if ref_account is None:
                raise ValueError("缺少參考數據：科目映射")

            self.logger.info(f"開始 SCT PR ERM 邏輯處理，處理日期：{processing_date}")

            # 階段 1: 設置基本欄位
            df = self._set_file_date(df, processing_date)

            # 階段 2: 確認狀態欄位
            status_column = self._get_status_column(df)

            # 階段 3: 應用配置驅動的 PR 狀態條件
            df = self._apply_pr_status_conditions(df, status_column, processing_date)

            # 階段 4: 處理格式錯誤
            df = self._handle_format_errors(df, status_column)

            # 階段 5: 設置是否估計入帳
            df = self._set_accrual_flag(df, status_column)

            # 階段 6: 設置會計欄位
            df = self._set_pr_accounting_fields(df, ref_account)

            # 更新上下文
            context.update_data(df)

            # 生成統計
            stats = self._generate_statistics(df, status_column)

            self.logger.info(
                f"SCT PR ERM 邏輯完成 - "
                f"需估列: {stats['accrual_count']:,} 筆, "
                f"總計: {stats['total_count']:,} 筆"
            )
            duration = time.time() - start_time

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"SCT PR ERM 邏輯已應用，{stats['accrual_count']:,} 筆需估列",
                duration=duration,
                metadata=stats
            )

        except Exception as e:
            self.logger.error(f"SCT PR ERM 邏輯處理失敗: {str(e)}", exc_info=True)
            context.add_error(f"SCT PR ERM 邏輯失敗: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    # ========== 階段 1: 基本設置 ==========

    def _set_file_date(self, df: pd.DataFrame, processing_date: int) -> pd.DataFrame:
        """設置檔案日期"""
        df['檔案日期'] = processing_date
        self.logger.debug(f"已設置檔案日期：{processing_date}")
        return df

    def _get_status_column(self, df: pd.DataFrame) -> str:
        """獲取狀態欄位名稱（PR 使用 'PR狀態'）"""
        if 'PR狀態' in df.columns:
            return 'PR狀態'
        else:
            df['PR狀態'] = np.nan
            return 'PR狀態'

    # ========== 階段 3: 應用狀態條件 ==========

    def _apply_pr_status_conditions(self, df: pd.DataFrame,
                                    status_column: str,
                                    processing_date: int) -> pd.DataFrame:
        """應用 PR 狀態判斷條件（配置驅動）"""
        engine_context: Dict[str, Any] = {
            'processing_date': processing_date,
            'prebuilt_masks': {},
        }

        self.logger.info("引擎驅動: 執行 SCT PR 配置化條件...")
        df, stats = self.engine.apply_rules(
            df, status_column, engine_context,
            processing_type='PR',
            update_no_status=True
        )

        total_hits = sum(stats.values())
        self.logger.info(
            f"SCT PR 引擎驅動完成: {len(stats)} 條規則, "
            f"共命中 {total_hits:,} 筆"
        )

        return df

    # ========== 階段 4: 處理格式錯誤 ==========

    def _handle_format_errors(self, df: pd.DataFrame,
                              status_column: str) -> pd.DataFrame:
        """處理格式錯誤與其他未匹配記錄"""
        no_status = (
            df[status_column].isna()
            | (df[status_column] == 'nan')
            | (df[status_column] == '')
        )

        # 格式錯誤
        format_error = df['YMs of Item Description'] == '100001,100002'
        mask_format_error = no_status & format_error
        df.loc[mask_format_error, status_column] = '格式錯誤，退單'

        error_count = mask_format_error.sum()
        if error_count > 0:
            self.logger.warning(f"發現 {error_count:,} 筆格式錯誤")

        # 其他（更新 no_status 後）
        no_status = (
            df[status_column].isna()
            | (df[status_column] == 'nan')
            | (df[status_column] == '')
        )
        df.loc[no_status, status_column] = '其他'

        other_count = no_status.sum()
        if other_count > 0:
            self.logger.info(f"其他: {other_count:,} 筆")

        return df

    # ========== 階段 5: 設置是否估計入帳 ==========

    def _set_accrual_flag(self, df: pd.DataFrame, status_column: str) -> pd.DataFrame:
        """根據 PR狀態 設置是否估計入帳"""
        df['是否估計入帳'] = 'N'

        mask_need_accrual = df[status_column].str.contains('已完成', na=False)

        df.loc[mask_need_accrual, '是否估計入帳'] = 'Y'

        accrual_count = mask_need_accrual.sum()
        self.logger.info(f"設置估列標記：{accrual_count:,} 筆需估列")

        return df

    # ========== 階段 6: 設置會計欄位 ==========

    def _set_pr_accounting_fields(self, df: pd.DataFrame,
                                  ref_account: pd.DataFrame) -> pd.DataFrame:
        """設置 PR 會計相關欄位（簡化版本，無預付款/負債科目）"""
        need_accrual = df['是否估計入帳'] == 'Y'

        if not need_accrual.any():
            self.logger.info("無需估列記錄，跳過會計欄位設置")
            return df

        accrual_count = need_accrual.sum()
        self.logger.info(f"處理 {accrual_count:,} 筆需估列記錄...")

        # 1. Account code
        df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']

        # 2. Account Name
        df = self._set_account_name(df, ref_account, need_accrual)

        # 3. Product code
        df.loc[need_accrual, 'Product code'] = df.loc[need_accrual, 'Product Code']

        # 4. Region_c
        df.loc[need_accrual, 'Region_c'] = self.col_defaults.get('region', 'TW')

        # 5. Dep.
        df = self._set_department(df, need_accrual)

        # 6. Currency_c
        df.loc[need_accrual, 'Currency_c'] = df.loc[need_accrual, 'Currency']

        # 7. Accr. Amount（PR 直接使用 Entry Amount）
        df.loc[need_accrual, 'Accr. Amount'] = df.loc[
            need_accrual, 'Entry Amount'
        ].astype('Float64')

        self.logger.info("會計欄位設置完成")

        return df

    def _set_account_name(self, df: pd.DataFrame, ref_account: pd.DataFrame,
                          mask: pd.Series) -> pd.DataFrame:
        """設置會計科目名稱（從 ref_SCTTW.xlsx 查詢）"""
        if ref_account.empty:
            self.logger.warning("參考科目資料為空，無法設置 Account Name")
            return df

        merged = pd.merge(
            df,
            ref_account[['Account', 'Account Desc']],
            how='left',
            left_on='Account code',
            right_on='Account'
        )

        df['Account Name'] = merged['Account Desc']

        missing_count = df.loc[mask, 'Account Name'].isna().sum()
        if missing_count > 0:
            self.logger.warning(f"{missing_count:,} 筆記錄無法找到對應的 Account Name")

        return df

    def _set_department(self, df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
        """設置部門代碼"""
        isin_dept = df['Account code'].astype('string').isin(
            [str(x) for x in self.dept_accounts]
        )

        dept_mask = mask & isin_dept
        if dept_mask.any():
            df.loc[dept_mask, 'Dep.'] = df.loc[dept_mask, 'Department'].str[:3]

        non_dept_mask = mask & ~isin_dept
        if non_dept_mask.any():
            df.loc[non_dept_mask, 'Dep.'] = self.col_defaults.get(
                'default_department', '000'
            )

        return df

    # ========== 輔助方法 ==========

    def _generate_statistics(self, df: pd.DataFrame, status_column: str) -> Dict[str, Any]:
        """生成統計資訊"""
        total_count = len(df)
        accrual_count = int((df['是否估計入帳'] == 'Y').sum())

        stats: Dict[str, Any] = {
            'total_count': total_count,
            'accrual_count': accrual_count,
            'accrual_percentage': round(
                (accrual_count / total_count * 100), 2
            ) if total_count > 0 else 0,
            'status_distribution': {}
        }

        if status_column in df.columns:
            status_counts = df[status_column].value_counts().to_dict()
            stats['status_distribution'] = {
                str(k): int(v) for k, v in status_counts.items()
            }

        return stats

    # ========== 驗證方法 ==========

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        df = context.data

        if df is None or df.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False

        required_columns = [
            'GL#',
            'Expected Received Month_轉換格式',
            'YMs of Item Description',
            'Item Description',
            'Remarked by Procurement',
            'Remarked by 上月 FN',
            'Currency',
            'Product Code',
            'Entry Amount',
            'Department'
        ]

        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            self.logger.error(f"缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False

        ref_account = context.get_auxiliary_data('reference_account')

        if ref_account is None:
            self.logger.error("缺少參考數據：科目映射")
            context.add_error("缺少參考數據：科目映射")
            return False

        processing_date = context.metadata.processing_date
        if not processing_date:
            self.logger.error("缺少處理日期")
            context.add_error("缺少處理日期")
            return False

        self.logger.info("輸入驗證通過")
        return True

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作"""
        self.logger.warning(f"回滾 SCT PR ERM 邏輯：{str(error)}")
