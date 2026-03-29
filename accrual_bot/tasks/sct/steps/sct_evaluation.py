"""
SCT ERM 邏輯判斷與評估 - 配置驅動版本
專門處理 PO (Purchase Order) 的狀態評估和會計欄位設置

模仿 SPX 的 SPXERMLogicStep 架構，使用共用 ConditionEngine。
初始業務規則與 SPX 相同，但從 [sct_erm_status_rules] 配置讀取，
方便日後獨立演進。

與 SPX 的差異：
- 使用 ref_SCTTW.xlsx 作為科目映射參考
- entity_type='SCT'，is_fa 讀取 fa_accounts.sct
- 配置從 sct_column_defaults / sct_erm_status_rules 讀取
"""

import time
from dataclasses import dataclass
from typing import Dict, Any
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.engines import ConditionEngine
from accrual_bot.utils.config import config_manager


@dataclass
class SCTERMConditions:
    """SCT ERM 判斷條件集合"""
    # 基礎條件
    no_status: pd.Series
    in_date_range: pd.Series
    erm_before_or_equal_file_date: pd.Series
    erm_after_file_date: pd.Series
    quantity_matched: pd.Series
    not_billed: pd.Series
    has_billing: pd.Series
    fully_billed: pd.Series
    has_unpaid_amount: pd.Series

    # 備註條件
    procurement_completed_or_rent: pd.Series
    fn_completed_or_posted: pd.Series
    pr_not_incomplete: pd.Series

    # FA 條件
    is_fa: pd.Series

    # 錯誤條件
    procurement_not_error: pd.Series
    out_of_date_range: pd.Series
    format_error: pd.Series


class SCTERMLogicStep(PipelineStep):
    """
    SCT ERM 邏輯步驟 - 配置驅動版本

    功能：
    1. 設置檔案日期
    2. 判斷多種 PO 狀態（從 [sct_erm_status_rules] 配置讀取）
    3. 根據狀態設置是否估計入帳
    4. 設置會計相關欄位（Account code, Product code, Dep.等）
    5. 計算預估金額（Accr. Amount）
    6. 處理預付款和負債科目
    7. 檢查 PR Product Code

    業務規則：
    - 「已完成」狀態的項目需要估列入帳
    - 其他狀態一律不估列（是否估計入帳 = N）
    - ERM 條件由配置引擎依 priority 順序執行
    """

    def __init__(self, name: str = "SCTERMLogic", **kwargs):
        super().__init__(
            name=name,
            description="Apply SCT ERM logic with config-driven status conditions",
            **kwargs
        )

        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list('FA_ACCOUNTS', 'sct', ['199999'])
        self.dept_accounts = config_manager.get_list('SCT', 'dept_accounts', [])

        # 初始化配置驅動引擎
        self.engine = ConditionEngine('sct_erm_status_rules', entity_type='SCT')

        # 讀取 SCT 欄位預設值
        self.col_defaults = config_manager._config_toml.get('sct_column_defaults', {})

        self.logger.info(f"Initialized {name} with FA accounts: {self.fa_accounts}")

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 ERM 邏輯"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date

            # 獲取參考數據
            ref_account = context.get_auxiliary_data('reference_account')
            ref_liability = context.get_auxiliary_data('reference_liability')

            if ref_account is None or ref_liability is None:
                raise ValueError("缺少參考數據：科目映射或負債科目")

            self.logger.info(f"開始 SCT ERM 邏輯處理，處理日期：{processing_date}")

            # 階段 1: 設置基本欄位
            df = self._set_file_date(df, processing_date)

            # 階段 2: 構建判斷條件
            status_column = self._get_status_column(df, context)
            conditions = self._build_conditions(df, processing_date, status_column)

            # 階段 3: 應用狀態條件
            df = self._apply_status_conditions(df, conditions, status_column)

            # 階段 4: 處理格式錯誤
            df = self._handle_format_errors(df, conditions, status_column)

            # 階段 5: 設置是否估計入帳
            df = self._set_accrual_flag(df, status_column)

            # 階段 6: 設置會計欄位
            df = self._set_accounting_fields(df, ref_account, ref_liability)

            # 階段 7: 檢查 PR Product Code
            df = self._check_pr_product_code(df)

            # 更新上下文
            context.update_data(df)

            # 生成統計
            stats = self._generate_statistics(df, status_column)

            self.logger.info(
                f"SCT ERM 邏輯完成 - "
                f"需估列: {stats['accrual_count']} 筆, "
                f"總計: {stats['total_count']} 筆"
            )
            duration = time.time() - start_time

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"SCT ERM 邏輯已應用，{stats['accrual_count']} 筆需估列",
                duration=duration,
                metadata=stats
            )

        except Exception as e:
            self.logger.error(f"SCT ERM 邏輯處理失敗: {str(e)}", exc_info=True)
            context.add_error(f"SCT ERM 邏輯失敗: {str(e)}")
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

    def _get_status_column(self, df: pd.DataFrame, context: ProcessingContext) -> str:
        """動態判斷狀態欄位"""
        if 'PO狀態' in df.columns:
            return 'PO狀態'
        elif 'PR狀態' in df.columns:
            return 'PR狀態'
        else:
            processing_type = context.metadata.processing_type
            return f"{processing_type}狀態"

    # ========== 階段 2: 構建條件 ==========

    def _build_conditions(self, df: pd.DataFrame, file_date: int,
                          status_column: str) -> SCTERMConditions:
        """構建所有判斷條件"""
        # 基礎狀態條件
        no_status = (df[status_column].isna()) | (df[status_column] == '') | (df[status_column] == 'nan')

        # 日期範圍條件
        ym_start = df['YMs of Item Description'].str[:6].astype('Int32')
        ym_end = df['YMs of Item Description'].str[7:].astype('Int32')
        erm = df['Expected Received Month_轉換格式']

        in_date_range = erm.between(ym_start, ym_end, inclusive='both')
        erm_before_or_equal_file_date = erm <= file_date
        erm_after_file_date = erm > file_date

        # 數量條件
        quantity_matched = df['Entry Quantity'] == df['Received Quantity']

        # 帳務條件
        not_billed = df['Entry Billed Amount'].astype('Float64') == 0
        has_billing = df['Billed Quantity'] != '0'
        fully_billed = (
            df['Entry Amount'].astype('Float64') -
            df['Entry Billed Amount'].astype('Float64')
        ) == 0
        has_unpaid_amount = (
            df['Entry Amount'].astype('Float64') -
            df['Entry Billed Amount'].astype('Float64')
        ) != 0

        # 備註條件
        procurement_completed_or_rent = df['Remarked by Procurement'].str.contains(
            '(?i)已完成|rent', na=False
        )
        fn_completed_or_posted = df['Remarked by 上月 FN'].str.contains(
            '(?i)已完成|已入帳', na=False
        )
        pr_not_incomplete = ~df['Remarked by 上月 FN PR'].str.contains(
            '(?i)未完成', na=False
        )

        # FA 條件
        is_fa = df['GL#'].astype('string').isin([str(x) for x in self.fa_accounts])

        # 錯誤條件
        procurement_not_error = df['Remarked by Procurement'] != 'error'
        out_of_date_range = (
            (in_date_range == False) &
            (df['YMs of Item Description'] != '100001,100002')
        )
        format_error = df['YMs of Item Description'] == '100001,100002'

        return SCTERMConditions(
            no_status=no_status,
            in_date_range=in_date_range,
            erm_before_or_equal_file_date=erm_before_or_equal_file_date,
            erm_after_file_date=erm_after_file_date,
            quantity_matched=quantity_matched,
            not_billed=not_billed,
            has_billing=has_billing,
            fully_billed=fully_billed,
            has_unpaid_amount=has_unpaid_amount,
            procurement_completed_or_rent=procurement_completed_or_rent,
            fn_completed_or_posted=fn_completed_or_posted,
            pr_not_incomplete=pr_not_incomplete,
            is_fa=is_fa,
            procurement_not_error=procurement_not_error,
            out_of_date_range=out_of_date_range,
            format_error=format_error
        )

    # ========== 階段 3: 應用狀態條件 ==========

    def _apply_status_conditions(self, df: pd.DataFrame,
                                 cond: SCTERMConditions,
                                 status_column: str) -> pd.DataFrame:
        """應用 ERM 狀態判斷條件（配置驅動）"""
        prebuilt_masks = {
            'no_status': cond.no_status,
            'erm_in_range': cond.in_date_range,
            'erm_le_date': cond.erm_before_or_equal_file_date,
            'erm_gt_date': cond.erm_after_file_date,
            'qty_matched': cond.quantity_matched,
            'not_billed': cond.not_billed,
            'has_billing': cond.has_billing,
            'fully_billed': cond.fully_billed,
            'has_unpaid': cond.has_unpaid_amount,
            'remark_completed': (cond.procurement_completed_or_rent
                                 | cond.fn_completed_or_posted),
            'pr_not_incomplete': cond.pr_not_incomplete,
            'is_fa': cond.is_fa,
            'not_fa': ~cond.is_fa,
            'not_error': cond.procurement_not_error,
            'out_of_range': cond.out_of_date_range,
            'format_error': cond.format_error,
        }

        engine_context = {
            'processing_date': df['檔案日期'].iloc[0] if '檔案日期' in df.columns else None,
            'prebuilt_masks': prebuilt_masks,
        }

        self.logger.info("引擎驅動: 執行 SCT ERM 配置化條件...")
        df, stats = self.engine.apply_rules(
            df, status_column, engine_context,
            processing_type='PO' if status_column == 'PO狀態' else 'PR',
            update_no_status=True
        )

        total_hits = sum(stats.values())
        self.logger.info(
            f"SCT ERM 引擎驅動完成: {len(stats)} 條規則, "
            f"共命中 {total_hits:,} 筆"
        )

        # 更新狀態條件遮罩 for _handle_format_errors
        cond.no_status = (df[status_column].isna()) | (df[status_column] == '') | (df[status_column] == 'nan')

        return df

    # ========== 階段 4: 處理格式錯誤 ==========

    def _handle_format_errors(self, df: pd.DataFrame,
                              cond: SCTERMConditions,
                              status_column: str) -> pd.DataFrame:
        """處理格式錯誤的記錄"""
        mask_format_error = cond.no_status & cond.format_error
        df.loc[mask_format_error, status_column] = '格式錯誤，退單'

        error_count = mask_format_error.sum()
        if error_count > 0:
            self.logger.warning(f"發現 {error_count} 筆格式錯誤")

        return df

    # ========== 階段 5: 設置是否估計入帳 ==========

    def _set_accrual_flag(self, df: pd.DataFrame, status_column: str) -> pd.DataFrame:
        """根據 PO/PR狀態 設置是否估計入帳（已完成→Y, 其他→N）"""
        mask_completed = df[status_column].str.contains('已完成', na=False)

        df.loc[mask_completed, '是否估計入帳'] = 'Y'
        df.loc[~mask_completed, '是否估計入帳'] = 'N'

        accrual_count = mask_completed.sum()
        self.logger.info(f"設置估列標記：{accrual_count} 筆需估列")

        return df

    # ========== 階段 6: 設置會計欄位 ==========

    def _set_accounting_fields(self, df: pd.DataFrame,
                               ref_account: pd.DataFrame,
                               ref_liability: pd.DataFrame) -> pd.DataFrame:
        """設置所有會計相關欄位"""
        need_accrual = df['是否估計入帳'] == 'Y'

        if not need_accrual.any():
            self.logger.info("無需估列記錄，跳過會計欄位設置")
            return df

        # 1. Account code
        df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']

        # 2. Account Name（通過 merge）
        df = self._set_account_name(df, ref_account)

        # 3. Product code
        df.loc[need_accrual, 'Product code'] = df.loc[need_accrual, 'Product Code']

        # 4. Region_c
        df.loc[need_accrual, 'Region_c'] = self.col_defaults.get('region', 'TW')

        # 5. Dep.（部門代碼）
        df = self._set_department(df, need_accrual)

        # 6. Currency_c
        df.loc[need_accrual, 'Currency_c'] = df.loc[need_accrual, 'Currency']

        # 7. Accr. Amount（預估金額）
        df = self._calculate_accrual_amount(df, need_accrual)

        # 8. 預付款處理
        df = self._handle_prepayment(df, need_accrual, ref_liability)

        self.logger.info("會計欄位設置完成")

        return df

    def _set_account_name(self, df: pd.DataFrame,
                          ref_account: pd.DataFrame) -> pd.DataFrame:
        """設置會計科目名稱（從 ref_SCTTW.xlsx 查詢）"""
        if ref_account.empty:
            self.logger.warning("參考科目資料為空")
            return df

        merged = pd.merge(
            df,
            ref_account[['Account', 'Account Desc']],
            how='left',
            left_on='Account code',
            right_on='Account'
        )

        df['Account Name'] = merged['Account Desc']

        return df

    def _set_department(self, df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
        """設置部門代碼（dept_accounts 中取前3碼，否則 '000'）"""
        isin_dept = df['Account code'].astype('string').isin(
            [str(x) for x in self.dept_accounts]
        )

        df.loc[mask & isin_dept, 'Dep.'] = \
            df.loc[mask & isin_dept, 'Department'].str[:3]

        df.loc[mask & ~isin_dept, 'Dep.'] = self.col_defaults.get(
            'default_department', '000'
        )

        return df

    def _calculate_accrual_amount(self, df: pd.DataFrame,
                                  mask: pd.Series) -> pd.DataFrame:
        """計算預估金額：Unit Price × (Entry Quantity - Billed Quantity)"""
        df['temp_amount'] = (
            df['Unit Price'].astype('Float64') *
            (df['Entry Quantity'].astype('Float64') -
             df['Billed Quantity'].astype('Float64'))
        )

        df.loc[mask, 'Accr. Amount'] = df.loc[mask, 'temp_amount']
        df.drop('temp_amount', axis=1, inplace=True)

        return df

    def _handle_prepayment(self, df: pd.DataFrame, mask: pd.Series,
                           ref_liability: pd.DataFrame) -> pd.DataFrame:
        """處理預付款和負債科目"""
        is_prepayment = df['Entry Prepay Amount'] != '0'
        df.loc[mask & is_prepayment, '是否有預付'] = 'Y'

        # 設置 Liability（無預付款的情況）
        if not ref_liability.empty:
            merged = pd.merge(
                df,
                ref_liability[['Account', 'Liability']],
                how='left',
                left_on='Account code',
                right_on='Account'
            )
            df['Liability'] = merged['Liability_y']

        # 有預付款的情況，覆蓋為預設值
        df.loc[mask & is_prepayment, 'Liability'] = self.col_defaults.get(
            'prepay_liability', '111112'
        )

        return df

    # ========== 階段 7: PR Product Code 檢查 ==========

    def _check_pr_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """檢查 PR 的 Product Code 是否與 Project 一致"""
        if 'Product code' not in df.columns or 'Project' not in df.columns:
            self.logger.warning("缺少 Product code 或 Project 欄位，跳過檢查")
            return df

        mask = df['Product code'].notnull()

        try:
            project_first_word = df.loc[mask, 'Project'].str.findall(
                r'^(\w+(?:))'
            ).apply(lambda x: x[0] if len(x) > 0 else '')

            product_match = (project_first_word == df.loc[mask, 'Product code'])

            df.loc[mask, 'PR Product Code Check'] = np.where(
                product_match, 'good', 'bad'
            )

            bad_count = (~product_match).sum()
            if bad_count > 0:
                self.logger.warning(f"發現 {bad_count} 筆 PR Product Code 不一致")

        except Exception as e:
            self.logger.error(f"PR Product Code 檢查失敗: {str(e)}")

        return df

    # ========== 輔助方法 ==========

    def _generate_statistics(self, df: pd.DataFrame, status_column: str) -> Dict[str, Any]:
        """生成統計資訊"""
        stats: Dict[str, Any] = {
            'total_count': len(df),
            'accrual_count': int((df['是否估計入帳'] == 'Y').sum()),
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
            'GL#', 'Expected Received Month_轉換格式',
            'YMs of Item Description', 'Entry Quantity',
            'Received Quantity', 'Billed Quantity',
            'Entry Amount', 'Entry Billed Amount',
            'Item Description', 'Remarked by Procurement',
            'Remarked by 上月 FN', 'Unit Price', 'Currency',
            'Product Code'
        ]

        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            self.logger.error(f"缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False

        ref_account = context.get_auxiliary_data('reference_account')
        ref_liability = context.get_auxiliary_data('reference_liability')

        if ref_account is None or ref_liability is None:
            self.logger.error("缺少參考數據")
            context.add_error("缺少參考數據")
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
        self.logger.warning(f"回滾 SCT ERM 邏輯：{str(error)}")
