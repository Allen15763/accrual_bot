"""
Base ERM Evaluation Step - ERM 邏輯評估步驟的基類

使用模板方法模式（Template Method Pattern）提取共用的 ERM 評估邏輯，
減少 SPT/SPX ERM 評估步驟之間的代碼重複。

Usage:
    class SPTERMLogicStep(BaseERMEvaluationStep):
        def _build_conditions(self, df, file_date, status_column):
            # SPT specific conditions
            ...

        def _apply_status_conditions(self, df, conditions, status_column):
            # SPT specific status logic
            ...
"""

import time
from abc import abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder


@dataclass
class BaseERMConditions:
    """
    基礎 ERM 條件集合

    包含所有實體通用的基礎條件。
    子類可以繼承並擴展為實體特定的條件類。
    """
    # 基礎狀態條件
    no_status: pd.Series

    # 日期範圍條件
    in_date_range: pd.Series
    erm_before_or_equal_file_date: pd.Series
    erm_after_file_date: pd.Series

    # 錯誤條件
    format_error: pd.Series
    out_of_date_range: pd.Series
    procurement_not_error: pd.Series


class BaseERMEvaluationStep(PipelineStep):
    """
    ERM 邏輯評估步驟的抽象基類

    提供通用的 ERM 評估功能，子類只需實現實體特定的邏輯。

    Template Method Pattern:
    - 模板方法: execute(), _generate_statistics()
    - 鉤子方法: _build_conditions(), _apply_status_conditions(),
                 _set_accounting_fields()

    Attributes:
        fa_accounts: FA 帳戶清單
        dept_accounts: 部門帳戶清單
        entity_type: 實體類型 (SPT/SPX)
    """

    def __init__(
        self,
        name: str,
        entity_type: str = "SPT",
        description: str = "Apply ERM logic with status conditions",
        **kwargs
    ):
        """
        初始化 ERM 評估步驟

        Args:
            name: 步驟名稱
            entity_type: 實體類型
            description: 步驟描述
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(name, description=description, **kwargs)

        self.entity_type = entity_type

        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list(
            entity_type, 'fa_accounts', ['199999']
        )
        self.dept_accounts = config_manager.get_list(
            entity_type, 'dept_accounts', []
        )

        self.logger.info(
            f"Initialized {name} for {entity_type} with FA accounts: {self.fa_accounts}"
        )

    # ========== 模板方法 (Template Methods) - 共用實現 ==========

    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行 ERM 邏輯（模板方法）

        執行流程:
        1. 設置基本欄位
        2. 構建判斷條件
        3. 應用狀態條件
        4. 處理格式錯誤
        5. 設置是否估計入帳
        6. 設置會計欄位
        7. 執行實體特定的後處理

        Args:
            context: 處理上下文

        Returns:
            StepResult: 步驟執行結果
        """
        start_time = time.time()

        try:
            df = context.data.copy()
            processing_date = context.get_variable('processing_date')

            # 獲取參考數據
            ref_account = context.get_auxiliary_data('reference_account')
            ref_liability = context.get_auxiliary_data('reference_liability')

            if ref_account is None or ref_liability is None:
                raise ValueError("缺少參考數據：科目映射或負債科目")

            self.logger.info(f"開始 ERM 邏輯處理，處理日期：{processing_date}")

            # ========== 階段 1: 設置基本欄位 ==========
            df = self._set_file_date(df, processing_date)

            # ========== 階段 2: 構建判斷條件 ==========
            status_column = self._get_status_column(df, context)
            conditions = self._build_conditions(df, processing_date, status_column)

            # ========== 階段 3: 應用狀態條件 ==========
            df = self._apply_status_conditions(df, conditions, status_column)

            # ========== 階段 4: 處理格式錯誤 ==========
            df = self._handle_format_errors(df, conditions, status_column)

            # ========== 階段 5: 設置是否估計入帳 ==========
            df = self._set_accrual_flag(df, status_column)

            # ========== 階段 6: 設置會計欄位 ==========
            df = self._set_accounting_fields(df, ref_account, ref_liability)

            # ========== 階段 7: 實體特定後處理 ==========
            df = self._post_process(df, context)

            # 更新上下文
            context.update_data(df)

            # 生成統計資訊
            stats = self._generate_statistics(df, status_column)

            duration = time.time() - start_time

            self.logger.info(
                f"ERM 邏輯完成 - "
                f"需估列: {stats['accrual_count']} 筆, "
                f"總計: {stats['total_count']} 筆"
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"ERM 邏輯已應用，{stats['accrual_count']} 筆需估列",
                duration=duration,
                metadata=stats
            )

        except Exception as e:
            self.logger.error(f"ERM 邏輯處理失敗: {str(e)}", exc_info=True)
            context.add_error(f"ERM 邏輯失敗: {str(e)}")
            duration = time.time() - start_time

            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    # ========== 共用方法 ==========

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

    def _handle_format_errors(
        self,
        df: pd.DataFrame,
        conditions: Any,
        status_column: str
    ) -> pd.DataFrame:
        """處理格式錯誤的記錄"""
        if hasattr(conditions, 'format_error') and hasattr(conditions, 'no_status'):
            mask_format_error = conditions.no_status & conditions.format_error
            df.loc[mask_format_error, status_column] = '格式錯誤，退單'

            error_count = mask_format_error.sum()
            if error_count > 0:
                self.logger.warning(f"發現 {error_count} 筆格式錯誤")

        return df

    def _set_accrual_flag(self, df: pd.DataFrame, status_column: str) -> pd.DataFrame:
        """
        根據狀態設置是否估計入帳

        基本邏輯：只有「已完成」狀態需要估列入帳
        子類可以覆寫此方法來實現不同的邏輯
        """
        mask_completed = df[status_column].str.contains('已完成', na=False)

        df.loc[mask_completed, '是否估計入帳'] = 'Y'
        df.loc[~mask_completed, '是否估計入帳'] = 'N'

        accrual_count = mask_completed.sum()
        self.logger.info(f"設置估列標記：{accrual_count} 筆需估列")

        return df

    def _generate_statistics(
        self,
        df: pd.DataFrame,
        status_column: str
    ) -> Dict[str, Any]:
        """生成統計資訊"""
        stats = {
            'total_count': len(df),
            'accrual_count': (df['是否估計入帳'] == 'Y').sum(),
            'status_distribution': {}
        }

        if status_column in df.columns:
            status_counts = df[status_column].value_counts().to_dict()
            stats['status_distribution'] = {
                str(k): int(v) for k, v in status_counts.items()
            }

        return stats

    def _log_condition_result(self, condition_name: str, count: int):
        """記錄條件判斷結果"""
        if count > 0:
            self.logger.debug(f"條件 [{condition_name}]: {count} 筆符合")

    # ========== 共用會計欄位設置 ==========

    def _set_account_name(
        self,
        df: pd.DataFrame,
        ref_account: pd.DataFrame,
        mask: pd.Series
    ) -> pd.DataFrame:
        """設置會計科目名稱"""
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
        """
        設置部門代碼

        規則：
        - 如果科目在 dept_accounts 清單中，取 Department 前3碼
        - 否則設為 '000'
        """
        isin_dept = df['Account code'].astype('string').isin(
            [str(x) for x in self.dept_accounts]
        )

        df.loc[mask & isin_dept, 'Dep.'] = \
            df.loc[mask & isin_dept, 'Department'].str[:3]

        df.loc[mask & ~isin_dept, 'Dep.'] = '000'

        return df

    def _calculate_accrual_amount(
        self,
        df: pd.DataFrame,
        mask: pd.Series
    ) -> pd.DataFrame:
        """
        計算預估金額

        公式：Unit Price × (Entry Quantity - Billed Quantity)
        """
        df['temp_amount'] = (
            df['Unit Price'].astype('Float64') *
            (df['Entry Quantity'].astype('Float64') -
             df['Billed Quantity'].astype('Float64'))
        )

        df.loc[mask, 'Accr. Amount'] = df.loc[mask, 'temp_amount']
        df.drop('temp_amount', axis=1, inplace=True)

        return df

    def _handle_prepayment(
        self,
        df: pd.DataFrame,
        mask: pd.Series,
        ref_liability: pd.DataFrame
    ) -> pd.DataFrame:
        """
        處理預付款和負債科目

        規則：
        - 有預付款：是否有預付 = 'Y'，Liability = '111112'
        - 無預付款：從參考資料查找 Liability
        """
        is_prepayment = df['Entry Prepay Amount'] != '0'
        df.loc[mask & is_prepayment, '是否有預付'] = 'Y'

        if not ref_liability.empty:
            merged = pd.merge(
                df,
                ref_liability[['Account', 'Liability']],
                how='left',
                left_on='Account code',
                right_on='Account'
            )
            df['Liability'] = merged['Liability_y']

        df.loc[mask & is_prepayment, 'Liability'] = '111112'

        return df

    # ========== 驗證方法 ==========

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        df = context.data

        if df is None or df.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False

        # 獲取必要欄位清單
        required_columns = self.get_required_columns()

        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            self.logger.error(f"缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False

        # 檢查參考數據
        ref_account = context.get_auxiliary_data('reference_account')
        ref_liability = context.get_auxiliary_data('reference_liability')

        if ref_account is None or ref_liability is None:
            self.logger.error("缺少參考數據")
            context.add_error("缺少參考數據")
            return False

        # 檢查處理日期
        processing_date = context.get_variable('processing_date')
        if processing_date is None:
            self.logger.error("缺少處理日期")
            context.add_error("缺少處理日期")
            return False

        self.logger.info("輸入驗證通過")
        return True

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作"""
        self.logger.warning(f"回滾 ERM 邏輯：{str(error)}")

    # ========== 鉤子方法 (Hook Methods) - 子類實現 ==========

    @abstractmethod
    def _build_conditions(
        self,
        df: pd.DataFrame,
        file_date: int,
        status_column: str
    ) -> Any:
        """
        構建所有判斷條件

        子類必須實現此方法，返回實體特定的條件對象。

        Args:
            df: 數據 DataFrame
            file_date: 處理日期
            status_column: 狀態欄位名稱

        Returns:
            Any: 條件對象（如 ERMConditions）
        """
        pass

    @abstractmethod
    def _apply_status_conditions(
        self,
        df: pd.DataFrame,
        conditions: Any,
        status_column: str
    ) -> pd.DataFrame:
        """
        應用狀態判斷條件

        子類必須實現此方法，應用實體特定的狀態邏輯。

        Args:
            df: 數據 DataFrame
            conditions: 條件對象
            status_column: 狀態欄位名稱

        Returns:
            pd.DataFrame: 處理後的 DataFrame
        """
        pass

    @abstractmethod
    def _set_accounting_fields(
        self,
        df: pd.DataFrame,
        ref_account: pd.DataFrame,
        ref_liability: pd.DataFrame
    ) -> pd.DataFrame:
        """
        設置會計相關欄位

        子類必須實現此方法，設置實體特定的會計欄位。

        Args:
            df: 數據 DataFrame
            ref_account: 科目參考數據
            ref_liability: 負債科目參考數據

        Returns:
            pd.DataFrame: 處理後的 DataFrame
        """
        pass

    # ========== 可選覆寫方法 ==========

    def _post_process(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """
        實體特定的後處理（可選覆寫）

        子類可以覆寫此方法來添加額外的處理邏輯。

        Args:
            df: 數據 DataFrame
            context: 處理上下文

        Returns:
            pd.DataFrame: 處理後的 DataFrame
        """
        return df

    def get_required_columns(self) -> List[str]:
        """
        獲取必要的欄位列表（可選覆寫）

        子類可以覆寫此方法來定義實體特定的必要欄位。

        Returns:
            List[str]: 必要欄位名稱列表
        """
        return [
            'GL#', 'Expected Received Month_轉換格式',
            'YMs of Item Description', 'Entry Quantity',
            'Received Quantity', 'Billed Quantity',
            'Entry Amount', 'Entry Billed Amount',
            'Item Description', 'Remarked by Procurement',
            'Remarked by 上月 FN', 'Unit Price', 'Currency',
            'Product Code'
        ]
