"""
SCT 會計科目預測步驟

功能：
根據 Product Code、Department、Item Description 的組合條件
預測會計科目 (predicted_account)、負債科目 (predicted_liability)
並記錄匹配條件 (matched_conditions)

配置驅動：規則定義在 stagging_sct.toml [sct_account_prediction.rules]
"""

import time
from dataclasses import dataclass
from typing import Dict, Any, List

import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder, create_error_metadata
from accrual_bot.utils.config import config_manager


@dataclass
class AccountPredictionConditions:
    """
    會計科目預測條件集合

    只追蹤已匹配的記錄，其他條件動態構建
    """

    matched: pd.Series


class SCTAccountPredictionStep(PipelineStep):
    """
    SCT 會計科目預測步驟

    功能：
    1. 根據業務規則預測會計科目與負債科目
    2. 記錄匹配到的條件
    3. 提供統計資訊

    業務規則：
    - 按照 rule_id 順序（從小到大）匹配
    - 一筆記錄只匹配第一個符合的條件
    - 未匹配的記錄保持為 NA

    條件類型：
    - product_code: Product Code 篩選（"0" = 不篩選）
    - department: Department 篩選（"0" = 不篩選）
    - description_keywords: Item Description 關鍵字（regex, case-insensitive）
    - min_amount: Entry Amount 下限（>=）
    - max_amount: Entry Amount 上限（<）

    輸出：
    - predicted_account: 預測會計科目
    - predicted_liability: 預測負債科目
    - matched_conditions: 匹配條件描述
    """

    def __init__(self, name: str = "SCTAccountPrediction", **kwargs):
        super().__init__(
            name=name,
            description="Predict account code based on SCT business rules",
            **kwargs
        )
        self.rules = self._load_rules_from_config()
        self.logger.info(f"Initialized {name} with {len(self.rules)} rules from config")

    def _load_rules_from_config(self) -> List[Dict[str, Any]]:
        """
        從配置檔案讀取預測規則

        Returns:
            List[Dict]: 規則列表，按 rule_id 排序
        """
        try:
            rules_config = config_manager._config_toml.get(
                'sct_account_prediction', {}
            ).get('rules', [])

            if not rules_config:
                self.logger.warning(
                    "未找到 sct_account_prediction.rules 配置，將使用空規則列表"
                )
                return []

            rules = sorted(rules_config, key=lambda x: x.get('rule_id', 999))

            self.logger.info(f"成功載入 {len(rules)} 條預測規則")

            for i, rule in enumerate(rules, 1):
                if 'account' not in rule or 'condition_desc' not in rule:
                    self.logger.warning(
                        f"規則 {i} 缺少必要欄位 (account 或 condition_desc)"
                    )

            return rules

        except Exception as e:
            self.logger.error(f"載入規則配置失敗: {str(e)}", exc_info=True)
            return []

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行會計科目預測"""
        start_time = time.time()

        try:
            df = context.data.copy()
            original_count = len(df)

            self.logger.info("開始 SCT 會計科目預測處理")

            # 階段 1: 初始化欄位
            df = self._initialize_fields(df)

            # 階段 2: 構建判斷條件
            conditions = self._build_conditions(df)

            # 階段 3: 應用預測規則
            df = self._apply_prediction_rules(df, conditions)

            # 更新上下文
            context.update_data(df)

            # 生成統計資訊
            stats = self._generate_statistics(df)

            self.logger.info(
                f"SCT 會計科目預測完成 - "
                f"已匹配: {stats['matched_count']} 筆 / "
                f"總計: {stats['total_count']} 筆 "
                f"({stats['match_rate']:.1f}%)"
            )

            duration = time.time() - start_time

            metadata = (StepMetadataBuilder()
                        .set_row_counts(original_count, len(df))
                        .set_process_counts(
                            processed=stats['matched_count'],
                            skipped=stats['unmatched_count'])
                        .add_custom('match_rate', f"{stats['match_rate']:.2f}%")
                        .add_custom('account_distribution', stats['account_distribution'])
                        .build())

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"預測完成，{stats['matched_count']} 筆已匹配",
                duration=duration,
                metadata=metadata
            )

        except Exception as e:
            self.logger.error(f"SCT 會計科目預測失敗: {str(e)}", exc_info=True)
            context.add_error(f"SCT 會計科目預測失敗: {str(e)}")
            duration = time.time() - start_time

            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='account_prediction'
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e),
                metadata=error_metadata
            )

    # ========== 階段 1: 初始化 ==========

    def _initialize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """初始化預測欄位"""
        df['predicted_account'] = pd.NA
        df['predicted_liability'] = pd.NA
        df['matched_conditions'] = pd.NA
        self.logger.debug("已初始化預測欄位")
        return df

    # ========== 階段 2: 構建條件 ==========

    def _build_conditions(self, df: pd.DataFrame) -> AccountPredictionConditions:
        """構建已匹配追蹤器"""
        matched = pd.Series([False] * len(df), index=df.index)
        return AccountPredictionConditions(matched=matched)

    # ========== 階段 3: 應用預測規則 ==========

    def _apply_prediction_rules(
        self,
        df: pd.DataFrame,
        cond: AccountPredictionConditions
    ) -> pd.DataFrame:
        """
        應用預測規則（配置驅動）

        按照 rule_id 順序執行，一筆記錄只匹配第一個符合的條件
        """
        if not self.rules:
            self.logger.warning("沒有可用的預測規則")
            return df

        for rule in self.rules:
            rule_id = rule.get('rule_id', 'unknown')
            account = rule.get('account')
            liability = rule.get('liability_account')
            condition_desc = rule.get('condition_desc', '')

            condition = self._build_rule_condition(df, rule, cond.matched)

            if condition.any():
                df.loc[condition, 'predicted_account'] = account
                df.loc[condition, 'matched_conditions'] = condition_desc
                if liability:
                    df.loc[condition, 'predicted_liability'] = liability
                cond.matched |= condition

                count = condition.sum()
                self._log_condition_result(f"規則 {rule_id}", count)

        return df

    def _build_rule_condition(
        self,
        df: pd.DataFrame,
        rule: Dict[str, Any],
        already_matched: pd.Series
    ) -> pd.Series:
        """
        根據規則配置構建條件

        Args:
            df: DataFrame
            rule: 規則字典
            already_matched: 已匹配的記錄

        Returns:
            pd.Series: 布林序列表示符合條件的記錄
        """
        # 從未匹配的記錄開始
        condition = ~already_matched

        # Product Code 條件（"0" = 不篩選）
        product_code = rule.get('product_code', '0')
        if product_code and product_code != '0':
            condition &= df['Product Code'].astype(str) == product_code

        # Department 條件（"0" = 不篩選）
        department = rule.get('department', '0')
        if department and department != '0':
            condition &= df['Department'].astype(str).str.contains(
                f"^{department}", na=False
            )

        # Item Description 關鍵字條件
        if rule.get('description_keywords'):
            keywords = rule['description_keywords']
            condition &= df['Item Description'].str.contains(
                keywords, case=False, na=False
            )

        # 金額下限（>=）
        if rule.get('min_amount') is not None:
            amount = pd.to_numeric(df['Entry Amount'], errors='coerce')
            condition &= amount >= rule['min_amount']

        # 金額上限（<）
        if rule.get('max_amount') is not None:
            amount = pd.to_numeric(df['Entry Amount'], errors='coerce')
            condition &= amount < rule['max_amount']

        return condition

    def _log_condition_result(self, rule_name: str, count: int):
        """記錄條件判斷結果"""
        if count > 0:
            self.logger.debug(f"{rule_name}: {count} 筆符合")

    # ========== 統計資訊 ==========

    def _generate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """生成統計資訊"""
        total_count = len(df)
        matched_count = int(df['predicted_account'].notna().sum())
        unmatched_count = total_count - matched_count
        match_rate = (matched_count / total_count * 100) if total_count > 0 else 0

        account_distribution: Dict[str, int] = {}
        if matched_count > 0:
            account_counts = df['predicted_account'].value_counts().to_dict()
            account_distribution = {
                str(k): int(v) for k, v in account_counts.items() if pd.notna(k)
            }

        return {
            'total_count': total_count,
            'matched_count': matched_count,
            'unmatched_count': unmatched_count,
            'match_rate': match_rate,
            'account_distribution': account_distribution
        }

    # ========== 驗證方法 ==========

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        df = context.data

        if df is None or df.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False

        required_columns = ['Product Code', 'Department', 'Item Description', 'Entry Amount']
        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            self.logger.error(f"缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False

        self.logger.info("輸入驗證通過")
        return True

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作"""
        self.logger.warning(f"回滾 SCT 會計科目預測：{str(error)}")
        if context.data is not None:
            for col in ['predicted_account', 'predicted_liability', 'matched_conditions']:
                if col in context.data.columns:
                    context.data.drop(col, axis=1, inplace=True)
