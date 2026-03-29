"""
SPT 會計標籤標記步驟

功能：
根據配置檔案中的業務規則標記 PO狀態 和 Remarked by FN

業務邏輯:
1. 優先級條件：更新 PO狀態 和 Remarked by FN（強制覆蓋）
2. ERM條件：僅更新 Remarked by FN（不更新狀態，估計與否由ERM決定）

配置來源:
- [spt_status_label_rules.priority_conditions]: 優先於ERM的條件
- [spt_status_label_rules.erm_conditions]: ERM條件
"""

import time
from typing import Dict, Any, List
from datetime import datetime
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder, create_error_metadata
from accrual_bot.utils.config import config_manager


class SPTStatusLabelStep(PipelineStep):
    """
    會計標籤標記步驟 (配置驅動)

    業務邏輯:
    1. 根據配置檔案中的規則標記會計標籤
    2. 優先級條件：更新 PO狀態 和 Remarked by FN（強制覆蓋）
    3. ERM條件：僅更新 Remarked by FN（不更新狀態，估計與否由ERM決定）

    配置來源:
    - [spt_status_label_rules.priority_conditions]: 優先於ERM的條件
    - [spt_status_label_rules.erm_conditions]: ERM條件

    輸入:
    - DataFrame with required columns

    輸出:
    - DataFrame with updated labels
    """

    def __init__(self,
                 name: str = "Accounting_Label_Marking",
                 status_column: str = "PO狀態",
                 remark_column: str = "Remarked by FN",
                 **kwargs):
        """
        初始化會計標籤標記步驟

        Args:
            name: 步驟名稱
            status_column: 狀態欄位名稱（預設為 PO狀態）
            remark_column: 備註欄位名稱（預設為 Remarked by FN）
        """
        super().__init__(
            name=name,
            description="Mark accounting labels based on business rules",
            **kwargs
        )
        self.status_column = status_column
        self.remark_column = remark_column

        # 從配置檔案讀取規則
        self.priority_rules = self._load_rules('priority_conditions')
        self.erm_rules = self._load_rules('erm_conditions')

        self.logger.info(f"已載入 {len(self.priority_rules)} 個優先級規則")
        self.logger.info(f"已載入 {len(self.erm_rules)} 個 ERM 規則")

    def _load_rules(self, rule_type: str) -> Dict[str, Dict[str, Any]]:
        """
        從配置檔案載入規則
        
        Args:
            rule_type: 規則類型 ('priority_conditions' 或 'erm_conditions')
            
        Returns:
            Dict[str, Dict]: 規則字典，key 為規則名稱
        """
        try:
            rules_config = config_manager._config_toml.get(
                'spt_status_label_rules', {}
            ).get(rule_type, {})

            if not rules_config:
                self.logger.warning(
                    f"未找到 spt_status_label_rules.{rule_type} 配置，將使用空規則列表"
                )
                return {}

            self.logger.info(f"成功載入 {len(rules_config)} 條 {rule_type} 規則")

            # 驗證規則
            exception_rules = ['exceed_period_but_pq_confirmed', 
                               'check_qty_and_pq_confirmed',
                               'parsing_err_but_pq_confirmed',
                               'incomplete_but_pq_confirmed',
                               'hris_bug']
            for rule_name, rule in rules_config.items():
                if 'remark' not in rule and rule_name not in exception_rules:
                    self.logger.warning(
                        f"規則 '{rule_name}' 缺少必要欄位 'remark'"
                    )

            return dict(rules_config)

        except Exception as e:
            self.logger.error(f"載入 {rule_type} 規則失敗: {str(e)}")
            return {}

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行會計標籤標記邏輯"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()
            input_count = len(df)

            self.logger.info("=" * 60)
            self.logger.info("🏷️  開始執行會計標籤標記...")
            self.logger.info(f"📊 總記錄數: {input_count:,}")
            self.logger.info("=" * 60)

            # 動態判斷狀態欄位名稱
            self.status_column = self._get_status_column(df)

            # === 階段 1: 應用優先級條件 ===
            self.logger.info("⚡ 應用優先級條件（強制覆蓋）...")
            priority_stats = self._apply_rules(
                df, self.priority_rules, update_status=True
            )

            # === 階段 2: 應用 ERM 條件 ===
            self.logger.info("📋 應用 ERM 條件（僅標記備註）...")
            erm_stats = self._apply_rules(
                df, self.erm_rules, update_status=False
            )

            # === 階段 3: 生成統計資訊 ===
            total_labeled = sum(priority_stats.values()) + sum(erm_stats.values())

            statistics = {
                'total_records': input_count,
                'priority_labeled': sum(priority_stats.values()),
                'erm_labeled': sum(erm_stats.values()),
                'total_labeled': total_labeled,
                'label_rate': f"{(total_labeled / input_count * 100):.2f}%" if input_count > 0 else "0.00%",
                'priority_breakdown': priority_stats,
                'erm_breakdown': erm_stats
            }

            # === 階段 4: 記錄詳細日誌 ===
            self._log_detailed_statistics(statistics)

            # === 階段 5: 更新上下文 ===
            df = self._update_accrual_col(df)
            context.update_data(df)

            duration = time.time() - start_time
            end_datetime = datetime.now()

            self.logger.info("=" * 60)
            self.logger.info(f"✅ 會計標籤標記完成 (耗時: {duration:.2f}秒)")
            self.logger.info("=" * 60)

            # 構建 metadata
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, len(df))
                        .set_process_counts(processed=total_labeled, skipped=input_count - total_labeled)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('priority_labeled', sum(priority_stats.values()))
                        .add_custom('erm_labeled', sum(erm_stats.values()))
                        .add_custom('statistics', statistics)
                        .build())

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message='-'.join([f"已標記 {total_labeled:,} 筆記錄\n",
                                 f"\t(優先級: {sum(priority_stats.values()):,}, ERM: {sum(erm_stats.values()):,})"]),
                duration=duration,
                metadata=metadata
            )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"❌ 會計標籤標記失敗: {str(e)}", exc_info=True)
            context.add_error(f"Accounting label marking failed: {str(e)}")

            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='accounting_label_marking'
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"會計標籤標記失敗: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )

    def _get_status_column(self, df: pd.DataFrame) -> str:
        """動態判斷狀態欄位名稱"""
        if 'PO狀態' in df.columns:
            return 'PO狀態'
        elif 'PR狀態' in df.columns:
            return 'PR狀態'
        else:
            # 如果都不存在，創建 PO狀態 欄位
            df['PO狀態'] = pd.NA
            return 'PO狀態'

    def _apply_rules(self, df: pd.DataFrame, rules: Dict[str, Dict[str, Any]],
                     update_status: bool) -> Dict[str, int]:
        """
        應用規則字典（配置驅動）

        Args:
            df: DataFrame
            rules: 規則字典 {rule_name: rule_config}
            update_status: 是否更新狀態欄位

        Returns:
            Dict[str, int]: 各規則的匹配計數
        """
        stats = {}

        if not rules:
            self.logger.warning("沒有可用的規則")
            return stats

        for rule_name, rule in rules.items():
            status = rule.get('status')
            remark = rule.get('remark')
            matched_condition = rule.get('note')

            # 構建條件
            condition = self._build_rule_condition(df, rule)

            # 應用規則
            count = condition.sum()
            if count > 0:
                # 更新備註（總是更新）
                df.loc[condition, self.remark_column] = remark
                df.loc[condition, 'matched_condition_on_status'] = matched_condition  # 暫時一併提供條件訊息

                # 更新狀態（僅優先級條件）
                if update_status and status:
                    df.loc[condition, self.status_column] = status

                self.logger.debug(f"  ✓ {rule_name}: {count:,} 筆")
                stats[rule_name] = count

        return stats

    def _build_rule_condition(self, df: pd.DataFrame,
                              rule: Dict[str, Any]) -> pd.Series:
        """
        根據規則配置構建條件

        支援的條件類型 (對應 toml 配置的 key):
        - keywords + field: 關鍵字匹配（指定欄位）
        - supplier: Supplier 精確匹配
        - dept: Department 精確匹配
        - dept_prefix: Department 前綴匹配
        - dept_exclude_prefix: Department 非前綴匹配
        - dept_include: Department 包含匹配 (regex)
        - dept_exclude: Department 不包含匹配 (regex)
        - requester: Requester 精確匹配
        - status_value_contains: 狀態欄位regex匹配
        - remarked_by_procurement: remarked_by_procurement內容精確匹配

        Args:
            df: DataFrame
            rule: 規則字典

        Returns:
            pd.Series: 布林序列表示符合條件的記錄
        """
        # 從全部記錄開始
        condition = pd.Series([True] * len(df), index=df.index)

        # === 關鍵字條件 (keywords + field) ===
        if 'keywords' in rule:
            keywords = rule['keywords']
            field = rule.get('field', 'Item Description')
            
            if field == 'Item Description':
                col_data = df.get('Item Description', pd.Series(dtype=str))
            else:
                col_data = df.get(field, pd.Series(dtype=str))
            
            keyword_condition = col_data.str.contains(keywords, na=False, regex=True)
            condition &= keyword_condition

        # === Supplier 條件 ===
        if 'supplier' in rule and rule['supplier']:
            supplier_col = self._get_column_by_pattern(df, r'(?i)supplier')
            if supplier_col:
                supplier = df.get(supplier_col, pd.Series(dtype=str))
                supplier_condition = supplier == rule['supplier']
                condition &= supplier_condition

        # === Department 條件 ===
        dept = df.get('Department', pd.Series(dtype=str))

        # 精確匹配
        if 'dept' in rule and rule['dept']:
            dept_condition = dept == rule['dept']
            condition &= dept_condition

        # 前綴匹配
        if 'dept_prefix' in rule and rule['dept_prefix']:
            prefix = rule['dept_prefix']
            dept_condition = dept.str.startswith(prefix, na=False)
            condition &= dept_condition

        # 非前綴匹配
        if 'dept_exclude_prefix' in rule and rule['dept_exclude_prefix']:
            prefix = rule['dept_exclude_prefix']
            dept_condition = ~dept.str.startswith(prefix, na=False)
            condition &= dept_condition

        # 包含匹配 (regex)
        if 'dept_include' in rule and rule['dept_include']:
            pattern = rule['dept_include']
            dept_condition = dept.str.contains(pattern, na=False, regex=True)
            condition &= dept_condition

        # 不包含匹配 (regex)
        if 'dept_exclude' in rule and rule['dept_exclude']:
            pattern = rule['dept_exclude']
            dept_condition = ~dept.str.contains(pattern, na=False, regex=True)
            condition &= dept_condition

        # === Requester 條件 ===
        if 'requester' in rule and rule['requester']:
            requester_col = self._get_column_by_pattern(df, r'(?i)requester')
            if requester_col:
                requester = df.get(requester_col, pd.Series(dtype=str))
                requester_condition = requester == rule['requester']
                condition &= requester_condition

        # ========== 新增：Status 條件 (PO狀態/PR狀態) ==========
        if 'status_value_contains' in rule and rule['status_value_contains']:
            # 動態判斷使用哪個狀態欄位（PO狀態 或 PR狀態）
            status_col = self.status_column  # 已在 execute() 中動態設定
            if status_col in df.columns:
                status_data = df.get(status_col, pd.Series(dtype=str))
                status_condition = status_data.str.contains(
                    rule['status_value_contains'], na=False, regex=True
                )
                condition &= status_condition

        # ========== 新增：Remarked by Procurement 條件（精確匹配）==========
        if 'remarked_by_procurement' in rule and rule['remarked_by_procurement']:
            procurement_col = self._get_column_by_pattern(df, r'(?i)remarked.*procurement')
            if procurement_col:
                procurement_data = df.get(procurement_col, pd.Series(dtype=str))
                procurement_condition = procurement_data == rule['remarked_by_procurement']
                condition &= procurement_condition

        return condition

    def _get_column_by_pattern(self, df: pd.DataFrame, pattern: str) -> str:
        """根據正則模式獲取欄位名稱"""
        matched_cols = df.filter(regex=pattern).columns
        return matched_cols[0] if len(matched_cols) > 0 else None

    def _log_detailed_statistics(self, stats: Dict[str, Any]):
        """記錄詳細統計日誌"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 會計標籤標記統計報告")
        self.logger.info("=" * 60)
        self.logger.info(f"📈 總記錄數: {stats['total_records']:,}")
        self.logger.info(f"🏷️  已標記: {stats['total_labeled']:,} ({stats['label_rate']})")
        self.logger.info(f"   • 優先級條件: {stats['priority_labeled']:,}")
        self.logger.info(f"   • ERM 條件: {stats['erm_labeled']:,}")

        if stats['priority_breakdown']:
            self.logger.info("\n📋 優先級條件明細:")
            for label, count in sorted(stats['priority_breakdown'].items()):
                self.logger.info(f"   • {label}: {count:,}")

        if stats['erm_breakdown']:
            self.logger.info("\n📋 ERM 條件明細:")
            for label, count in sorted(stats['erm_breakdown'].items()):
                self.logger.info(f"   • {label}: {count:,}")

        self.logger.info("=" * 60 + "\n")

    def _update_accrual_col(self, df: pd.DataFrame, accrual_col: str = '是否估計入帳') -> pd.DataFrame:
        df_copy = df.copy()
        df_copy[accrual_col] = np.where(
            df_copy[self.status_column].str.contains("已完成", na=False),
            'Y',
            df_copy[accrual_col]
        )
        return df_copy

    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入資料的完整性

        檢查項目:
        1. DataFrame 不為空
        2. 必要欄位存在
        3. 配置規則已載入
        """
        try:
            df = context.data

            # 檢查 DataFrame
            if df is None or df.empty:
                self.logger.error("❌ 輸入資料為空")
                return False

            # 檢查必要欄位（基本欄位）
            required_columns = ['Item Description', 'Department']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"❌ 缺少必要欄位: {missing_columns}")
                return False

            # 檢查 Supplier 欄位（支援不同命名）
            supplier_col = self._get_column_by_pattern(df, r'(?i)supplier')
            if not supplier_col:
                self.logger.error("❌ 缺少 Supplier 欄位")
                return False

            # 檢查或創建備註欄位
            if self.remark_column not in df.columns:
                self.logger.warning(f"⚠️  {self.remark_column} 欄位不存在，將自動創建")
                df[self.remark_column] = pd.NA

            # 檢查配置是否載入
            if not self.priority_rules and not self.erm_rules:
                self.logger.warning("⚠️  未載入任何規則，步驟將不會進行任何標記")

            self.logger.info("✅ 輸入驗證通過")
            return True

        except Exception as e:
            self.logger.error(f"❌ 驗證失敗: {str(e)}", exc_info=True)
            return False

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作（如需要）"""
        self.logger.warning(f"回滾會計標籤標記：{str(error)}")
        # 通常不需要特殊回滾操作
