"""
SPT Procurement Status Evaluation Step

採購狀態判斷 - 完全配置驅動版本
"""

from typing import Dict, List
import pandas as pd
import time
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager


class SPTProcurementStatusEvaluationStep(PipelineStep):
    """
    採購狀態判斷 - 完全配置驅動版本

    功能:
    1. 從配置載入所有條件規則
    2. 支援多種條件類型:
       - contains: 欄位包含指定模式 (正則)
       - equals: 欄位等於指定值
       - not_equals: 欄位不等於指定值
       - erm_in_range: ERM 在 Item Description 日期範圍內
       - erm_le_closing: ERM ≤ 結帳月
       - erm_gt_closing: ERM > 結帳月
    3. 支援條件組合: and, or
    4. 新增條件無需修改程式碼，只需更新 stagging.toml

    配置路徑: config/stagging.toml -> [[spt_procurement_status_rules.conditions]]
    """

    def __init__(self, status_column: str = "PO狀態", **kwargs):
        super().__init__(**kwargs)
        self.status_column = status_column
        self._load_conditions_from_config()

    def _load_conditions_from_config(self):
        """從配置載入條件規則"""
        config = config_manager._config_toml.get('spt_procurement_status_rules', {})
        self.conditions = config.get('conditions', [])

        # 按 priority 排序
        self.conditions = sorted(self.conditions, key=lambda x: x.get('priority', 999))

        self.logger.info(f"Loaded {len(self.conditions)} procurement status conditions")

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行採購狀態判斷"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()
            file_date = context.metadata.processing_date  # 結帳月份 YYYYMM

            self.logger.info(f"Evaluating procurement status with {len(self.conditions)} conditions...")
            self.logger.info(f"Status column: {self.status_column}, Closing date: {file_date}")

            # 確保狀態欄位存在
            if self.status_column not in df.columns:
                self.logger.warning(f"Status column '{self.status_column}' not found, creating it")
                df[self.status_column] = pd.NA

            # 重置狀態欄位，common的DateLogicStep有預先給狀態，採購端不看該條件。
            df[self.status_column] = pd.NA

            # 準備 ERM 相關欄位
            erm_data = self._prepare_erm_data(df)

            # 統計資訊
            stats = {
                'total_rows': len(df),
                'conditions_applied': 0,
                'rows_updated': 0
            }

            # 按優先順序應用每個條件
            for condition in self.conditions:
                rows_before = df[self.status_column].notna().sum()
                df = self._apply_condition(df, condition, erm_data, file_date)
                rows_after = df[self.status_column].notna().sum()

                rows_updated_by_condition = rows_after - rows_before
                if rows_updated_by_condition > 0:
                    self.logger.debug(
                        f"Condition {condition.get('priority')}: '{condition.get('status_value')}' "
                        f"applied to {rows_updated_by_condition} rows"
                    )
                    stats['conditions_applied'] += 1
                    stats['rows_updated'] += rows_updated_by_condition

            df = self._simple_clean(df)
            context.update_data(df)
            duration = time.time() - start_time

            # 統計結果
            status_counts = df[self.status_column].value_counts()
            self.logger.info(
                f"Procurement status evaluation complete: "
                f"{stats['rows_updated']}/{stats['total_rows']} rows updated "
                f"by {stats['conditions_applied']} conditions in {duration:.2f}s"
            )
            self.logger.info(f"Status distribution: {status_counts.to_dict()}")

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Evaluated {len(self.conditions)} conditions",
                duration=duration,
                metadata={
                    'total_conditions': len(self.conditions),
                    'conditions_applied': stats['conditions_applied'],
                    'rows_updated': stats['rows_updated'],
                    'status_distribution': status_counts.to_dict()
                }
            )

        except Exception as e:
            self.logger.error(f"Procurement status evaluation failed: {str(e)}", exc_info=True)
            context.add_error(f"Procurement status evaluation failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    def _prepare_erm_data(self, df: pd.DataFrame) -> Dict:
        """
        準備 ERM 相關資料供條件評估使用

        Returns:
            dict: {
                'ym_start': Series,  # 日期範圍起始月
                'ym_end': Series,    # 日期範圍結束月
                'erm': Series        # Expected Received Month
            }
        """
        ym_col = 'YMs of Item Description'
        erm_col = 'Expected Received Month_轉換格式'

        if ym_col not in df.columns or erm_col not in df.columns:
            self.logger.warning(
                f"ERM columns not found: '{ym_col}' or '{erm_col}', "
                "ERM-based conditions will be skipped"
            )
            return {}

        try:
            return {
                'ym_start': df[ym_col].str[:6].astype('Int32'),
                'ym_end': df[ym_col].str[7:].astype('Int32'),
                'erm': df[erm_col],
            }
        except Exception as e:
            self.logger.error(f"Failed to prepare ERM data: {str(e)}")
            return {}

    def _apply_condition(
        self,
        df: pd.DataFrame,
        condition: Dict,
        erm_data: Dict,
        file_date: int
    ) -> pd.DataFrame:
        """
        應用單一條件規則

        Args:
            df: DataFrame
            condition: 條件配置 (from TOML)
            erm_data: ERM 相關資料
            file_date: 結帳月份

        Returns:
            更新後的 DataFrame
        """
        checks = condition.get('checks', [])
        combine = condition.get('combine', 'and')
        status_value = condition.get('status_value')
        priority = condition.get('priority')
        note = condition.get('note')

        if not checks or not status_value:
            return df

        # 建立遮罩: 只處理尚未有狀態的記錄
        mask_no_status = df[self.status_column].isna() | (df[self.status_column] == 'nan')

        # 評估所有 checks
        check_masks = []
        for check in checks:
            mask = self._evaluate_check(df, check, erm_data, file_date)
            if mask is not None:
                check_masks.append(mask)

        if not check_masks:
            return df

        # 組合遮罩
        if combine == 'and':
            final_mask = check_masks[0]
            for m in check_masks[1:]:
                final_mask = final_mask & m
        else:  # or
            final_mask = check_masks[0]
            for m in check_masks[1:]:
                final_mask = final_mask | m

        # 新增狀態值
        df.loc[mask_no_status & final_mask, self.status_column] = status_value

        # 新增條件備註欄
        df.loc[mask_no_status & final_mask, 'condition_note'] = note

        return df

    def _evaluate_check(
        self,
        df: pd.DataFrame,
        check: Dict,
        erm_data: Dict,
        file_date: int
    ) -> pd.Series:
        """
        評估單一 check，返回布林 Series

        Args:
            df: DataFrame
            check: 單一 check 配置
            erm_data: ERM 相關資料
            file_date: 結帳月份

        Returns:
            布林 Series，或 None (如果條件無法評估)
        """
        check_type = check.get('type')
        field = check.get('field')
        pattern = check.get('pattern')
        value = check.get('value')

        # 欄位檢查類型
        if check_type == 'contains':
            if field not in df.columns:
                self.logger.warning(f"Field '{field}' not found for contains check")
                return None
            return df[field].str.contains(pattern, na=False, regex=True)
        
        elif check_type == 'not_contains':
            if field not in df.columns:
                self.logger.warning(f"Field '{field}' not found for contains check")
                return None
            return ~df[field].str.contains(pattern, na=False, regex=True)

        elif check_type == 'equals':
            if field not in df.columns:
                self.logger.warning(f"Field '{field}' not found for equals check")
                return None
            return df[field] == value

        elif check_type == 'not_equals':
            if field not in df.columns:
                self.logger.warning(f"Field '{field}' not found for not_equals check")
                return None
            return df[field] != value

        # ERM 檢查類型
        elif check_type == 'erm_in_range':
            if not erm_data:
                return pd.Series([False] * len(df))
            return erm_data['erm'].between(
                erm_data['ym_start'], erm_data['ym_end'], inclusive='both'
            )

        elif check_type == 'erm_le_closing':
            if not erm_data:
                return pd.Series([False] * len(df))
            return erm_data['erm'] <= (file_date)

        elif check_type == 'erm_gt_closing':
            if not erm_data:
                return pd.Series([False] * len(df))
            return erm_data['erm'] > file_date

        else:
            self.logger.warning(f"Unknown check type: {check_type}")
            return None

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for procurement status evaluation")
            return False

        # 檢查必要欄位
        required_columns = ['Item Description']
        missing = [col for col in required_columns if col not in context.data.columns]
        if missing:
            self.logger.error(f"Missing required columns: {missing}")
            return False

        # 檢查 file_date 變數
        file_date = context.metadata.processing_date
        if file_date is None:
            self.logger.error("Missing file_date variable")
            return False

        return True

    def _simple_clean(self, df) -> pd.DataFrame:
        df_copy = df.copy()
        remove_cols = [
            'Supplier', 
            'Expected Received Month_轉換格式', 
            'YMs of Item Description',
            '是否估計入帳'
        ]
        for col in remove_cols:
            if col in df_copy.columns:
                df_copy.pop(col)
        return df_copy

