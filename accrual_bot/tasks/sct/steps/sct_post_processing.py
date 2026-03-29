"""
SCT 資料格式化步驟

功能：
1. 格式化數值列（整數/浮點）
2. 格式化日期列（統一 YYYY-MM-DD）
3. 清理 NaN 值
4. 重新排列欄位順序（狀態欄位、review 欄位）
5. 重新命名欄位（snake_case）+ 資料型態轉換
6. 移除臨時欄位
7. 格式化 ERM（%b-%y → %Y/%m）
8. 篩選輸出欄位（TOML output_columns_po / output_columns_pr）

配置驅動：所有欄位清單定義在 stagging_sct.toml [sct_reformatting]
"""

import pandas as pd
from typing import Dict, Any, List

from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.post_processing import BasePostProcessingStep
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.helpers.data_utils import clean_po_data


class SCTPostProcessingStep(BasePostProcessingStep):
    """
    SCT 資料格式化 — 配置驅動

    功能：
    1. 根據 TOML 配置格式化數值、日期欄位
    2. 清理 NaN 值（'nan'/'<NA>' → pd.NA）
    3. 重新排列欄位順序
    4. snake_case 化欄位名（clean_po_data）
    5. 移除臨時計算欄位
    6. 格式化 ERM
    7. 篩選最終輸出欄位

    適用 PO + PR 兩種 pipeline，透過 context.metadata.processing_type 區分
    """

    def __init__(self, name: str = "SCTPostProcessing", **kwargs):
        super().__init__(
            name=name,
            description="SCT data reformatting and cleanup",
            **kwargs
        )
        self._config: Dict[str, Any] = config_manager._config_toml.get(
            'sct_reformatting', {}
        )
        self.logger.info(f"Initialized {name} with config keys: {list(self._config.keys())}")

    # ========== 主要處理邏輯 ==========

    def _process_data(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """SCT 資料格式化主流程"""
        self.logger.info("開始 SCT 資料格式化...")

        # 1. 格式化數值列
        df = self._format_numeric_columns(df)
        self._add_note("完成數值列格式化")

        # 2. 格式化日期列
        df = self._reformat_dates(df)
        self._add_note("完成日期列格式化")

        # 3. 清理 NaN 值
        df = self._clean_nan_values(df)
        self._add_note("完成 NaN 值清理")

        # 4. 重新排列欄位
        df = self._rearrange_columns(df)
        self._add_note("完成欄位重新排列")

        # 5. 重新命名欄位 + 資料型態
        df = self._rename_columns_dtype(df)
        self._add_note("完成欄位重命名和型態轉換")

        # 6. 確保 review/AP 欄位在最後
        df = self._rearrange_columns(df)
        self._add_note("完成最終欄位排列")

        # 7. 保存含臨時欄位的完整數據至 auxiliary
        self._save_temp_columns_data(df, context)
        self._add_note("完成暫時性數據保存")

        # 8. 移除臨時欄位
        processing_type: str = context.metadata.processing_type
        df = self._remove_temp_columns(df, processing_type)
        self._add_note("完成臨時欄位移除")

        # 9. 格式化 ERM
        df = self._reformat_erm(df)
        self._add_note("完成 ERM 格式化")

        # 10. 篩選輸出欄位
        df = self._select_output_columns(df, processing_type)
        self._add_note("完成輸出欄位篩選")

        self.logger.info("SCT 資料格式化完成")
        return df

    # ========== 格式化方法 ==========

    def _format_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化數值列（從 TOML 讀取欄位清單）"""
        # 整數列
        int_columns: List[str] = self._config.get('int_columns', ['Line#', 'GL#'])
        for col in int_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
                except Exception as e:
                    self.logger.warning(f"整數格式化失敗 {col}: {e}")

        # 浮點數列
        float_columns: List[str] = self._config.get('float_columns', [
            'Unit Price', 'Entry Amount', 'Accr. Amount'
        ])
        for col in float_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                except Exception as e:
                    self.logger.warning(f"浮點數格式化失敗 {col}: {e}")

        return df

    def _reformat_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化日期列（統一 YYYY-MM-DD）"""
        date_columns: List[str] = self._config.get('date_columns', [])
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.warning(f"日期格式化失敗 {col}: {e}")

        return df

    def _clean_nan_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理 NaN 值（'nan'/'<NA>' → pd.NA）"""
        # 動態偵測狀態欄位
        status_columns = [col for col in df.columns if '狀態' in col]
        nan_clean_columns: List[str] = self._config.get('nan_clean_columns', [])
        columns_to_clean = list(set(nan_clean_columns + status_columns))

        for col in columns_to_clean:
            if col in df.columns:
                df[col] = df[col].replace('nan', pd.NA)
                df[col] = df[col].replace('<NA>', pd.NA)

        # 特殊處理 Accr. Amount
        if 'Accr. Amount' in df.columns:
            try:
                df['Accr. Amount'] = (
                    df['Accr. Amount'].astype('string').str.replace(',', '')
                    .replace('nan', '0')
                    .replace('<NA>', '0')
                    .astype('Float64')
                )
                df['Accr. Amount'] = df['Accr. Amount'].apply(
                    lambda x: x if x != 0 else None
                )
            except Exception as e:
                self.logger.warning(f"Accr. Amount 清理失敗: {e}")

        return df

    def _rearrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列欄位順序"""
        # 上月 FN 備註移到 Remarked by FN 之後
        if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
            fn_index = df.columns.get_loc('Remarked by FN') + 1
            last_month_col = df.pop('Remarked by 上月 FN')
            df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)

        if 'Remarked by 上月 FN' in df.columns and 'Remarked by 上月 FN PR' in df.columns:
            fn_pr_index = df.columns.get_loc('Remarked by 上月 FN') + 1
            last_month_pr_col = df.pop('Remarked by 上月 FN PR')
            df.insert(fn_pr_index, 'Remarked by 上月 FN PR', last_month_pr_col)

        # 狀態欄位移到是否估計入帳前
        status_cols = [col for col in df.columns if '狀態' in col]
        if status_cols and '是否估計入帳' in df.columns:
            status_column = status_cols[0]
            accrual_index = df.columns.get_loc('是否估計入帳')
            if accrual_index > 0:
                po_status_col = df.pop(status_column)
                df.insert(accrual_index - 1, status_column, po_status_col)

        # PR 欄位移到 Noted by Procurement 之後
        if 'Noted by Procurement' in df.columns:
            noted_index = df.columns.get_loc('Noted by Procurement') + 1
            for col_name in ['Remarked by Procurement PR', 'Noted by Procurement PR']:
                if col_name in df.columns:
                    col = df.pop(col_name)
                    df.insert(noted_index, col_name, col)
                    noted_index += 1

        # review/AP 欄位移到最後（處理原始欄位名和 snake_case 名）
        tail_columns = self._config.get('tail_columns', [
            'Question from Reviewer', 'Check by AP'
        ])
        tail_columns_snake = self._config.get('tail_columns_snake', [
            'question_from_reviewer', 'check_by_ap'
        ])

        for tail_set in [tail_columns, tail_columns_snake]:
            present = [c for c in tail_set if c in df.columns]
            if len(present) == len(tail_set) and len(present) > 0:
                cols = [c for c in df.columns if c not in present]
                cols.extend(present)
                df = df[cols]

        return df

    def _rename_columns_dtype(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新命名欄位（snake_case 化）+ 資料型態轉換"""
        try:
            df = df.rename(columns={'Product code': 'product_code_c'})
            df = clean_po_data(df)
        except Exception as e:
            self.logger.warning(f"欄位重命名/型態轉換失敗: {e}")

        return df

    def _save_temp_columns_data(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> None:
        """保存含臨時欄位的完整數據至 auxiliary"""
        if isinstance(df, pd.DataFrame) and not df.empty:
            data_name = 'result_with_temp_cols'
            data_copy = df.copy()
            context.add_auxiliary_data(data_name, data_copy)
            self.logger.info(
                f"Added auxiliary data: {data_name} (shape: {data_copy.shape})"
            )

    def _remove_temp_columns(
        self,
        df: pd.DataFrame,
        processing_type: str
    ) -> pd.DataFrame:
        """移除臨時計算欄位"""
        temp_columns: List[str] = self._config.get('temp_columns', [
            '檔案日期', 'Expected Received Month_轉換格式',
            'YMs of Item Description',
            'expected_received_month_轉換格式', 'yms_of_item_description',
            'PR Product Code Check', 'pr_product_code_check',
            'matched_condition_on_status',
        ])

        if processing_type == 'PR':
            pr_extra: List[str] = self._config.get('pr_extra_temp_columns', [
                'remarked_by_procurement_pr', 'noted_by_procurement_pr',
                'remarked_by_上月_fn_pr',
            ])
            temp_columns = temp_columns + pr_extra

        for col in temp_columns:
            if col in df.columns:
                df = df.drop(columns=[col])

        return df

    def _reformat_erm(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化 ERM（%b-%y → %Y/%m）"""
        if 'expected_receive_month' in df.columns:
            try:
                df['expected_receive_month'] = (
                    pd.to_datetime(
                        df['expected_receive_month'],
                        format='%b-%y',
                        errors='coerce'
                    ).dt.strftime('%Y/%m')
                )
            except Exception as e:
                self.logger.warning(f"ERM 格式化失敗: {e}")

        return df

    def _select_output_columns(
        self,
        df: pd.DataFrame,
        processing_type: str
    ) -> pd.DataFrame:
        """篩選輸出欄位（從 TOML 讀取欄位清單）"""
        config_key = (
            'output_columns_po' if processing_type == 'PO'
            else 'output_columns_pr'
        )
        output_columns: List[str] = self._config.get(config_key, [])

        if not output_columns:
            self.logger.warning(f"未設定 {config_key}，返回全部欄位")
            return df

        # 容錯：只保留存在的欄位
        present = [col for col in output_columns if col in df.columns]
        missing = [col for col in output_columns if col not in df.columns]

        if missing:
            self.logger.warning(
                f"輸出欄位清單中有 {len(missing)} 個欄位不存在: {missing}"
            )

        return df[present]

    # ========== 驗證方法 ==========

    def _validate_result(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> Dict[str, Any]:
        """結果驗證"""
        issues: List[str] = []

        # 驗證 1: 檢查數值欄位型態
        numeric_check_cols = ['entry_amount', 'accr._amount']
        for col in numeric_check_cols:
            if col in df.columns:
                if df[col].dtype not in ['float64', 'Float64', 'int64', 'Int64']:
                    issues.append(
                        f"欄位 '{col}' 的資料型態不是數值型: {df[col].dtype}"
                    )

        # 驗證 2: 檢查臨時欄位是否已移除
        temp_cols_found = [
            col for col in df.columns
            if col in ['檔案日期', 'expected_received_month_轉換格式',
                       'yms_of_item_description']
        ]
        if temp_cols_found:
            issues.append(f"臨時欄位未完全移除: {temp_cols_found}")

        if issues:
            return {
                'is_valid': False,
                'message': f"驗證發現 {len(issues)} 個問題",
                'details': {'issues': issues}
            }

        return {
            'is_valid': True,
            'message': '所有驗證通過',
            'details': {
                'total_rows': len(df),
                'total_columns': len(df.columns)
            }
        }

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        if context.data is None or context.data.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False

        self.logger.info("輸入驗證通過")
        return True

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作"""
        self.logger.warning(f"回滾 SCT 資料格式化：{error}")
