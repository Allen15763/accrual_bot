"""
會計與 OPS 底稿比對驗證步驟
用於比對會計前期底稿與 OPS 驗收檔案的 locker 類型數量
"""

import re
import time
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder,
    create_error_metadata
)
from accrual_bot.utils.helpers.data_utils import extract_clean_description


class AccountingOPSValidationStep(PipelineStep):
    """
    會計與 OPS 底稿比對驗證步驟
    
    功能：
    1. 篩選 OPS 驗收月份小於當月的資料
    2. 依據 PO 單號聚合 OPS 的金額欄位
    3. 從會計資料的 Item Description 提取 locker_type
    4. 依據 PO# 聚合會計資料的 locker_type（同單同類型去重）
    5. 比對兩邊按單號聚合後的 locker 類型數量
    6. 輸出比對結果與差異報告
    
    使用範例：
        step = AccountingOPSValidationStep(
            name="ValidateAccountingOPS",
            amount_columns=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'],
            locker_pattern=r'SPX_門市智取櫃工程SPX locker\s?(.*)'
        )
    """
    
    # 預設的金額欄位
    DEFAULT_AMOUNT_COLUMNS = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 
        'DA', 'XA', 'XB', 'XC', 'XD', 'XE', 'XF',
        '超出櫃體安裝費', '超出櫃體運費', '裝運費'
    ]
    
    def __init__(
        self,
        name: str = "AccountingOPSValidation",
        amount_columns: Optional[List[str]] = None,
        locker_pattern: str = r'SPX\s+locker\s+([A-Z]{1,2}|控制主[櫃機]|[^\s]+?)(?:\s*第[一二]期款項|\s*訂金|\s*\d+%款項|\s*#)',
        **kwargs
    ):
        """
        初始化步驟
        
        Args:
            name: 步驟名稱
            amount_columns: OPS 資料要聚合的金額欄位列表
            locker_pattern: 提取 locker 類型的正則表達式
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(
            name,
            description="Validate accounting workpaper against OPS validation data",
            **kwargs
        )
        
        self.amount_columns = amount_columns or self.DEFAULT_AMOUNT_COLUMNS
        self.locker_pattern = locker_pattern
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行比對驗證"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            self.logger.info("Starting accounting and OPS validation...")
            
            # 階段 1: 取得資料
            df_accounting, df_ops = self._get_data_from_context(context)
            
            # 階段 2: 取得當月日期並篩選 OPS 資料
            processing_date = context.metadata.processing_date
            df_ops_filtered = self._filter_ops_by_date(df_ops, processing_date)
            
            # 階段 3: 聚合 OPS 資料
            df_ops_agg, df_ops_agg_by_type = self._aggregate_ops_data(df_ops_filtered)
            
            # 階段 4: 提取會計資料的 locker_type
            df_accounting_processed = self._extract_locker_type(df_accounting)
            
            # 階段 5: 聚合會計資料（去重）
            df_accounting_agg, df_accounting_agg_by_type = self._aggregate_accounting_data(
                df_accounting_processed
            )
            
            # 階段 6: 比對兩邊資料
            comparison_result = self._compare_data(
                df_accounting_agg, df_ops_agg,  # PO單號維度的表，暫時未使用
                df_accounting_agg_by_type, df_ops_agg_by_type)
            
            # 階段 7: 生成比對報告
            report = self._generate_report(comparison_result)
            
            # 儲存結果到 context
            self._store_results_to_context(context, comparison_result, report)
            
            # 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            # 構建 metadata
            metadata = (
                StepMetadataBuilder()
                .set_row_counts(
                    len(df_accounting) + len(df_ops),
                    len(comparison_result)
                )
                .set_process_counts(
                    processed=len(comparison_result),
                )
                .set_time_info(start_datetime, end_datetime)
                .add_custom('ops_records_filtered', len(df_ops_filtered))
                .add_custom('accounting_po_count', len(df_accounting_agg))
                .add_custom('ops_po_count', len(df_ops_agg))
                .add_custom('matched_count', report['matched_count'])
                .add_custom('mismatched_count', report['mismatched_count'])
                .add_custom('accounting_only_count', report['accounting_only_count'])
                .add_custom('ops_only_count', report['ops_only_count'])
                .build()
            )
            
            self.logger.info(
                f"Validation completed: {report['matched_count']} matched, "
                f"{report['mismatched_count']} mismatched in {duration:.2f}s"
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=comparison_result,
                message=(
                    f"Validated {len(comparison_result)} POs: "
                    f"{report['matched_count']} matched, "
                    f"{report['mismatched_count']} mismatched"
                ),
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            self.logger.error(f"Validation failed: {str(e)}", exc_info=True)
            context.add_error(f"Accounting/OPS validation failed: {str(e)}")
            
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='accounting_ops_validation'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Failed to validate data: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    def _get_data_from_context(
        self,
        context: ProcessingContext
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        從 context 取得會計和 OPS 資料
        
        Args:
            context: 處理上下文
            
        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: (會計資料, OPS資料)
        """
        df_accounting = context.get_auxiliary_data('accounting_workpaper')
        df_ops = context.get_auxiliary_data('ops_validation')
        
        if df_accounting is None or df_accounting.empty:
            raise ValueError("Accounting workpaper data not found in context")
        
        if df_ops is None or df_ops.empty:
            raise ValueError("OPS validation data not found in context")
        
        self.logger.info(
            f"Retrieved data: accounting={len(df_accounting)} rows, "
            f"ops={len(df_ops)} rows"
        )
        
        return df_accounting.copy(), df_ops.copy()
    
    def _filter_ops_by_date(
        self,
        df_ops: pd.DataFrame,
        processing_date: int
    ) -> pd.DataFrame:
        """
        篩選 OPS 資料：驗收月份必須小於當月
        
        Args:
            df_ops: OPS 資料
            processing_date: 當月日期 (格式: YYYYMM)
            
        Returns:
            pd.DataFrame: 篩選後的 OPS 資料
        """
        # 將 processing_date 轉換為 datetime（假設是 YYYYMM 格式）
        if isinstance(processing_date, int):
            processing_year = processing_date // 100
            processing_month = processing_date % 100
            current_date = pd.Timestamp(year=processing_year, month=processing_month, day=1)
        else:
            raise ValueError(f"Invalid processing_date format: {processing_date}")
        
        # 篩選驗收月份小於當月的資料
        if '驗收月份' not in df_ops.columns:
            raise ValueError("Column '驗收月份' not found in OPS data")
        
        df_filtered = df_ops[df_ops['驗收月份'] < current_date].copy()
        
        self.logger.info(
            f"Filtered OPS data by date: {len(df_filtered)}/{len(df_ops)} rows "
            f"(validation date < {processing_date})"
        )
        
        return df_filtered
    
    def _aggregate_ops_data(self, df_ops: pd.DataFrame) -> pd.DataFrame:
        """
        依據 PO單號 聚合 OPS 金額欄位
        
        Args:
            df_ops: OPS 資料
            
        Returns:
            pd.DataFrame: 聚合後的 OPS 資料
        """
        # 確認 PO單號 欄位存在
        if 'PO單號' not in df_ops.columns:
            raise ValueError("Column 'PO單號' not found in OPS data")
        
        # 篩選出存在的金額欄位
        existing_amount_cols = [
            col for col in self.amount_columns 
            if col in df_ops.columns
        ]
        
        if not existing_amount_cols:
            raise ValueError(
                f"None of the amount columns {self.amount_columns} "
                f"found in OPS data"
            )
        
        # 聚合：計算每個 PO 單號中非空金額欄位的數量
        # agg_dict = {col: lambda x: x.notna().sum() for col in existing_amount_cols}
        # df_agg = df_ops.groupby('PO單號', as_index=False).agg(agg_dict)
        df_agg = df_ops.groupby('PO單號', as_index=False)[self.amount_columns].sum()
        
        # 計算總 locker 數量（所有金額欄位的總和）不含費用
        exclude_fee_cols = [col for col in existing_amount_cols if '費' not in col]
        df_agg['ops_locker_count'] = df_agg[exclude_fee_cols].sum(axis=1)

        # 轉長表格方便核對
        df_agg_wide = df_agg.melt(
            id_vars='PO單號',
            value_vars=self.amount_columns,
            var_name='locker_type',
            value_name='num/amt'
        ).rename(columns={'PO單號': 'po_number'})
        
        # 保留 PO單號 和 locker 數量
        df_result = df_agg[['PO單號', 'ops_locker_count']].copy().rename(columns={'PO單號': 'po_number'})
        
        self.logger.info(
            f"Aggregated OPS data: {len(df_result)} unique POs"
        )
        
        return df_result, df_agg_wide
    
    def _extract_locker_type(self, df_accounting: pd.DataFrame) -> pd.DataFrame:
        """
        從會計資料的 Item Description 提取 locker_type
        
        Args:
            df_accounting: 會計資料
            
        Returns:
            pd.DataFrame: 加入 locker_type 欄位的會計資料
        """
        if 'Item Description' not in df_accounting.columns:
            raise ValueError("Column 'Item Description' not found in accounting data")
        
        def extract_locker_info(text):
            """提取 locker 資訊"""
            if not isinstance(text, str):
                return None
            
            match = re.search(self.locker_pattern, text)
            if match:
                return match.group(1).strip()
            return None
        
        # 提取 locker_type
        df_accounting['locker_type'] = (df_accounting['Item Description']
                                        .apply(extract_locker_info)
                                        .str.replace('主機', '主櫃')
                                        .str.replace('控制主櫃', 'DA')
                                        .str.replace('安裝運費', '裝運費')  # PO摘要寫"安裝運費"，驗收底稿寫"裝運費"
                                        )
        
        # 統計提取結果
        extracted_count = df_accounting['locker_type'].notna().sum()
        self.logger.info(
            f"Extracted locker_type: {extracted_count}/{len(df_accounting)} rows "
            f"({extracted_count/len(df_accounting)*100:.1f}%)"
        )
        
        return df_accounting
    
    def _aggregate_accounting_data(
        self,
        df_accounting: pd.DataFrame
    ) -> pd.DataFrame:
        """
        依據 PO# 聚合會計資料的 locker_type（同單同類型去重）
        
        Args:
            df_accounting: 會計資料（已提取 locker_type）
            
        Returns:
            pd.DataFrame: 聚合後的會計資料
        """
        if 'PO#' not in df_accounting.columns:
            raise ValueError("Column 'PO#' not found in accounting data")
        
        # 只保留有 locker_type 的資料
        df_with_locker = df_accounting[
            df_accounting['locker_type'].notna()
        ].copy()
        
        if df_with_locker.empty:
            self.logger.warning("No locker_type found in accounting data")
            return pd.DataFrame(columns=['PO#', 'accounting_locker_count'])
        
        # 按 PO# 和 locker_type 去重
        df_unique = df_with_locker[['PO#', 'memo', 'locker_type']].drop_duplicates()
        df_unique['memo'] = df_unique.memo.replace('', 0).astype('Float64')
        
        # 計算每個 PO# 的 locker 類型數量
        df_agg = df_unique.groupby('PO#', as_index=False).agg(
            accounting_locker_count=('locker_type', 'count')
        ).rename(columns={"PO#": 'po_number'})

        df_agg_by_type = df_unique.groupby(['PO#', 'locker_type'], as_index=False).agg(
            memo_sum=('memo', 'sum')
        ).rename(columns={"PO#": 'po_number'})
        
        self.logger.info(
            f"Aggregated accounting data: {len(df_agg)} unique POs, "
            f"total {df_agg['accounting_locker_count'].sum()} locker types "
            f"(after deduplication)"
        )
        
        return df_agg, df_agg_by_type
    
    def _compare_data(
        self,
        df_accounting: pd.DataFrame,
        df_ops: pd.DataFrame,
        df_acc_by_type: pd.DataFrame,
        df_ops_by_type: pd.DataFrame
    ) -> pd.DataFrame:
        """
        比對兩邊按單號聚合後的 locker 類型數量
        
        Args:
            df_accounting: 聚合後的會計資料
            df_ops: 聚合後的 OPS 資料
            
        Returns:
            pd.DataFrame: 比對結果
        """
        # 使用 outer join 確保包含所有 PO
        df_comparison_by_type = pd.merge(
            df_acc_by_type,
            df_ops_by_type,
            on=['po_number', 'locker_type'],
            how='outer',
            suffixes=('_acc', '_ops')
        )

        conditions = [
            df_comparison_by_type['memo_sum'].isna(),
            df_comparison_by_type['num/amt'].isna(),
            df_comparison_by_type['memo_sum'].sub(df_comparison_by_type['num/amt'], fill_value=0) == 0,
            df_comparison_by_type['memo_sum'].sub(df_comparison_by_type['num/amt'], fill_value=0) != 0,
        ]

        results = [
            "ops_only",
            "accounting_only",
            "matched",
            "mismatched"
        ]
        df_comparison_by_type['status'] = np.select(
            conditions, results, default=pd.NA
        )
        
        df_result = df_comparison_by_type.copy()
        
        df_result = df_result.sort_values(
            by=['po_number', 'locker_type'],
            ascending=[True, False]
        ).reset_index(drop=True)
        
        self.logger.info(
            f"Comparison completed: {len(df_result)} POs compared"
        )
        
        return df_result
    
    def _generate_report(self, df_comparison: pd.DataFrame) -> Dict[str, Any]:
        """
        生成比對報告
        
        Args:
            df_comparison: 比對結果
            
        Returns:
            Dict[str, Any]: 報告摘要
        """
        report = {
            'total_pos': len(df_comparison),
            'matched_count': len(df_comparison[df_comparison['status'] == 'matched']),
            'mismatched_count': len(df_comparison[df_comparison['status'] == 'mismatched']),
            'accounting_only_count': len(df_comparison[df_comparison['status'] == 'accounting_only']),
            'ops_only_count': len(df_comparison[df_comparison['status'] == 'ops_only']),
            # 'total_accounting_lockers': df_comparison['accounting_locker_count'].sum(),
            # 'total_ops_lockers': df_comparison['ops_locker_count'].sum(),
        }
        
        # 計算匹配率
        if report['total_pos'] > 0:
            report['match_rate'] = (
                report['matched_count'] / report['total_pos'] * 100
            )
        else:
            report['match_rate'] = 0.0
        
        self.logger.info(
            f"Validation Report: "
            f"Total={report['total_pos']}, "
            f"Matched={report['matched_count']} ({report['match_rate']:.1f}%), "
            f"Mismatched={report['mismatched_count']}, "
            f"Accounting Only={report['accounting_only_count']}, "
            f"OPS Only={report['ops_only_count']}"
        )
        
        return report
    
    def _store_results_to_context(
        self,
        context: ProcessingContext,
        comparison_result: pd.DataFrame,
        report: Dict[str, Any]
    ):
        """
        將比對結果儲存到 context
        
        Args:
            context: 處理上下文
            comparison_result: 比對結果
            report: 報告摘要
        """
        # 儲存完整比對結果
        context.add_auxiliary_data('validation_comparison', comparison_result)
        
        # 儲存報告摘要
        context.set_variable('validation_report', report)
        
        # 儲存不匹配的記錄（供後續處理）
        mismatched = comparison_result[
            comparison_result['status'].isin(['mismatched', 'accounting_only'])
        ].copy()
        context.add_auxiliary_data('validation_mismatches', mismatched)
        
        self.logger.info(
            f"Stored validation results to context: "
            f"{len(comparison_result)} total, {len(mismatched)} mismatches(include accounting_only)"
        )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 驗證是否通過
        """
        # 檢查會計資料
        df_accounting = context.get_auxiliary_data('accounting_workpaper')
        if df_accounting is None or df_accounting.empty:
            self.logger.error("Accounting workpaper data not found")
            context.add_error("Accounting workpaper data not found")
            return False
        
        # 檢查 OPS 資料
        df_ops = context.get_auxiliary_data('ops_validation')
        if df_ops is None or df_ops.empty:
            self.logger.error("OPS validation data not found")
            context.add_error("OPS validation data not found")
            return False
        
        # 檢查必要欄位
        required_accounting_cols = ['PO#', 'Item Description']
        missing_acc_cols = [
            col for col in required_accounting_cols 
            if col not in df_accounting.columns
        ]
        if missing_acc_cols:
            self.logger.error(
                f"Missing required columns in accounting data: {missing_acc_cols}"
            )
            context.add_error(
                f"Missing required columns in accounting data: {missing_acc_cols}"
            )
            return False
        
        required_ops_cols = ['PO單號', '驗收月份']
        missing_ops_cols = [
            col for col in required_ops_cols 
            if col not in df_ops.columns
        ]
        if missing_ops_cols:
            self.logger.error(
                f"Missing required columns in OPS data: {missing_ops_cols}"
            )
            context.add_error(
                f"Missing required columns in OPS data: {missing_ops_cols}"
            )
            return False
        
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """
        回滾操作
        
        Args:
            context: 處理上下文
            error: 觸發回滾的錯誤
        """
        self.logger.warning(
            f"Rolling back validation due to error: {str(error)}"
        )
        
        # 清理 context 中的驗證結果
        if 'validation_comparison' in context.auxiliary_data:
            del context.auxiliary_data['validation_comparison']
        if 'validation_mismatches' in context.auxiliary_data:
            del context.auxiliary_data['validation_mismatches']
        
        context.remove_variable('validation_report')
