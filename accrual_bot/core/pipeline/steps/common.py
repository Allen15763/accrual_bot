"""
通用處理步驟
適用於所有實體類型的基礎步驟
"""

from typing import List, Dict, Optional, Any
from datetime import datetime
import time
import os
import traceback
import pandas as pd
import numpy as np

from ..base import PipelineStep, StepResult, StepStatus
from ..context import ProcessingContext, ValidationResult

# === 階段二：工具函數整合 - 引入配置管理器 ===
try:
    from ....utils.config import config_manager
except ImportError:
    import sys
    from pathlib import Path
    current_dir = Path(__file__).parent.parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from utils.config import config_manager

from accrual_bot.utils.helpers.data_utils import (create_mapping_dict, 
                                                  safe_string_operation,
                                                  extract_date_range_from_description
                                                  )

from accrual_bot.utils.config.constants import STATUS_VALUES


class DataCleaningStep(PipelineStep):
    """
    數據清理步驟
    清理NaN值、去除空白字符、標準化數據
    """
    
    def __init__(self, 
                 name: str = "DataCleaning",
                 columns_to_clean: Optional[List[str]] = None,
                 **kwargs):
        super().__init__(name, description="Clean and standardize data", **kwargs)
        self.columns_to_clean = columns_to_clean
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據清理"""
        try:
            df = context.data.copy()
            
            # 清理指定列的NaN值
            if self.columns_to_clean:
                for col in self.columns_to_clean:
                    if col in df.columns:
                        df[col] = df[col].fillna('').astype(str).str.strip()
                        self.logger.debug(f"Cleaned column: {col}")
            
            # 去除所有字符串列的空白
            string_columns = df.select_dtypes(include=['object']).columns
            for col in string_columns:
                df[col] = df[col].astype(str).str.strip()
            
            # 替換常見的無效值
            df.replace(['nan', 'None', 'N/A', 'n/a'], '', inplace=True)
            
            # 更新上下文數據
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Cleaned {len(df)} rows",
                metadata={
                    'cleaned_columns': len(self.columns_to_clean) if self.columns_to_clean else len(string_columns)}
            )
            
        except Exception as e:
            self.logger.error(f"Data cleaning failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data to clean")
            return False
        return True


class DateFormattingStep(PipelineStep):
    """
    日期格式化步驟
    統一日期格式
    """
    
    def __init__(self,
                 name: str = "DateFormatting",
                 date_columns: Optional[Dict[str, str]] = None,
                 **kwargs):
        super().__init__(name, description="Format date columns", **kwargs)
        self.date_columns = date_columns or {
            'Expected Receive Month': '%b-%y',
            'Submission Date': '%d-%b-%y'
        }
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行日期格式化"""
        try:
            df = context.data.copy()
            formatted_count = 0
            
            for col, format_str in self.date_columns.items():
                if col in df.columns:
                    # 轉換日期格式
                    df[col] = pd.to_datetime(df[col], format=format_str, errors='coerce')
                    
                    # 統一輸出格式
                    if 'Month' in col:
                        df[col] = df[col].dt.strftime('%Y-%m')
                    else:
                        df[col] = df[col].dt.strftime('%Y-%m-%d')
                    
                    formatted_count += 1
                    self.logger.debug(f"Formatted date column: {col}")
            
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Formatted {formatted_count} date columns",
                metadata={'formatted_columns': formatted_count}
            )
            
        except Exception as e:
            self.logger.error(f"Date formatting failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.data is not None and not context.data.empty


class DateParsingStep(PipelineStep):
    """
    日期解析步驟
    從Item Description解析日期範圍
    
    ### 核心邏輯：
    - 使用正則表達式從Item Description中提取日期範圍
    - 支援多種日期格式 (YYYY/MM, YYYYMM, MMM-YY等)
    - 轉換為統一的YYYYMM格式
    - 處理特殊情況和錯誤格式
    """
    
    def __init__(self, name: str = "DateParsing", **kwargs):
        super().__init__(name, description="Parse dates from item description", **kwargs)
        # 階段二任務2.3：從config.ini讀取正則模式，不使用constants.py
        self.regex_patterns = config_manager.get_regex_patterns()
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行日期解析
        
        ### 實現邏輯摘要：
        1. 從Item Description提取日期範圍
        2. 轉換Expected Receive Month為數值格式
        3. 驗證日期範圍有效性
        4. 添加解析結果到新列
        """
        try:
            df = context.data.copy()
            
            # === 詳細實現邏輯 ===
            # 1. 轉換Expected Receive Month為YYYYMM格式
            # 2. 從Item Description提取日期（正則匹配）
            # 3. 處理各種日期格式
            # 4. 驗證和標準化日期範圍
            
            # 這裡實際實現時調用原有的parse_date_from_description邏輯
            
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Date parsing completed"
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        required_cols = ['Item Description']
        return all(col in context.data.columns for col in required_cols)


class ValidationStep(PipelineStep):
    """
    數據驗證步驟
    驗證數據完整性和業務規則
    """
    
    def __init__(self,
                 name: str = "Validation",
                 validations: Optional[List[str]] = None,
                 **kwargs):
        super().__init__(name, description="Validate data integrity", **kwargs)
        self.validations = validations or ['required_columns', 'data_types']
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行驗證"""
        try:
            validation_result = ValidationResult(is_valid=True)
            
            for validation_type in self.validations:
                if validation_type == 'required_columns':
                    self._validate_required_columns(context, validation_result)
                elif validation_type == 'data_types':
                    self._validate_data_types(context, validation_result)
                elif validation_type == 'business_rules':
                    self._validate_business_rules(context, validation_result)
            
            # 添加驗證結果到上下文
            context.add_validation(self.name, validation_result)
            
            if validation_result.is_valid:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SUCCESS,
                    message="Validation passed",
                    metadata={'warnings': len(validation_result.warnings)}
                )
            else:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="Validation failed",
                    metadata={
                        'errors': validation_result.errors,
                        'warnings': validation_result.warnings
                    }
                )
                
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _validate_required_columns(self, context: ProcessingContext, result: ValidationResult):
        """驗證必要列"""
        if context.is_po_processing():
            required = ['PO#', 'Item Description', 'GL#', 'Entry Amount']
        else:
            required = ['PR#', 'Item Description', 'GL#']
        
        missing = [col for col in required if col not in context.data.columns]
        if missing:
            result.add_error(f"Missing required columns: {missing}")
    
    def _validate_data_types(self, context: ProcessingContext, result: ValidationResult):
        """驗證數據類型"""
        # 檢查數值列
        numeric_columns = ['Entry Amount', 'Entry Quantity', 'Received Quantity']
        for col in numeric_columns:
            if col in context.data.columns:
                try:
                    pd.to_numeric(context.data[col], errors='coerce')
                except Exception as err:
                    self.logger.error(f"_validate_data_types Failed: {err}")
                    result.add_warning(f"Column {col} contains non-numeric values")
    
    def _validate_business_rules(self, context: ProcessingContext, result: ValidationResult):
        """
        驗證業務規則
        
        ### 業務規則摘要：
        - Entry Amount > 0 for accrual items
        - GL# must be valid account code
        - Department code format validation
        - Date range consistency
        """
        df = context.data
        
        # 檢查金額
        if 'Entry Amount' in df.columns:
            negative_amounts = df[df['Entry Amount'] < 0]
            if not negative_amounts.empty:
                result.add_warning(f"Found {len(negative_amounts)} items with negative amounts")
        
        # 其他業務規則驗證...
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.data is not None and not context.data.empty


class ExportStep(PipelineStep):
    """
    數據導出步驟
    將處理結果導出到文件
    """
    
    def __init__(self,
                 name: str = "Export",
                 format: str = "excel",
                 output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(name, description="Export processed data", **kwargs)
        self.format = format
        self.output_path = output_path or "output"
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行導出"""
        try:
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            entity = context.metadata.entity_type
            proc_type = context.metadata.processing_type
            
            if self.format == "excel":
                filename = f"{self.output_path}/{entity}_{proc_type}_{timestamp}.xlsx"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                context.data.to_excel(filename, index=False)
            elif self.format == "csv":
                filename = f"{self.output_path}/{entity}_{proc_type}_{timestamp}.csv"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                context.data.to_csv(filename, index=False)
            elif self.format == "parquet":
                filename = f"{self.output_path}/{entity}_{proc_type}_{timestamp}.parquet"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                context.data.to_parquet(filename, index=False)
            else:
                raise ValueError(f"Unsupported export format: {self.format}")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Exported to {filename}",
                metadata={'filename': filename, 'rows': len(context.data)}
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.data is not None and not context.data.empty


class DataIntegrationStep(PipelineStep):
    """
    數據整合步驟
    整合輔助數據源
    """
    
    def __init__(self,
                 name: str = "DataIntegration",
                 source_name: str = None,
                 join_columns: Optional[List[str]] = None,
                 how: str = "left",
                 **kwargs):
        super().__init__(name, description="Integrate auxiliary data", **kwargs)
        self.source_name = source_name
        self.join_columns = join_columns or ['PO#']
        self.how = how
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據整合"""
        try:
            # 獲取輔助數據
            aux_data = context.get_auxiliary_data(self.source_name)
            
            if aux_data is None:
                self.logger.warning(f"Auxiliary data '{self.source_name}' not found")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    message=f"No auxiliary data: {self.source_name}"
                )
            
            # 合併數據
            df = context.data.copy()
            merged_df = pd.merge(
                df,
                aux_data,
                on=self.join_columns,
                how=self.how,
                suffixes=('', f'_{self.source_name}')
            )
            
            context.update_data(merged_df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=merged_df,
                message=f"Integrated {self.source_name}",
                metadata={
                    'original_rows': len(df),
                    'merged_rows': len(merged_df),
                    'aux_rows': len(aux_data)
                }
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        # 檢查join列是否存在
        return all(col in context.data.columns for col in self.join_columns)


class ProductFilterStep(PipelineStep):
    """
    產品代碼過濾步驟
    
    功能: 過濾出包含/排除 特定 產品代碼的記錄，預設"LG_SPX"
    
    輸入: DataFrame with 'Product Code' column
    輸出: Filtered DataFrame
    """
    
    def __init__(self, 
                 name: str = "ProductFilter",
                 product_pattern: Optional[str] = None,
                 exclude: bool = False,
                 **kwargs):
        super().__init__(name, description="Filter product codes", **kwargs)
        # 從配置讀取 pattern，或使用提供的值
        self.product_pattern = product_pattern or config_manager.get(
            'SPX', 'product_pattern', '(?i)LG_SPX'
        )
        self.exclude = exclude
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行產品過濾"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            df = context.data.copy()
            original_count = len(df)
            
            self.logger.info(f"Filtering products with pattern: {self.product_pattern}")
            
            # 過濾
            mask = df['Product Code'].str.contains(self.product_pattern, na=False)
            if self.exclude:
                mask = ~mask
            filtered_df = df.loc[mask, :].reset_index(drop=True)
            
            context.update_data(filtered_df)
            
            filtered_count = len(filtered_df)
            removed_count = original_count - filtered_count
            
            # ✅ 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            self.logger.info(
                f"Product filtering complete: {original_count} -> {filtered_count} "
                f"(removed {removed_count} non-{self.product_pattern} items) in {duration:.2f}s"
            )
            
            if filtered_count == 0:
                context.add_warning("No products found after filtering")
            
            # ✅ 標準化 metadata
            filter_rate = filtered_count / original_count * 100
            speed = original_count / duration
            metadata = (StepMetadataBuilder()
                        .set_row_counts(original_count, filtered_count)
                        .set_process_counts(processed=filtered_count, skipped=removed_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('filter_pattern', self.product_pattern)
                        .add_custom('filter_rate', f"{(filter_rate):.2f}%" if original_count > 0 else "N/A")
                        .add_custom('processing_speed_rows_per_sec', f"{(speed):.0f}" if duration > 0 else "N/A")
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=filtered_df,
                message=f"Filtered to {filtered_count} items ({(filtered_count/original_count*100):.1f}%)",
                duration=duration,  # ✅ 新增
                metadata=metadata  # ✅ 標準化
            )
            
        except Exception as e:
            duration = time.time() - start_time  # ✅ 錯誤時也計算時間
            
            self.logger.error(f"Product filtering failed: {str(e)}", exc_info=True)
            context.add_error(f"Product filtering failed: {str(e)}")
            
            # ✅ 創建增強的錯誤 metadata
            error_metadata = create_error_metadata(
                e, context, self.name,
                filter_pattern=self.product_pattern,
                stage='product_filtering'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Product filtering failed: {str(e)}",
                duration=duration,  # ✅ 新增
                metadata=error_metadata  # ✅ 增強錯誤資訊
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data to filter")
            context.add_error("No data to filter")
            return False
        
        if 'Product Code' not in context.data.columns:
            self.logger.error("Missing 'Product Code' column")
            context.add_error("Missing 'Product Code' column")
            return False
        
        return True


class PreviousWorkpaperIntegrationStep(PipelineStep):
    """
    前期底稿整合步驟
    
    功能:
    1. 整合前期 PO 底稿
    2. 整合前期 PR 底稿
    3. 處理 memo 欄位
    
    輸入: DataFrame + Previous WP (PO and PR)
    輸出: DataFrame with previous workpaper info
    """
    
    def __init__(self, name: str = "PreviousWorkpaperIntegration", **kwargs):
        super().__init__(name, description="Integrate previous workpaper", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行前期底稿整合"""
        start_time = time.time()
        try:
            df = context.data.copy()
            previous_wp = context.get_auxiliary_data('previous')
            previous_wp_pr = context.get_auxiliary_data('previous_pr')
            m = context.get_variable('processing_month')
            
            if previous_wp is None and previous_wp_pr is None:
                self.logger.warning("No previous workpaper data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No previous workpaper data"
                )
            
            self.logger.info("Processing previous workpaper integration...")
            
            # 處理 PO 前期底稿
            if previous_wp is not None and not previous_wp.empty:
                df = self._process_previous_po(df, previous_wp, m)
                self.logger.info("Previous PO workpaper integrated")
            
            # 處理 PR 前期底稿
            if previous_wp_pr is not None and not previous_wp_pr.empty:
                df = self._process_previous_pr(df, previous_wp_pr)
                self.logger.info("Previous PR workpaper integrated")

            # 路徑參數傳入raw_pr才執行，把remark...PR跟Noted...PR欄位複製到沒...PR的欄位
            if 'raw_pr' in context.get_variable('file_paths').keys():
                df = self._copy_pr_result_non_pr_cols(df)
            
            context.update_data(df)
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Previous workpaper integrated successfully",
                duration=duration,
                metadata={
                    'po_integrated': previous_wp is not None,
                    'pr_integrated': previous_wp_pr is not None
                }
            )
            
        except Exception as e:
            self.logger.error(f"Previous workpaper integration failed: {str(e)}", exc_info=True)
            context.add_error(f"Previous workpaper integration failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _process_previous_po(self, df: pd.DataFrame, previous_wp: pd.DataFrame, m: int) -> pd.DataFrame:
        """處理前期 PO 底稿"""
        # 調用父類邏輯處理基本的前期底稿整合
        # 這裡需要實現類似 BasePOProcessor.process_previous_workpaper 的邏輯
        try:
            if previous_wp is None or previous_wp.empty:
                self.logger.info("前期底稿為空，跳過處理")
                return df
            
            # 重命名前期底稿中的列
            previous_wp_renamed = previous_wp.rename(
                columns={
                    'Remarked by FN': 'Remarked by FN_l',
                    'Remarked by Procurement': 'Remark by PR Team_l',
                    # 標準化後欄位名稱，for new previous
                    'remarked_by_fn': 'Remarked by FN_l',
                    'remarked_by_procurement': 'Remark by PR Team_l'
                }
            )

            # 獲取前期FN備註
            if 'PO Line' in df.columns:
                fn_mapping = create_mapping_dict(previous_wp_renamed.rename(columns={'po_line': 'PO Line'}), 
                                                 'PO Line', 'Remarked by FN_l')
                df['Remarked by 上月 FN'] = df['PO Line'].map(fn_mapping)
                
                # 獲取前期採購備註
                procurement_mapping = create_mapping_dict(
                    previous_wp_renamed.rename(columns={'po_line': 'PO Line'}), 
                    'PO Line', 'Remark by PR Team_l'
                )
                df['Remarked by 上月 Procurement'] = \
                    df['PO Line'].map(procurement_mapping)
            
            # 處理 memo 欄位
            if 'memo' in previous_wp.columns and 'PO Line' in df.columns:
                if 'PO Line' in previous_wp.columns:
                    memo_mapping = dict(zip(previous_wp['PO Line'], previous_wp['memo']))
                else:
                    memo_mapping = dict(zip(previous_wp['po_line'], previous_wp['memo']))
                df['memo'] = df['PO Line'].map(memo_mapping)
            
            # 處理前期 FN 備註
            if 'Remarked by FN' in previous_wp.columns:
                fn_mapping = dict(zip(previous_wp['PO Line'], previous_wp['Remarked by FN']))
            if 'remarked_by_fn' in previous_wp.columns:
                fn_mapping = dict(zip(previous_wp['po_line'], previous_wp['remarked_by_fn']))
            df['Remarked by 上月 FN'] = df['PO Line'].map(fn_mapping)
            return df
    
        except Exception as e:
            self.logger.error(f"處理前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理前期底稿時出錯")
    
    def _process_previous_pr(self, df: pd.DataFrame, previous_wp_pr: pd.DataFrame) -> pd.DataFrame:
        """處理前期 PR 底稿"""
        # 重命名前期 PR 底稿中的列
        if 'Remarked by FN' in previous_wp_pr.columns or 'remarked_by_fn' in previous_wp_pr.columns:
            previous_wp_pr = previous_wp_pr.rename(
                columns={'Remarked by FN': 'Remarked by FN_l',
                         'remarked_by_fn': 'Remarked by FN_l',
                         'pr_line': 'PR Line'}  # 標準化後欄位名稱，for new previous
            )
        
        # 獲取前期 PR FN 備註
        if 'Remarked by FN_l' in previous_wp_pr.columns and 'PR Line' in df.columns:
            pr_fn_mapping = dict(zip(previous_wp_pr['PR Line'], previous_wp_pr['Remarked by FN_l']))
            df['Remarked by 上月 FN PR'] = df['PR Line'].map(pr_fn_mapping)
        
        return df
    
    def _copy_pr_result_non_pr_cols(self, df, col_remark_fn: str = 'Remarked by 上月 FN'):
        df_copy = df.copy()

        if col_remark_fn in df_copy.columns:
            df_copy[col_remark_fn] = df_copy[col_remark_fn + ' PR']

        return df_copy
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for previous workpaper integration")
            return False
        
        return True


class ProcurementIntegrationStep(PipelineStep):
    """
    採購底稿整合步驟
    
    功能:
    1. 整合採購 PO 底稿
    2. 整合採購 PR 底稿
    3. 移除 SPT 模組給的狀態（SPX 有自己的狀態邏輯）
    
    輸入: DataFrame + Procurement WP (PO and PR)
    輸出: DataFrame with procurement info
    """
    
    def __init__(self, name: str = "ProcurementIntegration", **kwargs):
        super().__init__(name, description="Integrate procurement workpaper", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行採購底稿整合"""
        start_time = time.time()
        try:
            df = context.data.copy()
            procurement = context.get_auxiliary_data('procurement_po')
            procurement_pr = context.get_auxiliary_data('procurement_pr')
            
            if procurement is None and procurement_pr is None:
                self.logger.warning("No procurement data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No procurement data"
                )
            
            self.logger.info("Processing procurement integration...")
            
            # 處理 PO 採購底稿
            if procurement is not None and not procurement.empty:
                df = self._process_procurement_po(df, procurement)
                self.logger.info("Procurement PO integrated")
            
            # 處理 PR 採購底稿
            if procurement_pr is not None and not procurement_pr.empty:
                df = self._process_procurement_pr(df, procurement_pr)
                self.logger.info("Procurement PR integrated")
            
            # 路徑參數傳入raw_pr才執行，把remark...PR跟Noted...PR欄位複製到沒...PR的欄位
            if 'raw_pr' in context.get_variable('file_paths').keys():
                df = self._copy_pr_result_non_pr_cols(df)

            context.update_data(df)
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Procurement integrated successfully",
                metadata={
                    'po_integrated': procurement is not None,
                    'pr_integrated': procurement_pr is not None
                }
            )
            
        except Exception as e:
            self.logger.error(f"Procurement integration failed: {str(e)}", exc_info=True)
            context.add_error(f"Procurement integration failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _process_procurement_po(self, df: pd.DataFrame, procurement: pd.DataFrame) -> pd.DataFrame:
        """處理採購 PO 底稿"""
        # 調用父類邏輯處理基本的採購底稿整合
        # 這裡需要實現類似 BasePOProcessor.process_procurement_workpaper 的邏輯
        try:
            if procurement is None or procurement.empty:
                self.logger.info("採購底稿為空，跳過處理")
                return df
            
            # 重命名採購底稿中的列
            procurement_wp_renamed = procurement.rename(
                columns={
                    'Remarked by Procurement': 'Remark by PR Team',
                    'Noted by Procurement': 'Noted by PR',
                    # 標準化後欄位名稱，for new procurement
                    'remarked_by_procurement': 'Remark by PR Team',
                    'noted_by_procurement': 'Noted by PR',
                    'po_line': 'PO Line',
                    'pr_line': 'PR Line'
                }
            )
            
            # 通過PO Line獲取備註
            if 'PO Line' in df.columns:
                procurement_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PO Line', 'Remark by PR Team'
                )
                df['Remarked by Procurement'] = df['PO Line'].map(procurement_mapping)
                
                noted_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PO Line', 'Noted by PR'
                )
                df['Noted by Procurement'] = df['PO Line'].map(noted_mapping)
            
            # 通過PR Line獲取備註（如果PO Line沒有匹配到）
            if 'PR Line' in df.columns:
                pr_procurement_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PR Line', 'Remark by PR Team'
                )
                
                # 只更新尚未匹配的記錄
                df['Remarked by Procurement'] = \
                    (df.apply(lambda x: pr_procurement_mapping.get(x['PR Line'], None) 
                              if x['Remarked by Procurement'] is pd.NA else x['Remarked by Procurement'], axis=1))
            
            # 設置FN備註為採購備註
            df['Remarked by FN'] = df['Remarked by Procurement']
            
            # 標記不在採購底稿中的PO
            if 'PO Line' in df.columns and 'PR Line' in df.columns:
                po_list = procurement_wp_renamed.get('PO Line', pd.Series([])).tolist()
                pr_list = procurement_wp_renamed.get('PR Line', pd.Series([])).tolist()
                
                mask_not_in_wp = (
                    (~df['PO Line'].isin(po_list)) & 
                    (~df['PR Line'].isin(pr_list))
                )
                df.loc[mask_not_in_wp, 'PO狀態'] = STATUS_VALUES['NOT_IN_PROCUREMENT']
            
            # 獲取採購備註
            if 'Remarked by Procurement' in procurement.columns and 'PO Line' in df.columns:
                procurement_mapping = dict(zip(procurement['PO Line'], procurement['Remarked by Procurement']))
                df['Remarked by Procurement'] = df['PO Line'].map(procurement_mapping)
            if 'remarked_by_procurement' in procurement.columns and 'PO Line' in df.columns:
                procurement_mapping = dict(zip(procurement['po_line'], procurement['remarked_by_procurement']))
                df['Remarked by Procurement'] = df['PO Line'].map(procurement_mapping)
            
            # 移除 SPT 模組給的狀態（SPX 有自己的狀態邏輯）
            if 'PO狀態' in df.columns:
                df.loc[df['PO狀態'] == 'Not In Procurement WP', 'PO狀態'] = pd.NA
            
            return df
    
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    def _process_procurement_pr(self, df: pd.DataFrame, procurement_pr: pd.DataFrame) -> pd.DataFrame:
        """處理採購 PR 底稿"""
        # 重命名 PR 採購底稿中的列
        procurement_pr = procurement_pr.rename(
            columns={
                'Remarked by Procurement': 'Remark by PR Team',
                'Noted by Procurement': 'Noted by PR',
                # 標準化後欄位名稱，for new procurement
                'remarked_by_procurement': 'Remark by PR Team',
                'noted_by_procurement': 'Noted by PR',
                'pr_line': 'PR Line'
            }
        )
        
        # 獲取 PR 採購底稿中的備註
        if 'Remark by PR Team' in procurement_pr.columns and 'PR Line' in df.columns:
            pr_procurement_mapping = dict(zip(procurement_pr['PR Line'], procurement_pr['Remark by PR Team']))
            df['Remarked by Procurement PR'] = df['PR Line'].map(pr_procurement_mapping)
        
        if 'Noted by PR' in procurement_pr.columns and 'PR Line' in df.columns:
            pr_noted_mapping = dict(zip(procurement_pr['PR Line'], procurement_pr['Noted by PR']))
            df['Noted by Procurement PR'] = df['PR Line'].map(pr_noted_mapping)
        
        return df
    
    def _copy_pr_result_non_pr_cols(self, df,
                                    col_remark_procurement: str = 'Remarked by Procurement',
                                    col_note_procurement: str = 'Noted by Procurement'):
        df_copy = df.copy()
        if col_remark_procurement in df_copy.columns:
            df_copy[col_remark_procurement] = df_copy[col_remark_procurement + ' PR']

        if col_note_procurement in df_copy.columns:
            df_copy[col_note_procurement] = df_copy[col_note_procurement + ' PR']

        return df_copy
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for procurement integration")
            return False
        
        return True


class DateLogicStep(PipelineStep):
    """
    日期邏輯處理步驟
    
    功能:
    1. 提取和處理 Item Description 中的日期範圍
    2. 轉換 Expected Received Month 格式
    
    輸入: DataFrame
    輸出: DataFrame with processed date columns
    """
    
    def __init__(self, name: str = "DateLogic", **kwargs):
        super().__init__(name, description="Process date logic", **kwargs)
        self.regex_patterns = config_manager.get_regex_patterns()
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行日期邏輯處理"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            self.logger.info("Processing date logic...")
            
            # 調用父類的日期邏輯方法
            # 這裡需要實現類似 BasePOProcessor.apply_date_logic 的邏輯
            
            # 處理分潤合作
            if 'Item Description' in df.columns:
                mask_profit_sharing = safe_string_operation(
                    df['Item Description'], 'contains', '分潤合作', na=False
                )

                def get_status_column() -> str:
                    if context.get_variable('file_paths').get('raw_po'):
                        return 'PO狀態'
                    else:
                        return 'PR狀態'
                
                mask_no_status = (
                    df[get_status_column()].isna() | (df[get_status_column()] == 'nan')
                )
                
                df.loc[mask_profit_sharing & mask_no_status, get_status_column()] = '分潤'
                
            # 處理已入帳
            if 'PO Entry full invoiced status' in df.columns and context.metadata.entity_type != 'SPX':
                mask_posted = (
                    (df['PO狀態'].isna() | (df['PO狀態'] == 'nan')) & 
                    (df['PO Entry full invoiced status'].astype('string') == '1')
                )
                df.loc[mask_posted, 'PO狀態'] = STATUS_VALUES['POSTED']
                df.loc[df['PO狀態'] == STATUS_VALUES['POSTED'], '是否估計入帳'] = "N"
            
            # 解析日期
            df = self.parse_date_from_description(df)
                
            context.update_data(df)
            duration = time.time() - start_time
            
            self.logger.info("Date logic processing completed")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                duration=duration,
                message="Date logic processed successfully"
            )
            
        except Exception as e:
            self.logger.error(f"Date logic processing failed: {str(e)}", exc_info=True)
            context.add_error(f"Date logic processing failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for date logic")
            return False
        
        required_columns = ['Item Description']
        missing = [col for col in required_columns if col not in context.data.columns]
        if missing:
            self.logger.error(f"Missing required columns: {missing}")
            return False
        
        return True
    
    def parse_date_from_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        從描述欄位解析日期範圍
        
        Args:
            df: 包含Item Description的DataFrame
            
        Returns:
            pd.DataFrame: 添加了解析結果的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 將Expected Receive Month轉換為數值格式以便比較
            if 'Expected Receive Month' in df_copy.columns:
                df_copy['Expected Received Month_轉換格式'] = pd.to_datetime(
                    df_copy['Expected Receive Month'], 
                    format='%b-%y',
                    errors='coerce'
                ).dt.strftime('%Y%m').fillna('0').astype('Int32')
            
            # 解析Item Description中的日期範圍
            if 'Item Description' in df_copy.columns:
                df_copy['YMs of Item Description'] = df_copy['Item Description'].apply(
                    lambda x: extract_date_range_from_description(x, logger=self.logger)
                )
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"解析描述中的日期時出錯: {str(e)}", exc_info=True)
            raise ValueError("解析日期時出錯")
# =============================================================================
# 輔助工具類別
# =============================================================================

class StepMetadataBuilder:
    """
    StepResult metadata 構建器
    提供標準化的 metadata 結構和鏈式 API
    """
    
    def __init__(self):
        self.metadata = {
            # 基本統計
            'input_rows': 0,
            'output_rows': 0,
            'rows_changed': 0,
            
            # 處理統計
            'records_processed': 0,
            'records_skipped': 0,
            'records_failed': 0,
            
            # 時間資訊
            'start_time': None,
            'end_time': None,
        }
    
    def set_row_counts(self, input_rows: int, output_rows: int) -> 'StepMetadataBuilder':
        """設置行數統計"""
        self.metadata['input_rows'] = int(input_rows)
        self.metadata['output_rows'] = int(output_rows)
        self.metadata['rows_changed'] = int(output_rows - input_rows)
        return self
    
    def set_process_counts(self, processed: int = 0, skipped: int = 0, 
                           failed: int = 0) -> 'StepMetadataBuilder':
        """設置處理計數"""
        self.metadata['records_processed'] = int(processed)
        self.metadata['records_skipped'] = int(skipped)
        self.metadata['records_failed'] = int(failed)
        return self
    
    def set_time_info(self, start_time: datetime, end_time: datetime) -> 'StepMetadataBuilder':
        """設置時間資訊"""
        self.metadata['start_time'] = start_time.isoformat()
        self.metadata['end_time'] = end_time.isoformat()
        return self
    
    def add_custom(self, key: str, value: Any) -> 'StepMetadataBuilder':
        """添加自定義 metadata"""
        self.metadata[key] = value
        return self
    
    def build(self) -> Dict[str, Any]:
        """構建並返回 metadata 字典"""
        return self.metadata.copy()


def create_error_metadata(error: Exception, context: ProcessingContext, 
                          step_name: str, **kwargs) -> Dict[str, Any]:
    """
    創建增強的錯誤 metadata
    
    Args:
        error: 發生的異常
        context: 處理上下文
        step_name: 步驟名稱
        **kwargs: 額外的上下文資訊
    
    Returns:
        Dict[str, Any]: 錯誤 metadata 字典
    """
    error_metadata = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'error_traceback': traceback.format_exc(),
        'step_name': step_name,
    }
    
    # 添加數據快照
    if context.data is not None:
        error_metadata['data_snapshot'] = {
            'total_rows': len(context.data),
            'total_columns': len(context.data.columns),
            'columns': list(context.data.columns)[:20],  # 只列前20個欄位
        }
    else:
        error_metadata['data_snapshot'] = {'status': 'no_data'}
    
    # 添加上下文變量
    error_metadata['context_variables'] = {
        k: str(v)[:100] for k, v in context.variables.items()  # 限制長度
    }
    
    # 添加額外資訊
    error_metadata.update(kwargs)
    
    return error_metadata

