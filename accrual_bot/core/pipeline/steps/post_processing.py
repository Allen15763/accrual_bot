"""
通用後處理步驟框架

提供可擴展的後處理基類，用於在 Pipeline 的最後階段執行各種後處理任務。
使用模板方法模式，子類可以覆寫特定方法來實現自定義邏輯。

典型使用場景:
- 數據品質檢查
- 統計信息生成
- 特殊欄位計算
- 業務規則驗證
- 數據標準化
- 報表生成準備

"""

import time
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Any, Tuple, Callable
from datetime import datetime
from abc import abstractmethod

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder,
    create_error_metadata
)


class BasePostProcessingStep(PipelineStep):
    """
    通用後處理步驟基類
    
    使用模板方法模式，提供標準的後處理流程：
    1. 前置處理 (_pre_process)
    2. 主要處理 (_process_data)
    3. 後置處理 (_post_process)
    4. 結果驗證 (_validate_result)
    
    子類可以選擇性覆寫這些方法來實現自定義邏輯。
    
    特性:
    - 自動數據統計
    - 完整的錯誤處理
    - 詳細的執行日誌
    - 豐富的 metadata
    - 可配置的驗證規則
    
    使用範例:
    ```python
    class MyPostProcessing(BasePostProcessingStep):
        def _process_data(self, df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
            # 實現您的處理邏輯
            df['new_column'] = df['old_column'] * 2
            return df
    ```
    """
    
    def __init__(
        self,
        name: str = "PostProcessing",
        description: str = "Generic post-processing step",
        enable_statistics: bool = True,
        enable_validation: bool = True,
        required: bool = True,
        **kwargs
    ):
        """
        初始化後處理步驟
        
        Args:
            name: 步驟名稱
            description: 步驟描述
            enable_statistics: 是否啟用統計信息收集
            enable_validation: 是否啟用結果驗證
            required: 是否為必需步驟
            **kwargs: 傳遞給父類的其他參數
        """
        super().__init__(name, description, required, **kwargs)
        self.enable_statistics = enable_statistics
        self.enable_validation = enable_validation
        
        # 統計信息存儲
        self._statistics: Dict[str, Any] = {}
        self._processing_notes: List[str] = []
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行後處理步驟
        
        執行流程:
        1. 記錄起始狀態
        2. 前置處理
        3. 主要處理
        4. 後置處理
        5. 結果驗證
        6. 生成統計和 metadata
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 執行結果
        """
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            df = context.data.copy()
            input_count = len(df)
            input_columns = len(df.columns)
            
            self.logger.info(f"開始後處理: {input_count} 行, {input_columns} 欄")
            
            # === 階段 1: 前置處理 ===
            self.logger.debug("執行前置處理...")
            df = self._pre_process(df, context)
            self._add_note("前置處理完成")
            
            # === 階段 2: 主要處理 ===
            self.logger.debug("執行主要處理...")
            df = self._process_data(df, context)
            self._add_note("主要處理完成")
            
            # === 階段 3: 後置處理 ===
            self.logger.debug("執行後置處理...")
            df = self._post_process(df, context)
            self._add_note("後置處理完成")
            
            # === 階段 4: 結果驗證 ===
            if self.enable_validation:
                self.logger.debug("執行結果驗證...")
                validation_result = self._validate_result(df, context)
                if not validation_result['is_valid']:
                    warning_msg = f"驗證發現問題: {validation_result['message']}"
                    self.logger.warning(warning_msg)
                    context.add_warning(warning_msg)
                else:
                    self.logger.info(validation_result)
            
            # === 階段 5: 收集統計信息 ===
            if self.enable_statistics:
                self._collect_statistics(df, input_count, input_columns)
            
            # 更新上下文
            context.update_data(df)
            
            # 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            output_count = len(df)
            output_columns = len(df.columns)
            
            self.logger.info(
                f"後處理完成: {output_count} 行 ({output_count - input_count:+d}), "
                f"{output_columns} 欄 ({output_columns - input_columns:+d}), "
                f"耗時 {duration:.2f}秒"
            )
            
            # 生成 metadata
            metadata = self._build_metadata(
                input_count, output_count,
                input_columns, output_columns,
                start_datetime, end_datetime
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"後處理完成: 處理 {output_count} 行數據",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"後處理失敗: {str(e)}", exc_info=True)
            context.add_error(f"後處理失敗: {str(e)}")
            
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='post_processing'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"後處理失敗: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    # =========================================================================
    # 可覆寫的模板方法 (子類可以實現自定義邏輯)
    # =========================================================================
    
    def _pre_process(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """
        前置處理 (可選覆寫)
        
        在主要處理之前執行，通常用於:
        - 數據驗證
        - 欄位準備
        - 臨時變量初始化
        
        Args:
            df: 輸入數據
            context: 處理上下文
            
        Returns:
            pd.DataFrame: 處理後的數據
        """
        self.logger.debug("使用默認前置處理 (空操作)")
        return df
    
    @abstractmethod
    def _process_data(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """
        主要處理邏輯 (必須覆寫)
        
        這是後處理的核心方法，子類必須實現此方法。
        
        Args:
            df: 輸入數據
            context: 處理上下文
            
        Returns:
            pd.DataFrame: 處理後的數據
            
        Raises:
            NotImplementedError: 如果子類未實現此方法
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} 必須實現 _process_data() 方法"
        )
    
    def _post_process(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """
        後置處理 (可選覆寫)
        
        在主要處理之後執行，通常用於:
        - 數據清理
        - 格式標準化
        - 最終驗證
        
        Args:
            df: 輸入數據
            context: 處理上下文
            
        Returns:
            pd.DataFrame: 處理後的數據
        """
        self.logger.debug("使用默認後置處理 (空操作)")
        return df
    
    def _validate_result(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> Dict[str, Any]:
        """
        結果驗證 (可選覆寫)
        
        驗證處理後的數據是否符合預期。
        
        Args:
            df: 處理後的數據
            context: 處理上下文
            
        Returns:
            Dict: 驗證結果，包含:
                - is_valid (bool): 是否通過驗證
                - message (str): 驗證消息
                - details (Dict): 詳細信息
        """
        # 默認驗證: 檢查數據是否為空
        if df.empty:
            return {
                'is_valid': False,
                'message': '處理後數據為空',
                'details': {'row_count': 0}
            }
        
        return {
            'is_valid': True,
            'message': '驗證通過',
            'details': {'row_count': len(df)}
        }
    
    # =========================================================================
    # 輔助方法
    # =========================================================================
    
    def _collect_statistics(
        self,
        df: pd.DataFrame,
        input_count: int,
        input_columns: int
    ) -> None:
        """
        收集統計信息
        
        Args:
            df: 處理後的數據
            input_count: 輸入行數
            input_columns: 輸入欄位數
        """
        self._statistics = {
            'input_rows': input_count,
            'output_rows': len(df),
            'rows_changed': len(df) - input_count,
            'input_columns': input_columns,
            'output_columns': len(df.columns),
            'columns_changed': len(df.columns) - input_columns,
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'null_counts': df.isnull().sum().to_dict(),
            'dtypes': df.dtypes.astype(str).to_dict()
        }
    
    def _add_note(self, note: str) -> None:
        """添加處理備註"""
        self._processing_notes.append(note)
        self.logger.debug(note)
    
    def _build_metadata(
        self,
        input_count: int,
        output_count: int,
        input_columns: int,
        output_columns: int,
        start_datetime: datetime,
        end_datetime: datetime
    ) -> Dict[str, Any]:
        """
        構建 metadata
        
        Args:
            input_count: 輸入行數
            output_count: 輸出行數
            input_columns: 輸入欄位數
            output_columns: 輸出欄位數
            start_datetime: 開始時間
            end_datetime: 結束時間
            
        Returns:
            Dict: metadata 字典
        """
        metadata = (StepMetadataBuilder()
                    .set_row_counts(input_count, output_count)
                    .set_process_counts(processed=output_count)
                    .set_time_info(start_datetime, end_datetime)
                    .add_custom('input_columns', input_columns)
                    .add_custom('output_columns', output_columns)
                    .add_custom('columns_changed', output_columns - input_columns)
                    .add_custom('processing_notes', self._processing_notes)
                    .build())
        
        # 添加統計信息
        if self.enable_statistics and self._statistics:
            metadata['statistics'] = self._statistics
        
        return metadata
    
    # =========================================================================
    # 通用輔助工具方法
    # =========================================================================
    
    @staticmethod
    def safe_divide(
        numerator: pd.Series,
        denominator: pd.Series,
        default: Any = 0
    ) -> pd.Series:
        """
        安全除法，避免除以零
        
        Args:
            numerator: 分子
            denominator: 分母
            default: 除以零時的默認值
            
        Returns:
            pd.Series: 計算結果
        """
        return numerator.divide(denominator.replace(0, np.nan)).fillna(default)
    
    @staticmethod
    def convert_to_numeric(
        series: pd.Series,
        errors: str = 'coerce',
        default: Any = 0
    ) -> pd.Series:
        """
        安全轉換為數值類型
        
        Args:
            series: 輸入序列
            errors: 錯誤處理方式 ('raise', 'coerce', 'ignore')
            default: 轉換失敗時的默認值
            
        Returns:
            pd.Series: 轉換後的序列
        """
        result = pd.to_numeric(series, errors=errors)
        if errors == 'coerce':
            result = result.fillna(default)
        return result
    
    @staticmethod
    def clean_string_column(
        series: pd.Series,
        strip: bool = True,
        lower: bool = False,
        replace_dict: Optional[Dict[str, str]] = None
    ) -> pd.Series:
        """
        清理字符串欄位
        
        Args:
            series: 輸入序列
            strip: 是否去除首尾空白
            lower: 是否轉換為小寫
            replace_dict: 替換字典
            
        Returns:
            pd.Series: 清理後的序列
        """
        result = series.astype(str)
        
        if strip:
            result = result.str.strip()
        
        if lower:
            result = result.str.lower()
        
        if replace_dict:
            for old, new in replace_dict.items():
                result = result.str.replace(old, new, regex=False)
        
        return result
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 驗證是否通過
        """
        if context.data is None or context.data.empty:
            self.logger.error("無數據可供後處理")
            return False
        
        return True


# =============================================================================
# 專用後處理步驟範例
# =============================================================================

class DataQualityCheckStep(BasePostProcessingStep):
    """
    數據品質檢查步驟
    
    執行各種數據品質檢查:
    - 空值檢查
    - 重複值檢查
    - 數據類型檢查
    - 數值範圍檢查
    """
    
    def __init__(
        self,
        name: str = "DataQualityCheck",
        required_columns: Optional[List[str]] = None,
        max_null_ratio: float = 0.5,
        check_duplicates: bool = True,
        **kwargs
    ):
        """
        初始化數據品質檢查步驟
        
        Args:
            name: 步驟名稱
            required_columns: 必需欄位列表
            max_null_ratio: 允許的最大空值比例
            check_duplicates: 是否檢查重複值
            **kwargs: 其他參數
        """
        super().__init__(name, "Data quality check step", **kwargs)
        self.required_columns = required_columns or []
        self.max_null_ratio = max_null_ratio
        self.check_duplicates = check_duplicates
        
        self._quality_issues: List[Dict[str, Any]] = []
    
    def _process_data(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """
        執行數據品質檢查
        
        Args:
            df: 輸入數據
            context: 處理上下文
            
        Returns:
            pd.DataFrame: 原始數據 (此步驟不修改數據)
        """
        self.logger.info("執行數據品質檢查...")
        
        # 檢查 1: 必需欄位
        self._check_required_columns(df, context)
        
        # 檢查 2: 空值比例
        self._check_null_ratios(df, context)
        
        # 檢查 3: 重複值
        if self.check_duplicates:
            self._check_duplicates(df, context)
        
        # 生成品質報告
        self._generate_quality_report(context)
        
        return df
    
    def _check_required_columns(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> None:
        """檢查必需欄位"""
        missing_cols = set(self.required_columns) - set(df.columns)
        if missing_cols:
            issue = {
                'type': 'missing_columns',
                'severity': 'high',
                'message': f"缺少必需欄位: {missing_cols}"
            }
            self._quality_issues.append(issue)
            context.add_error(issue['message'])
    
    def _check_null_ratios(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> None:
        """檢查空值比例"""
        null_ratios = df.isnull().sum() / len(df)
        high_null_cols = null_ratios[null_ratios > self.max_null_ratio]
        
        if not high_null_cols.empty:
            issue = {
                'type': 'high_null_ratio',
                'severity': 'medium',
                'message': f"以下欄位空值比例超過 {self.max_null_ratio:.1%}",
                'details': high_null_cols.to_dict()
            }
            self._quality_issues.append(issue)
            context.add_warning(issue['message'])
    
    def _check_duplicates(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> None:
        """檢查重複值"""
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            issue = {
                'type': 'duplicates',
                'severity': 'low',
                'message': f"發現 {duplicate_count} 行重複數據",
                'details': {'count': int(duplicate_count)}
            }
            self._quality_issues.append(issue)
            context.add_warning(issue['message'])
    
    def _generate_quality_report(self, context: ProcessingContext) -> None:
        """生成品質報告"""
        if self._quality_issues:
            report = pd.DataFrame(self._quality_issues)
            context.add_auxiliary_data('quality_check_report', report)
            self.logger.info(f"發現 {len(self._quality_issues)} 個品質問題")
        else:
            self.logger.info("數據品質檢查通過，未發現問題")


class StatisticsGenerationStep(BasePostProcessingStep):
    """
    統計信息生成步驟
    
    生成各種統計信息和摘要報表
    """
    
    def __init__(
        self,
        name: str = "StatisticsGeneration",
        group_by_columns: Optional[List[str]] = None,
        agg_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        初始化統計生成步驟
        
        Args:
            name: 步驟名稱
            group_by_columns: 分組欄位
            agg_columns: 聚合欄位
            **kwargs: 其他參數
        """
        super().__init__(name, "Generate statistics and summaries", **kwargs)
        self.group_by_columns = group_by_columns or []
        self.agg_columns = agg_columns or []
    
    def _process_data(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """
        生成統計信息
        
        Args:
            df: 輸入數據
            context: 處理上下文
            
        Returns:
            pd.DataFrame: 原始數據 (此步驟不修改數據)
        """
        self.logger.info("生成統計信息...")
        
        # 生成基本統計
        basic_stats = self._generate_basic_stats(df)
        context.add_auxiliary_data('basic_statistics', basic_stats)
        
        # 生成分組統計
        if self.group_by_columns:
            group_stats = self._generate_group_stats(df)
            context.add_auxiliary_data('group_statistics', group_stats)
        
        return df
    
    def _generate_basic_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成基本統計"""
        stats = df.describe(include='all').T
        stats['null_count'] = df.isnull().sum()
        stats['null_ratio'] = df.isnull().sum() / len(df)
        return stats
    
    def _generate_group_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成分組統計"""
        if not self.agg_columns:
            # 如果沒有指定聚合欄位，使用計數
            return df.groupby(self.group_by_columns).size().reset_index(name='count')
        
        # 對數值欄位進行聚合
        numeric_cols = df[self.agg_columns].select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            return df.groupby(self.group_by_columns)[numeric_cols].agg(['sum', 'mean', 'count'])
        
        return pd.DataFrame()


# =============================================================================
# 工廠函數
# =============================================================================

def create_post_processing_chain(
    *steps: BasePostProcessingStep
) -> List[BasePostProcessingStep]:
    """
    創建後處理步驟鏈
    
    將多個後處理步驟組合成一個處理鏈。
    
    Args:
        *steps: 後處理步驟
        
    Returns:
        List[BasePostProcessingStep]: 步驟列表
    """
    return list(steps)
