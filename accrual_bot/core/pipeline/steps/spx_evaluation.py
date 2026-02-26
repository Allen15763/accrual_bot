"""
邏輯判斷、數據計算與更新
"""
import time
import re
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Any, Union
from datetime import datetime
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder


class StatusStage1Step(PipelineStep):
    """
    第一階段狀態判斷步驟（混合模式：配置驅動 + 程式碼保留）

    功能:
    根據關單清單及配置規則給予初始狀態

    配置驅動（從 stagging.toml [spx_status_stage1_rules] 讀取）：
    - 押金/保證金識別、BAO供應商GL調整、上月FN備註關單
    - 公共費用供應商、租金狀態、Intermediary狀態、資產待驗收

    程式碼保留（數據驅動，不適合配置化）：
    - 關單清單比對（待關單/已關單）
    - FA備註提取（xxxxxx入FA）
    - 日期格式轉換

    輸入: DataFrame + Closing list
    輸出: DataFrame with initial status
    """

    def __init__(self, name: str = "StatusStage1", **kwargs):
        super().__init__(name, description="Evaluate status stage 1", **kwargs)

        # 初始化配置驅動引擎
        from accrual_bot.core.pipeline.steps.spx_condition_engine import SPXConditionEngine
        self.engine = SPXConditionEngine('spx_status_stage1_rules')
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行第一階段狀態判斷"""
        start_time = time.time()
        
        try:
            df = context.data.copy()
            df_spx_closing = context.get_auxiliary_data('closing_list')
            processing_date = context.metadata.processing_date
            
            self.logger.info("🔄 開始執行第一階段狀態判斷...")
            
            # === 階段 1: 驗證數據 ===
            if df_spx_closing is None or df_spx_closing.empty:
                self.logger.warning("⚠️  關單清單為空，跳過狀態判斷")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No closing list data"
                )
            
            self.logger.info(f"📅 處理日期: {processing_date}")
            self.logger.info(f"📊 輸入記錄數: {len(df):,}")
            self.logger.info(f"📋 關單清單記錄數: {len(df_spx_closing):,}")
            
            # === 階段 2: 給予狀態標籤 ===
            self.logger.info("🏷️  開始分配狀態標籤...")
            df = self._give_status_stage_1(df, 
                                           df_spx_closing, 
                                           processing_date,
                                           entity_type=context.metadata.entity_type)
            
            # === 階段 3: 生成摘要 ===
            tag_column = 'PO狀態' if 'PO狀態' in df.columns else 'PR狀態'
            summary = self._generate_label_summary(df, tag_column)
            
            # === 階段 4: 記錄摘要到 Logger ===
            self._log_label_summary(summary, tag_column)
            
            # === 階段 5: 更新上下文 ===
            context.update_data(df)
            
            duration = time.time() - start_time
            
            self.logger.info(f"✅ 第一階段狀態判斷完成 (耗時: {duration:.2f}秒)")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"狀態標籤分配完成: {summary['labeled_count']} 筆已標籤",
                duration=duration,
                metadata=summary  # 將完整摘要放入 metadata
            )
            
        except Exception as e:
            self.logger.error(f"❌ 第一階段狀態判斷失敗: {str(e)}", exc_info=True)
            context.add_error(f"Status stage 1 evaluation failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _generate_label_summary(self, df: pd.DataFrame, 
                                tag_column: str) -> Dict[str, Any]:
        """
        生成標籤分配的詳細摘要
        
        統計內容：
        1. 各標籤數量與百分比
        2. 分類統計（已完成、未完成、錯誤等）
        3. 需要關注的異常標籤
        
        Args:
            df: 處理後的 DataFrame
            tag_column: 標籤欄位名稱 ('PO狀態' 或 'PR狀態')
            
        Returns:
            Dict: 包含完整統計信息的字典
        """
        total_count = len(df)
        
        # 標籤分布統計
        label_counts = df[tag_column].value_counts().to_dict()
        label_percentages = (df[tag_column].value_counts(normalize=True) * 100).to_dict()
        
        # 分類統計
        completed_labels = ['已完成_租金', '已完成_intermediary', '已入帳']
        incomplete_labels = ['未完成_租金', '未完成_intermediary']
        pending_labels = ['待關單', 'Pending_validating']
        closed_labels = ['已關單', '參照上月關單']
        error_labels = [k for k in label_counts.keys() if 'error' in str(k).lower()]
        
        # 構建摘要
        summary = {
            'total_records': total_count,
            'labeled_count': df[tag_column].notna().sum(),
            'unlabeled_count': df[tag_column].isna().sum(),
            
            # 標籤分布
            'label_distribution': label_counts,
            'label_percentages': {k: round(v, 2) for k, v in label_percentages.items()},
            
            # 分類統計
            'category_stats': {
                'completed': sum(label_counts.get(label, 0) for label in completed_labels),
                'incomplete': sum(label_counts.get(label, 0) for label in incomplete_labels),
                'pending': sum(label_counts.get(label, 0) for label in pending_labels),
                'closed': sum(label_counts.get(label, 0) for label in closed_labels),
                'errors': sum(label_counts.get(label, 0) for label in error_labels),
            },
            
            # Top 5 標籤
            'top_5_labels': dict(sorted(label_counts.items(), 
                                        key=lambda x: x[1], 
                                        reverse=True)[:5]),
        }
        
        return summary
    
    def _log_label_summary(self, summary: Dict[str, Any], tag_column: str):
        """
        以結構化方式記錄標籤摘要到 logger
        
        輸出格式清晰易讀，便於監控和調試
        
        Args:
            summary: 摘要統計數據
            tag_column: 標籤欄位名稱
        """
        self.logger.info("=" * 60)
        self.logger.info(f"📊 {tag_column} 標籤分配摘要")
        self.logger.info("=" * 60)
        
        # 總覽統計
        self.logger.info(f"📈 總記錄數: {summary['total_records']:,}")
        self.logger.info(f"   ├─ 已標籤: {summary['labeled_count']:,} "
                         f"({summary['labeled_count']/summary['total_records']*100:.1f}%)")
        self.logger.info(f"   └─ 未標籤: {summary['unlabeled_count']:,}")
        
        # 分類統計
        self.logger.info("\n📂 分類統計:")
        category_stats = summary['category_stats']
        for category, count in category_stats.items():
            if count > 0:
                self.logger.info(f"   • {category:12s}: {count:5,} "
                                 f"({count/summary['total_records']*100:5.1f}%)")
        
        # Top 5 標籤
        self.logger.info("\n🏆 Top 5 標籤:")
        for i, (label, count) in enumerate(summary['top_5_labels'].items(), 1):
            percentage = summary['label_percentages'].get(label, 0)
            self.logger.info(f"   {i}. {label:30s}: {count:5,} ({percentage:5.1f}%)")
        
        # 異常警告
        if category_stats['errors'] > 0:
            self.logger.warning(f"\n⚠️  發現 {category_stats['errors']} 筆錯誤記錄")
        
        self.logger.info("=" * 60)
    
    def _log_label_condition(self, condition_name: str, 
                             count: int, 
                             label: str):
        """
        記錄單一標籤條件的結果
        
        參考 SPXERMLogicStep._log_condition_result 的風格
        
        Args:
            condition_name: 條件名稱
            count: 符合條件的記錄數
            label: 賦予的標籤
        """
        if count > 0:
            self.logger.debug(f"✓ [{condition_name:30s}] → '{label:20s}': {count:5,} 筆")
    
    def _give_status_stage_1(self,
                             df: pd.DataFrame,
                             df_spx_closing: pd.DataFrame,
                             date,
                             **kwargs) -> pd.DataFrame:
        """給予第一階段狀態 - 混合模式（配置驅動 + 程式碼保留）

        執行順序：
        1. [代碼] 日期格式轉換
        2. [代碼] 關單清單比對（待關單/已關單）
        3. [代碼] FA備註提取（xxxxxx入FA）
        4. [配置] 引擎驅動的可配置條件（押金、GL調整、租金、資產等）

        Args:
            df: PO/PR DataFrame
            df_spx_closing: SPX關單數據DataFrame
            date: 處理日期 (YYYYMM)

        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        entity_type = kwargs.get('entity_type', 'SPX')
        is_po = 'PO狀態' in df.columns
        tag_column = 'PO狀態' if is_po else 'PR狀態'
        processing_type = 'PO' if is_po else 'PR'

        # === 1：日期格式轉換 ===
        df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(
            df['Remarked by 上月 FN']
        )
        if 'Remarked by 上月 FN PR' in df.columns:
            df['Remarked by 上月 FN PR'] = self.convert_date_format_in_remark(
                df['Remarked by 上月 FN PR']
            )

        # === 2：關單清單比對（數據驅動）===
        c1, c2 = self.is_closed_spx(df_spx_closing)
        if is_po:
            id_col = 'PO#'
            closing_col = 'po_no'
        else:
            id_col = 'PR#'
            closing_col = 'new_pr_no'

        # 先取得關單清單的po_no
        to_be_close = (df_spx_closing.loc[c1, closing_col].unique()
                       if c1.any() else [])
        closed = (df_spx_closing.loc[c2, closing_col].unique()
                  if c2.any() else [])
        
        # 把要關單的資料分為整張關跟部分Item關
        to_be_close_all, to_be_close_partial = self._closing_by_line(df_spx_closing, to_be_close)
        closed_all, closed_partial = self._closing_by_line(df_spx_closing, closed)
        # 加上前綴
        to_be_close_all, to_be_close_partial = self._add_prefix(to_be_close_all), self._add_prefix(to_be_close_partial)
        closed_all, closed_partial = self._add_prefix(closed_all), self._add_prefix(closed_partial)

        # 整張關
        line_col = id_col.replace('#', ' Line')
        df = self._apply_closing_status(
            df, id_col, tag_column,
            to_be_close_all, '待關單', f'{id_col}在待關單清單'
        )
        df = self._apply_closing_status(
            df, id_col, tag_column,
            closed_all, '已關單', f'{id_col}在已關單清單'
        )
        # 部分 Item 關
        df = self._apply_closing_status(
            df, line_col, tag_column,
            to_be_close_partial, '待關單', f'{line_col}在待關單清單'
        )
        df = self._apply_closing_status(
            df, line_col, tag_column,
            closed_partial, '已關單', f'{line_col}在已關單清單'
        )

        # === 3：FA備註提取（需 regex extract）===
        # PO: Remarked by 上月 FN + Remarked by 上月 FN PR
        # PR: Remarked by 上月 FN
        fn_col = 'Remarked by 上月 FN'
        has_fa = df[fn_col].astype('string').str.contains('入FA', na=False)
        not_partial = ~df[fn_col].astype('string').str.contains('部分完成', na=False)
        cond_fa_fn = has_fa & not_partial
        if cond_fa_fn.any():
            extracted = self.extract_fa_remark(df.loc[cond_fa_fn, fn_col])
            df.loc[cond_fa_fn, tag_column] = extracted
            self._log_label_condition(
                f'{processing_type}備註入FA(FN)', cond_fa_fn.sum(), 'xxxxxx入FA'
            )

        if is_po and 'Remarked by 上月 FN PR' in df.columns:
            fn_pr_col = 'Remarked by 上月 FN PR'
            has_fa_pr = df[fn_pr_col].astype('string').str.contains('入FA', na=False)
            not_partial_pr = ~df[fn_pr_col].astype('string').str.contains('部分完成', na=False)
            cond_fa_pr = has_fa_pr & not_partial_pr
            if cond_fa_pr.any():
                extracted_pr = self.extract_fa_remark(
                    df.loc[cond_fa_pr, fn_pr_col]
                )
                df.loc[cond_fa_pr, tag_column] = extracted_pr
                self._log_label_condition(
                    'PR備註入FA', cond_fa_pr.sum(), 'xxxxxx入FA'
                )

        # === 配置驅動段：引擎處理可配置條件 ===
        # 建立 PO/PR 欄位名稱映射（引擎 config 中使用通用名稱）
        supplier_col = 'PO Supplier' if is_po else 'PR Supplier'
        requester_col = 'PR Requester' if is_po else 'Requester'

        # 建立欄位別名映射，引擎 config 中的 "Supplier" 映射到實際欄位
        if 'Supplier' not in df.columns and supplier_col in df.columns:
            df['Supplier'] = df[supplier_col]
        if 'Requester' not in df.columns and requester_col in df.columns:
            df['Requester'] = df[requester_col]

        engine_context = {
            'processing_date': date,
            'entity_type': entity_type,
            'prebuilt_masks': {},  # 引擎會自動計算內建 mask
        }

        self.logger.info(
            f"🔄 引擎驅動: 執行 {processing_type} 配置化條件..."
        )
        df, engine_stats = self.engine.apply_rules(
            df, tag_column, engine_context,
            processing_type=processing_type,
            update_no_status=True
        )

        # 記錄引擎統計
        total_engine_hits = sum(engine_stats.values())
        self.logger.info(
            f"✅ 引擎驅動完成: {len(engine_stats)} 條規則, "
            f"共命中 {total_engine_hits:,} 筆"
        )

        # 清理臨時欄位
        for temp_col in ['Supplier', 'Requester']:
            if temp_col in df.columns and temp_col not in [
                'PO Supplier', 'PR Supplier', 'PR Requester'
            ]:
                # 只清理我們添加的別名
                actual_cols = [supplier_col, requester_col]
                if temp_col not in actual_cols:
                    df.drop(columns=[temp_col], inplace=True, errors='ignore')

        self.logger.info("成功給予第一階段狀態")
        return df
    
    def is_closed_spx(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """判斷SPX關單狀態
        
        Args:
            df: 關單數據DataFrame
            
        Returns:
            Tuple[pd.Series, pd.Series]: (待關單條件, 已關單條件)
        """
        # [0]有新的PR編號，但FN未上系統關單的
        condition_to_be_closed = (
            (~df['new_pr_no'].isna()) & 
            (df['new_pr_no'] != '') & 
            (df['done_by_fn'].isna())
        )
        
        # [1]有新的PR編號，但FN已經上系統關單的
        condition_closed = (
            (~df['new_pr_no'].isna()) & 
            (df['new_pr_no'] != '') & 
            (~df['done_by_fn'].isna())
        )
        
        return condition_to_be_closed, condition_closed
    
    def _closing_by_line(self, df: pd.DataFrame, po_no: List) -> List:

        remove_all = []
        remove_partial = []

        filtered_df = df[df['po_no'].isin(po_no)].copy()

        for index, row in filtered_df.iterrows():
            po = row['po_no']
            line = str(row['line_no']).strip()  # 確保轉成字串並去除前後空白
            
            # 情況 1：如果是 ALL
            if line == 'ALL':
                remove_all.append(po)
                
            # 情況 2：如果是 Line 開頭的指定行號
            elif line.startswith('Line'):
                # 步驟 A: 把 "Line" 拔掉，只留後面的數字和符號
                num_str = line.replace('Line', '').strip()
                
                # 步驟 B: 使用正則表達式，支援頓號 (、) 或是半形逗號 (,) 切割
                parts = re.split(r'[、,]', num_str)
                
                # 步驟 C: 針對切割出來的每一段做判斷
                for part in parts:
                    part = part.strip()
                    
                    # 如果這段裡面有波浪號 (代表是範圍，例如 2~12)
                    if '~' in part:
                        start_str, end_str = part.split('~')
                        # 確保裡面真的是數字
                        if start_str.isdigit() and end_str.isdigit():
                            start_num = int(start_str)
                            end_num = int(end_str)
                            for i in range(start_num, end_num + 1):
                                remove_partial.append(f"{po}-{i}")
                    
                    # 如果這段只是純數字 (代表是跳號的單一數字，例如 11)
                    elif part.isdigit():
                        remove_partial.append(f"{po}-{part}")
        return remove_all, remove_partial
    
    def _add_prefix(self, array: List) -> List:
        """新增前綴使其符合HRIS產出的PO#格式"""
        return ['SPTTW-' + i for i in array]

    def _apply_closing_status(self, df: pd.DataFrame,
                              match_col: str,
                              tag_column: str,
                              closing_list: List,
                              status: str,
                              label: str) -> pd.DataFrame:
        """比對關單清單並賦予狀態標籤

        Args:
            df: 主資料 DataFrame
            match_col: 用於比對的欄位名稱（如 'PO#' 或 'PO Line'）
            tag_column: 狀態寫入的目標欄位（'PO狀態' 或 'PR狀態'）
            closing_list: 關單編號清單
            status: 要賦予的狀態值（'待關單' 或 '已關單'）
            label: 日誌標籤描述

        Returns:
            pd.DataFrame: 更新後的 DataFrame
        """
        if not closing_list:
            return df
        mask = df[match_col].astype('string').isin(
            [str(x) for x in closing_list]
        )
        df.loc[mask, tag_column] = status
        self._log_label_condition(label, mask.sum(), status)
        return df

    def convert_date_format_in_remark(self, series: pd.Series) -> pd.Series:
        """轉換備註中的日期格式 (YYYY/MM -> YYYYMM)
        
        Args:
            series: 包含日期的Series
            
        Returns:
            pd.Series: 轉換後的Series
        """
        try:
            return series.astype('string').str.replace(r'(\d{4})/(\d{2})', r'\1\2', regex=True)
        except Exception as e:
            self.logger.error(f"轉換日期格式時出錯: {str(e)}", exc_info=True)
            return series
        
    def extract_fa_remark(self, series: pd.Series) -> pd.Series:
        """提取FA備註中的日期
        
        Args:
            series: 包含FA備註的Series
            
        Returns:
            pd.Series: 提取的日期Series
        """
        try:
            return series.astype('string').str.extract(r'(\d{6}入FA)', expand=False)
        except Exception as e:
            self.logger.error(f"提取FA備註時出錯: {str(e)}", exc_info=True)
            return series
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for status stage 1")
            return False
        
        return True


@dataclass
class ERMConditions:
    """ERM 判斷條件集合 - 提高可讀性"""
    # 基礎條件組件
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


class SPXERMLogicStep(PipelineStep):
    """
    SPX ERM 邏輯步驟 - 配置驅動版本

    功能：
    1. 設置檔案日期
    2. 判斷 11 種 PO/PR 狀態（從 [spx_erm_status_rules] 配置讀取）
    3. 根據狀態設置是否估計入帳
    4. 設置會計相關欄位（Account code, Product code, Dep.等）
    5. 計算預估金額（Accr. Amount）
    6. 處理預付款和負債科目
    7. 檢查 PR Product Code

    業務規則：
    - SPX 邏輯：「已完成」狀態的項目需要估列入帳
    - 其他狀態一律不估列（是否估計入帳 = N）
    - 11 個 ERM 條件由配置引擎依 priority 順序執行

    輸入：
    - DataFrame with required columns
    - Reference data (科目映射、負債科目)
    - Processing date

    輸出：
    - DataFrame with PO/PR狀態, 是否估計入帳, and accounting fields
    """

    def __init__(self, name: str = "SPX_ERM_Logic", **kwargs):
        super().__init__(
            name=name,
            description="Apply SPX ERM logic with 11 status conditions",
            **kwargs
        )

        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list('SPX', 'fa_accounts', ['199999'])
        self.dept_accounts = config_manager.get_list('SPX', 'dept_accounts', [])

        # 初始化配置驅動引擎
        from accrual_bot.core.pipeline.steps.spx_condition_engine import SPXConditionEngine
        self.engine = SPXConditionEngine('spx_erm_status_rules')

        self.logger.info(f"Initialized {name} with FA accounts: {self.fa_accounts}")
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 ERM 邏輯"""
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
            status_column: str = self._get_status_column(df, context)
            conditions = self._build_conditions(df, processing_date, status_column)
            
            # ========== 階段 3: 應用 11 個狀態條件 ==========
            df = self._apply_status_conditions(df, conditions, status_column)
            
            # ========== 階段 4: 處理格式錯誤 ==========
            df = self._handle_format_errors(df, conditions, status_column)
            
            # ========== 階段 5: 設置是否估計入帳 ==========
            df = self._set_accrual_flag(df, status_column)
            
            # ========== 階段 6: 設置會計欄位 ==========
            df = self._set_accounting_fields(df, ref_account, ref_liability)
            
            # ========== 階段 7: 檢查 PR Product Code ==========
            df = self._check_pr_product_code(df)
            
            # 更新上下文
            context.update_data(df)
            
            # 生成統計資訊
            stats = self._generate_statistics(df, status_column)
            
            self.logger.info(
                f"ERM 邏輯完成 - "
                f"需估列: {stats['accrual_count']} 筆, "
                f"總計: {stats['total_count']} 筆"
            )
            duration = time.time() - start_time
            
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
            # 根據 context 創建欄位
            processing_type = context.metadata.processing_type
            return f"{processing_type}狀態"
    
    # ========== 階段 2: 構建條件 ==========
    
    def _build_conditions(self, df: pd.DataFrame, file_date: int,
                          status_column: str) -> ERMConditions:
        """
        構建所有判斷條件
        
        將條件邏輯集中在此處，提高可讀性和維護性
        """
        # 基礎狀態條件
        no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
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
        
        return ERMConditions(
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
                                 cond: ERMConditions,
                                 status_column: str) -> pd.DataFrame:
        """
        應用 ERM 狀態判斷條件（配置驅動）

        將預先計算的 ERMConditions 轉為 prebuilt_masks，
        由 SPXConditionEngine 依配置順序執行。
        """
        # 將 ERMConditions 轉為引擎的 prebuilt_masks
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

        self.logger.info("🔄 引擎驅動: 執行 ERM 配置化條件...")
        df, stats = self.engine.apply_rules(
            df, status_column, engine_context,
            processing_type='PO' if 'PO狀態' == status_column else 'PR',
            update_no_status=True
        )

        # 記錄統計
        total_hits = sum(stats.values())
        self.logger.info(
            f"✅ ERM 引擎驅動完成: {len(stats)} 條規則, "
            f"共命中 {total_hits:,} 筆"
        )

        return df
    
    def _log_condition_result(self, condition_name: str, count: int):
        """記錄條件判斷結果"""
        if count > 0:
            self.logger.debug(f"條件 [{condition_name}]: {count} 筆符合")
    
    # ========== 階段 4: 處理格式錯誤 ==========
    
    def _handle_format_errors(self, df: pd.DataFrame, 
                              cond: ERMConditions,
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
        """
        根據 PO/PR狀態 設置是否估計入帳
        
        SPX 邏輯：只有「已完成」狀態需要估列入帳
        """
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
        
        # 1. Account code
        df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']
        
        # 2. Account Name（通過 merge）
        df = self._set_account_name(df, ref_account, need_accrual)
        
        # 3. Product code
        df.loc[need_accrual, 'Product code'] = df.loc[need_accrual, 'Product Code']
        
        # 4. Region_c（SPX 固定值）
        col_defaults = config_manager._config_toml.get('spx_column_defaults', {})
        df.loc[need_accrual, 'Region_c'] = col_defaults.get('region', 'TW')
        
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
    
    def _set_account_name(self, df: pd.DataFrame, ref_account: pd.DataFrame,
                          mask: pd.Series) -> pd.DataFrame:
        """設置會計科目名稱"""
        if ref_account.empty:
            self.logger.warning("參考科目資料為空")
            return df
        
        # 使用 merge 從參考資料取得科目名稱
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
        
        # 在 dept_accounts 中的科目
        df.loc[mask & isin_dept, 'Dep.'] = \
            df.loc[mask & isin_dept, 'Department'].str[:3]
        
        # 不在 dept_accounts 中的科目
        col_defaults = config_manager._config_toml.get('spx_column_defaults', {})
        df.loc[mask & ~isin_dept, 'Dep.'] = col_defaults.get('default_department', '000')
        
        return df
    
    def _calculate_accrual_amount(self, df: pd.DataFrame, 
                                  mask: pd.Series) -> pd.DataFrame:
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
    
    def _handle_prepayment(self, df: pd.DataFrame, mask: pd.Series,
                           ref_liability: pd.DataFrame) -> pd.DataFrame:
        """
        處理預付款和負債科目
        
        規則：
        - 有預付款：是否有預付 = 'Y'，Liability = '111112'
        - 無預付款：從參考資料查找 Liability
        """
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
        
        # 有預付款的情況，覆蓋為 '111112'
        col_defaults = config_manager._config_toml.get('spx_column_defaults', {})
        df.loc[mask & is_prepayment, 'Liability'] = col_defaults.get(
            'prepay_liability', '111112'
        )
        
        return df
    
    # ========== 階段 7: PR Product Code 檢查 ==========
    
    def _check_pr_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        檢查 PR 的 Product Code 是否與 Project 一致
        
        規則：
        從 Project 欄位提取第一個詞，與 Product code 比對
        - 一致：good
        - 不一致：bad
        """
        if 'Product code' not in df.columns or 'Project' not in df.columns:
            self.logger.warning("缺少 Product code 或 Project 欄位，跳過檢查")
            return df
        
        mask = df['Product code'].notnull()
        
        try:
            # 提取 Project 的第一個詞
            project_first_word = df.loc[mask, 'Project'].str.findall(
                r'^(\w+(?:))'
            ).apply(lambda x: x[0] if len(x) > 0 else '')
            
            # 比對
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
    
    # ========== 驗證方法 ==========
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        df = context.data
        
        if df is None or df.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False
        
        # 檢查必要欄位
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
        """回滾操作（如需要）"""
        self.logger.warning(f"回滾 ERM 邏輯：{str(error)}")
        # SPX ERM 步驟通常不需要特殊回滾操作


class PPEContractDateUpdateStep(PipelineStep):
    """
    PPE 合約日期更新步驟
    
    功能：
    統一同一店號（sp_code）的合約起止日期
    """
    
    def __init__(self, name: str = "PPEContractDateUpdate", **kwargs):
        super().__init__(name, description="Update contract dates", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行日期更新"""
        start_time = datetime.now()
        
        try:
            df = context.data.copy()
            
            # 更新合約日期
            df_updated = self._update_contract_dates(df)
            
            context.update_data(df_updated)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_updated,
                message="合約日期更新完成",
                duration=duration
            )
            
        except Exception as e:
            self.logger.error(f"日期更新失敗: {str(e)}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _update_contract_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """更新合約日期（複製自 SpxPpeProcessor）"""
        df_updated = df.copy()
        
        # 轉換日期格式
        date_columns = [
            'contract_start_day_filing', 
            'contract_end_day_filing',
            'contract_start_day_renewal', 
            'contract_end_day_renewal'
        ]
        
        for col in date_columns:
            if col in df_updated.columns:
                df_updated[col] = pd.to_datetime(df_updated[col], errors='coerce')
        
        # 按 sp_code 分組更新
        for sp_code in df_updated['sp_code'].unique():
            mask = df_updated['sp_code'] == sp_code
            sp_data = df_updated[mask]
            
            # 收集所有日期
            start_dates = []
            end_dates = []
            
            for col in ['contract_start_day_filing', 'contract_start_day_renewal']:
                if col in df_updated.columns:
                    dates = sp_data[col].dropna().tolist()
                    start_dates.extend(dates)
            
            for col in ['contract_end_day_filing', 'contract_end_day_renewal']:
                if col in df_updated.columns:
                    dates = sp_data[col].dropna().tolist()
                    end_dates.extend(dates)
            
            # 更新為最小起始日和最大結束日
            if start_dates:
                min_start = min(start_dates)
                for col in ['contract_start_day_filing', 'contract_start_day_renewal']:
                    if col in df_updated.columns:
                        df_updated.loc[mask, col] = min_start
            
            if end_dates:
                max_end = max(end_dates)
                for col in ['contract_end_day_filing', 'contract_end_day_renewal']:
                    if col in df_updated.columns:
                        df_updated.loc[mask, col] = max_end
        
        return df_updated.drop_duplicates()

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for update")
            return False
        
        return True

class PPEMonthDifferenceStep(PipelineStep):
    """
    PPE 月份差異計算步驟
    
    功能：
    計算合約結束日期與當前月份的差異
    """
    
    def __init__(self, 
                 name: str = "PPEMonthDifference",
                 current_month: int = None,
                 **kwargs):
        super().__init__(name, description="Calculate month difference", **kwargs)
        self.current_month = current_month
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行月份差異計算"""
        start_time = datetime.now()
        
        try:
            df = context.data.copy()
            
            # 獲取當前月份
            current_month = (self.current_month or 
                             context.get_variable('current_month'))
            
            if not current_month:
                raise ValueError("未提供當前月份參數")
            
            # 選擇必要欄位
            selected_cols = [
                'sp_code', 
                'address', 
                'contract_start_day_filing', 
                'contract_end_day_renewal'
            ]
            
            # 計算月份差異
            df_result = self._calculate_month_difference(
                df[selected_cols],
                'contract_end_day_renewal',
                current_month
            )
            
            # 新增截斷地址欄位（用於地址模糊匹配）
            df_result['truncated_address'] = df_result['address'].apply(
                self._truncate_address_at_hao
            )
            
            context.update_data(df_result)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(len(df), len(df_result))
                        .set_time_info(start_time, datetime.now())
                        .add_custom('current_month', current_month)
                        .add_custom('average_months_diff', 
                                    float(df_result['months_diff'].mean()))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_result,
                message=f"月份差異計算完成: 當前月份 {current_month}",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"月份差異計算失敗: {str(e)}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _calculate_month_difference(self, df: pd.DataFrame, 
                                    date_column: str, 
                                    target_ym: int) -> pd.DataFrame:
        """計算月份差異"""
        df_result = df.copy()
        
        # 確保日期格式
        df_result[date_column] = pd.to_datetime(df_result[date_column])
        
        # 目標日期
        target_year = target_ym // 100
        target_month = target_ym % 100
        target_date = datetime(target_year, target_month, 1)
        
        # 計算差異
        def months_difference(date1, date2):
            return (date1.year - date2.year) * 12 + (date1.month - date2.month)
        
        df_result['months_diff'] = df_result[date_column].apply(
            lambda x: months_difference(x, target_date)
        ).add(1)
        
        return df_result
    
    def _truncate_address_at_hao(self, address: str) -> str:
        """截斷地址到「號」"""
        if not isinstance(address, str):
            return address
        
        pattern = r'^.*?號'
        match = re.search(pattern, address)
        return match.group(0) if match else address

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for calculating difference")
            return False
        
        return True