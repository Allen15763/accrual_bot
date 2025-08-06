"""
SPX PO處理器
繼承自BasePOProcessor，實現SPX特有的PO處理邏輯
"""

import os
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any

try:
    from .po_processor import BasePOProcessor
    from ...utils.logging import get_logger
    from ...data.importers.google_sheets_importer import GoogleSheetsImporter
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    from pathlib import Path
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from core.processors.po_processor import BasePOProcessor
    from utils.logging import get_logger
    from data.importers.google_sheets_importer import GoogleSheetsImporter


class SpxPOProcessor(BasePOProcessor):
    """SPX PO處理器，繼承自BasePOProcessor"""
    
    def __init__(self):
        """初始化SPX PO處理器"""
        super().__init__("SPX")
        self.logger = get_logger(self.__class__.__name__)
        
        # SPX特有配置
        self.bao_categories = ['Cost of Logistics and Warehouse - Water', 
                               'Cost of Logistics and Warehouse - Electricity']
        self.bao_supplier = 'TW_寶倉物流股份有限公司'
        
        # SPX部門相關帳戶
        self.dept_accounts = ['650005', '610104', '630001', '650003', '600301', 
                              '610110', '610105', '600310', '620003', '610311']
    
    def filter_spx_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """過濾SPX產品代碼
        
        Args:
            df: 原始PO數據
            
        Returns:
            pd.DataFrame: 過濾後的DataFrame
        """
        try:
            filtered_df = df.loc[df['Product Code'].str.contains('(?i)LG_SPX'), :].reset_index(drop=True)
            self.logger.info(f"SPX產品過濾: {len(df)} -> {len(filtered_df)} 筆記錄")
            return filtered_df
        except Exception as e:
            self.logger.error(f"過濾SPX產品代碼時出錯: {str(e)}", exc_info=True)
            return df
    
    def add_cols(self, df: pd.DataFrame, m: int) -> Tuple[pd.DataFrame, int]:
        """添加SPX特有的必要列
        
        Args:
            df: 原始PO數據
            m: 月份
            
        Returns:
            Tuple[pd.DataFrame, int]: 添加了必要列的DataFrame和更新的月份
        """
        try:
            # 先執行父類別的邏輯
            df, m = super().add_basic_columns(df, m)
            
            # 再額外新增SPX特定欄位
            df['memo'] = np.nan
            df['GL DATE'] = np.nan
            df['Remarked by Procurement PR'] = np.nan
            df['Noted by Procurement PR'] = np.nan
            df['Remarked by 上月 FN PR'] = np.nan
            
            self.logger.info("成功添加SPX特有列")
            return df, m
        except Exception as e:
            self.logger.error(f"添加SPX特有列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加SPX特有列時出錯")
    
    def get_closing_note(self) -> pd.DataFrame:
        """獲取關單數據 - 優化版本支持並發處理
        
        Returns:
            pd.DataFrame: 關單數據框
        """
        try:
            # 獲取Google Sheets配置
            config = {
                'certificate_path': self.config_manager.get('CREDENTIALS', 'certificate_path', ''),
                'scopes': self.config_manager.get_list('CREDENTIALS', 'scopes')
            }
            
            # 創建Google Sheets導入器
            sheets_importer = GoogleSheetsImporter(config)
            
            # 準備查詢參數
            spreadsheet_id = '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE'
            queries = [
                (spreadsheet_id, '2023年_done', 'A:J'),
                (spreadsheet_id, '2024年', 'A:J'),
                (spreadsheet_id, '2025年', 'A:J')
            ]
            
            # 並發執行查詢
            dfs = []
            for query in queries:
                try:
                    df_sheet = sheets_importer.read_sheet(query[0], query[1], query[2])
                    if df_sheet is not None and not df_sheet.empty:
                        dfs.append(df_sheet)
                except Exception as e:
                    self.logger.warning(f"讀取工作表 {query[1]} 失敗: {str(e)}")
            
            if not dfs:
                self.logger.warning("未能獲取任何關單數據")
                return pd.DataFrame()
            
            # 合併結果
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # 處理數據
            combined_df.dropna(subset=['Date'], inplace=True)
            combined_df.rename(columns={
                'Date': 'date', 
                'Type': 'type', 
                'PO Number': 'po_no', 
                'Requester': 'requester', 
                'Supplier': 'supplier',
                'Line Number / ALL': 'line_no', 
                'Reason': 'reason', 
                'New PR Number': 'new_pr_no', 
                'Remark': 'remark', 
                'Done(V)': 'done_by_fn'
            }, inplace=True)
            
            # 過濾空日期
            combined_df = combined_df.query("date!=''").reset_index(drop=True)
            
            self.logger.info(f"成功獲取關單數據，共 {len(combined_df)} 筆記錄")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"獲取關單數據時出錯: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
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
    
    def get_period_from_ap_invoice(self, df: pd.DataFrame, df_ap: pd.DataFrame, yyyymm: int) -> pd.DataFrame:
        """從AP發票填入期間到PO數據中（排除月份m之後的期間）
        
        Args:
            df: PO DataFrame
            df_ap: AP發票DataFrame
            yyyymm: 年月 (YYYYMM格式)
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 移除缺少'PO Number'的行並重置索引
            df_ap = df_ap.dropna(subset=['PO Number']).reset_index(drop=True)
            
            # 創建組合鍵用於匹配
            df_ap['po_line'] = (
                df_ap['Company'].astype(str) + '-' + 
                df_ap['PO Number'].astype(str) + '-' + 
                df_ap['PO_LINE_NUMBER'].astype(str)
            )
            
            # 將'Period'轉換為datetime然後格式化為整數yyyymm
            df_ap['period'] = (
                pd.to_datetime(df_ap['Period'], format='%b-%y', errors='coerce')
                .dt.strftime('%Y%m')
                .fillna('0')
                .astype('int32')
            )
            
            # 只保留期間在yyyymm之前的AP發票，並且對每個po_line保留最新的期間
            df_ap = (
                df_ap.loc[df_ap['period'] <= yyyymm, :]
                .sort_values(by=['po_line', 'period'])
                .drop_duplicates(subset='po_line', keep='last')
                .reset_index(drop=True)
            )
            
            # 根據匹配的'PO Line'將期間信息合併到df中
            df = df.merge(df_ap[['po_line', 'period']], left_on='PO Line', right_on='po_line', how='left')
            df['GL DATE'] = df['period']
            df.drop(columns=['po_line', 'period'], inplace=True)
            
            self.logger.info("成功添加GL DATE")
            return df
        except Exception as e:
            self.logger.error(f"添加GL DATE時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加GL DATE時出錯")
    
    def convert_date_format_in_remark(self, series: pd.Series) -> pd.Series:
        """轉換備註中的日期格式 (YYYY/MM -> YYYYMM)
        
        Args:
            series: 包含日期的Series
            
        Returns:
            pd.Series: 轉換後的Series
        """
        try:
            return series.astype(str).str.replace(r'(\d{4})/(\d{2})', r'\1\2', regex=True)
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
            return series.astype(str).str.extract(r'(\d{6}入FA)', expand=False)
        except Exception as e:
            self.logger.error(f"提取FA備註時出錯: {str(e)}", exc_info=True)
            return series
    
    def judge_previous(self, df: pd.DataFrame, previous_wp: pd.DataFrame, m: int, 
                       previous_wp_pr: pd.DataFrame = None) -> pd.DataFrame:
        """處理前期底稿（PO和PR）
        
        Args:
            df: PO DataFrame
            previous_wp: PO前期底稿DataFrame
            m: 月份
            previous_wp_pr: PR前期底稿DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 先處理PO前期底稿
            df = super().process_previous_workpaper(df, previous_wp, m)
            
            # 處理memo欄位
            if previous_wp is not None and not previous_wp.empty and 'memo' in previous_wp.columns:
                memo_mapping = self.get_mapping_dict(previous_wp, 'PO Line', 'memo')
                df['memo'] = df['PO Line'].map(memo_mapping)
            
            # 處理PR前期底稿
            if previous_wp_pr is not None and not previous_wp_pr.empty:
                # 重命名前期PR底稿中的列
                previous_wp_pr = previous_wp_pr.rename(
                    columns={'Remarked by FN': 'Remarked by FN_l'}
                )
                
                # 獲取前期PR FN備註
                pr_fn_mapping = self.get_mapping_dict(previous_wp_pr, 'PR Line', 'Remarked by FN_l')
                df['Remarked by 上月 FN PR'] = df['PR Line'].map(pr_fn_mapping)
            
            self.logger.info("成功處理SPX前期底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理SPX前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理SPX前期底稿時出錯")
    
    def judge_procurement(self, df: pd.DataFrame, procurement: pd.DataFrame, 
                          procurement_pr: pd.DataFrame = None) -> pd.DataFrame:
        """處理採購底稿（PO和PR）
        
        Args:
            df: PO DataFrame
            procurement: PO採購底稿DataFrame
            procurement_pr: PR採購底稿DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 先處理PO採購底稿
            df = super().process_procurement_workpaper(df, procurement)
            
            # 移除SPT模組給的狀態（SPX有自己的狀態邏輯）
            df.loc[df['PO狀態'] == 'Not In Procurement WP', 'PO狀態'] = pd.NA
            
            # 處理PR採購底稿
            if procurement_pr is not None and not procurement_pr.empty:
                # 重命名PR採購底稿中的列
                procurement_pr = procurement_pr.rename(
                    columns={
                        'Remarked by Procurement': 'Remark by PR Team',
                        'Noted by Procurement': 'Noted by PR'
                    }
                )
                
                # 獲取PR採購底稿中的備註
                pr_procurement_mapping = self.get_mapping_dict(procurement_pr, 'PR Line', 'Remark by PR Team')
                df['Remarked by Procurement PR'] = df['PR Line'].map(pr_procurement_mapping)
                
                pr_noted_mapping = self.get_mapping_dict(procurement_pr, 'PR Line', 'Noted by PR')
                df['Noted by Procurement PR'] = df['PR Line'].map(pr_noted_mapping)
            
            self.logger.info("成功處理SPX採購底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理SPX採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理SPX採購底稿時出錯")
    
    def give_status_stage_1(self, df: pd.DataFrame, df_spx_closing: pd.DataFrame) -> pd.DataFrame:
        """給予第一階段狀態 - SPX特有邏輯
        
        Args:
            df: PO DataFrame
            df_spx_closing: SPX關單數據DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 依據已關單條件取得對應的PO#
            c1, c2 = self.is_closed_spx(df_spx_closing)
            to_be_close = df_spx_closing.loc[c1, 'po_no'].unique() if c1.any() else []
            closed = df_spx_closing.loc[c2, 'po_no'].unique() if c2.any() else []
            
            # 定義「上月FN」備註關單條件
            remarked_close_by_fn_last_month = (
                df['Remarked by 上月 FN'].str.contains('刪|關', na=False) | 
                df['Remarked by 上月 FN PR'].str.contains('刪|關', na=False)
            )
            
            # 統一轉換日期格式
            df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
            df['Remarked by 上月 FN PR'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN PR'])
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
            cond1 = df['Item Description'].str.contains('(?i)押金|保證金|Deposit|找零金|定存', na=False)
            is_fa = df['GL#'].astype(str) == self.config_manager.get('FA_ACCOUNTS', 'spx', '199999')
            cond_exclude = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA
            df.loc[cond1 & ~is_fa & ~cond_exclude, 'PO狀態'] = '摘要內有押金/保證金/Deposit/找零金'
            
            # 條件2：供應商與類別對應，做GL調整
            cond2 = (df['PO Supplier'] == self.bao_supplier) & (df['Category'].isin(self.bao_categories))
            df.loc[cond2, 'PO狀態'] = 'GL調整'
            
            # 條件3：該PO#在待關單清單中
            cond3 = df['PO#'].astype(str).isin([str(x) for x in to_be_close])
            df.loc[cond3, 'PO狀態'] = '待關單'
            
            # 條件4：該PO#在已關單清單中
            cond4 = df['PO#'].astype(str).isin([str(x) for x in closed])
            df.loc[cond4, 'PO狀態'] = '已關單'
            
            # 條件5：上月FN備註含有「刪」或「關」
            cond5 = remarked_close_by_fn_last_month
            df.loc[cond5, 'PO狀態'] = '參照上月關單'
            
            # 條件6：若「Remarked by 上月 FN」含有「入FA」，則提取該數字，並更新狀態
            cond6 = (
                (df['Remarked by 上月 FN'].str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN'].str.contains('部分完成', na=False))
            )
            if cond6.any():
                extracted_fn = self.extract_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                df.loc[cond6, 'PO狀態'] = extracted_fn
            
            # 條件7：若「Remarked by 上月 FN PR」含有「入FA」，則提取該數字，並更新狀態
            cond7 = (
                (df['Remarked by 上月 FN PR'].str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN PR'].str.contains('部分完成', na=False))
            )
            if cond7.any():
                extracted_pr = self.extract_fa_remark(df.loc[cond7, 'Remarked by 上月 FN PR'])
                df.loc[cond7, 'PO狀態'] = extracted_pr
            
            self.logger.info("成功給予第一階段狀態")
            return df
        
        except Exception as e:
            self.logger.error(f"給予第一階段狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("給予第一階段狀態時出錯")
    
    def erm(self, df: pd.DataFrame, ym: int, ref_a: pd.DataFrame, ref_b: pd.DataFrame) -> pd.DataFrame:
        """處理SPX特有的ERM邏輯
        
        Args:
            df: PO DataFrame
            ym: 年月
            ref_a: 科目參考數據
            ref_b: 負債參考數據
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 設置檔案日期
            df['檔案日期'] = ym
            
            # 定義ERM狀態條件
            is_fa = df['GL#'].astype(str) == self.config_manager.get('FA_ACCOUNTS', 'spx', '199999')
            
            # 條件1：已入帳（明確標註）
            df.loc[df['Remarked by 上月 FN'].str.contains('(?i)已入帳', na=False), 'PO狀態'] = '已入帳'
            
            # 條件2：已入帳（有GL DATE且符合其他條件）
            condition_booked = (
                (~df['GL DATE'].isna()) & 
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both')) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Billed Quantity'] != '0') &
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成|已入帳', na=False))) &
                (~is_fa)
            )

            # 條件3：已完成
            condition_completed = (
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成', na=False))) &
                (~df['Remarked by 上月 FN PR'].str.contains('(?i)未完成', na=False)) &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) == 0)
            )
            
            # 條件4：全付完，未關單
            condition_paid_not_closed = (
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成', na=False))) &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) != 0) &
                (df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float) == 0)
            )
            
            # 條件5：已完成但有未付款部分
            condition_completed_with_unpaid = (
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成', na=False))) &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) != 0) &
                (df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float) != 0)
            )
            
            # 條件6：需檢查收貨
            condition_check_receipt = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] != df['Received Quantity'])
            )
            
            # 條件7：未完成
            condition_incomplete = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] > df['檔案日期'])
            )
            
            # 條件8：範圍錯誤_租金
            condition_range_error_lease = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002') & 
                (df['Item Description'].str.contains('(?i)租金', na=False))
            )

            # 條件9：範圍錯誤_薪資
            condition_range_error_salary = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002') & 
                (df['Item Description'].str.contains('(?i)派遣|Salary|Agency Fee', na=False))
            )

            # 條件10：範圍錯誤
            condition_range_error = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002')
            )
            
            # 條件11：部分完成ERM
            condition_partially_completed_erm = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002') &
                (df['Received Quantity'].astype(float) != 0) &
                (df['Entry Quantity'] != df['Received Quantity'])
            )
            
            # 組合所有條件
            conditions = [
                condition_booked,
                condition_completed,
                condition_paid_not_closed,
                condition_completed_with_unpaid,
                condition_check_receipt,
                condition_incomplete,
                condition_range_error_lease,
                condition_range_error_salary,
                condition_range_error,
                condition_partially_completed_erm
            ]
            
            # 對應的結果
            results = [
                '已入帳',
                '已完成',
                '全付完，未關單?',
                '已完成',
                'Check收貨',
                '未完成',
                'error(Description Period is out of ERM)_租金',
                'error(Description Period is out of ERM)_薪資',
                'error(Description Period is out of ERM)',
                '部分完成ERM',
            ]
            
            # 應用條件
            df['PO狀態'] = np.select(conditions, results, default=df['PO狀態'])
            
            # 處理格式錯誤
            mask_format_error = (
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) & 
                (df['YMs of Item Description'] == '100001,100002')
            )
            df.loc[mask_format_error, 'PO狀態'] = '格式錯誤，退單'
            
            # 根據PO狀態設置估計入帳 - SPX邏輯：已完成->入帳，其餘N
            mask_completed = (df['PO狀態'] == '已完成')
            df.loc[mask_completed, '是否估計入帳'] = 'Y'
            df.loc[~mask_completed, '是否估計入帳'] = 'N'
            
            need_to_accrual = df['是否估計入帳'] == 'Y'
            
            # 設置Account code
            df.loc[need_to_accrual, 'Account code'] = df.loc[need_to_accrual, 'GL#']
            
            # 設置Account Name
            df['Account Name'] = pd.merge(
                df, ref_a, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Account Desc']
            
            # 設置Product code - SPX固定值
            df.loc[need_to_accrual, 'Product code'] = "LG_SPX_OWN"
            
            # 設置Region_c - SPX固定值
            df.loc[need_to_accrual, 'Region_c'] = "TW"
            
            # 設置Dep. - SPX特有邏輯
            isin_dept_account = df['Account code'].astype(str).isin(self.dept_accounts)
            df.loc[need_to_accrual & isin_dept_account, 'Dep.'] = \
                df.loc[need_to_accrual & isin_dept_account, 'Department'].str[:3]
            df.loc[need_to_accrual & ~isin_dept_account, 'Dep.'] = '000'
            
            # 設置Currency_c
            df.loc[need_to_accrual, 'Currency_c'] = df.loc[need_to_accrual, 'Currency']
            
            # 設置Accr. Amount - SPX特殊計算方式
            df['temp_amount'] = (
                df['Unit Price'].astype(float) * 
                (df['Entry Quantity'].astype(float) - df['Billed Quantity'].astype(float))
            )
            df.loc[need_to_accrual, 'Accr. Amount'] = df.loc[need_to_accrual, 'temp_amount']
            df.drop('temp_amount', axis=1, inplace=True)
            
            # 設置是否有預付
            is_prepayment = df['Entry Prepay Amount'] != '0'
            df.loc[need_to_accrual & is_prepayment, '是否有預付'] = 'Y'

            # 設置Liability
            df['Liability'] = pd.merge(
                df, ref_b, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Liability_y']
            df.loc[need_to_accrual & is_prepayment, 'Liability'] = '111112'
            
            # 設置PR Product Code Check
            if 'Product code' in df.columns and 'Project' in df.columns:
                mask_product_code = df['Product code'].notnull()
                try:
                    product_match = df.loc[mask_product_code, 'Project'].str.findall(r'^(\w+(?:))').apply(
                        lambda x: x[0] if len(x) > 0 else ''
                    ) == df.loc[mask_product_code, 'Product code']
                    
                    df.loc[mask_product_code, 'PR Product Code Check'] = np.where(
                        product_match, 'good', 'bad'
                    )
                except Exception as e:
                    self.logger.error(f"設置PR Product Code Check時出錯: {str(e)}", exc_info=True)
                    raise ValueError("設置PR Product Code Check時出錯")
            
            self.logger.info("成功處理SPX ERM邏輯")
            return df
            
        except Exception as e:
            self.logger.error(f"處理SPX ERM邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理SPX ERM邏輯時出錯")
    
    def reformate(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化最終數據 - SPX特有格式
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 格式化後的DataFrame
        """
        try:
            # 格式化數值列
            df = self.format_numeric_columns_safely(
                df, 
                ['Line#', 'GL#'],
                ['Unit Price', 'Entry Amount', 'Entry Invoiced Amount', 
                 'Entry Billed Amount', 'Entry Prepay Amount', 
                 'PO Entry full invoiced status', 'Accr. Amount']
            )
            
            # 格式化日期
            df = self.reformat_dates(df)
            
            # 移除臨時計算列
            temp_columns = ['檔案日期', 'Expected Received Month_轉換格式', 'YMs of Item Description']
            for col in temp_columns:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)
            
            # 清理nan值
            columns_to_clean = [
                '是否估計入帳', 'PR Product Code Check', 'PO狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df = self.clean_nan_values(df, columns_to_clean)
            
            # SPX特有的欄位重新排列
            # 重新排列上月備註欄位位置
            if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
                fn_index = df.columns.get_loc('Remarked by FN') + 1
                last_month_col = df.pop('Remarked by 上月 FN')
                df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)

            if 'Remarked by 上月 FN' in df.columns and 'Remarked by 上月 FN PR' in df.columns:
                fn_pr_index = df.columns.get_loc('Remarked by 上月 FN') + 1
                last_month_pr_col = df.pop('Remarked by 上月 FN PR')
                df.insert(fn_pr_index, 'Remarked by 上月 FN PR', last_month_pr_col)
            
            # 重新排列PO狀態欄位位置 - 放在"是否估計入帳"之前
            if 'PO狀態' in df.columns and '是否估計入帳' in df.columns:
                accrual_index = df.columns.get_loc('是否估計入帳')
                po_status_col = df.pop('PO狀態')
                df.insert(accrual_index, 'PO狀態', po_status_col)

            # 重新排列PR欄位位置
            if 'Noted by Procurement' in df.columns:
                noted_index = df.columns.get_loc('Noted by Procurement') + 1
                
                for col_name in ['Remarked by Procurement PR', 'Noted by Procurement PR']:
                    if col_name in df.columns:
                        col = df.pop(col_name)
                        df.insert(noted_index, col_name, col)
                        noted_index += 1
            
            self.logger.info("成功格式化SPX數據")
            return df
            
        except Exception as e:
            self.logger.error(f"格式化SPX數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化SPX數據時出錯")
    
    def process(self, fileUrl: str, file_name: str, 
                fileUrl_previwp: str = None, fileUrl_p: str = None, 
                fileUrl_ap: str = None, fileUrl_previwp_pr: str = None,
                fileUrl_p_pr: str = None) -> None:
        """處理SPX PO數據的主流程
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期PO底稿文件路徑
            fileUrl_p: 採購PO底稿文件路徑
            fileUrl_ap: AP invoice文件路徑
            fileUrl_previwp_pr: 前期PR底稿文件路徑
            fileUrl_p_pr: 採購PR底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"開始處理SPX PO數據: {file_name}")
            
            # 導入原始數據
            df, date, m = self.importer.import_rawdata(fileUrl, file_name)
            
            # 導入參考數據 - 用SPT的ref檔案參照會計科目和負債科目
            ref_ac, ref_liability = self.importer.import_reference_data('SPT')

            # 過濾SPX產品代碼
            df = self.filter_spx_product_code(df)
            
            # 添加必要列
            df, m = self.add_cols(df, m)
            
            # 處理AP invoice - SPX特有邏輯
            if fileUrl_ap:
                try:
                    df_ap = self.importer.import_ap_invoice(fileUrl_ap)
                    if df_ap is not None and not df_ap.empty:
                        df = self.get_period_from_ap_invoice(df, df_ap, date)
                        self.logger.info("成功處理AP發票數據")
                    else:
                        self.logger.warning("AP發票數據為空或無效")
                except Exception as e:
                    self.logger.warning(f"處理AP發票時出錯: {str(e)}")
            
            # 處理前期底稿(PO和PR)
            previous_wp = None
            previous_wp_pr = None
            
            if fileUrl_previwp:
                try:
                    previous_wp = self.importer.import_previous_wp(fileUrl_previwp)
                except Exception as e:
                    self.logger.warning(f"導入前期PO底稿失敗: {str(e)}")
            
            if fileUrl_previwp_pr:
                try:
                    previous_wp_pr = self.importer.import_previous_wp(fileUrl_previwp_pr)
                except Exception as e:
                    self.logger.warning(f"導入前期PR底稿失敗: {str(e)}")
            
            if previous_wp is not None or previous_wp_pr is not None:
                df = self.judge_previous(df, previous_wp, m, previous_wp_pr)
                self.logger.info("成功處理前期底稿")
            
            # 處理採購底稿(PO和PR)
            procurement = None
            procurement_pr = None
            
            if fileUrl_p:
                try:
                    procurement = self.importer.import_procurement(fileUrl_p)
                except Exception as e:
                    self.logger.warning(f"導入採購PO底稿失敗: {str(e)}")
            
            if fileUrl_p_pr:
                try:
                    procurement_pr = self.importer.import_procurement(fileUrl_p_pr)
                except Exception as e:
                    self.logger.warning(f"導入採購PR底稿失敗: {str(e)}")
            
            if procurement is not None or procurement_pr is not None:
                df = self.judge_procurement(df, procurement, procurement_pr)
                
                # 會計使用:該欄位用於後續狀態判斷，故不可以為null
                if df['Remarked by Procurement'].isna().all():
                    error_str = "參照採購底稿的Remarked by Procurement錯誤。e.g. null value"
                    self.logger.error(error_str)
                    raise ValueError(error_str)
                
                self.logger.info("成功處理採購底稿")
            
            # 處理日期邏輯
            df = self.apply_date_logic(df)
            
            # 獲取關單數據並給予第一階段狀態
            df_spx_closing = self.get_closing_note()
            df = self.give_status_stage_1(df, df_spx_closing)
            
            # 處理ERM邏輯
            df = self.erm(df, date, ref_ac, ref_liability)

            # 格式化數據
            df = self.reformate(df)
            
            # 導出文件
            self.export_file(df, date, 'PO')
            
            self.logger.info(f"成功完成SPX PO數據處理: {file_name}")
            
        except Exception as e:
            self.logger.error(f"處理SPX PO數據時出錯: {str(e)}", exc_info=True)
            raise ValueError(f"處理SPX PO數據時出錯: {str(e)}")
    
    def concurrent_spx_process(self, file_paths: Dict[str, str]) -> None:
        """SPX PO 並發處理主流程
        
        Args:
            file_paths: 文件路徑字典，包含所有需要處理的文件路徑
        
        Returns:
            None
        """
        try:
            self.logger.info(f"開始並發處理SPX PO數據: {file_paths.get('po_file_name', 'Unknown')}")
            
            # 調用process方法處理
            self.process(
                fileUrl=file_paths.get('po_file', ''),
                file_name=file_paths.get('po_file_name', ''),
                fileUrl_previwp=file_paths.get('previous_wp', None),
                fileUrl_p=file_paths.get('procurement', None),
                fileUrl_ap=file_paths.get('ap_invoice', None),
                fileUrl_previwp_pr=file_paths.get('previous_wp_pr', None),
                fileUrl_p_pr=file_paths.get('procurement_pr', None)
            )
            
            self.logger.info("成功完成SPX PO並發處理")
            
        except Exception as e:
            self.logger.error(f"SPX PO並發處理時出錯: {str(e)}", exc_info=True)
            raise ValueError(f"SPX PO並發處理時出錯: {str(e)}")
