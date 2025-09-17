"""
更新後的SPX PO處理器
整合AsyncDataImporter，完全兼容原始版本的並發導入方式
"""

import os
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any

try:
    from .po_processor import BasePOProcessor
    from ...utils.logging import get_logger
    from ...utils import get_unique_filename, classify_description, give_account_by_keyword
    from ...data.importers.google_sheets_importer import GoogleSheetsImporter
    from ...data.importers.async_data_importer import AsyncDataImporter  # 新增
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
    from utils import get_unique_filename, classify_description, give_account_by_keyword
    from data.importers.google_sheets_importer import GoogleSheetsImporter
    from data.importers.async_data_importer import AsyncDataImporter  # 新增


class SpxPOProcessor(BasePOProcessor):
    """SPX PO處理器，繼承自BasePOProcessor"""
    
    def __init__(self):
        """初始化SPX PO處理器"""
        super().__init__("SPX")
        self.logger = get_logger(self.__class__.__name__)
        
        # SPX部門相關帳戶
        self.dept_accounts = self.config_manager.get_list(self.entity_type, 'exp_accounts')
    
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

            df_ap['match_type'] = df_ap['Match Type'].fillna('system_filled')
            
            # 只保留期間在yyyymm之前的AP發票，並且對每個po_line保留最新的期間
            df_ap = (
                df_ap.loc[df_ap['period'] <= yyyymm, :]
                .sort_values(by=['po_line', 'period'])
                .drop_duplicates(subset='po_line', keep='last')
                .reset_index(drop=True)
            )
            
            # 根據匹配的'PO Line'將期間信息合併到df中
            df = df.merge(df_ap[['po_line', 'period', 'match_type']], left_on='PO Line', right_on='po_line', how='left')
            df['GL DATE'] = df['period']
            df.drop(columns=['po_line', 'period'], inplace=True)
            
            self.logger.info("成功添加GL DATE and match_type")
            return df
        except Exception as e:
            self.logger.error(f"添加GL DATE時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加GL DATE時出錯")
    
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
            mask_completed = (df['PO狀態'].str.contains('已完成', na=False))
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
            df.loc[need_to_accrual, 'Product code'] = df.loc[need_to_accrual, 'Product Code']
            
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
            df['Accr. Amount'] = (df['Accr. Amount'].str.replace(',', '')
                                  .fillna(0)
                                  .astype(float)
                                  .apply(lambda x: x if x != 0 else None))
            
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

            # 把本期驗收數量/金額移到memo前面
            if '本期驗收數量/金額' in df.columns:
                memo_index = df.columns.get_loc('memo')
                validation_col = df.pop('本期驗收數量/金額')
                df.insert(memo_index, '本期驗收數量/金額', validation_col)
            
            self.logger.info("成功格式化SPX數據")
            return df
            
        except Exception as e:
            self.logger.error(f"格式化SPX數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化SPX數據時出錯")
    
    def process(self, fileUrl: str, file_name: str, 
                fileUrl_previwp: str = None, fileUrl_p: str = None, 
                fileUrl_ap: str = None, fileUrl_previwp_pr: str = None,
                fileUrl_p_pr: str = None,
                fileUrl_opsValidation: str = None) -> None:
        """處理SPX PO數據的主流程 - 使用AsyncDataImporter並發導入
        
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
            
            # 準備文件信息字典 - SPX特有的文件結構
            file_info = {
                'raw_po': fileUrl,
                'previous': fileUrl_previwp if fileUrl_previwp else None,
                'procurement_po': fileUrl_p if fileUrl_p else None,
                'ap_invoice': fileUrl_ap if fileUrl_ap else None,
                'previous_pr': fileUrl_previwp_pr if fileUrl_previwp_pr else None,
                'procurement_pr': fileUrl_p_pr if fileUrl_p_pr else None
            }
            
            # 創建並發導入器 - 與原始版本完全相同的使用方式
            async_importer = AsyncDataImporter()
            
            # 準備並發導入任務
            file_types = []
            file_paths = []
            file_names = {}
            
            for file_type, file_path in file_info.items():
                if file_path:
                    file_types.append(file_type)
                    file_paths.append(file_path)
                    file_names[file_type] = os.path.basename(file_path)
            
            # if os.path.isfile(r'C:\SEA\Accrual\prpo_bot\prpo_bot_renew_v2\output\import_results.pkl'):
            if os.path.isfile(r'C:\SEA\Accrual\prpo_bot\prpo_bot_renew_v2\output\import_results.pkl') is not True:
                import pickle as pkl
                with open(r'C:\SEA\Accrual\prpo_bot\prpo_bot_renew_v2\output\import_results.pkl', 'rb') as f:
                    import_results = pkl.load(f)
                self.logger.info('Loaded existing files.')
            else:
                # 並發導入所有文件 - 與原始版本相同的調用方式
                import_results = async_importer.concurrent_read_files(
                    file_types, 
                    file_paths, 
                    file_names=file_names,
                    config={'certificate_path': self.config_manager.get('CREDENTIALS', 'certificate_path'),
                            'scopes': self.config_manager.get_list('CREDENTIALS', 'scopes')},
                    ap_columns=self.config_manager.get_list('SPX', 'ap_columns')
                )
            
            # 檢測並處理原始PO數據
            if 'raw_po' in import_results:
                raw_po_result = import_results['raw_po']
                if isinstance(raw_po_result, tuple) and len(raw_po_result) == 3:
                    df, date, m = raw_po_result
                else:
                    self.logger.error("原始PO數據格式不正確")
                    raise ValueError("原始PO數據格式不正確")
            else:
                self.logger.error("無法導入原始PO數據")
                raise ValueError("無法導入原始PO數據")
            
            # 導入參考數據 - 用SPT的ref檔案參照會計科目和負債科目
            ref_ac, ref_liability = async_importer.import_reference_data('SPT')

            # 過濾SPX產品代碼
            df = self.filter_spx_product_code(df)
            
            # 添加必要列
            df, m = self.add_cols(df, m)
            
            # 處理AP invoice - SPX特有邏輯
            if 'ap_invoice' in import_results:
                ap_invoice_result = import_results['ap_invoice']
                if self._is_valid_data(ap_invoice_result):
                    df_ap = ap_invoice_result
                    df = self.get_period_from_ap_invoice(df, df_ap, date)
                    self.logger.info("成功處理AP發票數據")
                else:
                    self.logger.warning("AP發票數據為空或無效")
            
            # 處理前期底稿(PO和PR)
            previous_wp = None
            previous_wp_pr = None
            
            if 'previous' in import_results:
                previous_result = import_results['previous']
                if self._is_valid_data(previous_result):
                    previous_wp = previous_result
            
            if 'previous_pr' in import_results:
                previous_pr_result = import_results['previous_pr']
                if self._is_valid_data(previous_pr_result):
                    previous_wp_pr = previous_pr_result
            
            if previous_wp is not None and previous_wp_pr is not None:
                df = self.judge_previous(df, previous_wp, m, previous_wp_pr)
                # 處理memo欄位
                if 'memo' in previous_wp.columns:
                    memo_mapping = self.get_mapping_dict(previous_wp, 'PO Line', 'memo')
                    df['memo'] = df['PO Line'].map(memo_mapping)
                self.logger.info("成功處理前期底稿(PO和PR)")
            else:
                self.logger.warning("前期底稿(PO或PR)為空或無效")
            
            # 處理採購底稿(PO和PR)
            procurement = None
            procurement_pr = None
            
            if 'procurement_po' in import_results:
                procurement_result = import_results['procurement_po']
                if self._is_valid_data(procurement_result):
                    procurement = procurement_result
            
            if 'procurement_pr' in import_results:
                procurement_pr_result = import_results['procurement_pr']
                if self._is_valid_data(procurement_pr_result):
                    procurement_pr = procurement_pr_result
            
            if procurement is not None and procurement_pr is not None:
                df = self.judge_procurement(df, procurement, procurement_pr)
                # 會計使用:該欄位用於後續狀態判斷，故不可以為null
                if df['Remarked by Procurement'].isna().all():
                    error_str = "參照採購底稿的Remarked by Procurement錯誤。e.g. null value"
                    self.logger.error(error_str)
                    raise ValueError(error_str)
                self.logger.info("成功處理採購底稿(PO和PR)")
            else:
                self.logger.warning("採購底稿(PO或PR)為空或無效")
            
            # 處理日期邏輯
            df = self.apply_date_logic(df)
            
            # 獲取關單數據並給予第一階段狀態
            df_spx_closing = self.get_closing_note()
            df = self.give_status_stage_1(df, df_spx_closing, date)
            
            # 處理ERM邏輯
            df = self.erm(df, date, ref_ac, ref_liability)

            # 處理驗收
            if fileUrl_opsValidation:
                locker_non_discount, locker_discount, kiosk_data = \
                    self.process_validation_data(fileUrl_opsValidation, date)
                df = self.apply_validation_data_to_po(df, locker_non_discount, locker_discount, kiosk_data)
            # 格式化數據
            df = self.reformate(df)

            df['CATEGORY'] = df['Item Description'].apply(classify_description)
            # 暫時全放提供使用者參考不驗證"是否估計入帳"
            df = give_account_by_keyword(df, 'Item Description', export_keyword=True)
            # df['Predicted_Account'] = np.where(df['是否估計入帳'] == 'Y', df['Predicted_Account'], None)
            # df['Matched_Keyword'] = np.where(df['是否估計入帳'] == 'Y', df['Matched_Keyword'], None)
            df = self.is_installment(df)
            
            # 導出文件
            self._save_output(df, file_name)
            
            self.logger.info(f"成功完成SPX PO數據處理: {file_name}")
            
        except Exception as e:
            self.logger.error(f"處理SPX PO數據時出錯: {str(e)}", exc_info=True)
            raise ValueError(f"處理SPX PO數據時出錯: {str(e)}")
        
    def process_validation_data(self, 
                                validation_file_path: str, 
                                target_date: int) -> Tuple[Dict[str, dict], Dict[str, dict], Dict[str, dict]]:
        """
        處理驗收數據 - 智取櫃和繳費機驗收明細
        
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Tuple[Dict, Dict, Dict]: (智取櫃非折扣驗收數量, 智取櫃折扣驗收數量, 繳費機驗收數量)
            
        Raises:
            ValueError: 當驗收數據處理失敗時
        """
        try:
            self.logger.info(f"開始處理驗收數據: {validation_file_path}")
            
            # 檢查檔案是否存在
            if not os.path.exists(validation_file_path):
                self.logger.error(f"驗收檔案不存在: {validation_file_path}")
                raise FileNotFoundError(f"驗收檔案不存在: {validation_file_path}")
            
            # 處理智取櫃驗收明細
            locker_data = self._process_locker_validation_data(validation_file_path, target_date)
            
            # 處理繳費機驗收明細
            kiosk_data = self._process_kiosk_validation_data(validation_file_path, target_date)
            
            self.logger.info("成功完成驗收數據處理")
            return locker_data['non_discount'], locker_data['discount'], kiosk_data
            
        except Exception as e:
            self.logger.error(f"處理驗收數據時發生錯誤: {str(e)}", exc_info=True)
            raise ValueError(f"處理驗收數據失敗: {str(e)}")

    def _process_locker_validation_data(self, validation_file_path: str, target_date: int) -> Dict[str, dict]:
        """
        處理智取櫃驗收明細數據
        
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Dict[str, dict]: 包含非折扣和折扣驗收數量的字典
        """
        try:
            # 讀取智取櫃驗收明細
            df_locker = pd.read_excel(
                validation_file_path, 
                sheet_name='智取櫃驗收明細', 
                header=1, 
                usecols='A:AE'
            )
            
            # 檢查數據是否為空
            if df_locker.empty:
                self.logger.warning("智取櫃驗收明細數據為空")
                return {'non_discount': {}, 'discount': {}}
            
            # 設置欄位名稱
            locker_columns = self.config_manager.get_list(self.entity_type, 'locker_columns')
            if len(df_locker.columns) != len(locker_columns):
                self.logger.warning(f"智取櫃欄位數量不匹配: 期望 {len(locker_columns)}, 實際 {len(df_locker.columns)}")
            
            df_locker.columns = locker_columns
            
            # 過濾掉驗收月份為空的記錄
            df_locker = df_locker.loc[~df_locker['驗收月份'].isna(), :].reset_index(drop=True)
            
            if df_locker.empty:
                self.logger.warning("過濾後智取櫃驗收明細數據為空")
                return {'non_discount': {}, 'discount': {}}
            
            # 轉換驗收月份格式
            df_locker['validated_month'] = pd.to_datetime(
                df_locker['驗收月份'], 
                errors='coerce'
            ).dt.strftime('%Y%m').astype('Int64')  # 使用 nullable integer
            
            # 移除無效日期的記錄
            df_locker = df_locker.dropna(subset=['validated_month']).reset_index(drop=True)
            
            # 篩選目標月份的數據
            df_locker_filtered = df_locker.loc[df_locker['validated_month'] == target_date, :]
            
            if df_locker_filtered.empty:
                self.logger.info(f"智取櫃驗收明細中沒有找到 {target_date} 的數據")
                return {'non_discount': {}, 'discount': {}}
            
            # 定義聚合欄位
            agg_cols = [
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'DA', 
                'XA', 'XB', 'XC', 'XD', 'XE', 'XF',
                '超出櫃體安裝費', '超出櫃體運費', '裝運費'
            ]
            
            # 檢查聚合欄位是否存在
            missing_cols = [col for col in agg_cols if col not in df_locker_filtered.columns]
            if missing_cols:
                self.logger.warning(f"智取櫃數據中缺少欄位: {missing_cols}")
                agg_cols = [col for col in agg_cols if col in df_locker_filtered.columns]
            
            # 分類處理折扣和非折扣驗收
            validation_results = self._categorize_validation_data(df_locker_filtered, agg_cols, 'locker')
            
            self.logger.info(f"智取櫃驗收處理完成 - 非折扣: {len(validation_results['non_discount'])} 筆, "
                             f"折扣: {len(validation_results['discount'])} 筆")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"處理智取櫃驗收數據時發生錯誤: {str(e)}", exc_info=True)
            raise

    def _process_kiosk_validation_data(self, validation_file_path: str, target_date: int) -> Dict[str, dict]:
        """
        處理繳費機驗收明細數據
        
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Dict[str, dict]: 繳費機驗收數量字典
        """
        try:
            # 讀取繳費機驗收明細
            df_kiosk = pd.read_excel(
                validation_file_path, 
                sheet_name='繳費機驗收明細', 
                usecols='A:J'
            )
            
            # 檢查數據是否為空
            if df_kiosk.empty:
                self.logger.warning("繳費機驗收明細數據為空")
                return {}
            
            # 檢查必要欄位是否存在
            required_cols = ['PO單號', '驗收月份']
            missing_required = [col for col in required_cols if col not in df_kiosk.columns]
            if missing_required:
                self.logger.error(f"繳費機數據缺少必要欄位: {missing_required}")
                return {}
            
            # 過濾掉驗收月份為空的記錄
            df_kiosk = df_kiosk.loc[~df_kiosk['驗收月份'].isna(), :].reset_index(drop=True)
            
            if df_kiosk.empty:
                self.logger.warning("過濾後繳費機驗收明細數據為空")
                return {}
            
            # 轉換驗收月份格式
            df_kiosk['validated_month'] = pd.to_datetime(
                df_kiosk['驗收月份'], 
                errors='coerce'
            ).dt.strftime('%Y%m').astype('Int64')
            
            # 移除無效日期的記錄
            df_kiosk = df_kiosk.dropna(subset=['validated_month']).reset_index(drop=True)
            
            # 篩選目標月份的數據
            df_kiosk_filtered = df_kiosk.loc[df_kiosk['validated_month'] == target_date, :]
            
            if df_kiosk_filtered.empty:
                self.logger.info(f"繳費機驗收明細中沒有找到 {target_date} 的數據")
                return {}
            
            # 取得當期驗收數
            kiosk_validation = df_kiosk_filtered['PO單號'].value_counts().to_dict()
            
            self.logger.info(f"繳費機驗收處理完成 - {len(kiosk_validation)} 筆")
            return kiosk_validation
            
        except Exception as e:
            self.logger.error(f"處理繳費機驗收數據時發生錯誤: {str(e)}", exc_info=True)
            # 繳費機數據處理失敗不應該阻止整個流程
            return {}

    def _categorize_validation_data(self, df: pd.DataFrame, agg_cols: List[str], data_type: str) -> Dict[str, dict]:
        """
        分類驗收數據為折扣和非折扣類型
        
        Args:
            df: 驗收數據DataFrame
            agg_cols: 需要聚合的欄位列表
            data_type: 數據類型標識 ('locker' 或 'kiosk')
            
        Returns:
            Dict[str, dict]: 包含 'non_discount' 和 'discount' 鍵的字典
        """
        try:
            validation_results = {'non_discount': {}, 'discount': {}}
            
            # 檢查是否有 discount 欄位
            if 'discount' not in df.columns:
                self.logger.warning(f"{data_type}數據中沒有 discount 欄位，所有數據將歸類為非折扣")
                df['discount'] = ''
            
            # 確保 discount 欄位為字符串類型
            df['discount'] = df['discount'].fillna('').astype(str)
            
            # 非折扣驗收 (不包含 X折驗收 的記錄)
            try:
                non_discount_condition = ~df['discount'].str.contains(r'\d折驗收', na=False, regex=True)
                df_non_discount = df.loc[non_discount_condition, :]
                
                if not df_non_discount.empty and 'PO單號' in df_non_discount.columns:
                    validation_results['non_discount'] = (
                        df_non_discount.groupby(['PO單號'])[agg_cols]
                        .sum()
                        .to_dict('index')
                    )
            except Exception as e:
                self.logger.error(f"處理非折扣{data_type}數據時出錯: {str(e)}")
            
            # 折扣驗收 (包含 X折驗收 的記錄)
            try:
                discount_condition = df['discount'].str.contains(r'\d折驗收', na=False, regex=True)
                df_discount = df.loc[discount_condition, :]
                
                if not df_discount.empty and 'PO單號' in df_discount.columns:
                    validation_results['discount'] = (
                        df_discount.groupby(['PO單號'])[agg_cols]
                        .sum()
                        .to_dict('index')
                    )
            except Exception as e:
                self.logger.error(f"處理折扣{data_type}數據時出錯: {str(e)}")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"分類{data_type}驗收數據時發生錯誤: {str(e)}", exc_info=True)
            return {'non_discount': {}, 'discount': {}}
    
    def apply_validation_data_to_po(self, 
                                    df: pd.DataFrame, 
                                    locker_non_discount: Dict[str, dict], 
                                    locker_discount: Dict[str, dict], 
                                    kiosk_data: Dict[str, dict]) -> pd.DataFrame:
        """
        將驗收數據應用到PO DataFrame中
        
        Args:
            df: PO DataFrame
            locker_non_discount: 智取櫃非折扣驗收數據 {PO#: {A:value, B:value, ...}}
            locker_discount: 智取櫃折扣驗收數據 {PO#: {A:value, B:value, ...}}
            kiosk_data: 繳費機驗收數據 {PO#: value}
            
        Returns:
            pd.DataFrame: 更新後的PO DataFrame
        """
        try:
            self.logger.info("開始將驗收數據應用到PO DataFrame")
            
            # 初始化本期驗收數量/金額欄位
            df['本期驗收數量/金額'] = 0
            
            # 獲取供應商配置
            locker_suppliers = self.config_manager.get(self.entity_type, 'locker_suppliers', [])
            kiosk_suppliers = self.config_manager.get(self.entity_type, 'kiosk_suppliers', [])
            
            # 確保PO#欄位存在
            if 'PO#' not in df.columns:
                self.logger.error("DataFrame中缺少PO#欄位")
                return df
            
            # 確保必要欄位存在
            required_columns = ['Item Description', 'PO Supplier']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"DataFrame中缺少必要欄位: {missing_columns}")
                return df
            
            # 處理智取櫃非折扣驗收
            df = self._apply_locker_validation(df, locker_non_discount, locker_suppliers, is_discount=False)
            
            # 處理智取櫃折扣驗收  
            df = self._apply_locker_validation(df, locker_discount, locker_suppliers, is_discount=True)
            
            # 處理繳費機驗收
            df = self._apply_kiosk_validation(df, kiosk_data, kiosk_suppliers)

            df = self.modify_relevant_columns(df)
            
            self.logger.info("成功完成驗收數據應用")
            return df
            
        except Exception as e:
            self.logger.error(f"應用驗收數據時發生錯誤: {str(e)}", exc_info=True)
            return df
    
    def _apply_locker_validation(self, 
                                 df: pd.DataFrame, 
                                 locker_data: Dict[str, dict], 
                                 locker_suppliers: List[str], 
                                 is_discount: bool = False) -> pd.DataFrame:
        """
        應用智取櫃驗收數據
        
        Args:
            df: PO DataFrame
            locker_data: 智取櫃驗收數據 {PO#: {A:value, B:value, ...}}
            locker_suppliers: 智取櫃供應商列表
            is_discount: 是否為折扣驗收
            
        Returns:
            pd.DataFrame: 更新後的DataFrame
        """
        try:
            if not locker_data:
                self.logger.info(f"智取櫃{'折扣' if is_discount else '非折扣'}驗收數據為空")
                return df
            
            # 定義櫃體種類的正則表達式模式
            patterns = {
                # A~K類櫃體，後面非英文字母數字組合，但允許中文字符
                'A': r'locker\s*A(?![A-Za-z0-9])',
                'B': r'locker\s*B(?![A-Za-z0-9])', 
                'C': r'locker\s*C(?![A-Za-z0-9])',
                'D': r'locker\s*D(?![A-Za-z0-9])',
                'E': r'locker\s*E(?![A-Za-z0-9])',
                'F': r'locker\s*F(?![A-Za-z0-9])',
                'G': r'locker\s*G(?![A-Za-z0-9])',
                'H': r'locker\s*H(?![A-Za-z0-9])',
                'I': r'locker\s*I(?![A-Za-z0-9])',
                'J': r'locker\s*J(?![A-Za-z0-9])',
                'K': r'locker\s*K(?![A-Za-z0-9])',
                # DA類（控制主櫃）
                'DA': r'locker\s*控制主[櫃|機]',
                # X類
                'XA': r'locker\s*XA(?![A-Za-z0-9])',
                'XB': r'locker\s*XB(?![A-Za-z0-9])',
                'XC': r'locker\s*XC(?![A-Za-z0-9])',
                'XD': r'locker\s*XD(?![A-Za-z0-9])',
                'XE': r'locker\s*XE(?![A-Za-z0-9])',
                'XF': r'locker\s*XF(?![A-Za-z0-9])',
                # 特殊種類
                '裝運費': r'locker\s*安裝運費',
                '超出櫃體安裝費': r'locker\s*超出櫃體安裝費', 
                '超出櫃體運費': r'locker\s*超出櫃體運費'
            }
            
            processed_count = 0
            
            # 遍歷DataFrame
            for idx, row in df.iterrows():
                try:
                    po_number = row['PO#']
                    item_desc = str(row['Item Description'])
                    po_supplier = str(row['PO Supplier'])
                    
                    # 檢查基本條件
                    if not self._check_locker_conditions(po_number, item_desc, po_supplier, 
                                                         locker_data, locker_suppliers, is_discount):
                        continue
                    
                    # 提取櫃體種類
                    cabinet_type = self._extract_cabinet_type(item_desc, patterns)
                    
                    if cabinet_type and po_number in locker_data:
                        # 獲取對應的驗收數據
                        po_validation_data = locker_data[po_number]
                        
                        if cabinet_type in po_validation_data:
                            # 檢查是否已經有映射值，避免覆蓋
                            current_value = df.at[idx, '本期驗收數量/金額']
                            if current_value == 0:  # 只有當前值為0時才設置新值
                                validation_value = po_validation_data[cabinet_type]
                                df.at[idx, '本期驗收數量/金額'] = validation_value
                                processed_count += 1
                                
                                self.logger.debug(f"PO#{po_number} 櫃體類型{cabinet_type} "
                                                  f"{'折扣' if is_discount else '非折扣'}驗收: {validation_value}")
                            else:
                                self.logger.debug(f"PO#{po_number} 櫃體類型{cabinet_type} "
                                                  f"{'折扣' if is_discount else '非折扣'}驗收已有值({current_value})，跳過設置")
                
                except Exception as e:
                    self.logger.error(f"處理第{idx}行智取櫃數據時出錯: {str(e)}")
                    continue
            
            self.logger.info(f"智取櫃{'折扣' if is_discount else '非折扣'}驗收處理完成，共處理 {processed_count} 筆記錄")
            return df
            
        except Exception as e:
            self.logger.error(f"應用智取櫃驗收數據時發生錯誤: {str(e)}", exc_info=True)
            return df
    
    def _check_locker_conditions(self, 
                                 po_number: str, 
                                 item_desc: str, 
                                 po_supplier: str, 
                                 locker_data: Dict[str, dict], 
                                 locker_suppliers: List[str], 
                                 is_discount: bool) -> bool:
        """
        檢查智取櫃處理條件
        
        Args:
            po_number: PO編號
            item_desc: 項目描述
            po_supplier: PO供應商
            locker_data: 智取櫃驗收數據
            locker_suppliers: 智取櫃供應商列表
            is_discount: 是否為折扣驗收
            
        Returns:
            bool: 是否符合條件
        """
        try:
            # 檢查PO#是否在字典keys中
            if po_number not in locker_data:
                return False
            
            # 檢查Item Description是否包含"門市智取櫃"
            if '門市智取櫃' not in item_desc:
                return False
            
            # 對於非折扣驗收，檢查是否不包含"減價"
            if not is_discount and '減價' in item_desc:
                return False
            
            # 檢查PO Supplier是否在配置的suppliers中
            if locker_suppliers and po_supplier not in locker_suppliers:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"檢查智取櫃條件時出錯: {str(e)}")
            return False
    
    def _extract_cabinet_type(self, item_desc: str, patterns: Dict[str, str]) -> Optional[str]:
        """
        從Item Description中提取櫃體種類
        
        Args:
            item_desc: 項目描述
            patterns: 正則表達式模式字典
            
        Returns:
            Optional[str]: 提取到的櫃體種類，若未找到則返回None
        """
        try:
            import re
            
            # 按照優先級順序檢查模式（特殊類型優先，避免誤匹配）
            priority_order = ['DA', '裝運費', '超出櫃體安裝費', '超出櫃體運費', 
                              'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
                              'XA', 'XB', 'XC', 'XD', 'XE', 'XF']
            
            for cabinet_type in priority_order:
                if cabinet_type in patterns:
                    pattern = patterns[cabinet_type]
                    if re.search(pattern, item_desc, re.IGNORECASE):
                        return cabinet_type
            
            return None
            
        except Exception as e:
            self.logger.error(f"提取櫃體種類時出錯: {str(e)}")
            return None
    
    def _apply_kiosk_validation(self, 
                                df: pd.DataFrame, 
                                kiosk_data: Dict[str, dict], 
                                kiosk_suppliers: List[str]) -> pd.DataFrame:
        """
        應用繳費機驗收數據
        
        Args:
            df: PO DataFrame
            kiosk_data: 繳費機驗收數據 {PO#: value}
            kiosk_suppliers: 繳費機供應商列表
            
        Returns:
            pd.DataFrame: 更新後的DataFrame
        """
        try:
            if not kiosk_data:
                self.logger.info("繳費機驗收數據為空")
                return df
            
            processed_count = 0
            
            # 遍歷DataFrame
            for idx, row in df.iterrows():
                try:
                    po_number = row['PO#']
                    item_desc = str(row['Item Description'])
                    po_supplier = str(row['PO Supplier'])
                    
                    # 檢查條件
                    if not self._check_kiosk_conditions(po_number, item_desc, po_supplier, 
                                                        kiosk_data, kiosk_suppliers):
                        continue
                    
                    # 檢查是否已經有映射值，避免覆蓋
                    current_value = df.at[idx, '本期驗收數量/金額']
                    if current_value == 0:  # 只有當前值為0時才設置新值
                        validation_value = kiosk_data[po_number]
                        df.at[idx, '本期驗收數量/金額'] = validation_value
                        processed_count += 1
                        
                        self.logger.debug(f"PO#{po_number} 繳費機驗收: {validation_value}")
                    else:
                        self.logger.debug(f"PO#{po_number} 繳費機驗收已有值({current_value})，跳過設置")
                
                except Exception as e:
                    self.logger.error(f"處理第{idx}行繳費機數據時出錯: {str(e)}")
                    continue
            
            self.logger.info(f"繳費機驗收處理完成，共處理 {processed_count} 筆記錄")
            return df
            
        except Exception as e:
            self.logger.error(f"應用繳費機驗收數據時發生錯誤: {str(e)}", exc_info=True)
            return df
    
    def _check_kiosk_conditions(self, 
                                po_number: str, 
                                item_desc: str, 
                                po_supplier: str, 
                                kiosk_data: Dict[str, dict], 
                                kiosk_suppliers: List[str]) -> bool:
        """
        檢查繳費機處理條件
        
        Args:
            po_number: PO編號
            item_desc: 項目描述
            po_supplier: PO供應商
            kiosk_data: 繳費機驗收數據
            kiosk_suppliers: 繳費機供應商列表
            
        Returns:
            bool: 是否符合條件
        """
        try:
            # 檢查PO#是否在字典keys中
            if po_number not in kiosk_data:
                return False
            
            # 檢查Item Description是否包含"門市繳費機"
            if '門市繳費機' not in item_desc:
                return False
            
            # 檢查PO Supplier是否在配置的suppliers中
            if kiosk_suppliers and po_supplier not in kiosk_suppliers:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"檢查繳費機條件時出錯: {str(e)}")
            return False

    def modify_relevant_columns(self, df):
        df_copy = df.copy()
        
        need_to_accrual = df_copy['本期驗收數量/金額'] != 0
        df_copy.loc[need_to_accrual, '是否估計入帳'] = 'Y'
            
        # 設置Account code
        df_copy.loc[need_to_accrual, 'Account code'] = self.config_manager.get_fa_accounts(self.entity_type)[0]
        
        # 設置Account Name
        df_copy.loc[need_to_accrual, 'Account Name'] = 'AP,FA Clear Account'
        
        # 設置Product code - SPX固定值
        df_copy.loc[need_to_accrual, 'Product code'] = df_copy.loc[need_to_accrual, 'Product Code']
        
        # 設置Region_c - SPX固定值
        df_copy.loc[need_to_accrual, 'Region_c'] = "TW"
        
        # 設置Dep.
        df_copy.loc[need_to_accrual, 'Dep.'] = '000'
        
        # 設置Currency_c
        df_copy.loc[need_to_accrual, 'Currency_c'] = df_copy.loc[need_to_accrual, 'Currency']
        
        # 設置Accr. Amount
        df_copy['temp_amount'] = (
            df_copy['Unit Price'].astype(float) * df_copy['本期驗收數量/金額'].fillna(0).astype(float)
        )
        non_shipping = ~df_copy['Item Description'].str.contains('運費|安裝費')
        df_copy.loc[need_to_accrual & non_shipping, 'Accr. Amount'] = \
            df_copy.loc[need_to_accrual & non_shipping, 'temp_amount']
        df_copy.loc[need_to_accrual & ~non_shipping, 'Accr. Amount'] = \
            df_copy.loc[need_to_accrual & ~non_shipping, '本期驗收數量/金額']
        df_copy.drop('temp_amount', axis=1, inplace=True)
        
        # 設置是否有預付
        is_prepayment = df_copy['Entry Prepay Amount'] != '0'
        df_copy.loc[need_to_accrual & is_prepayment, '是否有預付'] = 'Y'

        # 設置Liability
        df_copy.loc[need_to_accrual, 'Liability'] = '200414'
        return df_copy

    def _is_valid_data(self, data) -> bool:
        """檢查數據是否有效
        
        Args:
            data: 要檢查的數據
            
        Returns:
            bool: 數據是否有效
        """
        try:
            if data is None:
                return False
            
            # 檢查 DataFrame
            if isinstance(data, pd.DataFrame):
                return not data.empty
            
            # 檢查 Series
            if isinstance(data, pd.Series):
                return not data.empty
            
            # 檢查列表或元組
            if isinstance(data, (list, tuple)):
                return len(data) > 0
            
            # 檢查字典
            if isinstance(data, dict):
                return len(data) > 0
            
            # 其他情況，假設非None即有效
            return True
            
        except Exception as e:
            self.logger.error(f"檢查數據有效性時出錯: {str(e)}", exc_info=True)
            return False
    
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
        
    def _save_output(self, df: pd.DataFrame, original_filename: str) -> Optional[str]:
        """保存輸出檔案"""
        try:
            
            # 清理DataFrame中的<NA>值
            df_export = df.replace('<NA>', np.nan)
            # 生成輸出檔案名稱
            base_name = os.path.splitext(original_filename)[0]
            date = base_name[:7]
            output_filename = f"{base_name}_processed_{self.entity_type}.xlsx"
            
            # 創建輸出目錄
            output_dir = "output"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            output_path = os.path.join(output_dir, output_filename)
            
            # 確保文件名唯一
            output_path = get_unique_filename(os.path.dirname(output_path) or '.', 
                                              os.path.basename(output_path))
            # 保存Excel檔案
            df_export.to_excel(output_path, index=False)
            
            self.logger.info(f"輸出檔案已保存: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"保存輸出檔案失敗: {e}")
            return None
        
    def is_installment(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        mask1 = df_copy['Item Description'].str.contains('裝修')
        mask2 = df_copy['Item Description'].str.contains('第[一|二|三]期款項')

        conditions = [
            (mask1 & mask2),      # Condition for 'Installment'
            (mask1)               # Condition for 'General' (if not an installment)
        ]
        choices = [
            '分期',               # Value if condition 1 is met
            '一般'                # Value if condition 2 is met
        ]

        df_copy['裝修一般/分期'] = np.select(conditions, choices, default=None)
        return df_copy