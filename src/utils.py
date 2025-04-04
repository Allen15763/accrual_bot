import os
import sys
import logging
import configparser
from typing import Tuple, List, Dict, Optional, Union, Any, Callable

import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def resource_path(relative_path):
    """打包環境時，透過 sys._MEIPASS 拼出正確路徑；未打包時則回傳原始路徑。"""
    if hasattr(sys, '_MEIPASS'):
        # sys._MEIPASS 為 PyInstaller 解壓後的臨時資料夾
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)

class ConfigManager:
    """配置管理器，負責加載和提供配置信息"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._config = configparser.ConfigParser()
        
        # 確定配置文件路徑
        if getattr(sys, 'frozen', False):
            base_dir = sys._MEIPASS
        else:
            base_dir = os.path.abspath(os.path.dirname(__file__))
        
        config_path = os.path.join(base_dir, 'config.ini')
        
        # 加載配置
        self._config.read(config_path, encoding='utf-8')
        self._initialized = True
    
    def get(self, section: str, key: str, fallback: Any = None) -> Any:
        """獲取配置值"""
        return self._config.get(section, key, fallback=fallback)
    
    def get_list(self, section: str, key: str, fallback: List = None) -> List:
        """獲取列表類型的配置值"""
        if fallback is None:
            fallback = []
        
        value = self.get(section, key)
        if value:
            # 處理逗號分隔的字符串列表
            if ',' in value:
                return [item.strip() for item in value.split(',')]
            # 處理引號包裹的元組樣式字符串
            elif "'" in value:
                return [item.strip().strip("'") for item in value.split(',')]
            else:
                return [value]
        return fallback


class Logger:
    """日誌管理器，集中處理日誌配置和輸出"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self.config = ConfigManager()
        self._initialized = True
        self._setup_logger()
    
    def _setup_logger(self):

        """配置日誌系統"""
        # 獲取根記錄器
        root_logger = logging.getLogger('')
        
        # 清除現有處理器
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        """配置日誌系統"""
        log_level = self.config.get('LOGGING', 'level', 'INFO')
        log_format = self.config.get('LOGGING', 'format', '%(asctime)s %(levelname)s: %(message)s')
        
        # 設置日誌級別
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        level = level_map.get(log_level, logging.INFO)
        
        # 配置日誌路徑
        log_path = self.config.get('PATHS', 'log_path', 'logs')
        if not os.path.exists(log_path):
            os.makedirs(log_path, exist_ok=True)
        
        # 生成日誌文件名
        from datetime import datetime
        day = datetime.strftime(datetime.now(), '%Y-%m-%d')
        log_name = os.path.join(log_path, f'PRPO_{day}.log')
        
        # 配置日誌
        logging.basicConfig(
            level=level,
            format=log_format,
            filename=log_name,
            filemode='a'
        )
        
        # 添加控制台輸出
        console = logging.StreamHandler()
        console.setLevel(level)
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        root_logger.addHandler(console)
    
    def get_logger(self, name: str = None) -> logging.Logger:
        """獲取命名日誌器"""
        return logging.getLogger(name)


class GoogleSheetsBase:
    def __init__(self, config: Dict[str, Any]):
        self.certificate = resource_path(config.get("certificate_path"))
        self.scopes = config.get("scopes")

    def getData(self, 
                spreadsheet_id: str, 
                year_month: str,  # 改為接收 yyyymm 字串
                range_value: str = None,
                skip_header: bool = True) -> pd.DataFrame:
        """
        讀取 Google Sheets 上的資料。

        Args:
            spreadsheet_id (str): Google Sheet 的 ID。
            year_month (str): yyyymm 格式的字串，用來指定 sheet name (例如 "202310")。
            range_value (str, optional): Sheet 中讀取資料的範圍，例如 "A1:Z100"。
                                       如果為 None，則讀取整個 sheet。 Defaults to None.
            skip_header (bool, optional): 是否跳過第一列(標題列)。 Defaults to True.

        Returns:
            pd.DataFrame: 從 Google Sheets 讀取的資料。
        """

        creds = service_account.Credentials.from_service_account_file(
            self.certificate, scopes=self.scopes)

        try:
            service = build('sheets', 'v4', credentials=creds)

            # 根據 yyyymm 字串建立 sheet name
            sheet_name = year_month
            
            # 組合 range_sheet_name
            if range_value:
                range_sheet_name = f"{sheet_name}!{range_value}"
            else:
                range_sheet_name = sheet_name

            # Call the Sheets API
            sheet = service.spreadsheets()
            # Return a dict, keys:['range', 'majorDimension', 'values']
            result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                        range=range_sheet_name).execute()

            if result.get("values"):
                if skip_header:
                    columns = result['values'][1]
                    data = result['values'][2:]
                else:
                    # 如果不需要 skip header，則使用整組資料的第一個row當做 column name
                    if result['values']:
                        columns = result['values'][0]
                        data = result['values']
                    else:
                        columns = []
                        data = []
                df = pd.DataFrame(data, columns=columns)
            else:
                df = pd.DataFrame()
            
            return df
            
        except HttpError as err:
            print(f"HttpError occurred: {err}")
            return pd.DataFrame()  # 返回空的 DataFrame

        except Exception as err:
            print(f"An unexpected error occurred: {err}")
            return pd.DataFrame()  # 返回空的 DataFrame
        
class ClosingList2025(GoogleSheetsBase):
    def __init__(self, config):
        super().__init__(config)
        self.spreadsheet_id = '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE'
        self.range_sheet_name = '2025年'
        self.range_value = 'A:J'

class ClosingList2024(GoogleSheetsBase):
    def __init__(self, config):
        super().__init__(config)
        self.spreadsheet_id = '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE'
        self.range_sheet_name = '2024年'
        self.range_value = 'A:J'

class ClosingList2023(GoogleSheetsBase):
    def __init__(self, config):
        super().__init__(config)
        self.spreadsheet_id = '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE'
        self.range_sheet_name = '2023年_done'
        self.range_value = 'A:J'


class DataImporter:
    """數據導入器，統一處理各種數據文件的導入"""
    
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        self.config = ConfigManager()
    
    def import_rawdata(self, url: str, name: str) -> Tuple[pd.DataFrame, int]:
        """導入PR數據
        
        Args:
            url: 文件路徑
            name: 文件名
            
        Returns:
            Tuple[pd.DataFrame, int]: 數據框和年月值
        """
        try:
            self.logger.info(f"正在導入PR數據文件: {name}")
            
            if name.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, dtype=str)
                
            df.encoding = 'big5'
            
            # 數據轉換
            df['Line#'] = round(df['Line#'].astype(float), 0).astype(int).astype(str)
            df['GL#'] = np.where(df['GL#'] == 'N.A.', '666666', df['GL#'])
            
            # 從文件名獲取年月
            try:
                ym = int(name[0:6])
            except ValueError:
                self.logger.warning(f"無法從文件名 {name} 獲取年月值，使用默認值0")
                ym = 0
                
            self.logger.info(f"成功導入PR數據, 形狀: {df.shape}")
            return df, ym
            
        except Exception as e:
            self.logger.error(f"導入數據文件 {name} 時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_rawdata_POonly(self, url: str, name: str) -> Tuple[pd.DataFrame, int, int]:
        """導入PO數據
        
        Args:
            url: 文件路徑
            name: 文件名
            
        Returns:
            Tuple[pd.DataFrame, int, int]: 數據框、年月值和月份值
        """
        try:
            self.logger.info(f"正在導入PO數據文件: {name}")
            
            if name.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, dtype=str)
                
            df.encoding = 'big5'
            
            # 數據轉換
            df['Line#'] = round(df['Line#'].astype(float), 0).astype(int).astype(str)
            df['GL#'] = np.where(df['GL#'] == 'N.A.', '666666', df['GL#'])
            df['GL#'] = round(df['GL#'].fillna('666666').astype(float), 0).astype(int).astype(str)
            
            # 從文件名獲取年月和月份
            try:
                ym = int(name[0:6])
                m = int(name[4:6])
            except ValueError:
                self.logger.warning(f"無法從文件名 {name} 獲取年月值，使用默認值")
                ym, m = 0, 0
                
            self.logger.info(f"成功導入PO數據, 形狀: {df.shape}")
            return df, ym, m
            
        except Exception as e:
            self.logger.error(f"導入PO數據文件 {name} 時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_reference_data(self, entity_type: str = 'MOB') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """導入參考數據
        
        Args:
            entity_type: 實體類型，'MOB'或'SPT'
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: 科目參考數據和負債參考數據
        """
        try:
            path_key = f"ref_path_{entity_type.lower()}"
            url = self.config.get('PATHS', path_key)
            
            self.logger.info(f"正在導入 {entity_type} 參考數據: {url}")
            
            ac_ref = pd.read_excel(url, dtype=str)
            
            ref_for_ac = ac_ref.iloc[:, 1:3]
            ref_for_liability = ac_ref.loc[:, ['Account', 'Liability']]
            
            return ref_for_ac, ref_for_liability
            
        except Exception as e:
            self.logger.error(f"導入參考數據時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_procurement(self, url: str) -> pd.DataFrame:
        """導入採購底稿
        
        Args:
            url: 文件路徑
            
        Returns:
            pd.DataFrame: 採購底稿數據
        """
        try:
            self.logger.info(f"正在導入採購底稿(PR): {url}")
            
            if url.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, header=0, dtype=str)
                
            df.encoding = 'big5'
            df['PR Line'] = df['PR#'].astype(str) + "-" + df['Line#'].astype(str)
            
            self.logger.info(f"成功導入採購底稿(PR), 形狀: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"導入採購底稿時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_closing_list(self, url: str) -> List[str]:
        """導入關單清單
        
        Args:
            url: 文件路徑
            
        Returns:
            List[str]: 關單清單項目
        """
        try:
            self.logger.info(f"正在導入關單清單: {url}")
            
            if url.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, dtype=str)
                
            df.encoding = 'big5'
            
            # 2022/3/10 確認只會是A欄 PO# or PR#
            mapping_list = df.iloc[:, 0].tolist()
            unique_list = list(set(mapping_list))
            
            self.logger.info(f"成功導入關單清單, 項目數: {len(unique_list)}")
            return unique_list
            
        except Exception as e:
            self.logger.error(f"導入關單清單時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_previous_wp(self, url: str) -> pd.DataFrame:
        """導入前期底稿
        
        Args:
            url: 文件路徑
            
        Returns:
            pd.DataFrame: 前期底稿數據
        """
        try:
            self.logger.info(f"正在導入前期底稿: {url}")
            
            y = pd.read_excel(url, dtype=str)
            y['Line#'] = round(y['Line#'].astype(float), 0).astype(int).astype(str)
            
            if 'PO#' in y.columns:
                y['PO Line'] = y['PO#'].astype(str) + "-" + y['Line#'].astype(str)
            else:
                y['PR Line'] = y['PR#'].astype(str) + "-" + y['Line#'].astype(str)
                
            self.logger.info(f"成功導入前期底稿, 形狀: {y.shape}")
            return y
            
        except Exception as e:
            self.logger.error(f"導入前期底稿時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_procurement_PO(self, url: str) -> pd.DataFrame:
        """導入採購底稿(PO)
        
        Args:
            url: 文件路徑
            
        Returns:
            pd.DataFrame: 採購底稿數據
        """
        try:
            self.logger.info(f"正在導入採購底稿(PO): {url}")
            
            if url.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, header=0, dtype=str)
                
            df.encoding = 'big5'
            df['Line#'] = round(df['Line#'].astype(float), 0).astype(int).astype(str)
            df['PO Line'] = df['PO#'].astype(str) + "-" + df['Line#'].astype(str)
            
            self.logger.info(f"成功導入採購底稿(PO), 形狀: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"導入採購底稿(PO)時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_closing_list_PO(self, url: str) -> List[str]:
        """導入關單清單(PO)
        
        Args:
            url: 文件路徑
            
        Returns:
            List[str]: 關單清單項目
        """
        try:
            self.logger.info(f"正在導入關單清單(PO): {url}")
            
            if url.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, dtype=str)
                
            df.encoding = 'big5'
            
            mapping_list = df.iloc[:, 0].tolist()
            unique_list = list(set(mapping_list))
            
            self.logger.info(f"成功導入關單清單(PO), 項目數: {len(unique_list)}")
            return unique_list
            
        except Exception as e:
            self.logger.error(f"導入關單清單(PO)時出錯: {str(e)}", exc_info=True)
            raise

    def import_spx_closing_list(self, config: Dict[str, Any]) -> pd.DataFrame:
        """導入SPX關單清單"""
        try:
            self.logger.info("正在導入關單清單(SPX)")
            # 將各年份對應的類別放入串列中
            closing_list_classes = [ClosingList2023, ClosingList2024, ClosingList2025]
            dfs = []
            for cls in closing_list_classes:
                instance = cls(config)
                self.logger.info(f"正在導入 {cls.__name__} 資料")
                df = instance.getData(instance.spreadsheet_id, instance.range_sheet_name, instance.range_value)
                dfs.append(df)
            # 合併所有 DataFrame 並重設索引
            combined_df = pd.concat(dfs, ignore_index=True)
            # 移除 'Date' 欄位為空的列
            combined_df.dropna(subset=['Date'], inplace=True)
            combined_df.rename(columns={'Date': 'date', 'Type': 'type', 'PO Number': 'po_no', 
                                        'Requester': 'requester', 'Supplier': 'supplier',
                                        'Line Number / ALL': 'line_no', 'Reason': 'reason', 
                                        'New PR Number': 'new_pr_no', 'Remark': 'remark', 
                                        'Done(V)': 'done_by_fn'}, inplace=True)
            combined_df = combined_df.query("date!=''").reset_index(drop=True)
            self.logger.info("完成導入關單清單(SPX)")
            return combined_df
        except Exception as e:
            self.logger.error("導入SPX關單清單時出錯: %s", str(e), exc_info=True)
            raise

    def import_ap_invoice(self, url: str, cols: list) -> pd.DataFrame:
        """導入AP invoice"""
        try:
            self.logger.info("正在導入AP invoice(SPX)")
            necessary_cols = cols
            df = pd.read_excel(url, dtype=str, header=1, sheet_name=1, usecols=necessary_cols)
            self.logger.info("完成導入AP invoice(SPX)")
            return df
        except Exception as e:
            self.logger.error("導入AP invoice時出錯: %s", str(e), exc_info=True)
            raise

class ReconEntryAmt:
    """對帳金額比較類"""
    
    def __init__(self, df_pre_acwp: pd.DataFrame, df_cur_procwp: pd.DataFrame, 
                 is_pr: bool, vs_pr: bool):
        """
        檢測如前期會計底稿有人工修改Entry amount，下載當期HRIS時避免用到未修改的amount。
        
        Args:
            df_pre_acwp: 前期會計底稿
            df_cur_procwp: 當期採購底稿
            is_pr: 是否為前期會計底稿之PR, else 前期會計底稿之PR
            vs_pr: 是否比對當期採購底稿之PR, else 比對當期採購底稿之PO
        """
        self.df_pre_acwp = df_pre_acwp
        self.df_cur_procwp = df_cur_procwp
        self.is_pr = is_pr
        self.vs_pr = vs_pr
        self.logger = Logger().get_logger(__name__)
    
    def get_previous_kv(self) -> Dict[str, str]:
        """獲取前期會計底稿的鍵值對"""
        def filter_necessary_rows(df):
            return df['是否估計入帳'] == 'Y'
            
        try:
            if self.is_pr:
                kv = self.df_pre_acwp.loc[filter_necessary_rows(self.df_pre_acwp), 
                                          ['PR Line', 'Accr. Amount_variable']].set_index('PR Line').to_dict('index')
                kv = {k: v['Accr. Amount_variable'] for k, v in kv.items()}
            else:
                kv = self.df_pre_acwp.loc[filter_necessary_rows(self.df_pre_acwp), 
                                          ['PO Line', 'Accr. Amount_variable']].set_index('PO Line').to_dict('index')
                kv = {k: v['Accr. Amount_variable'] for k, v in kv.items()}
                
            return kv
            
        except Exception as e:
            self.logger.error(f"獲取前期會計底稿鍵值對時出錯: {str(e)}", exc_info=True)
            return {}
    
    def get_current_kv(self) -> Dict[str, str]:
        """獲取當期採購底稿的鍵值對"""
        try:
            if self.vs_pr:
                kv = self.df_cur_procwp.loc[:, ['PR Line', 'Entry Amount']].set_index('PR Line').to_dict('index')
                kv = {k: v['Entry Amount'] for k, v in kv.items()}
            else:
                kv = self.df_cur_procwp.loc[:, ['PO Line', 'Entry Amount']].set_index('PO Line').to_dict('index')
                kv = {k: v['Entry Amount'] for k, v in kv.items()}
                
            return kv
            
        except Exception as e:
            self.logger.error(f"獲取當期採購底稿鍵值對時出錯: {str(e)}", exc_info=True)
            return {}
    
    def compare_kv(self) -> Dict[str, int]:
        """比較前期會計底稿和當期採購底稿的金額差異"""
        try:
            if all([self.is_pr, self.vs_pr]):
                # PR vs PR_proc
                kv_ac, kv_proc = self.get_previous_kv(), self.get_current_kv()
                kv_dif = {
                    k: int(float(v.replace(',', ''))) - int(float(kv_proc[k].replace(',', '')))
                    for k, v in kv_ac.items() if k in kv_proc
                }
                
            elif not self.is_pr and not self.vs_pr:
                # PO vs PO_proc
                kv_ac, kv_proc = self.get_previous_kv(), self.get_current_kv()
                kv_dif = {
                    k: int(float(v.replace(',', ''))) - int(float(kv_proc[k].replace(',', '')))
                    for k, v in kv_ac.items() if k in kv_proc
                }
                
            else:
                # PR vs PO_proc
                kv_ac, kv_proc = self.get_previous_kv(), self.get_current_kv()
                kv_proc = {k.replace('PO', 'PR'): v for k, v in kv_proc.items()}
                kv_dif = {
                    k: int(float(v.replace(',', ''))) - int(float(kv_proc[k].replace(',', '')))
                    for k, v in kv_ac.items() if k in kv_proc
                }
                
            self.logger.info(f"金額差異比較完成，找到 {len(kv_dif)} 個差異項")
            return kv_dif
            
        except Exception as e:
            self.logger.error(f"比較金額差異時出錯: {str(e)}", exc_info=True)
            return {}
    
    @staticmethod
    def get_difference(p1: str, p2: str, p3: str) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
        """
        比較三種不同類型的差異
        
        Args:
            p1: 前期會計底稿路徑
            p2: 當期採購底稿PR路徑
            p3: 當期採購底稿PO路徑
            
        Returns:
            Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]: PR vs PR, PR vs PO, PO vs PO 的差異
        """
        logger = Logger().get_logger(__name__)
        
        try:
            logger.info("開始比較差異")
            logger.info(f"前期會計底稿: {p1}")
            logger.info(f"當期採購底稿PR: {p2}")
            logger.info(f"當期採購底稿PO: {p3}")
            
            def read_excel_file(file_path, sheet_name=0):
                return pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
            
            # 讀取文件
            df_ac_pr = read_excel_file(p1, sheet_name='PR')
            df_ac_po = read_excel_file(p1, sheet_name='PO')
            df_proc_pr = read_excel_file(p2)
            df_proc_po = read_excel_file(p3)
            
            # 比較
            pr_v_pr = ReconEntryAmt(df_ac_pr, df_proc_pr, True, True)
            kv_dif_rr = pr_v_pr.compare_kv()
            
            pr_v_po = ReconEntryAmt(df_ac_pr, df_proc_po, True, False)
            kv_dif_ro = pr_v_po.compare_kv()
            
            po_v_oo = ReconEntryAmt(df_ac_po, df_proc_po, False, False)
            kv_dif_oo = po_v_oo.compare_kv()
            
            logger.info("差異比較完成")
            return kv_dif_rr, kv_dif_ro, kv_dif_oo
            
        except Exception as e:
            logger.error(f"比較差異時出錯: {str(e)}", exc_info=True)
            raise


# 最新API，保留向後兼容性，同時提供更清晰的接口
class Utils:
    """兼容舊版API的工具類"""
    
    @staticmethod
    def import_rawdata(url, name):
        """導入原始數據"""
        return DataImporter().import_rawdata(url, name)
    
    @staticmethod
    def import_rawdata_POonly(url, name):
        """僅導入PO數據"""
        return DataImporter().import_rawdata_POonly(url, name)
    
    @staticmethod
    def import_ref_ac_MOB():
        """導入MOB參考數據"""
        return DataImporter().import_reference_data('MOB')
    
    @staticmethod
    def import_ref_ac_SPT():
        """導入SPT參考數據"""
        return DataImporter().import_reference_data('SPT')
    
    @staticmethod
    def import_procurement(url):
        """導入採購底稿"""
        return DataImporter().import_procurement(url)
    
    @staticmethod
    def import_closing_list(url):
        """導入關單清單"""
        return DataImporter().import_closing_list(url)
    
    @staticmethod
    def import_previous_wp(url):
        """導入前期底稿"""
        return DataImporter().import_previous_wp(url)
    
    @staticmethod
    def import_procurement_PO(url):
        """導入採購底稿(PO)"""
        return DataImporter().import_procurement_PO(url)
    
    @staticmethod
    def import_closing_list_PO(url):
        """導入關單清單(PO)"""
        return DataImporter().import_closing_list_PO(url)


import concurrent.futures


class AsyncDataImporter(DataImporter):
    """支持並發讀取的數據導入器，繼承自DataImporter，保留原有功能"""
    
    def __init__(self):
        super().__init__()
        self.max_workers = 5  # 預設並行處理線程數
    
    def set_max_workers(self, max_workers: int):
        """設置最大工作線程數"""
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.logger.warning(f"無效的工作線程數: {max_workers}，使用預設值: {self.max_workers}")
    
    def concurrent_import(self, import_tasks: List[Tuple[Callable, List, Dict]]) -> List[Any]:
        """並發執行多個導入任務
        
        Args:
            import_tasks: 導入任務列表，每個元素是一個元組 (import_func, args, kwargs)
            
        Returns:
            List[Any]: 導入結果列表，順序與導入任務列表相同
        """
        try:
            self.logger.info(f"開始並發執行 {len(import_tasks)} 個導入任務")
            results = [None] * len(import_tasks)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(import_tasks), 
                                                                       self.max_workers)) as executor:
                # 提交所有任務
                futures = []
                for i, (import_func, args, kwargs) in enumerate(import_tasks):
                    future = executor.submit(import_func, *args, **kwargs)
                    futures.append((i, future))
                
                # 處理所有任務結果
                name_1 = 'import_rawdata_POonly'
                name_2 = 'import_rawdata'
                for i, future in futures:
                    try:
                        result = future.result()
                        # 確保特定函數的返回類型一致性
                        if import_func.__name__ == name_1 and isinstance(result, tuple) and len(result) == 3:
                            # import_rawdata_POonly的結果是(df, ym, m)
                            results[i] = result
                        elif import_func.__name__ == name_2 and isinstance(result, tuple) and len(result) == 2:
                            # import_rawdata的結果是(df, ym)
                            results[i] = result
                        else:
                            # 其他函數的結果直接保存
                            results[i] = result
                        self.logger.debug(f"任務 {i} 執行成功: {import_func.__name__}")
                    except Exception as e:
                        self.logger.error(f"任務 {i} 執行出錯: {str(e)}", exc_info=True)
                        results[i] = None
            
            succeeded = sum(1 for v in results if v is not None)
            self.logger.info(f"並發執行完成，成功執行 {succeeded}/{len(import_tasks)} 個任務")
            return results
            
        except Exception as e:
            self.logger.error(f"並發執行導入任務時出錯: {str(e)}", exc_info=True)
            raise
    
    def concurrent_read_files(self, file_types: List[str], file_paths: List[str], **kwargs) -> Dict[str, Any]:
        """並發讀取多個文件
        
        Args:
            file_types: 文件類型列表，例如 ['raw', 'closing', 'previous', 'procurement']
            file_paths: 文件路徑列表，順序與文件類型列表相同
            **kwargs: 其他參數，包括：
                - file_names (Dict[str, str]): 文件名映射
                - config (Dict): SPX關單清單的配置
                - ap_columns (List[str]): AP發票的列名
            
        Returns:
            Dict[str, Any]: 讀取結果字典，鍵是文件類型，值是讀取結果
        """
        try:
            self.logger.info(f"開始並發讀取 {len(file_paths)} 個文件")
            
            if len(file_types) != len(file_paths):
                raise ValueError("文件類型列表和文件路徑列表長度不一致")
            
            # 根據文件類型選擇導入方法
            import_tasks = []
            for i, (file_type, file_path) in enumerate(zip(file_types, file_paths)):
                file_name = kwargs.get('file_names', {}).get(file_type, os.path.basename(file_path))
                
                if file_type == 'raw':
                    import_func = self.import_rawdata
                    import_tasks.append((import_func, [file_path, file_name], {}))
                elif file_type == 'raw_po':
                    import_func = self.import_rawdata_POonly
                    import_tasks.append((import_func, [file_path, file_name], {}))
                elif file_type == 'closing':
                    import_func = self.import_closing_list
                    import_tasks.append((import_func, [file_path], {}))
                elif file_type == 'closing_po':
                    import_func = self.import_closing_list_PO
                    import_tasks.append((import_func, [file_path], {}))
                elif file_type == 'previous':
                    import_func = self.import_previous_wp
                    import_tasks.append((import_func, [file_path], {}))
                elif file_type == 'previous_pr':
                    import_func = self.import_previous_wp
                    import_tasks.append((import_func, [file_path], {}))
                elif file_type == 'procurement_pr':
                    import_func = self.import_procurement
                    import_tasks.append((import_func, [file_path], {}))
                elif file_type == 'procurement_po':
                    import_func = self.import_procurement_PO
                    import_tasks.append((import_func, [file_path], {}))
                elif file_type == 'spx_closing':
                    import_func = self.import_spx_closing_list
                    import_tasks.append((import_func, [kwargs.get('config', {})], {}))
                elif file_type == 'ap_invoice':
                    import_func = self.import_ap_invoice
                    import_tasks.append((import_func, [file_path, kwargs.get('ap_columns', [])], {}))
                else:
                    self.logger.warning(f"未知的文件類型: {file_type}")
                    continue
            
            # 並發執行導入任務
            results = self.concurrent_import(import_tasks)
            
            # 構建結果字典
            result_dict = {}
            for i, file_type in enumerate(file_types):
                if i < len(results):
                    result_dict[file_type] = results[i]
            
            return result_dict
            
        except Exception as e:
            self.logger.error(f"並發讀取文件時出錯: {str(e)}", exc_info=True)
            raise

    def batch_import_reference_data(self, entity_types: List[str]) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        """並發導入多個實體的參考數據
        
        Args:
            entity_types: 實體類型列表，例如 ['MOB', 'SPT']
            
        Returns:
            Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]: 參考數據字典，鍵是實體類型，值是(參考數據, 負債參考數據)的元組
        """
        try:
            self.logger.info(f"開始並發導入 {len(entity_types)} 個實體的參考數據")
            
            import_tasks = []
            for entity_type in entity_types:
                import_tasks.append((self.import_reference_data, [entity_type], {}))
            
            results = self.concurrent_import(import_tasks)
            
            result_dict = {}
            for i, entity_type in enumerate(entity_types):
                if i < len(results) and results[i] is not None:
                    result_dict[entity_type] = results[i]
            
            return result_dict
            
        except Exception as e:
            self.logger.error(f"並發導入參考數據時出錯: {str(e)}", exc_info=True)
            raise

    def optimize_pandas_read(self, file_path: str, is_excel: bool = True, **kwargs) -> pd.DataFrame:
        """優化pandas讀取文件的方法
        
        Args:
            file_path: 文件路徑
            is_excel: 是否為Excel文件，若為False則視為CSV
            **kwargs: 傳遞給pd.read_excel或pd.read_csv的參數
            
        Returns:
            pd.DataFrame: 讀取的數據框
        """
        try:
            # 預設參數
            if is_excel:
                default_kwargs = {
                    'dtype': str,
                    'engine': 'openpyxl'  # 使用openpyxl引擎以便更好的內存管理
                }
            else:
                default_kwargs = {
                    'dtype': str, 
                    'encoding': 'utf-8-sig',
                    'header': 0
                }
            
            # 合併參數
            for k, v in default_kwargs.items():
                if k not in kwargs:
                    kwargs[k] = v
            
            # 讀取文件
            df = pd.read_excel(file_path, **kwargs) if is_excel else pd.read_csv(file_path, **kwargs)
            
            return df
            
        except Exception as e:
            self.logger.error(f"優化讀取文件時出錯: {str(e)}", exc_info=True)
            raise

class AsyncGoogleSheetsBase(GoogleSheetsBase):
    """支持並發讀取的Google Sheets基類"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.max_workers = 5  # 預設工作線程數
    
    def concurrent_get_data(self, queries: List[Tuple[str, str, Optional[str], bool]]) -> List[pd.DataFrame]:
        """並發讀取多個Google Sheets數據
        
        Args:
            queries: 查詢列表，每個元素是一個元組 (spreadsheet_id, year_month, range_value, skip_header)
            
        Returns:
            List[pd.DataFrame]: 數據框列表，順序與查詢列表相同
        """
        try:
            logger = Logger().get_logger(__name__)
            logger.info(f"開始並發讀取 {len(queries)} 個Google Sheets數據")
            
            results = [None] * len(queries)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(queries), self.max_workers)) as executor:
                futures = []
                
                for i, (spreadsheet_id, year_month, range_value, skip_header) in enumerate(queries):
                    future = executor.submit(
                        self.getData, 
                        spreadsheet_id, 
                        year_month, 
                        range_value, 
                        skip_header
                    )
                    futures.append((i, future))
                
                for i, future in futures:
                    try:
                        results[i] = future.result()
                        logger.debug(f"查詢 {i} 執行成功")
                    except Exception as e:
                        logger.error(f"查詢 {i} 執行出錯: {str(e)}", exc_info=True)
                        results[i] = pd.DataFrame()
            
            succeeded = sum(1 for v in results if not v.empty)
            logger.info(f"並發讀取完成，成功讀取 {succeeded}/{len(queries)} 個Google Sheets數據")
            return results
            
        except Exception as e:
            Logger().get_logger(__name__).error(f"並發讀取Google Sheets數據時出錯: {str(e)}", exc_info=True)
            return [pd.DataFrame()] * len(queries)

if __name__ == "__main__":
    # 測試代碼
    # config = {
    #     "certificate_path": "./src/credentials.json",
    #     "scopes": ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    # }
    # gs = GoogleSheetsBase(config)
    # df = gs.getData("1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE", "2025年", "A:J", skip_header=1)
    # print(df)

    # gs = ClosingList2023(config)
    # df = gs.getData(gs.spreadsheet_id, gs.range_sheet_name, gs.range_value)
    # print(df)

    di = DataImporter()
    config_manager = ConfigManager()
    config = {'certificate_path': config_manager.get('CREDENTIALS', 'certificate_path'),
              'scopes': config_manager.get_list('CREDENTIALS', 'scopes')}
    df = di.import_spx_closing_list(config)
    print(df, df.shape)