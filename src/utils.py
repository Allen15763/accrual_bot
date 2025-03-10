import os
import sys
import logging
import configparser
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any

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
            self.logger.info(f"正在導入數據文件: {name}")
            
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
                
            self.logger.info(f"成功導入數據, 形狀: {df.shape}")
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
            self.logger.info(f"正在導入採購底稿: {url}")
            
            if url.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, header=0, dtype=str)
                
            df.encoding = 'big5'
            df['PR Line'] = df['PR#'].astype(str) + "-" + df['Line#'].astype(str)
            
            self.logger.info(f"成功導入採購底稿, 形狀: {df.shape}")
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
