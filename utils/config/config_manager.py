"""
配置管理器
提供統一的配置加載和管理功能
"""

import os
import sys
import configparser
from typing import Dict, List, Any, Optional, Union
from pathlib import Path


def get_resource_path(relative_path: str) -> str:
    """
    獲取資源檔案路徑，適配打包環境
    
    Args:
        relative_path: 相對路徑
        
    Returns:
        str: 完整路徑
    """
    if hasattr(sys, '_MEIPASS'):
        # PyInstaller 打包環境
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)


class ConfigManager:
    """配置管理器，單例模式"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._config = configparser.ConfigParser()
        self._config_data = {}
        self._load_config()
        self._initialized = True
    
    def _load_config(self) -> None:
        """加載配置檔案"""
        try:
            # 確定配置檔案路徑
            if getattr(sys, 'frozen', False):
                base_dir = sys._MEIPASS
            else:
                # 開發環境，從原始src目錄載入config.ini
                current_dir = Path(__file__).parent.parent.parent.parent.parent
                base_dir = current_dir / 'prpo_bot_renew' / 'src'
            
            config_path = os.path.join(base_dir, 'config.ini')
            
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"配置檔案不存在: {config_path}")
            
            # 加載配置
            self._config.read(config_path, encoding='utf-8')
            
            # 轉換為字典格式便於使用
            self._convert_to_dict()
            
        except Exception as e:
            # 使用stderr記錄錯誤，避免print
            sys.stderr.write(f"載入配置檔案時出錯: {e}\n")
            # 設定預設配置
            self._set_default_config()
    
    def _convert_to_dict(self) -> None:
        """將配置轉換為字典格式"""
        for section_name in self._config.sections():
            self._config_data[section_name] = {}
            for key, value in self._config.items(section_name):
                self._config_data[section_name][key] = value
    
    def _set_default_config(self) -> None:
        """設定預設配置"""
        self._config_data = {
            'GENERAL': {
                'pt_ym': r'(\\d{4}\\/(0[1-9]|1[0-2])(\\s|$))',
                'pt_ymd': r'(\\d{4}\\/(0[1-9]|1[0-2])\\/((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))(\\s|$))',
                'pt_ymtoym': r'(\\d{4}\\/(0[1-9]|1[0-2])[-]\\d{4}\\/(0[1-9]|1[0-2])(\\s|$))',
                'pt_ymdtoymd': r'(\\d{4}\\/(0[1-9]|1[0-2])\\/((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))[-]\\d{4}\\/(0[1-9]|1[0-2])\\/((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))(\\s|$))'
            },
            'LOGGING': {
                'level': 'INFO',
                'format': '%(asctime)s %(levelname)s: %(message)s'
            },
            'CREDENTIALS': {
                'certificate_path': './credentials.json',
                'scopes': 'https://www.googleapis.com/auth/spreadsheets.readonly'
            }
        }
    
    def get(self, section: str, key: str, fallback: Any = None) -> str:
        """
        獲取配置值
        
        Args:
            section: 配置段落名稱
            key: 配置鍵名
            fallback: 預設值
            
        Returns:
            str: 配置值
        """
        try:
            return self._config_data.get(section, {}).get(key, fallback)
        except Exception:
            return fallback
    
    def get_int(self, section: str, key: str, fallback: int = 0) -> int:
        """
        獲取整數配置值
        
        Args:
            section: 配置段落名稱
            key: 配置鍵名
            fallback: 預設值
            
        Returns:
            int: 配置值
        """
        try:
            value = self.get(section, key)
            return int(value) if value is not None else fallback
        except (ValueError, TypeError):
            return fallback
    
    def get_float(self, section: str, key: str, fallback: float = 0.0) -> float:
        """
        獲取浮點數配置值
        
        Args:
            section: 配置段落名稱
            key: 配置鍵名
            fallback: 預設值
            
        Returns:
            float: 配置值
        """
        try:
            value = self.get(section, key)
            return float(value) if value is not None else fallback
        except (ValueError, TypeError):
            return fallback
    
    def get_boolean(self, section: str, key: str, fallback: bool = False) -> bool:
        """
        獲取布林配置值
        
        Args:
            section: 配置段落名稱
            key: 配置鍵名
            fallback: 預設值
            
        Returns:
            bool: 配置值
        """
        try:
            value = self.get(section, key)
            if value is None:
                return fallback
            return value.lower() in ('true', '1', 'yes', 'on')
        except (AttributeError, TypeError):
            return fallback
    
    def get_list(self, section: str, key: str, separator: str = ',', fallback: List = None) -> List[str]:
        """
        獲取列表配置值
        
        Args:
            section: 配置段落名稱
            key: 配置鍵名
            separator: 分隔符
            fallback: 預設值
            
        Returns:
            List[str]: 配置值列表
        """
        if fallback is None:
            fallback = []
            
        try:
            value = self.get(section, key)
            if value is None:
                return fallback
            return [item.strip() for item in value.split(separator) if item.strip()]
        except (AttributeError, TypeError):
            return fallback
    
    def get_section(self, section: str) -> Dict[str, str]:
        """
        獲取整個配置段落
        
        Args:
            section: 配置段落名稱
            
        Returns:
            Dict[str, str]: 配置段落字典
        """
        return self._config_data.get(section, {})
    
    def has_section(self, section: str) -> bool:
        """
        檢查是否存在配置段落
        
        Args:
            section: 配置段落名稱
            
        Returns:
            bool: 是否存在
        """
        return section in self._config_data
    
    def has_option(self, section: str, key: str) -> bool:
        """
        檢查是否存在配置選項
        
        Args:
            section: 配置段落名稱
            key: 配置鍵名
            
        Returns:
            bool: 是否存在
        """
        return section in self._config_data and key in self._config_data[section]
    
    def set_config(self, section: str, key: str, value: str) -> None:
        """
        設定配置值（運行時配置，不會寫入檔案）
        
        Args:
            section: 配置段落名稱
            key: 配置鍵名
            value: 配置值
        """
        if section not in self._config_data:
            self._config_data[section] = {}
        self._config_data[section][key] = value
    
    def get_fa_accounts(self, entity_type: str) -> List[str]:
        """
        獲取FA帳戶列表
        
        Args:
            entity_type: 實體類型 ('mob', 'spt', 'spx')
            
        Returns:
            List[str]: FA帳戶列表
        """
        entity_key = entity_type.lower()
        return self.get_list('FA_ACCOUNTS', entity_key)
    
    def get_pivot_config(self, entity_type: str, data_type: str) -> Dict[str, Any]:
        """
        獲取透視表配置
        
        Args:
            entity_type: 實體類型 ('MOB', 'SPT', 'SPX')
            data_type: 數據類型 ('pr', 'po')
            
        Returns:
            Dict[str, Any]: 透視表配置
        """
        section_name = entity_type.upper()
        config = {}
        
        # 獲取索引配置
        index_key = f'{data_type}_pivot_index'
        config['index'] = self.get_list(section_name, index_key)
        
        # 獲取其他透視表配置
        config['sm_cr_pivot_cols'] = self.get_list(section_name, 'sm_cr_pivot_cols')
        config['ga_cr_pivot_cols'] = self.get_list(section_name, 'ga_cr_pivot_cols')
        config['pivot_value_col'] = self.get(section_name, 'pivot_value_col')
        
        return config
    
    def get_regex_patterns(self) -> Dict[str, str]:
        """
        獲取正規表達式模式
        
        Returns:
            Dict[str, str]: 正規表達式字典
        """
        general_section = self.get_section('GENERAL')
        return {
            'pt_YM': general_section.get('pt_ym'),
            'pt_YMD': general_section.get('pt_ymd'),
            'pt_YMtoYM': general_section.get('pt_ymtoym'),
            'pt_YMDtoYMD': general_section.get('pt_ymdtoymd')
        }
    
    def get_credentials_config(self) -> Dict[str, Any]:
        """
        獲取憑證配置
        
        Returns:
            Dict[str, Any]: 憑證配置
        """
        return {
            'certificate_path': self.get('CREDENTIALS', 'certificate_path'),
            'scopes': self.get_list('CREDENTIALS', 'scopes')
        }
    
    def reload_config(self) -> None:
        """重新加載配置"""
        self._initialized = False
        self._load_config()
        self._initialized = True


# 全域配置管理器實例
config_manager = ConfigManager()
