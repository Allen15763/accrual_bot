"""
配置管理器
提供統一的配置加載和管理功能
"""

import os
import sys
import configparser
import logging
import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import tomllib


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


def resolve_flexible_path(relative_path: str, reference_file: str = None) -> Optional[str]:
    """
    彈性解析路徑，支援不同執行目錄
    
    Args:
        relative_path: 相對路徑 (如 './secret/credentials.json')
        reference_file: 參考文件路徑 (如 __file__)
        
    Returns:
        Optional[str]: 解析成功的完整路徑，失敗則返回None
    """
    # 移除路徑開頭的 './' 
    clean_path = relative_path.lstrip('./')
    
    # 候選路徑列表 (按優先級排序)
    candidate_paths = []
    
    # 1. 基於當前工作目錄的路徑
    candidate_paths.append(os.path.join(os.getcwd(), clean_path))
    
    # 2. 基於當前工作目錄下的 accrual_bot 子目錄
    accrual_bot_cwd = os.path.join(os.getcwd(), 'accrual_bot', clean_path)
    candidate_paths.append(accrual_bot_cwd)
    
    # 3. 如果提供了參考文件，基於參考文件的目錄
    if reference_file:
        ref_dir = Path(reference_file).parent
        # 往上找到 accrual_bot 目錄
        while ref_dir.name != 'accrual_bot' and ref_dir.parent != ref_dir:
            ref_dir = ref_dir.parent
        
        if ref_dir.name == 'accrual_bot':
            candidate_paths.append(str(ref_dir / clean_path))
    
    # 4. 基於腳本文件位置推算的 accrual_bot 目錄
    if reference_file:
        script_dir = Path(reference_file).parent.parent.parent  # 從 utils/config/ 往上三層
        candidate_paths.append(str(script_dir / clean_path))
    
    # 5. 嘗試常見的專案結構路徑
    common_patterns = [
        # 如果在 prpo_bot_renew_v2 目錄執行
        os.path.join(os.getcwd(), 'accrual_bot', clean_path),
        # 如果在 accrual_bot 目錄執行  
        os.path.join(os.getcwd(), clean_path),
        # 如果在上級目錄執行
        os.path.join(os.getcwd(), '..', 'accrual_bot', clean_path),
    ]
    candidate_paths.extend(common_patterns)
    
    # 去除重複路徑
    unique_paths = []
    seen_paths = set()
    for path in candidate_paths:
        normalized = os.path.normpath(path)
        if normalized not in seen_paths:
            unique_paths.append(normalized)
            seen_paths.add(normalized)
    
    # 依序檢查每個候選路徑
    for path in unique_paths:
        if os.path.exists(path) and os.path.isfile(path):
            return os.path.abspath(path)
    
    return None


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
        self._simple_logger = None  # 簡單日誌記錄器
        self._setup_simple_logger()  # 先設置簡單日誌
        self._load_config()
        self._initialized = True
    
    def _setup_simple_logger(self) -> None:
        """
        設置簡單的日誌記錄器，避免循環導入
        使用Python內建的logging模組，不依賴自定義的日誌系統
        """
        self._simple_logger = logging.getLogger('config_manager')
        self._simple_logger.setLevel(logging.INFO)
        
        # 避免重複添加處理器
        if not self._simple_logger.handlers:
            # 控制台處理器
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self._simple_logger.addHandler(console_handler)
            
            # 檔案處理器 - 使用固定路徑，避免依賴配置
            try:
                # 使用專案根目錄下的 logs 資料夾
                log_dir = Path(__file__).cwd() / 'logs'
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"stdout [{timestamp}] INFO ConfigManager: log_dir: {log_dir}", file=sys.stdout)
                log_dir.mkdir(parents=True, exist_ok=True)
                
                log_file = log_dir / f"config_manager_{datetime.datetime.now().strftime('%Y%m%d')}.log"
                
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(logging.DEBUG)
                file_handler.setFormatter(formatter)
                self._simple_logger.addHandler(file_handler)
                
            except Exception as err:
                # 檔案日誌失敗不影響整體運作
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"stderr [{timestamp}] ERROR ConfigManager: {err}", file=sys.stderr)
                pass
            # 避免向上傳播，防止重複輸出
            self._simple_logger.propagate = False
    
    def _load_config(self) -> None:
        """
        加載配置檔案 - 改進版本，支援更靈活的路徑解析
        
        此方法實現了一套漸進式的配置檔案搜尋策略，能夠適應不同的執行環境和專案結構。
        它會按照優先級順序嘗試多個可能的配置檔案路徑，直到找到有效的配置檔案或回退到預設配置。
        
        路徑解析策略:
            1. 打包環境（PyInstaller）: 使用 sys._MEIPASS 路徑
            2. 開發環境: 按以下優先級順序搜尋
                - 基於當前檔案位置推算: ../../../config/config.ini
                - 基於當前工作目錄: ./accrual_bot/config/config.ini  
                - 直接從當前工作目錄: ./config/config.ini
                - 使用彈性路徑解析函數: resolve_flexible_path()
        
        功能特性:
            - 支援 PyInstaller 打包環境和開發環境
            - 多路徑搜尋策略，提高配置檔案發現成功率
            - 完善的錯誤處理和日誌記錄
            - 自動回退到預設配置機制
            - UTF-8 編碼支援
            - 路徑標準化和去重處理
        
        執行流程:
            1. 檢測執行環境（打包 vs 開發）
            2. 根據環境確定配置檔案搜尋路徑列表
            3. 按優先級順序嘗試每個路徑
            4. 找到有效配置檔案後進行載入和轉換
            5. 如果所有路徑都失敗，使用預設配置
            6. 記錄成功載入或失敗資訊
        
        Args:
            無參數
            
        Returns:
            None: 此方法沒有返回值，但會更新以下實例屬性：
                - self._config: configparser.ConfigParser 物件
                - self._config_data: 轉換後的字典格式配置數據
        
        Raises:
            不會向外拋出異常，所有異常都會被捕獲並處理：
                - FileNotFoundError: 配置檔案不存在時自動回退到預設配置
                - UnicodeDecodeError: 檔案編碼問題時記錄錯誤並使用預設配置
                - configparser.Error: 配置檔案格式錯誤時使用預設配置
                - Exception: 其他未預期的異常會被記錄並使用預設配置
        
        Side Effects:
            - 修改 self._config 和 self._config_data 實例屬性
            - 向 sys.stderr 輸出錯誤訊息
            - 向標準輸出打印成功載入訊息
            - 如果找不到配置檔案，會調用 self._set_default_config()
            - 會調用 self._convert_to_dict() 進行數據格式轉換
        
        Usage Examples:
            ```python
            # 在 ConfigManager.__init__ 中調用
            config_manager = ConfigManager()
            # _load_config() 會自動被調用，無需手動調用
            
            # 如果需要重新載入配置
            config_manager._load_config()
            ```
        
        Path Resolution Priority (開發環境):
            1. Path(__file__).parent.parent.parent / 'config' / 'config.ini'
            相對於當前檔案的標準專案結構路徑
            
            2. Path.cwd() / 'accrual_bot' / 'config' / 'config.ini' 
            從工作目錄下的 accrual_bot 子目錄尋找
            
            3. Path.cwd() / 'config' / 'config.ini'
            直接從工作目錄的 config 子目錄尋找
            
            4. resolve_flexible_path('config/config.ini', __file__)
            使用彈性路徑解析函數作為備選方案
        
        Notes:
            - 此方法是私有方法，僅供 ConfigManager 內部使用
            - 配置檔案必須是有效的 INI 格式
            - 支援中文配置內容（UTF-8 編碼）
            - 在找不到配置檔案時會輸出嘗試過的所有路徑以供調試
            - 成功載入時會打印配置檔案的完整路徑
            
        Compatibility:
            - Python 3.6+
            - 支援 Windows、macOS、Linux
            - 相容 PyInstaller 打包環境
            - 適用於不同的專案目錄結構
            
        Version History:
            - v1.0: 基本配置載入功能
            - v2.0: 新增多路徑搜尋和彈性路徑解析支援
        """
        try:
            # 確定配置檔案路徑
            if getattr(sys, 'frozen', False):
                base_dir = sys._MEIPASS
                config_path = os.path.join(base_dir, 'config.ini')
            else:
                # 開發環境，嘗試多個可能的路徑
                possible_paths = [
                    # 1. 基於當前檔案位置推算的路徑
                    Path(__file__).parent.parent.parent / 'config' / 'config.ini',
                    # 2. 基於當前工作目錄的路徑
                    Path.cwd() / 'accrual_bot' / 'config' / 'config.ini',
                    # 3. 直接從當前工作目錄
                    Path.cwd() / 'config' / 'config.ini',
                    # 4. 使用彈性路徑解析(詳下)
                ]
                
                # 使用彈性路徑解析作為備選
                flexible_path = resolve_flexible_path('config/config.ini', __file__)
                if flexible_path:
                    possible_paths.append(Path(flexible_path))
                
                config_path = None
                for path in possible_paths:
                    if path.exists() and path.is_file():
                        config_path = str(path.absolute())
                        break
            
            if not config_path or not os.path.exists(config_path):
                # 如果還是找不到，記錄所有嘗試的路徑並使用預設配置
                attempted_paths = [str(p) for p in possible_paths] if 'possible_paths' in locals() else [config_path]
                self._log_warning(f"配置檔案不存在，嘗試過的路徑: {attempted_paths}，嘗試讀取zip內ini檔")

                try:
                    import zipfile

                    root_url = r'C:\SEA\Accrual\prpo_bot\prpo_bot_renew_v2\accrual_bot.zip'  # 硬編碼到原始package url
                    ini_in_zip_path = 'accrual_bot/config/config.ini'                       # 固定，除非專案架構異動
                    # 1. 打開 ZIP 檔案
                    with zipfile.ZipFile(root_url, 'r') as zf:
                        # 2. 檢查 ini_in_zip_path 是否存在於 ZIP 檔案中
                        if ini_in_zip_path in zf.namelist():
                            # 3. 從 ZIP 檔案中讀取 ini 文件的內容
                            # zf.read(ini_in_zip_path) 會返回字節數據
                            ini_bytes = zf.read(ini_in_zip_path)

                            # 4. 將字節數據轉換為字符串（需要解碼）
                            # 然後使用 io.StringIO 將字符串包裝成一個類文件對象
                            # configparser.read_string() 或 configparser.read_file() 都可以處理
                            ini_string = ini_bytes.decode('utf-8')

                            # 使用 read_string 直接從字符串讀取
                            self._config.read_string(ini_string)
                            # 或者使用 io.StringIO 模擬文件對象給 read_file
                            # config_file_like = io.StringIO(ini_string)
                            # _config.read_file(config_file_like)
                            self._convert_to_dict()
                            
                            self._log_info(f"成功從 '{ini_in_zip_path}' 讀取配置。")
                            return
                        else:
                            self._log_info(f"錯誤：ZIP 檔案中找不到 '{ini_in_zip_path}'。")
                            self._log_info(f"ZIP 檔案內容列表：{zf.namelist()}")

                except FileNotFoundError:
                    self._log_error(f"錯誤：找不到 ZIP 檔案 '{root_url}'。使用預設配置")
                    self._set_default_config()
                    return
                except zipfile.BadZipFile:
                    self._log_error(f"錯誤：'{root_url}' 不是一個有效的 ZIP 檔案。使用預設配置")
                    self._set_default_config()
                    return
                except UnicodeDecodeError:
                    self._log_error(f"錯誤：無法使用 'utf-8' 解碼 '{ini_in_zip_path}' 的內容，請檢查文件編碼。使用預設配置")
                    self._set_default_config()
                    return
                except Exception as e:
                    self._log_error(f"發生未知錯誤：{e} 使用預設配置")
                    self._set_default_config()
                    return
            
            # 加載配置
            self._config.read(config_path, encoding='utf-8')
            # 轉為字典方面使用; self._config_data
            self._convert_to_dict()

            def get_default_toml_path() -> str:
                file_path = 'stagging.toml'
                current_dir = Path(__file__).parent  # utils/config/

                # 拆解每層路徑
                parts = list(current_dir.parts)
                # 要移除的連續層級
                parts_to_remove = ['utils', 'config']
                # 尋找要移除的層級的起始索引
                try:
                    # 找到 'utils' 的索引
                    index = parts.index(parts_to_remove[0])
                    
                    # 確認 'utils' 的下一個就是 'helpers'
                    if parts[index:index + len(parts_to_remove)] == parts_to_remove:
                        # 從列表中移除這兩個元素
                        del parts[index:index + len(parts_to_remove)]

                        # 重新組合路徑
                        # parts[0] 是磁碟機代號 (例如 'C:\\')
                        # parts[1:] 是後面的所有部分
                        new_path = Path(parts[0]).joinpath(*parts[1:])  # accrual_bot/accrual_bot/
                        self._log_info(f"成功查找套件utils層並重組根目錄: {new_path}")

                except ValueError as e:
                    self._log_error(f"在路徑中找不到 'utils' 層級，無需變更。{e}")
                    new_path = current_dir

                # 構建配置文件的絕對路徑
                config_dir = new_path / 'config'  # accrual_bot/accrual_bot/config/
                file_path = config_dir / file_path
                self._log_info(f"config manager載入的toml路徑: {file_path}")
                return file_path
            with open(get_default_toml_path(), 'rb') as f:
                self._config_toml = tomllib.load(f)
            
            # 記錄成功載入 - 改進的日誌記錄
            self._log_info(f"成功載入配置檔案: {config_path}")
            
        except Exception as e:
            self._log_error(f"載入配置檔案時出錯: {e}")
            self._set_default_config()

    def _log_info(self, message: str) -> None:
        """記錄資訊訊息 - 使用簡單日誌，避免循環導入"""
        if self._simple_logger:
            self._simple_logger.info(message)
        else:
            # 備選：直接輸出到標準輸出
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sys.stdout.write(f"stdout [{timestamp}] INFO ConfigManager: {message}\n")
            sys.stdout.flush()

    def _log_warning(self, message: str) -> None:
        """記錄警告訊息 - 使用簡單日誌，避免循環導入"""
        if self._simple_logger:
            self._simple_logger.warning(message)
        else:
            # 備選：輸出到標準錯誤
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sys.stderr.write(f"stderr [{timestamp}] WARNING ConfigManager: {message}\n")

    def _log_error(self, message: str) -> None:
        """記錄錯誤訊息 - 使用簡單日誌，避免循環導入"""
        if self._simple_logger:
            self._simple_logger.error(message)
        else:
            # 備選：輸出到標準錯誤
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"stderr [{timestamp}] ERROR ConfigManager: {message}", file=sys.stderr)
    
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
                'certificate_path': './secret/credentials.json',
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
        獲取憑證配置，支援彈性路徑解析
        
        Returns:
            Dict[str, Any]: 憑證配置
        """
        cert_path_config = self.get('CREDENTIALS', 'certificate_path')
        
        # 如果是絕對路徑，直接使用
        if os.path.isabs(cert_path_config):
            resolved_cert_path = cert_path_config
        else:
            # 使用彈性路徑解析
            resolved_cert_path = resolve_flexible_path(cert_path_config, __file__)
            
            # 如果解析失敗，fallback 到原始路徑
            if resolved_cert_path is None:
                resolved_cert_path = cert_path_config
                # 記錄警告
                self._log_warning(f"無法解析憑證路徑: {cert_path_config}，使用原始路徑")
        
        return {
            'certificate_path': resolved_cert_path,
            'scopes': self.get_list('CREDENTIALS', 'scopes')
        }
    
    def get_resolved_path(self, section: str, key: str, fallback: str = None) -> Optional[str]:
        """
        獲取解析後的路徑配置
        
        Args:
            section: 配置段落
            key: 配置鍵
            fallback: 預設值
            
        Returns:
            Optional[str]: 解析後的完整路徑
        """
        path_config = self.get(section, key, fallback)
        
        if not path_config:
            return None
            
        # 如果是絕對路徑，直接返回
        if os.path.isabs(path_config):
            return path_config
        
        # 使用彈性路徑解析
        resolved_path = resolve_flexible_path(path_config, __file__)
        return resolved_path or path_config
    
    def reload_config(self) -> None:
        """重新加載配置"""
        self._initialized = False
        self._load_config()
        self._initialized = True


# 全域配置管理器實例
config_manager = ConfigManager()