"""
檔案操作相關工具函數
"""

import os
import sys
import shutil
from pathlib import Path
from typing import List, Optional, Union, Dict, Any, Tuple
import hashlib
import time

from ..config.constants import SUPPORTED_FILE_EXTENSIONS, EXCEL_EXTENSIONS, CSV_EXTENSIONS
from ..logging import get_logger

logger = get_logger('utils.file_utils')


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


def resolve_config_ref_path(ref_path: str) -> str:
    """
    解析 config 參照表路徑，支援 pip install 後的 importlib.resources fallback。

    優先順序：
    1. ref_path 本身存在 → 直接回傳
    2. 從套件內 accrual_bot.config 取得同名檔案

    Args:
        ref_path: 原始路徑（如 "accrual_bot/config/ref_SPTTW.xlsx"）

    Returns:
        str: 可存取的實際路徑
    """
    if ref_path and Path(ref_path).exists():
        return ref_path

    # pip install 後，相對路徑不存在，改從套件資源取得
    try:
        from importlib.resources import files as pkg_files
        filename = Path(ref_path).name if ref_path else ""
        if filename:
            pkg_path = pkg_files("accrual_bot.config").joinpath(filename)
            resolved = str(pkg_path)
            if Path(resolved).exists():
                logger.info(f"從套件資源解析 ref 路徑: {resolved}")
                return resolved
    except Exception:
        pass

    # 都找不到，回傳原始路徑（讓呼叫端處理錯誤）
    return ref_path


def validate_file_path(file_path: str, check_exists: bool = True) -> bool:
    """
    驗證檔案路徑是否有效
    
    Args:
        file_path: 檔案路徑
        check_exists: 是否檢查檔案存在
        
    Returns:
        bool: 是否有效
    """
    if not file_path or not isinstance(file_path, str):
        logger.warning(f"無效的檔案路徑（非字串或空值）: {file_path!r}")
        return False

    try:
        path = Path(file_path)

        # 檢查路徑格式是否有效
        if not path.name:
            logger.warning(f"路徑格式無效（無檔案名稱）: {file_path}")
            return False

        # 檢查檔案是否存在
        if check_exists and not path.exists():
            logger.warning(f"檔案不存在: {file_path}")
            return False

        # 檢查是否為檔案（而非目錄）
        if check_exists and not path.is_file():
            logger.warning(f"路徑非檔案（可能是目錄）: {file_path}")
            return False

        logger.debug(f"路徑驗證通過: {file_path}")
        return True

    except (OSError, ValueError) as e:
        logger.warning(f"驗證路徑時發生例外: {file_path} — {e}")
        return False


def validate_file_extension(file_path: str, allowed_extensions: List[str] = None) -> bool:
    """
    驗證檔案副檔名
    
    Args:
        file_path: 檔案路徑
        allowed_extensions: 允許的副檔名列表，預設為支援的檔案格式
        
    Returns:
        bool: 副檔名是否有效
    """
    if allowed_extensions is None:
        allowed_extensions = SUPPORTED_FILE_EXTENSIONS
    
    try:
        path = Path(file_path)
        return path.suffix.lower() in [ext.lower() for ext in allowed_extensions]
    except (AttributeError, OSError):
        return False


def get_file_extension(file_path: str) -> str:
    """
    獲取檔案副檔名
    
    Args:
        file_path: 檔案路徑
        
    Returns:
        str: 副檔名（包含點號）
    """
    try:
        return Path(file_path).suffix.lower()
    except (AttributeError, OSError):
        return ''


def is_excel_file(file_path: str) -> bool:
    """
    檢查是否為Excel檔案
    
    Args:
        file_path: 檔案路徑
        
    Returns:
        bool: 是否為Excel檔案
    """
    return validate_file_extension(file_path, EXCEL_EXTENSIONS)


def is_csv_file(file_path: str) -> bool:
    """
    檢查是否為CSV檔案
    
    Args:
        file_path: 檔案路徑
        
    Returns:
        bool: 是否為CSV檔案
    """
    return validate_file_extension(file_path, CSV_EXTENSIONS)


def ensure_directory_exists(directory_path: str) -> bool:
    """
    確保目錄存在，如不存在則創建
    
    Args:
        directory_path: 目錄路徑
        
    Returns:
        bool: 操作是否成功
    """
    try:
        path = Path(directory_path)
        if path.exists():
            logger.debug(f"目錄已存在: {directory_path}")
        else:
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"已建立目錄: {directory_path}")
        return True
    except OSError as e:
        logger.error(f"建立目錄失敗: {directory_path} — {e}")
        return False


def get_safe_filename(filename: str, max_length: int = 255) -> str:
    """
    獲取安全的檔案名稱（移除特殊字符）
    
    Args:
        filename: 原始檔案名
        max_length: 最大長度
        
    Returns:
        str: 安全的檔案名
    """
    # 移除或替換不安全的字符
    unsafe_chars = '<>:"/\\|?*'
    safe_filename = filename
    
    for char in unsafe_chars:
        safe_filename = safe_filename.replace(char, '_')
    
    # 移除開頭和結尾的空格和點號
    safe_filename = safe_filename.strip(' .')
    
    # 限制長度
    if len(safe_filename) > max_length:
        name, ext = os.path.splitext(safe_filename)
        safe_filename = name[:max_length - len(ext)] + ext
    
    return safe_filename


def get_unique_filename(base_path: str, filename: str) -> str:
    """
    獲取唯一的檔案名稱（如果檔案已存在，則添加數字後綴）
    
    Args:
        base_path: 基礎路徑
        filename: 檔案名稱
        
    Returns:
        str: 唯一的檔案完整路徑
    """
    base_dir = Path(base_path)
    file_path = base_dir / filename
    
    if not file_path.exists():
        return str(file_path)
    
    name_part, ext_part = os.path.splitext(filename)
    counter = 1
    
    while True:
        new_filename = f"{name_part}_{counter}{ext_part}"
        new_file_path = base_dir / new_filename
        
        if not new_file_path.exists():
            return str(new_file_path)
        
        counter += 1
        
        # 防止無限循環
        if counter > 9999:
            # 使用時間戳確保唯一性
            timestamp = str(int(time.time()))
            new_filename = f"{name_part}_{timestamp}{ext_part}"
            return str(base_dir / new_filename)


def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    獲取檔案信息
    
    Args:
        file_path: 檔案路徑
        
    Returns:
        Dict[str, Any]: 檔案信息字典
    """
    try:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"取得檔案資訊失敗，路徑不存在: {file_path}")
            return {}
        stat = path.stat()
        info = {
            'name': path.name,
            'stem': path.stem,
            'suffix': path.suffix,
            'size': stat.st_size,
            'size_mb': round(stat.st_size / 1024 / 1024, 2),
            'created_time': stat.st_ctime,
            'modified_time': stat.st_mtime,
            'is_file': path.is_file(),
            'is_dir': path.is_dir(),
            'exists': True,
            'absolute_path': str(path.absolute())
        }
        logger.debug(f"取得檔案資訊成功: {file_path} ({info['size_mb']} MB)")
        return info
    except (OSError, AttributeError) as e:
        logger.warning(f"取得檔案資訊時發生例外: {file_path} — {e}")
        return {}


def calculate_file_hash(file_path: str, algorithm: str = 'md5') -> Optional[str]:
    """
    計算檔案雜湊值
    
    Args:
        file_path: 檔案路徑
        algorithm: 雜湊算法 ('md5', 'sha1', 'sha256')
        
    Returns:
        Optional[str]: 雜湊值，如果失敗則返回None
    """
    try:
        hash_obj = hashlib.new(algorithm)
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_obj.update(chunk)
        
        return hash_obj.hexdigest()
    except (OSError, ValueError):
        return None


def copy_file_safely(src_path: str, dst_path: str, overwrite: bool = False) -> bool:
    """
    安全地複製檔案
    
    Args:
        src_path: 來源檔案路徑
        dst_path: 目標檔案路徑
        overwrite: 是否覆蓋現有檔案
        
    Returns:
        bool: 操作是否成功
    """
    try:
        src = Path(src_path)
        dst = Path(dst_path)

        # 檢查來源檔案是否存在
        if not src.exists():
            logger.warning(f"複製失敗，來源檔案不存在: {src_path}")
            return False

        # 檢查目標檔案是否已存在
        if dst.exists() and not overwrite:
            logger.warning(f"複製失敗，目標檔案已存在且未允許覆蓋: {dst_path}")
            return False

        # 確保目標目錄存在
        dst.parent.mkdir(parents=True, exist_ok=True)

        # 複製檔案
        shutil.copy2(src, dst)
        logger.info(f"檔案複製成功: {src_path} → {dst_path}")
        return True

    except (OSError, shutil.Error) as e:
        logger.error(f"複製檔案時發生例外: {src_path} → {dst_path} — {e}")
        return False


def move_file_safely(src_path: str, dst_path: str, overwrite: bool = False) -> bool:
    """
    安全地移動檔案
    
    Args:
        src_path: 來源檔案路徑
        dst_path: 目標檔案路徑
        overwrite: 是否覆蓋現有檔案
        
    Returns:
        bool: 操作是否成功
    """
    try:
        src = Path(src_path)
        dst = Path(dst_path)
        
        # 檢查來源檔案是否存在
        if not src.exists():
            return False
        
        # 檢查目標檔案是否已存在
        if dst.exists() and not overwrite:
            return False
        
        # 確保目標目錄存在
        dst.parent.mkdir(parents=True, exist_ok=True)
        
        # 移動檔案
        shutil.move(str(src), str(dst))
        return True
        
    except (OSError, shutil.Error):
        return False


def cleanup_temp_files(temp_dir: str, max_age_hours: int = 24) -> int:
    """
    清理臨時檔案
    
    Args:
        temp_dir: 臨時目錄路徑
        max_age_hours: 檔案最大保留時間（小時）
        
    Returns:
        int: 清理的檔案數量
    """
    try:
        temp_path = Path(temp_dir)
        if not temp_path.exists():
            return 0
        
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        cleaned_count = 0
        
        for file_path in temp_path.iterdir():
            if file_path.is_file():
                file_age = current_time - file_path.stat().st_mtime
                
                if file_age > max_age_seconds:
                    try:
                        file_path.unlink()
                        cleaned_count += 1
                    except OSError:
                        continue
        
        return cleaned_count
        
    except OSError:
        return 0


def find_files_by_pattern(directory: str, pattern: str, recursive: bool = True) -> List[str]:
    """
    根據模式尋找檔案
    
    Args:
        directory: 搜尋目錄
        pattern: 檔案模式（支援萬用字符）
        recursive: 是否遞歸搜尋
        
    Returns:
        List[str]: 符合條件的檔案路徑列表
    """
    try:
        dir_path = Path(directory)
        if not dir_path.exists():
            return []
        
        if recursive:
            files = dir_path.rglob(pattern)
        else:
            files = dir_path.glob(pattern)
        
        return [str(f) for f in files if f.is_file()]
        
    except OSError:
        return []


def load_toml(path: str) -> Dict[str, Any]:
    """
    載入 TOML 配置檔案

    Args:
        path: TOML 檔案路徑

    Returns:
        Dict[str, Any]: 配置字典，載入失敗時回傳空字典
    """
    import tomllib
    try:
        toml_path = Path(path)
        if not toml_path.exists():
            logger.warning(f"TOML 檔案不存在: {path}")
            return {}
        with open(toml_path, 'rb') as f:
            data = tomllib.load(f)
        logger.debug(f"已載入 TOML 配置: {path}")
        return data
    except Exception as e:
        logger.error(f"載入 TOML 檔案失敗: {path} — {e}")
        return {}


def get_directory_size(directory: str) -> Tuple[int, int]:
    """
    獲取目錄大小和檔案數量
    
    Args:
        directory: 目錄路徑
        
    Returns:
        Tuple[int, int]: (總大小位元組, 檔案數量)
    """
    try:
        dir_path = Path(directory)
        if not dir_path.exists():
            return 0, 0
        
        total_size = 0
        file_count = 0
        
        for file_path in dir_path.rglob('*'):
            if file_path.is_file():
                total_size += file_path.stat().st_size
                file_count += 1
        
        return total_size, file_count
        
    except OSError:
        return 0, 0
