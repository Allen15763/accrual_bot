"""
File Handler

處理檔案上傳、驗證與暫存管理。
"""

import os
import tempfile
import shutil
from typing import List, Optional, Any
import pandas as pd


class FileHandler:
    """處理檔案上傳與暫存"""

    def __init__(self, temp_dir: Optional[str] = None):
        """
        初始化 FileHandler

        Args:
            temp_dir: 暫存目錄路徑，None 則自動建立
        """
        if temp_dir:
            self.temp_dir = temp_dir
            os.makedirs(temp_dir, exist_ok=True)
        else:
            self.temp_dir = tempfile.mkdtemp(prefix="accrual_bot_ui_")

    def save_uploaded_file(self, uploaded_file: Any, file_key: str) -> str:
        """
        儲存上傳檔案到暫存目錄

        Args:
            uploaded_file: Streamlit UploadedFile 物件
            file_key: 檔案識別 key

        Returns:
            儲存的檔案路徑
        """
        # 建立安全的檔案名稱
        filename = self._sanitize_filename(uploaded_file.name)
        file_path = os.path.join(self.temp_dir, f"{file_key}_{filename}")

        # 寫入檔案
        with open(file_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())

        return file_path

    def validate_file(self, file_path: str, file_key: str) -> List[str]:
        """
        驗證檔案格式

        Args:
            file_path: 檔案路徑
            file_key: 檔案識別 key

        Returns:
            錯誤訊息清單，空列表表示驗證通過
        """
        errors = []

        # 檢查檔案是否存在
        if not os.path.exists(file_path):
            errors.append(f"{file_key}: 檔案不存在")
            return errors

        # 檢查檔案大小
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            errors.append(f"{file_key}: 檔案為空")
            return errors

        # 檢查檔案格式
        try:
            if file_path.endswith('.csv'):
                # 嘗試讀取前 5 行
                pd.read_csv(file_path, nrows=5)
            elif file_path.endswith(('.xlsx', '.xls')):
                # 嘗試讀取前 5 行
                pd.read_excel(file_path, nrows=5)
            else:
                errors.append(f"{file_key}: 不支援的檔案格式")

        except Exception as e:
            errors.append(f"{file_key}: 無法讀取檔案 - {str(e)}")

        return errors

    def validate_all_files(self, file_paths: dict) -> List[str]:
        """
        驗證所有檔案

        Args:
            file_paths: 檔案路徑字典

        Returns:
            錯誤訊息清單
        """
        all_errors = []
        for file_key, file_path in file_paths.items():
            errors = self.validate_file(file_path, file_key)
            all_errors.extend(errors)
        return all_errors

    def get_file_info(self, file_path: str) -> dict:
        """
        獲取檔案資訊

        Args:
            file_path: 檔案路徑

        Returns:
            檔案資訊字典
        """
        if not os.path.exists(file_path):
            return {}

        stat = os.stat(file_path)
        return {
            'size': stat.st_size,
            'size_mb': stat.st_size / (1024 * 1024),
            'modified_time': stat.st_mtime,
            'filename': os.path.basename(file_path),
        }

    def cleanup(self):
        """清理暫存檔案"""
        if os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except Exception as e:
                print(f"清理暫存目錄失敗: {e}")

    def _sanitize_filename(self, filename: str) -> str:
        """
        清理檔案名稱，移除不安全字元

        Args:
            filename: 原始檔案名稱

        Returns:
            清理後的檔案名稱
        """
        # 移除路徑分隔符號
        filename = filename.replace('/', '_').replace('\\', '_')
        # 移除特殊字元
        filename = filename.replace('..', '_')
        return filename

    def __del__(self):
        """解構時清理暫存檔案"""
        # 注意: 在某些情況下可能不需要自動清理
        # 可以根據需要調整
        pass
