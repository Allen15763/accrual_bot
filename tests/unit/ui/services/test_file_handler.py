"""
FileHandler 單元測試

測試檔案上傳、驗證、暫存管理功能。
"""

import os
import pytest
from unittest.mock import MagicMock, patch

from accrual_bot.ui.services.file_handler import FileHandler


@pytest.fixture
def handler(tmp_path):
    """建立使用暫存目錄的 FileHandler"""
    return FileHandler(temp_dir=str(tmp_path))


@pytest.fixture
def handler_auto_temp():
    """建立自動暫存目錄的 FileHandler"""
    h = FileHandler()
    yield h
    h.cleanup()


@pytest.fixture
def mock_uploaded_file():
    """模擬 Streamlit UploadedFile 物件"""
    mock = MagicMock()
    mock.name = "test_data.csv"
    mock.getbuffer.return_value = b"col1,col2\na,b\nc,d\n"
    return mock


@pytest.fixture
def sample_csv(tmp_path):
    """建立有效 CSV 測試檔案"""
    path = str(tmp_path / "valid.csv")
    with open(path, 'w', encoding='utf-8') as f:
        f.write("col1,col2\n1,2\n3,4\n")
    return path


@pytest.fixture
def empty_file(tmp_path):
    """建立空檔案"""
    path = str(tmp_path / "empty.csv")
    with open(path, 'w') as f:
        pass
    return path


# =============================================================================
# 初始化測試
# =============================================================================

@pytest.mark.unit
class TestInit:
    """測試 FileHandler 初始化"""

    def test_custom_temp_dir(self, tmp_path):
        """指定暫存目錄時應使用該目錄"""
        custom_dir = str(tmp_path / "custom")
        handler = FileHandler(temp_dir=custom_dir)
        assert handler.temp_dir == custom_dir
        assert os.path.isdir(custom_dir)

    def test_auto_temp_dir(self, handler_auto_temp):
        """未指定暫存目錄時應自動建立"""
        assert os.path.isdir(handler_auto_temp.temp_dir)
        assert "accrual_bot_ui_" in handler_auto_temp.temp_dir


# =============================================================================
# save_uploaded_file 測試
# =============================================================================

@pytest.mark.unit
class TestSaveUploadedFile:
    """測試檔案儲存功能"""

    def test_save_file_returns_path(self, handler, mock_uploaded_file):
        """儲存後應回傳檔案路徑"""
        path = handler.save_uploaded_file(mock_uploaded_file, 'raw_po')
        assert os.path.exists(path)
        assert 'raw_po' in path

    def test_save_file_content_correct(self, handler, mock_uploaded_file):
        """儲存的檔案內容應正確"""
        path = handler.save_uploaded_file(mock_uploaded_file, 'raw_po')
        with open(path, 'rb') as f:
            content = f.read()
        assert content == b"col1,col2\na,b\nc,d\n"

    def test_save_file_with_unsafe_name(self, handler):
        """檔案名稱含不安全字元時應被清理"""
        mock = MagicMock()
        mock.name = "../../../etc/passwd"
        mock.getbuffer.return_value = b"test"

        path = handler.save_uploaded_file(mock, 'test_key')
        # 確認路徑沒有跳脫暫存目錄
        assert handler.temp_dir in path
        assert '..' not in os.path.basename(path)


# =============================================================================
# validate_file 測試
# =============================================================================

@pytest.mark.unit
class TestValidateFile:
    """測試檔案驗證功能"""

    def test_valid_csv_no_errors(self, handler, sample_csv):
        """有效 CSV 檔案應回傳空錯誤清單"""
        errors = handler.validate_file(sample_csv, 'raw_po')
        assert errors == []

    def test_nonexistent_file(self, handler):
        """不存在的檔案應回傳錯誤"""
        errors = handler.validate_file('/nonexistent/path.csv', 'raw_po')
        assert len(errors) == 1
        assert '檔案不存在' in errors[0]

    def test_empty_file(self, handler, empty_file):
        """空檔案應回傳錯誤"""
        errors = handler.validate_file(empty_file, 'raw_po')
        assert len(errors) == 1
        assert '檔案為空' in errors[0]

    def test_unsupported_format(self, handler, tmp_path):
        """不支援的格式應回傳錯誤"""
        path = str(tmp_path / "data.json")
        with open(path, 'w') as f:
            f.write('{"key": "value"}')

        errors = handler.validate_file(path, 'raw_po')
        assert len(errors) == 1
        assert '不支援的檔案格式' in errors[0]

    def test_corrupted_csv(self, handler, tmp_path):
        """損壞的 CSV（但非空）應仍能通過基本驗證或回傳讀取錯誤"""
        path = str(tmp_path / "corrupted.csv")
        with open(path, 'w') as f:
            f.write("valid,csv\n1,2\n")
        # pandas 可以讀取這個，所以不應有錯誤
        errors = handler.validate_file(path, 'raw_po')
        assert errors == []


# =============================================================================
# validate_all_files 測試
# =============================================================================

@pytest.mark.unit
class TestValidateAllFiles:
    """測試批次驗證功能"""

    def test_all_valid(self, handler, sample_csv):
        """所有檔案都有效時應回傳空清單"""
        errors = handler.validate_all_files({'raw_po': sample_csv})
        assert errors == []

    def test_mixed_valid_invalid(self, handler, sample_csv):
        """混合有效和無效檔案時應回傳對應錯誤"""
        errors = handler.validate_all_files({
            'raw_po': sample_csv,
            'previous': '/nonexistent/file.csv',
        })
        assert len(errors) == 1
        assert 'previous' in errors[0]

    def test_empty_dict(self, handler):
        """空字典應回傳空錯誤清單"""
        errors = handler.validate_all_files({})
        assert errors == []


# =============================================================================
# get_file_info 測試
# =============================================================================

@pytest.mark.unit
class TestGetFileInfo:
    """測試檔案資訊查詢"""

    def test_existing_file_info(self, handler, sample_csv):
        """存在的檔案應回傳完整資訊"""
        info = handler.get_file_info(sample_csv)
        assert 'size' in info
        assert 'size_mb' in info
        assert 'modified_time' in info
        assert 'filename' in info
        assert info['size'] > 0
        assert info['filename'] == 'valid.csv'

    def test_nonexistent_file_info(self, handler):
        """不存在的檔案應回傳空字典"""
        info = handler.get_file_info('/nonexistent/file.csv')
        assert info == {}


# =============================================================================
# cleanup 測試
# =============================================================================

@pytest.mark.unit
class TestCleanup:
    """測試暫存清理功能"""

    def test_cleanup_removes_dir(self, tmp_path):
        """cleanup 應移除暫存目錄"""
        temp_dir = str(tmp_path / "to_clean")
        handler = FileHandler(temp_dir=temp_dir)

        # 建立一些檔案
        test_file = os.path.join(temp_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test")

        handler.cleanup()
        assert not os.path.exists(temp_dir)

    def test_cleanup_nonexistent_dir(self, handler):
        """目錄已不存在時 cleanup 不應拋出異常"""
        handler.temp_dir = '/nonexistent/dir'
        handler.cleanup()  # 不應拋出異常


# =============================================================================
# _sanitize_filename 測試
# =============================================================================

@pytest.mark.unit
class TestSanitizeFilename:
    """測試檔案名稱清理"""

    def test_normal_filename(self, handler):
        """一般檔案名稱不應被改變"""
        assert handler._sanitize_filename("data.csv") == "data.csv"

    def test_path_traversal(self, handler):
        """路徑穿越攻擊字元應被替換"""
        result = handler._sanitize_filename("../../etc/passwd")
        assert '/' not in result
        assert '..' not in result

    def test_backslash_replaced(self, handler):
        """反斜線應被替換"""
        result = handler._sanitize_filename("path\\to\\file.csv")
        assert '\\' not in result
