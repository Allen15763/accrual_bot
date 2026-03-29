"""
file_utils 模組單元測試
"""

import os
import time
import pytest
from pathlib import Path

from accrual_bot.utils.helpers.file_utils import (
    get_resource_path,
    validate_file_path,
    validate_file_extension,
    get_file_extension,
    is_excel_file,
    is_csv_file,
    ensure_directory_exists,
    get_safe_filename,
    get_unique_filename,
    get_file_info,
    calculate_file_hash,
    copy_file_safely,
    move_file_safely,
    cleanup_temp_files,
    find_files_by_pattern,
    load_toml,
    get_directory_size,
)


@pytest.mark.unit
class TestFileUtils:
    """file_utils 函數測試"""

    # ------------------------------------------------------------------ #
    # get_resource_path
    # ------------------------------------------------------------------ #
    def test_get_resource_path_returns_absolute(self):
        result = get_resource_path("data/file.csv")
        expected = os.path.join(os.path.abspath("."), "data/file.csv")
        assert result == expected

    # ------------------------------------------------------------------ #
    # validate_file_path
    # ------------------------------------------------------------------ #
    def test_validate_file_path_none_returns_false(self):
        assert validate_file_path(None) is False

    def test_validate_file_path_empty_string_returns_false(self):
        assert validate_file_path("") is False

    def test_validate_file_path_non_string_returns_false(self):
        assert validate_file_path(123) is False

    def test_validate_file_path_existing_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        assert validate_file_path(str(f), check_exists=True) is True

    def test_validate_file_path_nonexistent_file(self, tmp_path):
        assert validate_file_path(str(tmp_path / "missing.txt"), check_exists=True) is False

    def test_validate_file_path_skip_exists_check(self, tmp_path):
        # 檔案不存在但 check_exists=False 時應回傳 True
        assert validate_file_path(str(tmp_path / "missing.txt"), check_exists=False) is True

    def test_validate_file_path_directory_returns_false(self, tmp_path):
        # 目錄而非檔案應回傳 False
        assert validate_file_path(str(tmp_path), check_exists=True) is False

    # ------------------------------------------------------------------ #
    # validate_file_extension / get_file_extension
    # ------------------------------------------------------------------ #
    def test_validate_file_extension_supported(self):
        assert validate_file_extension("report.xlsx") is True
        assert validate_file_extension("data.csv") is True

    def test_validate_file_extension_unsupported(self):
        assert validate_file_extension("image.png") is False

    def test_validate_file_extension_custom_list(self):
        assert validate_file_extension("doc.parquet", [".parquet"]) is True
        assert validate_file_extension("doc.csv", [".parquet"]) is False

    def test_get_file_extension(self):
        assert get_file_extension("report.XLSX") == ".xlsx"
        assert get_file_extension("archive.tar.gz") == ".gz"

    # ------------------------------------------------------------------ #
    # is_excel_file / is_csv_file
    # ------------------------------------------------------------------ #
    def test_is_excel_file(self):
        assert is_excel_file("book.xlsx") is True
        assert is_excel_file("book.xls") is True
        assert is_excel_file("book.csv") is False

    def test_is_csv_file(self):
        assert is_csv_file("data.csv") is True
        assert is_csv_file("data.xlsx") is False

    # ------------------------------------------------------------------ #
    # ensure_directory_exists
    # ------------------------------------------------------------------ #
    def test_ensure_directory_exists_creates_new(self, tmp_path):
        new_dir = tmp_path / "a" / "b" / "c"
        assert ensure_directory_exists(str(new_dir)) is True
        assert new_dir.is_dir()

    def test_ensure_directory_exists_already_exists(self, tmp_path):
        assert ensure_directory_exists(str(tmp_path)) is True

    # ------------------------------------------------------------------ #
    # get_safe_filename
    # ------------------------------------------------------------------ #
    def test_get_safe_filename_removes_unsafe_chars(self):
        result = get_safe_filename('file<>:"/\\|?*.txt')
        assert result == "file_________.txt"

    def test_get_safe_filename_trims_length(self):
        long_name = "a" * 300 + ".csv"
        result = get_safe_filename(long_name, max_length=20)
        assert len(result) <= 20
        assert result.endswith(".csv")

    # ------------------------------------------------------------------ #
    # get_unique_filename
    # ------------------------------------------------------------------ #
    def test_get_unique_filename_no_conflict(self, tmp_path):
        result = get_unique_filename(str(tmp_path), "new.txt")
        assert result == str(tmp_path / "new.txt")

    def test_get_unique_filename_with_conflict(self, tmp_path):
        (tmp_path / "report.csv").write_text("x")
        result = get_unique_filename(str(tmp_path), "report.csv")
        assert result == str(tmp_path / "report_1.csv")

    # ------------------------------------------------------------------ #
    # get_file_info
    # ------------------------------------------------------------------ #
    def test_get_file_info_existing_file(self, tmp_path):
        f = tmp_path / "info.txt"
        f.write_text("content")
        info = get_file_info(str(f))
        assert info["name"] == "info.txt"
        assert info["stem"] == "info"
        assert info["suffix"] == ".txt"
        assert info["size"] == 7
        assert info["is_file"] is True

    def test_get_file_info_nonexistent(self, tmp_path):
        assert get_file_info(str(tmp_path / "nope.txt")) == {}

    # ------------------------------------------------------------------ #
    # calculate_file_hash
    # ------------------------------------------------------------------ #
    def test_calculate_file_hash_md5(self, tmp_path):
        f = tmp_path / "hash.txt"
        f.write_bytes(b"hello")
        h = calculate_file_hash(str(f), algorithm="md5")
        assert h is not None and len(h) == 32

    def test_calculate_file_hash_nonexistent(self, tmp_path):
        assert calculate_file_hash(str(tmp_path / "missing.bin")) is None

    # ------------------------------------------------------------------ #
    # copy_file_safely / move_file_safely
    # ------------------------------------------------------------------ #
    def test_copy_file_safely(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("data")
        dst = tmp_path / "dst.txt"
        assert copy_file_safely(str(src), str(dst)) is True
        assert dst.read_text() == "data"
        assert src.exists()  # 來源仍在

    def test_copy_file_safely_no_overwrite(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("old")
        dst = tmp_path / "dst.txt"
        dst.write_text("existing")
        assert copy_file_safely(str(src), str(dst), overwrite=False) is False

    def test_move_file_safely(self, tmp_path):
        src = tmp_path / "mv_src.txt"
        src.write_text("move_me")
        dst = tmp_path / "mv_dst.txt"
        assert move_file_safely(str(src), str(dst)) is True
        assert not src.exists()
        assert dst.read_text() == "move_me"

    def test_move_file_safely_src_missing(self, tmp_path):
        assert move_file_safely(str(tmp_path / "no.txt"), str(tmp_path / "dst.txt")) is False

    # ------------------------------------------------------------------ #
    # cleanup_temp_files
    # ------------------------------------------------------------------ #
    def test_cleanup_temp_files_removes_old(self, tmp_path):
        old_file = tmp_path / "old.tmp"
        old_file.write_text("x")
        # 將修改時間設為 48 小時前
        old_ts = time.time() - 48 * 3600
        os.utime(str(old_file), (old_ts, old_ts))

        new_file = tmp_path / "new.tmp"
        new_file.write_text("y")

        removed = cleanup_temp_files(str(tmp_path), max_age_hours=24)
        assert removed == 1
        assert not old_file.exists()
        assert new_file.exists()

    # ------------------------------------------------------------------ #
    # find_files_by_pattern
    # ------------------------------------------------------------------ #
    def test_find_files_by_pattern_recursive(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "a.csv").write_text("")
        (sub / "b.csv").write_text("")
        results = find_files_by_pattern(str(tmp_path), "*.csv", recursive=True)
        assert len(results) == 2

    def test_find_files_by_pattern_non_recursive(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "a.csv").write_text("")
        (sub / "b.csv").write_text("")
        results = find_files_by_pattern(str(tmp_path), "*.csv", recursive=False)
        assert len(results) == 1

    # ------------------------------------------------------------------ #
    # load_toml
    # ------------------------------------------------------------------ #
    def test_load_toml_valid(self, tmp_path):
        toml_file = tmp_path / "cfg.toml"
        toml_file.write_text('[section]\nkey = "value"\n')
        data = load_toml(str(toml_file))
        assert data == {"section": {"key": "value"}}

    def test_load_toml_missing_file(self, tmp_path):
        assert load_toml(str(tmp_path / "missing.toml")) == {}

    # ------------------------------------------------------------------ #
    # get_directory_size
    # ------------------------------------------------------------------ #
    def test_get_directory_size(self, tmp_path):
        (tmp_path / "a.bin").write_bytes(b"12345")
        (tmp_path / "b.bin").write_bytes(b"67890")
        total, count = get_directory_size(str(tmp_path))
        assert total == 10
        assert count == 2

    def test_get_directory_size_nonexistent(self, tmp_path):
        assert get_directory_size(str(tmp_path / "nope")) == (0, 0)
