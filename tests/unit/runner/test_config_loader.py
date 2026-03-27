"""
config_loader 單元測試

測試 load_run_config, load_file_paths, _calculate_date_vars,
_resolve_path_template, _convert_params, _deep_merge 等函式。
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from accrual_bot.runner.config_loader import (
    RunConfig,
    load_run_config,
    load_file_paths,
    _calculate_date_vars,
    _resolve_path_template,
    _convert_params,
    _deep_merge,
)


# ---------------------------------------------------------------------------
# Helper: 寫入 TOML 檔案（使用 bytes 避免 Windows 編碼問題）
# ---------------------------------------------------------------------------
def _write_toml(path: Path, content: str) -> Path:
    path.write_bytes(content.encode("utf-8"))
    return path


# ===========================================================================
# _calculate_date_vars
# ===========================================================================
@pytest.mark.unit
class TestCalculateDateVars:
    """測試日期變數計算"""

    def test_normal_month(self):
        """一般月份（非一月）的日期變數"""
        result = _calculate_date_vars(202506)
        assert result["YYYYMM"] == "202506"
        assert result["PREV_YYYYMM"] == "202505"
        assert result["YYMM"] == "2506"
        assert result["YYYY"] == "2025"
        assert result["MM"] == "06"

    def test_january_boundary(self):
        """一月份邊界：前一個月應為上年度十二月"""
        result = _calculate_date_vars(202601)
        assert result["YYYYMM"] == "202601"
        assert result["PREV_YYYYMM"] == "202512"
        assert result["YYMM"] == "2601"
        assert result["YYYY"] == "2026"
        assert result["MM"] == "01"

    def test_december(self):
        """十二月的日期變數"""
        result = _calculate_date_vars(202512)
        assert result["PREV_YYYYMM"] == "202511"
        assert result["MM"] == "12"

    @pytest.mark.parametrize(
        "date_input,expected_yymm",
        [
            (202501, "2501"),
            (202512, "2512"),
            (203003, "3003"),
        ],
    )
    def test_yymm_format(self, date_input, expected_yymm):
        """YYMM 格式化驗證"""
        result = _calculate_date_vars(date_input)
        assert result["YYMM"] == expected_yymm


# ===========================================================================
# _resolve_path_template
# ===========================================================================
@pytest.mark.unit
class TestResolvePathTemplate:
    """測試路徑模板變數替換"""

    def test_basic_replacement(self):
        """基本變數替換"""
        template = "/data/{YYYYMM}/file.csv"
        vars_ = {"YYYYMM": "202506"}
        assert _resolve_path_template(template, vars_) == "/data/202506/file.csv"

    def test_multiple_vars(self):
        """多個變數替換"""
        template = "{resources}/{YYYYMM}/prev/{PREV_YYYYMM}.xlsx"
        vars_ = {"resources": "/root", "YYYYMM": "202506", "PREV_YYYYMM": "202505"}
        assert _resolve_path_template(template, vars_) == "/root/202506/prev/202505.xlsx"

    def test_no_vars(self):
        """沒有變數的模板直接回傳"""
        assert _resolve_path_template("/static/path.csv", {}) == "/static/path.csv"

    def test_unreplaced_var_warns(self):
        """殘留未解析變數時應發出警告"""
        template = "/data/{YYYYMM}/{UNKNOWN}/file.csv"
        vars_ = {"YYYYMM": "202506"}
        with patch("accrual_bot.runner.config_loader.logger") as mock_logger:
            result = _resolve_path_template(template, vars_)
        # 仍回傳結果，但包含未解析的 token
        assert "{UNKNOWN}" in result
        mock_logger.warning.assert_called_once()


# ===========================================================================
# _convert_params
# ===========================================================================
@pytest.mark.unit
class TestConvertParams:
    """測試參數轉換"""

    def test_dtype_str_conversion(self):
        """dtype = 'str' 應轉為 Python str 型別"""
        result = _convert_params({"dtype": "str", "header": 0})
        assert result["dtype"] is str
        assert result["header"] == 0

    def test_no_dtype(self):
        """沒有 dtype 時其他參數照原樣回傳"""
        params = {"sheet_name": "Sheet1", "header": 3}
        result = _convert_params(params)
        assert result == params

    def test_dtype_non_str_value(self):
        """dtype 為非 'str' 值時不做轉換"""
        result = _convert_params({"dtype": "int"})
        assert result["dtype"] == "int"

    def test_empty_params(self):
        """空字典回傳空字典"""
        assert _convert_params({}) == {}


# ===========================================================================
# _deep_merge
# ===========================================================================
@pytest.mark.unit
class TestDeepMerge:
    """測試遞迴合併"""

    def test_simple_override(self):
        """簡單值覆蓋"""
        base = {"a": 1, "b": 2}
        override = {"b": 99}
        _deep_merge(base, override)
        assert base == {"a": 1, "b": 99}

    def test_nested_merge(self):
        """巢狀字典遞迴合併"""
        base = {"x": {"a": 1, "b": 2}, "y": 10}
        override = {"x": {"b": 99, "c": 3}}
        _deep_merge(base, override)
        assert base == {"x": {"a": 1, "b": 99, "c": 3}, "y": 10}

    def test_new_key(self):
        """新增不存在的 key"""
        base = {"a": 1}
        override = {"b": 2}
        _deep_merge(base, override)
        assert base == {"a": 1, "b": 2}

    def test_override_dict_with_scalar(self):
        """用純量覆蓋字典"""
        base = {"a": {"nested": 1}}
        override = {"a": "flat"}
        _deep_merge(base, override)
        assert base["a"] == "flat"


# ===========================================================================
# load_run_config
# ===========================================================================
@pytest.mark.unit
class TestLoadRunConfig:
    """測試載入執行配置"""

    def test_load_full_config(self, tmp_path):
        """完整配置載入"""
        toml_content = """\
[run]
entity = "SPT"
processing_type = "PR"
processing_date = 202603
source_type = "COMBINED"

[debug]
step_by_step = true
save_checkpoints = false
verbose = true

[resume]
enabled = true
checkpoint_name = "ckpt_1"
from_step = "Step2"

[output]
output_dir = "/tmp/out"
auto_export = false
"""
        config_path = _write_toml(tmp_path / "run_config.toml", toml_content)
        rc = load_run_config(config_path)

        assert isinstance(rc, RunConfig)
        assert rc.entity == "SPT"
        assert rc.processing_type == "PR"
        assert rc.processing_date == 202603
        assert rc.source_type == "COMBINED"
        assert rc.step_by_step is True
        assert rc.save_checkpoints is False
        assert rc.verbose is True
        assert rc.resume_enabled is True
        assert rc.checkpoint_name == "ckpt_1"
        assert rc.from_step == "Step2"
        assert rc.output_dir == "/tmp/out"
        assert rc.auto_export is False

    def test_load_defaults(self, tmp_path):
        """最小配置使用預設值"""
        config_path = _write_toml(tmp_path / "run_config.toml", "# empty\n")
        rc = load_run_config(config_path)

        assert rc.entity == "SPX"
        assert rc.processing_type == "PO"
        assert rc.processing_date == 202512
        assert rc.step_by_step is False
        assert rc.save_checkpoints is True
        assert rc.verbose is False
        assert rc.resume_enabled is False
        assert rc.output_dir == "./output"
        assert rc.auto_export is True

    def test_missing_file_raises(self, tmp_path):
        """不存在的檔案應拋出例外"""
        with pytest.raises(FileNotFoundError):
            load_run_config(tmp_path / "nonexistent.toml")


# ===========================================================================
# load_file_paths
# ===========================================================================
@pytest.mark.unit
class TestLoadFilePaths:
    """測試載入路徑配置"""

    @staticmethod
    def _make_paths_toml(tmp_path: Path, content: str) -> Path:
        return _write_toml(tmp_path / "paths.toml", content)

    def test_basic_load(self, tmp_path):
        """基本路徑載入及變數替換"""
        toml_content = """\
[base]
resources = "/res"

[myent.po]
raw_po = "/data/{YYYYMM}/file.csv"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)
        result = load_file_paths("MYENT", "PO", 202506, paths_file=paths_file)

        assert "raw_po" in result
        assert result["raw_po"]["path"] == "/data/202506/file.csv"
        assert result["raw_po"]["params"] == {}

    def test_with_params(self, tmp_path):
        """路徑配合 params 區塊"""
        toml_content = """\
[base]
resources = "/res"

[ent.pr]
raw = "/data/{YYYYMM}.csv"

[ent.pr.params]
raw = { encoding = "utf-8", dtype = "str" }
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)
        result = load_file_paths("ENT", "PR", 202506, paths_file=paths_file)

        assert result["raw"]["params"]["encoding"] == "utf-8"
        # dtype = "str" 應轉為 str 型別
        assert result["raw"]["params"]["dtype"] is str

    def test_invalid_entity_raises(self, tmp_path):
        """不存在的實體應拋出 ValueError"""
        toml_content = """\
[base]
resources = "/res"

[spx.po]
raw = "/data/file.csv"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)
        with pytest.raises(ValueError, match="找不到實體"):
            load_file_paths("INVALID", "PO", 202506, paths_file=paths_file)

    def test_invalid_type_raises(self, tmp_path):
        """不存在的處理類型應拋出 ValueError"""
        toml_content = """\
[base]
resources = "/res"

[spx.po]
raw = "/data/file.csv"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)
        with pytest.raises(ValueError, match="找不到"):
            load_file_paths("SPX", "INVALID_TYPE", 202506, paths_file=paths_file)

    def test_wildcard_single_match(self, tmp_path):
        """萬用字元路徑只有一個匹配"""
        toml_content = """\
[base]
resources = "/res"

[ent.po]
raw = "/data/202506_*.csv"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)

        with patch("accrual_bot.runner.config_loader.glob", return_value=["/data/202506_v1.csv"]):
            result = load_file_paths("ENT", "PO", 202506, paths_file=paths_file)
        assert result["raw"]["path"] == "/data/202506_v1.csv"

    def test_wildcard_multiple_matches_picks_last(self, tmp_path):
        """萬用字元多個匹配時選字典序最後一個"""
        toml_content = """\
[base]
resources = "/res"

[ent.po]
raw = "/data/202506_*.csv"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)

        matches = ["/data/202506_a.csv", "/data/202506_c.csv", "/data/202506_b.csv"]
        with patch("accrual_bot.runner.config_loader.glob", return_value=matches):
            result = load_file_paths("ENT", "PO", 202506, paths_file=paths_file)
        # sorted: a, b, c -> 最後一個是 c
        assert result["raw"]["path"] == "/data/202506_c.csv"

    def test_wildcard_no_match_keeps_pattern(self, tmp_path):
        """萬用字元無匹配時保留原始模板（含已替換的變數）"""
        toml_content = """\
[base]
resources = "/res"

[ent.po]
raw = "/data/{YYYYMM}_*.csv"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)

        with patch("accrual_bot.runner.config_loader.glob", return_value=[]):
            result = load_file_paths("ENT", "PO", 202506, paths_file=paths_file)
        # 變數已替換但萬用字元仍在
        assert result["raw"]["path"] == "/data/202506_*.csv"

    def test_local_override(self, tmp_path):
        """paths.local.toml 覆蓋主配置"""
        main_toml = """\
[base]
resources = "/main/res"

[ent.po]
raw = "{resources}/{YYYYMM}/file.csv"
"""
        local_toml = """\
[base]
resources = "/local/res"
"""
        _write_toml(tmp_path / "paths.toml", main_toml)
        _write_toml(tmp_path / "paths.local.toml", local_toml)

        result = load_file_paths("ENT", "PO", 202506, paths_file=tmp_path / "paths.toml")
        assert result["raw"]["path"] == "/local/res/202506/file.csv"

    def test_resources_var_substitution(self, tmp_path):
        """base.resources 應作為 {resources} 變數替換"""
        toml_content = """\
[base]
resources = "/my/root"

[ent.po]
raw = "{resources}/sub/{YYYYMM}.csv"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)
        result = load_file_paths("ENT", "PO", 202503, paths_file=paths_file)
        assert result["raw"]["path"] == "/my/root/sub/202503.csv"

    def test_prev_yyyymm_substitution(self, tmp_path):
        """PREV_YYYYMM 變數替換（含一月邊界）"""
        toml_content = """\
[base]
resources = ""

[ent.po]
prev = "/data/{PREV_YYYYMM}/prev.xlsx"
"""
        paths_file = self._make_paths_toml(tmp_path, toml_content)
        result = load_file_paths("ENT", "PO", 202601, paths_file=paths_file)
        assert result["prev"]["path"] == "/data/202512/prev.xlsx"
