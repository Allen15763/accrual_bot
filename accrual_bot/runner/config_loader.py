"""
Config Loader - 載入執行配置和路徑配置

Functions:
    load_run_config: 載入 run_config.toml
    load_file_paths: 載入並解析 paths.toml
"""

import tomllib
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Any, Dict, Optional

from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class RunConfig:
    """執行配置資料類別"""
    # 基本設定
    entity: str
    processing_type: str
    processing_date: int

    # Debug 設定
    step_by_step: bool = False
    save_checkpoints: bool = True
    verbose: bool = False

    # Resume 設定
    resume_enabled: bool = False
    checkpoint_name: str = ""
    from_step: str = ""

    # Output 設定
    output_dir: str = "./output"
    auto_export: bool = True


def get_config_dir() -> Path:
    """取得配置檔案目錄"""
    return Path(__file__).parent.parent / "config"


def load_run_config(config_path: Optional[Path] = None) -> RunConfig:
    """
    載入執行配置

    Args:
        config_path: 配置檔案路徑，預設為 config/run_config.toml

    Returns:
        RunConfig: 執行配置物件
    """
    if config_path is None:
        config_path = get_config_dir() / "run_config.toml"

    logger.info(f"載入執行配置: {config_path}")

    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    run = config.get("run", {})
    debug = config.get("debug", {})
    resume = config.get("resume", {})
    output = config.get("output", {})

    return RunConfig(
        # 基本設定
        entity=run.get("entity", "SPX"),
        processing_type=run.get("processing_type", "PO"),
        processing_date=run.get("processing_date", 202512),
        # Debug 設定
        step_by_step=debug.get("step_by_step", False),
        save_checkpoints=debug.get("save_checkpoints", True),
        verbose=debug.get("verbose", False),
        # Resume 設定
        resume_enabled=resume.get("enabled", False),
        checkpoint_name=resume.get("checkpoint_name", ""),
        from_step=resume.get("from_step", ""),
        # Output 設定
        output_dir=output.get("output_dir", "./output"),
        auto_export=output.get("auto_export", True),
    )


def load_file_paths(
    entity: str,
    processing_type: str,
    processing_date: int,
    paths_file: Optional[Path] = None
) -> Dict[str, Any]:
    """
    載入並解析檔案路徑配置

    Args:
        entity: 業務實體 (SPX/SPT/MOB)
        processing_type: 處理類型 (PO/PR/PPE)
        processing_date: 處理日期 (YYYYMM)
        paths_file: 路徑配置檔案，預設為 config/paths.toml

    Returns:
        Dict[str, Any]: 解析後的檔案路徑字典，格式為 {key: {'path': str, 'params': dict}}
    """
    if paths_file is None:
        paths_file = get_config_dir() / "paths.toml"

    logger.info(f"載入路徑配置: {paths_file}")

    with open(paths_file, "rb") as f:
        paths_config = tomllib.load(f)

    # 計算日期變數
    date_vars = _calculate_date_vars(processing_date)

    # 取得 base paths
    base = paths_config.get("base", {})
    date_vars["resources"] = base.get("resources", "")

    # 取得實體和類型的路徑配置
    entity_lower = entity.lower()
    type_lower = processing_type.lower()

    paths_section = paths_config.get(entity_lower, {}).get(type_lower, {})
    params_section = paths_config.get(entity_lower).get(f"{type_lower}").get('params')

    # 解析路徑模板
    file_paths: Dict[str, Any] = {}

    for key, path_template in paths_section.items():
        # 跳過 params 區塊，因為path跟params的key相同，直接按key放進去即可。
        if key == "params" or not isinstance(path_template, str):
            continue

        # 替換變數
        resolved_path = _resolve_path_template(path_template, date_vars)

        # 處理萬用字元 (選擇最新的檔案)
        if "*" in resolved_path:
            matches = glob(resolved_path)
            if matches:
                resolved_path = sorted(matches)[-1]
                logger.debug(f"  萬用字元解析: {key} -> {resolved_path}")
            else:
                logger.warning(f"  找不到符合的檔案: {path_template}")

        # 取得參數
        params = params_section.get(key, {})

        # 轉換 params 中的特殊值
        params = _convert_params(params)

        file_paths[key] = {
            "path": resolved_path,
            "params": params
        }

        logger.debug(f"  {key}: {resolved_path}")

    logger.info(f"載入 {len(file_paths)} 個檔案路徑配置")
    return file_paths


def _calculate_date_vars(processing_date: int) -> Dict[str, str]:
    """
    計算日期相關變數

    Args:
        processing_date: 處理日期 (YYYYMM)

    Returns:
        Dict[str, str]: 日期變數字典
    """
    year = processing_date // 100
    month = processing_date % 100

    # 計算前一個月
    if month > 1:
        prev_year = year
        prev_month = month - 1
    else:
        prev_year = year - 1
        prev_month = 12

    return {
        "YYYYMM": str(processing_date),
        "PREV_YYYYMM": f"{prev_year}{prev_month:02d}",
        "YYMM": f"{year % 100:02d}{month:02d}",
        "YYYY": str(year),
        "MM": f"{month:02d}",
    }


def _resolve_path_template(template: str, vars: Dict[str, str]) -> str:
    """
    解析路徑模板，替換變數

    Args:
        template: 路徑模板字串
        vars: 變數字典

    Returns:
        str: 解析後的路徑
    """
    result = template
    for var_name, var_value in vars.items():
        result = result.replace(f"{{{var_name}}}", var_value)
    return result


def _convert_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    轉換參數中的特殊值

    Args:
        params: 原始參數字典

    Returns:
        Dict[str, Any]: 轉換後的參數字典
    """
    result = {}
    for key, value in params.items():
        # 處理 dtype = "str" -> dtype = str
        if key == "dtype" and value == "str":
            result[key] = str
        # 處理布林值
        elif key == "keep_default_na" and isinstance(value, bool):
            result[key] = value
        else:
            result[key] = value
    return result
