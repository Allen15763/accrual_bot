"""
會計與 OPS 底稿比對 Pipeline 封裝函數
提供便捷的 Pipeline 執行接口

放置位置：C:\SEA\Accrual\prpo_bot\accrual_bot\main_pipeline.py
"""

import asyncio
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
import pandas as pd

from accrual_bot.core.pipeline import Pipeline, PipelineBuilder
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.spx_loading import AccountingOPSDataLoadingStep
from accrual_bot.core.pipeline.steps.spx_ppe_qty_validation import AccountingOPSValidationStep
from accrual_bot.core.pipeline.steps.spx_exporting import AccountingOPSExportingStep
from accrual_bot.utils.logging import get_logger


logger = get_logger("AccountingOPSPipeline")


# ==================== 主要封裝函數 ====================

def run_accounting_ops_validation(
    accounting_file: str,
    ops_file: str,
    processing_date: int,
    output_dir: str = "output",
    accounting_params: Optional[Dict[str, Any]] = None,
    ops_params: Optional[Dict[str, Any]] = None,
    amount_columns: Optional[List[str]] = None,
    locker_pattern: Optional[str] = None,
    entity_type: str = 'SPX',
    processing_type: str = 'memo validate',
    sheet_names: Optional[Dict[str, str]] = None,
    filename_template: Optional[str] = None
) -> Tuple[ProcessingContext, Dict[str, pd.DataFrame]]:
    """
    執行會計與 OPS 底稿比對驗證 Pipeline
    
    這是最主要的封裝函數，提供完整的參數配置能力。
    
    Args:
        accounting_file: 會計底稿檔案路徑
        ops_file: OPS 驗收檔案路徑
        processing_date: 處理日期 (格式：YYYYMM，如：202509)
        output_dir: 輸出目錄路徑，預設為 "output"
        accounting_params: 會計底稿讀取參數，預設為 None 使用標準配置
            格式：{'sheet_name': 0, 'usecols': [...], 'header': 0, 'dtype': str}
        ops_params: OPS 底稿讀取參數，預設為 None 使用標準配置
            格式：{'sheet_name': '智取櫃驗收明細', 'usecols': 'A:AE', 'header': 1}
        amount_columns: OPS 金額欄位列表，預設為 None 使用標準配置
        locker_pattern: Locker 類型提取的正則表達式，預設為 None 使用標準配置
        entity_type: 實體類型，預設為 'SPX'
        processing_type: 處理類型，預設為 'memo validate'
        sheet_names: 輸出 Excel 的 sheet 名稱對應，預設為 None 使用標準配置
        filename_template: 輸出檔案名稱模板，預設為 None 使用標準配置
    
    Returns:
        Tuple[ProcessingContext, Dict[str, pd.DataFrame]]: 
            - ProcessingContext: 執行後的 context 物件
            - Dict[str, pd.DataFrame]: 包含三份資料的字典
                * 'accounting_workpaper': 會計底稿
                * 'ops_validation': OPS 底稿
                * 'validation_comparison': 比對結果
    
    Examples:
        >>> # 最簡單的使用方式（使用所有預設值）
        >>> context, data = run_accounting_ops_validation(
        ...     accounting_file='path/to/accounting.xlsx',
        ...     ops_file='path/to/ops.xlsx',
        ...     processing_date=202509
        ... )
        
        >>> # 自訂參數的使用方式
        >>> context, data = run_accounting_ops_validation(
        ...     accounting_file='path/to/accounting.xlsx',
        ...     ops_file='path/to/ops.xlsx',
        ...     processing_date=202509,
        ...     output_dir='custom_output',
        ...     accounting_params={
        ...         'sheet_name': 0,
        ...         'usecols': ['PO#', 'PO Line', 'Item Description', 'memo'],
        ...         'header': 0
        ...     },
        ...     sheet_names={
        ...         'accounting_workpaper': '會計底稿',
        ...         'ops_validation': 'OPS底稿',
        ...         'validation_comparison': '比對結果'
        ...     }
        ... )
        
        >>> # 取得比對結果
        >>> comparison_df = data['validation_comparison']
        >>> print(f"總共比對 {len(comparison_df)} 筆資料")
    
    Raises:
        FileNotFoundError: 如果檔案不存在
        ValueError: 如果參數無效
        Exception: 其他執行錯誤
    """
    logger.info("=" * 80)
    logger.info("開始執行會計與 OPS 底稿比對驗證 Pipeline")
    logger.info(f"會計底稿: {accounting_file}")
    logger.info(f"OPS 底稿: {ops_file}")
    logger.info(f"處理日期: {processing_date}")
    logger.info("=" * 80)
    
    # 驗證檔案存在性
    _validate_files(accounting_file, ops_file)
    
    # 使用預設值或使用者提供的參數
    accounting_params = accounting_params or _get_default_accounting_params()
    ops_params = ops_params or _get_default_ops_params()
    amount_columns = amount_columns or _get_default_amount_columns()
    locker_pattern = locker_pattern or _get_default_locker_pattern()
    sheet_names = sheet_names or _get_default_sheet_names()
    filename_template = filename_template or "{entity}_{type}_{date}_{timestamp}.xlsx"
    
    # 構建 Pipeline
    pipeline = _build_pipeline(
        accounting_file=accounting_file,
        ops_file=ops_file,
        accounting_params=accounting_params,
        ops_params=ops_params,
        amount_columns=amount_columns,
        locker_pattern=locker_pattern,
        output_dir=output_dir,
        sheet_names=sheet_names,
        filename_template=filename_template
    )
    
    # 創建 Context
    context = ProcessingContext(
        data=pd.DataFrame(),
        entity_type=entity_type,
        processing_date=processing_date,
        processing_type=processing_type
    )
    
    # 執行 Pipeline
    logger.info("開始執行 Pipeline...")
    result = asyncio.run(pipeline.execute(context))
    
    # 檢查執行結果
    if not result.get('success'):
        error_msg = f"Pipeline 執行失敗: {result.get('errors')}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info("Pipeline 執行成功！")
    
    # 取得結果資料
    data = {
        'accounting_workpaper': context.get_auxiliary_data('accounting_workpaper'),
        'ops_validation': context.get_auxiliary_data('ops_validation'),
        'validation_comparison': context.get_auxiliary_data('validation_comparison')
    }
    
    # 輸出統計資訊
    _log_statistics(data, context)
    
    return context, data


# ==================== 內部輔助函數 ====================

def _validate_files(accounting_file: str, ops_file: str):
    """驗證檔案存在性"""
    if not Path(accounting_file).exists():
        raise FileNotFoundError(f"會計底稿檔案不存在: {accounting_file}")
    
    if not Path(ops_file).exists():
        raise FileNotFoundError(f"OPS 底稿檔案不存在: {ops_file}")


def _get_default_accounting_params() -> Dict[str, Any]:
    """取得會計底稿的預設讀取參數"""
    return {
        'sheet_name': 0,
        'usecols': ['PO#', 'PO Line', 'Item Description', 'memo'],
        'header': 0,
        'dtype': str
    }


def _get_default_ops_params() -> Dict[str, Any]:
    """取得 OPS 底稿的預設讀取參數"""
    return {
        'sheet_name': '智取櫃驗收明細',
        'usecols': 'A:AE',
        'header': 1
    }


def _get_default_amount_columns() -> List[str]:
    """取得預設的金額欄位列表"""
    return [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
        'I', 'J', 'K', 'DA', 'XA', 'XB', 'XC', 'XD', 'XE', 'XF',
        '超出櫃體安裝費', '超出櫃體運費', '裝運費'
    ]


def _get_default_locker_pattern() -> str:
    """取得預設的 locker pattern"""
    return r'SPX\s+locker\s+([A-Z]{1,2}|控制主[櫃機]|[^\s]+?)(?:\s*第[一二]期款項|\s*訂金|\s*\d+%款項|\s*#)'


def _get_default_sheet_names() -> Dict[str, str]:
    """取得預設的 sheet 名稱對應"""
    return {
        'accounting_workpaper': 'acc_raw',
        'ops_validation': 'ops_raw',
        'validation_comparison': 'result'
    }


def _build_pipeline(
    accounting_file: str,
    ops_file: str,
    accounting_params: Dict[str, Any],
    ops_params: Dict[str, Any],
    amount_columns: List[str],
    locker_pattern: str,
    output_dir: str,
    sheet_names: Dict[str, str],
    filename_template: str
) -> Pipeline:
    """構建 Pipeline"""
    builder = PipelineBuilder("AccountingOPS_Validation", "SPX")
    
    pipeline = (
        builder
        .add_step(
            AccountingOPSDataLoadingStep(
                name="LoadAccountingOPS",
                file_paths={
                    'accounting_workpaper': {
                        'path': accounting_file,
                        'params': accounting_params
                    },
                    'ops_validation': {
                        'path': ops_file,
                        'params': ops_params
                    }
                },
                required_columns={
                    'accounting': ['PO Line', 'memo'],
                    'ops': ['A', 'B', 'C']
                }
            )
        )
        .add_step(
            AccountingOPSValidationStep(
                name="ValidateAccountingOPS",
                amount_columns=amount_columns,
                locker_pattern=locker_pattern
            )
        )
        .add_step(
            AccountingOPSExportingStep(
                name="ExportResults",
                output_dir=output_dir,
                filename_template=filename_template,
                sheet_names=sheet_names
            )
        )
        .build()
    )
    
    return pipeline


def _log_statistics(data: Dict[str, pd.DataFrame], context: ProcessingContext):
    """輸出統計資訊"""
    logger.info("=" * 80)
    logger.info("執行結果統計")
    logger.info("-" * 80)
    
    # 基本統計
    logger.info(f"會計底稿筆數: {len(data['accounting_workpaper'])}")
    logger.info(f"OPS 底稿筆數: {len(data['ops_validation'])}")
    logger.info(f"比對結果筆數: {len(data['validation_comparison'])}")
    
    # 比對結果統計
    comparison_df = data['validation_comparison']
    if 'status' in comparison_df.columns:
        status_counts = comparison_df['status'].value_counts()
        logger.info("-" * 80)
        logger.info("比對狀態統計:")
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count} 筆")
    
    # 輸出檔案路徑
    output_path = context.get_variable('export_output_path')
    if output_path:
        logger.info("-" * 80)
        logger.info(f"輸出檔案: {output_path}")
    
    logger.info("=" * 80)


# ==================== 使用範例 ====================

if __name__ == "__main__":

    # 完整參數的使用方式
    print("\n範例 2: 完整參數配置")
    print("-" * 60)
    
    context, data = run_accounting_ops_validation(
        accounting_file=r'C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202510\202509_PO_FN_改memo.xlsx',
        ops_file=r'C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202510\SPX智取櫃及繳費機驗收明細(For FN)_2510.xlsx',
        processing_date=202510,
        output_dir="output",
        sheet_names={
            'accounting_workpaper': '會計底稿',
            'ops_validation': 'OPS底稿',
            'validation_comparison': '比對結果'
        }
    )
    
    print(f"會計底稿: {len(data['accounting_workpaper'])} 筆")
    print(f"OPS 底稿: {len(data['ops_validation'])} 筆")
    print(f"比對結果: {len(data['validation_comparison'])} 筆")
    