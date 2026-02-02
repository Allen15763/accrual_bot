"""
SPT Combined Procurement Data Loading Step

COMBINED 模式資料載入步驟 - 同時載入 PO 和 PR 資料
"""

from typing import Tuple, Dict, Any
import pandas as pd
import time
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.datasources import DataSourceFactory


class CombinedProcurementDataLoadingStep(PipelineStep):
    """
    COMBINED 模式資料載入步驟

    功能:
    1. 同時載入 raw_po 和 raw_pr 資料
    2. 載入採購前期底稿的 PO 和 PR sheets
    3. 將資料分別存儲到 auxiliary_data

    設計原則:
    - 不影響現有的單一模式 loading steps
    - 使用 auxiliary_data 存儲，避免混淆主 data
    - 提供詳細的載入統計資訊
    """

    def __init__(self, name: str = "CombinedProcurementDataLoading", file_paths: Dict[str, Any] = None, **kwargs):
        super().__init__(name, description="Load PO and PR data for combined processing", **kwargs)
        self.file_paths = file_paths or {}

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行資料載入"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            load_summary = {
                'po_loaded': False,
                'pr_loaded': False,
                'po_rows': 0,
                'pr_rows': 0,
                'po_previous_loaded': False,
                'pr_previous_loaded': False,
                'file_date': None
            }

            # 1. 載入 PO 資料
            if 'raw_po' in self.file_paths:
                self.logger.info("Loading raw PO data...")
                po_result = await self._load_po_data()
                if po_result:
                    po_df, file_date = po_result
                    context.set_auxiliary_data('po_data', po_df)
                    load_summary['po_loaded'] = True
                    load_summary['po_rows'] = len(po_df)
                    load_summary['file_date'] = file_date
                    context.set_variable('file_date', file_date)
                    self.logger.info(f"✓ Loaded PO data: {len(po_df)} rows")
                else:
                    self.logger.warning("✗ Failed to load PO data")
            else:
                self.logger.warning("raw_po not provided")

            # 2. 載入 PR 資料
            if 'raw_pr' in self.file_paths:
                self.logger.info("Loading raw PR data...")
                pr_result = await self._load_pr_data()
                if pr_result:
                    pr_df, file_date = pr_result
                    context.set_auxiliary_data('pr_data', pr_df)
                    load_summary['pr_loaded'] = True
                    load_summary['pr_rows'] = len(pr_df)
                    if load_summary['file_date'] is None:
                        load_summary['file_date'] = file_date
                        context.set_variable('file_date', file_date)
                    self.logger.info(f"✓ Loaded PR data: {len(pr_df)} rows")
                else:
                    self.logger.warning("✗ Failed to load PR data")
            else:
                self.logger.warning("raw_pr not provided")

            # 檢查是否至少載入一個
            if not load_summary['po_loaded'] and not load_summary['pr_loaded']:
                raise ValueError("Failed to load both PO and PR data. At least one is required.")

            # 3. 載入採購前期底稿
            if 'procurement_previous' in self.file_paths:
                self.logger.info("Loading procurement previous workpaper...")
                prev_results = await self._load_procurement_previous()

                if prev_results['po'] is not None:
                    context.set_auxiliary_data('procurement_previous_po', prev_results['po'])
                    load_summary['po_previous_loaded'] = True
                    self.logger.info(f"✓ Loaded procurement previous PO: {len(prev_results['po'])} rows")

                if prev_results['pr'] is not None:
                    context.set_auxiliary_data('procurement_previous_pr', prev_results['pr'])
                    load_summary['pr_previous_loaded'] = True
                    self.logger.info(f"✓ Loaded procurement previous PR: {len(prev_results['pr'])} rows")

                # 記錄檔案路徑供驗證使用
                context.set_variable('procurement_previous_path', prev_results['file_path'])
            else:
                self.logger.warning("procurement_previous not provided")

            # 設置主 data 為空 DataFrame（後續步驟會分別處理 PO 和 PR）
            context.update_data(pd.DataFrame())

            duration = time.time() - start_time

            # 生成摘要訊息
            summary_msg = self._generate_load_summary(load_summary)
            self.logger.info(f"\n{summary_msg}")

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=pd.DataFrame(),  # 空 DataFrame
                message=f"Loaded PO: {load_summary['po_rows']}, PR: {load_summary['pr_rows']}",
                duration=duration,
                metadata=load_summary
            )

        except Exception as e:
            self.logger.error(f"Combined data loading failed: {str(e)}", exc_info=True)
            context.add_error(f"Data loading failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    async def _load_po_data(self) -> Tuple[pd.DataFrame, int]:
        """載入 PO 資料"""
        try:
            file_config = self.file_paths['raw_po']

            if isinstance(file_config, dict):
                file_path = file_config.get('path')
                params = file_config.get('params', {})
            else:
                file_path = file_config
                params = {}

            source = await DataSourceFactory.create_source(file_path, **params)
            df = await source.read()

            # 從檔名提取日期
            file_date = self._extract_date_from_filename(file_path)

            return df, file_date

        except Exception as e:
            self.logger.error(f"Failed to load PO data: {str(e)}")
            return None

    async def _load_pr_data(self) -> Tuple[pd.DataFrame, int]:
        """載入 PR 資料"""
        try:
            file_config = self.file_paths['raw_pr']

            if isinstance(file_config, dict):
                file_path = file_config.get('path')
                params = file_config.get('params', {})
            else:
                file_path = file_config
                params = {}

            source = await DataSourceFactory.create_source(file_path, **params)
            df = await source.read()

            # 從檔名提取日期
            file_date = self._extract_date_from_filename(file_path)

            return df, file_date

        except Exception as e:
            self.logger.error(f"Failed to load PR data: {str(e)}")
            return None

    async def _load_procurement_previous(self) -> Dict[str, Any]:
        """
        載入採購前期底稿（PO 和 PR sheets）

        Returns:
            Dict: {'po': DataFrame, 'pr': DataFrame, 'file_path': str}
        """
        result = {
            'po': None,
            'pr': None,
            'file_path': None
        }

        try:
            file_config = self.file_paths['procurement_previous']

            if isinstance(file_config, dict):
                file_path = file_config.get('path')
                base_params = file_config.get('params', {})
            else:
                file_path = file_config
                base_params = {}

            result['file_path'] = file_path

            # 載入 PO sheet
            try:
                po_params = base_params.copy()
                po_params['sheet_name'] = 'PO'
                source_po = await DataSourceFactory.create_source(file_path, **po_params)
                result['po'] = await source_po.read()
            except Exception as e:
                self.logger.warning(f"Failed to load PO sheet from previous workpaper: {str(e)}")

            # 載入 PR sheet
            try:
                pr_params = base_params.copy()
                pr_params['sheet_name'] = 'PR'
                source_pr = await DataSourceFactory.create_source(file_path, **pr_params)
                result['pr'] = await source_pr.read()
            except Exception as e:
                self.logger.warning(f"Failed to load PR sheet from previous workpaper: {str(e)}")

        except Exception as e:
            self.logger.error(f"Failed to load procurement previous workpaper: {str(e)}")

        return result

    def _extract_date_from_filename(self, file_path: str) -> int:
        """
        從檔名提取日期 (YYYYMM)

        Args:
            file_path: 檔案路徑

        Returns:
            int: YYYYMM 格式的日期
        """
        import re
        from pathlib import Path

        filename = Path(file_path).name

        # 嘗試匹配 YYYYMM 格式
        match = re.search(r'(\d{6})', filename)
        if match:
            return int(match.group(1))

        # 無法提取，返回預設值
        self.logger.warning(f"Cannot extract date from filename: {filename}, using default 000000")
        return 0

    def _generate_load_summary(self, summary: Dict) -> str:
        """生成載入摘要報告"""
        lines = ["=" * 60, "Combined Procurement Data Loading Summary", "=" * 60]

        lines.append(f"File Date: {summary['file_date']}")
        lines.append("")

        # PO 資料
        lines.append(f"PO Data:     {'✓ Loaded' if summary['po_loaded'] else '✗ Not Loaded'}")
        if summary['po_loaded']:
            lines.append(f"  - Rows: {summary['po_rows']}")

        # PR 資料
        lines.append(f"PR Data:     {'✓ Loaded' if summary['pr_loaded'] else '✗ Not Loaded'}")
        if summary['pr_loaded']:
            lines.append(f"  - Rows: {summary['pr_rows']}")

        lines.append("")

        # 前期底稿
        lines.append(
            f"PO Previous: {'✓ Loaded' if summary['po_previous_loaded'] else '✗ Not Loaded'}"
        )
        lines.append(
            f"PR Previous: {'✓ Loaded' if summary['pr_previous_loaded'] else '✗ Not Loaded'}"
        )

        lines.append("=" * 60)
        return "\n".join(lines)

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        # 檢查至少有 raw_po 或 raw_pr
        has_po = 'raw_po' in self.file_paths
        has_pr = 'raw_pr' in self.file_paths

        if not has_po and not has_pr:
            self.logger.error("Neither raw_po nor raw_pr provided")
            return False

        return True
