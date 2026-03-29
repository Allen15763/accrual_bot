import os
import time
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime
import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder,
    create_error_metadata
)
from accrual_bot.core.datasources import (
    DataSourceFactory,
    DataSourcePool
)


class SPXExportStep(PipelineStep):
    """
    SPX 導出步驟
    
    功能: 將處理完成的數據導出到 Excel
    
    輸入: Processed DataFrame
    輸出: Excel file path
    """
    
    def __init__(self, 
                 name: str = "SPXExport",
                 output_dir: str = "output",
                 **kwargs):
        super().__init__(name, description="Export SPX processed data", **kwargs)
        self.output_dir = output_dir
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行導出"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            # 清理 <NA> 值
            df_export = df.replace('<NA>', pd.NA)
            
            # 生成文件名
            processing_date = context.metadata.processing_date
            entity_type = context.metadata.entity_type
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            
            filename = f"{entity_type}_PO_{processing_date}_processed_{timestamp}.xlsx"
            
            # 創建輸出目錄
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
            
            output_path = os.path.join(self.output_dir, filename)
            
            # 確保文件名唯一
            counter = 1
            while os.path.exists(output_path):
                filename = f"{entity_type}_PO_{processing_date}_processed_{timestamp}_{counter}.xlsx"
                output_path = os.path.join(self.output_dir, filename)
                counter += 1
            
            # 導出 Excel
            # df_export.to_excel(output_path, index=False)
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df_export.to_excel(writer, sheet_name='PO', index=False)
                context.get_auxiliary_data('locker_non_discount').to_excel(writer, 
                                                                           sheet_name='locker_non_discount')
                context.get_auxiliary_data('locker_discount').to_excel(writer, 
                                                                       sheet_name='locker_discount')
                context.get_auxiliary_data('kiosk_data').to_excel(writer, 
                                                                  sheet_name='kiosk_data')
            
            self.logger.info(f"Data exported to: {output_path}")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Exported to {output_path}",
                duration=duration,
                metadata={
                    'output_path': output_path,
                    'rows_exported': len(df_export),
                    'columns_exported': len(df_export.columns)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Export failed: {str(e)}", exc_info=True)
            context.add_error(f"Export failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data to export")
            return False
        
        return True
    

class AccountingOPSExportingStep(PipelineStep):
    """
    會計與 OPS 底稿比對結果輸出步驟
    用於將會計前期底稿、OPS 驗收檔案及比對結果輸出到 Excel
    
    功能：
    1. 從 ProcessingContext 取得三份資料
       - accounting_workpaper: 會計前期底稿
       - ops_validation: OPS 驗收檔案底稿
       - validation_comparison: 比對結果
    2. 使用 datasources 模組的 ExcelSource 統一寫入
    3. 輸出到單一 Excel 檔案的多個 sheet
    4. 自動生成檔案名稱（含時間戳），避免覆蓋
    5. 提供詳細的執行 metadata
    
    使用範例：
        step = AccountingOPSExportingStep(
            name="ExportResults",
            output_dir="output",
            filename_template="{entity}_{type}_{date}_{timestamp}.xlsx",
            sheet_names={
                'accounting_workpaper': 'acc_raw',
                'ops_validation': 'ops_raw',
                'validation_comparison': 'result'
            }
        )
    """
    
    # 預設的 sheet 名稱對應
    DEFAULT_SHEET_NAMES = {
        'accounting_workpaper': 'acc_raw',
        'ops_validation': 'ops_raw',
        'validation_comparison': 'result'
    }
    
    def __init__(
        self,
        name: str = "AccountingOPSExporting",
        output_dir: str = "output",
        filename_template: str = "{entity}_{type}_{date}_{timestamp}.xlsx",
        sheet_names: Optional[Dict[str, str]] = None,
        include_index: bool = False,
        **kwargs
    ):
        """
        初始化步驟
        
        Args:
            name: 步驟名稱
            output_dir: 輸出目錄路徑
            filename_template: 檔案名稱模板，支援格式化變數：
                {entity}: 實體類型 (SPX/MOB/SPT)
                {type}: 處理類型 (PO/PR)
                {date}: 處理日期 (YYYYMM)
                {timestamp}: 時間戳 (YYYYMMDD_HHMMSS)
            sheet_names: 自訂 sheet 名稱對應，格式：
                {
                    'accounting_workpaper': '會計底稿',
                    'ops_validation': 'OPS底稿',
                    'validation_comparison': '比對結果'
                }
            include_index: 是否包含 DataFrame 的 index
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(
            name,
            description="Export accounting and OPS validation results to Excel",
            **kwargs
        )
        
        self.output_dir = Path(output_dir)
        self.filename_template = filename_template
        self.sheet_names = sheet_names or self.DEFAULT_SHEET_NAMES
        self.include_index = include_index
        
        # 數據源連接池
        self.pool = DataSourcePool()
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行輸出"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            self.logger.info("Starting export of accounting and OPS validation results...")
            
            # 階段 1: 從 context 取得資料
            data_dict = self._get_data_from_context(context)
            
            # 階段 2: 生成輸出檔案路徑
            output_path = self._generate_output_path(context)
            
            # 階段 3: 確保輸出目錄存在
            self._ensure_output_directory()
            
            # 階段 4: 使用 datasources 模組寫入 Excel
            sheets_written = await self._write_to_excel(output_path, data_dict)
            
            # 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            # 構建 metadata
            total_rows = sum(len(df) for df in data_dict.values())
            
            metadata = (
                StepMetadataBuilder()
                .set_row_counts(total_rows, total_rows)
                .set_process_counts(processed=total_rows)
                .set_time_info(start_datetime, end_datetime)
                .add_custom('output_path', str(output_path))
                .add_custom('sheets_written', sheets_written)
                .add_custom('file_size_bytes', output_path.stat().st_size)
                .add_custom('accounting_rows', len(data_dict.get('accounting_workpaper', pd.DataFrame())))
                .add_custom('ops_rows', len(data_dict.get('ops_validation', pd.DataFrame())))
                .add_custom('comparison_rows', len(data_dict.get('validation_comparison', pd.DataFrame())))
                .build()
            )
            
            self.logger.info(
                f"Successfully exported {len(sheets_written)} sheets "
                f"to {output_path} in {duration:.2f}s"
            )
            
            # 儲存輸出路徑到 context（供後續步驟使用）
            context.set_variable('export_output_path', str(output_path))
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=(
                    f"Exported {len(sheets_written)} sheets to {output_path.name}\n"
                    f"Sheets: {', '.join(sheets_written)}"
                ),
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            self.logger.error(f"Export failed: {str(e)}", exc_info=True)
            context.add_error(f"Export failed: {str(e)}")
            
            error_metadata = create_error_metadata(
                e, context, self.name,
                output_dir=str(self.output_dir),
                stage='export'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Failed to export data: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
        
        finally:
            # 清理資源
            await self._cleanup_resources()
    
    def _get_data_from_context(
        self,
        context: ProcessingContext
    ) -> Dict[str, pd.DataFrame]:
        """
        從 context 取得所有需要輸出的資料
        
        Args:
            context: 處理上下文
            
        Returns:
            Dict[str, pd.DataFrame]: 資料字典
            
        Raises:
            ValueError: 如果缺少必要資料
        """
        data_dict = {}
        missing_data = []
        
        # 定義需要的資料及是否為必要
        required_data = {
            'accounting_workpaper': True,   # 必要
            'ops_validation': True,          # 必要
            'validation_comparison': True    # 必要
        }
        
        for data_name, is_required in required_data.items():
            df = context.get_auxiliary_data(data_name)
            
            if df is None or df.empty:
                if is_required:
                    missing_data.append(data_name)
                    self.logger.error(f"Required data not found: {data_name}")
                else:
                    self.logger.warning(f"Optional data not found: {data_name}")
            else:
                data_dict[data_name] = df.copy()
                self.logger.info(f"Retrieved {data_name}: {df.shape}")
        
        if missing_data:
            raise ValueError(
                f"Missing required data in context: {', '.join(missing_data)}"
            )
        
        return data_dict
    
    def _generate_output_path(self, context: ProcessingContext) -> Path:
        """
        生成輸出檔案路徑
        
        Args:
            context: 處理上下文
            
        Returns:
            Path: 輸出檔案路徑
        """
        # 準備格式化變數
        format_vars = {
            'entity': context.metadata.entity_type,
            'type': context.metadata.processing_type,
            'date': context.metadata.processing_date,
            'timestamp': datetime.now().strftime('%Y%m%d_%H%M%S')
        }
        
        # 格式化檔案名稱
        filename = self.filename_template.format(**format_vars)
        
        # 生成完整路徑
        output_path = self.output_dir / filename
        
        # 確保檔案名稱唯一（避免覆蓋）
        counter = 1
        original_path = output_path
        while output_path.exists():
            stem = original_path.stem
            suffix = original_path.suffix
            filename = f"{stem}_{counter}{suffix}"
            output_path = self.output_dir / filename
            counter += 1
        
        self.logger.info(f"Generated output path: {output_path}")
        return output_path
    
    def _ensure_output_directory(self):
        """確保輸出目錄存在"""
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created output directory: {self.output_dir}")
    
    async def _write_to_excel(
        self,
        output_path: Path,
        data_dict: Dict[str, pd.DataFrame]
    ) -> List[str]:
        """
        使用 pd.ExcelWriter 寫入 Excel（多 sheet）
        
        Args:
            output_path: 輸出檔案路徑
            data_dict: 資料字典
            
        Returns:
            List[str]: 成功寫入的 sheet 名稱列表
        """
        sheets_written = []
        
        try:
            # 使用 pd.ExcelWriter 確保多 sheet 寫入的正確性
            # 這裡暫時不使用 datasources，因為 ExcelSource 的 write() 
            # 方法在多 sheet 寫入時可能不夠靈活
            
            # 方案 1: 直接使用 pd.ExcelWriter（推薦，更可靠）
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for data_name, df in data_dict.items():
                    sheet_name = self.sheet_names.get(data_name, data_name)
                    
                    # 清理 <NA> 值
                    df_export = df.replace('<NA>', pd.NA)
                    
                    # 寫入 sheet
                    df_export.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=self.include_index
                    )
                    
                    sheets_written.append(sheet_name)
                    self.logger.info(
                        f"Wrote sheet '{sheet_name}': {len(df_export)} rows, "
                        f"{len(df_export.columns)} columns"
                    )
            
            # 方案 2: 使用 datasources（如果需要）
            # 注意：需要依序寫入多個 sheet
            # source = DataSourceFactory.create_from_file(
            #     str(output_path),
            #     engine='openpyxl'
            # )
            # self.pool.add_source('output_excel', source)
            # 
            # for i, (data_name, df) in enumerate(data_dict.items()):
            #     sheet_name = self.sheet_names.get(data_name, data_name)
            #     df_export = df.replace('<NA>', pd.NA)
            #     
            #     # 第一個 sheet 使用覆寫模式，後續使用追加模式
            #     mode = 'w' if i == 0 else 'a'
            #     
            #     success = await source.write(
            #         df_export,
            #         sheet_name=sheet_name,
            #         index=self.include_index,
            #         mode=mode,
            #         if_sheet_exists='replace'
            #     )
            #     
            #     if success:
            #         sheets_written.append(sheet_name)
            
            return sheets_written
            
        except Exception as e:
            self.logger.error(f"Error writing to Excel: {str(e)}")
            raise
    
    async def _cleanup_resources(self):
        """清理資源"""
        try:
            await self.pool.close_all()
            self.logger.debug("All data sources closed")
        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {str(e)}")
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 驗證是否通過
        """
        # 檢查必要資料是否存在
        required_data = [
            'accounting_workpaper',
            'ops_validation',
            'validation_comparison'
        ]
        
        missing_data = []
        for data_name in required_data:
            if not context.has_auxiliary_data(data_name):
                missing_data.append(data_name)
                self.logger.error(f"Required data not found: {data_name}")
        
        if missing_data:
            context.add_error(
                f"Missing required data for export: {', '.join(missing_data)}"
            )
            return False
        
        # 檢查輸出目錄路徑是否有效
        try:
            # 嘗試創建目錄（如果不存在）
            if not self.output_dir.exists():
                self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Invalid output directory: {str(e)}")
            context.add_error(f"Invalid output directory: {str(e)}")
            return False
        
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """
        回滾操作
        
        Args:
            context: 處理上下文
            error: 觸發回滾的錯誤
        """
        self.logger.warning(
            f"Rolling back export due to error: {str(error)}"
        )
        
        # 檢查是否有部分寫入的檔案，如果有則嘗試刪除
        output_path_str = context.get_variable('export_output_path')
        if output_path_str:
            output_path = Path(output_path_str)
            if output_path.exists():
                try:
                    output_path.unlink()
                    self.logger.info(f"Deleted partial output file: {output_path}")
                except Exception as e:
                    self.logger.error(
                        f"Failed to delete partial output file: {str(e)}"
                    )
        
        # 清理資源
        await self._cleanup_resources()


class SPXPRExportStep(PipelineStep):
    """
    SPX PR 導出步驟
    
    功能：將處理完成的 PR 數據導出到 Excel
    
    與 SPXExportStep 的差異：
    - 只輸出主數據（context.data），不處理輔助數據
    - 檔案名稱格式：{entity_type}_PR_{processing_date}_processed_{timestamp}.xlsx
    - 單一 sheet 輸出（sheet名稱：'PR'）
    - 更簡化的邏輯，專注於 PR 數據
    
    使用範例：
        step = SPXPRExportStep(
            name="ExportPRData",
            output_dir="output",
            sheet_name="PR"
        )
    """
    
    def __init__(
        self,
        name: str = "SPXPRExport",
        output_dir: str = "output",
        sheet_name: str = "PR",
        include_index: bool = False,
        **kwargs
    ):
        """
        初始化 PR 導出步驟
        
        Args:
            name: 步驟名稱
            output_dir: 輸出目錄路徑
            sheet_name: Excel sheet 名稱
            include_index: 是否包含 DataFrame 的 index
            **kwargs: 其他 PipelineStep 參數
        """
        super().__init__(
            name,
            description="Export SPX PR processed data to Excel",
            **kwargs
        )
        
        self.output_dir = Path(output_dir)
        self.sheet_name = sheet_name
        self.include_index = include_index
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 PR 數據導出"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            entity_type = context.metadata.entity_type
            processing_type = context.metadata.processing_type
            self.logger.info("=" * 70)
            self.logger.info(f"📤 開始導出 {entity_type} {processing_type} 處理結果")
            self.logger.info("=" * 70)
            
            # 階段 1: 獲取數據
            df = context.data.copy()
            
            if df.empty:
                raise ValueError("主數據為空，無法導出")
            
            # 階段 2: 清理數據
            df_export = self._clean_data(df)
            
            # 階段 3: 生成輸出路徑
            output_path = self._generate_output_path(context)
            
            # 階段 4: 確保輸出目錄存在
            self._ensure_output_directory()
            
            # 階段 5: 寫入 Excel
            self._write_to_excel(output_path, df_export)
            
            # 計算執行時間
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            # 構建 metadata
            metadata = (
                StepMetadataBuilder()
                .set_row_counts(len(df_export), len(df_export))
                .set_process_counts(processed=len(df_export))
                .set_time_info(start_datetime, end_datetime)
                .add_custom('output_path', str(output_path))
                .add_custom('file_size_bytes', output_path.stat().st_size)
                .add_custom('columns_exported', len(df_export.columns))
                .add_custom('sheet_name', self.sheet_name)
                .build()
            )
            
            self.logger.info("=" * 70)
            self.logger.info(f"✅ {self.sheet_name} 數據導出完成")
            self.logger.info(f"📁 輸出路徑：{output_path}")
            self.logger.info(f"📊 導出記錄：{len(df_export):,} 筆")
            self.logger.info(f"📋 欄位數量：{len(df_export.columns)} 個")
            self.logger.info(f"⏱️  執行時間：{duration:.2f} 秒")
            self.logger.info("=" * 70)
            
            # 儲存輸出路徑到 context（供後續使用或參考）
            context.set_variable(f'{processing_type.lower()}_export_output_path', str(output_path))
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"成功導出 {len(df_export):,} 筆 {processing_type} 數據到 {output_path.name}",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            self.logger.error(f"❌ {processing_type} 數據導出失敗：{str(e)}", exc_info=True)
            context.add_error(f"{processing_type} 導出失敗：{str(e)}")
            
            error_metadata = create_error_metadata(
                e, context, self.name,
                output_dir=str(self.output_dir),
                stage=f'{processing_type.lower()}_export'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"導出失敗：{str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理數據，準備導出
        
        清理操作：
        1. 替換 <NA> 為 pandas NA
        2. 移除完全空白的行（可選）
        
        Args:
            df: 原始數據
            
        Returns:
            pd.DataFrame: 清理後的數據
        """
        self.logger.debug("清理導出數據...")
        
        # 替換 <NA> 值
        df_clean = df.replace('<NA>', pd.NA)
        
        # 記錄清理結果
        self.logger.debug(f"  ✓ 數據清理完成：{len(df_clean)} 行")
        
        return df_clean
    
    def _generate_output_path(self, context: ProcessingContext) -> Path:
        """
        生成輸出檔案路徑
        
        檔案命名格式：{entity_type}_PR_{processing_date}_processed_{timestamp}.xlsx
        
        Args:
            context: 處理上下文
            
        Returns:
            Path: 輸出檔案路徑
        """
        # 準備檔案名稱組件
        entity_type = context.metadata.entity_type
        processing_date = context.metadata.processing_date
        processing_type = context.metadata.processing_type
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 生成檔案名稱
        filename = f"{entity_type}_{processing_type}_{processing_date}_processed_{timestamp}.xlsx"
        
        # 生成完整路徑
        output_path = self.output_dir / filename
        
        # 確保檔案名稱唯一（避免覆蓋）
        counter = 1
        original_path = output_path
        while output_path.exists():
            stem = original_path.stem
            suffix = original_path.suffix
            filename = f"{stem}_{counter}{suffix}"
            output_path = self.output_dir / filename
            counter += 1
            
            if counter > 100:  # 安全限制
                raise RuntimeError("無法生成唯一的檔案名稱（已嘗試 100 次）")
        
        self.logger.debug(f"生成輸出路徑：{output_path}")
        return output_path
    
    def _ensure_output_directory(self):
        """確保輸出目錄存在"""
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"創建輸出目錄：{self.output_dir}")
    
    def _write_to_excel(self, output_path: Path, df: pd.DataFrame):
        """
        寫入 Excel 檔案
        
        使用 pd.ExcelWriter 確保數據正確寫入
        
        Args:
            output_path: 輸出檔案路徑
            df: 要寫入的數據
        """
        self.logger.debug(f"寫入 Excel：{output_path}")
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(
                    writer,
                    sheet_name=self.sheet_name,
                    index=self.include_index
                )
            
            self.logger.debug(
                f"  ✓ 成功寫入 sheet '{self.sheet_name}'："
                f"{len(df)} 行，{len(df.columns)} 列"
            )
            
        except Exception as e:
            self.logger.error(f"寫入 Excel 失敗：{str(e)}")
            raise
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入數據
        
        檢查項目：
        1. 主數據不為空
        2. 處理類型為 PR
        3. 輸出目錄路徑有效
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 驗證是否通過
        """
        # 檢查主數據
        if context.data is None or context.data.empty:
            self.logger.error("❌ 主數據為空，無法導出")
            context.add_error("主數據為空，無法導出")
            return False
        
        # 檢查處理類型（可選，取決於是否需要強制 PR）
        if context.metadata.processing_type != "PR":
            self.logger.warning(
                f"⚠️  處理類型不是 PR（當前：{context.metadata.processing_type}），"
                "但仍繼續導出"
            )
        
        # 檢查輸出目錄路徑是否有效
        try:
            # 嘗試創建目錄（如果不存在）
            if not self.output_dir.exists():
                self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"❌ 無效的輸出目錄：{str(e)}")
            context.add_error(f"無效的輸出目錄：{str(e)}")
            return False
        
        self.logger.debug("✅ 輸入驗證通過")
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """
        回滾操作
        
        如果導出過程中產生部分檔案，嘗試刪除
        
        Args:
            context: 處理上下文
            error: 觸發回滾的錯誤
        """
        self.logger.warning(f"⚠️  回滾 PR 導出：{str(error)}")
        
        # 檢查是否有部分寫入的檔案
        processing_type = context.metadata.processing_type
        output_path_str = context.get_variable(f'{processing_type.lower()}_export_output_path')
        if output_path_str:
            output_path = Path(output_path_str)
            if output_path.exists():
                try:
                    output_path.unlink()
                    self.logger.info(f"✓ 已刪除部分輸出檔案：{output_path}")
                except Exception as e:
                    self.logger.error(f"❌ 無法刪除部分輸出檔案：{str(e)}")
