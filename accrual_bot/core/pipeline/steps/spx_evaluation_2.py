"""
訂金屬性的 PO 狀態更新步驟

針對 Item Description 中包含「訂金」字樣的 PO，
根據最大的 Expected Received Month 判斷是否需要更新為「已完成」狀態

建議放置位置: 在 StatusStage1Step 和 SPXERMLogicStep 之間
文件位置: accrual_bot/core/pipeline/steps/spx_evaluation.py
"""

import time
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder, create_error_metadata


class DepositStatusUpdateStep(PipelineStep):
    """
    訂金 PO 狀態更新步驟
    
    業務邏輯:
    1. 篩選 Item Description 包含「訂金」的記錄
    2. 以 PO# 為 key 進行分組
    3. 找出每個 PO# 的最大 Expected Received Month_轉換格式
    4. 若最大月份等於當月，則該 PO# 的所有記錄標記為「已完成(deposit)」
    5. 其他記錄保持原狀態不變
    
    輸入要求:
    - DataFrame 需包含欄位: PO#, Item Description, Expected Received Month_轉換格式, PO狀態
    - processing_date 格式為 YYYYMM
    
    輸出:
    - 更新 PO狀態 欄位的 DataFrame
    """
    
    def __init__(self, 
                 name: str = "Update_Deposit_PO_Status",
                 description_column: str = "Item Description",
                 po_column: str = "PO#",
                 date_column: str = "Expected Received Month_轉換格式",
                 status_column: str = "PO狀態",
                 deposit_keyword: str = "訂金",
                 completed_status: str = "已完成(deposit)",
                 **kwargs):
        """
        初始化訂金狀態更新步驟
        
        Args:
            name: 步驟名稱
            description_column: 品項描述欄位名稱
            po_column: PO編號欄位名稱
            date_column: 預期收貨月份欄位名稱
            status_column: PO狀態欄位名稱
            deposit_keyword: 訂金關鍵字（可調整為「押金」等）
            completed_status: 完成狀態的標籤文字
        """
        super().__init__(
            name=name,
            description="Update PO status for deposit items based on max received month",
            **kwargs
        )
        self.description_column = description_column
        self.po_column = po_column
        self.date_column = date_column
        self.status_column = status_column
        self.deposit_keyword = deposit_keyword
        self.completed_status = completed_status
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行訂金 PO 狀態更新邏輯"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            df = context.data.copy()
            input_count = len(df)
            processing_date = context.metadata.processing_date
            current_month = processing_date  # YYYYMM 格式
            
            self.logger.info("=" * 60)
            self.logger.info("🔄 開始執行訂金 PO 狀態更新...")
            self.logger.info(f"📅 當前處理月份: {current_month}")
            self.logger.info(f"📊 總記錄數: {input_count:,}")
            self.logger.info("=" * 60)
            
            # === 階段 1: 數據驗證 ===
            validation_result = self._validate_data(df, current_month)
            if not validation_result['is_valid']:
                return self._create_validation_failed_result(
                    validation_result, 
                    df, 
                    time.time() - start_time
                )
            
            # === 階段 2: 篩選訂金相關記錄 ===
            exclude_cols = ['已入帳']
            deposit_mask = df[self.description_column].astype(str).str.contains(
                self.deposit_keyword, 
                case=False, 
                na=False
            )
            status_mask = (~df['PO狀態'].isin(exclude_cols))
            deposit_df = df[deposit_mask & status_mask].copy()
            deposit_count = len(deposit_df)
            
            self.logger.info(f"🔍 篩選出包含「{self.deposit_keyword}」的記錄: {deposit_count:,} 筆")
            
            if deposit_count == 0:
                self.logger.info("ℹ️  無訂金相關記錄，跳過狀態更新")
                return self._create_skipped_result(df, time.time() - start_time)
            
            # === 階段 3: 按 PO# 分組並找出最大月份 ===
            self.logger.info(f"📋 開始按 {self.po_column} 分組分析...")
            
            # 計算每個 PO# 的最大收貨月份
            
            deposit_df = df.loc[df['PO#'].isin(deposit_df['PO#'].unique()), :].copy()
            max_month_by_po = (deposit_df
                               .groupby(self.po_column)[self.date_column]
                               .max()
                               .to_dict()
                               )
            
            unique_pos = len(max_month_by_po)
            self.logger.info(f"📦 涉及的 PO 數量: {unique_pos:,} 個")
            
            # === 階段 4: 判斷並更新狀態 ===
            pos_to_complete = []
            for po_num, max_month in max_month_by_po.items():
                if pd.notna(max_month) and max_month <= current_month:
                    pos_to_complete.append(po_num)
            
            self.logger.info(f"✅ 需要標記為「{self.completed_status}」的 PO: {len(pos_to_complete):,} 個")
            
            # 更新狀態
            update_mask = (
                deposit_mask & 
                status_mask & 
                df[self.po_column].isin(pos_to_complete)
            )
            
            original_status = df.loc[update_mask, self.status_column].copy()
            df.loc[update_mask, self.status_column] = self.completed_status
            updated_count = update_mask.sum()
            
            self.logger.info(f"🔄 實際更新的記錄數: {updated_count:,} 筆")
            
            # === 階段 5: 生成詳細統計 ===
            statistics = self._generate_statistics(
                df=df,
                deposit_count=deposit_count,
                unique_pos=unique_pos,
                pos_to_complete=pos_to_complete,
                updated_count=updated_count,
                max_month_by_po=max_month_by_po,
                current_month=current_month
            )
            
            # === 階段 6: 記錄詳細日誌 ===
            self._log_detailed_statistics(statistics)
            
            # === 階段 7: 更新上下文 ===
            context.update_data(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            self.logger.info("=" * 60)
            self.logger.info(f"✅ 訂金 PO 狀態更新完成 (耗時: {duration:.2f}秒)")
            self.logger.info("=" * 60)
            
            # 構建 metadata
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, len(df))
                        .set_process_counts(
                            processed=deposit_count,
                            skipped=input_count - deposit_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('deposit_records', deposit_count)
                        .add_custom('unique_pos', unique_pos)
                        .add_custom('pos_marked_completed', len(pos_to_complete))
                        .add_custom('records_updated', updated_count)
                        .add_custom('current_month', current_month)
                        .add_custom('statistics', statistics)
                        .build()
                        )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"更新 {updated_count} 筆訂金 PO 狀態為「{self.completed_status}」",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"❌ 訂金 PO 狀態更新失敗: {str(e)}", exc_info=True)
            context.add_error(f"Deposit PO status update failed: {str(e)}")
            
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='deposit_status_update'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"訂金狀態更新失敗: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入數據的完整性
        
        檢查項目:
        1. DataFrame 不為空
        2. 必要欄位存在
        3. processing_date 已設定
        """
        try:
            df = context.data
            
            # 檢查 DataFrame
            if df is None or df.empty:
                self.logger.error("❌ 輸入數據為空")
                return False
            
            # 檢查必要欄位
            required_columns = [
                self.description_column,
                self.po_column,
                self.date_column,
                self.status_column
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"❌ 缺少必要欄位: {missing_columns}")
                return False
            
            # 檢查 processing_date
            processing_date = context.metadata.processing_date
            if not processing_date:
                self.logger.error("❌ 未設定 processing_date")
                return False
            
            self.logger.info("✅ 輸入驗證通過")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 驗證失敗: {str(e)}", exc_info=True)
            return False
    
    def _validate_data(self, df: pd.DataFrame, current_month: int) -> Dict[str, Any]:
        """
        詳細的數據驗證
        
        Returns:
            Dict 包含 is_valid 和 errors
        """
        errors = []
        
        # 檢查月份格式
        if not (100000 <= current_month <= 999999):
            errors.append(f"processing_date 格式錯誤: {current_month}，應為 YYYYMM 格式")
        
        # 檢查狀態欄位是否存在
        if self.status_column not in df.columns:
            self.logger.warning(f"⚠️  {self.status_column} 欄位不存在，將自動創建")
            df[self.status_column] = None
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors
        }
    
    def _generate_statistics(self,
                             df: pd.DataFrame,
                             deposit_count: int,
                             unique_pos: int,
                             pos_to_complete: List[str],
                             updated_count: int,
                             max_month_by_po: Dict,
                             current_month: int) -> Dict[str, Any]:
        """生成詳細統計信息"""
        
        # PO 號列表
        completed_po_list = pos_to_complete[:10]  # 只顯示前 10 個
        
        # 統計各狀態的數量
        status_distribution = df[self.status_column].value_counts().to_dict()
        
        # 計算完成率
        completion_rate = (len(pos_to_complete) / unique_pos * 100) if unique_pos > 0 else 0
        
        return {
            'total_records': len(df),
            'deposit_records': deposit_count,
            'deposit_percentage': f"{deposit_count/len(df)*100:.2f}%",
            'unique_deposit_pos': unique_pos,
            'pos_marked_completed': len(pos_to_complete),
            'completion_rate': f"{completion_rate:.2f}%",
            'records_updated': updated_count,
            'current_month': current_month,
            'status_distribution': status_distribution,
            'sample_completed_pos': completed_po_list,
            'max_months_sample': dict(list(max_month_by_po.items())[:5])
        }
    
    def _log_detailed_statistics(self, stats: Dict[str, Any]):
        """記錄詳細統計日誌"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 訂金 PO 狀態更新統計報告")
        self.logger.info("=" * 60)
        self.logger.info(f"📈 總記錄數: {stats['total_records']:,}")
        self.logger.info(f"🔖 訂金記錄數: {stats['deposit_records']:,} ({stats['deposit_percentage']})")
        self.logger.info(f"📦 涉及 PO 數: {stats['unique_deposit_pos']:,}")
        self.logger.info(f"✅ 標記完成 PO: {stats['pos_marked_completed']:,} ({stats['completion_rate']})")
        self.logger.info(f"🔄 更新記錄數: {stats['records_updated']:,}")
        self.logger.info(f"📅 當前月份: {stats['current_month']}")
        
        if stats['sample_completed_pos']:
            self.logger.info("\n💼 已完成 PO 樣本 (前10個):")
            for po in stats['sample_completed_pos']:
                self.logger.info(f"   • {po}")
        
        self.logger.info("\n📋 狀態分布:")
        for status, count in stats['status_distribution'].items():
            self.logger.info(f"   • {status}: {count:,}")
        
        self.logger.info("=" * 60 + "\n")
    
    def _create_validation_failed_result(self, 
                                         validation_result: Dict, 
                                         df: pd.DataFrame,
                                         duration: float) -> StepResult:
        """創建驗證失敗的結果"""
        error_msg = "; ".join(validation_result['errors'])
        self.logger.error(f"❌ 數據驗證失敗: {error_msg}")
        
        return StepResult(
            step_name=self.name,
            status=StepStatus.FAILED,
            data=df,
            message=f"數據驗證失敗: {error_msg}",
            duration=duration,
            metadata={'validation_errors': validation_result['errors']}
        )
    
    def _create_skipped_result(self, df: pd.DataFrame, duration: float) -> StepResult:
        """創建跳過執行的結果"""
        return StepResult(
            step_name=self.name,
            status=StepStatus.SKIPPED,
            data=df,
            message=f"無包含「{self.deposit_keyword}」的記錄，跳過狀態更新",
            duration=duration,
            metadata={'reason': 'no_deposit_records'}
        )


# =============================================================================
# 使用範例
# =============================================================================

async def example_usage():
    """展示如何使用 DepositStatusUpdateStep"""
    import asyncio
    
    # 創建測試數據
    test_data = pd.DataFrame({
        'PO#': ['PO001', 'PO001', 'PO002', 'PO002', 'PO003'],
        'Item Description': ['訂金-設備', '設備安裝', '訂金-軟體', '軟體授權', '一般採購'],
        'Expected Received Month_轉換格式': [202510, 202509, 202510, 202510, 202509],
        'PO狀態': [None, None, None, None, None]
    })
    
    # 創建處理上下文
    context = ProcessingContext(
        data=test_data,
        entity_type='SPX',
        processing_date=202510,
        processing_type='PO'
    )
    
    # 創建步驟實例
    step = DepositStatusUpdateStep(
        name="Update_Deposit_Status",
        required=True
    )
    
    # 執行步驟
    result = await step(context)
    
    # 檢查結果
    if result.is_success:
        print("✅ 執行成功！")
        print(f"\n更新後的數據:\n{context.data}")
        print(f"\n統計信息:\n{result.metadata.get('statistics')}")
    else:
        print(f"❌ 執行失敗: {result.message}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(example_usage())