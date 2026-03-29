"""
分潤數據更新步驟

針對 SPT PO 數據中包含分潤關鍵字的記錄，
設置對應的 GL#、Product Code 和估計入帳狀態

建議放置位置: 在 DateLogicStep 和 ERM 邏輯之間
文件位置: accrual_bot/core/pipeline/steps/spt_steps.py
"""

import time
import pandas as pd
from typing import Dict, Any, Tuple
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder, create_error_metadata
from accrual_bot.utils.config import config_manager


class CommissionDataUpdateStep(PipelineStep):
    """
    分潤數據更新步驟
    
    業務邏輯:
    1. 識別包含分潤關鍵字的記錄（Affiliate/Shopee 和 AMS）
    2. 設置 Remarked by FN = '分潤'
    3. 更新 GL# 和 Product Code
    4. 根據 GL# 和 PO狀態判斷是否估計入帳
    
    分潤類型:
    - Type 1 (Affiliate/Shopee): GL# 650022, Product Code EC_SPE_COM
    - Type 2 (AMS): GL# 650019, Product Code EC_AMS_COST
    """
    
    # 分潤配置
    COMMISSION_CONFIG = {
        'affiliate': {
            'keywords': r'(?i)Affiliate commission|Shopee commission|蝦皮分潤計劃會員分潤金',
            'exclude_keywords': ['品牌加碼'],
            'gl_number': '650022',
            'product_code': 'EC_SPE_COM',
            'remark': '分潤',
            'name': 'Affiliate/Shopee分潤'
        },
        'ams': {
            'keywords': r'(?i)AMS commission',
            'include_and_keywords': ['Affiliate分潤合作', '品牌加碼'],
            'gl_number': '650019',
            'product_code': 'EC_AMS_COST',
            'remark': '分潤',
            'name': 'AMS分潤'
        }
    }
    
    def __init__(self, 
                 name: str = "Update_Commission_Data",
                 description_column: str = "Item Description",
                 status_column: str = "PO狀態",
                 **kwargs):
        """
        初始化分潤更新步驟
        
        Args:
            name: 步驟名稱
            description_column: 品項描述欄位名稱
            status_column: PO狀態欄位名稱
        """
        super().__init__(
            name=name,
            description="Update commission data with proper GL# and Product Code",
            **kwargs
        )
        self.description_column = description_column
        self.status_column = status_column
        # 優先從 TOML config 讀取；若無配置則 fallback 至 class-level 常數
        spt_cfg = config_manager._config_toml.get('spt', {})
        self.commission_config = spt_cfg.get('commission', CommissionDataUpdateStep.COMMISSION_CONFIG)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行分潤數據更新邏輯"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            # 檢查實體類型 - 僅適用於 SPT
            if context.metadata.entity_type != 'SPT':
                self.logger.info(f"⏭️  跳過分潤更新 - 僅適用於 SPT，當前為 {context.metadata.entity_type}")
                return self._create_skipped_result(
                    context.data, 
                    "Commission update only applies to SPT entity",
                    time.time() - start_time
                )
            
            df = context.data.copy()
            input_count = len(df)
            
            self.logger.info("=" * 60)
            self.logger.info("💰 開始執行分潤數據更新...")
            self.logger.info(f"📊 總記錄數: {input_count:,}")
            self.logger.info("=" * 60)
            
            # === 階段 1: 數據驗證 ===
            validation_result = self._validate_data(df)
            if not validation_result['is_valid']:
                return self._create_validation_failed_result(
                    validation_result, 
                    df, 
                    time.time() - start_time
                )
            
            # === 階段 2: 識別分潤記錄 ===
            affiliate_mask, ams_mask = self._identify_commission_records(df)
            affiliate_count = affiliate_mask.sum()
            ams_count = ams_mask.sum()
            total_commission = affiliate_count + ams_count
            
            self.logger.info("🔍 識別分潤記錄:")
            self.logger.info(f"   • Affiliate/Shopee 分潤: {affiliate_count:,} 筆")
            self.logger.info(f"   • AMS 分潤: {ams_count:,} 筆")
            self.logger.info(f"   • 總計: {total_commission:,} 筆")
            
            if total_commission == 0:
                self.logger.info("ℹ️  無分潤相關記錄，跳過更新")
                return self._create_skipped_result(df, "No commission records found", time.time() - start_time)
            
            # === 階段 3: 更新分潤數據 ===
            self.logger.info("🔄 開始更新分潤數據...")
            
            # 更新 Affiliate/Shopee 分潤
            if affiliate_count > 0:
                self._update_commission_records(
                    df, 
                    affiliate_mask, 
                    'affiliate',
                    self.commission_config['affiliate']
                )
                self.logger.info(f"✅ 已更新 {affiliate_count:,} 筆 Affiliate/Shopee 分潤")
            
            # 更新 AMS 分潤
            if ams_count > 0:
                self._update_commission_records(
                    df, 
                    ams_mask, 
                    'ams',
                    self.commission_config['ams']
                )
                self.logger.info(f"✅ 已更新 {ams_count:,} 筆 AMS 分潤")
            
            # === 階段 4: 設置估計入帳; will be 0，此步驟僅標記remark，後續依據狀態更新估計 ===
            accrual_count = self._set_accrual_estimation(df)
            self.logger.info(f"💵 設置估計入帳: {accrual_count:,} 筆")
            
            # === 階段 5: 生成統計 ===
            statistics = self._generate_statistics(
                df=df,
                affiliate_count=affiliate_count,
                ams_count=ams_count,
                total_commission=total_commission,
                accrual_count=accrual_count,
                input_count=input_count
            )
            
            # === 階段 6: 記錄詳細日誌 ===
            self._log_detailed_statistics(statistics)
            
            # === 階段 7: 更新上下文 ===
            context.update_data(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            self.logger.info("=" * 60)
            self.logger.info(f"✅ 分潤數據更新完成 (耗時: {duration:.2f}秒)")
            self.logger.info("=" * 60)
            
            # 構建 metadata
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, len(df))
                        .set_process_counts(processed=total_commission, skipped=input_count - total_commission)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('affiliate_commission', affiliate_count)
                        .add_custom('ams_commission', ams_count)
                        .add_custom('total_commission', total_commission)
                        .add_custom('accrual_set', accrual_count)
                        .add_custom('statistics', statistics)
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"更新 {total_commission} 筆分潤數據 (Affiliate: {affiliate_count}, AMS: {ams_count})",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"❌ 分潤數據更新失敗: {str(e)}", exc_info=True)
            context.add_error(f"Commission data update failed: {str(e)}")
            
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='commission_update'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"分潤更新失敗: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入數據的完整性
        
        檢查項目:
        1. DataFrame 不為空
        2. 必要欄位存在
        3. 實體類型為 SPT
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
                'GL#',
                'Remarked by FN',
                'Account code',
                'Product code'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.warning(f"⚠️  部分欄位不存在（將自動創建）: {missing_columns}")
                # 自動創建缺失欄位
                for col in missing_columns:
                    df[col] = None
            
            # 檢查實體類型
            if context.metadata.entity_type != 'SPT':
                self.logger.info(f"ℹ️  實體類型為 {context.metadata.entity_type}，將跳過分潤更新")
            
            self.logger.info("✅ 輸入驗證通過")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 驗證失敗: {str(e)}", exc_info=True)
            return False
    
    def _validate_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        詳細的數據驗證
        
        Returns:
            Dict 包含 is_valid 和 errors
        """
        errors = []
        
        # 檢查描述欄位
        if self.description_column not in df.columns:
            errors.append(f"缺少必要欄位: {self.description_column}")
        
        # 檢查 GL# 欄位
        if 'GL#' not in df.columns:
            self.logger.warning("⚠️  GL# 欄位不存在，將自動創建")
            df['GL#'] = None
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors
        }
    
    def _identify_commission_records(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """
        識別分潤記錄
        
        Returns:
            Tuple[pd.Series, pd.Series]: (affiliate_mask, ams_mask)
        """
        # Affiliate/Shopee 分潤
        affiliate_config = self.commission_config['affiliate']
        affiliate_mask = df[self.description_column].str.contains(
            affiliate_config['keywords'], 
            na=False, 
            regex=True
        )
        
        # 排除「品牌加碼」
        for exclude_kw in affiliate_config['exclude_keywords']:
            affiliate_mask &= ~df[self.description_column].str.contains(exclude_kw, na=False)
        
        # AMS 分潤 - 情況1: 包含 AMS commission
        ams_config = self.commission_config['ams']
        ams_mask_1 = df[self.description_column].str.contains(
            ams_config['keywords'], 
            na=False, 
            regex=True
        )
        
        # AMS 分潤 - 情況2: 同時包含 Affiliate分潤合作 和 品牌加碼
        ams_mask_2 = df[self.description_column].str.contains(
            ams_config['include_and_keywords'][0], 
            na=False
        ) & df[self.description_column].str.contains(
            ams_config['include_and_keywords'][1], 
            na=False
        )
        
        ams_mask = ams_mask_1 | ams_mask_2
        
        return affiliate_mask, ams_mask
    
    def _update_commission_records(self, 
                                   df: pd.DataFrame,
                                   mask: pd.Series,
                                   commission_type: str,
                                   config: Dict) -> None:
        """
        更新分潤記錄
        
        Args:
            df: DataFrame
            mask: 記錄遮罩
            commission_type: 分潤類型 ('affiliate' or 'ams')
            config: 配置字典
        """
        if not mask.any():
            return
        
        # 更新備註
        df.loc[mask, 'Remarked by FN'] = config['remark']
        
        # 更新 GL# 和 Account code
        df.loc[mask, 'GL#'] = config['gl_number']
        df.loc[mask, 'Account code'] = config['gl_number']
        
        # 更新 Product code
        df.loc[mask, 'Product code'] = config['product_code']
    
    def _set_accrual_estimation(self, df: pd.DataFrame) -> int:
        """
        設置分潤的估計入帳狀態
        
        邏輯: 
        - GL# 為 650022 或 650019
        - Remarked by FN = '分潤'
        - PO狀態 包含「已完成」
        
        Returns:
            int: 設置估計入帳的記錄數
        """
        # 確保 GL# 為字串類型
        df['GL#'] = df['GL#'].astype(str)
        
        accrual_mask = (
            ((df['GL#'] == '650022') | (df['GL#'] == '650019')) &
            (df['Remarked by FN'] == '分潤') &
            (df[self.status_column].astype(str).str.contains('已完成', na=False))
        )
        
        df.loc[accrual_mask, '是否估計入帳'] = "Y"
        
        return accrual_mask.sum()
    
    def _generate_statistics(self,
                             df: pd.DataFrame,
                             affiliate_count: int,
                             ams_count: int,
                             total_commission: int,
                             accrual_count: int,
                             input_count: int) -> Dict[str, Any]:
        """生成詳細統計信息"""
        commission_rate = (total_commission / input_count * 100) if input_count > 0 else 0
        accrual_rate = (accrual_count / total_commission * 100) if total_commission > 0 else 0
        
        return {
            'total_records': input_count,
            'commission_records': total_commission,
            'commission_percentage': f"{commission_rate:.2f}%",
            'affiliate_commission': affiliate_count,
            'ams_commission': ams_count,
            'accrual_set': accrual_count,
            'accrual_rate': f"{accrual_rate:.2f}%",
            'gl_distribution': {
                '650022 (Affiliate/Shopee)': affiliate_count,
                '650019 (AMS)': ams_count
            }
        }
    
    def _log_detailed_statistics(self, stats: Dict[str, Any]):
        """記錄詳細統計日誌"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 分潤數據更新統計報告")
        self.logger.info("=" * 60)
        self.logger.info(f"📈 總記錄數: {stats['total_records']:,}")
        self.logger.info(f"💰 分潤記錄數: {stats['commission_records']:,} ({stats['commission_percentage']})")
        self.logger.info(f"   • Affiliate/Shopee: {stats['affiliate_commission']:,}")
        self.logger.info(f"   • AMS: {stats['ams_commission']:,}")
        self.logger.info(f"💵 估計入帳: {stats['accrual_set']:,} ({stats['accrual_rate']})")
        
        self.logger.info("\n📋 GL# 分布:")
        for gl, count in stats['gl_distribution'].items():
            self.logger.info(f"   • {gl}: {count:,}")
        
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
    
    def _create_skipped_result(self, df: pd.DataFrame, reason: str, duration: float) -> StepResult:
        """創建跳過執行的結果"""
        return StepResult(
            step_name=self.name,
            status=StepStatus.SKIPPED,
            data=df,
            message=reason,
            duration=duration,
            metadata={'reason': reason}
        )


class PayrollDetectionStep(PipelineStep):
    """
    Payroll 偵測步驟
    
    業務邏輯:
    1. 識別 EBS Task 或 Item Description 包含 "payroll" 關鍵字的記錄
    2. 僅更新空的 Remarked by FN 欄位，避免覆蓋已有標籤
    3. 設置 Remarked by FN = 'Payroll'
    
    適用範圍:
    - 僅適用於 SPT 實體
    - 檢查欄位: EBS Task, Item Description
    - 目標欄位: Remarked by FN
    """
    
    # Payroll 偵測配置
    PAYROLL_CONFIG = {
        'keywords': r'(?i)payroll',  # 不區分大小寫
        'label': 'Payroll',
        'name': 'Payroll標籤'
    }
    
    def __init__(self, 
                 name: str = "Detect_Payroll_Records",
                 ebs_task_column: str = "EBS Task",
                 description_column: str = "Item Description",
                 remark_column: str = "Remarked by FN",
                 **kwargs):
        """
        初始化 Payroll 偵測步驟
        
        Args:
            name: 步驟名稱
            ebs_task_column: EBS Task 欄位名稱
            description_column: 品項描述欄位名稱
            remark_column: 備註欄位名稱
        """
        super().__init__(
            name=name,
            description="Detect and label Payroll records based on EBS Task and Item Description",
            **kwargs
        )
        self.ebs_task_column = ebs_task_column
        self.description_column = description_column
        self.remark_column = remark_column
        # 優先從 TOML config 讀取；若無配置則 fallback 至 class-level 常數
        spt_cfg = config_manager._config_toml.get('spt', {})
        self.payroll_config = spt_cfg.get('payroll', PayrollDetectionStep.PAYROLL_CONFIG)

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 Payroll 偵測邏輯"""
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            # 檢查實體類型 - 僅適用於 SPT
            if context.metadata.entity_type != 'SPT':
                self.logger.info(f"⏭️  跳過 Payroll 偵測 - 僅適用於 SPT，當前為 {context.metadata.entity_type}")
                return self._create_skipped_result(
                    context.data, 
                    "Payroll detection only applies to SPT entity",
                    time.time() - start_time
                )
            
            df = context.data.copy()
            input_count = len(df)
            
            self.logger.info("=" * 60)
            self.logger.info("🔍 開始執行 Payroll 記錄偵測...")
            self.logger.info(f"📊 總記錄數: {input_count:,}")
            self.logger.info("=" * 60)
            
            # === 階段 1: 數據驗證 ===
            validation_result = self._validate_data(df)
            if not validation_result['is_valid']:
                return self._create_validation_failed_result(
                    validation_result, 
                    df, 
                    time.time() - start_time
                )
            
            # === 階段 2: 識別 Payroll 記錄 ===
            payroll_mask = self._identify_payroll_records(df)
            payroll_count = payroll_mask.sum()
            
            self.logger.info("🔍 識別 Payroll 記錄:")
            self.logger.info(f"   • 包含 payroll 關鍵字: {payroll_count:,} 筆")
            
            if payroll_count == 0:
                self.logger.info("ℹ️  無 Payroll 相關記錄，跳過更新")
                return self._create_skipped_result(df, "No payroll records found", time.time() - start_time)
            
            # === 階段 3: 過濾已有標籤的記錄 ===
            # 只更新 Remarked by FN 為空的記錄
            empty_remark_mask = (df[self.remark_column].isna() | 
                                 (df[self.remark_column] == '') | 
                                 (df[self.remark_column] == 'nan'))
            update_mask = payroll_mask & empty_remark_mask
            update_count = update_mask.sum()
            skipped_count = payroll_count - update_count
            
            self.logger.info("📋 標籤更新策略:")
            self.logger.info(f"   • 可更新記錄（空標籤）: {update_count:,} 筆")
            self.logger.info(f"   • 跳過記錄（已有標籤）: {skipped_count:,} 筆")
            
            if update_count == 0:
                self.logger.info("ℹ️  所有 Payroll 記錄均已有標籤，跳過更新")
                return self._create_skipped_result(df, "All payroll records already labeled", time.time() - start_time)
            
            # === 階段 4: 更新標籤 ===
            self.logger.info("🔄 開始更新 Payroll 標籤...")
            df.loc[update_mask, self.remark_column] = self.payroll_config['label']

            status_cols = [i for i in df.columns if '狀態' in i]
            if not status_cols:
                self.logger.warning("未找到含'狀態'的欄位，跳過狀態欄位更新")
            else:
                status_column = status_cols[0]
                is_status_na = df[status_column].isna()
                df.loc[update_mask & is_status_na, status_column] = self.payroll_config['label']
            self.logger.info(f"✅ 已更新 {update_count:,} 筆 Payroll 記錄")
            
            # === 階段 5: 生成統計 ===
            statistics = self._generate_statistics(
                df=df,
                payroll_count=payroll_count,
                update_count=update_count,
                skipped_count=skipped_count,
                input_count=input_count
            )
            
            # === 階段 6: 記錄詳細日誌 ===
            self._log_detailed_statistics(statistics)
            
            # === 階段 7: 更新上下文 ===
            context.update_data(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            
            self.logger.info("=" * 60)
            self.logger.info(f"✅ Payroll 偵測完成 (耗時: {duration:.2f}秒)")
            self.logger.info("=" * 60)
            
            # 構建 metadata
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, len(df))
                        .set_process_counts(processed=update_count, skipped=skipped_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('payroll_detected', payroll_count)
                        .add_custom('payroll_labeled', update_count)
                        .add_custom('already_labeled', skipped_count)
                        .add_custom('statistics', statistics)
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"偵測到 {payroll_count} 筆 Payroll 記錄，更新 {update_count} 筆標籤",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"❌ Payroll 偵測失敗: {str(e)}", exc_info=True)
            context.add_error(f"Payroll detection failed: {str(e)}")
            
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='payroll_detection'
            )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Payroll 偵測失敗: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入數據的完整性
        
        檢查項目:
        1. DataFrame 不為空
        2. 必要欄位存在
        3. 實體類型為 SPT
        """
        try:
            df = context.data
            
            # 檢查 DataFrame
            if df is None or df.empty:
                self.logger.error("❌ 輸入數據為空")
                return False
            
            # 檢查必要欄位
            required_columns = [
                self.ebs_task_column,
                self.description_column,
                self.remark_column
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                # EBS Task 可能不存在，不算致命錯誤
                if self.ebs_task_column in missing_columns and len(missing_columns) == 1:
                    self.logger.warning(f"⚠️  {self.ebs_task_column} 欄位不存在，將僅檢查 {self.description_column}")
                else:
                    # 如果 Remarked by FN 不存在，自動創建
                    if self.remark_column in missing_columns:
                        self.logger.warning(f"⚠️  {self.remark_column} 欄位不存在，將自動創建")
                        df[self.remark_column] = None
                    
                    # 如果 Item Description 也不存在，則無法執行
                    if self.description_column in missing_columns:
                        self.logger.error(f"❌ 缺少必要欄位: {self.description_column}")
                        return False
            
            # 檢查實體類型
            if context.metadata.entity_type != 'SPT':
                self.logger.info(f"ℹ️  實體類型為 {context.metadata.entity_type}，將跳過 Payroll 偵測")
            
            self.logger.info("✅ 輸入驗證通過")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 驗證失敗: {str(e)}", exc_info=True)
            return False
    
    def _validate_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        詳細的數據驗證
        
        Returns:
            Dict 包含 is_valid 和 errors
        """
        errors = []
        
        # 檢查描述欄位（必要）
        if self.description_column not in df.columns:
            errors.append(f"缺少必要欄位: {self.description_column}")
        
        # 檢查 Remarked by FN 欄位（可自動創建）
        if self.remark_column not in df.columns:
            self.logger.warning(f"⚠️  {self.remark_column} 欄位不存在，將自動創建")
            df[self.remark_column] = None
        
        # EBS Task 不是必須的
        if self.ebs_task_column not in df.columns:
            self.logger.info(f"ℹ️  {self.ebs_task_column} 欄位不存在，將僅檢查 {self.description_column}")
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors
        }
    
    def _identify_payroll_records(self, df: pd.DataFrame) -> pd.Series:
        """
        識別 Payroll 記錄
        
        檢查邏輯: EBS Task OR Item Description 包含 "payroll" (不區分大小寫)
        
        Returns:
            pd.Series: Boolean mask
        """
        keywords = self.payroll_config['keywords']
        
        # 初始化為全 False
        payroll_mask = pd.Series(False, index=df.index)
        
        # 檢查 EBS Task
        if self.ebs_task_column in df.columns:
            ebs_mask = df[self.ebs_task_column].astype(str).str.contains(
                keywords, 
                na=False, 
                regex=True
            )
            payroll_mask |= ebs_mask
            ebs_count = ebs_mask.sum()
            self.logger.debug(f"   • {self.ebs_task_column} 匹配: {ebs_count:,} 筆")
        
        # 檢查 Item Description
        if self.description_column in df.columns:
            desc_mask = df[self.description_column].astype(str).str.contains(
                keywords, 
                na=False, 
                regex=True
            )
            payroll_mask |= desc_mask
            desc_count = desc_mask.sum()
            self.logger.debug(f"   • {self.description_column} 匹配: {desc_count:,} 筆")
        
        return payroll_mask
    
    def _generate_statistics(self,
                             df: pd.DataFrame,
                             payroll_count: int,
                             update_count: int,
                             skipped_count: int,
                             input_count: int) -> Dict[str, Any]:
        """生成詳細統計信息"""
        detection_rate = (payroll_count / input_count * 100) if input_count > 0 else 0
        update_rate = (update_count / payroll_count * 100) if payroll_count > 0 else 0
        
        return {
            'total_records': input_count,
            'payroll_detected': payroll_count,
            'detection_percentage': f"{detection_rate:.2f}%",
            'payroll_labeled': update_count,
            'already_labeled': skipped_count,
            'update_rate': f"{update_rate:.2f}%",
            'label_applied': self.payroll_config['label']
        }
    
    def _log_detailed_statistics(self, stats: Dict[str, Any]):
        """記錄詳細統計日誌"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 Payroll 偵測統計報告")
        self.logger.info("=" * 60)
        self.logger.info(f"📈 總記錄數: {stats['total_records']:,}")
        self.logger.info(f"🔍 Payroll 記錄數: {stats['payroll_detected']:,} ({stats['detection_percentage']})")
        self.logger.info(f"✅ 已更新標籤: {stats['payroll_labeled']:,} ({stats['update_rate']})")
        self.logger.info(f"⏭️  跳過（已有標籤）: {stats['already_labeled']:,}")
        self.logger.info(f"🏷️  應用標籤: {stats['label_applied']}")
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
    
    def _create_skipped_result(self, df: pd.DataFrame, reason: str, duration: float) -> StepResult:
        """創建跳過執行的結果"""
        return StepResult(
            step_name=self.name,
            status=StepStatus.SKIPPED,
            data=df,
            message=reason,
            duration=duration,
            metadata={'reason': reason}
        )