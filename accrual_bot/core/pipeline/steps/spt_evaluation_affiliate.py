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
                    self.COMMISSION_CONFIG['affiliate']
                )
                self.logger.info(f"✅ 已更新 {affiliate_count:,} 筆 Affiliate/Shopee 分潤")
            
            # 更新 AMS 分潤
            if ams_count > 0:
                self._update_commission_records(
                    df, 
                    ams_mask, 
                    'ams',
                    self.COMMISSION_CONFIG['ams']
                )
                self.logger.info(f"✅ 已更新 {ams_count:,} 筆 AMS 分潤")
            
            # === 階段 4: 設置估計入帳 ===
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
                'Product code_c'
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
        affiliate_config = self.COMMISSION_CONFIG['affiliate']
        affiliate_mask = df[self.description_column].str.contains(
            affiliate_config['keywords'], 
            na=False, 
            regex=True
        )
        
        # 排除「品牌加碼」
        for exclude_kw in affiliate_config['exclude_keywords']:
            affiliate_mask &= ~df[self.description_column].str.contains(exclude_kw, na=False)
        
        # AMS 分潤 - 情況1: 包含 AMS commission
        ams_config = self.COMMISSION_CONFIG['ams']
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
        df.loc[mask, 'Product code_c'] = config['product_code']
    
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
