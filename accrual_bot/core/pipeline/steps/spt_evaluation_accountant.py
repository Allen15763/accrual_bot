import time
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from dataclasses import dataclass

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder, create_error_metadata
from accrual_bot.utils.config import config_manager


@dataclass
class SPTStatusLabelConditions:
    """會計標籤判斷條件集合 - 提高可讀性"""

    # Item Description 關鍵字條件
    has_ssp: pd.Series
    has_logistics_fee: pd.Series
    has_handling_fee: pd.Series
    has_remittance_fee: pd.Series
    has_shipping_fee: pd.Series
    has_hidden_code_fee: pd.Series
    has_commissions: pd.Series
    has_seller_affiliate: pd.Series
    has_refund: pd.Series
    has_service_charges: pd.Series

    # Department 和 Supplier 組合條件
    tradewan_non_g21: pd.Series
    tradewan_g21: pd.Series
    jianqiang_non_mkt: pd.Series
    jianqiang_mkt: pd.Series

    # Department 特定條件
    g44_telecom: pd.Series

    # Supplier 特定條件
    ctbc_bank: pd.Series

    # Requester 組合條件
    sherry_wu_s01: pd.Series
    chen_hung_i_g42_twn: pd.Series

    # Supplier + Item Description 組合條件
    welfare_fund: pd.Series
    cobranded_card: pd.Series
    rent_global_life: pd.Series
    rent_taipei_wenchuang: pd.Series
    rent_united_daily: pd.Series


class SPTStatusLabelStep(PipelineStep):
    """
    會計標籤標記步驟

    業務邏輯:
    1. 根據配置檔案中的規則標記會計標籤
    2. 優先級條件：更新 PO狀態 和 Remarked by FN（強制覆蓋）
    3. ERM條件：僅更新 Remarked by FN（不更新狀態，估計與否由ERM決定）

    配置來源:
    - [spt_status_label_rules.priority_conditions]: 優先於ERM的條件
    - [spt_status_label_rules.erm_conditions]: ERM條件

    輸入:
    - DataFrame with required columns

    輸出:
    - DataFrame with updated labels
    """

    def __init__(self,
                 name: str = "Accounting_Label_Marking",
                 status_column: str = "PO狀態",
                 remark_column: str = "Remarked by FN",
                 **kwargs):
        """
        初始化會計標籤標記步驟

        Args:
            name: 步驟名稱
            status_column: 狀態欄位名稱（預設為 PO狀態）
            remark_column: 備註欄位名稱（預設為 Remarked by FN）
        """
        super().__init__(
            name=name,
            description="Mark accounting labels based on business rules",
            **kwargs
        )
        self.status_column = status_column
        self.remark_column = remark_column

        # 從配置檔案讀取規則
        self.priority_rules = self._load_priority_rules()
        self.erm_rules = self._load_erm_rules()

        self.logger.info(f"已載入 {len(self.priority_rules)} 個優先級規則")
        self.logger.info(f"已載入 {len(self.erm_rules)} 個 ERM 規則")

    def _load_priority_rules(self) -> Dict[str, Dict[str, Any]]:
        """從配置檔案載入優先級規則"""
        try:
            rules = config_manager._config_toml.get('spt_status_label_rules', {}).get('priority_conditions', {})
            return dict(rules) if rules else {}
        except Exception as e:
            self.logger.error(f"載入優先級規則失敗: {str(e)}")
            return {}

    def _load_erm_rules(self) -> Dict[str, Dict[str, Any]]:
        """從配置檔案載入 ERM 規則"""
        try:
            rules = config_manager._config_toml.get('spt_status_label_rules', {}).get('erm_conditions', {})
            return dict(rules) if rules else {}
        except Exception as e:
            self.logger.error(f"載入 ERM 規則失敗: {str(e)}")
            return {}

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行會計標籤標記邏輯"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()
            input_count = len(df)

            self.logger.info("=" * 60)
            self.logger.info("🏷️  開始執行會計標籤標記...")
            self.logger.info(f"📊 總記錄數: {input_count:,}")
            self.logger.info("=" * 60)

            # 動態判斷狀態欄位名稱
            self.status_column = self._get_status_column(df)

            # === 階段 1: 構建條件 ===
            self.logger.info("🔍 構建判斷條件...")
            conditions = self._build_conditions(df)

            # === 階段 2: 應用優先級條件 ===
            self.logger.info("⚡ 應用優先級條件（強制覆蓋）...")
            priority_stats = self._apply_priority_conditions(df, conditions)

            # === 階段 3: 應用 ERM 條件 ===
            self.logger.info("📋 應用 ERM 條件（僅標記備註）...")
            erm_stats = self._apply_erm_conditions(df, conditions)

            # === 階段 4: 生成統計資訊 ===
            total_labeled = sum(priority_stats.values()) + sum(erm_stats.values())

            statistics = {
                'total_records': input_count,
                'priority_labeled': sum(priority_stats.values()),
                'erm_labeled': sum(erm_stats.values()),
                'total_labeled': total_labeled,
                'label_rate': f"{(total_labeled / input_count * 100):.2f}%" if input_count > 0 else "0.00%",
                'priority_breakdown': priority_stats,
                'erm_breakdown': erm_stats
            }

            # === 階段 5: 記錄詳細日誌 ===
            self._log_detailed_statistics(statistics)

            # === 階段 6: 更新上下文 ===
            context.update_data(df)

            duration = time.time() - start_time
            end_datetime = datetime.now()

            self.logger.info("=" * 60)
            self.logger.info(f"✅ 會計標籤標記完成 (耗時: {duration:.2f}秒)")
            self.logger.info("=" * 60)

            # 構建 metadata
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, len(df))
                        .set_process_counts(processed=total_labeled, skipped=input_count - total_labeled)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('priority_labeled', sum(priority_stats.values()))
                        .add_custom('erm_labeled', sum(erm_stats.values()))
                        .add_custom('statistics', statistics)
                        .build())

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message='-'.join([f"已標記 {total_labeled:,} 筆記錄\n",
                                 f"\t(優先級: {sum(priority_stats.values()):,}, ERM: {sum(erm_stats.values()):,})"]),
                duration=duration,
                metadata=metadata
            )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"❌ 會計標籤標記失敗: {str(e)}", exc_info=True)
            context.add_error(f"Accounting label marking failed: {str(e)}")

            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='accounting_label_marking'
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"會計標籤標記失敗: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )

    def _get_status_column(self, df: pd.DataFrame) -> str:
        """動態判斷狀態欄位名稱"""
        if 'PO狀態' in df.columns:
            return 'PO狀態'
        elif 'PR狀態' in df.columns:
            return 'PR狀態'
        else:
            # 如果都不存在，創建 PO狀態 欄位
            df['PO狀態'] = pd.NA
            return 'PO狀態'

    def _build_conditions(self, df: pd.DataFrame) -> SPTStatusLabelConditions:
        """
        構建所有判斷條件

        將條件邏輯集中在此處，提高可讀性和維護性
        """

        # === Item Description 關鍵字條件 ===
        item_desc = df.get('Item Description', pd.Series(dtype=str))

        has_ssp = item_desc.str.contains(r'(?i)SSP', na=False, regex=True)
        has_logistics_fee = item_desc.str.contains(r'(?i)Logistics fee|Logistic fee', na=False, regex=True)
        has_handling_fee = item_desc.str.contains(r'(?i)Handling fee', na=False, regex=True)
        has_remittance_fee = item_desc.str.contains(r'(?i)Remittance fee', na=False, regex=True)
        has_shipping_fee = item_desc.str.contains(r'(?i)shipping fee', na=False, regex=True)
        has_hidden_code_fee = item_desc.str.contains(r'物流隱碼費|隱碼費', na=False, regex=True)
        has_commissions = item_desc.str.contains(r'(?i)Commissions', na=False, regex=True)
        has_seller_affiliate = item_desc.str.contains(r'(?i)Seller affiliate', na=False, regex=True)
        has_refund = item_desc.str.contains(r'(?i)refund', na=False, regex=True)
        has_service_charges = item_desc.str.contains(r'(?i)service charges', na=False, regex=True)

        # === Department 和 Supplier 組合條件 ===
        dept = df.get('Department', pd.Series(dtype=str))
        supplier_col: str = df.filter(regex='(?i)supplier').columns[0]
        supplier = df.get(supplier_col, pd.Series(dtype=str))

        # 關貿網路條件
        is_tradewan = supplier == 'TW_關貿網路股份有限公司'
        dept_starts_g21 = dept.str.startswith('G21', na=False)
        tradewan_non_g21 = is_tradewan & (~dept_starts_g21)
        tradewan_g21 = is_tradewan & dept_starts_g21

        # 漸強賴伯條件
        is_jianqiang = supplier == 'TW_漸強賴伯股份有限公司'
        dept_has_mkt = dept.str.contains(r'(?i)Marketing', na=False, regex=True)
        jianqiang_non_mkt = is_jianqiang & (~dept_has_mkt)
        jianqiang_mkt = is_jianqiang & dept_has_mkt

        # === Department 特定條件 ===
        g44_telecom = (
            (dept == 'G44 - Corporate IT (BE)') &
            item_desc.str.contains(r'電信費|通信費|月租費|通話費|簡訊|行動上網', na=False, regex=True)
        )

        # === Supplier 特定條件 ===
        ctbc_bank = supplier == 'TW_中國信託商業銀行股份有限公司'

        # === Requester 組合條件 ===
        requester_col: str = df.filter(regex='(?i)requester').columns[0]
        requester = df.get(requester_col, pd.Series(dtype=str))

        # Sherry Wu + S01
        is_sherry = (requester == 'Sherry Wu (吳欣怡)')
        sherry_wu_s01 = is_sherry & (dept == 'S01 - Marketing & Publishing')

        # Chen Hung I + G42 + 台灣固網
        is_chen = (requester == 'Chen Hung I (陳虹沂)')
        chen_hung_i_g42_twn = (
            is_chen &
            (dept == 'G42 - Corporate Infrastructure') &
            (supplier == 'TW_台灣固網股份有限公司')
        )

        # === Supplier + Item Description 組合條件 ===

        # 福委會 + Employee welfare fund
        welfare_fund = (
            (supplier == 'TW_新加坡商蝦皮娛樂電商有限公司台灣分公司聯合職工福利委員會') &
            item_desc.str.contains(r'(?i)Employee welfare fund', na=False, regex=True)
        )

        # 國泰世華 + 聯名卡
        cobranded_card = (
            (supplier == 'TW_國泰世華商業銀行信用卡作業部') &
            item_desc.str.contains(r'(?i)Co-Branded Card Campaign Fee 聯名卡', na=False, regex=True)
        )

        # 租金條件
        rent_global_life = (
            (supplier == 'TW_全球人壽保險股份有限公司') &
            item_desc.str.contains(r'辦公室租', na=False, regex=True)
        )

        rent_taipei_wenchuang = (
            (supplier == 'TW_臺北文創開發股份有限公司') &
            item_desc.str.contains(r'辦公室租', na=False, regex=True)
        )

        rent_united_daily = (
            (supplier == 'TW_聯合報股份有限公司') &
            item_desc.str.contains(r'(?i)office rental fee|deposit', na=False, regex=True)
        )

        return SPTStatusLabelConditions(
            has_ssp=has_ssp,
            has_logistics_fee=has_logistics_fee,
            has_handling_fee=has_handling_fee,
            has_remittance_fee=has_remittance_fee,
            has_shipping_fee=has_shipping_fee,
            has_hidden_code_fee=has_hidden_code_fee,
            has_commissions=has_commissions,
            has_seller_affiliate=has_seller_affiliate,
            has_refund=has_refund,
            has_service_charges=has_service_charges,
            tradewan_non_g21=tradewan_non_g21,
            tradewan_g21=tradewan_g21,
            jianqiang_non_mkt=jianqiang_non_mkt,
            jianqiang_mkt=jianqiang_mkt,
            g44_telecom=g44_telecom,
            ctbc_bank=ctbc_bank,
            sherry_wu_s01=sherry_wu_s01,
            chen_hung_i_g42_twn=chen_hung_i_g42_twn,
            welfare_fund=welfare_fund,
            cobranded_card=cobranded_card,
            rent_global_life=rent_global_life,
            rent_taipei_wenchuang=rent_taipei_wenchuang,
            rent_united_daily=rent_united_daily
        )

    def _apply_priority_conditions(self, df: pd.DataFrame,
                                   cond: SPTStatusLabelConditions) -> Dict[str, int]:
        """
        應用優先級條件（強制覆蓋）

        更新: PO狀態 和 Remarked by FN

        Returns:
            Dict[str, int]: 各條件的匹配計數
        """
        stats = {}

        # Blaire 相關條件
        blaire_conditions = [
            ('SSP', cond.has_ssp, '不估計(Blaire)', 'Blaire'),
            ('Logistics fee', cond.has_logistics_fee, '不估計(Blaire)', 'Blaire'),
            ('Handling fee', cond.has_handling_fee, '不估計(Blaire)', 'Blaire'),
            ('Remittance fee', cond.has_remittance_fee, '不估計(Blaire)', 'Blaire'),
            ('Shipping fee', cond.has_shipping_fee, '不估計(Blaire)', 'Blaire'),
            ('隱碼費', cond.has_hidden_code_fee, '不估計(Blaire)', 'Blaire'),
            ('Commissions', cond.has_commissions, '不估計(Blaire)', 'Blaire'),
            ('Seller affiliate', cond.has_seller_affiliate, '不估計(Blaire)', 'Blaire'),
            ('Refund', cond.has_refund, '不估計(Blaire)', 'Blaire'),
            ('Service charges', cond.has_service_charges, '不估計(Blaire)', 'Blaire'),
            ('關貿(非G21)', cond.tradewan_non_g21, '不估計(Blaire)', 'Blaire'),
        ]

        for name, mask, status, remark in blaire_conditions:
            count = mask.sum()
            if count > 0:
                df.loc[mask, self.status_column] = status
                df.loc[mask, self.remark_column] = remark
                self.logger.debug(f"  ✓ {name}: {count:,} 筆")
                stats[name] = count

        # Shirley 條件
        if cond.g44_telecom.sum() > 0:
            df.loc[cond.g44_telecom, self.status_column] = '不估計(Shirley)'
            df.loc[cond.g44_telecom, self.remark_column] = 'Shirley'
            count = cond.g44_telecom.sum()
            self.logger.debug(f"  ✓ G44電信費: {count:,} 筆")
            stats['G44電信費'] = count

        # Cindy 條件
        cindy_conditions = [
            ('中國信託', cond.ctbc_bank, '不估計(Cindy)', 'Cindy'),
            ('Sherry Wu待確認', cond.sherry_wu_s01, '待確認(Cindy)', 'Cindy'),
        ]

        for name, mask, status, remark in cindy_conditions:
            count = mask.sum()
            if count > 0:
                df.loc[mask, self.status_column] = status
                df.loc[mask, self.remark_column] = remark
                self.logger.debug(f"  ✓ {name}: {count:,} 筆")
                stats[name] = count

        # Hosting 條件
        if cond.chen_hung_i_g42_twn.sum() > 0:
            df.loc[cond.chen_hung_i_g42_twn, self.status_column] = '不估計(Hosting)'
            df.loc[cond.chen_hung_i_g42_twn, self.remark_column] = 'Hosting'
            count = cond.chen_hung_i_g42_twn.sum()
            self.logger.debug(f"  ✓ Hosting(台灣固網): {count:,} 筆")
            stats['Hosting'] = count

        # Michael 條件
        michael_conditions = [
            ('福委會', cond.welfare_fund, '不估計(Michael)', 'Michael'),
            ('聯名卡', cond.cobranded_card, '不估計(Michael)', 'Michael'),
        ]

        for name, mask, status, remark in michael_conditions:
            count = mask.sum()
            if count > 0:
                df.loc[mask, self.status_column] = status
                df.loc[mask, self.remark_column] = remark
                self.logger.debug(f"  ✓ {name}: {count:,} 筆")
                stats[name] = count

        # 租金條件
        rent_conditions = [
            ('全球人壽租金', cond.rent_global_life, '不估計(租金)', '租金'),
            ('臺北文創租金', cond.rent_taipei_wenchuang, '不估計(租金)', '租金'),
            ('聯合報租金', cond.rent_united_daily, '不估計(租金)', '租金'),
        ]

        for name, mask, status, remark in rent_conditions:
            count = mask.sum()
            if count > 0:
                df.loc[mask, self.status_column] = status
                df.loc[mask, self.remark_column] = remark
                self.logger.debug(f"  ✓ {name}: {count:,} 筆")
                stats[name] = count

        return stats

    def _apply_erm_conditions(self, df: pd.DataFrame,
                              cond: SPTStatusLabelConditions) -> Dict[str, int]:
        """
        應用 ERM 條件（僅更新 Remarked by FN）

        不更新狀態，估計與否由後續 ERM 步驟決定

        Returns:
            Dict[str, int]: 各條件的匹配計數
        """
        stats = {}

        # ERM 條件：關貿(G21)
        if cond.tradewan_g21.sum() > 0:
            df.loc[cond.tradewan_g21, self.remark_column] = 'Blaire'
            count = cond.tradewan_g21.sum()
            self.logger.debug(f"  ✓ 關貿(G21): {count:,} 筆")
            stats['關貿(G21)'] = count

        # ERM 條件：漸強(非Marketing)
        if cond.jianqiang_non_mkt.sum() > 0:
            df.loc[cond.jianqiang_non_mkt, self.remark_column] = 'Cindy-漸強'
            count = cond.jianqiang_non_mkt.sum()
            self.logger.debug(f"  ✓ 漸強(非MKT): {count:,} 筆")
            stats['漸強(非MKT)'] = count

        # ERM 條件：漸強(Marketing)
        if cond.jianqiang_mkt.sum() > 0:
            df.loc[cond.jianqiang_mkt, self.remark_column] = '漸強MKT'
            count = cond.jianqiang_mkt.sum()
            self.logger.debug(f"  ✓ 漸強(MKT): {count:,} 筆")
            stats['漸強(MKT)'] = count

        return stats

    def _log_detailed_statistics(self, stats: Dict[str, Any]):
        """記錄詳細統計日誌"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 會計標籤標記統計報告")
        self.logger.info("=" * 60)
        self.logger.info(f"📈 總記錄數: {stats['total_records']:,}")
        self.logger.info(f"🏷️  已標記: {stats['total_labeled']:,} ({stats['label_rate']})")
        self.logger.info(f"   • 優先級條件: {stats['priority_labeled']:,}")
        self.logger.info(f"   • ERM 條件: {stats['erm_labeled']:,}")

        if stats['priority_breakdown']:
            self.logger.info("\n📋 優先級條件明細:")
            for label, count in sorted(stats['priority_breakdown'].items()):
                self.logger.info(f"   • {label}: {count:,}")

        if stats['erm_breakdown']:
            self.logger.info("\n📋 ERM 條件明細:")
            for label, count in sorted(stats['erm_breakdown'].items()):
                self.logger.info(f"   • {label}: {count:,}")

        self.logger.info("=" * 60 + "\n")

    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入資料的完整性

        檢查項目:
        1. DataFrame 不為空
        2. 必要欄位存在
        """
        try:
            df = context.data

            # 檢查 DataFrame
            if df is None or df.empty:
                self.logger.error("❌ 輸入資料為空")
                return False

            # 檢查必要欄位（基本欄位）
            required_columns = [
                'Item Description',
                'Department',
                'Supplier'
            ]

            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"❌ 缺少必要欄位: {missing_columns}")
                return False

            # 檢查或創建狀態和備註欄位
            if self.remark_column not in df.columns:
                self.logger.warning(f"⚠️  {self.remark_column} 欄位不存在，將自動創建")
                df[self.remark_column] = pd.NA

            # 狀態欄位會在 execute 中動態判斷

            self.logger.info("✅ 輸入驗證通過")
            return True

        except Exception as e:
            self.logger.error(f"❌ 驗證失敗: {str(e)}", exc_info=True)
            return False

    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作（如需要）"""
        self.logger.warning(f"回滾會計標籤標記：{str(error)}")
        # 通常不需要特殊回滾操作
