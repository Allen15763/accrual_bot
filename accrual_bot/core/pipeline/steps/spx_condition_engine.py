"""
SPX 配置驅動條件引擎

從 stagging.toml 讀取規則，動態建構 pandas boolean mask 並依序應用狀態標籤。
支援兩類規則：
- spx_status_stage1_rules: 第一階段狀態標籤（StatusStage1Step）
- spx_erm_status_rules: ERM 邏輯狀態判斷（SPXERMLogicStep）

設計原則：
- 僅供 SPX 使用，不影響 SPT 現有邏輯
- 混合模式：可配置的條件由引擎處理，數據驅動的條件保留程式碼
"""

from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

from accrual_bot.utils.config import config_manager
from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)


class SPXConditionEngine:
    """SPX 配置驅動的條件引擎

    從 stagging.toml 讀取條件規則，動態建構 pandas boolean mask 並應用狀態。
    每個規則包含 priority、status_value、note、combine 和 checks 列表。

    Usage:
        engine = SPXConditionEngine('spx_erm_status_rules')
        df, stats = engine.apply_rules(df, 'PO狀態', context)
    """

    def __init__(self, config_section: str):
        """
        初始化引擎

        Args:
            config_section: TOML 配置區段名稱
                           如 'spx_status_stage1_rules' 或 'spx_erm_status_rules'
        """
        self.config_section = config_section
        self.rules = self._load_rules()
        logger.info(f"SPXConditionEngine 已載入 {len(self.rules)} 條規則 "
                    f"(來源: {config_section})")

    def _load_rules(self) -> List[Dict[str, Any]]:
        """載入並按 priority 排序的規則列表"""
        section = config_manager._config_toml.get(self.config_section, {})
        conditions = section.get('conditions', [])

        if not conditions:
            logger.warning(f"未找到 {self.config_section}.conditions 配置")
            return []

        # 按 priority 排序
        sorted_rules = sorted(conditions, key=lambda r: r.get('priority', 999))
        return sorted_rules

    def apply_rules(
        self,
        df: pd.DataFrame,
        status_column: str,
        context: Dict[str, Any],
        processing_type: str = "PO",
        update_no_status: bool = True
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """依序應用所有規則

        Args:
            df: 目標 DataFrame
            status_column: 狀態欄位名稱（如 'PO狀態' 或 'PR狀態'）
            context: 上下文資訊，包含：
                - processing_date: int (YYYYMM)
                - prebuilt_masks: Dict[str, pd.Series] (預先計算的 mask)
                - entity_type: str
            processing_type: 處理類型 ('PO' 或 'PR')
            update_no_status: 每個規則執行後是否重新計算 no_status

        Returns:
            Tuple[DataFrame, Dict]: (更新後的 df, 各規則命中數統計)
        """
        stats: Dict[str, int] = {}

        # 先定位「尚無狀態」的列，僅對這些列進行規則匹配
        no_status = (
            df[status_column].isna()
            | (df[status_column] == '')
            | (df[status_column] == 'nan')
        )
        already_has = int((~no_status).sum())
        if already_has > 0:
            logger.info(
                f"[{self.config_section}] 輸入資料已有 {already_has:,} 筆"
                f"含狀態值，引擎僅處理其餘 {int(no_status.sum()):,} 筆"
            )

        for rule in self.rules:
            # 檢查 apply_to 過濾
            apply_to = rule.get('apply_to', ['PO', 'PR'])
            if processing_type not in apply_to:
                continue

            priority = rule.get('priority', 0)
            status_value = self._resolve_status_value(rule)
            note = rule.get('note', '')
            combine = rule.get('combine', 'and')
            checks = rule.get('checks', [])

            if not checks:
                continue

            # 建構組合 mask
            mask = self._build_combined_mask(
                df, checks, combine, status_column, context
            )

            if mask is None:
                continue

            # 限縮：僅命中「尚無狀態」的列
            mask = mask & no_status

            count = mask.sum()
            rule_key = f"priority_{priority}_{status_value}"
            stats[rule_key] = int(count)

            if count > 0:
                df.loc[mask, status_column] = status_value
                logger.debug(
                    f"✓ [priority={priority:2d}] → '{status_value}': "
                    f"{count:5,} 筆 | {note}"
                )
                df.loc[mask, 'matched_condition_on_status'] = note

                # 更新 no_status：已被賦值的列不再參與後續規則
                no_status = no_status & ~mask

            # 同步更新 prebuilt_masks 中的 no_status
            if update_no_status and 'prebuilt_masks' in context:
                context['prebuilt_masks']['no_status'] = no_status

        return df, stats

    def _resolve_status_value(self, rule: Dict[str, Any]) -> str:
        """解析狀態值，支援直接值或引用"""
        if 'status_value' in rule:
            return rule['status_value']
        if 'status_value_key' in rule:
            return self._resolve_ref(rule['status_value_key'])
        return ''

    def _build_combined_mask(
        self,
        df: pd.DataFrame,
        checks: List[Dict[str, Any]],
        combine: str,
        status_column: str,
        context: Dict[str, Any]
    ) -> Optional[pd.Series]:
        """建構多個 check 的組合 mask

        Args:
            combine: 'and' 或 'or'
        """
        masks: List[pd.Series] = []

        for check in checks:
            mask = self._evaluate_check(df, check, status_column, context)
            if mask is not None:
                masks.append(mask)

        if not masks:
            return None

        if combine == 'or':
            result = masks[0]
            for m in masks[1:]:
                result = result | m
        else:  # 'and'
            result = masks[0]
            for m in masks[1:]:
                result = result & m

        return result

    def _evaluate_check(
        self,
        df: pd.DataFrame,
        check: Dict[str, Any],
        status_column: str,
        context: Dict[str, Any]
    ) -> Optional[pd.Series]:
        """評估單一 check，回傳 boolean Series

        支援的 check type:
        1. 欄位比對類: contains, not_contains, equals, not_equals, in_list, not_in_list
        2. 欄位狀態類: is_not_null, is_null, no_status
        3. ERM/日期類: erm_le_date, erm_gt_date, erm_in_range, out_of_range,
                      desc_erm_le_date, desc_erm_gt_date, desc_erm_not_error
        4. 帳務類: qty_matched, qty_not_matched, not_billed, has_billing,
                  fully_billed, has_unpaid, format_error
        5. 備註類: remark_completed, pr_not_incomplete, not_error
        6. FA類: is_fa, not_fa
        """
        check_type = check.get('type', '')
        field = check.get('field', '')
        prebuilt = context.get('prebuilt_masks', {})

        # === 預先計算的 mask（從 context 取得）===
        if check_type in prebuilt:
            return prebuilt[check_type]

        # === 欄位比對類 ===
        if check_type == 'contains':
            return self._check_contains(df, field, check, na=False)

        if check_type == 'not_contains':
            result = self._check_contains(df, field, check, na=False)
            return ~result if result is not None else None

        if check_type == 'equals':
            return self._check_equals(df, field, check)

        if check_type == 'not_equals':
            result = self._check_equals(df, field, check)
            return ~result if result is not None else None

        if check_type == 'in_list':
            return self._check_in_list(df, field, check)

        if check_type == 'not_in_list':
            result = self._check_in_list(df, field, check)
            return ~result if result is not None else None

        # === 欄位狀態類 ===
        if check_type == 'is_not_null':
            if field not in df.columns:
                return None
            return (~df[field].isna()) & (df[field] != '') & (df[field] != 'nan')

        if check_type == 'is_null':
            if field not in df.columns:
                return None
            return df[field].isna() | (df[field] == '') | (df[field] == 'nan')

        if check_type == 'no_status':
            return (df[status_column].isna()) | (df[status_column] == 'nan')

        # === ERM/日期類（從 prebuilt_masks 或即時計算）===
        processing_date = context.get('processing_date')

        if check_type == 'erm_le_date':
            if 'erm_le_date' in prebuilt:
                return prebuilt['erm_le_date']
            if processing_date and 'Expected Received Month_轉換格式' in df.columns:
                return df['Expected Received Month_轉換格式'] <= processing_date
            return None

        if check_type == 'erm_gt_date':
            if 'erm_gt_date' in prebuilt:
                return prebuilt['erm_gt_date']
            if processing_date and 'Expected Received Month_轉換格式' in df.columns:
                return df['Expected Received Month_轉換格式'] > processing_date
            return None

        if check_type == 'erm_in_range':
            if 'erm_in_range' in prebuilt:
                return prebuilt['erm_in_range']
            return self._compute_erm_in_range(df)

        if check_type == 'out_of_range':
            if 'out_of_range' in prebuilt:
                return prebuilt['out_of_range']
            in_range = self._compute_erm_in_range(df)
            if in_range is not None:
                format_err = df['YMs of Item Description'] == '100001,100002'
                return (~in_range) & (~format_err)
            return None

        if check_type == 'desc_erm_le_date':
            if processing_date and 'YMs of Item Description' in df.columns:
                return (df['YMs of Item Description']
                        .str[7:].astype('Int64') <= processing_date)
            return None

        if check_type == 'desc_erm_gt_date':
            if processing_date and 'YMs of Item Description' in df.columns:
                return (df['YMs of Item Description']
                        .str[:6].astype('Int64') > processing_date)
            return None

        if check_type == 'desc_erm_not_error':
            if 'YMs of Item Description' in df.columns:
                return (df['YMs of Item Description']
                        .str[:6].astype('Int64') != 100001)
            return None

        # === 帳務類 ===
        if check_type == 'qty_matched':
            if 'qty_matched' in prebuilt:
                return prebuilt['qty_matched']
            if all(c in df.columns for c in ['Entry Quantity', 'Received Quantity']):
                return df['Entry Quantity'] == df['Received Quantity']
            return None

        if check_type == 'qty_not_matched':
            matched = self._evaluate_check(
                df, {'type': 'qty_matched'}, status_column, context
            )
            return ~matched if matched is not None else None

        if check_type == 'not_billed':
            if 'not_billed' in prebuilt:
                return prebuilt['not_billed']
            if 'Entry Billed Amount' in df.columns:
                return df['Entry Billed Amount'].astype('Float64') == 0
            return None

        if check_type == 'has_billing':
            if 'has_billing' in prebuilt:
                return prebuilt['has_billing']
            if 'Billed Quantity' in df.columns:
                return df['Billed Quantity'] != '0'
            return None

        if check_type == 'fully_billed':
            if 'fully_billed' in prebuilt:
                return prebuilt['fully_billed']
            if all(c in df.columns for c in ['Entry Amount', 'Entry Billed Amount']):
                diff = (df['Entry Amount'].astype('Float64')
                        - df['Entry Billed Amount'].astype('Float64'))
                return diff == 0
            return None

        if check_type == 'has_unpaid':
            if 'has_unpaid' in prebuilt:
                return prebuilt['has_unpaid']
            if all(c in df.columns for c in ['Entry Amount', 'Entry Billed Amount']):
                diff = (df['Entry Amount'].astype('Float64')
                        - df['Entry Billed Amount'].astype('Float64'))
                return diff != 0
            return None

        if check_type == 'format_error':
            if 'format_error' in prebuilt:
                return prebuilt['format_error']
            if 'YMs of Item Description' in df.columns:
                return df['YMs of Item Description'] == '100001,100002'
            return None

        # === 備註類 ===
        if check_type == 'remark_completed':
            if 'remark_completed' in prebuilt:
                return prebuilt['remark_completed']
            procurement = df.get('Remarked by Procurement', pd.Series(dtype='str'))
            fn = df.get('Remarked by 上月 FN', pd.Series(dtype='str'))
            pq_match = procurement.str.contains('(?i)已完成|rent', na=False)
            fn_match = fn.str.contains('(?i)已完成|已入帳', na=False)
            return pq_match | fn_match

        if check_type == 'pr_not_incomplete':
            if 'pr_not_incomplete' in prebuilt:
                return prebuilt['pr_not_incomplete']
            if 'Remarked by 上月 FN PR' in df.columns:
                return ~df['Remarked by 上月 FN PR'].str.contains(
                    '(?i)未完成', na=False
                )
            return pd.Series([True] * len(df), index=df.index)

        if check_type == 'not_error':
            if 'not_error' in prebuilt:
                return prebuilt['not_error']
            if 'Remarked by Procurement' in df.columns:
                return df['Remarked by Procurement'] != 'error'
            return pd.Series([True] * len(df), index=df.index)

        # === FA 類 ===
        if check_type == 'is_fa':
            if 'is_fa' in prebuilt:
                return prebuilt['is_fa']
            fa_accounts = config_manager._config_toml.get('fa_accounts', {}).get('spx')
            fa_accounts = [str(i) for i in fa_accounts]
            if 'GL#' in df.columns:
                return df['GL#'].astype('string').isin(
                    [str(x) for x in fa_accounts]
                )
            return None

        if check_type == 'not_fa':
            fa_mask = self._evaluate_check(
                df, {'type': 'is_fa'}, status_column, context
            )
            return ~fa_mask if fa_mask is not None else None

        logger.warning(f"未知的 check type: {check_type}")
        return None

    # ========== 欄位比對輔助方法 ==========

    def _check_contains(
        self,
        df: pd.DataFrame,
        field: str,
        check: Dict[str, Any],
        na: bool = False
    ) -> Optional[pd.Series]:
        """欄位包含正則模式"""
        if not field or field not in df.columns:
            return None

        pattern = self._resolve_pattern(check)
        if not pattern:
            return None

        return df[field].astype('string').str.contains(pattern, na=na, regex=True)

    def _check_equals(
        self,
        df: pd.DataFrame,
        field: str,
        check: Dict[str, Any]
    ) -> Optional[pd.Series]:
        """欄位等於值"""
        if not field or field not in df.columns:
            return None

        value = self._resolve_value(check)
        if value is None:
            return None

        cast = check.get('cast')
        if cast:
            casted = df[field].astype(cast)
            return casted == type(value)(value) if isinstance(value, (int, float)) else casted == value

        return df[field].astype('string') == str(value)

    def _check_in_list(
        self,
        df: pd.DataFrame,
        field: str,
        check: Dict[str, Any]
    ) -> Optional[pd.Series]:
        """欄位在列表中"""
        if not field or field not in df.columns:
            return None

        values = self._resolve_list(check)
        if values is None:
            return None

        return df[field].isin(values)

    # ========== 值解析輔助方法 ==========

    def _resolve_pattern(self, check: Dict[str, Any]) -> Optional[str]:
        """解析正則模式，支援直接值或引用"""
        if 'pattern' in check:
            return check['pattern']
        if 'pattern_key' in check:
            return str(self._resolve_ref(check['pattern_key']))
        return None

    def _resolve_value(self, check: Dict[str, Any]) -> Any:
        """解析值，支援直接值或引用"""
        if 'value' in check:
            return check['value']
        if 'value_key' in check:
            return self._resolve_ref(check['value_key'])
        return None

    def _resolve_list(self, check: Dict[str, Any]) -> Optional[List]:
        """解析列表，支援直接值或引用"""
        if 'values' in check:
            return check['values']
        if 'list_key' in check:
            ref = self._resolve_ref(check['list_key'])
            if isinstance(ref, list):
                return ref
            if isinstance(ref, str):
                return [ref]
        return None

    def _resolve_ref(self, key: str) -> Any:
        """解析引用 key，支援點分隔路徑

        範例：
        - 'spx.deposit_keywords' → config_toml['spx']['deposit_keywords']
        - 'fa_accounts.spx' → config_toml['fa_accounts']['spx']
        - 'spx.asset_suppliers' → 合併 kiosk_suppliers + locker_suppliers
        """
        # 特殊處理：asset_suppliers = kiosk + locker
        if key == 'spx.asset_suppliers':
            kiosk = config_manager._config_toml.get('spx', {}).get('kiosk_suppliers')
            locker = config_manager._config_toml.get('spx', {}).get('locker_suppliers')
            return kiosk + locker

        parts = key.split('.')
        current = config_manager._config_toml

        for part in parts:
            if not isinstance(current, dict):
                logger.warning(f"配置引用解析失敗: {key} ('{part}' 非 dict)")
                return None

            value = current.get(part)
            if value is None:
                # 嘗試不區分大小寫
                for k, v in current.items():
                    if k.lower() == part.lower():
                        value = v
                        break

            if value is None:
                logger.warning(f"配置引用解析失敗: {key} (在 '{part}' 處)")
                return None

            current = value

        return current

    # ========== ERM 計算輔助方法 ==========

    def _compute_erm_in_range(self, df: pd.DataFrame) -> Optional[pd.Series]:
        """計算 ERM 是否在摘要日期區間內"""
        if not all(c in df.columns for c in [
            'Expected Received Month_轉換格式', 'YMs of Item Description'
        ]):
            return None

        ym_start = df['YMs of Item Description'].str[:6].astype('Int32')
        ym_end = df['YMs of Item Description'].str[7:].astype('Int32')
        erm = df['Expected Received Month_轉換格式']

        return erm.between(ym_start, ym_end, inclusive='both')
