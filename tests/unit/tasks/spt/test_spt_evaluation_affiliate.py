"""
SPT 分潤/Payroll 步驟測試

測試 CommissionDataUpdateStep 和 PayrollDetectionStep 的核心功能：
- CommissionDataUpdateStep:
  - execute() 主流程（成功、跳過、失敗）
  - _identify_commission_records() regex 識別
  - _update_commission_records() 欄位更新
  - _set_accrual_estimation() 估計入帳
  - validate_input() 輸入驗證
- PayrollDetectionStep:
  - execute() 主流程（成功、跳過、失敗）
  - _identify_payroll_records() 多欄位偵測
  - validate_input() 輸入驗證
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# 共用 fixtures
# ============================================================

def _make_commission_df(n: int = 5) -> pd.DataFrame:
    """建立含分潤測試欄位的 DataFrame"""
    return pd.DataFrame({
        'Item Description': [
            'Affiliate commission payment Q1',
            'Shopee commission 202503',
            'AMS commission for March',
            'Regular purchase order',
            'Affiliate分潤合作 品牌加碼 special',
        ][:n],
        'GL#': [None] * n,
        'Account code': [None] * n,
        'Product code': [None] * n,
        'Remarked by FN': [None] * n,
        'PO狀態': [None] * n,
        '是否估計入帳': [None] * n,
    })


def _make_payroll_df(n: int = 5) -> pd.DataFrame:
    """建立含 Payroll 測試欄位的 DataFrame"""
    return pd.DataFrame({
        'EBS Task': [
            'payroll processing',
            'Regular task',
            'PAYROLL administration',
            'Office supplies',
            'Regular task',
        ][:n],
        'Item Description': [
            'Regular item',
            'payroll service',
            'Regular item',
            'Regular item',
            'Regular item',
        ][:n],
        'Remarked by FN': [None] * n,
        'PO狀態': [None] * n,
    })


@pytest.fixture
def commission_context():
    """分潤測試用 ProcessingContext"""
    df = _make_commission_df()
    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202503,
        processing_type='PO',
    )
    return ctx


@pytest.fixture
def payroll_context():
    """Payroll 測試用 ProcessingContext"""
    df = _make_payroll_df()
    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202503,
        processing_type='PO',
    )
    return ctx


@pytest.fixture(autouse=True)
def mock_config():
    """自動 mock config_manager（模組層級）"""
    with patch('accrual_bot.tasks.spt.steps.spt_evaluation_affiliate.config_manager') as mock:
        mock._config_toml = {}
        yield mock


# ============================================================
# CommissionDataUpdateStep 初始化測試
# ============================================================

@pytest.mark.unit
class TestCommissionDataUpdateStepInit:
    """CommissionDataUpdateStep 初始化測試"""

    def test_default_name(self):
        """測試預設步驟名稱"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        assert step.name == "Update_Commission_Data"

    def test_custom_name(self):
        """測試自訂步驟名稱"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep(name="Custom_Commission")
        assert step.name == "Custom_Commission"

    def test_fallback_to_class_config_when_toml_empty(self):
        """測試 TOML 無配置時 fallback 至 class-level 常數"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        assert 'affiliate' in step.commission_config
        assert 'ams' in step.commission_config

    def test_uses_toml_config_when_available(self, mock_config):
        """測試 TOML 有配置時使用 TOML 設定"""
        custom_cfg = {
            'affiliate': {
                'keywords': r'(?i)custom_keyword',
                'exclude_keywords': [],
                'gl_number': '999999',
                'product_code': 'CUSTOM',
                'remark': '自訂',
                'name': '自訂分潤',
            },
            'ams': {
                'keywords': r'(?i)custom_ams',
                'include_and_keywords': ['kw1', 'kw2'],
                'gl_number': '888888',
                'product_code': 'CUSTOM_AMS',
                'remark': '自訂AMS',
                'name': '自訂AMS分潤',
            },
        }
        mock_config._config_toml = {'spt': {'commission': custom_cfg}}
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        assert step.commission_config['affiliate']['gl_number'] == '999999'


# ============================================================
# CommissionDataUpdateStep._identify_commission_records 測試
# ============================================================

@pytest.mark.unit
class TestCommissionIdentifyRecords:
    """_identify_commission_records 識別分潤記錄測試"""

    def test_affiliate_keyword_match(self):
        """測試 Affiliate commission 關鍵字識別"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        affiliate_mask, ams_mask = step._identify_commission_records(df)
        # Row 0: 'Affiliate commission' -> affiliate
        # Row 1: 'Shopee commission' -> affiliate
        assert affiliate_mask[0] is True or affiliate_mask[0] == True
        assert affiliate_mask[1] is True or affiliate_mask[1] == True

    def test_ams_keyword_match(self):
        """測試 AMS commission 關鍵字識別"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        affiliate_mask, ams_mask = step._identify_commission_records(df)
        # Row 2: 'AMS commission' -> ams
        assert ams_mask[2] == True

    def test_regular_purchase_no_match(self):
        """測試一般採購不匹配任何分潤"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        affiliate_mask, ams_mask = step._identify_commission_records(df)
        # Row 3: 'Regular purchase order' -> 無匹配
        assert affiliate_mask[3] == False
        assert ams_mask[3] == False

    def test_exclude_keywords_filter(self):
        """測試「品牌加碼」排除邏輯 — Affiliate分潤合作+品牌加碼應歸為 AMS"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        affiliate_mask, ams_mask = step._identify_commission_records(df)
        # Row 4: 'Affiliate分潤合作 品牌加碼' -> affiliate 被 exclude，ams include_and 匹配
        assert affiliate_mask[4] == False
        assert ams_mask[4] == True

    def test_case_insensitive_matching(self):
        """測試大小寫不敏感匹配"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'Item Description': ['AFFILIATE COMMISSION', 'ams COMMISSION'],
            'GL#': [None, None],
        })
        affiliate_mask, ams_mask = step._identify_commission_records(df)
        assert affiliate_mask[0] == True
        assert ams_mask[1] == True

    def test_na_description_no_error(self):
        """測試描述欄位含 NaN 不應報錯"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'Item Description': [None, pd.NA, 'Affiliate commission'],
            'GL#': [None, None, None],
        })
        affiliate_mask, ams_mask = step._identify_commission_records(df)
        assert affiliate_mask[0] == False
        assert affiliate_mask[1] == False
        assert affiliate_mask[2] == True


# ============================================================
# CommissionDataUpdateStep._update_commission_records 測試
# ============================================================

@pytest.mark.unit
class TestCommissionUpdateRecords:
    """_update_commission_records 欄位更新測試"""

    def test_update_affiliate_fields(self):
        """測試 Affiliate 分潤欄位正確更新"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        mask = pd.Series([True, False, False, False, False])
        config = step.commission_config['affiliate']
        step._update_commission_records(df, mask, 'affiliate', config)

        assert df.loc[0, 'Remarked by FN'] == '分潤'
        assert df.loc[0, 'GL#'] == '650022'
        assert df.loc[0, 'Account code'] == '650022'
        assert df.loc[0, 'Product code'] == 'EC_SPE_COM'
        # 未匹配的行不應更新
        assert df.loc[1, 'GL#'] is None

    def test_update_ams_fields(self):
        """測試 AMS 分潤欄位正確更新"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        mask = pd.Series([False, False, True, False, False])
        config = step.commission_config['ams']
        step._update_commission_records(df, mask, 'ams', config)

        assert df.loc[2, 'GL#'] == '650019'
        assert df.loc[2, 'Product code'] == 'EC_AMS_COST'

    def test_empty_mask_no_update(self):
        """測試空 mask 不更新任何記錄"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        mask = pd.Series([False] * 5)
        config = step.commission_config['affiliate']
        step._update_commission_records(df, mask, 'affiliate', config)
        assert df['GL#'].isna().all()


# ============================================================
# CommissionDataUpdateStep._set_accrual_estimation 測試
# ============================================================

@pytest.mark.unit
class TestCommissionSetAccrualEstimation:
    """_set_accrual_estimation 估計入帳設置測試"""

    def test_accrual_set_when_completed(self):
        """測試 GL# 匹配且 PO狀態 已完成時設置 Y"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'GL#': ['650022', '650019', '650022'],
            'Remarked by FN': ['分潤', '分潤', '分潤'],
            'PO狀態': ['已完成', '已完成(billed)', '未完成'],
            '是否估計入帳': [None, None, None],
        })
        count = step._set_accrual_estimation(df)
        assert count == 2
        assert df.loc[0, '是否估計入帳'] == 'Y'
        assert df.loc[1, '是否估計入帳'] == 'Y'

    def test_no_accrual_when_not_completed(self):
        """測試 PO狀態不含「已完成」時不設置"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'GL#': ['650022'],
            'Remarked by FN': ['分潤'],
            'PO狀態': ['未完成'],
            '是否估計入帳': [None],
        })
        count = step._set_accrual_estimation(df)
        assert count == 0

    def test_no_accrual_when_different_gl(self):
        """測試 GL# 不匹配時不設置"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'GL#': ['999999'],
            'Remarked by FN': ['分潤'],
            'PO狀態': ['已完成'],
            '是否估計入帳': [None],
        })
        count = step._set_accrual_estimation(df)
        assert count == 0


# ============================================================
# CommissionDataUpdateStep.execute 測試
# ============================================================

@pytest.mark.unit
class TestCommissionExecute:
    """CommissionDataUpdateStep execute 完整流程測試"""

    @pytest.mark.asyncio
    async def test_execute_success_with_commissions(self, commission_context):
        """測試包含分潤記錄的成功執行"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        result = await step.execute(commission_context)

        assert result.status == StepStatus.SUCCESS
        assert result.data is not None
        assert 'affiliate_commission' in result.metadata
        assert 'ams_commission' in result.metadata
        assert result.metadata['total_commission'] > 0

    @pytest.mark.asyncio
    async def test_execute_no_commission_records(self):
        """測試無分潤記錄時返回 SKIPPED"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'Item Description': ['Regular item A', 'Regular item B'],
            'GL#': [None, None],
            'Remarked by FN': [None, None],
            'Account code': [None, None],
            'Product code': [None, None],
            'PO狀態': [None, None],
            '是否估計入帳': [None, None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_skipped_for_non_spt(self):
        """測試非 SPT 實體跳過執行"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        ctx = ProcessingContext(data=df, entity_type='SPX',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_validation_failed_missing_column(self):
        """測試缺少描述欄位時驗證失敗"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'SomeOtherColumn': ['data'],
            'GL#': [None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    async def test_execute_updates_context_data(self, commission_context):
        """測試執行後 context.data 被正確更新"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        await step.execute(commission_context)
        # 驗證 context data 已更新（至少有分潤記錄的 GL# 被設置）
        updated_df = commission_context.data
        commission_rows = updated_df[updated_df['Remarked by FN'] == '分潤']
        assert len(commission_rows) > 0


# ============================================================
# CommissionDataUpdateStep.validate_input 測試
# ============================================================

@pytest.mark.unit
class TestCommissionValidateInput:
    """CommissionDataUpdateStep validate_input 測試"""

    @pytest.mark.asyncio
    async def test_validate_success(self, commission_context):
        """測試完整數據驗證通過"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        result = await step.validate_input(commission_context)
        assert result is True

    @pytest.mark.asyncio
    async def test_validate_empty_data_returns_false(self):
        """測試空數據驗證失敗"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        ctx = ProcessingContext(data=pd.DataFrame(), entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_creates_missing_columns(self):
        """測試缺少的欄位會被自動創建"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame({
            'Item Description': ['test'],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is True
        assert 'GL#' in ctx.data.columns
        assert 'Remarked by FN' in ctx.data.columns

    @pytest.mark.asyncio
    async def test_validate_non_spt_still_passes(self):
        """測試非 SPT 實體仍通過驗證（跳過在 execute 處理）"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        ctx = ProcessingContext(data=df, entity_type='SPX',
                                processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is True


# ============================================================
# CommissionDataUpdateStep._generate_statistics 測試
# ============================================================

@pytest.mark.unit
class TestCommissionGenerateStatistics:
    """_generate_statistics 統計信息測試"""

    def test_statistics_content(self):
        """測試統計信息包含正確內容"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = _make_commission_df()
        stats = step._generate_statistics(
            df=df, affiliate_count=2, ams_count=1,
            total_commission=3, accrual_count=1, input_count=5
        )
        assert stats['total_records'] == 5
        assert stats['commission_records'] == 3
        assert stats['affiliate_commission'] == 2
        assert stats['ams_commission'] == 1
        assert stats['accrual_set'] == 1
        assert '60.00%' in stats['commission_percentage']

    def test_statistics_zero_input(self):
        """測試 input_count=0 時不除零"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import CommissionDataUpdateStep
        step = CommissionDataUpdateStep()
        df = pd.DataFrame()
        stats = step._generate_statistics(
            df=df, affiliate_count=0, ams_count=0,
            total_commission=0, accrual_count=0, input_count=0
        )
        assert stats['commission_percentage'] == '0.00%'
        assert stats['accrual_rate'] == '0.00%'


# ============================================================
# PayrollDetectionStep 初始化測試
# ============================================================

@pytest.mark.unit
class TestPayrollDetectionStepInit:
    """PayrollDetectionStep 初始化測試"""

    def test_default_name(self):
        """測試預設步驟名稱"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        assert step.name == "Detect_Payroll_Records"

    def test_fallback_to_class_config(self):
        """測試 TOML 無配置時 fallback 至 class-level 常數"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        assert step.payroll_config['label'] == 'Payroll'


# ============================================================
# PayrollDetectionStep._identify_payroll_records 測試
# ============================================================

@pytest.mark.unit
class TestPayrollIdentifyRecords:
    """_identify_payroll_records Payroll 記錄識別測試"""

    def test_ebs_task_match(self):
        """測試 EBS Task 欄位匹配 payroll"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = _make_payroll_df()
        mask = step._identify_payroll_records(df)
        # Row 0: EBS Task 含 'payroll' -> True
        assert mask[0] == True
        # Row 2: EBS Task 含 'PAYROLL' (大寫) -> True
        assert mask[2] == True

    def test_description_match(self):
        """測試 Item Description 欄位匹配 payroll"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = _make_payroll_df()
        mask = step._identify_payroll_records(df)
        # Row 1: Item Description 含 'payroll service' -> True
        assert mask[1] == True

    def test_no_match(self):
        """測試不含 payroll 的記錄不匹配"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = _make_payroll_df()
        mask = step._identify_payroll_records(df)
        # Row 3: 'Office supplies' -> False
        assert mask[3] == False
        # Row 4: 'Regular task' -> False
        assert mask[4] == False

    def test_missing_ebs_task_column(self):
        """測試缺少 EBS Task 欄位時僅檢查 Description"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'Item Description': ['payroll item', 'regular item'],
            'Remarked by FN': [None, None],
        })
        mask = step._identify_payroll_records(df)
        assert mask[0] == True
        assert mask[1] == False


# ============================================================
# PayrollDetectionStep.execute 測試
# ============================================================

@pytest.mark.unit
class TestPayrollExecute:
    """PayrollDetectionStep execute 完整流程測試"""

    @pytest.mark.asyncio
    async def test_execute_success(self, payroll_context):
        """測試成功偵測並標記 Payroll 記錄"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        result = await step.execute(payroll_context)

        assert result.status == StepStatus.SUCCESS
        assert result.data is not None
        assert result.metadata['payroll_detected'] > 0
        assert result.metadata['payroll_labeled'] > 0

    @pytest.mark.asyncio
    async def test_execute_no_payroll_records(self):
        """測試無 Payroll 記錄時返回 SKIPPED"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'EBS Task': ['Regular task'],
            'Item Description': ['Regular item'],
            'Remarked by FN': [None],
            'PO狀態': [None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_skipped_for_non_spt(self):
        """測試非 SPT 實體跳過執行"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = _make_payroll_df()
        ctx = ProcessingContext(data=df, entity_type='SPX',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_skips_already_labeled_records(self):
        """測試已有標籤的 Payroll 記錄不被覆蓋"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'EBS Task': ['payroll task', 'payroll admin'],
            'Item Description': ['item A', 'item B'],
            'Remarked by FN': ['分潤', None],  # 第一筆已有標籤
            'PO狀態': [None, None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        # 第一筆保留原標籤
        assert result.data.loc[0, 'Remarked by FN'] == '分潤'
        # 第二筆更新為 Payroll
        assert result.data.loc[1, 'Remarked by FN'] == 'Payroll'

    @pytest.mark.asyncio
    async def test_execute_all_already_labeled_skipped(self):
        """測試所有 Payroll 記錄均已有標籤時返回 SKIPPED"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'EBS Task': ['payroll task'],
            'Item Description': ['item'],
            'Remarked by FN': ['分潤'],
            'PO狀態': [None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_validation_failed_missing_description(self):
        """測試缺少 Item Description 欄位時驗證失敗"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'SomeColumn': ['data'],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    async def test_execute_updates_status_column(self):
        """測試 Payroll 更新狀態欄位（含'狀態'的欄位）"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'EBS Task': ['payroll task'],
            'Item Description': ['item'],
            'Remarked by FN': [None],
            'PO狀態': [None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        # PO狀態 為 NA 且被標記為 payroll 時應更新
        assert result.data.loc[0, 'PO狀態'] == 'Payroll'

    @pytest.mark.asyncio
    async def test_execute_no_status_column(self):
        """測試無含'狀態'欄位時不報錯"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'EBS Task': ['payroll task'],
            'Item Description': ['item'],
            'Remarked by FN': [None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS


# ============================================================
# PayrollDetectionStep.validate_input 測試
# ============================================================

@pytest.mark.unit
class TestPayrollValidateInput:
    """PayrollDetectionStep validate_input 測試"""

    @pytest.mark.asyncio
    async def test_validate_success(self, payroll_context):
        """測試完整數據驗證通過"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        result = await step.validate_input(payroll_context)
        assert result is True

    @pytest.mark.asyncio
    async def test_validate_empty_data_returns_false(self):
        """測試空數據驗證失敗"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        ctx = ProcessingContext(data=pd.DataFrame(), entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_missing_description_returns_false(self):
        """測試缺少 Item Description 欄位驗證失敗"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'EBS Task': ['task'],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_missing_ebs_task_only_passes(self):
        """測試僅缺少 EBS Task 時驗證仍通過（非致命）"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'Item Description': ['test'],
            'Remarked by FN': [None],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is True

    @pytest.mark.asyncio
    async def test_validate_creates_missing_remark_column(self):
        """測試缺少 Remarked by FN 時自動創建"""
        from accrual_bot.tasks.spt.steps.spt_evaluation_affiliate import PayrollDetectionStep
        step = PayrollDetectionStep()
        df = pd.DataFrame({
            'Item Description': ['test'],
        })
        ctx = ProcessingContext(data=df, entity_type='SPT',
                                processing_date=202503, processing_type='PO')
        result = await step.validate_input(ctx)
        assert result is True
        assert 'Remarked by FN' in ctx.data.columns
