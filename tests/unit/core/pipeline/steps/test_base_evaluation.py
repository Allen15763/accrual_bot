"""BaseERMEvaluationStep 單元測試"""
import pytest
from unittest.mock import patch
import pandas as pd

from accrual_bot.core.pipeline.steps.base_evaluation import (
    BaseERMEvaluationStep,
    BaseERMConditions
)
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus
from tests.fixtures.sample_data import (
    create_minimal_erm_df,
    create_reference_account_df,
    create_reference_liability_df,
    create_complex_erm_scenario_df
)


# --- 測試用具體實現 ---

class ConcreteERMStep(BaseERMEvaluationStep):
    """測試用的具體 ERMStep"""

    def _build_conditions(self, df: pd.DataFrame, file_date: int, status_column: str):
        """構建簡單的測試條件"""
        return BaseERMConditions(
            no_status=df[status_column].isna(),
            in_date_range=pd.Series([True] * len(df), index=df.index),
            erm_before_or_equal_file_date=pd.Series([True] * len(df), index=df.index),
            erm_after_file_date=pd.Series([False] * len(df), index=df.index),
            format_error=df['YMs of Item Description'].str.contains('格式錯誤', na=False),
            out_of_date_range=pd.Series([False] * len(df), index=df.index),
            procurement_not_error=pd.Series([True] * len(df), index=df.index)
        )

    def _apply_status_conditions(self, df, conditions, status_column):
        """應用簡單的狀態邏輯"""
        # 格式錯誤 且 無狀態 -> 格式錯誤，退單
        mask_format_error = conditions.format_error & conditions.no_status
        df.loc[mask_format_error, status_column] = '格式錯誤，退單'

        # 其他無狀態 -> 已完成
        mask_no_status = conditions.no_status & ~mask_format_error
        df.loc[mask_no_status, status_column] = '已完成'

        return df

    def _set_accounting_fields(self, df, ref_account, ref_liability):
        """設置會計欄位"""
        # 簡單合併 Account Name
        df = df.merge(
            ref_account[['Account', 'Account Desc']],
            left_on='Account code',
            right_on='Account',
            how='left',
            suffixes=('', '_ref')
        )
        df['Account Name'] = df['Account Desc']
        df = df.drop(columns=['Account', 'Account Desc'], errors='ignore')

        # 設置 Liability
        df = df.merge(
            ref_liability[['Account', 'Liability']],
            left_on='Account code',
            right_on='Account',
            how='left',
            suffixes=('', '_lib')
        )
        df = df.drop(columns=['Account_lib'], errors='ignore')

        return df


# --- 測試套件 ---

@pytest.mark.unit
class TestBaseERMEvaluationStep:
    """BaseERMEvaluationStep 測試套件"""

    @pytest.fixture
    def step(self):
        """創建測試步驟"""
        return ConcreteERMStep(name="TestERM")

    @pytest.fixture
    def context_with_data(self):
        """創建包含測試數據的 context"""
        df = create_minimal_erm_df()
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO'
        )

        # 添加參考數據
        ctx.add_auxiliary_data('reference_account', create_reference_account_df())
        ctx.add_auxiliary_data('reference_liability', create_reference_liability_df())

        return ctx

    # --- 檔案日期設置測試 ---

    def test_set_file_date(self, step):
        """測試設置檔案日期欄位"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        processing_date = 202512

        result = step._set_file_date(df, processing_date)

        assert '檔案日期' in result.columns
        assert all(result['檔案日期'] == 202512)

    # --- 狀態欄位判斷測試 ---

    def test_get_status_column_po(self, step, context_with_data):
        """測試 PO 類型返回 'PO狀態'"""
        df = context_with_data.data
        df['PO狀態'] = pd.NA

        status_col = step._get_status_column(df, context_with_data)

        assert status_col == 'PO狀態'

    def test_get_status_column_pr(self, step):
        """測試 PR 類型返回 'PR狀態'"""
        df = pd.DataFrame({'PR狀態': [pd.NA, pd.NA]})
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PR'
        )

        status_col = step._get_status_column(df, ctx)

        assert status_col == 'PR狀態'

    # --- 格式錯誤處理測試 ---

    def test_handle_format_errors(self, step):
        """測試格式錯誤標記"""
        df = pd.DataFrame({
            'PO狀態': [pd.NA, pd.NA, '已完成']
        })

        conditions = BaseERMConditions(
            no_status=pd.Series([True, True, False]),
            in_date_range=pd.Series([True, True, True]),
            erm_before_or_equal_file_date=pd.Series([True, True, True]),
            erm_after_file_date=pd.Series([False, False, False]),
            format_error=pd.Series([True, False, False]),
            out_of_date_range=pd.Series([False, False, False]),
            procurement_not_error=pd.Series([True, True, True])
        )

        result = step._handle_format_errors(df, conditions, 'PO狀態')

        # 第一筆有格式錯誤且無狀態，應該被標記
        assert result.loc[0, 'PO狀態'] == '格式錯誤，退單'
        # 第二筆無格式錯誤，應該保持原樣
        assert pd.isna(result.loc[1, 'PO狀態'])
        # 第三筆已有狀態，應該保持原樣
        assert result.loc[2, 'PO狀態'] == '已完成'

    # --- 估列標記設置測試 ---

    def test_set_accrual_flag_completed_status(self, step):
        """測試「已完成」狀態設為 'Y'"""
        df = pd.DataFrame({
            'PO狀態': ['已完成', '未完成', '已完成(check qty)']
        })

        result = step._set_accrual_flag(df, 'PO狀態')

        assert '是否估計入帳' in result.columns
        assert result.loc[0, '是否估計入帳'] == 'Y'
        assert result.loc[1, '是否估計入帳'] == 'N'
        assert result.loc[2, '是否估計入帳'] == 'Y'  # 包含「已完成」

    def test_set_accrual_flag_no_completed(self, step):
        """測試無「已完成」時全部為 'N'"""
        df = pd.DataFrame({
            'PO狀態': ['未完成', 'Check收貨', '格式錯誤']
        })

        result = step._set_accrual_flag(df, 'PO狀態')

        assert all(result['是否估計入帳'] == 'N')

    # --- 統計信息生成測試 ---

    def test_generate_statistics(self, step):
        """測試統計信息生成"""
        df = pd.DataFrame({
            'PO狀態': ['已完成', '已完成', '未完成', '格式錯誤', '已完成'],
            '是否估計入帳': ['Y', 'Y', 'N', 'N', 'Y']
        })

        stats = step._generate_statistics(df, 'PO狀態')

        assert stats['total_count'] == 5  # 實際實現使用 total_count
        assert stats['accrual_count'] == 3  # 3筆 'Y'
        assert '已完成' in stats['status_distribution']
        assert stats['status_distribution']['已完成'] == 3
        assert stats['status_distribution']['未完成'] == 1
        assert stats['status_distribution']['格式錯誤'] == 1

    # --- 執行流程測試 ---

    @pytest.mark.asyncio
    async def test_execute_success_flow(
        self, step, context_with_data, mock_config_manager
    ):
        """測試完整執行流程成功"""
        result = await step.execute(context_with_data)

        assert result.status == StepStatus.SUCCESS

        df = context_with_data.data

        # 驗證檔案日期已設置
        assert '檔案日期' in df.columns

        # 驗證狀態已更新
        assert 'PO狀態' in df.columns
        assert not all(df['PO狀態'].isna())

        # 驗證估列標記已設置
        assert '是否估計入帳' in df.columns
        assert set(df['是否估計入帳'].unique()).issubset({'Y', 'N'})

        # 驗證會計欄位已設置
        assert 'Account Name' in df.columns
        assert 'Liability' in df.columns

    @pytest.mark.asyncio
    async def test_execute_missing_reference_data(
        self, step, mock_config_manager
    ):
        """測試缺少參考數據時失敗"""
        df = create_minimal_erm_df()
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO'
        )
        # 不添加參考數據

        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        # 實際實現拋出 ValueError 而非 KeyError
        assert 'ValueError' in result.metadata.get('error_type', '') or result.error is not None

    # --- 條件構建測試 ---

    def test_build_conditions_structure(
        self, step, context_with_data
    ):
        """測試條件構建返回正確結構"""
        df = context_with_data.data

        conditions = step._build_conditions(df, 202512, 'PO狀態')

        assert isinstance(conditions, BaseERMConditions)
        assert hasattr(conditions, 'no_status')
        assert hasattr(conditions, 'format_error')
        assert len(conditions.no_status) == len(df)

    # --- 狀態條件應用測試 ---

    def test_apply_status_conditions(
        self, step, context_with_data
    ):
        """測試狀態條件應用"""
        df = context_with_data.data.copy()

        conditions = step._build_conditions(df, 202512, 'PO狀態')
        result = step._apply_status_conditions(df, conditions, 'PO狀態')

        # 驗證狀態已被更新
        assert not all(result['PO狀態'].isna())

        # 驗證格式錯誤被標記
        format_error_mask = result['YMs of Item Description'].str.contains('格式錯誤', na=False)
        if format_error_mask.any():
            assert '格式錯誤' in result.loc[format_error_mask, 'PO狀態'].values[0]

    # --- 會計欄位設置測試 ---

    def test_set_accounting_fields(
        self, step, context_with_data
    ):
        """測試會計欄位設置"""
        df = context_with_data.data.copy()
        ref_account = context_with_data.get_auxiliary_data('reference_account')
        ref_liability = context_with_data.get_auxiliary_data('reference_liability')

        result = step._set_accounting_fields(df, ref_account, ref_liability)

        # 驗證 Account Name 被添加
        assert 'Account Name' in result.columns

        # 驗證 Liability 被添加
        assert 'Liability' in result.columns

        # 驗證合併正確
        account_with_ref = result[result['Account code'] == '100000']
        if not account_with_ref.empty:
            assert account_with_ref.iloc[0]['Account Name'] == 'Cash'
            assert account_with_ref.iloc[0]['Liability'] == '111111'

    # --- 複雜場景測試 ---

    @pytest.mark.asyncio
    async def test_complex_scenario(
        self, step, mock_config_manager
    ):
        """測試複雜 ERM 場景"""
        df = create_complex_erm_scenario_df()
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO'
        )
        ctx.add_auxiliary_data('reference_account', create_reference_account_df())
        ctx.add_auxiliary_data('reference_liability', create_reference_liability_df())

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS

        df_result = ctx.data

        # 驗證統計
        stats = result.metadata
        assert stats['total_count'] == 4  # 實際實現使用 total_count
        assert stats['accrual_count'] >= 0

        # 驗證不同狀態記錄
        completed_count = (df_result['PO狀態'].str.contains('已完成', na=False)).sum()
        assert completed_count >= 1


# --- 參數化測試 ---

@pytest.mark.parametrize("status,expected_accrual", [
    ('已完成', 'Y'),
    ('未完成', 'N'),
    ('Check收貨', 'N'),
    ('格式錯誤，退單', 'N'),
    ('已完成(check qty)', 'Y'),
    ('已完成(exceed period)', 'Y'),
])
def test_accrual_flag_various_statuses(status, expected_accrual):
    """參數化測試不同狀態的估列標記"""
    step = ConcreteERMStep(name="Test")
    df = pd.DataFrame({'PO狀態': [status]})

    result = step._set_accrual_flag(df, 'PO狀態')

    assert result.loc[0, '是否估計入帳'] == expected_accrual


# --- _set_account_name 測試 ---

@pytest.mark.unit
class TestSetAccountName:
    """測試 _set_account_name 方法"""

    @pytest.fixture
    def step(self):
        return ConcreteERMStep(name="TestERM")

    def test_set_account_name_maps_correctly(self, step):
        """測試科目名稱正確映射 - GL# 對應 Account Name"""
        df = pd.DataFrame({
            'Account code': ['100000', '100001', '100002'],
            'PO狀態': ['已完成', '已完成', '已完成'],
        })
        ref_account = create_reference_account_df()
        mask = pd.Series([True, True, True], index=df.index)

        result = step._set_account_name(df, ref_account, mask)

        assert 'Account Name' in result.columns
        assert result.loc[0, 'Account Name'] == 'Cash'
        assert result.loc[1, 'Account Name'] == 'Receivables'
        assert result.loc[2, 'Account Name'] == 'Inventory'

    def test_set_account_name_empty_ref(self, step):
        """測試參考科目資料為空時，直接返回原始 DataFrame"""
        df = pd.DataFrame({
            'Account code': ['100000'],
        })
        ref_account = pd.DataFrame(columns=['Account', 'Account Desc'])
        mask = pd.Series([True], index=df.index)

        result = step._set_account_name(df, ref_account, mask)

        # 參考資料為空，不應該新增 Account Name 欄位（或維持原樣）
        assert 'Account Name' not in result.columns or result['Account Name'].isna().all()

    def test_set_account_name_unmatched_code(self, step):
        """測試無法匹配的科目代碼，Account Name 應為 NaN"""
        df = pd.DataFrame({
            'Account code': ['999999'],
        })
        ref_account = create_reference_account_df()
        mask = pd.Series([True], index=df.index)

        result = step._set_account_name(df, ref_account, mask)

        assert 'Account Name' in result.columns
        assert pd.isna(result.loc[0, 'Account Name'])


# --- _set_department 測試 ---

@pytest.mark.unit
class TestSetDepartment:
    """測試 _set_department 方法"""

    @pytest.fixture
    def step(self):
        """建立帶有 dept_accounts 的測試步驟"""
        s = ConcreteERMStep(name="TestERM")
        s.dept_accounts = ['100001']
        return s

    def test_set_department_in_dept_accounts(self, step):
        """測試科目在 dept_accounts 清單中時，取 Department 前3碼"""
        df = pd.DataFrame({
            'Account code': ['100001'],
            'Department': ['ABC123'],
            'Dep.': [''],
        })
        mask = pd.Series([True], index=df.index)

        result = step._set_department(df, mask)

        assert result.loc[0, 'Dep.'] == 'ABC'

    def test_set_department_not_in_dept_accounts(self, step):
        """測試科目不在 dept_accounts 清單中時，設為 '000'"""
        df = pd.DataFrame({
            'Account code': ['100000'],
            'Department': ['XYZ789'],
            'Dep.': [''],
        })
        mask = pd.Series([True], index=df.index)

        result = step._set_department(df, mask)

        assert result.loc[0, 'Dep.'] == '000'


# --- _calculate_accrual_amount 測試 ---

@pytest.mark.unit
class TestCalculateAccrualAmount:
    """測試 _calculate_accrual_amount 方法"""

    @pytest.fixture
    def step(self):
        return ConcreteERMStep(name="TestERM")

    def test_calculate_accrual_amount_basic(self, step):
        """測試基本預估金額計算: Unit Price * (Entry Qty - Billed Qty)"""
        df = pd.DataFrame({
            'Unit Price': [100.0, 200.0],
            'Entry Quantity': [10, 20],
            'Billed Quantity': [5, 15],
        })
        mask = pd.Series([True, True], index=df.index)

        result = step._calculate_accrual_amount(df, mask)

        assert 'Accr. Amount' in result.columns
        # 100 * (10 - 5) = 500
        assert float(result.loc[0, 'Accr. Amount']) == 500.0
        # 200 * (20 - 15) = 1000
        assert float(result.loc[1, 'Accr. Amount']) == 1000.0
        # temp_amount 欄位應被移除
        assert 'temp_amount' not in result.columns

    def test_calculate_accrual_amount_partial_mask(self, step):
        """測試部分 mask 為 False 時，只計算 mask 為 True 的列"""
        df = pd.DataFrame({
            'Unit Price': [100.0, 200.0],
            'Entry Quantity': [10, 20],
            'Billed Quantity': [5, 15],
        })
        mask = pd.Series([True, False], index=df.index)

        result = step._calculate_accrual_amount(df, mask)

        assert float(result.loc[0, 'Accr. Amount']) == 500.0
        assert pd.isna(result.loc[1, 'Accr. Amount'])


# --- _handle_prepayment 測試 ---

@pytest.mark.unit
class TestHandlePrepayment:
    """測試 _handle_prepayment 方法"""

    @pytest.fixture
    def step(self):
        return ConcreteERMStep(name="TestERM")

    def test_handle_prepayment_with_prepay(self, step):
        """測試有預付款時，是否有預付 = 'Y'，Liability = '111112'"""
        df = pd.DataFrame({
            'Entry Prepay Amount': ['100', '0'],
            'Account code': ['100000', '100001'],
            '是否有預付': [pd.NA, pd.NA],
            'Liability': [pd.NA, pd.NA],
        })
        mask = pd.Series([True, True], index=df.index)
        ref_liability = create_reference_liability_df()

        result = step._handle_prepayment(df, mask, ref_liability)

        # 第一筆有預付款
        assert result.loc[0, '是否有預付'] == 'Y'
        assert result.loc[0, 'Liability'] == '111112'

    def test_handle_prepayment_without_prepay(self, step):
        """測試無預付款時，從參考資料查找 Liability"""
        df = pd.DataFrame({
            'Entry Prepay Amount': ['0', '0'],
            'Account code': ['100000', '100001'],
            '是否有預付': [pd.NA, pd.NA],
            'Liability': [pd.NA, pd.NA],
        })
        mask = pd.Series([True, True], index=df.index)
        ref_liability = create_reference_liability_df()

        result = step._handle_prepayment(df, mask, ref_liability)

        # 無預付款，Liability 從參考資料查找
        assert result.loc[0, 'Liability'] == '111111'
        assert result.loc[1, 'Liability'] == '111112'

    def test_handle_prepayment_empty_ref_liability(self, step):
        """測試參考負債資料為空時的處理"""
        df = pd.DataFrame({
            'Entry Prepay Amount': ['100', '0'],
            'Account code': ['100000', '100001'],
            '是否有預付': [pd.NA, pd.NA],
            'Liability': [pd.NA, pd.NA],
        })
        mask = pd.Series([True, True], index=df.index)
        ref_liability = pd.DataFrame(columns=['Account', 'Liability'])

        result = step._handle_prepayment(df, mask, ref_liability)

        # 有預付款的仍應設置 Liability = '111112'
        assert result.loc[0, '是否有預付'] == 'Y'
        assert result.loc[0, 'Liability'] == '111112'
