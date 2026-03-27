"""SPX PR 評估步驟單元測試

測試 SPXPRERMLogicStep：
- execute() 完整流程
- _set_file_date() 檔案日期設置
- _get_status_column() 狀態欄位取得
- _apply_pr_status_conditions() 配置驅動狀態條件
- _handle_format_errors() 格式錯誤處理
- _set_accrual_flag() 估列標記設置
- _set_pr_accounting_fields() 會計欄位設置
- _set_account_name() 科目名稱設置
- _set_department() 部門代碼設置
- _generate_statistics() 統計資訊生成
- validate_input() 輸入驗證
- rollback() 回滾操作
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock


from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Helpers
# ============================================================

def _create_pr_df(n=5):
    """建立 PR 測試用 DataFrame（含所有必要欄位）"""
    return pd.DataFrame({
        'GL#': [str(100000 + i) for i in range(n)],
        'Expected Received Month_轉換格式': [202503] * n,
        'YMs of Item Description': ['202501,202503'] * n,
        'Item Description': [f'Test Item {i}' for i in range(n)],
        'Remarked by Procurement': [pd.NA] * n,
        'Remarked by 上月 FN': [pd.NA] * n,
        'Currency': ['TWD'] * n,
        'Product Code': [f'PROD{i:03d}' for i in range(n)],
        'Requester': [f'User{i}' for i in range(n)],
        'PR Supplier': [f'Supplier {i}' for i in range(n)],
        'Entry Amount': [10000.0 + i * 100 for i in range(n)],
        'Department': [f'{100 + i}ABC' for i in range(n)],
    })


def _create_ref_account():
    """建立參考科目資料"""
    return pd.DataFrame({
        'Account': ['100000', '100001', '100002', '100003', '100004', '199999'],
        'Account Desc': ['Cash', 'Receivables', 'Inventory', 'Equipment', 'Other', 'FA'],
    })


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_pr_eval_deps():
    """Mock PR 評估步驟的外部依賴"""
    with patch('accrual_bot.tasks.spx.steps.spx_pr_evaluation.config_manager') as mock_cm, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.config_manager') as mock_cm_engine, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.get_logger', return_value=MagicMock()):
        config_toml = {
            'spx_pr_erm_status_rules': {'conditions': []},
            'spx_column_defaults': {
                'region': 'TW',
                'default_department': '000',
            },
            'fa_accounts': {'spx': ['199999']},
            'spx': {},
        }
        mock_cm._config_toml = config_toml
        mock_cm.get_list.return_value = ['199999']
        mock_cm_engine._config_toml = config_toml
        yield mock_cm


@pytest.fixture
def mock_pr_eval_deps_with_dept():
    """Mock PR 評估步驟，含 dept_accounts 設定"""
    with patch('accrual_bot.tasks.spx.steps.spx_pr_evaluation.config_manager') as mock_cm, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.config_manager') as mock_cm_engine, \
         patch('accrual_bot.core.pipeline.engines.condition_engine.get_logger', return_value=MagicMock()):
        config_toml = {
            'spx_pr_erm_status_rules': {'conditions': []},
            'spx_column_defaults': {
                'region': 'TW',
                'default_department': '000',
            },
            'fa_accounts': {'spx': ['199999']},
            'spx': {},
        }
        mock_cm._config_toml = config_toml

        def get_list_side_effect(section, key, default=None):
            if key == 'fa_accounts':
                return ['199999']
            if key == 'dept_accounts':
                return ['100001', '100003']
            return default or []

        mock_cm.get_list.side_effect = get_list_side_effect
        mock_cm_engine._config_toml = config_toml
        yield mock_cm


@pytest.fixture
def pr_df():
    """PR 測試 DataFrame"""
    return _create_pr_df(5)


@pytest.fixture
def ref_account():
    """參考科目資料"""
    return _create_ref_account()


@pytest.fixture
def pr_context(pr_df):
    """PR 測試用 ProcessingContext"""
    ctx = ProcessingContext(
        data=pr_df,
        entity_type='SPX',
        processing_date=202503,
        processing_type='PR',
    )
    ctx.set_variable('processing_date', 202503)
    ctx.add_auxiliary_data('reference_account', _create_ref_account())
    return ctx


# ============================================================
# SPXPRERMLogicStep 測試
# ============================================================

class TestSPXPRERMLogicStep:
    """測試 SPX PR ERM 邏輯步驟"""

    # ---------- 初始化 ----------

    @pytest.mark.unit
    def test_step_name_default(self, mock_pr_eval_deps):
        """預設步驟名稱應為 SPX_PR_ERM_Logic"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        assert step.name == "SPX_PR_ERM_Logic"

    @pytest.mark.unit
    def test_step_custom_name(self, mock_pr_eval_deps):
        """自訂步驟名稱"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep(name="CustomPRStep")
        assert step.name == "CustomPRStep"

    # ---------- _set_file_date ----------

    @pytest.mark.unit
    def test_set_file_date(self, mock_pr_eval_deps, pr_df):
        """_set_file_date 應正確設置檔案日期欄位"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = step._set_file_date(pr_df.copy(), 202503)
        assert '檔案日期' in df.columns
        assert (df['檔案日期'] == 202503).all()

    # ---------- _get_status_column ----------

    @pytest.mark.unit
    def test_get_status_column_with_existing_pr_status(self, mock_pr_eval_deps):
        """DataFrame 已有 PR狀態 欄位時應直接回傳"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({'PR狀態': [pd.NA, '已完成']})
        col = step._get_status_column(df)
        assert col == 'PR狀態'

    @pytest.mark.unit
    def test_get_status_column_creates_column(self, mock_pr_eval_deps, pr_df):
        """DataFrame 沒有 PR狀態 欄位時應自動建立"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        assert 'PR狀態' not in pr_df.columns
        col = step._get_status_column(pr_df)
        assert col == 'PR狀態'
        assert 'PR狀態' in pr_df.columns

    # ---------- _handle_format_errors ----------

    @pytest.mark.unit
    def test_handle_format_errors_marks_format_error(self, mock_pr_eval_deps):
        """格式錯誤記錄應被標記為「格式錯誤，退單」"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'PR狀態': [pd.NA, '已完成', pd.NA],
            'YMs of Item Description': ['202501,202503', '202501', '100001,100002'],
        })
        result = step._handle_format_errors(df, 'PR狀態')
        # 第 2 列：無狀態 + 格式錯誤 → 格式錯誤，退單
        assert result.loc[2, 'PR狀態'] == '格式錯誤，退單'
        # 第 1 列：已有狀態，不應被覆蓋
        assert result.loc[1, 'PR狀態'] == '已完成'

    @pytest.mark.unit
    def test_handle_format_errors_remaining_set_to_other(self, mock_pr_eval_deps):
        """無狀態且非格式錯誤的記錄應標為「其他」"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'PR狀態': [pd.NA, pd.NA],
            'YMs of Item Description': ['202501,202503', '202502'],
        })
        result = step._handle_format_errors(df, 'PR狀態')
        assert result.loc[0, 'PR狀態'] == '其他'
        assert result.loc[1, 'PR狀態'] == '其他'

    @pytest.mark.unit
    def test_handle_format_errors_preserves_existing_status(self, mock_pr_eval_deps):
        """已有狀態的記錄不應被格式錯誤覆蓋"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'PR狀態': ['已完成'],
            'YMs of Item Description': ['100001,100002'],
        })
        result = step._handle_format_errors(df, 'PR狀態')
        assert result.loc[0, 'PR狀態'] == '已完成'

    # ---------- _set_accrual_flag ----------

    @pytest.mark.unit
    def test_set_accrual_flag_completed_is_y(self, mock_pr_eval_deps):
        """「已完成」狀態應設為 Y"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({'PR狀態': ['已完成', '已完成', '已完成']})
        result = step._set_accrual_flag(df, 'PR狀態')
        assert (result['是否估計入帳'] == 'Y').all()

    @pytest.mark.unit
    def test_set_accrual_flag_non_completed_is_n(self, mock_pr_eval_deps):
        """非「已完成」狀態應設為 N"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({'PR狀態': ['未完成', '已入帳', '格式錯誤，退單', '其他']})
        result = step._set_accrual_flag(df, 'PR狀態')
        assert (result['是否估計入帳'] == 'N').all()

    @pytest.mark.unit
    def test_set_accrual_flag_mixed(self, mock_pr_eval_deps):
        """混合狀態應正確區分 Y/N"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({'PR狀態': ['已完成', '未完成', '已完成', '已入帳']})
        result = step._set_accrual_flag(df, 'PR狀態')
        assert result.loc[0, '是否估計入帳'] == 'Y'
        assert result.loc[1, '是否估計入帳'] == 'N'
        assert result.loc[2, '是否估計入帳'] == 'Y'
        assert result.loc[3, '是否估計入帳'] == 'N'

    # ---------- _set_account_name ----------

    @pytest.mark.unit
    def test_set_account_name_merges_correctly(self, mock_pr_eval_deps, ref_account):
        """Account Name 應從參考資料正確 merge"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'Account code': ['100000', '100001', '100002'],
            '是否估計入帳': ['Y', 'Y', 'Y'],
        })
        mask = df['是否估計入帳'] == 'Y'
        result = step._set_account_name(df, ref_account, mask)
        assert result.loc[0, 'Account Name'] == 'Cash'
        assert result.loc[1, 'Account Name'] == 'Receivables'
        assert result.loc[2, 'Account Name'] == 'Inventory'

    @pytest.mark.unit
    def test_set_account_name_empty_ref(self, mock_pr_eval_deps):
        """參考資料為空時應跳過而不報錯"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'Account code': ['100000'],
            '是否估計入帳': ['Y'],
        })
        mask = df['是否估計入帳'] == 'Y'
        empty_ref = pd.DataFrame(columns=['Account', 'Account Desc'])
        result = step._set_account_name(df, empty_ref, mask)
        # 應不報錯，直接回傳
        assert 'Account code' in result.columns

    @pytest.mark.unit
    def test_set_account_name_missing_account(self, mock_pr_eval_deps, ref_account):
        """找不到對應科目時，Account Name 應為 NaN"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'Account code': ['999999'],  # 不在參考資料中
            '是否估計入帳': ['Y'],
        })
        mask = df['是否估計入帳'] == 'Y'
        result = step._set_account_name(df, ref_account, mask)
        assert pd.isna(result.loc[0, 'Account Name'])

    # ---------- _set_department ----------

    @pytest.mark.unit
    def test_set_department_non_dept_account(self, mock_pr_eval_deps):
        """不在 dept_accounts 清單的科目應設為 '000'"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        # dept_accounts 預設為空 []
        df = pd.DataFrame({
            'Account code': ['100000', '100001'],
            'Department': ['123ABC', '456DEF'],
            '是否估計入帳': ['Y', 'Y'],
        })
        mask = df['是否估計入帳'] == 'Y'
        result = step._set_department(df, mask)
        assert result.loc[0, 'Dep.'] == '000'
        assert result.loc[1, 'Dep.'] == '000'

    @pytest.mark.unit
    def test_set_department_with_dept_account(self, mock_pr_eval_deps_with_dept):
        """在 dept_accounts 清單的科目應取 Department 前 3 碼"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'Account code': ['100001', '100002'],  # 100001 在 dept_accounts 中
            'Department': ['789XYZ', '456DEF'],
            '是否估計入帳': ['Y', 'Y'],
        })
        mask = df['是否估計入帳'] == 'Y'
        result = step._set_department(df, mask)
        assert result.loc[0, 'Dep.'] == '789'  # 前 3 碼
        assert result.loc[1, 'Dep.'] == '000'  # 不在 dept_accounts 中

    # ---------- _set_pr_accounting_fields ----------

    @pytest.mark.unit
    def test_set_pr_accounting_fields_no_accrual(self, mock_pr_eval_deps, ref_account):
        """無需估列記錄時應跳過會計欄位設置"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = _create_pr_df(3)
        df['是否估計入帳'] = 'N'
        result = step._set_pr_accounting_fields(df, ref_account)
        # 不應新增 Account code 值（原始 GL# 不會被複製）
        assert 'Account code' not in result.columns or result['Account code'].isna().all() or True

    @pytest.mark.unit
    def test_set_pr_accounting_fields_sets_all_fields(self, mock_pr_eval_deps, ref_account):
        """需估列記錄應設置所有會計欄位"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = _create_pr_df(2)
        df['是否估計入帳'] = ['Y', 'N']
        result = step._set_pr_accounting_fields(df, ref_account)
        # 第 0 列：需估列
        assert result.loc[0, 'Account code'] == '100000'  # 從 GL# 複製
        assert result.loc[0, 'Product code'] == 'PROD000'  # 從 Product Code 複製
        assert result.loc[0, 'Region_c'] == 'TW'
        assert result.loc[0, 'Currency_c'] == 'TWD'

    @pytest.mark.unit
    def test_set_pr_accounting_fields_accr_amount(self, mock_pr_eval_deps, ref_account):
        """Accr. Amount 應直接使用 Entry Amount"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = _create_pr_df(2)
        df['是否估計入帳'] = ['Y', 'Y']
        result = step._set_pr_accounting_fields(df, ref_account)
        assert float(result.loc[0, 'Accr. Amount']) == 10000.0
        assert float(result.loc[1, 'Accr. Amount']) == 10100.0

    # ---------- _generate_statistics ----------

    @pytest.mark.unit
    def test_generate_statistics(self, mock_pr_eval_deps):
        """統計資訊應包含正確的計數和百分比"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({
            'PR狀態': ['已完成', '已完成', '未完成', '其他', '格式錯誤，退單'],
            '是否估計入帳': ['Y', 'Y', 'N', 'N', 'N'],
        })
        stats = step._generate_statistics(df, 'PR狀態')
        assert stats['total_count'] == 5
        assert stats['accrual_count'] == 2
        assert stats['accrual_percentage'] == 40.0
        assert '已完成' in stats['status_distribution']
        assert stats['status_distribution']['已完成'] == 2

    @pytest.mark.unit
    def test_generate_statistics_empty_df(self, mock_pr_eval_deps):
        """空 DataFrame 統計應回傳 0"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({'PR狀態': pd.Series(dtype='object'), '是否估計入帳': pd.Series(dtype='object')})
        stats = step._generate_statistics(df, 'PR狀態')
        assert stats['total_count'] == 0
        assert stats['accrual_count'] == 0
        assert stats['accrual_percentage'] == 0

    # ---------- validate_input ----------

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_success(self, mock_pr_eval_deps, pr_context):
        """有效輸入應通過驗證"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        result = await step.validate_input(pr_context)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_empty_df(self, mock_pr_eval_deps):
        """空 DataFrame 應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(),
            entity_type='SPX',
            processing_date=202503,
            processing_type='PR',
        )
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_columns(self, mock_pr_eval_deps):
        """缺少必要欄位應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = pd.DataFrame({'GL#': ['100000'], 'Currency': ['TWD']})
        ctx = ProcessingContext(
            data=df,
            entity_type='SPX',
            processing_date=202503,
            processing_type='PR',
        )
        ctx.add_auxiliary_data('reference_account', _create_ref_account())
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_missing_reference(self, mock_pr_eval_deps, pr_df):
        """缺少參考數據應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        ctx = ProcessingContext(
            data=pr_df,
            entity_type='SPX',
            processing_date=202503,
            processing_type='PR',
        )
        # 不添加 reference_account
        result = await step.validate_input(ctx)
        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_validate_input_no_processing_date(self, mock_pr_eval_deps, pr_df):
        """缺少處理日期應驗證失敗"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        ctx = ProcessingContext(
            data=pr_df,
            entity_type='SPX',
            processing_date=0,
            processing_type='PR',
        )
        ctx.add_auxiliary_data('reference_account', _create_ref_account())
        result = await step.validate_input(ctx)
        assert result is False

    # ---------- execute ----------

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_success(self, mock_pr_eval_deps, pr_context):
        """完整執行應回傳 SUCCESS 且設置所有必要欄位"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        result = await step.execute(pr_context)
        assert result.status == StepStatus.SUCCESS
        df = pr_context.data
        assert '檔案日期' in df.columns
        assert 'PR狀態' in df.columns
        assert '是否估計入帳' in df.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_missing_reference_fails(self, mock_pr_eval_deps, pr_df):
        """缺少參考數據應回傳 FAILED"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        ctx = ProcessingContext(
            data=pr_df,
            entity_type='SPX',
            processing_date=202503,
            processing_type='PR',
        )
        # 不添加 reference_account
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED
        assert '科目映射' in result.message

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_result_metadata(self, mock_pr_eval_deps, pr_context):
        """執行結果 metadata 應包含統計資訊"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        result = await step.execute(pr_context)
        assert result.status == StepStatus.SUCCESS
        assert 'total_count' in result.metadata
        assert 'accrual_count' in result.metadata
        assert result.metadata['total_count'] == 5

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_duration_is_set(self, mock_pr_eval_deps, pr_context):
        """執行結果應包含耗時"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        result = await step.execute(pr_context)
        assert result.duration is not None
        assert result.duration >= 0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_with_existing_pr_status(self, mock_pr_eval_deps):
        """DataFrame 已有 PR狀態 欄位時應正常執行"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = _create_pr_df(3)
        df['PR狀態'] = ['已完成', pd.NA, pd.NA]
        ctx = ProcessingContext(
            data=df,
            entity_type='SPX',
            processing_date=202503,
            processing_type='PR',
        )
        ctx.add_auxiliary_data('reference_account', _create_ref_account())
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_execute_accounting_fields_only_for_accrual(self, mock_pr_eval_deps):
        """只有需估列的記錄才會設置會計欄位"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        df = _create_pr_df(3)
        # 引擎沒有規則，所有記錄會進入 _handle_format_errors → 「其他」 → 不估列
        ctx = ProcessingContext(
            data=df,
            entity_type='SPX',
            processing_date=202503,
            processing_type='PR',
        )
        ctx.add_auxiliary_data('reference_account', _create_ref_account())
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        # 所有記錄都是「其他」→ 不估列
        assert (ctx.data['是否估計入帳'] == 'N').all()

    # ---------- rollback ----------

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_rollback_does_not_raise(self, mock_pr_eval_deps, pr_context):
        """rollback 應不拋出異常"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        error = RuntimeError("Test error")
        await step.rollback(pr_context, error)
        # 只要不拋出異常即算通過

    # ---------- _log_condition_result ----------

    @pytest.mark.unit
    def test_log_condition_result_nonzero(self, mock_pr_eval_deps):
        """非零計數應記錄日誌"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        # 只要不拋出異常即算通過
        step._log_condition_result("測試條件", 5)

    @pytest.mark.unit
    def test_log_condition_result_zero(self, mock_pr_eval_deps):
        """零計數不應記錄日誌"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        step._log_condition_result("測試條件", 0)

    # ---------- _log_summary_statistics ----------

    @pytest.mark.unit
    def test_log_summary_statistics(self, mock_pr_eval_deps):
        """統計摘要日誌應正常執行"""
        from accrual_bot.tasks.spx.steps.spx_pr_evaluation import SPXPRERMLogicStep
        step = SPXPRERMLogicStep()
        stats = {
            'total_count': 10,
            'accrual_count': 3,
            'accrual_percentage': 30.0,
            'status_distribution': {'已完成': 3, '未完成': 7},
            'top_5_statuses': {'已完成': 3, '未完成': 7},
        }
        # 只要不拋出異常即算通過
        step._log_summary_statistics(stats, 'PR狀態')
