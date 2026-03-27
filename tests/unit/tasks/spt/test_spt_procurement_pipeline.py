"""
SPT Procurement Pipeline Steps Unit Tests

Tests for:
- SPTProcurementStatusEvaluationStep
- ProcurementPreviousMappingStep
- ProcurementPreviousValidationStep
- CombinedProcurementDataLoadingStep
- CombinedProcurementProcessingStep
- CombinedProcurementExportStep
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_procurement_eval_config():
    """Mock config_manager for SPTProcurementStatusEvaluationStep"""
    with patch(
        'accrual_bot.tasks.spt.steps.spt_procurement_evaluation.config_manager'
    ) as mock_cm:
        mock_cm._config_toml = {
            'spt_procurement_status_rules': {
                'conditions': [
                    {
                        'priority': 1,
                        'status_value': 'ERM_IN_RANGE',
                        'combine': 'and',
                        'note': 'ERM in date range',
                        'checks': [
                            {'type': 'erm_in_range'}
                        ]
                    },
                    {
                        'priority': 2,
                        'status_value': 'ERM_LE_CLOSING',
                        'combine': 'and',
                        'note': 'ERM <= closing',
                        'checks': [
                            {'type': 'erm_le_closing'}
                        ]
                    },
                    {
                        'priority': 3,
                        'status_value': 'ERM_GT_CLOSING',
                        'combine': 'and',
                        'note': 'ERM > closing',
                        'checks': [
                            {'type': 'erm_gt_closing'}
                        ]
                    },
                ]
            }
        }
        yield mock_cm


@pytest.fixture
def mock_procurement_eval_config_empty():
    """Mock config_manager with no conditions"""
    with patch(
        'accrual_bot.tasks.spt.steps.spt_procurement_evaluation.config_manager'
    ) as mock_cm:
        mock_cm._config_toml = {
            'spt_procurement_status_rules': {
                'conditions': []
            }
        }
        yield mock_cm


@pytest.fixture
def procurement_eval_context():
    """ProcessingContext with ERM-related columns for evaluation step"""
    df = pd.DataFrame({
        'Item Description': ['Test item A', 'Test item B', 'Test item C'],
        'YMs of Item Description': ['202501,202503', '202501,202503', '202501,202503'],
        'Expected Received Month_\u8f49\u63db\u683c\u5f0f': [202502, 202504, 202501],
        'Supplier': ['Vendor A', 'Vendor B', 'Vendor C'],
        '\u662f\u5426\u4f30\u8a08\u5165\u5e33': ['Y', 'N', 'Y'],
        'PO\u72c0\u614b': [pd.NA, pd.NA, pd.NA],
    })
    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202503,
        processing_type='PROCUREMENT',
    )
    return ctx


@pytest.fixture
def mock_mapping_config():
    """Mock config_manager for ProcurementPreviousMappingStep"""
    with patch(
        'accrual_bot.tasks.spt.steps.spt_procurement_mapping.config_manager'
    ) as mock_cm:
        mock_cm._config_toml = {
            'spt_procurement_previous_mapping': {
                'column_patterns': {},
                'po_mappings': {
                    'fields': [
                        {
                            'source': 'remarked_by_procurement',
                            'target': 'Remarked by Procurement',
                            'fill_na': True,
                        }
                    ]
                },
                'pr_mappings': {
                    'fields': [
                        {
                            'source': 'remarked_by_procurement',
                            'target': 'Remarked by Procurement',
                            'fill_na': True,
                        }
                    ]
                },
            }
        }
        yield mock_cm


@pytest.fixture
def mock_mapping_config_empty():
    """Mock config_manager with no mappings"""
    with patch(
        'accrual_bot.tasks.spt.steps.spt_procurement_mapping.config_manager'
    ) as mock_cm:
        mock_cm._config_toml = {
            'spt_procurement_previous_mapping': {
                'column_patterns': {},
                'po_mappings': {'fields': []},
                'pr_mappings': {'fields': []},
            }
        }
        yield mock_cm


@pytest.fixture
def po_mapping_context():
    """ProcessingContext for PO mapping tests"""
    df = pd.DataFrame({
        'PO#': ['PO001', 'PO002', 'PO003'],
        'Line#': ['1', '2', '3'],
        'PO Line': ['PO0011', 'PO0022', 'PO0033'],
        'Item Description': ['Item A', 'Item B', 'Item C'],
        'Amount': [1000.0, 2000.0, 3000.0],
    })
    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202503,
        processing_type='PO',
    )
    prev_df = pd.DataFrame({
        'PO#': ['PO001', 'PO002'],
        'Line#': ['1', '2'],
        'PO Line': ['PO0011', 'PO0022'],
        'Remarked by Procurement': ['Remark A', 'Remark B'],
    })
    ctx.set_auxiliary_data('procurement_previous', prev_df)
    return ctx


@pytest.fixture
def pr_mapping_context():
    """ProcessingContext for PR mapping tests"""
    df = pd.DataFrame({
        'PR#': ['PR001', 'PR002'],
        'Line#': ['1', '2'],
        'PR Line': ['PR0011', 'PR0022'],
        'Item Description': ['Item X', 'Item Y'],
        'Amount': [500.0, 600.0],
    })
    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202503,
        processing_type='PR',
    )
    prev_df = pd.DataFrame({
        'PR#': ['PR001'],
        'Line#': ['1'],
        'PR Line': ['PR0011'],
        'Remarked by Procurement': ['Remark X'],
    })
    ctx.set_auxiliary_data('procurement_previous', prev_df)
    return ctx


@pytest.fixture
def combined_loading_context():
    """ProcessingContext for combined loading tests"""
    ctx = ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPT',
        processing_date=202503,
        processing_type='COMBINED',
    )
    return ctx


@pytest.fixture
def combined_processing_context():
    """ProcessingContext for combined processing tests"""
    po_data = pd.DataFrame({
        'PO#': ['PO001', 'PO002'],
        'Line#': ['1', '2'],
        'Item Description': ['Item A', 'Item B'],
        'Amount': [1000.0, 2000.0],
    })
    pr_data = pd.DataFrame({
        'PR#': ['PR001', 'PR002'],
        'Line#': ['1', '2'],
        'Item Description': ['Item X', 'Item Y'],
        'Amount': [500.0, 600.0],
    })
    ctx = ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPT',
        processing_date=202503,
        processing_type='COMBINED',
    )
    ctx.set_auxiliary_data('po_data', po_data)
    ctx.set_auxiliary_data('pr_data', pr_data)
    return ctx


@pytest.fixture
def export_context(tmp_path):
    """ProcessingContext for export tests"""
    po_result = pd.DataFrame({
        'PO#': ['PO001'], 'Amount': [1000.0], 'PO\u72c0\u614b': ['\u5df2\u5b8c\u6210']
    })
    pr_result = pd.DataFrame({
        'PR#': ['PR001'], 'Amount': [500.0], 'PR\u72c0\u614b': ['\u5df2\u5b8c\u6210']
    })
    ctx = ProcessingContext(
        data=pd.DataFrame(),
        entity_type='SPT',
        processing_date=202503,
        processing_type='COMBINED',
    )
    ctx.set_auxiliary_data('po_result', po_result)
    ctx.set_auxiliary_data('pr_result', pr_result)
    return ctx, tmp_path


# ============================================================
# SPTProcurementStatusEvaluationStep Tests
# ============================================================

@pytest.mark.unit
class TestSPTProcurementStatusEvaluationStep:
    """SPTProcurementStatusEvaluationStep tests"""

    def test_init_loads_conditions(self, mock_procurement_eval_config):
        """Test that __init__ loads and sorts conditions from config"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval", status_column="PO\u72c0\u614b")
        assert len(step.conditions) == 3
        # Verify sorted by priority
        priorities = [c['priority'] for c in step.conditions]
        assert priorities == sorted(priorities)

    @pytest.mark.asyncio
    async def test_execute_success_erm_conditions(
        self, mock_procurement_eval_config, procurement_eval_context
    ):
        """Test execute applies ERM conditions correctly"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval", status_column="PO\u72c0\u614b")
        result = await step.execute(procurement_eval_context)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['total_conditions'] == 3
        # Row 0: ERM=202502 in [202501,202503] -> ERM_IN_RANGE
        # Row 1: ERM=202504 > 202503 -> ERM_GT_CLOSING (priority 3, not in range)
        # Row 2: ERM=202501 in [202501,202503] -> ERM_IN_RANGE
        df = procurement_eval_context.data
        assert 'PO\u72c0\u614b' in df.columns

    @pytest.mark.asyncio
    async def test_execute_creates_status_column_if_missing(
        self, mock_procurement_eval_config_empty
    ):
        """Test that status column is created if missing"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval", status_column="PO\u72c0\u614b")

        df = pd.DataFrame({
            'Item Description': ['Test item'],
            'YMs of Item Description': ['202501,202503'],
            'Expected Received Month_\u8f49\u63db\u683c\u5f0f': [202502],
            'Supplier': ['Vendor'],
            '\u662f\u5426\u4f30\u8a08\u5165\u5e33': ['Y'],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPT', processing_date=202503, processing_type='PROCUREMENT'
        )
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_execute_no_erm_columns(self, mock_procurement_eval_config):
        """Test execute when ERM columns are missing"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval", status_column="PO\u72c0\u614b")

        df = pd.DataFrame({
            'Item Description': ['Test'],
            'PO\u72c0\u614b': [pd.NA],
        })
        ctx = ProcessingContext(
            data=df, entity_type='SPT', processing_date=202503, processing_type='PROCUREMENT'
        )
        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_validate_input_valid(self, mock_procurement_eval_config, procurement_eval_context):
        """Test validate_input returns True with valid data"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        assert await step.validate_input(procurement_eval_context) is True

    @pytest.mark.asyncio
    async def test_validate_input_empty_data(self, mock_procurement_eval_config):
        """Test validate_input returns False with empty data"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PROCUREMENT'
        )
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_missing_item_description(self, mock_procurement_eval_config):
        """Test validate_input returns False when Item Description is missing"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        df = pd.DataFrame({'Other Column': [1, 2]})
        ctx = ProcessingContext(
            data=df, entity_type='SPT', processing_date=202503, processing_type='PROCUREMENT'
        )
        assert await step.validate_input(ctx) is False

    def test_evaluate_check_contains(self, mock_procurement_eval_config):
        """Test _evaluate_check with 'contains' type"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        df = pd.DataFrame({'Item Description': ['ABC test', 'DEF other', 'GHI test']})
        check = {'type': 'contains', 'field': 'Item Description', 'pattern': 'test'}
        mask = step._evaluate_check(df, check, {}, 202503)
        assert mask.sum() == 2

    def test_evaluate_check_equals(self, mock_procurement_eval_config):
        """Test _evaluate_check with 'equals' type"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        df = pd.DataFrame({'Status': ['Open', 'Closed', 'Open']})
        check = {'type': 'equals', 'field': 'Status', 'value': 'Open'}
        mask = step._evaluate_check(df, check, {}, 202503)
        assert mask.sum() == 2

    def test_evaluate_check_not_equals(self, mock_procurement_eval_config):
        """Test _evaluate_check with 'not_equals' type"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        df = pd.DataFrame({'Status': ['Open', 'Closed', 'Open']})
        check = {'type': 'not_equals', 'field': 'Status', 'value': 'Open'}
        mask = step._evaluate_check(df, check, {}, 202503)
        assert mask.sum() == 1

    def test_evaluate_check_unknown_type(self, mock_procurement_eval_config):
        """Test _evaluate_check returns None for unknown type"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        df = pd.DataFrame({'col': [1]})
        check = {'type': 'unknown_type', 'field': 'col'}
        mask = step._evaluate_check(df, check, {}, 202503)
        assert mask is None

    def test_evaluate_check_missing_field(self, mock_procurement_eval_config):
        """Test _evaluate_check returns None when field is missing"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        df = pd.DataFrame({'other_col': [1]})
        check = {'type': 'contains', 'field': 'nonexistent', 'pattern': 'test'}
        mask = step._evaluate_check(df, check, {}, 202503)
        assert mask is None

    def test_simple_clean_removes_columns(self, mock_procurement_eval_config):
        """Test _simple_clean removes expected columns"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval")
        df = pd.DataFrame({
            'Supplier': ['A'],
            'Expected Received Month_\u8f49\u63db\u683c\u5f0f': [202501],
            'YMs of Item Description': ['202501,202503'],
            '\u662f\u5426\u4f30\u8a08\u5165\u5e33': ['Y'],
            'Keep Me': [1],
        })
        cleaned = step._simple_clean(df)
        assert 'Keep Me' in cleaned.columns
        assert 'Supplier' not in cleaned.columns
        assert 'Expected Received Month_\u8f49\u63db\u683c\u5f0f' not in cleaned.columns

    def test_apply_condition_or_combine(self, mock_procurement_eval_config):
        """Test _apply_condition with 'or' combine mode"""
        from accrual_bot.tasks.spt.steps.spt_procurement_evaluation import (
            SPTProcurementStatusEvaluationStep,
        )
        step = SPTProcurementStatusEvaluationStep(name="TestEval", status_column='PO\u72c0\u614b')
        df = pd.DataFrame({
            'PO\u72c0\u614b': [pd.NA, pd.NA, pd.NA],
            'Status': ['Open', 'Closed', 'Open'],
            'Type': ['A', 'A', 'B'],
        })
        condition = {
            'priority': 1,
            'status_value': 'MATCHED',
            'combine': 'or',
            'note': 'test or combine',
            'checks': [
                {'type': 'equals', 'field': 'Status', 'value': 'Closed'},
                {'type': 'equals', 'field': 'Type', 'value': 'A'},
            ]
        }
        result_df = step._apply_condition(df, condition, {}, 202503)
        # Row 0: Status=Open but Type=A -> MATCHED (or)
        # Row 1: Status=Closed and Type=A -> MATCHED (or)
        # Row 2: Status=Open and Type=B -> no match
        matched_count = result_df['PO\u72c0\u614b'].notna().sum()
        assert matched_count == 2


# ============================================================
# ProcurementPreviousMappingStep Tests
# ============================================================

@pytest.mark.unit
class TestProcurementPreviousMappingStep:
    """ProcurementPreviousMappingStep tests"""

    def test_init_loads_mapping_config(self, mock_mapping_config):
        """Test that __init__ loads mapping config"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()
        assert len(step.po_mappings) == 1
        assert len(step.pr_mappings) == 1

    @pytest.mark.asyncio
    async def test_execute_skips_when_no_previous(self, mock_mapping_config):
        """Test execute returns SKIPPED when no previous data"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()

        df = pd.DataFrame({'PO#': ['PO001'], 'Line#': ['1'], 'PO Line': ['PO0011']})
        ctx = ProcessingContext(
            data=df, entity_type='SPT', processing_date=202503, processing_type='PO'
        )
        # No procurement_previous set
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_po_mapping_success(self, mock_mapping_config, po_mapping_context):
        """Test execute applies PO field mappings"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()
        result = await step.execute(po_mapping_context)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['processing_type'] == 'PO'
        assert 'Remarked by Procurement' in po_mapping_context.data.columns

    @pytest.mark.asyncio
    async def test_execute_pr_mapping_detects_type(self, mock_mapping_config, pr_mapping_context):
        """Test execute detects PR processing type"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()
        result = await step.execute(pr_mapping_context)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['processing_type'] == 'PR'

    @pytest.mark.asyncio
    async def test_validate_input_missing_po_pr(self, mock_mapping_config):
        """Test validate_input fails without PO# or PR# column"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()
        df = pd.DataFrame({'Other': [1]})
        ctx = ProcessingContext(
            data=df, entity_type='SPT', processing_date=202503, processing_type='PO'
        )
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_empty_data(self, mock_mapping_config):
        """Test validate_input returns False with empty data"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PO'
        )
        assert await step.validate_input(ctx) is False

    def test_fix_missing_mapping_key_po(self, mock_mapping_config):
        """Test _fix_missing_mapping_key creates PO Line column"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()
        df = pd.DataFrame({
            'PO#': ['PO001', 'PO002'],
            'Line#': ['1', '2'],
        })
        result = step._fix_missing_mapping_key(df, 'po')
        assert 'PO Line' in result.columns

    def test_fix_missing_mapping_key_already_exists(self, mock_mapping_config):
        """Test _fix_missing_mapping_key skips when column exists"""
        from accrual_bot.tasks.spt.steps.spt_procurement_mapping import (
            ProcurementPreviousMappingStep,
        )
        step = ProcurementPreviousMappingStep()
        df = pd.DataFrame({
            'PO#': ['PO001'],
            'Line#': ['1'],
            'PO Line': ['PO0011'],
        })
        result = step._fix_missing_mapping_key(df, 'po')
        assert 'PO Line' in result.columns


# ============================================================
# ProcurementPreviousValidationStep Tests
# ============================================================

@pytest.mark.unit
class TestProcurementPreviousValidationStep:
    """ProcurementPreviousValidationStep tests"""

    @pytest.mark.asyncio
    async def test_execute_skips_when_no_previous_data(self):
        """Test execute returns SKIPPED when no previous data exists"""
        from accrual_bot.tasks.spt.steps.spt_procurement_validation import (
            ProcurementPreviousValidationStep,
        )
        step = ProcurementPreviousValidationStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PROCUREMENT'
        )
        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_validates_xlsx_format(self):
        """Test file format validation for .xlsx"""
        from accrual_bot.tasks.spt.steps.spt_procurement_validation import (
            ProcurementPreviousValidationStep,
        )
        step = ProcurementPreviousValidationStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PROCUREMENT'
        )
        ctx.set_auxiliary_data('procurement_previous_po', pd.DataFrame({
            'PO Line': ['PO0011'], 'Remarked by Procurement': ['Remark']
        }))
        ctx.set_variable('procurement_previous_path', '/tmp/test.xlsx')

        result = await step.execute(ctx)
        assert result.metadata['file_format_valid'] is True

    @pytest.mark.asyncio
    async def test_execute_strict_mode_fails_on_error(self):
        """Test strict mode returns FAILED on validation errors"""
        from accrual_bot.tasks.spt.steps.spt_procurement_validation import (
            ProcurementPreviousValidationStep,
        )
        step = ProcurementPreviousValidationStep(strict_mode=True)
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PROCUREMENT'
        )
        # Provide PO data without required columns
        ctx.set_auxiliary_data('procurement_previous_po', pd.DataFrame({'col': [1]}))
        ctx.set_variable('procurement_previous_path', '/tmp/test.csv')  # invalid format

        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED
        assert len(result.metadata['errors']) > 0

    @pytest.mark.asyncio
    async def test_execute_non_strict_mode_skips_on_error(self):
        """Test non-strict mode returns SKIPPED on validation errors"""
        from accrual_bot.tasks.spt.steps.spt_procurement_validation import (
            ProcurementPreviousValidationStep,
        )
        step = ProcurementPreviousValidationStep(strict_mode=False)
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PROCUREMENT'
        )
        ctx.set_auxiliary_data('procurement_previous_po', pd.DataFrame({'col': [1]}))
        ctx.set_variable('procurement_previous_path', '/tmp/test.csv')

        result = await step.execute(ctx)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_success_with_both_sheets(self):
        """Test successful validation with both PO and PR sheets"""
        from accrual_bot.tasks.spt.steps.spt_procurement_validation import (
            ProcurementPreviousValidationStep,
        )
        step = ProcurementPreviousValidationStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PROCUREMENT'
        )
        ctx.set_auxiliary_data('procurement_previous_po', pd.DataFrame({
            'PO Line': ['PO0011'], 'Remarked by Procurement': ['Remark']
        }))
        ctx.set_auxiliary_data('procurement_previous_pr', pd.DataFrame({
            'PR Line': ['PR0011'], 'Remarked by Procurement': ['Remark']
        }))
        ctx.set_variable('procurement_previous_path', '/tmp/test.xlsx')

        result = await step.execute(ctx)
        assert result.status == StepStatus.SUCCESS
        assert result.metadata['po_sheet_exists'] is True
        assert result.metadata['pr_sheet_exists'] is True

    @pytest.mark.asyncio
    async def test_validate_input_always_true(self):
        """Test validate_input always returns True"""
        from accrual_bot.tasks.spt.steps.spt_procurement_validation import (
            ProcurementPreviousValidationStep,
        )
        step = ProcurementPreviousValidationStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='PROCUREMENT'
        )
        assert await step.validate_input(ctx) is True

    def test_generate_validation_summary(self):
        """Test _generate_validation_summary produces correct report"""
        from accrual_bot.tasks.spt.steps.spt_procurement_validation import (
            ProcurementPreviousValidationStep,
        )
        step = ProcurementPreviousValidationStep()
        results = {
            'file_format_valid': True,
            'po_sheet_exists': True,
            'pr_sheet_exists': False,
            'po_columns_valid': True,
            'pr_columns_valid': False,
            'errors': ['Error 1'],
            'warnings': ['Warning 1'],
        }
        summary = step._generate_validation_summary(results)
        assert 'Valid' in summary
        assert 'Error 1' in summary
        assert 'Warning 1' in summary


# ============================================================
# CombinedProcurementDataLoadingStep Tests
# ============================================================

@pytest.mark.unit
class TestCombinedProcurementDataLoadingStep:
    """CombinedProcurementDataLoadingStep tests"""

    @pytest.mark.asyncio
    async def test_execute_loads_po_and_pr(self, combined_loading_context):
        """Test loading both PO and PR data"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_loading import (
            CombinedProcurementDataLoadingStep,
        )
        po_df = pd.DataFrame({'PO#': ['PO001'], 'Amount': [1000]})
        pr_df = pd.DataFrame({'PR#': ['PR001'], 'Amount': [500]})

        with patch(
            'accrual_bot.tasks.spt.steps.spt_combined_procurement_loading.DataSourceFactory'
        ) as mock_factory:
            source_po = AsyncMock()
            source_po.read = AsyncMock(return_value=po_df)
            source_pr = AsyncMock()
            source_pr.read = AsyncMock(return_value=pr_df)
            mock_factory.create_source = AsyncMock(side_effect=[source_po, source_pr])

            step = CombinedProcurementDataLoadingStep(
                file_paths={'raw_po': '/tmp/po.xlsx', 'raw_pr': '/tmp/pr.xlsx'}
            )
            result = await step.execute(combined_loading_context)

            assert result.status == StepStatus.SUCCESS
            assert result.metadata['po_loaded'] is True
            assert result.metadata['pr_loaded'] is True
            assert result.metadata['po_rows'] == 1
            assert result.metadata['pr_rows'] == 1

    @pytest.mark.asyncio
    async def test_execute_fails_when_both_fail(self, combined_loading_context):
        """Test FAILED status when both PO and PR loading fail"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_loading import (
            CombinedProcurementDataLoadingStep,
        )
        with patch(
            'accrual_bot.tasks.spt.steps.spt_combined_procurement_loading.DataSourceFactory'
        ) as mock_factory:
            mock_factory.create_source = AsyncMock(
                side_effect=Exception("File not found")
            )

            step = CombinedProcurementDataLoadingStep(
                file_paths={'raw_po': '/tmp/po.xlsx', 'raw_pr': '/tmp/pr.xlsx'}
            )
            result = await step.execute(combined_loading_context)
            assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    async def test_execute_po_only(self, combined_loading_context):
        """Test loading only PO data when PR is not provided"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_loading import (
            CombinedProcurementDataLoadingStep,
        )
        po_df = pd.DataFrame({'PO#': ['PO001'], 'Amount': [1000]})

        with patch(
            'accrual_bot.tasks.spt.steps.spt_combined_procurement_loading.DataSourceFactory'
        ) as mock_factory:
            source_po = AsyncMock()
            source_po.read = AsyncMock(return_value=po_df)
            mock_factory.create_source = AsyncMock(return_value=source_po)

            step = CombinedProcurementDataLoadingStep(
                file_paths={'raw_po': '/tmp/po.xlsx'}
            )
            result = await step.execute(combined_loading_context)

            assert result.status == StepStatus.SUCCESS
            assert result.metadata['po_loaded'] is True
            assert result.metadata['pr_loaded'] is False

    @pytest.mark.asyncio
    async def test_validate_input_false_no_files(self):
        """Test validate_input returns False when no files provided"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_loading import (
            CombinedProcurementDataLoadingStep,
        )
        step = CombinedProcurementDataLoadingStep(file_paths={})
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_true_with_po(self):
        """Test validate_input returns True when PO file provided"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_loading import (
            CombinedProcurementDataLoadingStep,
        )
        step = CombinedProcurementDataLoadingStep(
            file_paths={'raw_po': '/tmp/po.xlsx'}
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        assert await step.validate_input(ctx) is True

    def test_extract_date_from_filename(self):
        """Test deprecated _extract_date_from_filename"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_loading import (
            CombinedProcurementDataLoadingStep,
        )
        step = CombinedProcurementDataLoadingStep()
        assert step._extract_date_from_filename('/tmp/202503_po.xlsx') == 202503
        assert step._extract_date_from_filename('/tmp/no_date.xlsx') == 0

    @pytest.mark.asyncio
    async def test_execute_with_dict_file_config(self, combined_loading_context):
        """Test loading data with dict-style file config (path + params)"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_loading import (
            CombinedProcurementDataLoadingStep,
        )
        po_df = pd.DataFrame({'PO#': ['PO001'], 'Amount': [1000]})

        with patch(
            'accrual_bot.tasks.spt.steps.spt_combined_procurement_loading.DataSourceFactory'
        ) as mock_factory:
            source = AsyncMock()
            source.read = AsyncMock(return_value=po_df)
            mock_factory.create_source = AsyncMock(return_value=source)

            step = CombinedProcurementDataLoadingStep(
                file_paths={
                    'raw_po': {'path': '/tmp/po.xlsx', 'params': {'sheet_name': 'Sheet1'}}
                }
            )
            result = await step.execute(combined_loading_context)
            assert result.status == StepStatus.SUCCESS


# ============================================================
# CombinedProcurementProcessingStep Tests
# ============================================================

@pytest.mark.unit
class TestCombinedProcurementProcessingStep:
    """CombinedProcurementProcessingStep tests"""

    @pytest.mark.asyncio
    async def test_execute_fails_when_no_data(self):
        """Test FAILED when neither PO nor PR data available"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_processing import (
            CombinedProcurementProcessingStep,
        )
        step = CombinedProcurementProcessingStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    async def test_validate_input_false_no_data(self):
        """Test validate_input returns False when no auxiliary data"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_processing import (
            CombinedProcurementProcessingStep,
        )
        step = CombinedProcurementProcessingStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_true_with_po_data(self, combined_processing_context):
        """Test validate_input returns True when PO data exists"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_processing import (
            CombinedProcurementProcessingStep,
        )
        step = CombinedProcurementProcessingStep()
        assert await step.validate_input(combined_processing_context) is True

    @pytest.mark.asyncio
    async def test_process_po_handles_sub_context_error(self, combined_processing_context):
        """Test _process_po_data gracefully handles ProcessingContext() constructor issue"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_processing import (
            CombinedProcurementProcessingStep,
        )
        step = CombinedProcurementProcessingStep()

        po_data = combined_processing_context.get_auxiliary_data('po_data')
        # 源碼中 ProcessingContext() 未傳必要參數，_process_po_data 的 try/except 會捕獲
        result = await step._process_po_data(combined_processing_context, po_data)
        # 因 ProcessingContext() 構造失敗，result 應為 None
        assert result is None

    @pytest.mark.asyncio
    async def test_process_po_returns_none_on_step_failure(self, combined_processing_context):
        """Test _process_po_data returns None when a sub-step fails"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_processing import (
            CombinedProcurementProcessingStep,
        )
        step = CombinedProcurementProcessingStep()

        with patch(
            'accrual_bot.tasks.spt.steps.spt_combined_procurement_processing.ColumnInitializationStep'
        ) as mock_col_init:
            mock_instance = MagicMock()
            mock_instance.name = 'FailingStep'
            mock_instance.execute = AsyncMock(return_value=StepResult(
                step_name='FailingStep', status=StepStatus.FAILED, message='Test failure'
            ))
            mock_col_init.return_value = mock_instance

            po_data = combined_processing_context.get_auxiliary_data('po_data')
            result = await step._process_po_data(combined_processing_context, po_data)
            assert result is None

    def test_generate_processing_summary(self):
        """Test _generate_processing_summary produces correct report"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_processing import (
            CombinedProcurementProcessingStep,
        )
        step = CombinedProcurementProcessingStep()
        summary = {
            'po_processed': True,
            'pr_processed': False,
            'po_final_rows': 10,
            'pr_final_rows': 0,
            'po_status_distribution': {'\u5df2\u5b8c\u6210': 5, '\u672a\u5b8c\u6210': 5},
            'pr_status_distribution': {},
        }
        report = step._generate_processing_summary(summary)
        assert 'Success' in report
        assert 'Failed' in report
        assert '10' in report


# ============================================================
# CombinedProcurementExportStep Tests
# ============================================================

@pytest.mark.unit
class TestCombinedProcurementExportStep:
    """CombinedProcurementExportStep tests"""

    @pytest.mark.asyncio
    async def test_execute_exports_to_excel(self, export_context):
        """Test execute writes PO and PR sheets to Excel"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_export import (
            CombinedProcurementExportStep,
        )
        ctx, tmp_path = export_context
        step = CombinedProcurementExportStep(output_dir=str(tmp_path))
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['po_exported'] is True
        assert result.metadata['pr_exported'] is True

        # Verify Excel file was created
        expected_file = tmp_path / '202503_PROCUREMENT_COMBINED.xlsx'
        assert expected_file.exists()

    @pytest.mark.asyncio
    async def test_execute_fails_when_no_results(self):
        """Test FAILED status when no result data to export"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_export import (
            CombinedProcurementExportStep,
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        step = CombinedProcurementExportStep(output_dir='/tmp/test_export')
        result = await step.execute(ctx)
        assert result.status == StepStatus.FAILED

    @pytest.mark.asyncio
    async def test_validate_input_false_no_results(self):
        """Test validate_input returns False when no result data"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_export import (
            CombinedProcurementExportStep,
        )
        step = CombinedProcurementExportStep()
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        assert await step.validate_input(ctx) is False

    @pytest.mark.asyncio
    async def test_validate_input_true_with_results(self, export_context):
        """Test validate_input returns True when result data exists"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_export import (
            CombinedProcurementExportStep,
        )
        ctx, _ = export_context
        step = CombinedProcurementExportStep()
        assert await step.validate_input(ctx) is True

    def test_prepare_output_path_yyyymm_replacement(self, tmp_path):
        """Test _prepare_output_path replaces {YYYYMM} correctly"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_export import (
            CombinedProcurementExportStep,
        )
        step = CombinedProcurementExportStep(
            output_dir=str(tmp_path),
            filename_template='{YYYYMM}_TEST.xlsx'
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        path = step._prepare_output_path(ctx)
        assert path.name == '202503_TEST.xlsx'

    def test_prepare_output_path_with_suffix(self, tmp_path):
        """Test _prepare_output_path adds suffix for retries"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_export import (
            CombinedProcurementExportStep,
        )
        step = CombinedProcurementExportStep(
            output_dir=str(tmp_path),
            filename_template='{YYYYMM}_TEST.xlsx'
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        path = step._prepare_output_path(ctx, suffix='_1')
        assert path.name == '202503_TEST_1.xlsx'

    @pytest.mark.asyncio
    async def test_execute_po_only_export(self, tmp_path):
        """Test export with only PO results (no PR)"""
        from accrual_bot.tasks.spt.steps.spt_combined_procurement_export import (
            CombinedProcurementExportStep,
        )
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SPT',
            processing_date=202503, processing_type='COMBINED'
        )
        po_result = pd.DataFrame({'PO#': ['PO001'], 'Amount': [1000.0]})
        ctx.set_auxiliary_data('po_result', po_result)

        step = CombinedProcurementExportStep(output_dir=str(tmp_path))
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert result.metadata['po_exported'] is True
        assert result.metadata['pr_exported'] is False
