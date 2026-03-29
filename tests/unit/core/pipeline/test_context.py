"""Comprehensive unit tests for ProcessingContext, ValidationResult, and ContextMetadata."""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch

from accrual_bot.core.pipeline.context import (
    ProcessingContext,
    ValidationResult,
    ContextMetadata,
)


@pytest.fixture
def sample_df():
    return pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})


@pytest.fixture
def context(sample_df):
    with patch("accrual_bot.core.pipeline.context.get_logger"):
        return ProcessingContext(
            data=sample_df,
            entity_type="SPX",
            processing_date=202512,
            processing_type="PO",
        )


@pytest.mark.unit
class TestValidationResult:
    def test_initial_valid_state(self):
        result = ValidationResult(is_valid=True)
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_add_error_sets_invalid(self):
        result = ValidationResult(is_valid=True)
        result.add_error("something broke")
        assert result.is_valid is False
        assert result.errors == ["something broke"]

    def test_add_multiple_errors(self):
        result = ValidationResult(is_valid=True)
        result.add_error("error 1")
        result.add_error("error 2")
        assert len(result.errors) == 2
        assert result.is_valid is False

    def test_add_warning_keeps_valid(self):
        result = ValidationResult(is_valid=True)
        result.add_warning("minor issue")
        assert result.is_valid is True
        assert result.warnings == ["minor issue"]

    def test_initially_invalid(self):
        result = ValidationResult(is_valid=False)
        assert result.is_valid is False


@pytest.mark.unit
class TestContextMetadata:
    def test_creation(self):
        meta = ContextMetadata(
            entity_type="SPT", processing_date=202601, processing_type="PR"
        )
        assert meta.entity_type == "SPT"
        assert meta.processing_date == 202601
        assert meta.processing_type == "PR"
        assert isinstance(meta.created_at, datetime)
        assert isinstance(meta.updated_at, datetime)

    def test_update_changes_updated_at(self):
        meta = ContextMetadata(
            entity_type="SPX", processing_date=202512, processing_type="PO"
        )
        old_updated = meta.updated_at
        meta.update()
        assert meta.updated_at >= old_updated


@pytest.mark.unit
class TestProcessingContext:
    def test_init(self, context, sample_df):
        assert context.metadata.entity_type == "SPX"
        assert context.metadata.processing_date == 202512
        assert context.metadata.processing_type == "PO"
        pd.testing.assert_frame_equal(context.data, sample_df)

    def test_default_processing_type_is_po(self, sample_df):
        with patch("accrual_bot.core.pipeline.context.get_logger"):
            ctx = ProcessingContext(
                data=sample_df, entity_type="SPT", processing_date=202601
            )
        assert ctx.metadata.processing_type == "PO"

    def test_update_data(self, context):
        new_df = pd.DataFrame({"C": [10, 20]})
        context.update_data(new_df)
        pd.testing.assert_frame_equal(context.data, new_df)

    def test_get_data_copy_returns_independent_copy(self, context):
        copy = context.get_data_copy()
        copy["A"] = [99, 99, 99]
        assert list(context.data["A"]) == [1, 2, 3]

    # --- Auxiliary data ---
    def test_auxiliary_data_crud(self, context):
        assert context.has_auxiliary_data("ref") is False
        context.add_auxiliary_data("ref", pd.DataFrame({"x": [1]}))
        assert context.has_auxiliary_data("ref") is True
        assert isinstance(context.get_auxiliary_data("ref"), pd.DataFrame)
        assert context.list_auxiliary_data() == ["ref"]

    def test_get_auxiliary_data_missing_returns_none(self, context):
        assert context.get_auxiliary_data("nonexistent") is None

    def test_set_auxiliary_data_alias(self, context):
        context.set_auxiliary_data("key", "value")
        assert context.get_auxiliary_data("key") == "value"

    def test_auxiliary_data_property_returns_copy(self, context):
        context.add_auxiliary_data("k", "v")
        prop = context.auxiliary_data
        prop["new"] = "added"
        assert "new" not in context._auxiliary_data

    # --- Variables ---
    def test_variable_crud(self, context):
        assert context.has_variable("flag") is False
        context.set_variable("flag", True)
        assert context.has_variable("flag") is True
        assert context.get_variable("flag") is True

    def test_get_variable_default(self, context):
        assert context.get_variable("missing", "fallback") == "fallback"
        assert context.get_variable("missing") is None

    # --- Errors and warnings ---
    def test_errors_and_warnings(self, context):
        assert context.has_errors() is False
        assert context.has_warnings() is False
        context.add_error("err")
        context.add_warning("warn")
        assert context.has_errors() is True
        assert context.has_warnings() is True
        context.clear_errors()
        assert context.has_errors() is False
        assert context.has_warnings() is True
        context.clear_warnings()
        assert context.has_warnings() is False

    # --- Validations ---
    def test_add_validation_propagates_errors_and_warnings(self, context):
        vr = ValidationResult(is_valid=False)
        vr.add_error("bad data")
        vr.add_warning("check this")
        context.add_validation("step1", vr)
        assert "[step1] bad data" in context.errors
        assert "[step1] check this" in context.warnings
        assert context.get_validation("step1") is vr

    def test_is_valid_all_pass(self, context):
        context.add_validation("v1", ValidationResult(is_valid=True))
        context.add_validation("v2", ValidationResult(is_valid=True))
        assert context.is_valid() is True

    def test_is_valid_one_fails(self, context):
        context.add_validation("v1", ValidationResult(is_valid=True))
        bad = ValidationResult(is_valid=True)
        bad.add_error("fail")
        context.add_validation("v2", bad)
        assert context.is_valid() is False

    def test_is_valid_empty_validations(self, context):
        assert context.is_valid() is True

    # --- History ---
    def test_history(self, context):
        assert context.get_last_step() is None
        context.add_history("LoadStep", "SUCCESS", rows=100)
        history = context.get_history()
        assert len(history) == 1
        assert history[0]["step"] == "LoadStep"
        assert history[0]["status"] == "SUCCESS"
        assert history[0]["rows"] == 100
        assert context.get_last_step()["step"] == "LoadStep"

    def test_get_history_returns_copy(self, context):
        context.add_history("s1", "OK")
        h = context.get_history()
        h.append({"step": "fake"})
        assert len(context.get_history()) == 1

    # --- Processing type helpers ---
    def test_po_processing_helpers(self, context):
        assert context.is_po_processing() is True
        assert context.is_pr_processing() is False
        assert context.get_status_column() == "PO狀態"
        assert context.get_id_column() == "PO#"

    def test_pr_processing_helpers(self, sample_df):
        with patch("accrual_bot.core.pipeline.context.get_logger"):
            ctx = ProcessingContext(
                data=sample_df,
                entity_type="SPX",
                processing_date=202512,
                processing_type="PR",
            )
        assert ctx.is_pr_processing() is True
        assert ctx.is_po_processing() is False
        assert ctx.get_status_column() == "PR狀態"
        assert ctx.get_id_column() == "PR#"

    # --- Entity config ---
    def test_get_entity_config_known(self, context):
        config = context.get_entity_config()
        assert "fa_accounts" in config

    def test_get_entity_config_unknown(self, sample_df):
        with patch("accrual_bot.core.pipeline.context.get_logger"):
            ctx = ProcessingContext(
                data=sample_df,
                entity_type="UNKNOWN",
                processing_date=202512,
            )
        assert ctx.get_entity_config() == {}

    # --- Serialization ---
    def test_to_dict(self, context):
        context.set_variable("v", 1)
        context.add_auxiliary_data("aux", "data")
        context.add_error("e")
        context.add_warning("w")
        context.add_history("step", "OK")
        d = context.to_dict()
        assert d["entity_type"] == "SPX"
        assert d["processing_date"] == 202512
        assert d["processing_type"] == "PO"
        assert d["data_shape"] == (3, 2)
        assert d["auxiliary_data"] == ["aux"]
        assert d["variables"] == ["v"]
        assert d["errors"] == 1
        assert d["warnings"] == 1
        assert d["history_steps"] == 1

    def test_repr(self, context):
        r = repr(context)
        assert "SPX" in r
        assert "202512" in r
        assert "PO" in r
