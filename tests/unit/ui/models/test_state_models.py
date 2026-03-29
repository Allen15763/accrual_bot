"""
Tests for accrual_bot.ui.models.state_models

驗證所有 session state 資料結構的正確性。
"""

import pytest
from accrual_bot.ui.models.state_models import (
    ExecutionStatus,
    PipelineConfig,
    FileUploadState,
    ExecutionState,
    ResultState,
)


@pytest.mark.unit
class TestExecutionStatus:
    """ExecutionStatus 列舉測試"""

    def test_enum_values(self):
        """驗證所有列舉值"""
        assert ExecutionStatus.IDLE.value == "idle"
        assert ExecutionStatus.RUNNING.value == "running"
        assert ExecutionStatus.COMPLETED.value == "completed"
        assert ExecutionStatus.FAILED.value == "failed"
        assert ExecutionStatus.PAUSED.value == "paused"

    def test_enum_member_count(self):
        """驗證列舉成員數量"""
        assert len(ExecutionStatus) == 5


@pytest.mark.unit
class TestPipelineConfig:
    """PipelineConfig 資料類別測試"""

    def test_default_values(self):
        """驗證預設值"""
        config = PipelineConfig()
        assert config.entity == ""
        assert config.processing_type == ""
        assert config.processing_date == 0
        assert config.enabled_steps == []
        assert config.procurement_source_type == ""

    def test_custom_values(self):
        """驗證自訂值"""
        config = PipelineConfig(
            entity="SPX",
            processing_type="PO",
            processing_date=202512,
            enabled_steps=["SPXDataLoading", "ColumnAddition"],
            procurement_source_type="COMBINED",
        )
        assert config.entity == "SPX"
        assert config.processing_type == "PO"
        assert config.processing_date == 202512
        assert config.enabled_steps == ["SPXDataLoading", "ColumnAddition"]
        assert config.procurement_source_type == "COMBINED"

    def test_enabled_steps_field_independence(self):
        """驗證 default_factory 建立獨立實例"""
        config_a = PipelineConfig()
        config_b = PipelineConfig()
        config_a.enabled_steps.append("StepA")
        assert config_b.enabled_steps == []


@pytest.mark.unit
class TestFileUploadState:
    """FileUploadState 資料類別測試"""

    def test_default_values(self):
        """驗證預設值"""
        state = FileUploadState()
        assert state.uploaded_files == {}
        assert state.file_paths == {}
        assert state.validation_errors == []
        assert state.required_files_complete is False

    def test_custom_values(self):
        """驗證自訂值"""
        state = FileUploadState(
            uploaded_files={"raw_po": "file_obj"},
            file_paths={"raw_po": "/tmp/raw_po.csv"},
            validation_errors=["缺少必填檔案"],
            required_files_complete=True,
        )
        assert state.uploaded_files == {"raw_po": "file_obj"}
        assert state.file_paths == {"raw_po": "/tmp/raw_po.csv"}
        assert state.validation_errors == ["缺少必填檔案"]
        assert state.required_files_complete is True

    def test_field_independence(self):
        """驗證 default_factory 建立獨立實例"""
        state_a = FileUploadState()
        state_b = FileUploadState()
        state_a.file_paths["key"] = "value"
        state_a.validation_errors.append("err")
        assert state_b.file_paths == {}
        assert state_b.validation_errors == []


@pytest.mark.unit
class TestExecutionState:
    """ExecutionState 資料類別測試"""

    def test_default_values(self):
        """驗證預設值"""
        state = ExecutionState()
        assert state.status == ExecutionStatus.IDLE
        assert state.current_step == ""
        assert state.completed_steps == []
        assert state.failed_steps == []
        assert state.step_results == {}
        assert state.logs == []
        assert state.error_message == ""
        assert state.start_time is None
        assert state.end_time is None

    def test_default_status_is_idle(self):
        """驗證預設狀態為 IDLE"""
        state = ExecutionState()
        assert state.status is ExecutionStatus.IDLE
        assert state.status.value == "idle"

    def test_custom_values(self):
        """驗證自訂值"""
        state = ExecutionState(
            status=ExecutionStatus.COMPLETED,
            current_step="SPXExport",
            completed_steps=["SPXDataLoading", "ColumnAddition"],
            failed_steps=["SPXExport"],
            step_results={"SPXDataLoading": {"rows": 100}},
            logs=["開始執行", "完成"],
            error_message="匯出失敗",
            start_time=1000.0,
            end_time=2000.0,
        )
        assert state.status == ExecutionStatus.COMPLETED
        assert state.current_step == "SPXExport"
        assert len(state.completed_steps) == 2
        assert state.failed_steps == ["SPXExport"]
        assert state.step_results["SPXDataLoading"]["rows"] == 100
        assert len(state.logs) == 2
        assert state.error_message == "匯出失敗"
        assert state.start_time == 1000.0
        assert state.end_time == 2000.0

    def test_field_independence(self):
        """驗證 default_factory 建立獨立實例"""
        state_a = ExecutionState()
        state_b = ExecutionState()
        state_a.completed_steps.append("Step1")
        state_a.logs.append("log1")
        state_a.step_results["key"] = "val"
        assert state_b.completed_steps == []
        assert state_b.logs == []
        assert state_b.step_results == {}


@pytest.mark.unit
class TestResultState:
    """ResultState 資料類別測試"""

    def test_default_values(self):
        """驗證預設值"""
        state = ResultState()
        assert state.success is False
        assert state.output_data is None
        assert state.auxiliary_data == {}
        assert state.statistics == {}
        assert state.execution_time == 0.0
        assert state.checkpoint_path is None

    def test_custom_values(self):
        """驗證自訂值"""
        import pandas as pd

        df = pd.DataFrame({"col": [1, 2, 3]})
        state = ResultState(
            success=True,
            output_data=df,
            auxiliary_data={"ref": "data"},
            statistics={"total_rows": 100},
            execution_time=12.5,
            checkpoint_path="/tmp/checkpoint.pkl",
        )
        assert state.success is True
        assert len(state.output_data) == 3
        assert state.auxiliary_data == {"ref": "data"}
        assert state.statistics["total_rows"] == 100
        assert state.execution_time == 12.5
        assert state.checkpoint_path == "/tmp/checkpoint.pkl"

    def test_field_independence(self):
        """驗證 default_factory 建立獨立實例"""
        state_a = ResultState()
        state_b = ResultState()
        state_a.auxiliary_data["key"] = "val"
        state_a.statistics["k"] = 1
        assert state_b.auxiliary_data == {}
        assert state_b.statistics == {}
