"""
Session State Data Models

定義所有 Streamlit session state 使用的資料結構。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum


class ExecutionStatus(Enum):
    """Pipeline 執行狀態"""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class PipelineConfig:
    """Pipeline 配置狀態"""
    entity: str = ""                    # SPT, SPX (MOB 隱藏)
    processing_type: str = ""           # PO, PR, PPE
    processing_date: int = 0            # YYYYMM 格式
    template_name: str = ""             # 選擇的範本名稱
    enabled_steps: List[str] = field(default_factory=list)  # 已啟用的步驟清單


@dataclass
class FileUploadState:
    """檔案上傳狀態"""
    uploaded_files: Dict[str, Any] = field(default_factory=dict)  # 上傳的 UploadedFile 物件
    file_paths: Dict[str, str] = field(default_factory=dict)      # 暫存檔案路徑
    validation_errors: List[str] = field(default_factory=list)    # 驗證錯誤訊息
    required_files_complete: bool = False                          # 必填檔案是否完整


@dataclass
class ExecutionState:
    """Pipeline 執行狀態"""
    status: ExecutionStatus = ExecutionStatus.IDLE
    current_step: str = ""                                         # 目前執行的步驟
    completed_steps: List[str] = field(default_factory=list)      # 已完成的步驟
    failed_steps: List[str] = field(default_factory=list)         # 失敗的步驟
    step_results: Dict[str, Any] = field(default_factory=dict)    # 各步驟的執行結果
    logs: List[str] = field(default_factory=list)                 # 執行日誌
    error_message: str = ""                                        # 錯誤訊息
    start_time: Optional[float] = None                             # 開始時間
    end_time: Optional[float] = None                               # 結束時間


@dataclass
class ResultState:
    """Pipeline 執行結果"""
    success: bool = False
    output_data: Optional[Any] = None                              # 主輸出數據 (DataFrame)
    auxiliary_data: Dict[str, Any] = field(default_factory=dict)  # 輔助數據
    statistics: Dict[str, Any] = field(default_factory=dict)      # 統計資訊
    execution_time: float = 0.0                                    # 執行時間 (秒)
    checkpoint_path: Optional[str] = None                          # Checkpoint 儲存路徑
