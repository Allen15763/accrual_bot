"""
Tasks Module - Entity-specific task implementations

Each entity (SPT, SPX, SCT) has its own task folder containing:
- steps/: Entity-specific pipeline steps
- pipeline_orchestrator.py: Pipeline configuration for the entity

公開介面：
- UnifiedPipelineService: 統一的 pipeline 建構與查詢服務
"""

__all__ = ["UnifiedPipelineService"]


def __getattr__(name):
    if name == "UnifiedPipelineService":
        from accrual_bot.tasks.pipeline_service import UnifiedPipelineService
        return UnifiedPipelineService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
