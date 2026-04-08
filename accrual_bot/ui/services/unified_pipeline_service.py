"""
Unified Pipeline Service — 向後相容 re-export

實際實作已搬移至 accrual_bot.tasks.pipeline_service。
此模組保留以相容現有 import 路徑。
"""

from accrual_bot.tasks.pipeline_service import UnifiedPipelineService  # noqa: F401
