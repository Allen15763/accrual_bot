"""
SPX 配置驅動條件引擎（向後相容包裝）

核心邏輯已提取至 accrual_bot.core.pipeline.engines.condition_engine.ConditionEngine。
本模組保留 SPXConditionEngine 類別供既有程式碼使用。
"""

from accrual_bot.core.pipeline.engines.condition_engine import ConditionEngine  # noqa: F401


class SPXConditionEngine(ConditionEngine):
    """SPX 配置驅動的條件引擎（向後相容別名）

    繼承自 ConditionEngine，固定 entity_type='SPX'。

    Usage:
        engine = SPXConditionEngine('spx_erm_status_rules')
        df, stats = engine.apply_rules(df, 'PO狀態', context)
    """

    def __init__(self, config_section: str):
        super().__init__(config_section, entity_type='SPX')
