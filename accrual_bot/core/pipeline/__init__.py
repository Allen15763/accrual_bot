"""
Pipeline 模組
提供靈活的數據處理Pipeline架構
"""

# 基礎組件
from .base import (
    PipelineStep,
    StepResult,
    StepStatus,
    ConditionalStep,
    ParallelStep,
    SequentialStep
)

# 上下文管理
from .context import (
    ProcessingContext,
    ValidationResult,
    ContextMetadata
)

# Pipeline主類
from .pipeline import (
    Pipeline,
    PipelineBuilder,
    PipelineConfig,
    PipelineExecutor
)

# 工廠和配置
from .factory import (
    PipelineFactory,
    StepRegistry
)

from .config_manager import (
    PipelineConfigManager,
    EntityConfig,
    ProcessingMode
)

# 實體策略
from .entity_strategies import (
    EntityStrategy,
    MOBStrategy,
    SPTStrategy,
    SPXStrategy,
    EntityStrategyFactory,
    AdaptivePipelineManager
)

# 模板
from .templates import (
    PipelineTemplate,
    StandardPOTemplate,
    FullPOWithIntegrationTemplate,
    SimplePRTemplate,
    SPXSpecialTemplate,
    DataQualityCheckTemplate,
    PipelineTemplateManager
)

# 步驟
from .steps import *

__all__ = [
    # Base
    'PipelineStep',
    'StepResult',
    'StepStatus',
    'ConditionalStep',
    'ParallelStep',
    'SequentialStep',
    
    # Context
    'ProcessingContext',
    'ValidationResult',
    'ContextMetadata',
    
    # Pipeline
    'Pipeline',
    'PipelineBuilder',
    'PipelineConfig',
    'PipelineExecutor',
    
    # Factory
    'PipelineFactory',
    'StepRegistry',
    
    # Config
    'PipelineConfigManager',
    'EntityConfig',
    'ProcessingMode',
    
    # Strategies
    'EntityStrategy',
    'MOBStrategy',
    'SPTStrategy',
    'SPXStrategy',
    'EntityStrategyFactory',
    'AdaptivePipelineManager',
    
    # Templates
    'PipelineTemplate',
    'StandardPOTemplate',
    'FullPOWithIntegrationTemplate',
    'SimplePRTemplate',
    'SPXSpecialTemplate',
    'DataQualityCheckTemplate',
    'PipelineTemplateManager'
]
