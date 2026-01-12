# Accrual Bot 測試指南

## 概述

本項目使用 pytest 測試框架，包含單元測試、集成測試和覆蓋率報告功能。

## 測試結構

```
tests/
├── conftest.py                    # 共用 fixtures
├── pytest.ini                     # pytest 配置
├── fixtures/                      # 測試數據生成器
│   └── sample_data.py
├── unit/                          # 單元測試
│   ├── core/pipeline/steps/
│   │   ├── test_base_loading.py          # BaseLoadingStep 測試
│   │   └── test_base_evaluation.py       # BaseERMEvaluationStep 測試
│   ├── tasks/
│   │   ├── spt/
│   │   │   └── test_spt_orchestrator.py  # SPT Orchestrator 測試
│   │   └── spx/
│   │       └── test_spx_orchestrator.py  # SPX Orchestrator 測試
│   └── utils/config/
│       └── test_config_manager.py        # ConfigManager 測試
└── integration/                   # 集成測試
    └── test_pipeline_orchestrators.py    # Pipeline 集成測試
```

## 安裝測試依賴

```bash
# 啟動虛擬環境
./venv/Scripts/activate  # Windows

# 安裝開發依賴
python -m pip install -e ".[dev]"
```

## 運行測試

### 使用測試腳本（推薦）

```bash
# 運行所有測試
.\run_tests.bat

# 只運行單元測試
.\run_tests.bat unit

# 只運行集成測試
.\run_tests.bat integration

# 生成覆蓋率報告
.\run_tests.bat coverage
```

### 使用 pytest 命令

```bash
# 運行所有測試
pytest tests/ -v

# 運行特定測試文件
pytest tests/unit/tasks/spt/test_spt_orchestrator.py -v

# 運行特定測試類
pytest tests/unit/tasks/spt/test_spt_orchestrator.py::TestSPTPipelineOrchestrator -v

# 運行特定測試函數
pytest tests/unit/tasks/spt/test_spt_orchestrator.py::TestSPTPipelineOrchestrator::test_init_reads_config -v

# 運行標記為 unit 的測試
pytest tests/ -v -m unit

# 運行標記為 integration 的測試
pytest tests/ -v -m integration

# 運行標記為 slow 的測試
pytest tests/ -v -m slow

# 生成覆蓋率報告
pytest tests/ --cov=accrual_bot --cov-report=html --cov-report=term-missing
```

## 覆蓋率目標

- **Pipeline Orchestrators**: ≥ 90%
- **Base Classes**: ≥ 85%
- **ConfigManager**: 100%
- **整體**: ≥ 80%

查看覆蓋率報告：

```bash
# Windows
start htmlcov\index.html

# 或直接打開
htmlcov/index.html
```

## 測試標記

- `@pytest.mark.unit`: 單元測試
- `@pytest.mark.integration`: 集成測試
- `@pytest.mark.slow`: 執行時間較長的測試
- `@pytest.mark.asyncio`: 異步測試

## 常見問題

### 1. 測試失敗：找不到模塊

確保已安裝開發依賴並在虛擬環境中運行：

```bash
./venv/Scripts/activate
python -m pip install -e ".[dev]"
```

### 2. 異步測試錯誤

確保安裝了 `pytest-asyncio`：

```bash
python -m pip install pytest-asyncio>=0.23.0
```

### 3. Mock 相關錯誤

確保安裝了 `pytest-mock`：

```bash
python -m pip install pytest-mock>=3.12.0
```

## 編寫新測試

### 單元測試模板

```python
import pytest
from accrual_bot.your_module import YourClass


class TestYourClass:
    """YourClass 測試套件"""

    @pytest.fixture
    def instance(self):
        """創建測試實例"""
        return YourClass()

    def test_basic_functionality(self, instance):
        """測試基本功能"""
        result = instance.method()
        assert result == expected_value

    @pytest.mark.asyncio
    async def test_async_method(self, instance):
        """測試異步方法"""
        result = await instance.async_method()
        assert result is not None
```

### 使用 Fixtures

```python
def test_with_context(processing_context):
    """使用共用 fixture"""
    assert processing_context.entity_type == 'SPT'
    assert len(processing_context.data) > 0
```

## 最佳實踐

1. **測試命名**: 使用描述性名稱，例如 `test_build_po_pipeline_with_config`
2. **測試隔離**: 每個測試應獨立運行，不依賴其他測試
3. **使用 Fixtures**: 利用 pytest fixtures 減少重複代碼
4. **Mock 外部依賴**: 使用 `unittest.mock` 或 `pytest-mock` mock 外部服務
5. **參數化測試**: 使用 `@pytest.mark.parametrize` 測試多種輸入
6. **異步測試**: 使用 `@pytest.mark.asyncio` 標記異步測試

## 持續集成

測試可集成到 CI/CD pipeline：

```yaml
# .github/workflows/test.yml 示例
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: 3.11
      - run: pip install -e ".[dev]"
      - run: pytest tests/ --cov=accrual_bot --cov-report=xml
      - uses: codecov/codecov-action@v2
```
