#!/bin/bash
# P0 快速煙霧測試
cd "$(dirname "$0")/.." || exit 1
python -m pytest \
    tests/unit/core/pipeline/test_context.py \
    tests/unit/core/pipeline/test_base_classes.py \
    tests/unit/core/pipeline/test_pipeline.py \
    tests/unit/core/pipeline/test_pipeline_builder.py \
    tests/unit/core/pipeline/test_checkpoint.py \
    -v --tb=short "$@"
