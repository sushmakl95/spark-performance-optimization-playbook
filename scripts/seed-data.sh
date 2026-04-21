#!/usr/bin/env bash
# Generate synthetic dataset at a chosen scale.
# Usage: bash scripts/seed-data.sh [1x|10x|100x] [--skewed]

set -euo pipefail

SCALE="${1:-1x}"
SKEWED_FLAG=""

if [[ "${2:-}" == "--skewed" ]]; then
    SKEWED_FLAG="--skewed"
fi

echo "[seed] Generating $SCALE dataset${SKEWED_FLAG:+ (skewed)}..."
python -m data.generators.ecommerce \
    --output-dir data/generated \
    --scale "$SCALE" \
    $SKEWED_FLAG

echo "[seed] Done. Dataset at data/generated/"
ls -lh data/generated/
