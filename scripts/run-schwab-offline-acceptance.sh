#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -n "${UV_BIN:-}" ]]; then
  UV="$UV_BIN"
elif command -v uv >/dev/null 2>&1; then
  UV="$(command -v uv)"
elif [[ -x "$HOME/.local/bin/uv" ]]; then
  UV="$HOME/.local/bin/uv"
else
  echo "uv was not found; set UV_BIN or install uv" >&2
  exit 127
fi

"$UV" run --frozen pytest -q \
  tests/test_schwab_adapter.py \
  tests/test_schwab_collector.py \
  tests/test_schwab_gateway.py \
  tests/test_schwab_verifier.py \
  tests/schwab \
  tests/test_ibkr_quota_plan.py \
  tests/test_provider_adapter.py \
  tests/test_marketdata.py \
  tests/test_storage.py \
  tests/data_platform/test_storage_research_e2e.py

"$UV" run --frozen ruff check \
  src/spx_spark/schwab \
  src/spx_spark/data_platform/research \
  tests/test_schwab_adapter.py \
  tests/test_schwab_collector.py \
  tests/test_schwab_gateway.py \
  tests/test_schwab_verifier.py \
  tests/schwab \
  tests/test_ibkr_quota_plan.py \
  tests/data_platform/test_storage_research_e2e.py
