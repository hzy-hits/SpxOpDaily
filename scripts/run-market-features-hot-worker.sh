#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-market-features-hot-worker"
if [[ -x "$ENTRYPOINT" ]]; then
  exec "$ENTRYPOINT" "$@"
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

# The module fallback makes a rolling deploy safe before the new console-script
# shim has been regenerated; both paths execute the same persistent runtime.
exec "$PYTHON" -m spx_spark.application.runtime.market_features_hot_worker "$@"
