#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-intraday-shock-hot-worker"
if [[ -x "$ENTRYPOINT" ]]; then
  exec "$ENTRYPOINT" "$@"
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

# Keep rolling deploys safe before the regenerated console-script shim exists.
exec "$PYTHON" -m spx_spark.application.runtime.intraday_shock_hot_worker "$@"
