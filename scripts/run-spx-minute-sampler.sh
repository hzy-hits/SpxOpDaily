#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-spx-minute-sampler"
if [[ -x "$ENTRYPOINT" ]]; then
  exec "$ENTRYPOINT" "$@"
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi
exec "$PYTHON" -m spx_spark.application.runtime.spx_minute_sampler "$@"
