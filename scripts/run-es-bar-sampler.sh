#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-es-bar-sampler"
if [[ -x "$ENTRYPOINT" ]]; then
  exec "$ENTRYPOINT" "$@"
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

# Keep rolling deployment viable until uv has regenerated the console shim.
exec "$PYTHON" -m spx_spark.application.runtime.es_bar_sampler "$@"
