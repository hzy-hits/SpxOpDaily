#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PUBLISH_PATH="${SPXW_SURFACE_PUBLISH_PATH:-/srv/data/spx-spark/data/published/spxw-surface/snapshot.json}"
mkdir -p "$(dirname "$PUBLISH_PATH")"

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-surface-dashboard"
if [[ -x "$ENTRYPOINT" ]]; then
  exec "$ENTRYPOINT" --output-path "$PUBLISH_PATH" "$@"
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

exec "$PYTHON" -m spx_spark.surface_dashboard --output-path "$PUBLISH_PATH" "$@"
