#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATA_ROOT="${MARKET_DATA_DATA_ROOT:-/srv/data/spx-spark/data}"
RUNTIME_DIR="${SPXW_SURFACE_REPLAY_RUNTIME_DIR:-$DATA_ROOT/published/spxw-surface/runtime}"
SOCKET_PATH="${SPXW_SURFACE_REPLAY_SOCKET_PATH:-$RUNTIME_DIR/replay-api.sock}"

mkdir -p "$RUNTIME_DIR"
chmod 0700 "$RUNTIME_DIR"

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-surface-replay-service"
if [[ -x "$ENTRYPOINT" ]]; then
  exec "$ENTRYPOINT" \
    --data-root "$DATA_ROOT" \
    --unix-socket "$SOCKET_PATH" \
    "$@"
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

exec "$PYTHON" -m spx_spark.surface_replay_service \
  --data-root "$DATA_ROOT" \
  --unix-socket "$SOCKET_PATH" \
  "$@"
