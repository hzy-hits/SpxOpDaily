#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATA_ROOT="${MARKET_DATA_DATA_ROOT:-/srv/data/spx-spark/data}"
RUNTIME_DIR="${SPX_SURFACE_REPLAY_RUNTIME_DIR:-$DATA_ROOT/published/spxw-surface/runtime}"
SOCKET_PATH="${SPX_SURFACE_REPLAY_SOCKET_PATH:-$RUNTIME_DIR/replay-api.sock}"

mkdir -p "$RUNTIME_DIR"
chmod 0700 "$RUNTIME_DIR"
export SPX_CORE_SOCKET_PATH="$SOCKET_PATH"

UVICORN="$ROOT/.venv/bin/uvicorn"
if [[ ! -x "$UVICORN" ]]; then
  printf 'missing uvicorn: %s (run uv sync --frozen first)\n' "$UVICORN" >&2
  exit 1
fi

exec "$UVICORN" spx_spark.web.replay_api:create_default_app \
  --factory \
  --uds "$SOCKET_PATH" \
  --no-access-log \
  "$@"
