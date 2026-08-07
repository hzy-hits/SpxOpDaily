#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LIVE_DATA_ROOT="${MARKET_DATA_DATA_ROOT:-/srv/data/spx-spark/data}"
LIVE_PUBLISH_ROOT="${SPX_SURFACE_PUBLISH_DIR:-$LIVE_DATA_ROOT/published/spxw-surface}"
LIVE_STATE_ROOT="${SPX_LIVE_SESSION_STATE_ROOT:-$LIVE_PUBLISH_ROOT/live/policy=live-v2/bucket=1m}"
LIVE_RUNTIME_ROOT="${SPX_LIVE_SESSION_RUNTIME_ROOT:-$LIVE_PUBLISH_ROOT/runtime/live}"
LIVE_SOCKET_PATH="${SPX_CORE_SOCKET_PATH:-$LIVE_RUNTIME_ROOT/live-api.sock}"

for required_dir in "$LIVE_STATE_ROOT" "$LIVE_RUNTIME_ROOT"; do
  if [[ ! -d "$required_dir" ]]; then
    printf 'missing pre-created live surface directory: %s (run install-spxw-surface-live-service.sh)\n' \
      "$required_dir" >&2
    exit 1
  fi
done

UVICORN="$ROOT/.venv/bin/uvicorn"
if [[ ! -x "$UVICORN" ]]; then
  printf 'missing uvicorn: %s (run uv sync --frozen first)\n' "$UVICORN" >&2
  exit 1
fi

export SPX_CORE_SOCKET_PATH="$LIVE_SOCKET_PATH"
export SPX_LIVE_SESSION_POLL_SECONDS="${SPX_LIVE_SESSION_POLL_SECONDS:-0.25}"
exec "$UVICORN" spx_spark.web.live_api:create_default_app \
  --factory \
  --uds "$LIVE_SOCKET_PATH" \
  --no-access-log \
  "$@"
