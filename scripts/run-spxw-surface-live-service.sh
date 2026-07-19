#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LIVE_DATA_ROOT="${MARKET_DATA_DATA_ROOT:-/srv/data/spx-spark/data}"
LIVE_PUBLISH_ROOT="${SPXW_SURFACE_PUBLISH_DIR:-$LIVE_DATA_ROOT/published/spxw-surface}"
LIVE_INPUT_PATH="${SPXW_SURFACE_LIVE_INPUT_PATH:-$LIVE_PUBLISH_ROOT/snapshot.json}"
LIVE_STATE_ROOT="${SPXW_SURFACE_LIVE_STATE_ROOT:-$LIVE_PUBLISH_ROOT/live}"
LIVE_RUNTIME_ROOT="${SPXW_SURFACE_LIVE_RUNTIME_ROOT:-$LIVE_PUBLISH_ROOT/runtime/live}"
LIVE_SOCKET_PATH="${SPXW_SURFACE_LIVE_SOCKET_PATH:-$LIVE_RUNTIME_ROOT/live-api.sock}"

for required_dir in "$LIVE_STATE_ROOT" "$LIVE_RUNTIME_ROOT"; do
  if [[ ! -d "$required_dir" ]]; then
    printf 'missing pre-created live surface directory: %s (run install-spxw-surface-live-service.sh)\n' \
      "$required_dir" >&2
    exit 1
  fi
done

ENTRYPOINT="$ROOT/.venv/bin/spx-spark-surface-live-service"
if [[ -x "$ENTRYPOINT" ]]; then
  exec "$ENTRYPOINT" \
    --input-path "$LIVE_INPUT_PATH" \
    --state-root "$LIVE_STATE_ROOT" \
    --unix-socket "$LIVE_SOCKET_PATH" \
    --poll-seconds 0.25 \
    "$@"
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

exec "$PYTHON" -m spx_spark.surface_live_session_http \
  --input-path "$LIVE_INPUT_PATH" \
  --state-root "$LIVE_STATE_ROOT" \
  --unix-socket "$LIVE_SOCKET_PATH" \
  --poll-seconds 0.25 \
  "$@"
