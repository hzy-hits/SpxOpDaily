#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${MARKET_DATA_DATA_ROOT:-/srv/data/spx-spark/data}"
RUNTIME_DIR="${SPXW_SURFACE_REPLAY_RUNTIME_DIR:-$DATA_ROOT/published/spxw-surface/runtime}"
SOCKET_PATH="${SPXW_SURFACE_REPLAY_SOCKET_PATH:-$RUNTIME_DIR/replay-api.sock}"
PYTHON="$ROOT/.venv/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

sessions_json="$(
  curl --silent --show-error --fail --max-time 10 \
    --unix-socket "$SOCKET_PATH" \
    http://localhost/api/v1/replay/sessions
)"
session_date="$(
  "$PYTHON" -c \
    'import json,sys; rows=json.load(sys.stdin).get("sessions", []); print(rows[0]["session_date"] if rows else "")' \
    <<<"$sessions_json"
)"

if [[ -z "$session_date" ]]; then
  printf 'no replay session available\n'
  exit 0
fi

curl --silent --show-error --fail --max-time 90 \
  --unix-socket "$SOCKET_PATH" \
  "http://localhost/api/v1/replay/sessions/$session_date/timeline?step_minutes=5" \
  >/dev/null
printf 'warmed replay catalog for %s\n' "$session_date"
