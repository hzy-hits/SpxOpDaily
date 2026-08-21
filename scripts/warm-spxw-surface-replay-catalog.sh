#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${MARKET_DATA_DATA_ROOT:-/srv/data/spx-spark/data}"
RUNTIME_DIR="${SPXW_SURFACE_REPLAY_RUNTIME_DIR:-$DATA_ROOT/published/spxw-surface/runtime}"
SOCKET_PATH="${SPXW_SURFACE_REPLAY_SOCKET_PATH:-$RUNTIME_DIR/core-api.sock}"
PYTHON="$ROOT/.venv/bin/python"

if command -v flock >/dev/null 2>&1; then
  LOCK_ROOT="${XDG_RUNTIME_DIR:-/tmp}"
  exec 9>"$LOCK_ROOT/spx-spark-session-finalize.lock"
  if ! flock -n 9; then
    printf 'replay warm skipped: session finalizer is active\n'
    exit 0
  fi
fi

if [[ ! -x "$PYTHON" ]]; then
  printf 'missing virtualenv Python: %s (run uv sync first)\n' "$PYTHON" >&2
  exit 1
fi

core_ready=false
for _attempt in $(seq 1 30); do
  if curl --silent --fail --max-time 2 \
    --unix-socket "$SOCKET_PATH" http://localhost/healthz >/dev/null; then
    core_ready=true
    break
  fi
  sleep 2
done
if [[ "$core_ready" != true ]]; then
  printf 'replay core socket not ready: %s\n' "$SOCKET_PATH" >&2
  exit 7
fi

sessions_json="$(
  curl --silent --show-error --fail --max-time 30 \
    --unix-socket "$SOCKET_PATH" \
    http://localhost/api/v1/replay/sessions
)"
mapfile -t session_dates < <(
  "$PYTHON" -c \
    'import datetime,json,sys; rows=json.load(sys.stdin).get("sessions", []); print("\n".join(value for row in rows if isinstance(row, dict) and isinstance((value := row.get("session_date")), str) and datetime.date.fromisoformat(value)))' \
    <<<"$sessions_json"
)

if (( ${#session_dates[@]} == 0 )); then
  printf 'no replay session available\n'
  exit 0
fi

latest_session="${session_dates[0]}"
timeline_json="$(
  curl --silent --show-error --fail --max-time 60 \
    --unix-socket "$SOCKET_PATH" \
    "http://localhost/api/v1/replay/sessions/$latest_session/timeline?step_minutes=5"
)"
mapfile -t frame_times < <(
  "$PYTHON" -c \
    'import json,sys; payload=json.load(sys.stdin); rows=payload.get("surface_frames") or payload.get("frames", []); print("\n".join(row["at"] for row in rows if isinstance(row, dict) and isinstance(row.get("at"), str)))' \
    <<<"$timeline_json"
)
if (( ${#frame_times[@]} == 0 )); then
  printf 'warmed latest replay timeline; no surface frame available: %s\n' "$latest_session"
  exit 0
fi

latest_landing_time="${frame_times[-1]}"
curl --silent --show-error --fail --max-time 180 \
  --unix-socket "$SOCKET_PATH" \
  --get \
  --data-urlencode "at=$latest_landing_time" \
  --data-urlencode "role=front" \
  --data-urlencode "weighting=oi_weighted" \
  --data-urlencode "bucket_minutes=5" \
  --data-urlencode "price_step=5" \
  "http://localhost/api/v1/replay/sessions/$latest_session/session-surface" \
  >/dev/null
printf 'warmed latest replay timeline and landing surface: %s\n' "$latest_session"
