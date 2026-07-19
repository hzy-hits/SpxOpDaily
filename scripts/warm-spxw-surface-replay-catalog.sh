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
timeline_count=0
surface_count=0
latest_frame_times=()
for session_date in "${session_dates[@]}"; do
  timeline_json="$(
    curl --silent --show-error --fail --max-time 90 \
      --unix-socket "$SOCKET_PATH" \
      "http://localhost/api/v1/replay/sessions/$session_date/timeline?step_minutes=5"
  )"
  mapfile -t frame_times < <(
    "$PYTHON" -c \
      'import json,sys; payload=json.load(sys.stdin); rows=payload.get("surface_frames") or payload.get("frames", []); print("\n".join(row["at"] for row in rows if isinstance(row, dict) and isinstance(row.get("at"), str)))' \
      <<<"$timeline_json"
  )
  timeline_count=$((timeline_count + 1))
  if (( ${#frame_times[@]} == 0 )); then
    continue
  fi

  surface_times=("${frame_times[-1]}")
  if [[ "$session_date" == "$latest_session" ]]; then
    latest_frame_times=("${frame_times[@]}")
  fi
  for frame_at in "${surface_times[@]}"; do
    curl --silent --show-error --fail --max-time 90 \
      --unix-socket "$SOCKET_PATH" \
      --get \
      --data-urlencode "at=$frame_at" \
      --data-urlencode "role=front" \
      --data-urlencode "weighting=oi_weighted" \
      --data-urlencode "bucket_minutes=5" \
      --data-urlencode "price_step=5" \
      "http://localhost/api/v1/replay/sessions/$session_date/session-surface" \
      >/dev/null
    surface_count=$((surface_count + 1))
  done
done

# Land every catalog date first. Only then spend the remaining warm window on
# every playhead of the latest session for smooth same-day replay.
if (( ${#latest_frame_times[@]} > 1 )); then
  latest_landing_time="${latest_frame_times[-1]}"
  latest_seed_time="$(
    "$PYTHON" -c \
      'import datetime,sys; value=datetime.datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))-datetime.timedelta(seconds=3); print(value.isoformat().replace("+00:00", "Z"))' \
      "$latest_landing_time"
  )"

  # An off-timeline, non-default landing selector is normally absent for each
  # new source fingerprint. Building it once seeds the replay worker's causal
  # frame LRU; the default landing artifact may already be a disk-cache hit.
  curl --silent --show-error --fail --max-time 90 \
    --unix-socket "$SOCKET_PATH" \
    --get \
    --data-urlencode "at=$latest_seed_time" \
    --data-urlencode "role=front" \
    --data-urlencode "weighting=volume_weighted" \
    --data-urlencode "bucket_minutes=5" \
    --data-urlencode "price_step=2.5" \
    "http://localhost/api/v1/replay/sessions/$latest_session/session-surface" \
    >/dev/null
  surface_count=$((surface_count + 1))

  for frame_at in "${latest_frame_times[@]}"; do
    if [[ "$frame_at" == "$latest_landing_time" ]]; then
      continue
    fi
    curl --silent --show-error --fail --max-time 90 \
      --unix-socket "$SOCKET_PATH" \
      --get \
      --data-urlencode "at=$frame_at" \
      --data-urlencode "role=front" \
      --data-urlencode "weighting=oi_weighted" \
      --data-urlencode "bucket_minutes=5" \
      --data-urlencode "price_step=5" \
      "http://localhost/api/v1/replay/sessions/$latest_session/session-surface" \
      >/dev/null
    surface_count=$((surface_count + 1))
  done
fi
printf 'warmed %s replay timelines and %s session surface requests; full playback=%s\n' \
  "$timeline_count" "$surface_count" "$latest_session"
