#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="${IBGATEWAY_APP:-/home/ubuntu/apps/ibgateway/ibgateway}"
LOG_DIR="$ROOT/logs"
PID_FILE="$ROOT/logs/ibgateway.pid"
XVFB_PID_FILE="$ROOT/logs/ibgateway-xvfb.pid"
LOG_FILE="$ROOT/logs/ibgateway.log"
DISPLAY_ID="${IBGATEWAY_DISPLAY:-:99}"
DISPLAY_NUM="${DISPLAY_ID#:}"
XVFB_LOCK="/tmp/.X${DISPLAY_NUM}-lock"
XVFB_SOCKET="/tmp/.X11-unix/X${DISPLAY_NUM}"

mkdir -p "$LOG_DIR"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "IB Gateway already appears to be running, pid=$(cat "$PID_FILE")"
  exit 0
fi

if [[ ! -x "$APP" ]]; then
  echo "IB Gateway executable not found or not executable: $APP" >&2
  exit 1
fi

cleanup_stale_display() {
  local lock_pid=""

  if [[ -f "$XVFB_LOCK" ]]; then
    lock_pid="$(tr -d '[:space:]' < "$XVFB_LOCK" || true)"
    if [[ "$lock_pid" =~ ^[0-9]+$ ]] && kill -0 "$lock_pid" 2>/dev/null; then
      return
    fi
    rm -f "$XVFB_LOCK" "$XVFB_SOCKET"
  elif [[ -S "$XVFB_SOCKET" ]]; then
    rm -f "$XVFB_SOCKET"
  fi
}

wait_for_display() {
  local attempt

  for attempt in {1..30}; do
    if xdpyinfo -display "$DISPLAY_ID" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done

  return 1
}

if [[ -f "$XVFB_PID_FILE" ]] && kill -0 "$(cat "$XVFB_PID_FILE")" 2>/dev/null; then
  echo "Xvfb already appears to be running, pid=$(cat "$XVFB_PID_FILE")"
else
  rm -f "$XVFB_PID_FILE"
  cleanup_stale_display
  setsid Xvfb "$DISPLAY_ID" -screen 0 1280x1024x24 -ac -noreset -nolisten tcp >"$LOG_DIR/xvfb.log" 2>&1 < /dev/null &
  echo "$!" > "$XVFB_PID_FILE"
fi

if ! wait_for_display; then
  echo "Xvfb did not become ready on $DISPLAY_ID" >&2
  echo "xvfb log: $LOG_DIR/xvfb.log" >&2
  tail -n 80 "$LOG_DIR/xvfb.log" >&2 || true
  exit 1
fi

: > "$LOG_FILE"
setsid env DISPLAY="$DISPLAY_ID" "$APP" >"$LOG_FILE" 2>&1 < /dev/null &
echo "$!" > "$PID_FILE"
sleep 3

if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "IB Gateway exited during startup" >&2
  echo "log: $LOG_FILE" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
fi

echo "Started IB Gateway under Xvfb"
echo "pid: $(cat "$PID_FILE")"
echo "xvfb pid: $(cat "$XVFB_PID_FILE")"
echo "display: $DISPLAY_ID"
echo "log: $LOG_FILE"
