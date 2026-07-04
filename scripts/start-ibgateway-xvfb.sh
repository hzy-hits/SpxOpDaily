#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="${IBGATEWAY_APP:-/home/ubuntu/apps/ibgateway/ibgateway}"
LOG_DIR="$ROOT/logs"
PID_FILE="$ROOT/logs/ibgateway.pid"
XVFB_PID_FILE="$ROOT/logs/ibgateway-xvfb.pid"
LOG_FILE="$ROOT/logs/ibgateway.log"
DISPLAY_ID="${IBGATEWAY_DISPLAY:-:99}"

mkdir -p "$LOG_DIR"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "IB Gateway already appears to be running, pid=$(cat "$PID_FILE")"
  exit 0
fi

if [[ ! -x "$APP" ]]; then
  echo "IB Gateway executable not found or not executable: $APP" >&2
  exit 1
fi

if [[ -f "$XVFB_PID_FILE" ]] && kill -0 "$(cat "$XVFB_PID_FILE")" 2>/dev/null; then
  echo "Xvfb already appears to be running, pid=$(cat "$XVFB_PID_FILE")"
else
  nohup Xvfb "$DISPLAY_ID" -screen 0 1280x1024x24 -nolisten tcp >"$LOG_DIR/xvfb.log" 2>&1 &
  echo "$!" > "$XVFB_PID_FILE"
  sleep 1
fi

nohup env DISPLAY="$DISPLAY_ID" "$APP" >"$LOG_FILE" 2>&1 &
echo "$!" > "$PID_FILE"

echo "Started IB Gateway under Xvfb"
echo "pid: $(cat "$PID_FILE")"
echo "xvfb pid: $(cat "$XVFB_PID_FILE")"
echo "display: $DISPLAY_ID"
echo "log: $LOG_FILE"
