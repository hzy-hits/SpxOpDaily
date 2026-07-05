#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT/logs"
PID_FILE="$ROOT/logs/ibgateway-vnc.pid"
DISPLAY_ID="${IBGATEWAY_DISPLAY:-:99}"
PORT="${IBGATEWAY_VNC_PORT:-5909}"
LOG_FILE="$ROOT/logs/ibgateway-vnc.log"

mkdir -p "$LOG_DIR"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "IB Gateway VNC already appears to be running, pid=$(cat "$PID_FILE")"
  exit 0
fi

if ! command -v x11vnc >/dev/null 2>&1; then
  echo "x11vnc is not installed. Install it with: sudo apt-get install -y x11vnc" >&2
  exit 1
fi

if ! xdpyinfo -display "$DISPLAY_ID" >/dev/null 2>&1; then
  echo "Display $DISPLAY_ID is not ready. Start IB Gateway first with scripts/start-ibgateway-xvfb.sh" >&2
  exit 1
fi

setsid x11vnc \
  -display "$DISPLAY_ID" \
  -localhost \
  -nopw \
  -forever \
  -shared \
  -rfbport "$PORT" \
  -o "$LOG_FILE" \
  >/dev/null 2>&1 < /dev/null &
echo "$!" > "$PID_FILE"
sleep 1

if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "x11vnc exited during startup" >&2
  echo "log: $LOG_FILE" >&2
  tail -n 80 "$LOG_FILE" >&2 || true
  exit 1
fi

echo "Started IB Gateway VNC bridge"
echo "pid: $(cat "$PID_FILE")"
echo "display: $DISPLAY_ID"
echo "local port: 127.0.0.1:$PORT"
echo "log: $LOG_FILE"
