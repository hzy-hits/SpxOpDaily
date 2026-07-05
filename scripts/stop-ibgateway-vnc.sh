#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/logs/ibgateway-vnc.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "Stopped IB Gateway VNC pid=$PID"
  else
    echo "IB Gateway VNC process not running: $PID"
  fi
  rm -f "$PID_FILE"
else
  echo "No IB Gateway VNC pid file found: $PID_FILE"
fi
