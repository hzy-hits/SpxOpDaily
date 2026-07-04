#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/logs/ibgateway.pid"
XVFB_PID_FILE="$ROOT/logs/ibgateway-xvfb.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "Stopped IB Gateway pid=$PID"
  else
    echo "IB Gateway process not running: $PID"
  fi
  rm -f "$PID_FILE"
else
  echo "No IB Gateway pid file found: $PID_FILE"
fi

if [[ -f "$XVFB_PID_FILE" ]]; then
  XVFB_PID="$(cat "$XVFB_PID_FILE")"
  if kill -0 "$XVFB_PID" 2>/dev/null; then
    kill "$XVFB_PID"
    echo "Stopped Xvfb pid=$XVFB_PID"
  else
    echo "Xvfb process not running: $XVFB_PID"
  fi
  rm -f "$XVFB_PID_FILE"
fi
