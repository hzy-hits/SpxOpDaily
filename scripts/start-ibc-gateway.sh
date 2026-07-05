#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IBC_DIR="${IBC_INSTALL_DIR:-/home/ubuntu/apps/ibc}"
IBC_INI="${IBC_CONFIG:-/srv/data/spx-spark/runtime/ibc/config.ini}"
LOG_DIR="${IBC_LOG_DIR:-/srv/data/spx-spark/logs/ibc}"
DISPLAY_ID="${IBGATEWAY_DISPLAY:-:99}"
DISPLAY_NUM="${DISPLAY_ID#:}"
XVFB_PID_FILE="$ROOT/logs/ibgateway-xvfb.pid"
XVFB_LOCK="/tmp/.X${DISPLAY_NUM}-lock"
XVFB_SOCKET="/tmp/.X11-unix/X${DISPLAY_NUM}"
REAL_GATEWAY_DIR="${IBGATEWAY_APP_DIR:-/home/ubuntu/apps/ibgateway}"
COMPAT_ROOT="${IBC_TWS_COMPAT_ROOT:-/home/ubuntu/apps/ibc-ibgateway-compat}"
TWS_SETTINGS_PATH="${IBC_TWS_SETTINGS_PATH:-/home/ubuntu/Jts}"
TWS_MAJOR_VRSN="${IBC_TWS_MAJOR_VRSN:-}"
TWOFA_TIMEOUT_ACTION="${IBC_TWOFA_TIMEOUT_ACTION:-restart}"
TRADING_MODE_OVERRIDE="${IBC_TRADING_MODE:-}"

mkdir -p "$ROOT/logs" "$LOG_DIR"

detect_major_version() {
  local jar version
  while IFS= read -r jar; do
    version="${jar##*/}"
    version="${version#twslaunch-}"
    version="${version%.jar}"
    if [[ "$version" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$version"
      return 0
    fi
  done < <(find "$REAL_GATEWAY_DIR/jars" -maxdepth 1 -type f -name 'twslaunch-*.jar' 2>/dev/null | sort -r)
  return 1
}

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

ensure_display() {
  if [[ -f "$XVFB_PID_FILE" ]] && kill -0 "$(cat "$XVFB_PID_FILE")" 2>/dev/null; then
    :
  else
    rm -f "$XVFB_PID_FILE"
    cleanup_stale_display
    setsid Xvfb "$DISPLAY_ID" -screen 0 1280x1024x24 -ac -noreset -nolisten tcp >"$ROOT/logs/xvfb.log" 2>&1 < /dev/null &
    echo "$!" > "$XVFB_PID_FILE"
  fi

  if ! wait_for_display; then
    echo "Xvfb did not become ready on $DISPLAY_ID" >&2
    tail -n 80 "$ROOT/logs/xvfb.log" >&2 || true
    exit 1
  fi
}

ensure_compat_gateway_tree() {
  if [[ -z "$TWS_MAJOR_VRSN" ]]; then
    TWS_MAJOR_VRSN="$(detect_major_version || true)"
  fi
  if [[ -z "$TWS_MAJOR_VRSN" ]]; then
    echo "Unable to detect IB Gateway major version from $REAL_GATEWAY_DIR/jars" >&2
    exit 1
  fi

  local compat_dir="$COMPAT_ROOT/ibgateway/$TWS_MAJOR_VRSN"
  mkdir -p "$compat_dir"
  ln -sfn "$REAL_GATEWAY_DIR/jars" "$compat_dir/jars"
  ln -sfn "$REAL_GATEWAY_DIR/.install4j" "$compat_dir/.install4j"
  ln -sfn "$REAL_GATEWAY_DIR/ibgateway.vmoptions" "$compat_dir/ibgateway.vmoptions"
}

if [[ "${1:-}" == "--check" ]]; then
  ensure_compat_gateway_tree
  echo "IBC_DIR=$IBC_DIR"
  echo "IBC_INI=$IBC_INI"
  echo "LOG_DIR=$LOG_DIR"
  echo "DISPLAY=$DISPLAY_ID"
  echo "TWS_MAJOR_VRSN=$TWS_MAJOR_VRSN"
  echo "TWS_PATH=$COMPAT_ROOT"
  echo "TWS_SETTINGS_PATH=$TWS_SETTINGS_PATH"
  [[ -f "$IBC_INI" ]] && echo "config=present" || echo "config=missing"
  exit 0
fi

if [[ ! -x "$IBC_DIR/scripts/ibcstart.sh" ]]; then
  echo "IBC is not installed at $IBC_DIR. Run scripts/install-ibc.sh first." >&2
  exit 1
fi

if [[ ! -f "$IBC_INI" ]]; then
  echo "IBC config not found: $IBC_INI" >&2
  echo "Run scripts/configure-ibc-secrets.sh first." >&2
  exit 1
fi

ensure_display
ensure_compat_gateway_tree

args=(
  "$TWS_MAJOR_VRSN"
  "--gateway"
  "--tws-path=$COMPAT_ROOT"
  "--tws-settings-path=$TWS_SETTINGS_PATH"
  "--ibc-path=$IBC_DIR"
  "--ibc-ini=$IBC_INI"
  "--on2fatimeout=$TWOFA_TIMEOUT_ACTION"
)

if [[ -n "$TRADING_MODE_OVERRIDE" ]]; then
  args+=("--mode=$TRADING_MODE_OVERRIDE")
fi

cd "$ROOT"
exec env DISPLAY="$DISPLAY_ID" "$IBC_DIR/scripts/ibcstart.sh" "${args[@]}"
