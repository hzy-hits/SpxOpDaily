#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIVE_USER_UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
LIVE_DATA_ROOT="${MARKET_DATA_DATA_ROOT:-/srv/data/spx-spark/data}"
LIVE_PUBLISH_ROOT="${SPXW_SURFACE_PUBLISH_DIR:-$LIVE_DATA_ROOT/published/spxw-surface}"
LIVE_STATE_ROOT="${SPXW_SURFACE_LIVE_STATE_ROOT:-$LIVE_PUBLISH_ROOT/live}"
LIVE_RUNTIME_ROOT="${SPXW_SURFACE_LIVE_RUNTIME_ROOT:-$LIVE_PUBLISH_ROOT/runtime/live}"
LIVE_SOCKET_PATH="${SPXW_SURFACE_LIVE_SOCKET_PATH:-$LIVE_RUNTIME_ROOT/live-api.sock}"
LIVE_EXPECTED_UID="${SPXW_SURFACE_UID:-1001}"
LIVE_EXPECTED_GID="${SPXW_SURFACE_GID:-1001}"

if (( $# > 1 )); then
  printf 'usage: %s [--now]\n' "$0" >&2
  exit 2
fi
if (( $# == 1 )) && [[ "$1" != "--now" ]]; then
  printf 'usage: %s [--now]\n' "$0" >&2
  exit 2
fi

if [[ "$LIVE_EXPECTED_UID" != "$(id -u)" || "$LIVE_EXPECTED_GID" != "$(id -g)" ]]; then
  printf 'SPXW surface container UID:GID %s:%s must match service owner %s:%s\n' \
    "$LIVE_EXPECTED_UID" "$LIVE_EXPECTED_GID" "$(id -u)" "$(id -g)" >&2
  exit 1
fi

if command -v docker >/dev/null 2>&1 && docker inspect spxw-surface >/dev/null 2>&1; then
  LIVE_CONTAINER_IDENTITY="$(docker inspect --format '{{.Config.User}}' spxw-surface)"
  if [[ "$LIVE_CONTAINER_IDENTITY" != "$LIVE_EXPECTED_UID:$LIVE_EXPECTED_GID" ]]; then
    printf 'running spxw-surface container user %s does not match expected %s:%s\n' \
      "$LIVE_CONTAINER_IDENTITY" "$LIVE_EXPECTED_UID" "$LIVE_EXPECTED_GID" >&2
    exit 1
  fi
fi

mkdir -p "$LIVE_USER_UNIT_DIR"
install -d -m 0700 "$LIVE_STATE_ROOT" "$LIVE_RUNTIME_ROOT"

validate_private_dir() {
  local target="$1"
  local identity mode
  identity="$(stat -c '%u:%g' "$target")"
  mode="$(stat -c '%a' "$target")"
  if [[ "$identity" != "$LIVE_EXPECTED_UID:$LIVE_EXPECTED_GID" || "$mode" != "700" ]]; then
    printf 'unsafe live surface directory %s: owner=%s mode=%s expected=%s:%s mode=700\n' \
      "$target" "$identity" "$mode" "$LIVE_EXPECTED_UID" "$LIVE_EXPECTED_GID" >&2
    exit 1
  fi
}

validate_live_socket() {
  local identity mode
  if [[ ! -S "$LIVE_SOCKET_PATH" ]]; then
    printf 'live surface Unix socket was not created: %s\n' "$LIVE_SOCKET_PATH" >&2
    exit 1
  fi
  identity="$(stat -c '%u:%g' "$LIVE_SOCKET_PATH")"
  mode="$(stat -c '%a' "$LIVE_SOCKET_PATH")"
  if [[ "$identity" != "$LIVE_EXPECTED_UID:$LIVE_EXPECTED_GID" || "$mode" != "660" ]]; then
    printf 'unsafe live surface socket %s: owner=%s mode=%s expected=%s:%s mode=660\n' \
      "$LIVE_SOCKET_PATH" "$identity" "$mode" "$LIVE_EXPECTED_UID" \
      "$LIVE_EXPECTED_GID" >&2
    exit 1
  fi
  curl --fail --silent --show-error --unix-socket "$LIVE_SOCKET_PATH" \
    http://localhost/healthz >/dev/null
}

validate_private_dir "$LIVE_STATE_ROOT"
validate_private_dir "$LIVE_RUNTIME_ROOT"

ln -sfn \
  "$ROOT/systemd/spx-spark-surface-live.service" \
  "$LIVE_USER_UNIT_DIR/spx-spark-surface-live.service"
systemctl --user daemon-reload
systemctl --user enable spx-spark-surface-live.service

if [[ "${1:-}" == "--now" ]]; then
  systemctl --user restart spx-spark-surface-live.service
  for _attempt in $(seq 1 100); do
    if [[ -S "$LIVE_SOCKET_PATH" ]] && curl --fail --silent \
      --unix-socket "$LIVE_SOCKET_PATH" http://localhost/healthz >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  validate_live_socket
  systemctl --user status spx-spark-surface-live.service --no-pager
elif systemctl --user is-active --quiet spx-spark-surface-live.service; then
  validate_live_socket
fi

printf 'Installed spx-spark-surface-live.service\n'
printf '  state: %s\n' "$LIVE_STATE_ROOT"
printf '  socket: %s\n' "$LIVE_SOCKET_PATH"
