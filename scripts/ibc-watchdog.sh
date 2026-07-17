#!/usr/bin/env bash
set -euo pipefail

# Watchdog for the "gateway process alive but API dead" failure mode.
#
# systemd Restart=always only helps when the IBC/Gateway process exits. When
# Gateway hangs on a login dialog, loses its session silently, or the API
# socket never comes up, the process stays alive and nothing recovers. This
# watchdog restarts ibc-gateway.service after the API port has been dead for
# several consecutive checks.
#
# Safety rules:
# - Does nothing while runtime mode is "protected" (manual trading session).
# - Does nothing when the service is not enabled (manual stop is respected).
# - A healthy API port always resets the failure counter, so it can never
#   restart a working session or fight a phone login (IBC uses
#   ExistingSessionDetectedAction=secondary and yields anyway).

# systemd user services do not include ~/.local/bin in PATH; without this the
# `uv run` data-plane probe exits 127 and every probe "fails", which restarts
# the Gateway (and triggers a fresh 2FA push) every THRESHOLD checks forever.
export PATH="$HOME/.local/bin:$PATH"

SERVICE="${IBC_SYSTEMD_SERVICE:-ibc-gateway.service}"
HOST="${IBKR_HOST:-127.0.0.1}"
PORT="${IBKR_PORT:-4001}"
STATE_FILE="${IBC_WATCHDOG_STATE:-/srv/data/spx-spark/runtime/ibc/watchdog-failures}"
THRESHOLD="${IBC_WATCHDOG_FAILURE_THRESHOLD:-3}"
# Daily cap on gateway restarts: a broken probe would otherwise restart (and
# trigger a fresh 2FA push) every THRESHOLD checks forever.
MAX_RESTARTS_PER_DAY="${IBC_WATCHDOG_MAX_RESTARTS_PER_DAY:-6}"
RESTART_STATE="${IBC_WATCHDOG_RESTART_STATE:-/srv/data/spx-spark/runtime/ibc/watchdog-restarts}"
RUNTIME_MODE_FILE="${RUNTIME_MODE_PATH:-runtime/mode.json}"
# Grace period after a gateway (re)start: login + 2FA + farm reconnect can take
# minutes, and counting probe failures during that window creates a restart ->
# 2FA -> restart loop.
STARTUP_GRACE_SECONDS="${IBC_WATCHDOG_STARTUP_GRACE_SECONDS:-600}"

log() { echo "[ibc-watchdog] $*"; }

restarts_today() {
  local today saved_date saved_count
  today="$(date +%F)"
  if [[ -f "$RESTART_STATE" ]]; then
    saved_date="$(cut -d: -f1 "$RESTART_STATE" 2>/dev/null || true)"
    saved_count="$(cut -d: -f2 "$RESTART_STATE" 2>/dev/null || true)"
    if [[ "$saved_date" == "$today" && "$saved_count" =~ ^[0-9]+$ ]]; then
      echo "$saved_count"
      return
    fi
  fi
  echo 0
}

restart_gateway() {
  local count
  count="$(restarts_today)"
  if (( count >= MAX_RESTARTS_PER_DAY )); then
    log "daily restart cap reached ($count/$MAX_RESTARTS_PER_DAY); not restarting $SERVICE"
    return 1
  fi
  mkdir -p "$(dirname "$RESTART_STATE")"
  printf '%s:%s\n' "$(date +%F)" "$((count + 1))" > "$RESTART_STATE"
  systemctl --user restart "$SERVICE"
}

enabled="$(systemctl --user is-enabled "$SERVICE" 2>/dev/null || true)"
if [[ "$enabled" != "enabled" && "$enabled" != "linked" && "$enabled" != "alias" ]]; then
  log "service $SERVICE is not enabled ($enabled); skipping"
  exit 0
fi

if [[ -f "$RUNTIME_MODE_FILE" ]]; then
  if python3 - "$RUNTIME_MODE_FILE" <<'PY'
import json
import sys
from datetime import datetime, timezone

try:
    with open(sys.argv[1], encoding="utf-8") as handle:
        raw = json.load(handle)
except Exception:
    sys.exit(1)

if str(raw.get("mode", "")).replace("-", "_") != "protected":
    sys.exit(1)

expires = raw.get("expires_at")
if expires:
    parsed = datetime.fromisoformat(str(expires).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    if datetime.now(tz=timezone.utc) >= parsed:
        sys.exit(1)

sys.exit(0)
PY
  then
    log "runtime mode is protected; skipping"
    exit 0
  fi
fi

service_uptime_seconds() {
  local started_at started_epoch
  started_at="$(systemctl --user show "$SERVICE" --property=ActiveEnterTimestamp --value 2>/dev/null || true)"
  [[ -n "$started_at" ]] || { echo ""; return; }
  started_epoch="$(date -d "$started_at" +%s 2>/dev/null || true)"
  [[ -n "$started_epoch" ]] || { echo ""; return; }
  echo "$(( $(date +%s) - started_epoch ))"
}

if timeout 5 bash -c "exec 3<>/dev/tcp/$HOST/$PORT" 2>/dev/null; then
  ROOT="${SPX_SPARK_ROOT:-/home/ubuntu/spx-spark}"
  DATA_PLANE_STATE="${IBC_DATA_PLANE_STATE:-/srv/data/spx-spark/runtime/ibc/data-plane-failures}"
  DATA_PLANE_THRESHOLD="${IBC_DATA_PLANE_FAILURE_THRESHOLD:-3}"

  if ! command -v uv >/dev/null 2>&1; then
    log "uv not found in PATH; skipping data plane probe instead of counting a failure"
    rm -f "$STATE_FILE"
    exit 0
  fi

  probe_exit=0
  probe_output="$(cd "$ROOT" && timeout 60 uv run --no-sync spx-spark-ibkr-farm-probe --json 2>&1)" || probe_exit=$?
  if (( probe_exit == 0 )); then
    rm -f "$STATE_FILE" "$DATA_PLANE_STATE"
    log "API port $HOST:$PORT and data plane are healthy"
    exit 0
  fi

  uptime_seconds="$(service_uptime_seconds)"
  if [[ -n "$uptime_seconds" ]] && (( uptime_seconds < STARTUP_GRACE_SECONDS )); then
    log "data plane probe failed but $SERVICE started ${uptime_seconds}s ago (<${STARTUP_GRACE_SECONDS}s grace); not counting"
    exit 0
  fi
  log "data plane probe exit=$probe_exit: $(tail -n 1 <<<"$probe_output")"

  mkdir -p "$(dirname "$DATA_PLANE_STATE")"
  probe_failures=0
  if [[ -f "$DATA_PLANE_STATE" ]]; then
    probe_failures="$(tr -d '[:space:]' < "$DATA_PLANE_STATE" || echo 0)"
  fi
  [[ "$probe_failures" =~ ^[0-9]+$ ]] || probe_failures=0
  probe_failures=$((probe_failures + 1))
  printf '%s\n' "$probe_failures" > "$DATA_PLANE_STATE"

  if (( probe_failures < DATA_PLANE_THRESHOLD )); then
    log "API port $HOST:$PORT is up but data plane probe failed ($probe_failures/$DATA_PLANE_THRESHOLD)"
    exit 0
  fi

  log "data plane unhealthy for $probe_failures consecutive checks; requesting restart of $SERVICE"
  rm -f "$STATE_FILE" "$DATA_PLANE_STATE"
  restart_gateway
  exit 0
fi

mkdir -p "$(dirname "$STATE_FILE")"
failures=0
if [[ -f "$STATE_FILE" ]]; then
  failures="$(tr -d '[:space:]' < "$STATE_FILE" || echo 0)"
fi
[[ "$failures" =~ ^[0-9]+$ ]] || failures=0
failures=$((failures + 1))
printf '%s\n' "$failures" > "$STATE_FILE"

if (( failures < THRESHOLD )); then
  log "API port $HOST:$PORT is dead ($failures/$THRESHOLD); waiting for more evidence"
  exit 0
fi

log "API port $HOST:$PORT dead for $failures consecutive checks; requesting restart of $SERVICE"
rm -f "$STATE_FILE"
restart_gateway
