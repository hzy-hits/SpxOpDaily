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

SERVICE="${IBC_SYSTEMD_SERVICE:-ibc-gateway.service}"
HOST="${IBKR_HOST:-127.0.0.1}"
PORT="${IBKR_PORT:-4001}"
STATE_FILE="${IBC_WATCHDOG_STATE:-/srv/data/spx-spark/runtime/ibc/watchdog-failures}"
THRESHOLD="${IBC_WATCHDOG_FAILURE_THRESHOLD:-3}"
RUNTIME_MODE_FILE="${RUNTIME_MODE_PATH:-runtime/mode.json}"

log() { echo "[ibc-watchdog] $*"; }

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

if timeout 5 bash -c "exec 3<>/dev/tcp/$HOST/$PORT" 2>/dev/null; then
  ROOT="${SPX_SPARK_ROOT:-/home/ubuntu/spx-spark}"
  DATA_PLANE_STATE="${IBC_DATA_PLANE_STATE:-/srv/data/spx-spark/runtime/ibc/data-plane-failures}"
  DATA_PLANE_THRESHOLD="${IBC_DATA_PLANE_FAILURE_THRESHOLD:-3}"

  if (
    cd "$ROOT"
    timeout 60 uv run spx-spark-ibkr-farm-probe --json >/dev/null 2>&1
  ); then
    rm -f "$STATE_FILE" "$DATA_PLANE_STATE"
    log "API port $HOST:$PORT and data plane are healthy"
    exit 0
  fi

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

  log "data plane unhealthy for $probe_failures consecutive checks; restarting $SERVICE"
  rm -f "$STATE_FILE" "$DATA_PLANE_STATE"
  systemctl --user restart "$SERVICE"
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

log "API port $HOST:$PORT dead for $failures consecutive checks; restarting $SERVICE"
rm -f "$STATE_FILE"
systemctl --user restart "$SERVICE"
