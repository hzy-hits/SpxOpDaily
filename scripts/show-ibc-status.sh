#!/usr/bin/env bash
set -euo pipefail

SERVICE="${IBC_SYSTEMD_SERVICE:-ibc-gateway.service}"
CONFIG="${IBC_CONFIG_PATH:-/srv/data/spx-spark/runtime/ibc/config.ini}"

echo "service=$SERVICE"
echo "active=$(systemctl --user is-active "$SERVICE" 2>/dev/null || true)"
systemctl --user show "$SERVICE" \
  -p ActiveState \
  -p SubState \
  -p NRestarts \
  -p ExecMainPID \
  --no-pager 2>/dev/null || true

echo
echo "ports:"
if ss -ltnp | grep -E ':(4001|7462)\b' >/dev/null 2>&1; then
  ss -ltnp | grep -E ':(4001|7462)\b'
else
  echo "  no 4001/7462 listeners"
fi

echo
echo "safe_config:"
if [[ -r "$CONFIG" ]]; then
  grep -E '^(TradingMode|SecondFactorDevice|SecondFactorAuthenticationTimeout|ReloginAfterSecondFactorAuthenticationTimeout|ExitAfterSecondFactorAuthenticationTimeout|ExistingSessionDetectedAction|ReadOnlyLogin|ReadOnlyApi|OverrideTwsApiPort)=' "$CONFIG" \
    | sed -E 's/^(IbLoginId|IbPassword)=.*/\1=<redacted>/'
else
  echo "  config not readable: $CONFIG"
fi

echo
echo "recent_markers:"
journalctl --user -u "$SERVICE" -n 240 --no-pager 2>/dev/null \
  | grep -E 'Second Factor Authentication|Existing session detected|Login has completed|Gateway finished|Read-Only API|TWS API socket port|CommandServer listening|IBC returned exit status|Socket|Login attempt|Authenticating' \
  | tail -40 \
  || true
