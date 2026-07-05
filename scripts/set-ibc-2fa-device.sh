#!/usr/bin/env bash
set -euo pipefail

CONFIG="${IBC_CONFIG_PATH:-/srv/data/spx-spark/runtime/ibc/config.ini}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 '<device name exactly as shown in IBKR Gateway 2FA dialog>'" >&2
  exit 2
fi

device="$*"
if [[ "$device" == *$'\n'* || "$device" == *$'\r'* ]]; then
  echo "Device name must be a single line." >&2
  exit 2
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "IBC config not found: $CONFIG" >&2
  exit 1
fi

DEVICE="$device" perl -0pi -e '
  $d = $ENV{DEVICE};
  if (!s/^SecondFactorDevice=.*/SecondFactorDevice=$d/m) {
    $_ .= "\nSecondFactorDevice=$d\n";
  }
' "$CONFIG"

chmod 600 "$CONFIG"
echo "Set SecondFactorDevice in $CONFIG"
echo "Restart with: systemctl --user restart ibc-gateway.service"
