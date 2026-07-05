#!/usr/bin/env bash
set -euo pipefail

if systemctl --user list-unit-files ibc-gateway.service >/dev/null 2>&1; then
  systemctl --user stop ibc-gateway.service || true
fi

pkill -TERM -f 'ibcalpha.ibc.IbcGateway' 2>/dev/null || true
pkill -TERM -f 'IBC.jar' 2>/dev/null || true

echo "Stopped IBC Gateway processes if they were running."
