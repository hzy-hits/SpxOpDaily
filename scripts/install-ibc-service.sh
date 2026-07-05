#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_UNIT_DIR="$HOME/.config/systemd/user"
CONFIG_PATH="${IBC_CONFIG:-/srv/data/spx-spark/runtime/ibc/config.ini}"

mkdir -p "$USER_UNIT_DIR"
ln -sfn "$ROOT/systemd/ibc-gateway.service" "$USER_UNIT_DIR/ibc-gateway.service"
systemctl --user daemon-reload
systemctl --user enable ibc-gateway.service

echo "Installed user service: ibc-gateway.service"
if [[ -f "$CONFIG_PATH" ]]; then
  echo "Config present: $CONFIG_PATH"
else
  echo "Config missing: $CONFIG_PATH"
  echo "Run scripts/configure-ibc-secrets.sh before starting the service."
fi

if [[ "${1:-}" == "--now" ]]; then
  systemctl --user start ibc-gateway.service
  systemctl --user status ibc-gateway.service --no-pager
fi
