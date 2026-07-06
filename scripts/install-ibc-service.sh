#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_UNIT_DIR="$HOME/.config/systemd/user"
CONFIG_PATH="${IBC_CONFIG:-/srv/data/spx-spark/runtime/ibc/config.ini}"

mkdir -p "$USER_UNIT_DIR"
ln -sfn "$ROOT/systemd/ibc-gateway.service" "$USER_UNIT_DIR/ibc-gateway.service"
ln -sfn "$ROOT/systemd/ibc-watchdog.service" "$USER_UNIT_DIR/ibc-watchdog.service"
ln -sfn "$ROOT/systemd/ibc-watchdog.timer" "$USER_UNIT_DIR/ibc-watchdog.timer"
systemctl --user daemon-reload
systemctl --user enable ibc-gateway.service
systemctl --user enable --now ibc-watchdog.timer

echo "Installed user service: ibc-gateway.service"
echo "Installed watchdog timer: ibc-watchdog.timer (checks the API port every 2 minutes)"

if ! loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=yes'; then
  echo "WARNING: user lingering is off; user services stop at logout and do not start at boot."
  echo "Enable it with: sudo loginctl enable-linger $USER"
fi
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
