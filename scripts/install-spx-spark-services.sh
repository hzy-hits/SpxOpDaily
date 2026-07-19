#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_UNIT_DIR="$HOME/.config/systemd/user"

mkdir -p "$USER_UNIT_DIR"
ln -sfn "$ROOT/systemd/spx-spark-24h.service" "$USER_UNIT_DIR/spx-spark-24h.service"
ln -sfn "$ROOT/systemd/spx-spark-market-features-hot.service" "$USER_UNIT_DIR/spx-spark-market-features-hot.service"
ln -sfn "$ROOT/systemd/spx-spark-intraday-shock-hot.service" "$USER_UNIT_DIR/spx-spark-intraday-shock-hot.service"
ln -sfn "$ROOT/systemd/spx-spark-notification-delivery.service" "$USER_UNIT_DIR/spx-spark-notification-delivery.service"
ln -sfn "$ROOT/systemd/spx-spark-ibkr-stream.service" "$USER_UNIT_DIR/spx-spark-ibkr-stream.service"
ln -sfn "$ROOT/systemd/spx-spark-post-close-review.service" "$USER_UNIT_DIR/spx-spark-post-close-review.service"
ln -sfn "$ROOT/systemd/spx-spark-post-close-review.timer" "$USER_UNIT_DIR/spx-spark-post-close-review.timer"
ln -sfn "$ROOT/systemd/spx-spark-morning-map.service" "$USER_UNIT_DIR/spx-spark-morning-map.service"
ln -sfn "$ROOT/systemd/spx-spark-morning-map.timer" "$USER_UNIT_DIR/spx-spark-morning-map.timer"
ln -sfn "$ROOT/systemd/spx-spark-maintenance-daily.service" "$USER_UNIT_DIR/spx-spark-maintenance-daily.service"
ln -sfn "$ROOT/systemd/spx-spark-maintenance-daily.timer" "$USER_UNIT_DIR/spx-spark-maintenance-daily.timer"
ln -sfn "$ROOT/systemd/spx-spark-maintenance-weekly.service" "$USER_UNIT_DIR/spx-spark-maintenance-weekly.service"
ln -sfn "$ROOT/systemd/spx-spark-maintenance-weekly.timer" "$USER_UNIT_DIR/spx-spark-maintenance-weekly.timer"
ln -sfn "$ROOT/systemd/spx-spark-data-compact.service" "$USER_UNIT_DIR/spx-spark-data-compact.service"
ln -sfn "$ROOT/systemd/spx-spark-data-compact.timer" "$USER_UNIT_DIR/spx-spark-data-compact.timer"
ln -sfn "$ROOT/systemd/spx-spark-data-compact-weekend.service" "$USER_UNIT_DIR/spx-spark-data-compact-weekend.service"
ln -sfn "$ROOT/systemd/spx-spark-data-compact-weekend.timer" "$USER_UNIT_DIR/spx-spark-data-compact-weekend.timer"
ln -sfn "$ROOT/systemd/spx-spark-backtest-weekly.service" "$USER_UNIT_DIR/spx-spark-backtest-weekly.service"
ln -sfn "$ROOT/systemd/spx-spark-backtest-weekly.timer" "$USER_UNIT_DIR/spx-spark-backtest-weekly.timer"

systemctl --user daemon-reload
systemctl --user enable spx-spark-24h.service
systemctl --user enable spx-spark-market-features-hot.service
systemctl --user enable spx-spark-intraday-shock-hot.service
systemctl --user enable spx-spark-notification-delivery.service
systemctl --user enable spx-spark-ibkr-stream.service
systemctl --user enable spx-spark-post-close-review.timer
systemctl --user enable spx-spark-morning-map.timer
systemctl --user enable --now spx-spark-maintenance-daily.timer
systemctl --user enable --now spx-spark-maintenance-weekly.timer
systemctl --user enable --now spx-spark-data-compact.timer
systemctl --user enable --now spx-spark-data-compact-weekend.timer
systemctl --user enable --now spx-spark-backtest-weekly.timer

echo "Installed user services:"
echo "  spx-spark-24h.service"
echo "  spx-spark-market-features-hot.service"
echo "  spx-spark-intraday-shock-hot.service"
echo "  spx-spark-notification-delivery.service"
echo "  spx-spark-ibkr-stream.service"
echo "  spx-spark-post-close-review.timer"
echo "  spx-spark-morning-map.timer"
echo "  spx-spark-maintenance-daily.timer (07:30 CST dry-run)"
echo "  spx-spark-maintenance-weekly.timer (Sun 13:00 CST non-destructive audit)"
echo "  spx-spark-data-compact.timer (hourly at :08 + jitter; never deletes raw)"
echo "  spx-spark-data-compact-weekend.timer (Sat/Sun 08:30 CST bulk catch-up)"
echo "  spx-spark-backtest-weekly.timer (Mon 09:17 CST 0DTE level backtest)"

if ! loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=yes'; then
  echo "WARNING: user lingering is off; user services stop at logout and do not start at boot."
  echo "Enable it with: sudo loginctl enable-linger $USER"
fi

if [[ "${1:-}" == "--now" ]]; then
  # Remove both hot paths from the shared scheduler before starting their sole owners.
  systemctl --user restart spx-spark-24h.service
  systemctl --user restart spx-spark-ibkr-stream.service
  systemctl --user restart spx-spark-market-features-hot.service
  systemctl --user restart spx-spark-intraday-shock-hot.service
  systemctl --user restart spx-spark-notification-delivery.service
  systemctl --user status spx-spark-24h.service spx-spark-market-features-hot.service spx-spark-intraday-shock-hot.service spx-spark-notification-delivery.service spx-spark-ibkr-stream.service --no-pager
fi
