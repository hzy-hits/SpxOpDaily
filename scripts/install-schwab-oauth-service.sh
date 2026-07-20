#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_UNIT_DIR="$HOME/.config/systemd/user"

cd "$ROOT"
uv run --frozen spx-spark-schwab-oauth status >/dev/null

mkdir -p "$USER_UNIT_DIR"
ln -sfn \
  "$ROOT/systemd/spx-spark-schwab-oauth.service" \
  "$USER_UNIT_DIR/spx-spark-schwab-oauth.service"
ln -sfn \
  "$ROOT/systemd/spx-spark-schwab-reauth-reminder.service" \
  "$USER_UNIT_DIR/spx-spark-schwab-reauth-reminder.service"
ln -sfn \
  "$ROOT/systemd/spx-spark-schwab-reauth-reminder.timer" \
  "$USER_UNIT_DIR/spx-spark-schwab-reauth-reminder.timer"

systemctl --user daemon-reload
systemctl --user enable --now spx-spark-schwab-oauth.service
systemctl --user enable --now spx-spark-schwab-reauth-reminder.timer
systemctl --user status spx-spark-schwab-oauth.service --no-pager
systemctl --user status spx-spark-schwab-reauth-reminder.timer --no-pager
