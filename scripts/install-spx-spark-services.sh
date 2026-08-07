#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_UNIT_DIR="$HOME/.config/systemd/user"

if ! git -C "$ROOT" fetch --quiet origin master; then
  echo "Refusing deployment: could not refresh origin/master" >&2
  exit 1
fi
DEPLOY_BRANCH="$(git -C "$ROOT" branch --show-current)"
DEPLOY_HEAD="$(git -C "$ROOT" rev-parse HEAD)"
DEPLOY_ORIGIN_MASTER="$(git -C "$ROOT" rev-parse refs/remotes/origin/master)"
if [[ "$DEPLOY_BRANCH" != "master" ]]; then
  echo "Refusing deployment from branch: $DEPLOY_BRANCH (expected master)" >&2
  exit 1
fi
if [[ -n "$(git -C "$ROOT" status --porcelain=v1)" ]]; then
  echo "Refusing deployment from a dirty worktree" >&2
  exit 1
fi
if [[ "$DEPLOY_HEAD" != "$DEPLOY_ORIGIN_MASTER" ]]; then
  echo "Refusing deployment: HEAD does not equal origin/master" >&2
  exit 1
fi

cd "$ROOT"
uv sync --frozen

mkdir -p "$USER_UNIT_DIR"
ln -sfn "$ROOT/systemd/spx-core.service" "$USER_UNIT_DIR/spx-core.service"
ln -sfn "$ROOT/systemd/spx-spark-24h.service" "$USER_UNIT_DIR/spx-spark-24h.service"
ln -sfn "$ROOT/systemd/spx-spark-es-bar-sampler.service" "$USER_UNIT_DIR/spx-spark-es-bar-sampler.service"
ln -sfn "$ROOT/systemd/spx-spark-spx-minute-sampler.service" "$USER_UNIT_DIR/spx-spark-spx-minute-sampler.service"
ln -sfn "$ROOT/systemd/spx-spark-market-features-hot.service" "$USER_UNIT_DIR/spx-spark-market-features-hot.service"
ln -sfn "$ROOT/systemd/spx-spark-market-regime-signal.service" "$USER_UNIT_DIR/spx-spark-market-regime-signal.service"
ln -sfn "$ROOT/systemd/spx-spark-intraday-shock-hot.service" "$USER_UNIT_DIR/spx-spark-intraday-shock-hot.service"
ln -sfn "$ROOT/systemd/spx-spark-notification-delivery.service" "$USER_UNIT_DIR/spx-spark-notification-delivery.service"
ln -sfn "$ROOT/systemd/spx-spark-surface-dashboard.service" "$USER_UNIT_DIR/spx-spark-surface-dashboard.service"
ln -sfn "$ROOT/systemd/spx-spark-ibkr-stream.service" "$USER_UNIT_DIR/spx-spark-ibkr-stream.service"
ln -sfn "$ROOT/systemd/spx-spark-post-close-review.service" "$USER_UNIT_DIR/spx-spark-post-close-review.service"
ln -sfn "$ROOT/systemd/spx-spark-post-close-review.timer" "$USER_UNIT_DIR/spx-spark-post-close-review.timer"
ln -sfn "$ROOT/systemd/spx-spark-session-finalize.service" "$USER_UNIT_DIR/spx-spark-session-finalize.service"
ln -sfn "$ROOT/systemd/spx-spark-session-finalize.timer" "$USER_UNIT_DIR/spx-spark-session-finalize.timer"
ln -sfn "$ROOT/systemd/spx-spark-storage-pressure.service" "$USER_UNIT_DIR/spx-spark-storage-pressure.service"
ln -sfn "$ROOT/systemd/spx-spark-storage-pressure.timer" "$USER_UNIT_DIR/spx-spark-storage-pressure.timer"
ln -sfn "$ROOT/systemd/spx-spark-rth-daily-acceptance.service" "$USER_UNIT_DIR/spx-spark-rth-daily-acceptance.service"
ln -sfn "$ROOT/systemd/spx-spark-rth-daily-acceptance.timer" "$USER_UNIT_DIR/spx-spark-rth-daily-acceptance.timer"
ln -sfn "$ROOT/systemd/spx-spark-order-map.service" "$USER_UNIT_DIR/spx-spark-order-map.service"
ln -sfn "$ROOT/systemd/spx-spark-order-map.timer" "$USER_UNIT_DIR/spx-spark-order-map.timer"
ln -sfn "$ROOT/systemd/spx-spark-order-map-status.service" "$USER_UNIT_DIR/spx-spark-order-map-status.service"
ln -sfn "$ROOT/systemd/spx-spark-order-map-status.timer" "$USER_UNIT_DIR/spx-spark-order-map-status.timer"
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

# Pre-create the narrowly writable live-surface paths before its strict
# filesystem sandbox is established, then install its user unit idempotently.
"$ROOT/scripts/install-spxw-surface-live-service.sh"

# A clean Git tree is not enough if an older copied user unit remains active.
# Refuse the deployment when any already-installed tracked unit differs from
# the exact unit in origin/master.
for tracked_unit in "$ROOT"/systemd/spx-spark-*; do
  deployed_unit="$USER_UNIT_DIR/$(basename "$tracked_unit")"
  if [[ -e "$deployed_unit" ]] && ! cmp -s "$tracked_unit" "$deployed_unit"; then
    echo "Refusing deployment: systemd unit drift at $deployed_unit" >&2
    exit 1
  fi
done

systemctl --user daemon-reload
systemctl --user enable spx-spark-24h.service
systemctl --user enable spx-spark-es-bar-sampler.service
systemctl --user enable spx-spark-spx-minute-sampler.service
systemctl --user enable spx-spark-market-features-hot.service
systemctl --user enable spx-spark-market-regime-signal.service
systemctl --user enable spx-spark-intraday-shock-hot.service
systemctl --user enable spx-spark-notification-delivery.service
systemctl --user enable spx-spark-surface-dashboard.service
systemctl --user enable spx-spark-ibkr-stream.service
# The deterministic finalizer now owns the post-close artifact, LLM and push
# ordering from one payload. Keep the old review service for explicit manual
# compatibility, but fence its timer so a deployment cannot duplicate work.
systemctl --user disable --now spx-spark-post-close-review.timer
systemctl --user enable --now spx-spark-session-finalize.timer
systemctl --user enable --now spx-spark-storage-pressure.timer
systemctl --user enable --now spx-spark-rth-daily-acceptance.timer
systemctl --user enable --now spx-spark-order-map.timer
systemctl --user enable --now spx-spark-order-map-status.timer
systemctl --user enable spx-spark-morning-map.timer
systemctl --user enable --now spx-spark-maintenance-daily.timer
systemctl --user enable --now spx-spark-maintenance-weekly.timer
systemctl --user enable --now spx-spark-data-compact.timer
systemctl --user enable --now spx-spark-data-compact-weekend.timer
systemctl --user enable --now spx-spark-backtest-weekly.timer

echo "Installed user services:"
echo "  spx-core.service (installed but disabled pending staging/cutover)"
echo "  spx-spark-24h.service"
echo "  spx-spark-es-bar-sampler.service"
echo "  spx-spark-spx-minute-sampler.service"
echo "  spx-spark-market-features-hot.service"
echo "  spx-spark-market-regime-signal.service"
echo "  spx-spark-intraday-shock-hot.service"
echo "  spx-spark-notification-delivery.service"
echo "  spx-spark-surface-dashboard.service"
echo "  spx-spark-surface-live.service"
echo "  spx-spark-ibkr-stream.service"
echo "  spx-spark-post-close-review.service (manual compatibility only)"
echo "  spx-spark-post-close-review.timer (disabled; superseded by session finalizer)"
echo "  spx-spark-session-finalize.timer (daily 18:00 America/New_York)"
echo "  spx-spark-storage-pressure.timer (hourly; artifact-gated pressure check)"
echo "  spx-spark-rth-daily-acceptance.timer (17:30 America/New_York)"
echo "  spx-spark-order-map.timer"
echo "  spx-spark-order-map-status.timer (exchange-local GTH/RTH clock)"
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
  # Stop the legacy embedded writer before the independent canonical writer
  # starts. Only after the sampler is active may the new read-only heavy worker
  # return, so a rolling deployment cannot create a dual-writer window.
  systemctl --user stop spx-spark-market-features-hot.service
  systemctl --user restart spx-spark-ibkr-stream.service
  systemctl --user restart spx-spark-es-bar-sampler.service
  systemctl --user restart spx-spark-spx-minute-sampler.service
  SAMPLER_READY_TIMEOUT_SECONDS="${SPX_SPARK_ES_BAR_READY_TIMEOUT_SECONDS:-45}"
  if [[ ! "$SAMPLER_READY_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid SPX_SPARK_ES_BAR_READY_TIMEOUT_SECONDS: $SAMPLER_READY_TIMEOUT_SECONDS" >&2
    exit 1
  fi
  SAMPLER_READY_DEADLINE=$((SECONDS + SAMPLER_READY_TIMEOUT_SECONDS))
  until "$ROOT/scripts/run-es-bar-sampler.sh" --check-ready >/dev/null 2>&1; do
    if ! systemctl --user is-active --quiet spx-spark-es-bar-sampler.service; then
      echo "Refusing to restart market features: ES bar sampler stopped before readiness" >&2
      exit 1
    fi
    if (( SECONDS >= SAMPLER_READY_DEADLINE )); then
      echo "Refusing to restart market features: ES bar sampler did not publish a fresh fenced observation within ${SAMPLER_READY_TIMEOUT_SECONDS}s" >&2
      "$ROOT/scripts/run-es-bar-sampler.sh" --check-ready || true
      exit 1
    fi
    sleep 1
  done
  # Remove both hot paths from the shared scheduler before starting their sole owners.
  systemctl --user restart spx-spark-24h.service
  systemctl --user restart spx-spark-market-features-hot.service
  systemctl --user restart spx-spark-market-regime-signal.service
  systemctl --user restart spx-spark-intraday-shock-hot.service
  systemctl --user restart spx-spark-notification-delivery.service
  systemctl --user restart spx-spark-surface-dashboard.service
  systemctl --user restart spx-spark-session-finalize.timer
  systemctl --user restart spx-spark-storage-pressure.timer
  systemctl --user restart spx-spark-rth-daily-acceptance.timer
  systemctl --user restart spx-spark-order-map.timer
  systemctl --user restart spx-spark-order-map-status.timer
  "$ROOT/scripts/install-spxw-surface-live-service.sh" --now
  systemctl --user status spx-spark-24h.service spx-spark-es-bar-sampler.service spx-spark-spx-minute-sampler.service spx-spark-market-features-hot.service spx-spark-market-regime-signal.service spx-spark-intraday-shock-hot.service spx-spark-notification-delivery.service spx-spark-surface-dashboard.service spx-spark-surface-live.service spx-spark-ibkr-stream.service spx-spark-session-finalize.timer spx-spark-storage-pressure.timer spx-spark-rth-daily-acceptance.timer spx-spark-order-map.timer spx-spark-order-map-status.timer --no-pager
fi
