#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

channel="${ALERT_NOTIFY_OPENCLAW_CHANNEL:-openclaw-weixin}"
account="${ALERT_NOTIFY_OPENCLAW_ACCOUNT:-}"
target="${ALERT_NOTIFY_OPENCLAW_TARGET:-}"
dry_run="${ALERT_NOTIFY_OPENCLAW_DRY_RUN:-true}"
message="${1:-SPX Spark test alert from OpenClaw notifier.}"

if [[ -z "$account" && -f "$HOME/.openclaw/openclaw-weixin/accounts.json" ]]; then
  account="$(jq -r '.[0] // empty' "$HOME/.openclaw/openclaw-weixin/accounts.json")"
fi

if [[ -z "$target" && -n "$account" ]]; then
  account_file="$HOME/.openclaw/openclaw-weixin/accounts/${account}.json"
  if [[ -f "$account_file" ]]; then
    target="$(jq -r '.userId // empty' "$account_file")"
  fi
fi

if [[ -z "$target" ]]; then
  echo "Missing ALERT_NOTIFY_OPENCLAW_TARGET and no default Weixin userId found." >&2
  exit 2
fi

args=(
  openclaw message send
  --channel "$channel"
  --target "$target"
  --message "$message"
  --json
)

if [[ -n "$account" ]]; then
  args+=(--account "$account")
fi

if [[ "$dry_run" != "false" && "$dry_run" != "0" ]]; then
  args+=(--dry-run)
fi

exec "${args[@]}"
