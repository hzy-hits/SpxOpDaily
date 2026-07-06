#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

enabled="${OPENCLAW_WEIXIN_KEEPALIVE_ENABLED:-true}"
if [[ "$enabled" == "false" || "$enabled" == "0" ]]; then
  echo '{"ok":true,"skipped":true,"reason":"disabled"}'
  exit 0
fi

channel="${ALERT_NOTIFY_OPENCLAW_CHANNEL:-openclaw-weixin}"
account="${ALERT_NOTIFY_OPENCLAW_ACCOUNT:-}"
target="${ALERT_NOTIFY_OPENCLAW_TARGET:-}"
message="${OPENCLAW_WEIXIN_KEEPALIVE_MESSAGE:-SPX Spark 通道保活}"
state_dir="${OPENCLAW_STATE_DIR:-$HOME/.openclaw}"
state_path="${OPENCLAW_WEIXIN_KEEPALIVE_STATE_PATH:-/srv/data/spx-spark/data/latest/openclaw_weixin_keepalive.json}"

# Previous run's status, used to send the Bark reminder only on the
# healthy -> broken transition instead of every 30 minutes.
prev_ok=""
if [[ -f "$state_path" ]]; then
  prev_ok="$(jq -r '.ok // empty' "$state_path" 2>/dev/null || true)"
fi

send_bark_reminder() {
  local reason="$1"
  local bark_enabled="${ALERT_NOTIFY_BARK_ENABLED:-false}"
  local bark_url="${ALERT_NOTIFY_BARK_URL:-}"
  if [[ "$bark_enabled" != "true" && "$bark_enabled" != "1" ]]; then
    return 0
  fi
  if [[ -z "$bark_url" ]]; then
    return 0
  fi
  if [[ "$prev_ok" == "false" ]]; then
    return 0
  fi
  curl -fsS -m 10 -X POST "$bark_url" \
    -H 'Content-Type: application/json; charset=utf-8' \
    -d "$(jq -n --arg reason "$reason" '{
      title: "SPX Spark 微信通道失效",
      body: ("微信 contextToken 失效（" + $reason + "）。请给微信机器人随便发一条消息重新激活推送。"),
      group: "spx-spark",
      level: "timeSensitive"
    }')" >/dev/null 2>&1 || true
}

if [[ -z "$account" && -f "$state_dir/openclaw-weixin/accounts.json" ]]; then
  account="$(jq -r '.[0] // empty' "$state_dir/openclaw-weixin/accounts.json")"
fi

if [[ -z "$target" && -n "$account" ]]; then
  account_file="$state_dir/openclaw-weixin/accounts/${account}.json"
  if [[ -f "$account_file" ]]; then
    target="$(jq -r '.userId // empty' "$account_file")"
  fi
fi

if [[ -z "$target" ]]; then
  echo '{"ok":false,"skipped":true,"reason":"missing_target"}' >&2
  exit 1
fi

context_tokens_file="$state_dir/openclaw-weixin/accounts/${account}.context-tokens.json"
has_context_token="false"
if [[ -f "$context_tokens_file" ]]; then
  if jq -e --arg target "$target" '.[$target] != null and (.[$target] | length) > 0' "$context_tokens_file" >/dev/null 2>&1; then
    has_context_token="true"
  fi
fi

if [[ "$has_context_token" != "true" ]]; then
  mkdir -p "$(dirname "$state_path")"
  jq -n \
    --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg target "$target" \
    --arg account "$account" \
    '{ok:false,skipped:true,reason:"contextToken_missing",target:$target,account:$account,at:$at}' \
    | tee "$state_path"
  echo "OpenClaw Weixin keepalive skipped: no contextToken for $target" >&2
  echo "Send any message to the OpenClaw Weixin bot once to seed the session." >&2
  send_bark_reminder "contextToken 缺失"
  exit 0
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

set +e
output="$("${args[@]}" 2>&1)"
exit_code=$?
set -e

ok="false"
error=""
message_id=""
if [[ $exit_code -eq 0 ]]; then
  if echo "$output" | jq -e '.payload.result.messageId // .messageId' >/dev/null 2>&1; then
    ok="true"
    message_id="$(echo "$output" | jq -r '.payload.result.messageId // .messageId // empty')"
  else
    error="unexpected_openclaw_response"
  fi
else
  error="$(echo "$output" | tr '\n' ' ' | sed 's/  */ /g')"
fi

mkdir -p "$(dirname "$state_path")"
jq -n \
  --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg target "$target" \
  --arg account "$account" \
  --argjson ok "$([[ "$ok" == "true" ]] && echo true || echo false)" \
  --arg error "$error" \
  --arg message_id "$message_id" \
  --arg has_context_token "$has_context_token" \
  '{ok:$ok,skipped:false,target:$target,account:$account,has_context_token:($has_context_token=="true"),message_id:$message_id,error:$error,at:$at}' \
  | tee "$state_path"

if [[ "$ok" == "true" ]]; then
  # Channel proven alive; flush any missed-message digest (best effort).
  uv run spx-spark-weixin-digest || true
fi

if [[ "$ok" != "true" ]]; then
  echo "OpenClaw Weixin keepalive failed: $error" >&2
  send_bark_reminder "保活发送失败: $error"
  exit 1
fi
