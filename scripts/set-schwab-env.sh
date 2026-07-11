#!/usr/bin/env bash
set -euo pipefail

key="${1:-}"
case "$key" in
  SCHWAB_APP_KEY|SCHWAB_APP_SECRET|SCHWAB_CALLBACK_URL|SCHWAB_TOKEN_FILE|\
  SCHWAB_OAUTH_STATE_FILE|SCHWAB_OAUTH_STATE_TTL_SECONDS|\
  SCHWAB_OAUTH_BIND_HOST|SCHWAB_OAUTH_BIND_PORT|\
  SCHWAB_GATEWAY_BIND_HOST|SCHWAB_GATEWAY_BIND_PORT|SCHWAB_GATEWAY_URL|\
  SCHWAB_HTTP_REQUESTS_PER_MINUTE|SCHWAB_HTTP_MAX_RETRIES|\
  SCHWAB_HTTP_RETRY_BASE_SECONDS|SCHWAB_HTTP_RETRY_MAX_SECONDS|\
  SCHWAB_HTTP_RETRY_AFTER_MAX_SECONDS|\
  SCHWAB_ACCESS_TOKEN)
    ;;
  *)
    echo "Unsupported Schwab environment key" >&2
    exit 2
    ;;
esac

value=""
IFS= read -r value || [[ -n "$value" ]]
if [[ -z "$value" && "$key" != "SCHWAB_ACCESS_TOKEN" ]]; then
  echo "Refusing an empty value for $key" >&2
  exit 2
fi
extra=""
if IFS= read -r extra || [[ -n "$extra" ]]; then
  echo "Environment value must contain exactly one line" >&2
  exit 2
fi

env_file="${SPX_ENV_FILE:-/home/ubuntu/spx-spark/.env}"
mkdir -p "$(dirname "$env_file")"
umask 077
temp_file="$(mktemp "${env_file}.tmp.XXXXXX")"
cleanup() {
  rm -f "$temp_file"
}
trap cleanup EXIT

if [[ -f "$env_file" ]]; then
  awk -v key="$key" 'index($0, key "=") != 1 { print }' "$env_file" >"$temp_file"
fi
printf '%s=%s\n' "$key" "$value" >>"$temp_file"
chmod 600 "$temp_file"
mv -f "$temp_file" "$env_file"
trap - EXIT
