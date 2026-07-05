#!/usr/bin/env bash
set -euo pipefail

if (($#)); then
  PORTS=("$@")
else
  PORTS=(4000 4001 4002 7496 7497)
fi

add_rule() {
  local tool="$1"
  local port="$2"

  if ! command -v "$tool" >/dev/null 2>&1; then
    return
  fi

  if sudo "$tool" -C INPUT ! -i lo -p tcp --dport "$port" -j DROP 2>/dev/null; then
    echo "$tool already blocks non-loopback IBKR API port $port"
    return
  fi

  sudo "$tool" -I INPUT 1 ! -i lo -p tcp --dport "$port" -j DROP
  echo "$tool blocks non-loopback IBKR API port $port"
}

for port in "${PORTS[@]}"; do
  add_rule iptables "$port"
  add_rule ip6tables "$port"
done
