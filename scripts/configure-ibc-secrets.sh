#!/usr/bin/env bash
set -euo pipefail

IBC_DIR="${IBC_INSTALL_DIR:-/home/ubuntu/apps/ibc}"
CONFIG_DIR="${IBC_CONFIG_DIR:-/srv/data/spx-spark/runtime/ibc}"
CONFIG_PATH="${IBC_CONFIG:-$CONFIG_DIR/config.ini}"
SOURCE_CONFIG="$IBC_DIR/config.ini"

if [[ ! -f "$SOURCE_CONFIG" ]]; then
  echo "IBC default config not found: $SOURCE_CONFIG" >&2
  echo "Run scripts/install-ibc.sh first." >&2
  exit 1
fi

mkdir -p "$CONFIG_DIR"
chmod 700 "$CONFIG_DIR"

PY_SCRIPT="$(mktemp)"
trap 'rm -f "$PY_SCRIPT"' EXIT
cat > "$PY_SCRIPT" <<'PY'
from __future__ import annotations

import getpass
import os
import re
import stat
import sys
from pathlib import Path


source = Path(sys.argv[1])
target = Path(sys.argv[2])


def prompt(default: str, label: str) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    return value or default


login_id = input("IBKR username: ").strip()
if not login_id:
    raise SystemExit("IBKR username is required")

password = getpass.getpass("IBKR password: ")
if not password:
    raise SystemExit("IBKR password is required")

mode = prompt("live", "Trading mode").lower()
if mode not in {"live", "paper"}:
    raise SystemExit("Trading mode must be live or paper")

default_port = "4001" if mode == "live" else "4002"
api_port = prompt(default_port, "Gateway API port")
if not api_port.isdigit():
    raise SystemExit("Gateway API port must be an integer")

read_only_login = prompt("no", "Read-only login (IB Gateway does not support this; Read-only API stays yes)").lower()
if read_only_login not in {"yes", "no"}:
    raise SystemExit("Read-only login must be yes or no")

second_factor_device = input("Second factor device name, if IBKR shows multiple devices [blank]: ").strip()
relogin_after_2fa_timeout = prompt("yes", "Relogin after IBKR Mobile 2FA timeout").lower()
if relogin_after_2fa_timeout not in {"yes", "no"}:
    raise SystemExit("Relogin after 2FA timeout must be yes or no")

existing_session_action = prompt("secondary", "Existing session action").lower()
if existing_session_action not in {"secondary", "primary", "primaryoverride", "manual"}:
    raise SystemExit("Existing session action must be secondary, primary, primaryoverride, or manual")

command_server_port = prompt("7462", "IBC command server port")
if not command_server_port.isdigit():
    raise SystemExit("IBC command server port must be an integer")

text = source.read_text(encoding="utf-8")


def set_key(payload: str, key: str, value: str) -> str:
    line = f"{key}={value}"
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
    if pattern.search(payload):
        return pattern.sub(line, payload, count=1)
    return payload.rstrip() + "\n\n" + line + "\n"


updates = {
    "IbLoginId": login_id,
    "IbPassword": password,
    "TradingMode": mode,
    "SecondFactorDevice": second_factor_device,
    "ReloginAfterSecondFactorAuthenticationTimeout": relogin_after_2fa_timeout,
    "ExitAfterSecondFactorAuthenticationTimeout": relogin_after_2fa_timeout,
    "AcceptNonBrokerageAccountWarning": "yes",
    "ExistingSessionDetectedAction": existing_session_action,
    "OverrideTwsApiPort": api_port,
    "ReadOnlyLogin": read_only_login,
    "ReadOnlyApi": "yes",
    "TrustedTwsApiClientIPs": "127.0.0.1",
    "AcceptIncomingConnectionAction": "accept",
    "CommandServerPort": command_server_port,
    "ControlFrom": "127.0.0.1",
    "BindAddress": "127.0.0.1",
    "DismissPasswordExpiryWarning": "no",
}

for key, value in updates.items():
    text = set_key(text, key, value)

target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(text, encoding="utf-8")
os.chmod(target, stat.S_IRUSR | stat.S_IWUSR)

print(f"Wrote {target}")
print("Permissions: 600")
print(f"TradingMode={mode}")
print(f"OverrideTwsApiPort={api_port}")
print(f"ReadOnlyLogin={read_only_login}")
print(f"ExistingSessionDetectedAction={existing_session_action}")
PY

python3 "$PY_SCRIPT" "$SOURCE_CONFIG" "$CONFIG_PATH"
