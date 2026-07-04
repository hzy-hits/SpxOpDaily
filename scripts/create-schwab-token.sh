#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

uv run --with schwab-py python - <<'PY'
from __future__ import annotations

import os
from getpass import getpass
from pathlib import Path

from schwab.auth import client_from_manual_flow

from spx_spark.config import SchwabSettings, load_dotenv


def env_or_prompt(name: str, prompt: str, *, secret: bool = False) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    if secret:
        return getpass(prompt).strip()
    return input(prompt).strip()


def confirm_overwrite(path: Path) -> None:
    if not path.exists():
        return
    force = os.getenv("SCHWAB_TOKEN_OVERWRITE", "").strip().lower()
    if force in {"1", "true", "yes", "y", "on"}:
        return
    answer = input(f"Token file already exists at {path}. Overwrite? [y/N]: ").strip().lower()
    if answer not in {"y", "yes"}:
        raise SystemExit("Aborted without changing the token file.")


def main() -> None:
    load_dotenv()
    settings = SchwabSettings.from_env()
    token_path = Path(settings.token_file).expanduser()
    token_path.parent.mkdir(parents=True, exist_ok=True)

    api_key = env_or_prompt("SCHWAB_APP_KEY", "Schwab App Key: ")
    app_secret = env_or_prompt("SCHWAB_APP_SECRET", "Schwab App Secret: ", secret=True)
    default_callback = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182").strip()
    callback_url = input(f"Callback URL [{default_callback}]: ").strip() or default_callback

    if not api_key or not app_secret:
        raise SystemExit("Missing Schwab App Key or App Secret.")

    confirm_overwrite(token_path)

    print()
    print("Schwab manual OAuth flow")
    print(f"- Callback URL: {callback_url}")
    print(f"- Token path:   {token_path}")
    print()
    print("Next steps:")
    print("1. Copy the login URL printed below into your local browser.")
    print("2. Log in to Schwab and approve access.")
    print("3. Your browser may fail to connect to 127.0.0.1. That is OK.")
    print("4. Copy the full final browser address back into this SSH terminal.")
    print()

    client_from_manual_flow(
        api_key=api_key,
        app_secret=app_secret,
        callback_url=callback_url,
        token_path=str(token_path),
        enforce_enums=False,
    )
    token_path.chmod(0o600)
    print()
    print(f"Wrote {token_path}")
    print("Now run: scripts/run-schwab-verifier.sh --skip-chains")


if __name__ == "__main__":
    main()
PY
