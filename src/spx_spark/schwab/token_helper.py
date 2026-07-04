from __future__ import annotations

import argparse
import os
from getpass import getpass
from pathlib import Path

from spx_spark.config import SchwabSettings, load_dotenv


def env_or_prompt(name: str, prompt: str, *, secret: bool = False) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    try:
        if secret:
            return getpass(prompt).strip()
        return input(prompt).strip()
    except EOFError as exc:
        raise SystemExit(
            f"Cannot read {name} from interactive input. "
            f"Run this script in an interactive SSH terminal or set {name} in .env."
        ) from exc


def prompt_with_default(prompt: str, default: str) -> str:
    try:
        value = input(f"{prompt} [{default}]: ").strip()
    except EOFError as exc:
        raise SystemExit(
            "Cannot read callback URL from interactive input. "
            "Run this script in an interactive SSH terminal or set SCHWAB_CALLBACK_URL in .env."
        ) from exc
    return value or default


def confirm_overwrite(path: Path) -> None:
    if not path.exists():
        return
    force = os.getenv("SCHWAB_TOKEN_OVERWRITE", "").strip().lower()
    if force in {"1", "true", "yes", "y", "on"}:
        return
    try:
        answer = input(f"Token file already exists at {path}. Overwrite? [y/N]: ").strip().lower()
    except EOFError as exc:
        raise SystemExit(
            f"Token file already exists at {path}. "
            "Set SCHWAB_TOKEN_OVERWRITE=true to replace it non-interactively."
        ) from exc
    if answer not in {"y", "yes"}:
        raise SystemExit("Aborted without changing the token file.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a Schwab OAuth token on a headless host.")
    parser.add_argument("--token-path", help="Override SCHWAB_TOKEN_FILE.")
    parser.add_argument("--callback-url", help="Override SCHWAB_CALLBACK_URL.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved paths/settings and exit before OAuth.",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()
    settings = SchwabSettings.from_env()
    token_path = Path(args.token_path or settings.token_file).expanduser()
    default_callback = (
        args.callback_url or os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182")
    ).strip()

    if args.dry_run:
        print(f"Token path:   {token_path}")
        print(f"Callback URL: {default_callback}")
        print("Dry run only; OAuth not started.")
        return 0

    token_path.parent.mkdir(parents=True, exist_ok=True)
    api_key = env_or_prompt("SCHWAB_APP_KEY", "Schwab App Key: ")
    app_secret = env_or_prompt("SCHWAB_APP_SECRET", "Schwab App Secret: ", secret=True)
    callback_url = args.callback_url or prompt_with_default("Callback URL", default_callback)

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

    try:
        from schwab.auth import client_from_manual_flow
    except ImportError as exc:
        raise SystemExit("Missing dependency: schwab-py. Run through scripts/create-schwab-token.sh.") from exc

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
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
