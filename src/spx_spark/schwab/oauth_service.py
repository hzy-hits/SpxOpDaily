from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import signal
import sys
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qs, urljoin, urlsplit
from urllib.request import ProxyHandler, build_opener

from schwab.auth import AuthContext, get_auth_context

from spx_spark.config import SchwabSettings, SchwabStreamSettings, StorageSettings
from spx_spark.schwab.auth_storage import (
    AtomicJsonFile,
    ExclusiveLockUnavailable,
    token_owner_lock_path,
)
from spx_spark.schwab.gateway import SchwabSessionManager


class OAuthCallbackError(RuntimeError):
    def __init__(self, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class PendingAuthorization:
    callback_url: str
    authorization_url: str
    state: str
    created_at: float
    expires_at: float

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PendingAuthorization":
        return cls(
            callback_url=str(raw["callback_url"]),
            authorization_url=str(raw["authorization_url"]),
            state=str(raw["state"]),
            created_at=float(raw["created_at"]),
            expires_at=float(raw["expires_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "callback_url": self.callback_url,
            "authorization_url": self.authorization_url,
            "state": self.state,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
        }

    def auth_context(self) -> AuthContext:
        return AuthContext(
            callback_url=self.callback_url,
            authorization_url=self.authorization_url,
            state=self.state,
        )


class OAuthCoordinator:
    def __init__(
        self,
        settings: SchwabSettings,
        manager: SchwabSessionManager,
        *,
        now: Callable[[], float] = time.time,
        auth_context_factory: Callable[..., AuthContext] = get_auth_context,
    ) -> None:
        self.settings = settings
        self.manager = manager
        self.state_store = AtomicJsonFile(settings.oauth_state_file)
        self.now = now
        self.auth_context_factory = auth_context_factory

    def authorize(self) -> PendingAuthorization:
        validate_oauth_settings(self.settings)
        context = self.auth_context_factory(
            self.settings.app_key,
            self.settings.callback_url,
        )
        created_at = self.now()
        pending = PendingAuthorization(
            callback_url=context.callback_url,
            authorization_url=context.authorization_url,
            state=context.state,
            created_at=created_at,
            expires_at=created_at + self.settings.oauth_state_ttl_seconds,
        )
        self.state_store.write(pending.to_dict())
        return pending

    def complete(self, raw_query: str) -> None:
        if len(raw_query) > 16_384:
            raise OAuthCallbackError("Callback query is too large", status=414)

        try:
            query = parse_qs(raw_query, keep_blank_values=True, max_num_fields=20)
        except ValueError as exc:
            raise OAuthCallbackError("Invalid callback query") from exc
        received_states = query.get("state", [])
        if len(received_states) != 1 or not received_states[0]:
            raise OAuthCallbackError("Missing OAuth state")

        with self.state_store.locked():
            if not self.state_store.exists:
                raise OAuthCallbackError("No pending Schwab authorization")
            try:
                pending = PendingAuthorization.from_dict(self.state_store.read_unlocked())
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                self.state_store.delete_unlocked()
                raise OAuthCallbackError("Pending authorization state is invalid") from exc

            if self.now() > pending.expires_at:
                self.state_store.delete_unlocked()
                raise OAuthCallbackError("Schwab authorization has expired")
            if pending.callback_url != self.settings.callback_url:
                raise OAuthCallbackError("Configured callback URL changed during authorization")
            received_state = received_states[0]
            if (
                len(received_state) > 256
                or not received_state.isascii()
                or not pending.state.isascii()
                or not hmac.compare_digest(received_state, pending.state)
            ):
                raise OAuthCallbackError("OAuth state mismatch")

            # Consume the state before exchanging the one-time code. A failed
            # exchange requires a new authorize command and cannot be replayed.
            self.state_store.delete_unlocked()

        if query.get("error"):
            raise OAuthCallbackError("Schwab authorization was not approved")
        codes = query.get("code", [])
        if len(codes) != 1 or not codes[0]:
            raise OAuthCallbackError("Missing Schwab authorization code")

        received_url = f"{pending.callback_url}?{raw_query}"
        try:
            self.manager.install_callback_token(
                auth_context=pending.auth_context(),
                received_url=received_url,
            )
        except Exception:  # noqa: BLE001 - never retain an exception containing the callback URL
            raise OAuthCallbackError(
                "Schwab token exchange failed; generate a new authorization URL",
                status=502,
            ) from None

    def pending(self) -> PendingAuthorization | None:
        if not self.state_store.exists:
            return None
        try:
            pending = PendingAuthorization.from_dict(self.state_store.read())
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
        if self.now() > pending.expires_at:
            return None
        return pending


def validate_oauth_settings(settings: SchwabSettings) -> None:
    missing = [
        name
        for name, value in (
            ("SCHWAB_APP_KEY", settings.app_key),
            ("SCHWAB_APP_SECRET", settings.app_secret),
            ("SCHWAB_CALLBACK_URL", settings.callback_url),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing Schwab OAuth settings: {', '.join(missing)}")
    parsed = urlsplit(settings.callback_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("SCHWAB_CALLBACK_URL must be an absolute HTTPS URL")
    if parsed.query or parsed.fragment:
        raise ValueError("SCHWAB_CALLBACK_URL cannot contain a query or fragment")
    api_url = urlsplit(settings.api_base_url)
    if (
        api_url.scheme != "https"
        or api_url.hostname != "api.schwabapi.com"
        or api_url.username
        or api_url.password
        or api_url.path not in {"", "/"}
        or api_url.query
        or api_url.fragment
    ):
        raise ValueError("SCHWAB_API_BASE_URL must be https://api.schwabapi.com")
    if settings.oauth_state_ttl_seconds <= 0:
        raise ValueError("SCHWAB_OAUTH_STATE_TTL_SECONDS must be positive")
    for name, port in (
        ("SCHWAB_OAUTH_BIND_PORT", settings.oauth_bind_port),
        ("SCHWAB_GATEWAY_BIND_PORT", settings.gateway_bind_port),
    ):
        if port < 1 or port > 65535:
            raise ValueError(f"{name} must be between 1 and 65535")
    if settings.oauth_bind_port == settings.gateway_bind_port:
        raise ValueError("Schwab callback and gateway must use different ports")
    validate_oauth_paths(settings)
    require_loopback(settings.oauth_bind_host, "SCHWAB_OAUTH_BIND_HOST")
    require_loopback(settings.gateway_bind_host, "SCHWAB_GATEWAY_BIND_HOST")


def require_loopback(host: str, setting_name: str) -> None:
    if host.lower() == "localhost":
        return
    try:
        address = ip_address(host)
    except ValueError as exc:
        raise ValueError(f"{setting_name} must be an IPv4 loopback address") from exc
    if address.version != 4 or not address.is_loopback:
        raise ValueError(f"{setting_name} must be an IPv4 loopback address")


def validate_oauth_paths(settings: SchwabSettings) -> None:
    token_store = AtomicJsonFile(settings.token_file)
    state_store = AtomicJsonFile(settings.oauth_state_file)
    paths = [
        token_store.path,
        token_store.lock_path,
        token_owner_lock_path(settings.token_file),
        state_store.path,
        state_store.lock_path,
    ]
    resolved = [Path(path).expanduser().resolve(strict=False) for path in paths]
    if len(set(resolved)) != len(resolved):
        raise ValueError("Schwab token, state, and lock paths must all be distinct")


def is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def status_payload(coordinator: OAuthCoordinator) -> dict[str, Any]:
    pending = coordinator.pending()
    token_created_at: int | None = None
    token_age_seconds: int | None = None
    if coordinator.manager.token_store.exists:
        try:
            raw_token = coordinator.manager.token_store.read()
            token_created_at = int(raw_token["creation_timestamp"])
            token_age_seconds = max(0, int(coordinator.now()) - token_created_at)
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            pass
    return {
        "callback_url": coordinator.settings.callback_url,
        "gateway_url": (
            f"http://{coordinator.settings.gateway_bind_host}:"
            f"{coordinator.settings.gateway_bind_port}"
        ),
        "oauth_pending": pending is not None,
        "oauth_expires_at": pending.expires_at if pending else None,
        "token_present": coordinator.manager.token_store.exists,
        "token_created_at": token_created_at,
        "token_age_seconds": token_age_seconds,
        "gateway_ready": probe_gateway_ready(coordinator.settings),
    }


def probe_gateway_ready(settings: SchwabSettings) -> bool:
    base_url = settings.gateway_url or (
        f"http://{settings.gateway_bind_host}:{settings.gateway_bind_port}"
    )
    try:
        opener = build_opener(ProxyHandler({}))
        with opener.open(
            urljoin(base_url.rstrip("/") + "/", "healthz"),
            timeout=1.0,
        ) as response:
            payload = json.load(response)
    except (OSError, URLError, ValueError, json.JSONDecodeError):
        return False
    return bool(isinstance(payload, dict) and payload.get("ready") is True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Schwab OAuth callback and data gateway.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("authorize", help="Create a one-time Schwab login URL.")
    subparsers.add_parser("serve", help="Serve the Cloudflare callback and localhost gateway.")
    subparsers.add_parser("status", help="Show redacted OAuth and gateway status.")
    return parser.parse_args(argv)


def initialize_optional_stream_runtime(
    manager: SchwabSessionManager,
) -> tuple[Any | None, str]:
    try:
        storage_settings = StorageSettings.from_env()
        stream_settings = SchwabStreamSettings.from_env(
            data_root=storage_settings.data_root
        )
        if stream_settings.mode == "off":
            return None, stream_settings.mode
        from spx_spark.schwab.stream_runtime import SchwabStreamRuntime

        return (
            SchwabStreamRuntime(
                manager,
                stream_settings,
                storage_settings,
            ),
            stream_settings.mode,
        )
    except Exception as exc:  # noqa: BLE001 - auxiliary failure cannot stop OAuth/gateway
        print(
            json.dumps(
                {
                    "event": "schwab_stream_initialization_failed",
                    "ok": False,
                    "error_type": type(exc).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        return None, "disabled_error"


def serve_fastapi(
    settings: SchwabSettings,
    coordinator: OAuthCoordinator,
    *,
    auxiliary_runner: Callable[[], None] | None = None,
    auxiliary_close: Callable[[], None] | None = None,
    stream_health: Callable[[], dict[str, Any]] | None = None,
) -> None:
    import uvicorn

    from spx_spark.web.schwab_api import create_app

    apps = (
        create_app(coordinator),
        create_app(coordinator, gateway=True, stream_health=stream_health),
    )
    bindings = (
        (settings.oauth_bind_host, settings.oauth_bind_port),
        (settings.gateway_bind_host, settings.gateway_bind_port),
    )
    servers = [
        uvicorn.Server(uvicorn.Config(
            app, host=host, port=port, access_log=False, log_config=None))
        for app, (host, port) in zip(apps, bindings)
    ]
    for server in servers:
        server.capture_signals = lambda: nullcontext()  # type: ignore[method-assign]
    auxiliary = (
        threading.Thread(target=auxiliary_runner, name="schwab-stream-supervisor", daemon=True)
        if auxiliary_runner else None
    )
    if auxiliary is not None:
        auxiliary.start()

    async def supervise() -> None:
        loop = asyncio.get_running_loop()

        def stop() -> None:
            for server in servers:
                server.should_exit = True

        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop)
        tasks = [asyncio.create_task(server.serve()) for server in servers]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        stop()
        await asyncio.gather(*tasks)

    try:
        asyncio.run(supervise())
    finally:
        if auxiliary_close is not None:
            auxiliary_close()
        if auxiliary is not None:
            auxiliary.join(timeout=5)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = SchwabSettings.from_env()
    validate_oauth_settings(settings)
    manager = SchwabSessionManager(settings)
    coordinator = OAuthCoordinator(settings, manager)

    if args.command == "authorize":
        pending = coordinator.authorize()
        print("Open this URL in your local browser:")
        print(pending.authorization_url)
        print(
            f"Authorization expires in {settings.oauth_state_ttl_seconds} seconds; "
            "the callback state is single-use."
        )
        return 0
    if args.command == "status":
        print(json.dumps(status_payload(coordinator), indent=2, sort_keys=True))
        return 0

    try:
        with manager.owner_lock.held():
            manager.load()
            stream_runtime, stream_mode = initialize_optional_stream_runtime(manager)
            print(
                json.dumps(
                    {
                        "event": "schwab_oauth_service_started",
                        "callback": f"{settings.oauth_bind_host}:{settings.oauth_bind_port}",
                        "gateway": f"{settings.gateway_bind_host}:{settings.gateway_bind_port}",
                        "token_present": manager.token_store.exists,
                        "stream_mode": stream_mode,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            try:
                serve_fastapi(
                    settings,
                    coordinator,
                    auxiliary_runner=(
                        stream_runtime.run_forever if stream_runtime is not None else None
                    ),
                    auxiliary_close=(
                        stream_runtime.close if stream_runtime is not None else None
                    ),
                    stream_health=(
                        stream_runtime.health if stream_runtime is not None else None
                    ),
                )
            except KeyboardInterrupt:
                pass
    except ExclusiveLockUnavailable:
        print(
            "Another Schwab gateway or manual token flow owns this token file.",
            file=sys.stderr,
        )
        return 3
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
