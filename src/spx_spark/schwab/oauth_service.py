from __future__ import annotations

import argparse
import hmac
import json
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from ipaddress import ip_address
from pathlib import Path
from types import TracebackType
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qs, parse_qsl, urljoin, urlsplit
from urllib.request import ProxyHandler, build_opener

from schwab.auth import AuthContext, get_auth_context

from spx_spark.config import SchwabSettings, SchwabStreamSettings, StorageSettings
from spx_spark.schwab.auth_storage import (
    AtomicJsonFile,
    ExclusiveLockUnavailable,
    token_owner_lock_path,
)
from spx_spark.schwab.gateway import (
    ALLOWED_MARKET_DATA_PATHS,
    SchwabGatewayRequestError,
    SchwabGatewayUnavailable,
    SchwabSessionManager,
)


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


def callback_handler_factory(
    coordinator: OAuthCoordinator,
) -> type[BaseHTTPRequestHandler]:
    callback_path = urlsplit(coordinator.settings.callback_url).path or "/"

    class CallbackHandler(BaseHTTPRequestHandler):
        server_version = "SPXSparkSchwabOAuth/1"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            target = urlsplit(self.path)
            if target.path == "/healthz":
                self._send_json(200, {"ok": True})
                return
            if target.path != callback_path:
                self._send_json(404, {"ok": False, "error": "not_found"})
                return
            try:
                coordinator.complete(target.query)
            except OAuthCallbackError as exc:
                self._send_html(exc.status, "Schwab authorization failed", str(exc))
                print(
                    json.dumps(
                        {"event": "schwab_oauth_callback", "ok": False, "status": exc.status}
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                return

            self._send_html(
                200,
                "Schwab authorization complete",
                "The token was stored on Oracle. You may close this tab.",
            )
            print(
                json.dumps({"event": "schwab_oauth_callback", "ok": True, "status": 200}),
                flush=True,
            )

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            self._send_json(405, {"ok": False, "error": "method_not_allowed"})

        def log_message(self, format: str, *args: Any) -> None:
            del format, args
            # BaseHTTPRequestHandler logs the full request target, including
            # OAuth code and state. Structured status events above are enough.

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self._send(status, "application/json; charset=utf-8", body)

        def _send_html(self, status: int, title: str, message: str) -> None:
            body = (
                "<!doctype html><html><head><meta charset='utf-8'>"
                f"<title>{title}</title></head><body><h1>{title}</h1>"
                f"<p>{message}</p></body></html>"
            ).encode("utf-8")
            self._send(status, "text/html; charset=utf-8", body)

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Pragma", "no-cache")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'none'; base-uri 'none'; "
                    "frame-ancestors 'none'; form-action 'none'",
                )
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                return

    return CallbackHandler


def gateway_handler_factory(
    manager: SchwabSessionManager,
    stream_health: Callable[[], dict[str, Any]] | None = None,
) -> type[BaseHTTPRequestHandler]:
    class GatewayHandler(BaseHTTPRequestHandler):
        server_version = "SPXSparkSchwabGateway/1"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if len(self.path) > 65_536:
                self._send_json(414, {"ok": False, "error": "request_target_too_large"})
                return
            host = urlsplit(f"//{self.headers.get('Host', '')}").hostname or ""
            if not is_loopback_host(host):
                self._send_json(403, {"ok": False, "error": "loopback_host_required"})
                return
            target = urlsplit(self.path)
            if target.path == "/livez":
                self._send_json(200, {"ok": True})
                return
            if target.path == "/healthz":
                health = manager.health()
                payload = health.to_dict()
                if stream_health is not None:
                    payload["stream"] = stream_health()
                self._send_json(200 if health.ready else 503, payload)
                return
            if target.path not in ALLOWED_MARKET_DATA_PATHS:
                self._send_json(404, {"ok": False, "error": "not_found"})
                return
            try:
                params = parse_qsl(target.query, keep_blank_values=True, max_num_fields=100)
                response = manager.request(target.path, params)
            except SchwabGatewayUnavailable:
                self._send_json(503, {"ok": False, "error": "schwab_auth_not_ready"})
                return
            except SchwabGatewayRequestError as exc:
                print(
                    json.dumps(
                        {
                            "event": "schwab_gateway_request",
                            "ok": False,
                            "error_type": str(exc),
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                self._send_json(502, {"ok": False, "error": "schwab_request_failed"})
                return
            except Exception as exc:  # noqa: BLE001 - never expose provider or token details
                print(
                    json.dumps(
                        {
                            "event": "schwab_gateway_request",
                            "ok": False,
                            "error_type": type(exc).__name__,
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                self._send_json(502, {"ok": False, "error": "schwab_request_failed"})
                return

            if response.status == 401:
                print(
                    json.dumps(
                        {
                            "event": "schwab_reauthorization_required",
                            "ok": False,
                            "status": 401,
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
            try:
                self.send_response(response.status)
                self.send_header("Content-Type", response.content_type)
                self.send_header("Content-Length", str(len(response.body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(response.body)
            except (BrokenPipeError, ConnectionResetError):
                return

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            self._send_json(405, {"ok": False, "error": "method_not_allowed"})

        def log_message(self, format: str, *args: Any) -> None:
            del format, args

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                return

    return GatewayHandler


class RedactedHTTPServer(HTTPServer):
    def handle_error(self, request: Any, client_address: Any) -> None:
        del request, client_address
        error_type = sys.exc_info()[0]
        print(
            json.dumps(
                {
                    "event": "schwab_http_handler_error",
                    "error_type": error_type.__name__ if error_type else "unknown",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


class RedactedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        del request, client_address
        error_type = sys.exc_info()[0]
        print(
            json.dumps(
                {
                    "event": "schwab_http_handler_error",
                    "error_type": error_type.__name__ if error_type else "unknown",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


class OAuthServers:
    def __init__(
        self,
        settings: SchwabSettings,
        coordinator: OAuthCoordinator,
        *,
        auxiliary_runner: Callable[[], None] | None = None,
        auxiliary_close: Callable[[], None] | None = None,
        stream_health: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self.callback_server = RedactedHTTPServer(
            (settings.oauth_bind_host, settings.oauth_bind_port),
            callback_handler_factory(coordinator),
        )
        self.gateway_server = RedactedThreadingHTTPServer(
            (settings.gateway_bind_host, settings.gateway_bind_port),
            gateway_handler_factory(coordinator.manager, stream_health),
        )
        self.critical_threads = [
            threading.Thread(
                target=self.callback_server.serve_forever,
                name="schwab-oauth-callback",
                daemon=True,
            ),
            threading.Thread(
                target=self.gateway_server.serve_forever,
                name="schwab-data-gateway",
                daemon=True,
            ),
        ]
        self.auxiliary_close = auxiliary_close
        self.threads = list(self.critical_threads)
        if auxiliary_runner is not None:
            self.threads.append(
                threading.Thread(
                    target=auxiliary_runner,
                    name="schwab-stream-supervisor",
                    daemon=True,
                )
            )

    def __enter__(self) -> "OAuthServers":
        for thread in self.threads:
            thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        self.close()

    def serve_forever(self) -> None:
        while all(thread.is_alive() for thread in self.critical_threads):
            for thread in self.critical_threads:
                thread.join(timeout=1)

    def close(self) -> None:
        if self.auxiliary_close is not None:
            self.auxiliary_close()
        self.callback_server.shutdown()
        self.gateway_server.shutdown()
        self.callback_server.server_close()
        self.gateway_server.server_close()
        for thread in self.threads:
            thread.join(timeout=5)


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
            with OAuthServers(
                settings,
                coordinator,
                auxiliary_runner=(
                    stream_runtime.run_forever if stream_runtime is not None else None
                ),
                auxiliary_close=(stream_runtime.close if stream_runtime is not None else None),
                stream_health=(stream_runtime.health if stream_runtime is not None else None),
            ) as servers:
                try:
                    servers.serve_forever()
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
