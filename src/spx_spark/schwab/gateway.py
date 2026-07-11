from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

from schwab.auth import (
    AuthContext,
    client_from_access_functions,
    client_from_received_url,
)

from spx_spark.config import SchwabSettings
from spx_spark.schwab.auth_storage import (
    AtomicJsonFile,
    ExclusiveFileLock,
    token_owner_lock_path,
)


ALLOWED_MARKET_DATA_PATHS = frozenset(
    {
        "/marketdata/v1/chains",
        "/marketdata/v1/quotes",
    }
)


class SchwabGatewayUnavailable(RuntimeError):
    pass


class SchwabGatewayRequestError(RuntimeError):
    pass


@dataclass(frozen=True)
class GatewayResponse:
    status: int
    content_type: str
    body: bytes


@dataclass(frozen=True)
class GatewayHealth:
    ready: bool
    reauth_required: bool
    last_success_at: float | None
    last_error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ready,
            "ready": self.ready,
            "reauth_required": self.reauth_required,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
        }


class SchwabSessionManager:
    """Own exactly one refresh-capable Schwab client for a token file."""

    def __init__(self, settings: SchwabSettings) -> None:
        self.settings = settings
        self.token_store = AtomicJsonFile(settings.token_file)
        self.owner_lock = ExclusiveFileLock(token_owner_lock_path(settings.token_file))
        self._lock = threading.RLock()
        self._client: Any | None = None
        self._reauth_required = False
        self._last_success_at: float | None = None
        self._last_error: str | None = None

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._client is not None

    def health(self) -> GatewayHealth:
        with self._lock:
            return GatewayHealth(
                ready=self._client is not None,
                reauth_required=self._reauth_required,
                last_success_at=self._last_success_at,
                last_error=self._last_error,
            )

    def load(self) -> bool:
        with self._lock:
            if not self._can_load_client():
                self._drop_client()
                self._reauth_required = True
                self._last_error = "token_missing"
                return False
            try:
                raw_token = self.token_store.read()
                validate_token_document(raw_token)
                new_client = client_from_access_functions(
                    self.settings.app_key,
                    self.settings.app_secret,
                    self.token_store.read,
                    self.token_store.write,
                    enforce_enums=False,
                )
            except (KeyError, OSError, TypeError, ValueError):
                self._drop_client()
                self._reauth_required = True
                self._last_error = "token_load_failed"
                return False
            new_client.set_timeout(self.settings.request_timeout_seconds)
            self._drop_client()
            self._client = new_client
            self._reauth_required = False
            self._last_error = None
            return True

    def install_callback_token(
        self,
        *,
        auth_context: AuthContext,
        received_url: str,
    ) -> None:
        """Exchange a callback URL, persist it atomically, then load a fresh client."""

        with self._lock:
            client_from_received_url(
                self.settings.app_key,
                self.settings.app_secret,
                auth_context,
                received_url,
                self.token_store.write,
                enforce_enums=False,
            )
            if not self.load():  # pragma: no cover - the exchange just wrote a token
                raise SchwabGatewayUnavailable("Schwab token was not persisted")

    def request(
        self,
        path: str,
        params: list[tuple[str, str]],
    ) -> GatewayResponse:
        if path not in ALLOWED_MARKET_DATA_PATHS:
            raise ValueError(f"Unsupported Schwab market-data path: {path}")

        with self._lock:
            if self._reauth_required:
                raise SchwabGatewayUnavailable(
                    "Schwab reauthorization is required; run spx-spark-schwab-oauth authorize"
                )
            if self._client is None and not self.load():
                raise SchwabGatewayUnavailable(
                    "Schwab authorization is not ready; run spx-spark-schwab-oauth authorize"
                )
            url = urljoin(self.settings.api_base_url.rstrip("/") + "/", path.lstrip("/"))
            try:
                response = self._client.session.get(url, params=params)
            except Exception as exc:  # noqa: BLE001 - classify without exposing provider details
                error_kind = type(exc).__name__
                self._last_error = error_kind
                if is_oauth_failure(exc):
                    self._drop_client()
                    self._reauth_required = True
                raise SchwabGatewayRequestError(error_kind) from None
            status = int(response.status_code)
            if status == 401:
                self._drop_client()
                self._reauth_required = True
                self._last_error = "http_401"
            elif 200 <= status < 300:
                self._last_success_at = time.time()
                self._last_error = None
            else:
                self._last_error = f"http_{status}"
            return GatewayResponse(
                status=status,
                content_type=response.headers.get("content-type", "application/json"),
                body=bytes(response.content),
            )

    def _can_load_client(self) -> bool:
        return bool(
            self.settings.app_key
            and self.settings.app_secret
            and self.token_store.exists
        )

    def _drop_client(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        close = getattr(client.session, "close", None)
        if callable(close):
            try:
                close()
            except Exception:  # noqa: BLE001 - best-effort cleanup only
                pass


def is_oauth_failure(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    module = type(exc).__module__.lower()
    return module.startswith("authlib.") or "oauth" in name or "token" in name


def validate_token_document(raw: dict[str, Any]) -> None:
    token = raw.get("token")
    if not isinstance(token, dict):
        raise ValueError("Schwab token wrapper is missing token data")
    for key in ("access_token", "refresh_token"):
        if not isinstance(token.get(key), str) or not token[key]:
            raise ValueError(f"Schwab token is missing {key}")
