from __future__ import annotations

import math
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urljoin

import httpx
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
class SchwabRequestPolicy:
    requests_per_minute: int = 120
    max_retries: int = 3
    retry_base_seconds: float = 0.5
    retry_max_seconds: float = 8.0
    retry_after_max_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.requests_per_minute <= 0:
            raise ValueError("Schwab requests_per_minute must be positive")
        if self.max_retries < 0:
            raise ValueError("Schwab max_retries cannot be negative")
        if not math.isfinite(self.retry_base_seconds) or self.retry_base_seconds < 0:
            raise ValueError("Schwab retry_base_seconds must be finite and non-negative")
        if not math.isfinite(self.retry_max_seconds):
            raise ValueError("Schwab retry_max_seconds must be finite")
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("Schwab retry_max_seconds cannot be below retry_base_seconds")
        if not math.isfinite(self.retry_after_max_seconds) or self.retry_after_max_seconds < 0:
            raise ValueError("Schwab retry_after_max_seconds must be finite and non-negative")

    @classmethod
    def from_env(cls) -> "SchwabRequestPolicy":
        return cls(
            requests_per_minute=_env_int("SCHWAB_HTTP_REQUESTS_PER_MINUTE", 120),
            max_retries=_env_int("SCHWAB_HTTP_MAX_RETRIES", 3),
            retry_base_seconds=_env_float("SCHWAB_HTTP_RETRY_BASE_SECONDS", 0.5),
            retry_max_seconds=_env_float("SCHWAB_HTTP_RETRY_MAX_SECONDS", 8.0),
            retry_after_max_seconds=_env_float(
                "SCHWAB_HTTP_RETRY_AFTER_MAX_SECONDS",
                30.0,
            ),
        )

    def backoff_seconds(self, retry_index: int) -> float:
        return min(
            self.retry_max_seconds,
            self.retry_base_seconds * (2**retry_index),
        )


class EvenIntervalRateLimiter:
    """Smooth a serial request stream to no more than the configured per-minute rate."""

    def __init__(
        self,
        requests_per_minute: int,
        *,
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
    ) -> None:
        self._interval_seconds = 60.0 / requests_per_minute
        self._monotonic = monotonic
        self._sleep = sleep
        self._next_allowed_at: float | None = None

    def acquire(self) -> None:
        now = float(self._monotonic())
        if self._next_allowed_at is None:
            self._next_allowed_at = now + self._interval_seconds
            return

        delay = self._next_allowed_at - now
        if delay > 0:
            self._sleep(delay)
            now = float(self._monotonic())
        self._next_allowed_at = max(now, self._next_allowed_at) + self._interval_seconds


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

    def __init__(
        self,
        settings: SchwabSettings,
        *,
        request_policy: SchwabRequestPolicy | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.token_store = AtomicJsonFile(settings.token_file)
        self.owner_lock = ExclusiveFileLock(token_owner_lock_path(settings.token_file))
        self.request_policy = request_policy or SchwabRequestPolicy.from_env()
        self._wall_clock = wall_clock
        self._sleep = sleep
        self._rate_limiter = EvenIntervalRateLimiter(
            self.request_policy.requests_per_minute,
            monotonic=monotonic,
            sleep=sleep,
        )
        self._request_lock = threading.RLock()
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
        with self._request_lock:
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

        with self._request_lock:
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

        with self._request_lock:
            with self._lock:
                if self._reauth_required:
                    raise SchwabGatewayUnavailable(
                        "Schwab reauthorization is required; "
                        "run spx-spark-schwab-oauth authorize"
                    )
                client = self._client
            if client is None:
                if not self.load():
                    raise SchwabGatewayUnavailable(
                        "Schwab authorization is not ready; "
                        "run spx-spark-schwab-oauth authorize"
                    )
                with self._lock:
                    client = self._client
            if client is None:  # pragma: no cover - load() guarantees this invariant
                raise SchwabGatewayUnavailable("Schwab authorization client is unavailable")
            url = urljoin(self.settings.api_base_url.rstrip("/") + "/", path.lstrip("/"))
            for retry_index in range(self.request_policy.max_retries + 1):
                self._rate_limiter.acquire()
                try:
                    response = client.session.get(url, params=params)
                except Exception as exc:  # noqa: BLE001 - classify without provider details
                    error_kind = type(exc).__name__
                    with self._lock:
                        self._last_error = error_kind
                    if is_oauth_failure(exc):
                        with self._lock:
                            if self._client is client:
                                self._drop_client()
                            self._reauth_required = True
                        raise SchwabGatewayRequestError(error_kind) from None
                    if is_transient_network_failure(exc) and self._can_retry(retry_index):
                        self._sleep(self.request_policy.backoff_seconds(retry_index))
                        continue
                    raise SchwabGatewayRequestError(error_kind) from None

                status = int(response.status_code)
                if status == 401:
                    with self._lock:
                        if self._client is client:
                            self._drop_client()
                        self._reauth_required = True
                        self._last_error = "http_401"
                    return gateway_response(response)
                if is_retryable_status(status) and self._can_retry(retry_index):
                    with self._lock:
                        self._last_error = f"http_{status}"
                    retry_delay = self._retry_delay(response, retry_index)
                    if retry_delay is not None:
                        self._sleep(retry_delay)
                        continue
                with self._lock:
                    if 200 <= status < 300:
                        self._last_success_at = float(self._wall_clock())
                        self._last_error = None
                    else:
                        self._last_error = f"http_{status}"
                return gateway_response(response)

            raise AssertionError("Schwab retry loop exhausted without a response")

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

    def _can_retry(self, retry_index: int) -> bool:
        return retry_index < self.request_policy.max_retries

    def _retry_delay(self, response: Any, retry_index: int) -> float | None:
        backoff = self.request_policy.backoff_seconds(retry_index)
        retry_after = parse_retry_after_seconds(
            response.headers.get("retry-after"),
            now=float(self._wall_clock()),
        )
        if retry_after is None:
            return backoff
        if retry_after > self.request_policy.retry_after_max_seconds:
            return None
        return max(backoff, retry_after)


def is_oauth_failure(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    module = type(exc).__module__.lower()
    return module.startswith("authlib.") or "oauth" in name or "token" in name


def is_transient_network_failure(exc: BaseException) -> bool:
    return isinstance(exc, (httpx.TransportError, ConnectionError, TimeoutError))


def is_retryable_status(status: int) -> bool:
    return status == 429 or 500 <= status < 600


def gateway_response(response: Any) -> GatewayResponse:
    return GatewayResponse(
        status=int(response.status_code),
        content_type=response.headers.get("content-type", "application/json"),
        body=bytes(response.content),
    )


def parse_retry_after_seconds(value: Any, *, now: float) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = retry_at.timestamp() - now
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    return float(value) if value else default


def validate_token_document(raw: dict[str, Any]) -> None:
    token = raw.get("token")
    if not isinstance(token, dict):
        raise ValueError("Schwab token wrapper is missing token data")
    for key in ("access_token", "refresh_token"):
        if not isinstance(token.get(key), str) or not token[key]:
            raise ValueError(f"Schwab token is missing {key}")
