from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from schwab.auth import AuthContext

from spx_spark.config import SchwabSettings
from spx_spark.schwab import gateway
from spx_spark.schwab.gateway import (
    SchwabGatewayUnavailable,
    SchwabRequestPolicy,
    SchwabSessionManager,
    parse_retry_after_seconds,
)


CALLBACK_URL = "https://schwab-auth.example.com/oauth/callback"


def make_settings(tmp_path: Path) -> SchwabSettings:
    return SchwabSettings(
        api_base_url="https://api.schwabapi.com",
        access_token="",
        token_file=str(tmp_path / "schwab-token.json"),
        verify_indexes=["$SPX"],
        verify_equities=["SPY"],
        verify_futures=["/ES"],
        verify_option_chains=["SPX"],
        option_chain_strike_count=10,
        quote_fields="quote,reference",
        request_timeout_seconds=12.0,
        app_key="app-key",
        app_secret="app-secret",
        callback_url=CALLBACK_URL,
        oauth_state_file=str(tmp_path / "schwab-oauth-state.json"),
        gateway_url="http://127.0.0.1:8184",
    )


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = {"content-type": "application/json"} | (headers or {})
        self.content = b'{"SPY": {"quote": {"lastPrice": 600}}}'


class FakeClient:
    def __init__(self) -> None:
        self.timeout: float | None = None
        self.requests: list[tuple[str, list[tuple[str, str]]]] = []
        self.session = self
        self.next_response = FakeResponse()
        self.outcomes: list[FakeResponse | BaseException] = []
        self.closed = False

    def set_timeout(self, timeout: float) -> None:
        self.timeout = timeout

    def get(self, url: str, params: list[tuple[str, str]]) -> FakeResponse:
        self.requests.append((url, params))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return self.next_response

    def close(self) -> None:
        self.closed = True


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def wall_clock(self) -> float:
        return 1_000.0 + self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


def write_token(settings: SchwabSettings) -> None:
    Path(settings.token_file).write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {"access_token": "secret", "refresh_token": "refresh"},
            }
        ),
        encoding="utf-8",
    )


def manager_with_fake_client(
    monkeypatch: pytest.MonkeyPatch,
    settings: SchwabSettings,
    fake_client: FakeClient,
    clock: FakeClock,
    policy: SchwabRequestPolicy,
) -> SchwabSessionManager:
    write_token(settings)
    monkeypatch.setattr(
        gateway,
        "client_from_access_functions",
        lambda *args, **kwargs: fake_client,
    )
    manager = SchwabSessionManager(
        settings,
        request_policy=policy,
        monotonic=clock.monotonic,
        wall_clock=clock.wall_clock,
        sleep=clock.sleep,
    )
    assert manager.load() is True
    return manager


def test_gateway_loads_one_refreshing_client_and_proxies_only_market_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    Path(settings.token_file).write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {"access_token": "secret", "refresh_token": "refresh"},
            }
        ),
        encoding="utf-8",
    )
    fake_client = FakeClient()
    calls: list[tuple[str, str]] = []

    def fake_from_access(
        app_key: str,
        app_secret: str,
        token_reader: object,
        token_writer: object,
        **kwargs: object,
    ) -> FakeClient:
        del token_reader, token_writer, kwargs
        calls.append((app_key, app_secret))
        return fake_client

    monkeypatch.setattr(gateway, "client_from_access_functions", fake_from_access)
    manager = SchwabSessionManager(settings)

    assert manager.load() is True
    assert manager.ready is True
    response = manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])

    assert response.status == 200
    assert calls == [("app-key", "app-secret")]
    assert fake_client.timeout == 12.0
    assert fake_client.requests[0][0] == "https://api.schwabapi.com/marketdata/v1/quotes"
    with pytest.raises(ValueError, match="Unsupported"):
        manager.request("/trader/v1/accounts", [])

    assert manager.client_for_streaming() is fake_client


def test_callback_exchange_discards_first_client_and_loads_gateway_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    loaded_client = FakeClient()
    exchange_calls: list[str] = []

    def fake_received(
        app_key: str,
        app_secret: str,
        auth_context: AuthContext,
        received_url: str,
        token_writer: object,
        **kwargs: object,
    ) -> object:
        del app_key, app_secret, auth_context, kwargs
        exchange_calls.append(received_url)
        token_writer(  # type: ignore[operator]
            {
                "creation_timestamp": 1,
                "token": {"access_token": "secret", "refresh_token": "refresh"},
            }
        )
        return SimpleNamespace(name="discard-me")

    monkeypatch.setattr(gateway, "client_from_received_url", fake_received)
    monkeypatch.setattr(
        gateway,
        "client_from_access_functions",
        lambda *args, **kwargs: loaded_client,
    )
    manager = SchwabSessionManager(settings)
    context = AuthContext(CALLBACK_URL, "https://schwab.example/auth", "state")

    manager.install_callback_token(
        auth_context=context,
        received_url=f"{CALLBACK_URL}?code=code&state=state",
    )

    assert manager.ready is True
    assert exchange_calls == [f"{CALLBACK_URL}?code=code&state=state"]
    assert loaded_client.timeout == 12.0


def test_gateway_is_unavailable_before_authorization(tmp_path: Path) -> None:
    manager = SchwabSessionManager(make_settings(tmp_path))
    with pytest.raises(SchwabGatewayUnavailable, match="authorization is not ready"):
        manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])


@pytest.mark.parametrize(
    "document",
    [
        {"creation_timestamp": 1},
        {"creation_timestamp": 1, "token": None},
    ],
)
def test_gateway_rejects_malformed_token_documents(
    tmp_path: Path,
    document: dict[str, object],
) -> None:
    settings = make_settings(tmp_path)
    Path(settings.token_file).write_text(json.dumps(document), encoding="utf-8")
    manager = SchwabSessionManager(settings)

    assert manager.load() is False
    health = manager.health()
    assert health.ready is False
    assert health.reauth_required is True
    assert health.last_error == "token_load_failed"


def test_unauthorized_response_latches_reauthorization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    fake_client = FakeClient()
    fake_client.outcomes = [FakeResponse(status_code=401), FakeResponse(status_code=200)]
    clock = FakeClock()
    load_count = 0

    def fake_from_access(*args: object, **kwargs: object) -> FakeClient:
        nonlocal load_count
        del args, kwargs
        load_count += 1
        return fake_client

    monkeypatch.setattr(gateway, "client_from_access_functions", fake_from_access)
    write_token(settings)
    manager = SchwabSessionManager(
        settings,
        request_policy=SchwabRequestPolicy(max_retries=3),
        monotonic=clock.monotonic,
        wall_clock=clock.wall_clock,
        sleep=clock.sleep,
    )
    assert manager.load() is True

    response = manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])
    assert response.status == 401
    health = manager.health()
    assert health.ready is False
    assert health.reauth_required is True
    assert health.last_error == "http_401"

    with pytest.raises(SchwabGatewayUnavailable, match="reauthorization is required"):
        manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])
    assert load_count == 1
    assert len(fake_client.requests) == 1
    assert clock.sleeps == []


def test_request_policy_is_configurable_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCHWAB_HTTP_REQUESTS_PER_MINUTE", "90")
    monkeypatch.setenv("SCHWAB_HTTP_MAX_RETRIES", "4")
    monkeypatch.setenv("SCHWAB_HTTP_RETRY_BASE_SECONDS", "0.25")
    monkeypatch.setenv("SCHWAB_HTTP_RETRY_MAX_SECONDS", "4")
    monkeypatch.setenv("SCHWAB_HTTP_RETRY_AFTER_MAX_SECONDS", "12")

    policy = SchwabRequestPolicy.from_env()

    assert policy == SchwabRequestPolicy(
        requests_per_minute=90,
        max_retries=4,
        retry_base_seconds=0.25,
        retry_max_seconds=4.0,
        retry_after_max_seconds=12.0,
    )


def test_retry_after_supports_delta_seconds_and_http_dates() -> None:
    assert parse_retry_after_seconds("2.5", now=1_000.0) == 2.5
    assert (
        parse_retry_after_seconds("Thu, 01 Jan 1970 00:16:50 GMT", now=1_000.0)
        == 10.0
    )
    assert parse_retry_after_seconds("invalid", now=1_000.0) is None


def test_global_rate_limit_smooths_requests_to_120_per_minute(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    fake_client = FakeClient()
    clock = FakeClock()
    manager = manager_with_fake_client(
        monkeypatch,
        settings,
        fake_client,
        clock,
        SchwabRequestPolicy(requests_per_minute=120, max_retries=0),
    )

    manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])
    manager.request("/marketdata/v1/quotes", [("symbols", "QQQ")])

    assert len(fake_client.requests) == 2
    assert clock.sleeps == [0.5]


def test_retryable_statuses_use_exponential_backoff_and_bounded_retry_after(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    fake_client = FakeClient()
    fake_client.outcomes = [
        FakeResponse(429, headers={"retry-after": "2.5"}),
        FakeResponse(502),
        FakeResponse(503),
        FakeResponse(504),
        FakeResponse(200),
    ]
    clock = FakeClock()
    manager = manager_with_fake_client(
        monkeypatch,
        settings,
        fake_client,
        clock,
        SchwabRequestPolicy(
            requests_per_minute=6_000,
            max_retries=4,
            retry_base_seconds=1.0,
            retry_max_seconds=8.0,
            retry_after_max_seconds=3.0,
        ),
    )

    response = manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])

    assert response.status == 200
    assert len(fake_client.requests) == 5
    assert clock.sleeps == [2.5, 2.0, 4.0, 8.0]
    assert manager.health().last_error is None


def test_retry_after_above_wait_cap_returns_without_retrying_early(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    fake_client = FakeClient()
    fake_client.outcomes = [
        FakeResponse(429, headers={"retry-after": "99"}),
        FakeResponse(200),
    ]
    clock = FakeClock()
    manager = manager_with_fake_client(
        monkeypatch,
        settings,
        fake_client,
        clock,
        SchwabRequestPolicy(
            requests_per_minute=6_000,
            max_retries=3,
            retry_after_max_seconds=30.0,
        ),
    )

    response = manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])

    assert response.status == 429
    assert len(fake_client.requests) == 1
    assert clock.sleeps == []
    assert manager.health().last_error == "http_429"


def test_network_failures_retry_exponentially_without_real_sleep(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    fake_client = FakeClient()
    fake_client.outcomes = [ConnectionError("offline"), TimeoutError(), FakeResponse(200)]
    clock = FakeClock()
    manager = manager_with_fake_client(
        monkeypatch,
        settings,
        fake_client,
        clock,
        SchwabRequestPolicy(
            requests_per_minute=6_000,
            max_retries=3,
            retry_base_seconds=1.0,
            retry_max_seconds=8.0,
        ),
    )

    response = manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])

    assert response.status == 200
    assert len(fake_client.requests) == 3
    assert clock.sleeps == [1.0, 2.0]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retryable_status_returns_final_response_after_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: int,
) -> None:
    settings = make_settings(tmp_path)
    fake_client = FakeClient()
    fake_client.outcomes = [FakeResponse(status) for _ in range(3)]
    clock = FakeClock()
    manager = manager_with_fake_client(
        monkeypatch,
        settings,
        fake_client,
        clock,
        SchwabRequestPolicy(
            requests_per_minute=6_000,
            max_retries=2,
            retry_base_seconds=1.0,
            retry_max_seconds=8.0,
        ),
    )

    response = manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])

    assert response.status == status
    assert len(fake_client.requests) == 3
    assert clock.sleeps == [1.0, 2.0]
    assert manager.health().last_error == f"http_{status}"


def test_network_failure_stops_after_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    fake_client = FakeClient()
    fake_client.outcomes = [ConnectionError("offline") for _ in range(3)]
    clock = FakeClock()
    manager = manager_with_fake_client(
        monkeypatch,
        settings,
        fake_client,
        clock,
        SchwabRequestPolicy(
            requests_per_minute=6_000,
            max_retries=2,
            retry_base_seconds=1.0,
            retry_max_seconds=8.0,
        ),
    )

    with pytest.raises(gateway.SchwabGatewayRequestError, match="ConnectionError"):
        manager.request("/marketdata/v1/quotes", [("symbols", "SPY")])

    assert len(fake_client.requests) == 3
    assert clock.sleeps == [1.0, 2.0]
    assert manager.health().ready is True


def test_health_remains_responsive_while_upstream_request_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingClient(FakeClient):
        def get(self, url: str, params: list[tuple[str, str]]) -> FakeResponse:
            self.requests.append((url, params))
            entered.set()
            assert release.wait(timeout=2.0)
            return FakeResponse(200)

    settings = make_settings(tmp_path)
    fake_client = BlockingClient()
    clock = FakeClock()
    manager = manager_with_fake_client(
        monkeypatch,
        settings,
        fake_client,
        clock,
        SchwabRequestPolicy(requests_per_minute=120, max_retries=0),
    )
    request_results: list[int] = []
    health_results: list[bool] = []
    health_done = threading.Event()
    request_thread = threading.Thread(
        target=lambda: request_results.append(
            manager.request("/marketdata/v1/quotes", [("symbols", "SPY")]).status
        )
    )
    health_thread = threading.Thread(
        target=lambda: (
            health_results.append(manager.health().ready),
            health_done.set(),
        )
    )

    request_thread.start()
    assert entered.wait(timeout=1.0)
    health_thread.start()
    try:
        assert health_done.wait(timeout=1.0)
        assert health_results == [True]
    finally:
        release.set()
        request_thread.join(timeout=2.0)
        health_thread.join(timeout=2.0)
    assert not request_thread.is_alive()
    assert request_results == [200]
