from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from schwab.auth import AuthContext

from spx_spark.config import SchwabSettings
from spx_spark.schwab import gateway
from spx_spark.schwab.gateway import SchwabGatewayUnavailable, SchwabSessionManager


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
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code
        self.headers = {"content-type": "application/json"}
        self.content = b'{"SPY": {"quote": {"lastPrice": 600}}}'


class FakeClient:
    def __init__(self) -> None:
        self.timeout: float | None = None
        self.requests: list[tuple[str, list[tuple[str, str]]]] = []
        self.session = self
        self.next_response = FakeResponse()

    def set_timeout(self, timeout: float) -> None:
        self.timeout = timeout

    def get(self, url: str, params: list[tuple[str, str]]) -> FakeResponse:
        self.requests.append((url, params))
        return self.next_response


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
    fake_client.next_response = FakeResponse(status_code=401)
    load_count = 0

    def fake_from_access(*args: object, **kwargs: object) -> FakeClient:
        nonlocal load_count
        del args, kwargs
        load_count += 1
        return fake_client

    monkeypatch.setattr(gateway, "client_from_access_functions", fake_from_access)
    manager = SchwabSessionManager(settings)
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
