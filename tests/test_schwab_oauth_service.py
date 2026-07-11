from __future__ import annotations

import json
import stat
from dataclasses import replace
from http.client import HTTPConnection
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from urllib.request import urlopen

import pytest
from schwab.auth import AuthContext

from spx_spark.config import SchwabSettings
from spx_spark.schwab.auth_storage import AtomicJsonFile
from spx_spark.schwab.gateway import GatewayHealth
from spx_spark.schwab.oauth_service import (
    OAuthCallbackError,
    OAuthCoordinator,
    OAuthServers,
    validate_oauth_settings,
)


CALLBACK_URL = "https://schwab-auth.example.com/oauth/callback"


def make_settings(tmp_path: Path, **overrides: object) -> SchwabSettings:
    values: dict[str, object] = {
        "api_base_url": "https://api.schwabapi.com",
        "access_token": "",
        "token_file": str(tmp_path / "schwab-token.json"),
        "verify_indexes": ["$SPX"],
        "verify_equities": ["SPY"],
        "verify_futures": ["/ES"],
        "verify_option_chains": ["SPX"],
        "option_chain_strike_count": 10,
        "quote_fields": "quote,reference",
        "request_timeout_seconds": 12.0,
        "app_key": "app-key",
        "app_secret": "app-secret",
        "callback_url": CALLBACK_URL,
        "oauth_bind_host": "127.0.0.1",
        "oauth_bind_port": 8183,
        "oauth_state_file": str(tmp_path / "schwab-oauth-state.json"),
        "oauth_state_ttl_seconds": 600,
        "gateway_bind_host": "127.0.0.1",
        "gateway_bind_port": 8184,
        "gateway_url": "http://127.0.0.1:8184",
    }
    values.update(overrides)
    return SchwabSettings(**values)  # type: ignore[arg-type]


class FakeManager:
    def __init__(self, token_file: str) -> None:
        self.token_store = AtomicJsonFile(token_file)
        self.received: list[tuple[AuthContext, str]] = []
        self.ready = False

    def install_callback_token(self, *, auth_context: AuthContext, received_url: str) -> None:
        self.received.append((auth_context, received_url))
        self.token_store.write(
            {
                "creation_timestamp": 1,
                "token": {"access_token": "redacted", "refresh_token": "redacted-refresh"},
            }
        )
        self.ready = True

    def health(self) -> GatewayHealth:
        return GatewayHealth(
            ready=self.ready,
            reauth_required=False,
            last_success_at=None,
            last_error=None,
        )


def auth_factory(api_key: str, callback_url: str) -> AuthContext:
    assert api_key == "app-key"
    assert callback_url == CALLBACK_URL
    return AuthContext(callback_url, "https://schwab.example/authorize?state=server-state", "server-state")


def test_atomic_json_file_writes_private_file(tmp_path: Path) -> None:
    store = AtomicJsonFile(tmp_path / "runtime" / "token.json")
    store.write({"token": {"access_token": "secret"}})

    assert store.read()["token"]["access_token"] == "secret"
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(store.lock_path.stat().st_mode) == 0o600


def test_authorize_persists_server_generated_state(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    manager = FakeManager(settings.token_file)
    coordinator = OAuthCoordinator(
        settings,
        manager,  # type: ignore[arg-type]
        now=lambda: 1000.0,
        auth_context_factory=auth_factory,
    )

    pending = coordinator.authorize()

    assert pending.state == "server-state"
    assert pending.expires_at == 1600.0
    saved = json.loads(Path(settings.oauth_state_file).read_text(encoding="utf-8"))
    assert saved["state"] == "server-state"
    assert "app-secret" not in json.dumps(saved)


def test_callback_uses_fixed_public_url_and_consumes_state(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    manager = FakeManager(settings.token_file)
    coordinator = OAuthCoordinator(
        settings,
        manager,  # type: ignore[arg-type]
        now=lambda: 1000.0,
        auth_context_factory=auth_factory,
    )
    coordinator.authorize()

    coordinator.complete("code=one-time-code&state=server-state")

    assert not Path(settings.oauth_state_file).exists()
    assert len(manager.received) == 1
    context, received_url = manager.received[0]
    assert context.state == "server-state"
    assert received_url == f"{CALLBACK_URL}?code=one-time-code&state=server-state"
    assert manager.token_store.exists


def test_wrong_state_does_not_consume_valid_pending_authorization(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    manager = FakeManager(settings.token_file)
    coordinator = OAuthCoordinator(
        settings,
        manager,  # type: ignore[arg-type]
        now=lambda: 1000.0,
        auth_context_factory=auth_factory,
    )
    coordinator.authorize()

    with pytest.raises(OAuthCallbackError, match="state mismatch"):
        coordinator.complete("code=code&state=attacker-state")

    assert Path(settings.oauth_state_file).exists()
    assert not manager.received


def test_non_ascii_state_is_a_redacted_bad_request(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    manager = FakeManager(settings.token_file)
    coordinator = OAuthCoordinator(
        settings,
        manager,  # type: ignore[arg-type]
        now=lambda: 1000.0,
        auth_context_factory=auth_factory,
    )
    coordinator.authorize()

    with pytest.raises(OAuthCallbackError, match="state mismatch"):
        coordinator.complete("code=code&state=%E2%98%83")

    assert Path(settings.oauth_state_file).exists()
    assert not manager.received


def test_exchange_failure_drops_sensitive_exception_chain(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)

    class FailingManager(FakeManager):
        def install_callback_token(
            self,
            *,
            auth_context: AuthContext,
            received_url: str,
        ) -> None:
            del auth_context, received_url
            raise RuntimeError("provider error contains code=SENSITIVE-CODE")

    coordinator = OAuthCoordinator(
        settings,
        FailingManager(settings.token_file),  # type: ignore[arg-type]
        now=lambda: 1000.0,
        auth_context_factory=auth_factory,
    )
    coordinator.authorize()

    with pytest.raises(OAuthCallbackError) as captured:
        coordinator.complete("code=SENSITIVE-CODE&state=server-state")

    assert "SENSITIVE-CODE" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__suppress_context__ is True


def test_expired_state_is_removed(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, oauth_state_ttl_seconds=10)
    manager = FakeManager(settings.token_file)
    clock = SimpleNamespace(value=1000.0)
    coordinator = OAuthCoordinator(
        settings,
        manager,  # type: ignore[arg-type]
        now=lambda: clock.value,
        auth_context_factory=auth_factory,
    )
    coordinator.authorize()
    clock.value = 1011.0

    with pytest.raises(OAuthCallbackError, match="expired"):
        coordinator.complete("code=code&state=server-state")

    assert not Path(settings.oauth_state_file).exists()
    assert not manager.received


def test_callback_url_must_be_https_without_query(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="absolute HTTPS"):
        validate_oauth_settings(make_settings(tmp_path, callback_url="http://example.com/cb"))
    with pytest.raises(ValueError, match="query or fragment"):
        validate_oauth_settings(make_settings(tmp_path, callback_url=f"{CALLBACK_URL}?x=1"))


def test_gateway_oauth_rejects_non_official_token_egress(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="api.schwabapi.com"):
        validate_oauth_settings(
            make_settings(tmp_path, api_base_url="https://attacker.example/schwab")
        )


def test_token_state_and_lock_paths_must_be_distinct(tmp_path: Path) -> None:
    token_path = tmp_path / "schwab-token.json"
    with pytest.raises(ValueError, match="must all be distinct"):
        validate_oauth_settings(
            make_settings(
                tmp_path,
                token_file=str(token_path),
                oauth_state_file=str(token_path),
            )
        )
    with pytest.raises(ValueError, match="must all be distinct"):
        validate_oauth_settings(
            make_settings(
                tmp_path,
                token_file=str(token_path),
                oauth_state_file=str(token_path.with_name(f"{token_path.name}.lock")),
            )
        )


def test_bind_hosts_reject_ipv6_until_server_uses_af_inet6(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="IPv4 loopback"):
        validate_oauth_settings(make_settings(tmp_path, oauth_bind_host="::1"))


def test_authorization_url_contains_only_public_state(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    coordinator = OAuthCoordinator(
        settings,
        FakeManager(settings.token_file),  # type: ignore[arg-type]
        auth_context_factory=auth_factory,
    )
    pending = coordinator.authorize()
    parsed = urlsplit(pending.authorization_url)
    assert parse_qs(parsed.query)["state"] == ["server-state"]
    assert settings.app_secret not in pending.authorization_url


def test_servers_keep_gateway_local_and_never_log_callback_query(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = make_settings(tmp_path)
    manager = FakeManager(settings.token_file)
    coordinator = OAuthCoordinator(
        settings,
        manager,  # type: ignore[arg-type]
        now=lambda: 1000.0,
        auth_context_factory=auth_factory,
    )
    coordinator.authorize()
    settings = replace(settings, oauth_bind_port=0, gateway_bind_port=0)
    coordinator.settings = settings

    with OAuthServers(settings, coordinator) as servers:
        callback_port = servers.callback_server.server_address[1]
        gateway_port = servers.gateway_server.server_address[1]
        with urlopen(f"http://127.0.0.1:{callback_port}/healthz") as response:
            assert json.load(response) == {"ok": True}
        with urlopen(
            f"http://127.0.0.1:{callback_port}/oauth/callback"
            "?code=sensitive-code&state=server-state"
        ) as response:
            assert response.status == 200
        with urlopen(f"http://127.0.0.1:{gateway_port}/healthz") as response:
            health = json.load(response)
            assert health["ok"] is True
            assert health["ready"] is True
            assert health["reauth_required"] is False
        with urlopen(f"http://127.0.0.1:{gateway_port}/livez") as response:
            assert json.load(response) == {"ok": True}
        connection = HTTPConnection("127.0.0.1", gateway_port)
        connection.request("GET", "/healthz", headers={"Host": "public.example.com"})
        assert connection.getresponse().status == 403
        connection.close()

    output = capsys.readouterr()
    assert "sensitive-code" not in output.out
    assert "sensitive-code" not in output.err
