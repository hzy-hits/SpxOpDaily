from __future__ import annotations

import stat
from pathlib import Path

import pytest
import schwab.auth

from spx_spark.schwab.auth_storage import ExclusiveFileLock, token_owner_lock_path
from spx_spark.schwab.token_helper import run


CALLBACK_URL = "https://127.0.0.1:8182"


def configure_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", CALLBACK_URL)


def test_manual_helper_uses_atomic_writer_and_releases_owner_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configure_env(monkeypatch)
    token_path = tmp_path / "token.json"

    def fake_manual_flow(**kwargs: object) -> object:
        writer = kwargs["token_write_func"]
        writer(  # type: ignore[operator]
            {
                "creation_timestamp": 1,
                "token": {"access_token": "secret", "refresh_token": "refresh"},
            }
        )
        return object()

    monkeypatch.setattr(schwab.auth, "client_from_manual_flow", fake_manual_flow)

    assert run(["--token-path", str(token_path), "--callback-url", CALLBACK_URL]) == 0
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600

    # A successful helper releases the process owner lock.
    with ExclusiveFileLock(token_owner_lock_path(token_path)).held():
        pass


def test_manual_helper_refuses_token_while_gateway_owns_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configure_env(monkeypatch)
    token_path = tmp_path / "token.json"
    called = False

    def fake_manual_flow(**kwargs: object) -> object:
        nonlocal called
        del kwargs
        called = True
        return object()

    monkeypatch.setattr(schwab.auth, "client_from_manual_flow", fake_manual_flow)

    with ExclusiveFileLock(token_owner_lock_path(token_path)).held():
        with pytest.raises(SystemExit, match="gateway owns this token file"):
            run(["--token-path", str(token_path), "--callback-url", CALLBACK_URL])

    assert called is False
