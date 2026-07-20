from __future__ import annotations

from types import SimpleNamespace

from spx_spark.application.schwab_reauth_reminder import (
    deliver_authorization_reminder,
)
from spx_spark.schwab.oauth_service import PendingAuthorization


def test_authorization_reminder_delivers_short_lived_url() -> None:
    pending = PendingAuthorization(
        callback_url="https://oauth.example/callback",
        authorization_url="https://schwab.example/authorize?state=public-state",
        state="public-state",
        created_at=1000.0,
        expires_at=1900.0,
    )
    calls: list[dict[str, object]] = []

    def deliverer(_settings: object, **kwargs: object) -> list[SimpleNamespace]:
        calls.append(kwargs)
        return [SimpleNamespace(sink="bark", attempted=True, ok=True)]

    result = deliver_authorization_reminder(
        pending,
        settings=SimpleNamespace(),  # type: ignore[arg-type]
        deliverer=deliverer,
    )

    assert result["delivered"] is True
    assert result["delivered_sinks"] == ["bark"]
    assert calls[0]["lane"] == "trade"
    assert pending.authorization_url in str(calls[0]["text"])
    assert "15 分钟" in str(calls[0]["text"])
