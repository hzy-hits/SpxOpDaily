"""Generate and deliver the weekly human-in-the-loop Schwab OAuth reminder."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from spx_spark.config import NotificationSettings, SchwabSettings
from spx_spark.notifier.sinks import deliver_trade_push
from spx_spark.schwab.gateway import SchwabSessionManager
from spx_spark.schwab.oauth_service import (
    OAuthCoordinator,
    PendingAuthorization,
    validate_oauth_settings,
)


def deliver_authorization_reminder(
    pending: PendingAuthorization,
    *,
    settings: NotificationSettings,
    deliverer: Callable[..., list[Any]] = deliver_trade_push,
) -> dict[str, Any]:
    """Deliver a short-lived authorization URL through the normal user sinks."""

    ttl_minutes = max(1, int((pending.expires_at - pending.created_at) // 60))
    text = (
        "Schwab refresh token 即将进入每周续期窗口。\n"
        f"请在 {ttl_minutes} 分钟内打开并完成授权：\n"
        f"{pending.authorization_url}\n"
        "成功后 Gateway 会自动热更新，无需重启服务。"
    )
    sinks = deliverer(
        settings,
        title="Schwab 周末授权提醒",
        text=text,
        kind="status",
        lane="trade",
        friend=False,
    )
    attempted = [sink for sink in sinks if sink.attempted]
    delivered = [sink for sink in attempted if sink.ok]
    return {
        "attempted_sinks": [sink.sink for sink in attempted],
        "delivered_sinks": [sink.sink for sink in delivered],
        "delivered": bool(delivered),
        "expires_at": pending.expires_at,
    }


def run() -> int:
    schwab_settings = SchwabSettings.from_env()
    validate_oauth_settings(schwab_settings)
    manager = SchwabSessionManager(schwab_settings)
    pending = OAuthCoordinator(schwab_settings, manager).authorize()
    result = deliver_authorization_reminder(
        pending,
        settings=NotificationSettings.from_env(),
    )
    print(
        json.dumps(
            {
                "event": "schwab_reauthorization_reminder",
                "ok": result["delivered"],
                **result,
            },
            sort_keys=True,
        )
    )
    return 0 if result["delivered"] else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
