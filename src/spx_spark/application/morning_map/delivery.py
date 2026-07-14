"""Morning-map notification delivery."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.application.morning_map.render import build_map_prompt, render_template
from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import dispatch_notification
from spx_spark.notifier.llm_writer import generate_push_text
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope, notification_event_id

def send_morning_map(
    payload: dict[str, Any],
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    previous_push: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    template = render_template(payload)
    text, writer = generate_push_text(
        template,
        build_map_prompt(payload, template, previous_push),
        settings,
        runner=runner,
    )

    event_id = notification_event_id(
        "morning_map",
        source="morning_map",
        occurred_at=now,
        identity=str(payload.get("trading_date") or payload.get("as_of") or now.date()),
    )
    dispatch = dispatch_notification(
        settings,
        NotificationEnvelope(
            event_id=event_id,
            source="morning_map",
            kind="morning_map",
            lane="scheduled_report",
            occurred_at=now,
        ),
        title="盘前地图",
        text=text,
        friend=True,
        runner=runner,
        attempted_at=now,
    )
    delivery_sinks = list(dispatch.sinks)
    delivered_ok = dispatch.delivered

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer != "template",
        "im_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }
