"""Morning-map notification delivery."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.application.morning_map.render import build_map_prompt, render_template
from spx_spark.config import NotificationSettings
from spx_spark.notifier.llm_writer import generate_push_text
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import any_delivery_ok, deliver_trade_push, im_delivery_ok

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

    delivery_sinks = deliver_trade_push(
        settings,
        title="盘前地图",
        text=text,
        kind="morning_map",
        lane="trade",
        friend=True,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not im_delivery_ok(delivery_sinks):
        append_missed(settings.missed_queue_path, text, kind="morning_map", at=now)

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer != "template",
        "im_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }
