"""Order-map notification delivery (IM sinks)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.application.order_map.prompts import build_order_prompt
from spx_spark.application.order_map.render import render_template
from spx_spark.config import NotificationSettings
from spx_spark.notifier.llm_writer import (
    generate_push_text,
)
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import any_delivery_ok, deliver_trade_push, im_delivery_ok


def send_order_map(
    payload: dict[str, Any],
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    extra_header: str | None = None,
    previous_push: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    template = render_template(payload)
    if extra_header:
        template = f"{extra_header}\n{template}"
    research_only = payload.get("research_only") is True
    if research_only:
        text, writer = template, "template"
    else:
        text, writer = generate_push_text(
            template,
            build_order_prompt(payload, template, previous_push),
            settings,
            runner=runner,
        )

    delivery_sinks = deliver_trade_push(
        settings,
        title="研究状态" if research_only else "挂单地图",
        text=text,
        kind="status" if research_only else "order_map",
        lane="ops" if research_only else "trade",
        friend=not research_only,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not research_only and not im_delivery_ok(delivery_sinks):
        append_missed(
            settings.missed_queue_path,
            text,
            kind="order_map_research" if research_only else "order_map",
            at=now,
        )

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer != "template",
        "im_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }
