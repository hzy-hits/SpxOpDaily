"""Order-map notification delivery (IM sinks)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.application.order_map.prompts import (
    GLOBEX_CONTEXT_SYSTEM_PROMPT,
    actionable_writer_output_valid,
    build_order_prompt,
    globex_writer_output_valid,
)
from spx_spark.application.order_map.render import render_template
from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import dispatch_notification
from spx_spark.notifier.llm_writer import (
    generate_push_text,
)
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope, notification_event_id


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
    text, writer = generate_push_text(
        template,
        build_order_prompt(payload, template, previous_push),
        settings,
        runner=runner,
        system=GLOBEX_CONTEXT_SYSTEM_PROMPT if research_only else None,
    )
    if writer != "template":
        valid = (
            globex_writer_output_valid(text, template)
            if research_only
            else actionable_writer_output_valid(text, template)
        )
        if not valid:
            text, writer = template, "template_validation_fallback"

    kind = "status" if research_only else "order_map"
    event_id = notification_event_id(
        kind,
        source="order_map",
        occurred_at=now,
        identity=str(payload.get("trading_date") or payload.get("as_of") or now.date()),
    )
    dispatch = dispatch_notification(
        settings,
        NotificationEnvelope(
            event_id=event_id,
            source="order_map",
            kind=kind,
            lane="scheduled_report",
            occurred_at=now,
        ),
        title="市场状态" if research_only else "条件交易地图",
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
        "used_agent": writer in {"grok_cli", "deepseek", "openclaw_agent"},
        "im_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }
