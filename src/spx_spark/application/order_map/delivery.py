"""Order-map notification delivery (IM sinks)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.application.notifications.report_enqueue import (
    daily_report_semantic,
    enqueue_report_notification,
)
from spx_spark.application.order_map.prompts import (
    GLOBEX_CONTEXT_SYSTEM_PROMPT,
    actionable_writer_output_valid,
    build_order_prompt,
    globex_writer_output_valid,
)
from spx_spark.application.order_map.render import render_template
from spx_spark.config import NotificationSettings
from spx_spark.notifier.llm_writer import (
    generate_push_text,
)
from spx_spark.notifier.model import CommandRunner, default_runner


def send_order_map(
    payload: dict[str, Any],
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    extra_header: str | None = None,
    previous_push: dict[str, Any] | None = None,
    event_identity: str | None = None,
    occurred_at: datetime | None = None,
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
    daily_semantic = (
        daily_report_semantic(
            payload,
            now=now,
            kind="order_map",
            source="order_map",
            identity_label="baseline_trading_date",
        )
        if not research_only and not extra_header
        else None
    )
    if (research_only or extra_header) and (occurred_at is None or event_identity is None):
        raise ValueError("status/refresh delivery requires an explicit stable semantic identity")
    if occurred_at is None:
        assert daily_semantic is not None
        occurred_at = daily_semantic.occurred_at
    if event_identity is None:
        assert daily_semantic is not None
        event_identity = daily_semantic.identity
    enqueue = enqueue_report_notification(
        settings,
        source="order_map",
        kind=kind,
        lane="scheduled_report",
        occurred_at=occurred_at,
        identity=event_identity,
        title="市场状态" if research_only else "条件交易地图",
        text=text,
        friend=True,
        enqueued_at=now,
    )

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer in {"grok_cli", "deepseek", "openclaw_agent"},
        "occurred_at": occurred_at.isoformat(),
        **enqueue,
    }
