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
from spx_spark.application.order_map.desk_projection_export import (
    rust_report_owner_enabled,
)
from spx_spark.application.order_map.render import render_template
from spx_spark.config import NotificationSettings
from spx_spark.notifier.llm_writer import (
    call_hypothesis_critic,
    generate_push_text,
)
from spx_spark.notifier.dispatcher import enqueue_notification, notification_event_exists
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.model import NotificationEnvelope


def enqueue_strategy_decision(
    decision: dict[str, Any], *, now: datetime
) -> dict[str, Any]:
    """Send one deterministic unified candidate through the existing trade-ready lane."""

    candidate = decision.get("candidate")
    if decision.get("action_authority") != "manual" or not isinstance(candidate, dict):
        return {"accepted": False, "outcome": "no_manual_candidate"}
    opportunity_id = str(candidate.get("opportunity_id") or "")
    occurred_at = _timestamp(decision.get("decision_at")) or now
    expires_at = _timestamp(candidate.get("opportunity_valid_until"))
    if not opportunity_id or expires_at is None or expires_at <= now:
        return {"accepted": False, "outcome": "candidate_identity_or_ttl_invalid"}
    settings = NotificationSettings.from_env()
    event_id = f"{opportunity_id}:ready"
    if notification_event_exists(settings, event_id):
        return {
            "accepted": True,
            "inserted": False,
            "duplicate": True,
            "outcome": "outbox_already_accepted",
            "event_id": event_id,
        }
    text = _render_strategy_candidate(decision, candidate)
    result = enqueue_notification(
        settings,
        NotificationEnvelope(
            event_id=event_id,
            source="strategy_decision",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=occurred_at,
            expires_at=expires_at,
            operator_opportunity_id=opportunity_id,
        ),
        title="SPX STRATEGY DECISION",
        text=text,
        feishu_text=text,
        friend=True,
        enqueued_at=now,
    )
    return {
        "accepted": result.accepted,
        "inserted": result.inserted,
        "duplicate": result.duplicate,
        "outcome": result.outcome,
        "event_id": result.envelope.event_id,
        "targets": list(result.targets),
    }


def _render_strategy_candidate(decision: dict[str, Any], candidate: dict[str, Any]) -> str:
    quote, economics = candidate.get("quote") or {}, candidate.get("economics") or {}
    utility = candidate.get("utility") or {}
    evidence = decision.get("probability_evidence") or {}
    legs = candidate.get("legs") or [candidate.get("long"), candidate.get("short")]
    contracts = " / ".join(
        str(leg.get("contract_id") or "-")
        for leg in legs
        if isinstance(leg, dict)
    )
    invalidation = candidate.get("invalidation_spx")
    return "\n".join((
        "SPX STRATEGY DECISION · MANUAL CANDIDATE",
        f"Desk View  {candidate.get('setup_kind')} · {candidate.get('direction')} · 仅人工限价",
        f"Execution  {contracts} · synthetic BBO {quote.get('bid')}/{quote.get('ask')} · 净借记 ≤ {quote.get('ask')}",
        f"有效期  {candidate.get('opportunity_valid_until')} · 提交前必须刷新报价 · 禁止市价",
        f"Risk  最大亏损 ${float(economics.get('max_loss_points') or 0) * 100:.0f} · SPX失效 {invalidation}",
        f"Targets  SPX {candidate.get('target_spx')}",
        f"Edge  P={utility.get('event_probability')} · Q={evidence.get('q')} "
        f"· Utility={utility.get('utility')} "
        f"· lower=${utility.get('conservative_lower_bound')}",
        f"样本  n={evidence.get('n_raw')} · n_eff={evidence.get('n_effective')} "
        f"· shrink={evidence.get('shrinkage_weight')}",
        "Data Quality  conservative BBO · uncalibrated bootstrap · automatic_ordering=false",
    ))


def _timestamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


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
    if rust_report_owner_enabled():
        raise RuntimeError(
            "legacy scheduled-report writer is fenced while Rust owns reports"
        )
    now = now or datetime.now(tz=timezone.utc)
    radar = payload.get("convexity_idea_radar")
    if isinstance(radar, dict):
        critic, critic_error = call_hypothesis_critic(radar)
        payload = {**payload, "convexity_idea_critic": critic or {
            "status": "deterministic_fallback", "reason": critic_error or "critic_unavailable"
        }}
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
