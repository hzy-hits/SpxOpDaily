"""Order-map notification delivery (IM sinks)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
from spx_spark.application.order_map.path_distribution import path_distribution_desk_text
from spx_spark.application.order_map.render import render_template
from spx_spark.config import NotificationSettings
from spx_spark.notifier.llm_writer import (
    call_hypothesis_critic,
    call_strategy_idea_memo,
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
    # Repeat cycles of the same stable opportunity are an outbox duplicate,
    # never a flood-control hit, so the dedup check must run first.
    if notification_event_exists(settings, event_id):
        return {
            "accepted": True,
            "inserted": False,
            "duplicate": True,
            "outcome": "outbox_already_accepted",
            "event_id": event_id,
        }
    flood = _flood_control_block(decision, candidate, settings, now=now)
    if flood is not None:
        return flood
    text = _render_strategy_candidate(decision, candidate)
    # Memo is research-only and must never block trade_ready delivery.
    memo = None
    memo_error: str | None = None
    try:
        memo, memo_error = call_strategy_idea_memo(decision)
    except Exception as exc:  # noqa: BLE001 - fail-open for bounded LLM side path
        memo, memo_error = None, f"idea_memo_exception:{type(exc).__name__}"
    if memo is not None:
        text = f"{text}\n\n{_render_strategy_idea_memo(memo)}"
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
    outcome = {
        "accepted": result.accepted,
        "inserted": result.inserted,
        "duplicate": result.duplicate,
        "outcome": result.outcome,
        "event_id": result.envelope.event_id,
        "targets": list(result.targets),
    }
    if memo is None:
        outcome["idea_memo"] = f"omitted:{memo_error or 'unavailable'}"
    return outcome


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
    decision_id = str(decision.get("decision_id") or "-")
    policy = str(decision.get("policy_version") or "-")
    git_sha = str(decision.get("runtime_git_sha") or "-")
    edge = candidate.get("edge") if isinstance(candidate.get("edge"), dict) else {}
    edge_status = edge.get("edge_status") or "research_unvalidated"
    policy_ev = _policy_ev_text(edge)
    path_text = path_distribution_desk_text(edge.get("path_distribution") if isinstance(edge.get("path_distribution"), dict) else None)
    path_line = f" · {path_text}" if path_text else ""
    if candidate.get("setup_kind") == "IRON_CONDOR_DELTA":
        return "\n".join((
            "SPX STRATEGY DECISION · MANUAL CANDIDATE",
            "Desk View  铁鹰 25Δ/5Δ · 仅人工限价",
            f"Execution  {contracts} · conservative credit {quote.get('credit')} · 净贷记 ≥ {quote.get('credit')}",
            f"有效期  {candidate.get('opportunity_valid_until')} · 提交前必须刷新报价 · 禁止市价",
            f"Risk  最大亏损 ${float(economics.get('max_loss_points') or 0) * 100:.0f} · 短腿失效 {invalidation}",
            f"Targets  短腿中点 {candidate.get('target_spx')}",
            f"Edge  status={edge_status} · 研究未校准{path_line} · {policy_ev}",
            "Data Quality  conservative credit BBO · automatic_ordering=false",
            f"决策 id={decision_id[:12]} 角色=MANUAL_CANDIDATE 原因=IRON_CONDOR_DELTA "
            f"版本 policy={policy} git={git_sha}",
        ))
    if candidate.get("setup_kind") == "EVENT_SETTLEMENT_THRESHOLD":
        view = candidate.get("view") if isinstance(candidate.get("view"), dict) else {}
        probability_event = (
            candidate.get("probability_event")
            if isinstance(candidate.get("probability_event"), dict)
            else {}
        )
        direction = str(candidate.get("direction") or "")
        relation = "高于" if direction == "UP" else "低于"
        threshold = view.get("threshold_level")
        odds = view.get("market_odds_proxy")
        gap = view.get("breakeven_gap_points")
        breakeven = economics.get("breakeven_spx")
        gap_text = (
            f"需比观点阈值额外走 {float(gap):+.2f}pt"
            if isinstance(gap, int | float)
            else "观点与结构阈值差 unavailable"
        )
        return "\n".join((
            "SPX STRATEGY DECISION · MANUAL CANDIDATE",
            f"观点  SPX 到期结算{relation}前收 {threshold} · {view.get('macro_event_name') or '事件'}",
            f"命题  {probability_event.get('kind')} · target_at={probability_event.get('target_at')}",
            f"Execution  {contracts} · synthetic BBO {quote.get('bid')}/{quote.get('ask')} · 净借记 ≤ {quote.get('ask')}",
            f"赔率  Debit/Width={_percent(odds)} · BE={breakeven} · {gap_text}",
            f"Risk  最大亏损 ${float(economics.get('max_loss_points') or 0) * 100:.0f} · 事件跳空时普通止损不保证成交",
            f"有效期  {candidate.get('opportunity_valid_until')} · 事件发布后原观点过期 · 禁止市价",
            f"Edge  status={edge_status} · P=未估计 · required≈{_percent(edge.get('required_p_breakeven'))} · {policy_ev}",
            "Data Quality  conservative BBO · thesis-driven/unvalidated · automatic_ordering=false",
            f"决策 id={decision_id[:12]} 角色=MANUAL_CANDIDATE 原因=EVENT_SETTLEMENT_THRESHOLD "
            f"版本 policy={policy} git={git_sha}",
        ))
    return "\n".join((
        "SPX STRATEGY DECISION · MANUAL CANDIDATE",
        f"Desk View  {candidate.get('setup_kind')} · {candidate.get('direction')} · 仅人工限价",
        f"Execution  {contracts} · synthetic BBO {quote.get('bid')}/{quote.get('ask')} · 净借记 ≤ {quote.get('ask')}",
        f"有效期  {candidate.get('opportunity_valid_until')} · 提交前必须刷新报价 · 禁止市价",
        f"Risk  最大亏损 ${float(economics.get('max_loss_points') or 0) * 100:.0f} · SPX失效 {invalidation}",
        f"Targets  SPX {candidate.get('target_spx')}",
        f"Edge  status={edge_status} · P={utility.get('event_probability')} · Q={evidence.get('q')} "
        f"· Utility={utility.get('utility')} "
        f"· lower=${utility.get('conservative_lower_bound')} · {policy_ev}{path_line}",
        f"样本  n={evidence.get('n_raw')} · n_eff={evidence.get('n_effective')} "
        f"· shrink={evidence.get('shrinkage_weight')}",
        "Data Quality  conservative BBO · uncalibrated bootstrap · automatic_ordering=false",
        f"决策 id={decision_id[:12]} 角色=MANUAL_CANDIDATE 原因={candidate.get('setup_kind') or '-'} "
        f"版本 policy={policy} git={git_sha}",
    ))


def _percent(value: object) -> str:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return f"{float(value):.1%}"
    return "n/a"


def _policy_ev_text(edge: dict[str, Any]) -> str:
    value = edge.get("policy_ev")
    n = edge.get("policy_ev_n")
    if isinstance(value, int | float) and not isinstance(value, bool):
        if isinstance(n, int | float) and not isinstance(n, bool):
            return f"policyEV={float(value):g}({int(n)})"
        return f"policyEV={float(value):g}"
    return "policyEV=n/a"


def _render_strategy_idea_memo(memo: dict[str, Any]) -> str:
    watch_levels = ", ".join(str(level) for level in memo.get("watch_levels") or ()) or "-"
    falsification = " | ".join(str(item) for item in memo.get("falsification") or ()) or "-"
    risks = " | ".join(str(item) for item in memo.get("risks") or ()) or "-"
    return "\n".join((
        "Idea Memo (research)",
        f"thesis  {memo.get('thesis') or '-'}",
        f"falsification  {falsification}",
        f"watch_levels  {watch_levels}",
        f"risks  {risks}",
    ))


def _timestamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _flood_control_block(
    decision: dict[str, Any],
    candidate: dict[str, Any],
    settings: NotificationSettings,
    *,
    now: datetime,
) -> dict[str, Any] | None:
    """Rate-limit human cards by what the outbox actually accepted.

    The authority is delivered/accepted notification events, not produced
    decisions: a decision persisted as ``selected`` whose card never reached
    the outbox must not consume quota, and the current decision (already
    persisted before delivery) must never block itself.
    """

    from spx_spark.application.order_map.strategy_regime import DEFAULT_STRATEGY_POLICY
    from spx_spark.infrastructure.operational_db import recent_selected_strategy_cards

    session_date = str(decision.get("session_date") or "")
    setup_kind = str(candidate.get("setup_kind") or "")
    direction = str(candidate.get("direction") or "")
    if not session_date or not setup_kind or not direction:
        return None
    trigger = candidate.get("trigger_level")
    trigger_level = float(trigger) if isinstance(trigger, (int, float)) else None
    opportunity_id = str(candidate.get("opportunity_id") or "")
    rows = recent_selected_strategy_cards(
        session_date=session_date,
        exclude_decision_id=str(decision.get("decision_id") or "") or None,
    )
    accepted: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_opportunity = str(row.get("opportunity_id") or "")
        if not row_opportunity or row_opportunity == opportunity_id:
            continue
        known = accepted.get(row_opportunity)
        if known is not None:
            if row["decision_at"] > known["decision_at"]:
                accepted[row_opportunity] = row
            continue
        if notification_event_exists(settings, f"{row_opportunity}:ready"):
            accepted[row_opportunity] = row
    cooldown_start = now - timedelta(
        seconds=max(DEFAULT_STRATEGY_POLICY.candidate_cooldown_seconds, 0.0)
    )
    session_direction = 0
    cooldown_hits = 0
    for row in accepted.values():
        if str(row.get("direction") or "").upper() != direction.upper():
            continue
        session_direction += 1
        if str(row.get("setup_kind") or "") != setup_kind:
            continue
        if row["decision_at"] < cooldown_start:
            continue
        stored_trigger = row.get("trigger_level")
        if trigger_level is None or stored_trigger is None:
            cooldown_hits += 1
        elif abs(float(stored_trigger) - float(trigger_level)) <= 0.01:
            cooldown_hits += 1
    counts = {"session_direction": session_direction, "cooldown_hits": cooldown_hits}
    if cooldown_hits > 0:
        return {
            "accepted": False,
            "outcome": "flood_control_cooldown",
            "counts": counts,
        }
    if session_direction >= DEFAULT_STRATEGY_POLICY.max_cards_per_direction_per_session:
        return {
            "accepted": False,
            "outcome": "flood_control_session_cap",
            "counts": counts,
        }
    return None


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
