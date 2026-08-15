"""Order-map notification delivery (IM sinks)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections.abc import Mapping
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
from spx_spark.application.order_map.models import SHANGHAI_TZ
from spx_spark.application.order_map.path_distribution import path_distribution_desk_text
from spx_spark.application.order_map.render import render_template
from spx_spark.application.order_map.strategy_ranker import (
    outbox_accepted_strategy_cards,
    session_direction_lock,
)
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    butterfly_max_entry_minutes,
    pin_stable_next_step_text,
    pin_stable_watch_phase,
    pin_watch_center,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.config import NotificationSettings
from spx_spark.notifier.llm_writer import (
    call_hypothesis_critic,
    call_strategy_idea_memo,
    generate_push_text,
)
from spx_spark.notifier.dispatcher import enqueue_notification, notification_event_exists
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.model import NotificationEnvelope


def enqueue_pin_stable_watch(
    decision: Mapping[str, Any], *, now: datetime
) -> dict[str, Any]:
    """Send one session-deduped LOOK/TRADE pin observation. Not a trade card."""

    regime = decision.get("regime") if isinstance(decision.get("regime"), dict) else {}
    facts = (
        decision.get("market_facts")
        if isinstance(decision.get("market_facts"), dict)
        else {}
    )
    session = facts.get("session") if isinstance(facts.get("session"), dict) else {}
    center = pin_watch_center(regime)
    session_date = str(decision.get("session_date") or facts.get("session_date") or "")
    if (
        center is None
        or not session_date
        or str(session.get("mode") or "").strip().lower() == "gth"
    ):
        return {"accepted": False, "outcome": "pin_stable_watch_not_applicable"}
    minutes = _finite_number(facts.get("minutes_to_close"))
    phase = pin_stable_watch_phase(minutes, DEFAULT_STRATEGY_POLICY)
    event_id = f"pin-stable:{session_date}:{center:g}:{phase}"
    occurred_at = _timestamp(decision.get("decision_at")) or now
    expires_at = _session_close_utc(session_date)
    if expires_at is None or expires_at <= occurred_at:
        return {"accepted": False, "outcome": "pin_stable_watch_expired"}
    settings = NotificationSettings.from_env()
    if notification_event_exists(settings, event_id):
        return {
            "accepted": True,
            "inserted": False,
            "duplicate": True,
            "outcome": "outbox_already_accepted",
            "event_id": event_id,
        }
    text = _render_pin_stable_watch(decision, center=center, minutes=minutes, phase=phase)
    result = enqueue_notification(
        settings,
        NotificationEnvelope(
            event_id=event_id,
            source="strategy_decision",
            kind="pin_stable_watch",
            lane="strategy_watch",
            occurred_at=occurred_at,
            expires_at=expires_at,
        ),
        title=_pin_watch_title(regime, center),
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
        title=_strategy_card_title(candidate),
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


def _pin_watch_title(regime: Mapping[str, Any], center: float) -> str:
    pin = regime.get("pin") if isinstance(regime.get("pin"), dict) else {}
    if str(pin.get("grade") or "") == "look" and regime.get("terminal_state") != "PIN_STABLE":
        return f"SPX 观察 · 今日中轴 {center:g}"
    return f"SPX 观察 · 稳定钉住 {center:g}"


def _render_pin_stable_watch(
    decision: Mapping[str, Any],
    *,
    center: float,
    minutes: float | None,
    phase: str,
) -> str:
    regime = decision.get("regime") if isinstance(decision.get("regime"), dict) else {}
    pin = regime.get("pin") if isinstance(regime.get("pin"), dict) else {}
    depin = _finite_number(pin.get("depin_risk"))
    limit = butterfly_max_entry_minutes(5.0, DEFAULT_STRATEGY_POLICY)
    clock = (
        f"距收盘 {minutes:g} 分钟"
        if minutes is not None
        else "距收盘暂缺"
    )
    if phase == "look":
        conclusion = f"钉住中心 {center:g} · 11–13 可看今日蝶 · 不自动下单"
        clock_line = "5 点蝶午盘窗 11:00–13:00 ET"
    elif phase == "clock_open":
        conclusion = f"钉住中心 {center:g} · 尾盘 5 点蝶时钟已开 · 不自动下单"
        clock_line = (
            f"5 点蝶尾盘门 ≤{limit:g} 分钟"
            if limit is not None
            else "5 点蝶尾盘门暂缺"
        )
    else:
        conclusion = f"钉住中心 {center:g} · 午盘看蝶窗已过 · 不自动下单"
        clock_line = (
            f"5 点蝶尾盘门 ≤{limit:g} 分钟（约 14:50 ET）"
            if limit is not None
            else "5 点蝶尾盘门暂缺"
        )
    depin_text = f"{depin:.2f}" if depin is not None else "暂缺"
    heading = _pin_watch_title(regime, center)
    return "\n".join(
        (
            f"【{heading}】",
            "",
            "## 结论",
            conclusion,
            "",
            "## 结构",
            f"{clock} · De-pin {depin_text}",
            clock_line,
            "",
            "## 下一步",
            pin_stable_next_step_text(minutes, DEFAULT_STRATEGY_POLICY),
            "",
            "## 数据",
            "不下自动单 · 人工观察",
        )
    )


def _session_close_utc(session_date: str) -> datetime | None:
    try:
        session = DEFAULT_MARKET_CALENDAR.session(datetime.fromisoformat(session_date).date())
    except ValueError:
        return None
    if session is None:
        return None
    close = session.close_at
    return close.astimezone(timezone.utc) if close.tzinfo else close.replace(tzinfo=timezone.utc)


def _render_strategy_candidate(decision: dict[str, Any], candidate: dict[str, Any]) -> str:
    quote, economics = candidate.get("quote") or {}, candidate.get("economics") or {}
    setup = str(candidate.get("setup_kind") or "")
    edge = candidate.get("edge") if isinstance(candidate.get("edge"), dict) else {}
    path_text = path_distribution_desk_text(
        edge.get("path_distribution") if isinstance(edge.get("path_distribution"), dict) else None
    )
    title = _strategy_card_title(candidate)
    until = _beijing_clock(candidate.get("opportunity_valid_until"))
    loss = _usd_loss(economics.get("max_loss_points"))
    if setup == "IRON_CONDOR_DELTA":
        credit = quote.get("credit")
        strikes = _iron_condor_strike_text(candidate)
        invalidation = _strike_pair(candidate.get("invalidation_spx")) or "-"
        target = _fmt_strike(candidate.get("target_spx"))
        lines = [
            f"【{title}】",
            "",
            "## 结论",
            f"铁鹰 卖5–20Δ 10点翼宽 {strikes} · 只许限价",
            "",
            "## 执行",
            f"四腿 {strikes}",
            f"净贷记 ≥ {_fmt_premium(credit)} · 提交前刷新报价 · 禁止市价",
            f"有效至 {until}（北京）",
            "",
            "## 风险",
            f"最大亏损 {loss} · 短腿 {invalidation} 被打穿即失效",
            "",
            "## 目标",
            f"短腿中点 {target} · 收到权利金即最大收益",
        ]
    elif str(candidate.get("strategy_type") or "").endswith("_BUTTERFLY"):
        ask = quote.get("ask")
        invalidation = _range_text(candidate.get("invalidation_spx"))
        target = _fmt_strike(candidate.get("target_spx") or candidate.get("center"))
        width = economics.get("width_points") or candidate.get("width")
        debit_frac = economics.get("debit_fraction_of_width")
        payoff = ""
        if isinstance(width, int | float) and isinstance(debit_frac, int | float):
            payoff = f" · 翼宽 {_fmt_strike(width)} 点 · 借记占 {_percent(debit_frac)}"
        lines = [
            f"【{title}】",
            "",
            "## 结论",
            f"{_setup_cn(setup)} · {_direction_cn(candidate.get('direction'))} · 只许限价",
            "",
            "## 执行",
            f"{_butterfly_leg_text(candidate)}",
            f"净借记 ≤ {_fmt_premium(ask)} · 提交前刷新报价 · 禁止市价",
            f"有效至 {until}（北京）",
            "",
            "## 风险",
            f"最大亏损 {loss} · SPX 离开 {invalidation} 失效",
            "",
            "## 目标",
            f"SPX {target}{payoff}",
        ]
    elif setup == "EVENT_SETTLEMENT_THRESHOLD":
        view = candidate.get("view") if isinstance(candidate.get("view"), dict) else {}
        direction = str(candidate.get("direction") or "")
        relation = "高于" if direction == "UP" else "低于"
        legs = _vertical_leg_text(candidate)
        ask = quote.get("ask")
        gap = view.get("breakeven_gap_points")
        gap_text = (
            f"要比观点阈值再走 {float(gap):+.1f} 点才到盈亏平衡"
            if isinstance(gap, int | float)
            else "观点与结构阈值差暂缺"
        )
        lines = [
            f"【{title}】",
            "",
            "## 结论",
            f"事件结算观点 · SPX 到期结算{relation}前收 {_fmt_strike(view.get('threshold_level'))}",
            str(view.get("macro_event_name") or "宏观事件"),
            "",
            "## 执行",
            f"{legs} · 净借记 ≤ {_fmt_premium(ask)} · 禁止市价",
            f"有效至 {until}（北京）· 事件发布后原观点过期",
            "",
            "## 风险",
            f"最大亏损 {loss} · 跳空时普通止损不保证成交",
            "",
            "## 目标",
            f"盈亏平衡 {_fmt_strike(economics.get('breakeven_spx'))} · {gap_text}",
        ]
    else:
        legs = _vertical_leg_text(candidate)
        ask = quote.get("ask")
        invalidation = _fmt_strike(candidate.get("invalidation_spx"))
        target = _fmt_strike(candidate.get("target_spx"))
        width = economics.get("width_points")
        debit_frac = economics.get("debit_fraction_of_width")
        payoff = ""
        if isinstance(width, int | float) and isinstance(debit_frac, int | float):
            payoff = f" · 翼宽 {_fmt_strike(width)} 点 · 借记占 {_percent(debit_frac)}"
        lines = [
            f"【{title}】",
            "",
            "## 结论",
            f"{_setup_cn(setup)} · {_direction_cn(candidate.get('direction'))} · 只许限价",
            "",
            "## 执行",
            f"{legs}",
            f"净借记 ≤ {_fmt_premium(ask)} · 提交前刷新报价 · 禁止市价",
            f"有效至 {until}（北京）",
            "",
            "## 风险",
            f"最大亏损 {loss} · SPX 跌破 {invalidation} 失效"
            if str(candidate.get("direction") or "") == "UP"
            else f"最大亏损 {loss} · SPX 升破 {invalidation} 失效",
            "",
            "## 目标",
            f"SPX {target}{payoff}",
        ]
    lines.extend(
        (
            "",
            "## 数据",
            "不下自动单 · 人工限价",
        )
    )
    if path_text:
        lines.append(f"{path_text}（未校准，不改结论）")
    del decision
    return "\n".join(lines)


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _percent(value: object) -> str:
    number = _finite_number(value)
    if number is None:
        return "暂缺"
    return f"{number:.0%}"


def _fmt_premium(value: object) -> str:
    number = _finite_number(value)
    if number is None:
        return "暂缺"
    return f"{number:.2f}"


def _fmt_strike(value: object) -> str:
    number = _finite_number(value)
    if number is None:
        return "-"
    return f"{number:.0f}" if number.is_integer() else f"{number:.1f}"


def _usd_loss(points: object) -> str:
    number = _finite_number(points)
    if number is None:
        return "暂缺"
    return f"${number * 100:.0f}"


def _setup_cn(setup: object) -> str:
    return {
        "GTH_DELTA_SCAN": "夜盘 delta 扫描",
        "GTH_WIDTH_SCAN": "夜盘宽度扫描",
        "GTH_ATM_PIN": "夜盘钉住",
        "IRON_CONDOR_DELTA": "铁鹰",
        "EVENT_SETTLEMENT_THRESHOLD": "事件结算观点",
        "TREND_PULLBACK": "趋势回踩",
        "ES_VOLUME_MOMENTUM": "ES量比动量",
        "STABLE_PIN": "稳定钉住",
        "CONFIRMATION_TARGET_PIN": "目标钉住",
    }.get(str(setup or ""), "结构扫描")


def _direction_cn(direction: object) -> str:
    return {"UP": "看涨", "DOWN": "看跌", "NEUTRAL": "中性"}.get(str(direction or ""), "方向未定")


def _strategy_type_cn(candidate: dict[str, Any]) -> str:
    raw = str(candidate.get("strategy_type") or "")
    return {
        "CALL_DEBIT_VERTICAL": "Call 价差",
        "PUT_DEBIT_VERTICAL": "Put 价差",
        "CALL_BUTTERFLY": "Call 蝶式",
        "PUT_BUTTERFLY": "Put 蝶式",
        "IRON_CONDOR": "铁鹰",
    }.get(raw, _setup_cn(candidate.get("setup_kind")))


def _leg_token(leg: object) -> str:
    if not isinstance(leg, dict):
        return "-"
    strike = leg.get("strike")
    right = str(leg.get("right") or "").upper()
    if strike is None:
        parts = str(leg.get("contract_id") or "").split(":")
        if len(parts) >= 2:
            strike, right = parts[-2], parts[-1].upper()
    if strike is None:
        return "-"
    return f"{_fmt_strike(strike)}{right}"


def _vertical_leg_text(candidate: dict[str, Any]) -> str:
    return f"买 {_leg_token(candidate.get('long'))} / 卖 {_leg_token(candidate.get('short'))}"


def _butterfly_leg_text(candidate: dict[str, Any]) -> str:
    legs = candidate.get("legs")
    if isinstance(legs, list) and len(legs) == 3:
        tokens = [_leg_token(leg) for leg in legs]
        if all(token != "-" for token in tokens):
            return f"买 {tokens[0]} / 卖 2×{tokens[1]} / 买 {tokens[2]}"
    center = _finite_number(candidate.get("center"))
    width = _finite_number(candidate.get("width"))
    right = str(candidate.get("right") or "").upper()
    if center is None or width is None or right not in {"C", "P"}:
        return "买 - / 卖 2×- / 买 -"
    return (
        f"买 {_fmt_strike(center - width)}{right} / "
        f"卖 2×{_fmt_strike(center)}{right} / "
        f"买 {_fmt_strike(center + width)}{right}"
    )


def _range_text(value: object) -> str:
    pair = _strike_pair(value)
    if pair is not None:
        return pair.replace("/", "–")
    return _fmt_strike(value)


def _iron_condor_strike_text(candidate: dict[str, Any]) -> str:
    strikes = candidate.get("strikes")
    if isinstance(strikes, list) and len(strikes) == 4:
        return "/".join(_fmt_strike(value) for value in strikes)
    legs = [
        candidate.get("put_long"),
        candidate.get("put_short"),
        candidate.get("call_short"),
        candidate.get("call_long"),
    ]
    tokens = [_leg_token(leg) for leg in legs if isinstance(leg, dict)]
    return "/".join(tokens) if tokens else "-"


def _strike_pair(value: object) -> str | None:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return "/".join(_fmt_strike(item) for item in value)
    if isinstance(value, int | float) and not isinstance(value, bool):
        return _fmt_strike(value)
    return None


def _beijing_clock(value: object) -> str:
    parsed = _timestamp(value)
    if parsed is None:
        return "刷新前"
    return parsed.astimezone(SHANGHAI_TZ).strftime("%H:%M")


def _strategy_card_title(candidate: dict[str, Any]) -> str:
    kind = _strategy_type_cn(candidate)
    if candidate.get("setup_kind") == "IRON_CONDOR_DELTA":
        return f"SPX 人工候选 · {kind} {_iron_condor_strike_text(candidate)}"
    long_token = _leg_token(candidate.get("long"))
    short_token = _leg_token(candidate.get("short"))
    if long_token != "-" and short_token != "-":
        return f"SPX 人工候选 · {kind} {long_token}/{short_token}"
    return f"SPX 人工候选 · {kind}"


def _render_strategy_idea_memo(memo: dict[str, Any]) -> str:
    watch_levels = "、".join(_fmt_strike(level) for level in memo.get("watch_levels") or ()) or "-"
    falsification = "；".join(str(item) for item in memo.get("falsification") or ()) or "-"
    risks = "；".join(str(item) for item in memo.get("risks") or ()) or "-"
    return "\n".join((
        "## 研究备忘",
        f"看法  {memo.get('thesis') or '-'}",
        f"证伪  {falsification}",
        f"盯盘  {watch_levels}",
        f"风险  {risks}",
        "以上只解释结构，不授权下单",
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
    session_mode = _decision_session_mode(decision, candidate)
    if not session_date or not setup_kind or not direction:
        return None
    trigger = candidate.get("trigger_level")
    trigger_level = float(trigger) if isinstance(trigger, (int, float)) else None
    opportunity_id = str(candidate.get("opportunity_id") or "")
    rows = recent_selected_strategy_cards(
        session_date=session_date,
        exclude_decision_id=str(decision.get("decision_id") or "") or None,
    )
    accepted = outbox_accepted_strategy_cards(
        rows,
        event_exists=lambda event_id: notification_event_exists(settings, event_id),
        exclude_opportunity_id=opportunity_id,
    )
    cooldown_start = now - timedelta(
        seconds=max(DEFAULT_STRATEGY_POLICY.candidate_cooldown_seconds, 0.0)
    )
    session_direction = 0
    cooldown_hits = 0
    for row in accepted:
        if str(row.get("direction") or "").upper() != direction.upper():
            continue
        if str(row.get("session_mode") or session_mode) != session_mode:
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
    if session_mode in {"gth", "rth"}:
        stick_seconds = (
            DEFAULT_STRATEGY_POLICY.gth_winner_stick_seconds
            if session_mode == "gth"
            else DEFAULT_STRATEGY_POLICY.rth_winner_stick_seconds
        )
        lock = session_direction_lock(
            accepted,
            now=now,
            stick_seconds=stick_seconds,
            session_mode=session_mode,
        )
        if lock is not None and direction.upper() != lock.direction.upper():
            return {
                "accepted": False,
                "outcome": (
                    "flood_control_gth_direction_lock"
                    if session_mode == "gth"
                    else "flood_control_rth_direction_lock"
                ),
                "counts": {
                    **counts,
                    "locked_direction": lock.direction,
                    "lock_started_at": lock.started_at.isoformat(),
                },
            }
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


def _decision_session_mode(decision: Mapping[str, Any], candidate: Mapping[str, Any]) -> str:
    facts = decision.get("market_facts") if isinstance(decision.get("market_facts"), dict) else {}
    session = facts.get("session") if isinstance(facts, dict) and isinstance(facts.get("session"), dict) else {}
    mode = str(session.get("mode") or "").strip().lower()
    if mode in {"gth", "rth"}:
        return mode
    setup = str(candidate.get("setup_kind") or "")
    if setup.startswith("GTH_"):
        return "gth"
    return "rth"


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
