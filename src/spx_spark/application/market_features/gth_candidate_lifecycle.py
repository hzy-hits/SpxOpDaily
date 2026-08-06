"""Lifecycle classification for manual GTH candidate delivery and replay."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Mapping

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.notifier.operator_cards import (
    beijing_time,
    option_contract_label,
    render_operator_card,
)


_REFRESH_BLOCKERS = frozenset(
    {
        "chain_implied_target_unavailable",
        "direct_es_invalidation_unavailable",
        "es_basis_unavailable",
        "long_leg_quote_in_future",
        "long_leg_quote_stale",
        "long_leg_quote_unavailable",
        "long_leg_transport_in_future",
        "long_leg_transport_stale",
        "provider_entry_control_blocked",
        "quote_unavailable",
        "short_leg_quote_in_future",
        "short_leg_quote_stale",
        "short_leg_quote_unavailable",
        "short_leg_transport_in_future",
        "short_leg_transport_stale",
        "spread_entry_limit_invalid",
        "spread_leg_nbbo_invalid",
        "spread_leg_provider_mismatch",
        "spread_leg_provider_unavailable",
        "spread_leg_source_time_unavailable",
        "spread_leg_source_timestamp_skew",
        "spread_leg_transport_time_unavailable",
        "spread_leg_transport_timestamp_skew",
        "spread_net_debit_invalid",
        "spread_net_nbbo_invalid",
        "spread_provider_not_ibkr",
        "spread_reward_risk_unavailable",
        "trend_anchor_geometry_unavailable",
        "trigger_level_unavailable",
    }
)

SourceLifecycleClass = Literal[
    "identified",
    "explicit_absence",
    "transient_absence",
]

_ACTIVE_PLAN_FIELDS = (
    "schema_version",
    "policy_version",
    "candidate_id",
    "source_signal_id",
    "reentry_generation",
    "direction",
    "path_kind",
    "position_type",
    "long_contract_id",
    "short_contract_id",
    "expiry",
    "session_date",
    "valid_until",
    "entry_limit",
    "trigger_level",
    "invalidation_spx",
    "invalidation_es",
    "invalidation_coordinate",
    "target_spx",
    "target_wall_kind",
    "exit_at",
    "automatic_ordering",
)

_TERMINAL_TTL_SECONDS = 15 * 60

_EXPLICIT_SOURCE_ABSENCE_REASONS = frozenset(
    {
        "level_source_expired",
        "level_source_expiry_unavailable",
        "level_source_formal_signal_absent",
        "level_source_invalidated",
        "level_source_not_confirmed",
        "level_source_quality_invalid",
        "trend_transition_session_mismatch",
    }
)

_GLOBAL_SOURCE_LIFECYCLE_END_REASONS = frozenset(
    {
        "trend_transition_session_mismatch",
    }
)


def classify_source_lifecycle(
    candidate: Mapping[str, object],
) -> SourceLifecycleClass:
    """Distinguish a source tombstone from a temporarily incomplete frame."""

    if candidate.get("source_signal_id"):
        return "identified"
    reasons = {str(item) for item in candidate.get("block_reasons") or () if item}
    if reasons & _EXPLICIT_SOURCE_ABSENCE_REASONS:
        return "explicit_absence"
    return "transient_absence"


def mark_refresh_pending(candidate: Mapping[str, object]) -> dict[str, object]:
    result = dict(candidate)
    reasons = {str(item) for item in result.get("block_reasons") or () if item}
    if (
        result.get("status") == "blocked"
        and result.get("source_signal_id")
        and reasons
        and reasons <= _REFRESH_BLOCKERS
    ):
        result.update(
            {
                "status": "refresh_pending",
                "manual_action_eligible": False,
                "signal_absence_reason": "market_data_refresh_pending",
            }
        )
    return result


def active_manual_plan_snapshot(
    candidate: Mapping[str, object],
    *,
    activated_at: object,
) -> dict[str, object]:
    candidate_id = str(candidate.get("candidate_id") or "")
    return {
        **{key: candidate.get(key) for key in _ACTIVE_PLAN_FIELDS},
        "ready_event_id": f"{candidate_id}:ready" if candidate_id else None,
        "activated_at": activated_at,
    }


def terminal_notification_intent(
    active_plan: Mapping[str, object],
    candidate: Mapping[str, object],
    *,
    causation_event_id: str,
    occurred_at: datetime,
    enqueued_at: datetime | None = None,
    release_reason: str | None = None,
) -> dict[str, object]:
    """Build one explicit terminal message without claiming a manual fill."""

    plan = dict(active_plan)
    reasons = tuple(str(item) for item in candidate.get("block_reasons") or () if item)
    reason_code = release_reason or _terminal_reason(reasons)
    action = (
        "exit"
        if release_reason is not None or any("invalidated" in reason for reason in reasons)
        else "cancel"
    )
    ready_event_id = str(plan.get("ready_event_id") or causation_event_id)
    event_id = f"{ready_event_id}:{action}"
    side = "PUT" if str(plan.get("direction") or "") == "down" else "CALL"
    long_contract = _contract_label(plan.get("long_contract_id"))
    short_contract = _contract_label(plan.get("short_contract_id"))
    action_label = "EXIT REVIEW" if action == "exit" else "READY CANCELLED"
    execution = (
        "EXIT REVIEW · 仅人工处理\n"
        "未成交  不操作；不要因为本消息新建仓位\n"
        "已成交  系统不知道你的成交状态；立即重新报价并人工限价退出/处置\n"
        "确认  本消息不代表订单已撤销、仓位已平仓或券商已接受指令"
        if action == "exit"
        else "CANCEL ENTRY · 禁止继续提交本 READY\n"
        "未成交  撤销挂单或忽略原卡\n"
        "已成交  系统不知道你的成交状态；立即核对券商并按原风险边界人工管理\n"
        "确认  本消息不代表订单已撤销、仓位已平仓或券商已接受指令"
    )
    risk_lines = [
        f"原卡  {ready_event_id}",
        f"合约  买 {long_contract} / 卖 {short_contract}",
        f"原因  {reason_code}",
    ]
    invalidation_spx = _number(plan.get("invalidation_spx"))
    invalidation_es = _number(plan.get("invalidation_es"))
    if invalidation_spx is not None:
        risk_lines.append(f"原止损  SPX {invalidation_spx:.2f}")
    if invalidation_es is not None:
        risk_lines.append(f"ES 风险线  {invalidation_es:.2f}")
    risk_lines.append("权限  自动下单、撤单和平仓均关闭")
    target = _number(plan.get("target_spx"))
    target_text = f"原目标  SPX {target:.2f}" if target is not None else "原目标  不可用"
    text = render_operator_card(
        desk_view=(
            f"🔴 {action_label} · {side} SPREAD\n"
            f"原 READY 生命周期已终止：{reason_code}\n"
            "系统没有券商订单/成交回报，不推断你已成交或持仓"
        ),
        execution=execution,
        risk="\n".join(risk_lines),
        targets="\n".join(
            (
                target_text,
                f"原计划退出  {beijing_time(plan.get('exit_at'))}",
            )
        ),
        data_quality=(
            "终止依据来自候选状态机；该通知只关闭原 READY 的人工入场授权。"
        ),
    )
    delivery_clock = enqueued_at or occurred_at
    expires_at = delivery_clock + timedelta(seconds=_TERMINAL_TTL_SECONDS)
    return {
        "event_id": event_id,
        "causation_event_id": ready_event_id,
        "source": "gth_level_manual_candidate",
        "kind": "virtual_strategy_exit",
        "lane": "strategy_lifecycle",
        "occurred_at": occurred_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "candidate_id": plan.get("candidate_id"),
        "source_signal_id": plan.get("source_signal_id"),
        "operator_opportunity_id": str(
            plan.get("source_signal_id") or plan.get("candidate_id") or ready_event_id
        ),
        "operator_generation": _generation(plan.get("reentry_generation")),
        "terminal_action": action,
        "terminal_reason": reason_code,
        "title": f"SPX GTH {action_label} · MANUAL ONLY",
        "text": text,
        "friend": True,
        "feishu_text": text,
        "enqueued_at": delivery_clock.isoformat(),
    }


def _terminal_reason(reasons: tuple[str, ...]) -> str:
    priority = (
        "level_source_invalidated",
        "trend_transition_session_mismatch",
        "level_source_expired",
        "level_source_not_confirmed",
        "level_source_formal_signal_absent",
        "level_source_quality_invalid",
    )
    return next((reason for reason in priority if reason in reasons), None) or (
        reasons[0] if reasons else "source_candidate_no_longer_manual_ready"
    )


def _contract_label(value: object) -> str:
    contract_id = str(value or "")
    if not contract_id:
        return "不可用"
    try:
        return option_contract_label(contract_id)
    except (TypeError, ValueError):
        return contract_id


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _generation(value: object) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return max(value, 0)
    return 0


def recover_active_manual_plan(
    state: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Close the accepted-outbox/active-plan crash window on the next tick."""

    saved = state.get("active_manual_plan")
    if isinstance(saved, Mapping) and saved:
        return dict(saved)
    previous = state.get("last_candidate")
    if not isinstance(previous, Mapping):
        return {}
    candidate_id = str(previous.get("candidate_id") or "")
    event_id = f"{candidate_id}:ready"
    accepted = {
        str(item)
        for key in ("accepted_notification_event_ids", "notified_event_ids")
        for item in state.get(key) or ()
        if item
    }
    settled = {str(item) for item in state.get("settled_notification_event_ids") or () if item}
    if not candidate_id or event_id not in accepted or event_id in settled:
        return {}
    return active_manual_plan_snapshot(
        previous,
        activated_at=state.get("updated_at") or now.isoformat(),
    )


def cancellation_scope(
    candidate: Mapping[str, object],
    lifecycle_events: Mapping[str, str],
    *,
    now: datetime,
) -> set[str]:
    """Cancel only invalidated source lifecycles; transient states preserve them."""

    status = str(candidate.get("status") or "")
    reasons = {str(item) for item in candidate.get("block_reasons") or () if item}
    if status in {"manual_ready", "refresh_pending"}:
        return set()
    if (
        not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now)
        or reasons & _GLOBAL_SOURCE_LIFECYCLE_END_REASONS
    ):
        return set(lifecycle_events)
    if "opposite_signal_conflicts_with_active_plan" in reasons:
        return set()
    source_id = str(candidate.get("source_signal_id") or "")
    if source_id:
        return {
            event_id
            for event_id, lifecycle_source_id in lifecycle_events.items()
            if lifecycle_source_id == source_id
        }
    if classify_source_lifecycle(candidate) == "explicit_absence":
        tombstone_id = str(candidate.get("source_tombstone_id") or "")
        if tombstone_id:
            return {
                event_id
                for event_id, lifecycle_source_id in lifecycle_events.items()
                if lifecycle_source_id == tombstone_id
            }
    return set()


def seed_replayed_candidate_ids(
    state: Mapping[str, object],
    *,
    replay_journal_path: Path,
) -> set[str]:
    """Upgrade legacy state without replaying an already-recorded candidate."""

    replayed = {
        str(item) for item in state.get("replayed_candidate_ids") or () if item
    }
    if "replayed_candidate_ids" in state:
        return replayed

    previous = state.get("last_candidate")
    if isinstance(previous, Mapping) and previous.get("status") in {
        "manual_ready",
        "structure_watch",
    }:
        candidate_id = str(previous.get("candidate_id") or "")
        if candidate_id:
            replayed.add(candidate_id)

    try:
        rows = Path(replay_journal_path).read_text(encoding="utf-8").splitlines()
    except OSError:
        return replayed
    for row in rows:
        try:
            record = json.loads(row)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, Mapping) or record.get("status") not in {
            "manual_ready",
            "structure_watch",
        }:
            continue
        candidate_id = str(record.get("candidate_id") or "")
        if candidate_id:
            replayed.add(candidate_id)
    return replayed


def unreplayed_candidate(
    candidate: Mapping[str, object],
    replayed_candidate_ids: set[str],
) -> str | None:
    if candidate.get("status") not in {"manual_ready", "structure_watch"}:
        return None
    candidate_id = str(candidate.get("candidate_id") or "")
    if not candidate_id or candidate_id in replayed_candidate_ids:
        return None
    return candidate_id
