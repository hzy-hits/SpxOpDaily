"""Durable, non-executable notifications for meaningful level-path transitions."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.application.order_map.level_decision_machine import (
    LevelObservation,
    LevelPhase,
    LevelTransition,
)
from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import enqueue_notification
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object


OPERATOR_TRANSITION_PHASES = frozenset(
    {
        LevelPhase.APPROACHING,
        LevelPhase.TESTING,
        LevelPhase.BREAK_PENDING,
        LevelPhase.REJECT_PENDING,
        LevelPhase.RETEST,
        LevelPhase.CONFIRMED,
        LevelPhase.INVALIDATED,
        LevelPhase.EXPIRED,
    }
)


def prepare_level_transition_delivery(
    transition: LevelTransition,
    observation: LevelObservation,
    *,
    now: datetime,
    notify_transitions: bool,
    formal_signal_enabled: bool,
    notifications_enabled: bool,
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    """Freeze one transition card before the state-machine commit.

    The caller persists the returned intent in the same atomic state write as
    the transition.  A separate replay step may then enqueue and acknowledge
    it without reopening the state-to-outbox crash window.
    """

    if not transition.changed:
        return None, None
    state = transition.state
    phase = transition.current_phase
    result: dict[str, object] = {
        "record_key": (
            f"{state.get('event_id') or 'far'}:"
            f"{state.get('transition_count') or 0}:{phase.value}"
        ),
        "at": _utc(now).isoformat(),
        "event_id": state.get("event_id"),
        "phase": phase.value,
        "previous_phase": transition.previous_phase.value,
        "formal_signal": formal_signal_enabled and phase is LevelPhase.CONFIRMED,
        "actionable": False,
        "notify_transitions_configured": notify_transitions,
        "delivery_gate": "independent_trade_ready_required",
        "reason": "internal_transition_only",
        "sinks": [],
        "accepted": False,
        "queued": False,
        "delivered": False,
    }
    if not (
        phase in OPERATOR_TRANSITION_PHASES
        and formal_signal_enabled
        and notify_transitions
        and notifications_enabled
    ):
        return result, None
    notification = NotificationSettings.from_env()
    if not notification.enabled:
        result["reason"] = "notification_disabled"
        return result, None
    if not any(
        bool(getattr(notification, field, False))
        for field in ("feishu_enabled", "bark_enabled", "bark_friend_enabled")
    ):
        result["reason"] = "no_delivery_sink"
        return result, None

    event_id = f"level-path:{state.get('event_id')}:{phase.value}"
    text = render_level_transition(transition, observation)
    result.update(
        {
            "notification_event_id": event_id,
            "reason": "setup_transition_pending",
        }
    )
    return result, {
        "event_id": event_id,
        "source": "level_decision",
        "kind": "level_setup_transition",
        "lane": "market_warning",
        "occurred_at": _utc(now).isoformat(),
        "title": "SPX SETUP TRANSITION",
        "text": text,
        "friend": True,
        "feishu_text": text,
        "enqueued_at": _utc(now).isoformat(),
        "audit": result,
    }


def merge_pending_level_transition(
    persisted: Mapping[str, object],
    intent: Mapping[str, object] | None,
) -> tuple[list[dict[str, object]], list[str]]:
    """Preserve pending intents and append a new event at most once."""

    pending = [
        dict(item)
        for item in persisted.get("pending_notifications") or []
        if isinstance(item, Mapping)
    ]
    accepted = sorted(
        {
            str(item)
            for item in persisted.get("accepted_notification_event_ids") or []
            if item
        }
    )[-200:]
    if intent is None:
        return pending, accepted
    event_id = str(intent.get("event_id") or "")
    if not event_id or event_id in accepted:
        return pending, accepted
    if not any(str(item.get("event_id") or "") == event_id for item in pending):
        pending.append(dict(intent))
    return pending, accepted


def flush_pending_level_transition_notifications(
    state_path: Path,
    *,
    now: datetime,
    only_event_id: str | None = None,
    enqueue=None,
) -> dict[str, object] | None:
    """Retry frozen setup cards and acknowledge only durable acceptance."""

    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        pending = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
            and (
                only_event_id is None
                or str(item.get("event_id") or "") == only_event_id
            )
        ]
    if not pending:
        return None

    notification = NotificationSettings.from_env()
    producer = enqueue or enqueue_notification
    accepted_ids: set[str] = set()
    last_result: dict[str, object] | None = None
    for item in pending:
        event_id = str(item.get("event_id") or "")
        occurred_at = _parse_aware(item.get("occurred_at"))
        audit = item.get("audit")
        last_result = dict(audit) if isinstance(audit, Mapping) else {
            "record_key": f"notification-recovery:{event_id}",
            "at": _utc(now).isoformat(),
            "notification_event_id": event_id,
            "accepted": False,
            "queued": False,
            "delivered": False,
        }
        if not event_id or occurred_at is None:
            last_result["reason"] = "invalid_pending_notification"
            continue
        if not notification.enabled:
            last_result["reason"] = "notification_disabled"
            continue
        if not any(
            bool(getattr(notification, field, False))
            for field in ("feishu_enabled", "bark_enabled", "bark_friend_enabled")
        ):
            last_result["reason"] = "no_delivery_sink"
            continue
        try:
            enqueued = producer(
                notification,
                NotificationEnvelope(
                    event_id=event_id,
                    source=str(item.get("source") or "level_decision"),
                    kind=str(item.get("kind") or "level_setup_transition"),
                    lane=str(item.get("lane") or "market_warning"),
                    occurred_at=occurred_at,
                ),
                title=str(item.get("title") or "SPX SETUP TRANSITION"),
                text=str(item.get("text") or ""),
                friend=item.get("friend") is True,
                feishu_text=str(item.get("feishu_text") or item.get("text") or ""),
                enqueued_at=_parse_aware(item.get("enqueued_at")) or _utc(now),
            )
        except Exception as exc:  # The frozen intent remains replayable.
            last_result["reason"] = f"delivery_error:{type(exc).__name__}"
            last_result["outcome"] = f"enqueue_error:{type(exc).__name__}"
            continue
        outcome = str(getattr(enqueued, "outcome", "") or "")
        suppressed = outcome == "quiet_window_suppressed"
        accepted = bool(enqueued.accepted)
        last_result.update(
            {
                "notification_event_id": event_id,
                "reason": (
                    "setup_transition_enqueued"
                    if accepted
                    else f"setup_transition_{outcome or 'rejected'}"
                ),
                "targets": list(enqueued.targets),
                "accepted": accepted,
                "inserted": enqueued.inserted,
                "duplicate": enqueued.duplicate,
                "queued": enqueued.queued_for_recovery,
                "delivered": bool(enqueued.delivered and outcome == "delivered"),
                "suppressed": suppressed,
                "outcome": outcome,
            }
        )
        if accepted:
            accepted_ids.add(event_id)

    if accepted_ids:
        with exclusive_state_lock(state_path):
            state = read_json_object(state_path)
            state["pending_notifications"] = [
                item
                for item in state.get("pending_notifications") or []
                if not isinstance(item, Mapping)
                or str(item.get("event_id") or "") not in accepted_ids
            ]
            accepted = {
                str(item)
                for item in state.get("accepted_notification_event_ids") or []
                if item
            }
            accepted.update(accepted_ids)
            state["accepted_notification_event_ids"] = sorted(accepted)[-200:]
            atomic_write_json_secure(state_path, state)
    return last_result


def render_level_transition(
    transition: LevelTransition,
    observation: LevelObservation,
) -> str:
    state = transition.state
    phase = transition.current_phase
    coordinate = str(state.get("trigger_coordinate_kind") or "unknown")
    spot_label = "SPX" if coordinate == "official_spx" else "SPX代理"
    generation = state.get("reentry_generation", 0)
    quality = str(state.get("quality_status") or "ready").upper()
    return "\n".join(
        (
            f"SPX Setup Transition · {phase.value.upper()}",
            f"Opportunity  {state.get('event_id') or '-'} · generation {generation}",
            (
                f"State  {transition.previous_phase.value.upper()} → {phase.value.upper()}"
                f" · {_path_label(state)}"
            ),
            (
                f"Structure  {_level_kind_label(state.get('level_kind'))} "
                f"{_format_level(_number(state.get('spx_level', state.get('level'))))}"
            ),
            (
                f"Location  {spot_label} {_format_level(observation.spx_spot)}"
                f" · ES {_format_level(observation.es)}"
            ),
            f"Next  {_phase_instruction(phase)}",
            f"Data Quality  {quality}",
            "Authority  仅结构状态；执行必须等待独立 MANUAL READY，不连接订单或成交。",
        )
    )


def _phase_instruction(phase: LevelPhase) -> str:
    return {
        LevelPhase.APPROACHING: "价格接近冻结关键位，等待进入测试区",
        LevelPhase.TESTING: "价格正在测试冻结关键位，等待突破或拒绝路径形成",
        LevelPhase.BREAK_PENDING: "已越过突破缓冲，等待保持与 ES 同向确认",
        LevelPhase.REJECT_PENDING: "关键位出现拒绝，等待保持与 ES 同向确认",
        LevelPhase.RETEST: "已回踩关键位，等待重新站稳并完成确认",
        LevelPhase.CONFIRMED: "方向结构已确认；继续等待合约、NBBO、R/R 与时效门控",
        LevelPhase.INVALIDATED: "本代机会已失效；离开 reset band 前不得重新武装",
        LevelPhase.EXPIRED: "本代机会已过期；离开 reset band 前不得重新武装",
    }.get(phase, "继续观察结构状态")


def _path_label(state: Mapping[str, object]) -> str:
    thesis = str(state.get("thesis") or "none")
    direction = str(state.get("direction") or "")
    return {
        ("breakout", "up"): "向上突破",
        ("breakout", "down"): "向下突破",
        ("fade", "up"): "下破拒绝后向上收复",
        ("fade", "down"): "上破拒绝后向下回落",
    }.get((thesis, direction), "路径待确认")


def _level_kind_label(value: object) -> str:
    return {
        "put_wall": "Put Wall",
        "flip_low": "Flip Low",
        "flip_high": "Flip High",
        "call_wall": "Call Wall",
    }.get(str(value or ""), str(value or "-"))


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _format_level(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "-"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("level transition timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _parse_aware(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)
