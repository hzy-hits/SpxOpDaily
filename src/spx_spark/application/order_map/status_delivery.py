"""Cadence and material-change gate for operator status cards."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.report_clock import rth_report_slot
from spx_spark.application.order_map.state import material_changes, payload_fingerprint


STATUS_KEY_WINDOW_PHASES = frozenset(
    ("europe_session", "us_data_hour", "us_open_hour", "us_midday_confirmation")
)
GTH_STATUS_PHASES = frozenset({"asia_globex", "europe_session", "us_data_hour"})
STATUS_SUMMARY_CADENCE_SECONDS = 60.0 * 60.0
GTH_STATUS_SUMMARY_CADENCE_SECONDS = 60.0 * 60.0
RTH_SLOT_LOOKBACK_GRACE_SECONDS = 15.0 * 60.0 - 0.001
RTH_OPEN_DESK_MAP_SLOT_INDEXES = frozenset({0, 2, 4})


def _decision_thesis(payload: dict[str, Any]) -> str:
    plans = payload.get("plan_candidates")
    if isinstance(plans, list) and len(plans) == 1 and isinstance(plans[0], dict):
        plan = plans[0]
        return f"plan:{plan.get('play') or '-'}@{finite_float(plan.get('level'))}"
    decision = payload.get("level_decision")
    if isinstance(decision, dict) and decision.get("formal_signal") is True:
        return "|".join(
            (
                "level",
                str(decision.get("event_id") or "-"),
                str(decision.get("thesis") or "-"),
                str(decision.get("direction") or "-"),
            )
        )
    return ""


def _status_fingerprint(payload: dict[str, Any]) -> dict[str, Any]:
    fingerprint = payload_fingerprint(payload)
    phase = payload.get("session_phase")
    status_phase = str(phase.get("name") or "") if isinstance(phase, dict) else ""
    fingerprint["status_phase"] = status_phase
    decision = payload.get("level_decision")
    frozen_levels = (
        decision.get("levels")
        if isinstance(decision, dict) and isinstance(decision.get("levels"), dict)
        else {}
    )
    for key in ("put_wall", "flip_low", "flip_high", "call_wall"):
        frozen_value = finite_float(frozen_levels.get(key))
        if frozen_value is not None:
            fingerprint[key] = frozen_value
    if isinstance(decision, dict):
        fingerprint.update(
            {
                "decision_event_id": str(decision.get("event_id") or ""),
                "decision_phase": str(decision.get("phase") or "far").lower(),
                "decision_direction": str(decision.get("direction") or "none"),
                "decision_thesis_kind": str(decision.get("thesis") or "none"),
                "decision_level_kind": str(decision.get("level_kind") or ""),
                "decision_level": finite_float(decision.get("level")),
                "decision_quality": (
                    "degraded"
                    if decision.get("quality_ok") is False
                    or decision.get("quality_status") == "degraded"
                    else "ready"
                ),
            }
        )
    # Skew-spread candidates are research observations that rotate with quote
    # microstructure.  Their churn must never turn a 15-minute snapshot into a
    # human Desk Map; dedicated setup/trade transitions own executable changes.
    fingerprint["skew_spread_shadow_id"] = ""
    fingerprint["decision_thesis"] = _decision_thesis(payload)
    plans = payload.get("plan_candidates")
    plan = plans[0] if isinstance(plans, list) and len(plans) == 1 else None
    if isinstance(plan, dict):
        fingerprint["trade_intent_id"] = str(plan.get("intent_id") or "")
        strike = finite_float(plan.get("strike"))
        right = str(plan.get("right") or "")
        fingerprint["trade_contract"] = f"{strike:g}{right}" if strike is not None else ""
    else:
        fingerprint["trade_intent_id"] = ""
        fingerprint["trade_contract"] = ""
    intent = payload.get("trade_intent")
    if isinstance(intent, dict):
        fingerprint["trade_intent_status"] = str(intent.get("status") or "")
        fingerprint["opportunity_key"] = str(
            intent.get("semantic_key") or intent.get("semantic_scope") or ""
        )
    else:
        fingerprint["trade_intent_status"] = ""
        fingerprint["opportunity_key"] = ""
    provider_control = payload.get("strategy_entry_control")
    if isinstance(provider_control, dict):
        fingerprint["gth_provider_block_reason"] = (
            ""
            if provider_control.get("allowed") is True
            else str(provider_control.get("reason") or "provider_unavailable")
        )
    else:
        fingerprint["gth_provider_block_reason"] = ""
    return fingerprint


def _thesis_label(value: str) -> str:
    if value.startswith("plan:"):
        play_and_level = value.removeprefix("plan:")
        play, _, level = play_and_level.partition("@")
        label = {
            "level_breakout_call": "向上突破",
            "level_breakout_put": "向下突破",
            "level_fade_call": "下破拒绝",
            "level_fade_put": "上破拒绝",
        }.get(play, play)
        return f"{label}@{level}" if level else label
    if value.startswith("regime:"):
        _, mode, direction = (value.split(":", 2) + ["unknown", "unknown"])[:3]
        mode_label = {
            "trending": "趋势",
            "mean_reverting": "均值回归",
            "transition": "过渡",
        }.get(mode, mode)
        direction_label = {"up": "偏多", "down": "偏空", "neutral": "中性"}.get(
            direction, direction
        )
        return f"{mode_label}{direction_label}"
    return value


def _status_material_changes(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> list[str]:
    changes = material_changes(previous, current)
    if not isinstance(previous, dict):
        return changes
    prior_event = str(previous.get("decision_event_id") or "")
    current_event = str(current.get("decision_event_id") or "")
    prior_phase = str(previous.get("decision_phase") or "")
    current_phase = str(current.get("decision_phase") or "")
    if prior_event == current_event and prior_phase != current_phase and (prior_phase or current_phase):
        changes.append(f"机会阶段 {_phase_label(prior_phase)}→{_phase_label(current_phase)}")
    elif prior_event != current_event and current_event:
        changes.append(
            f"新机会 {_thesis_label(str(current.get('decision_thesis_kind') or 'none'))}"
            f" {_phase_label(current_phase)}"
        )
    elif prior_event and not current_event:
        changes.append(f"机会结束 {_phase_label(prior_phase)}")
    prior_quality = str(previous.get("decision_quality") or "")
    current_quality = str(current.get("decision_quality") or "")
    if prior_quality and current_quality and prior_quality != current_quality:
        changes.append(f"数据质量 {prior_quality.upper()}→{current_quality.upper()}")
    prior_intent_status = str(previous.get("trade_intent_status") or "")
    current_intent_status = str(current.get("trade_intent_status") or "")
    if prior_intent_status != current_intent_status and current_intent_status:
        changes.append(f"执行状态 {prior_intent_status or '-'}→{current_intent_status}")
    prior_provider = str(previous.get("gth_provider_block_reason") or "")
    current_provider = str(current.get("gth_provider_block_reason") or "")
    if prior_provider != current_provider:
        if current_provider == "ibkr_competing_session":
            changes.append("GTH执行行情暂停 · IBKR 10197")
        elif prior_provider == "ibkr_competing_session" and not current_provider:
            changes.append("GTH执行行情恢复")
        elif current_provider:
            changes.append(f"GTH执行行情暂停 · {current_provider}")
        else:
            changes.append("GTH执行行情恢复")
    prior_thesis = str(previous.get("decision_thesis") or "")
    current_thesis = str(current.get("decision_thesis") or "")
    if prior_thesis != current_thesis and (prior_thesis or current_thesis):
        if prior_thesis and current_thesis:
            changes.append(
                f"决策剧本 {_thesis_label(prior_thesis)}→{_thesis_label(current_thesis)}"
            )
        elif current_thesis:
            changes.append(f"决策剧本建立 {_thesis_label(current_thesis)}")
        else:
            changes.append(f"决策剧本失效 {_thesis_label(prior_thesis)}")
    prior_intent = str(previous.get("trade_intent_id") or "")
    current_intent = str(current.get("trade_intent_id") or "")
    if prior_thesis == current_thesis and prior_intent != current_intent:
        prior_contract = str(previous.get("trade_contract") or "-")
        current_contract = str(current.get("trade_contract") or "-")
        if prior_intent and current_intent:
            changes.append(f"执行意图更新 {prior_contract}→{current_contract}")
        elif current_intent:
            changes.append(f"执行意图建立 {current_contract}")
        elif prior_intent:
            changes.append(f"执行意图失效 {prior_contract}")
    return changes


def status_delivery_reason(
    previous: dict[str, Any],
    fingerprint: dict[str, Any],
    changes: list[str],
    *,
    now: datetime,
    trading_date: str,
    position_risk: bool,
) -> str | None:
    if previous.get("last_status_date") != trading_date:
        return "initial_status"
    current_rth_slot = rth_report_slot(now)
    if current_rth_slot is not None:
        last_status_at = finite_float(previous.get("last_status_at"))
        previous_rth_slot = (
            rth_report_slot(
                datetime.fromtimestamp(last_status_at, tz=timezone.utc),
                start_grace_seconds=RTH_SLOT_LOOKBACK_GRACE_SECONDS,
            )
            if last_status_at is not None
            else None
        )
        if previous_rth_slot is not None and previous_rth_slot.key == current_rth_slot.key:
            return None
        if changes:
            return "material_changes"
        if _rth_desk_map_summary_due(current_rth_slot.index):
            return f"rth_desk_map:{current_rth_slot.key}"
        return None
    phase = str(fingerprint.get("status_phase") or "")
    previous_fingerprint = previous.get("status_fingerprint") or previous.get("fingerprint")
    previous_phase = (
        str(previous_fingerprint.get("status_phase") or "")
        if isinstance(previous_fingerprint, dict)
        else ""
    )
    if phase in STATUS_KEY_WINDOW_PHASES and previous_phase != phase:
        return f"key_window:{phase}"
    if position_risk:
        last_status_at = finite_float(previous.get("last_status_at"))
        if (
            last_status_at is None
            or now.timestamp() - last_status_at >= STATUS_SUMMARY_CADENCE_SECONDS
        ):
            return "open_position_risk"
        return None
    if phase in GTH_STATUS_PHASES:
        if changes:
            return "material_changes"
        if fingerprint.get("gth_provider_block_reason") == "ibkr_competing_session":
            return None
        last_status_at = finite_float(previous.get("last_status_at"))
        if (
            last_status_at is None
            or now.timestamp() - last_status_at >= GTH_STATUS_SUMMARY_CADENCE_SECONDS
        ):
            return f"gth_hourly_summary:{phase}"
        return None
    return "material_changes" if changes else None


def _rth_desk_map_summary_due(slot_index: int) -> bool:
    """Open-window maps are half-hourly; later unchanged maps are hourly."""

    return slot_index in RTH_OPEN_DESK_MAP_SLOT_INDEXES or (
        slot_index >= 8 and slot_index % 4 == 0
    )


def _phase_label(value: str) -> str:
    return {
        "far": "NO SETUP",
        "approaching": "APPROACHING",
        "testing": "TESTING",
        "break_pending": "BREAK PENDING",
        "reject_pending": "REJECT PENDING",
        "accepted": "ACCEPTED",
        "rejected": "REJECTED",
        "retest": "RETEST",
        "confirmed": "CONFIRMED",
        "invalidated": "INVALIDATED",
        "expired": "EXPIRED",
    }.get(value, value or "-")
