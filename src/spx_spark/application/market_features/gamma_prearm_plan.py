"""Event-driven, two-sided preparation cards for approaching Gamma levels."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.spring_gamma_operator import (
    spring_gamma_operator_line,
    spring_gamma_operator_view,
)
from spx_spark.application.market_features.prior_rth_context import (
    prior_session_operator_line,
    prior_session_signal_view,
)
from spx_spark.application.market_features.virtual_strategy_state import (
    flush_pending_notifications,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.notifier.operator_cards import option_contract_label
from spx_spark.state_io import (
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)


CONTRACT_VERSION = "gamma_prearm_plan.v1"
REPRICING_MAX_AGE_SECONDS = 45.0
DELIVERY_TTL_SECONDS = 90.0
PREPARATION_PHASES = frozenset({"approaching", "break_pending", "reject_pending"})


def evaluate_gamma_prearm_plan(
    repricing: Mapping[str, object],
    level_decision: Mapping[str, object],
    *,
    now: datetime,
    spring_gamma: Mapping[str, object] | None = None,
    prior_session: Mapping[str, object] | None = None,
    gth_position_fraction: float | None = None,
    invalidation_buffer_points: float = 3.0,
) -> dict[str, object]:
    """Build one conditional plan before price reaches a frozen Gamma level."""

    now = _utc(now)
    phase = str(level_decision.get("phase") or "")
    source_event_id = str(level_decision.get("event_id") or "")
    base: dict[str, object] = {
        "schema_version": 1,
        "contract_version": CONTRACT_VERSION,
        "kind": "gamma_level_prearm_plan",
        "status": "inactive",
        "plan_id": None,
        "source_event_id": source_event_id or None,
        "evaluated_at": now.isoformat(),
        "execution_eligible": False,
        "automatic_ordering": False,
        "broker_submission_allowed": False,
        "operator_action": "prepare_only",
        "block_reasons": [],
    }
    if phase not in PREPARATION_PHASES:
        return {**base, "block_reasons": ["level_not_approaching"]}
    if repricing.get("status") != "repriced" or repricing.get("phase") != phase:
        reason = (
            "approach_repricing_unavailable"
            if phase == "approaching"
            else "preparation_repricing_unavailable"
        )
        return {**base, "status": "blocked", "block_reasons": [reason]}
    if str(repricing.get("event_id") or "") != source_event_id:
        reason = (
            "approach_event_mismatch"
            if phase == "approaching"
            else "preparation_event_mismatch"
        )
        return {**base, "status": "blocked", "block_reasons": [reason]}
    repriced_at = _time(repricing.get("as_of"))
    age_seconds = (now - repriced_at).total_seconds() if repriced_at is not None else None
    if (
        age_seconds is None
        or age_seconds < -1.0
        or age_seconds > REPRICING_MAX_AGE_SECONDS
    ):
        reason = (
            "approach_repricing_stale"
            if phase == "approaching"
            else "preparation_repricing_stale"
        )
        return {**base, "status": "blocked", "block_reasons": [reason]}

    active_play = _active_play(level_decision) if phase != "approaching" else None
    candidates = [
        _plan_path(
            item,
            level=_number(repricing.get("spx_level")),
            invalidation_buffer_points=invalidation_buffer_points,
            geometry=(
                repricing.get("path_geometries", {}).get(str(item.get("play") or ""))
                if isinstance(repricing.get("path_geometries"), Mapping)
                else None
            ),
        )
        for item in repricing.get("candidates") or []
        if isinstance(item, Mapping)
        and item.get("execution_quote_status") == "executable"
        and (active_play is None or item.get("play") == active_play)
    ]
    paths = sorted(
        (item for item in candidates if item is not None),
        key=lambda item: (str(item["side"]), str(item["play"])),
    )
    if not paths:
        return {**base, "status": "blocked", "block_reasons": ["prearm_quote_unavailable"]}

    level = _number(repricing.get("spx_level"))
    spot = _number(repricing.get("pricing_spot"))
    expiry = str(repricing.get("expiry") or "")
    level_kind = str(repricing.get("level_kind") or "")
    if level is None or spot is None or len(expiry) != 8 or not level_kind:
        return {**base, "status": "blocked", "block_reasons": ["prearm_coordinate_incomplete"]}

    identity = "|".join(
        (
            CONTRACT_VERSION,
            expiry,
            level_kind,
            f"{level:.4f}",
            *(f"{item['play']}:{item['contract_id']}" for item in paths),
        )
    )
    plan_id = "gamma-prearm:" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    spring_gamma_view = spring_gamma_operator_view(
        spring_gamma,
        now=now,
        expected_expiry=expiry,
    )
    paths = [
        {
            **item,
            "prior_session_chase_risk": prior_session_signal_view(
                prior_session,
                direction="up" if item["side"] == "CALL" else "down",
                gth_position_fraction=gth_position_fraction,
            ).get("chase_risk"),
        }
        for item in paths
    ]
    prior_session_view = prior_session_signal_view(
        prior_session,
        gth_position_fraction=gth_position_fraction,
    )
    return {
        **base,
        "status": "prearm_ready",
        "notification_stage": phase,
        "plan_id": plan_id,
        "level_kind": level_kind,
        "level": level,
        "current_spx": spot,
        "distance_points": round(abs(spot - level), 2),
        "expiry": expiry,
        "paths": paths,
        "trigger_coordinate": (
            dict(repricing.get("trigger_coordinate"))
            if isinstance(repricing.get("trigger_coordinate"), Mapping)
            else {}
        ),
        "touch_time_estimate": (
            dict(repricing.get("touch_time_estimate"))
            if isinstance(repricing.get("touch_time_estimate"), Mapping)
            else {}
        ),
        "spring_gamma": spring_gamma_view,
        "prior_session": prior_session_view,
        "block_reasons": [],
    }


def process_gamma_prearm_plan(
    storage: StorageSettings,
    repricing: Mapping[str, object],
    level_decision: Mapping[str, object],
    *,
    now: datetime,
    spring_gamma: Mapping[str, object] | None = None,
    prior_session: Mapping[str, object] | None = None,
    gth_position_fraction: float | None = None,
    invalidation_buffer_points: float = 3.0,
    notification: NotificationSettings | None = None,
) -> dict[str, object]:
    """Persist and deliver a Gamma preparation plan once per semantic level."""

    now = _utc(now)
    plan = evaluate_gamma_prearm_plan(
        repricing,
        level_decision,
        now=now,
        spring_gamma=spring_gamma,
        prior_session=prior_session,
        gth_position_fraction=gth_position_fraction,
        invalidation_buffer_points=invalidation_buffer_points,
    )
    state_path = Path(storage.data_root) / "latest" / "gamma_prearm_plan_state.json"
    projection_path = Path(storage.data_root) / "latest" / "gamma_prearm_plan.json"
    event_id = (
        f"{plan['plan_id']}:{plan.get('notification_stage')}"
        if plan.get("status") == "prearm_ready"
        else None
    )
    settings = notification or NotificationSettings.from_env()
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        accepted = {
            str(item)
            for item in state.get("accepted_notification_event_ids") or []
            if item
        }
        settled = {
            str(item)
            for item in state.get("settled_notification_event_ids") or []
            if item
        }
        pending = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
        ]
        pending_ids = {str(item.get("event_id") or "") for item in pending}
        if (
            event_id
            and event_id not in accepted
            and event_id not in settled
            and event_id not in pending_ids
        ):
            pending.append(_notification_intent(plan, event_id=event_id, now=now))
        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "last_plan": dict(plan),
                "accepted_notification_event_ids": sorted(accepted)[-200:],
                "settled_notification_event_ids": sorted(settled)[-200:],
                "pending_notifications": pending,
            }
        )
        atomic_write_json_secure(state_path, state)
        atomic_write_json_secure(projection_path, plan)
    result = {"attempted": False, "accepted": False}
    if event_id:
        result = flush_pending_notifications(
            state_path,
            settings=settings,
            now=now,
            only_event_id=event_id,
        )
    return {
        **plan,
        "notification_attempted": bool(result.get("attempted")),
        "notification_accepted": bool(result.get("accepted")),
        "notification_outcome": result.get("outcome"),
    }


def _plan_path(
    candidate: Mapping[str, object],
    *,
    level: float | None,
    invalidation_buffer_points: float,
    geometry: object = None,
) -> dict[str, object] | None:
    play = str(candidate.get("play") or "")
    contract_id = str(candidate.get("contract_id") or "")
    side = str(candidate.get("right") or "")
    if play not in {
        "level_breakout_call",
        "level_breakout_put",
        "level_fade_call",
        "level_fade_put",
    } or not contract_id or side not in {"C", "P"}:
        return None
    return {
        "play": play,
        "side": "CALL" if side == "C" else "PUT",
        "contract_id": contract_id,
        "condition": _condition(play),
        "decision_bid": _number(candidate.get("execution_bid")),
        "decision_ask": _number(candidate.get("execution_ask")),
        "projected_low": _number(candidate.get("projection_range_low")),
        "projected_mid": _number(candidate.get("projected_mid")),
        "projected_high": _number(candidate.get("projection_range_high")),
        "limit_conservative": _number(candidate.get("limit_conservative")),
        "limit_aggressive": _number(candidate.get("limit_aggressive")),
        "frontrun_level": _number(candidate.get("frontrun_level")),
        "frontrun_limit": _number(candidate.get("frontrun_limit")),
        "touch_eta_minutes": _number(candidate.get("touch_eta_minutes")),
        "quote_provider": candidate.get("execution_quote_provider"),
        "invalidation_spx": (
            round(
                level - invalidation_buffer_points
                if side == "C"
                else level + invalidation_buffer_points,
                2,
            )
            if level is not None
            else None
        ),
        "confirmation_geometry": dict(geometry) if isinstance(geometry, Mapping) else None,
    }


def _notification_intent(
    plan: Mapping[str, object],
    *,
    event_id: str,
    now: datetime,
) -> dict[str, object]:
    level_label = {
        "put_wall": "Put Wall",
        "flip_low": "Flip Low",
        "flip_high": "Flip High",
        "call_wall": "Call Wall",
    }.get(str(plan.get("level_kind") or ""), "Gamma 位")
    pending = str(plan.get("notification_stage") or "") in {
        "break_pending",
        "reject_pending",
    }
    lines = [
        (
            "🟡 条件准备卡 · 已发生突破/拒绝，等确认"
            if pending
            else "🎯 GAMMA 伏击计划 · 先准备，未触发不下单"
        ),
        (
            f"区域  {level_label} {float(plan['level']):.2f} · "
            f"当前 SPX {float(plan['current_spx']):.2f} · "
            f"距离 {float(plan['distance_points']):.2f} 点"
        ),
    ]
    for index, item in enumerate(plan.get("paths") or (), start=1):
        if not isinstance(item, Mapping):
            continue
        price_range = _price_range(item)
        lines.append(
            f"路径{index}  {item['condition']}：{item['side']} · "
            f"{option_contract_label(str(item['contract_id']))} · "
            f"现价 {_quote_range(item)} · 最高参考 {price_range}"
        )
        invalidation = _number(item.get("invalidation_spx"))
        target = _geometry_target(item.get("confirmation_geometry"))
        lines.append(
            f"触发  状态机 CONFIRMED 后才入场 · "
            f"失效 SPX {'跌回' if item['side'] == 'CALL' else '收回'} "
            f"{invalidation:.2f}"
            if invalidation is not None
            else "触发  状态机 CONFIRMED 后才入场"
        )
        if target is not None:
            lines.append(f"空间  下一有效结构目标 {target:.2f}，READY 时重算盈亏比")
    lines.append(spring_gamma_operator_line(plan.get("spring_gamma")))
    lines.append(_prior_session_plan_line(plan))
    lines.extend(
        (
            (
                "动作  条件已出现；预填订单，只有 CONFIRMED 且重新报价通过后才提交"
                if pending
                else "动作  现在不追；只准备订单，价格到 Gamma 位并确认对应路径后再提交"
            ),
            "失效  Gamma 结构位变化、状态机离开本事件，或出现相反路径确认",
            "权限  预埋计划不是方向信号；自动下单关闭",
        )
    )
    text = "\n".join(lines)
    expires_at = now + timedelta(seconds=DELIVERY_TTL_SECONDS)
    return {
        "event_id": event_id,
        "source": "gamma_prearm_plan",
        "kind": "gamma_level_prearm_plan",
        "lane": "gamma_prearm_plan",
        "occurred_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "title": (
            "SPX 条件准备卡 · 等最终确认"
            if pending
            else "SPX GAMMA 伏击计划 · 等触发"
        ),
        "text": text,
        "friend": True,
        "feishu_text": text,
        "enqueued_at": now.isoformat(),
    }


def _condition(play: str) -> str:
    return {
        "level_breakout_call": "向上接受并站稳",
        "level_breakout_put": "向下接受并保持",
        "level_fade_call": "下沿拒绝并收复",
        "level_fade_put": "上沿拒绝并回落",
    }[play]


def _price_range(path: Mapping[str, object]) -> str:
    low = _number(path.get("projected_low"))
    high = _number(path.get("projected_high"))
    if low is not None and high is not None:
        return f"{low:.2f}–{high:.2f}"
    limit = _number(path.get("limit_conservative"))
    return f"≤ {limit:.2f}" if limit is not None else "触位时重算"


def _quote_range(path: Mapping[str, object]) -> str:
    bid = _number(path.get("decision_bid"))
    ask = _number(path.get("decision_ask"))
    if bid is not None and ask is not None:
        return f"{bid:.2f}/{ask:.2f}"
    return "重新报价"


def _geometry_target(value: object) -> float | None:
    return _number(value.get("target_spx")) if isinstance(value, Mapping) else None


def _active_play(level_decision: Mapping[str, object]) -> str | None:
    thesis = str(level_decision.get("thesis") or "")
    direction = str(level_decision.get("direction") or "")
    return {
        ("breakout", "up"): "level_breakout_call",
        ("breakout", "down"): "level_breakout_put",
        ("fade", "up"): "level_fade_call",
        ("fade", "down"): "level_fade_put",
    }.get((thesis, direction))


def _prior_session_plan_line(plan: Mapping[str, object]) -> str:
    line = prior_session_operator_line(plan.get("prior_session"))
    high_risk_sides = [
        str(item.get("side") or "")
        for item in plan.get("paths") or ()
        if isinstance(item, Mapping)
        and item.get("prior_session_chase_risk") == "high"
    ]
    if not high_risk_sides:
        return line
    return f"{line}；{'/'.join(high_risk_sides)} 同向极值追单需等待墙位接受，不能直接追"


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
