"""Persistence, contract, and rendering helpers for virtual strategy episodes."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.config import StorageSettings
from spx_spark.greek_reference import calculate_contract_reference, inputs_from_quote
from spx_spark.application.order_map.spot import actionable_live_price
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import InstrumentId, Provider
from spx_spark.options_map import actionable_chain_implied_reference
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState, configured_quote_use_decision
from spx_spark.strategy_contract import (
    normalize_block_reasons,
    parse_aware_time,
    policy_version,
    strategy_event_fields,
)

_TERMINAL_EXIT_ACTIONS = {
    "time_stop": "exit",
    "strategy_invalidation": "exit",
    "gth_dip_low_broken": "exit",
    "spread_value_saturation": "take_profit_or_exit",
    "premium_profit_target": "take_profit_or_reduce",
    "underlier_target_reached": "take_profit",
    "call_wall_touched": "take_profit_or_reduce",
    "delta_saturated": "reduce",
    "post_event_iv_crush_vanna_drag": "take_profit_or_exit",
    "gamma_convexity_decayed": "exit",
}


def _event_contract(
    source: Mapping[str, object], *, block_reasons: tuple[str, ...] | list[str]
) -> dict[str, object]:
    raw_coordinate = source.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    version = str(source.get("policy_version") or "") or policy_version(
        "virtual_strategy_audit.v3",
        {"source_kind": source.get("source_kind") or "legacy"},
    )
    return strategy_event_fields(
        policy_version_value=version,
        valid_until=parse_aware_time(source.get("valid_until"))
        or parse_aware_time(source.get("time_stop_at")),
        coordinate=coordinate,
        block_reasons=block_reasons,
    )


def _record_entry_decision(
    storage: StorageSettings,
    decision: Mapping[str, object],
    *,
    entry_decisions: dict[str, dict[str, object]],
    now: datetime,
) -> None:
    key = str(decision.get("source_signal_id") or decision.get("decision_id") or "")
    if not key:
        return
    prior = dict(entry_decisions.get(key) or {})
    if prior.get("terminal") is True:
        return
    payload = dict(decision)
    reasons = normalize_block_reasons(payload.get("block_reasons") or [])
    if payload.get("terminal") is True and "signal_expired" in reasons:
        reasons = normalize_block_reasons([*(prior.get("last_block_reasons") or []), *reasons])
        payload["block_reasons"] = reasons
    signature_material = {
        "status": payload.get("status"),
        "terminal": payload.get("terminal"),
        "block_reasons": reasons,
    }
    signature = hashlib.sha256(
        json.dumps(signature_material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    if signature != prior.get("signature"):
        _append_audit(storage, now, payload)
    entry_decisions[key] = {
        "signature": signature,
        "last_block_reasons": reasons,
        "last_evaluated_at": payload.get("evaluated_at") or now.isoformat(),
        "terminal": payload.get("terminal") is True,
        "status": payload.get("status"),
        "decision_id": payload.get("decision_id"),
    }


def _trim_entry_decisions(
    rows: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    ordered = sorted(
        ((str(key), dict(value)) for key, value in rows.items()),
        key=lambda item: str(item[1].get("last_evaluated_at") or ""),
    )
    return dict(ordered[-200:])


def _append_audit(storage: StorageSettings, now: datetime, payload: Mapping[str, object]) -> None:
    path = (
        Path(storage.data_root)
        / "features"
        / "virtual_strategy"
        / f"date={now.date().isoformat()}"
        / "events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(
            descriptor,
            (json.dumps(dict(payload), ensure_ascii=False, sort_keys=True) + "\n").encode(),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "virtual_strategy_state.json"


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _entry_observed_at(trade_intent: Mapping[str, object]) -> object:
    observation = trade_intent.get("entry_observation")
    return observation.get("at") if isinstance(observation, Mapping) else None


def _level_reached(
    price: float | None,
    level: float | None,
    *,
    direction: str,
    target: bool,
) -> bool:
    if price is None or level is None or direction not in {"up", "down"}:
        return False
    if target:
        return price >= level if direction == "up" else price <= level
    return price <= level if direction == "up" else price >= level


def _fmt(value: object) -> str:
    return f"{float(value):.2f}" if isinstance(value, int | float) else "-"


def _pct(value: object) -> str:
    return f"{float(value):.1%}" if isinstance(value, int | float) else "-"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _latest_created_at(latest: LatestState) -> str | None:
    created_at = getattr(latest, "created_at", None)
    return _utc(created_at).isoformat() if isinstance(created_at, datetime) else None


def _gth_signal_age_seconds(
    signal: Mapping[str, object],
    *,
    now: datetime,
    future_tolerance_seconds: float,
) -> float | None:
    """Return confirmation age only while the persisted signal is actionable."""

    now = _utc(now)
    confirmed_at = _time(signal.get("confirmed_at"))
    valid_until = _time(signal.get("valid_until"))
    if confirmed_at is None or valid_until is None or valid_until < confirmed_at:
        return None
    age = (now - confirmed_at).total_seconds()
    if age < -max(0.0, future_tolerance_seconds) or now >= valid_until:
        return None
    return age


def _should_replace_with_gth_spread(
    active: Mapping[str, object],
    gth_signal: Mapping[str, object],
) -> bool:
    """Let an exact GTH spread supersede a legacy single-leg shadow."""

    return bool(
        active
        and active.get("source_kind") == "gth_dip_reclaim_call"
        and active.get("position_type") != "call_debit_spread"
        and gth_signal.get("kind") == "gth_dip_reclaim_call"
        and isinstance(gth_signal.get("spread"), Mapping)
    )


def _gth_time_stop(now: datetime, *, policy: MarketFeatureSettings) -> datetime:
    """Return the current 0DTE expiry's DST-aware exit, never the next expiry."""

    now = _utc(now)
    clock = _exit_clock(policy.virtual_gth_exit_clock_et)
    expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    candidate = datetime.combine(expiry, clock, tzinfo=ET).astimezone(timezone.utc)
    if candidate <= now:
        return now
    backstop = now + timedelta(minutes=policy.virtual_gth_time_stop_minutes)
    return min(candidate, backstop)


def _exit_clock(value: object) -> time:
    try:
        parsed = time.fromisoformat(str(value))
    except ValueError:
        return time(9, 45)
    return time(parsed.hour, parsed.minute)


def _episode(
    *,
    source_id: str,
    source_kind: str,
    direction: str,
    contract_id: str,
    snapshot: Mapping[str, object],
    now: datetime,
    stop: datetime,
    invalidation_spx: float | None,
    target_spx: float | None,
    invalidation_es: float | None,
    source_contract: Mapping[str, object] | None = None,
    lifecycle_policy: object | None = None,
) -> dict[str, object]:
    if direction not in {"up", "down"}:
        return {}
    episode_id = "virtual:" + hashlib.sha256(f"{source_id}|{contract_id}".encode()).hexdigest()[:24]
    source = dict(source_contract or {})
    operator_opportunity_id = str(
        source.get("operator_opportunity_id")
        or source.get("event_id")
        or source_id
    )
    generation_value = source.get("reentry_generation", 0)
    reentry_generation = (
        generation_value
        if isinstance(generation_value, int) and not isinstance(generation_value, bool)
        else 0
    )
    raw_coordinate = source.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    lifecycle_policy_version = policy_version(
        "virtual_strategy_lifecycle.v3",
        {
            "source_kind": source_kind,
            "source_policy_version": source.get("policy_version"),
            "policy": lifecycle_policy,
        },
    )
    return {
        **strategy_event_fields(
            policy_version_value=lifecycle_policy_version,
            valid_until=stop,
            coordinate=coordinate,
            block_reasons=(),
        ),
        "episode_id": episode_id,
        "status": "active",
        "source_signal_id": source_id,
        "operator_opportunity_id": operator_opportunity_id,
        "reentry_generation": max(reentry_generation, 0),
        "source_kind": source_kind,
        "source_schema_version": source.get("schema_version"),
        "source_policy_version": source.get("policy_version"),
        "source_valid_until": source.get("valid_until") or source.get("expires_at"),
        "session_id": source.get("session_id") or source.get("session_date"),
        "direction": direction,
        "contract_id": contract_id,
        "opened_at": now.isoformat(),
        "time_stop_at": stop.isoformat(),
        "entry_mid": snapshot.get("mid"),
        "entry_bid": snapshot.get("bid"),
        "entry_ask": snapshot.get("ask"),
        "entry_snapshot": dict(snapshot),
        "entry_iv": snapshot.get("iv"),
        "entry_gamma": snapshot.get("gamma_per_point"),
        "entry_delta": snapshot.get("delta"),
        "invalidation_spx": invalidation_spx,
        "target_spx": target_spx,
        "invalidation_es": invalidation_es,
        "mfe_fraction": 0.0,
        "mae_fraction": 0.0,
        "health_status": "healthy",
        "automatic_ordering": False,
        "account_position_source": "none",
        "entry_basis": "decision_quote_snapshot",
        "execution_assumption": "none",
        "last": dict(snapshot),
    }


def _exit_decision(
    active: Mapping[str, object],
    current: Mapping[str, object],
    *,
    latest: LatestState,
    option_structure: Mapping[str, object],
    macro_event: Mapping[str, object],
    greek_decision: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
) -> tuple[str | None, str | None]:
    stop = _time(active.get("time_stop_at"))
    hard_exit = _rth_trade_hard_exit(active, now=now)
    if hard_exit is not None:
        stop = min(stop, hard_exit) if stop is not None else hard_exit
    pending_reason = _locked_pending_reason(active)
    if pending_reason is not None:
        pending_action = str(active.get("close_pending_action") or "") or _terminal_exit_action(
            pending_reason
        )
        if _number(current.get("bid")) is not None:
            return pending_reason, pending_action
        censor_at = (
            _episode_censor_at(active, stop=stop, policy=policy)
            if pending_reason == "time_stop" and stop is not None
            else _locked_exit_censor_at(active, stop=stop)
        )
        if censor_at is not None and now >= censor_at:
            return pending_reason, "censor"
        return pending_reason, "close_pending"
    if stop is not None and now >= stop:
        if _number(current.get("bid")) is not None:
            return "time_stop", "exit"
        censor_at = _episode_censor_at(active, stop=stop, policy=policy)
        if censor_at is not None and now >= censor_at:
            return "time_stop", "censor"
        return "time_stop", "close_pending"
    spx = _spx_reference(latest, current, active=active, now=now)
    es = _direct_reference(latest, "future:ES", as_of=now)
    invalidation_spx = _number(active.get("invalidation_spx"))
    target_spx = _number(active.get("target_spx"))
    invalidation_es = _number(active.get("invalidation_es"))
    direction = str(active.get("direction") or "")
    if (invalidation_spx is not None or target_spx is not None) and spx is None:
        return "underlier_data_unavailable", "exit_or_verify"
    if invalidation_spx is not None:
        invalidated = (direction == "up" and spx <= invalidation_spx) or (
            direction == "down" and spx >= invalidation_spx
        )
        if invalidated:
            return _terminal_exit(current, reason="strategy_invalidation", action="exit")
    if invalidation_es is not None:
        if es is None:
            return "underlier_data_unavailable", "exit_or_verify"
        invalidated = (direction == "up" and es <= invalidation_es) or (
            direction == "down" and es >= invalidation_es
        )
        if invalidated:
            return _terminal_exit(current, reason="gth_dip_low_broken", action="exit")
    mid = _number(current.get("mid"))
    if active.get("position_type") == "call_debit_spread":
        width = _number(active.get("spread_width_points"))
        if (
            width is not None
            and mid is not None
            and mid >= width * policy.virtual_gth_spread_saturation_fraction
        ):
            return _terminal_exit(
                current,
                reason="spread_value_saturation",
                action="take_profit_or_exit",
            )
    else:
        entry_mid = _number(active.get("entry_mid"))
        if (
            entry_mid
            and mid is not None
            and mid / entry_mid - 1.0 >= policy.virtual_profit_take_fraction
        ):
            return _terminal_exit(
                current,
                reason="premium_profit_target",
                action="take_profit_or_reduce",
            )
    if target_spx is not None and spx is not None:
        target_reached = (direction == "up" and spx >= target_spx) or (
            direction == "down" and spx <= target_spx
        )
        if target_reached:
            return _terminal_exit(
                current,
                reason="underlier_target_reached",
                action="take_profit",
            )
    call_wall = _number(option_structure.get("call_wall"))
    if (
        active.get("source_kind") == "gth_dip_reclaim_call"
        and call_wall is not None
        and spx is not None
        and spx >= call_wall - policy.virtual_wall_touch_points
    ):
        return _terminal_exit(
            current,
            reason="call_wall_touched",
            action="take_profit_or_reduce",
        )
    quality = current.get("quality") if isinstance(current.get("quality"), Mapping) else {}
    greek_exit_allowed = bool(
        greek_decision.get("mode") == "decision_grade" and quality.get("status") == "ok"
    )
    delta = abs(_number(current.get("delta")) or 0.0)
    if greek_exit_allowed and delta >= policy.greek_delta_saturation:
        return _terminal_exit(current, reason="delta_saturated", action="reduce")
    entry_iv = _number(active.get("entry_iv"))
    iv = _number(current.get("iv"))
    vanna = _number(current.get("vanna_delta_per_vol_point"))
    if (
        greek_exit_allowed
        and macro_event.get("mode") == "post_event"
        and entry_iv is not None
        and iv is not None
        and (entry_iv - iv) * 100.0 >= policy.virtual_iv_drop_vol_points
        and vanna is not None
        and vanna > 0
    ):
        return _terminal_exit(
            current,
            reason="post_event_iv_crush_vanna_drag",
            action="take_profit_or_exit",
        )
    entry_gamma = _number(active.get("entry_gamma"))
    gamma = _number(current.get("gamma_per_point"))
    color = _number(current.get("color_gamma_per_minute"))
    if (
        greek_exit_allowed
        and entry_gamma is not None
        and entry_gamma > 0
        and gamma is not None
        and gamma / entry_gamma <= policy.virtual_gamma_retention_fraction
        and color is not None
        and color < 0
    ):
        return _terminal_exit(current, reason="gamma_convexity_decayed", action="exit")
    # Market-data degradation must not erase a terminal trigger already
    # observed from the underlier, a wall, or a valid Greek input.  Those
    # reasons are evaluated first and become CLOSE_PENDING when bid is absent.
    if not current or mid is None:
        return "option_mark_unavailable", "exit_or_verify"
    return None, None


def _terminal_exit(
    current: Mapping[str, object],
    *,
    reason: str,
    action: str,
) -> tuple[str, str]:
    if _number(current.get("bid")) is None:
        return reason, "close_pending"
    return reason, action


def _terminal_exit_action(reason: str) -> str:
    return _TERMINAL_EXIT_ACTIONS.get(reason, "exit")


def _locked_pending_reason(active: Mapping[str, object]) -> str | None:
    if active.get("status") != "close_pending":
        return None
    reason = str(active.get("close_pending_reason") or "").strip()
    if reason == "time_stop_exit_bid_unavailable":
        return "time_stop"
    return reason or None


def _rth_trade_hard_exit(
    active: Mapping[str, object],
    *,
    now: datetime,
) -> datetime | None:
    """Cap persisted RTH trade-intent episodes at 13:00 ET, including legacy state."""

    if active.get("source_kind") != "trade_intent":
        return None
    session_day = _session_day(active.get("session_id"))
    if session_day is None:
        opened_at = _time(active.get("opened_at"))
        session_day = (opened_at or _utc(now)).astimezone(ET).date()
    return datetime.combine(session_day, time(13, 0), tzinfo=ET).astimezone(timezone.utc)


def _cap_rth_trade_episode(
    active: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Migrate a persisted RTH episode to its effective 13:00 ET lifecycle contract."""

    result = dict(active)
    hard_exit = _rth_trade_hard_exit(result, now=now)
    if hard_exit is None:
        return result
    persisted_stop = _time(result.get("time_stop_at"))
    effective_stop = min(persisted_stop, hard_exit) if persisted_stop else hard_exit
    if persisted_stop != effective_stop:
        result["pre_hard_exit_time_stop_at"] = result.get("time_stop_at")
        result["time_stop_at"] = effective_stop.isoformat()
        result["rth_hard_exit_at"] = hard_exit.isoformat()
        result["time_stop_policy"] = "rth_trade_intent_1300_et_cap"
    valid_until = _time(result.get("valid_until"))
    if valid_until is None or valid_until > effective_stop:
        result["pre_hard_exit_valid_until"] = result.get("valid_until")
        result["valid_until"] = effective_stop.isoformat()
    return result


def _session_day(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    try:
        return date.fromisoformat(normalized)
    except ValueError:
        pass
    try:
        return datetime.strptime(normalized, "%Y%m%d").date()
    except ValueError:
        return None


def _episode_censor_at(
    active: Mapping[str, object],
    *,
    stop: datetime,
    policy: MarketFeatureSettings,
) -> datetime | None:
    """Bound a missing time-stop bid by typed quote grace and expiry close."""

    grace_deadline = _utc(stop) + timedelta(
        seconds=max(0.0, policy.trade_quote_max_age_seconds)
    )
    session_close = _episode_session_close(active)
    return min(grace_deadline, session_close) if session_close is not None else grace_deadline


def _locked_exit_censor_at(
    active: Mapping[str, object],
    *,
    stop: datetime | None,
) -> datetime | None:
    """End a previously triggered exit no later than its lifecycle/session boundary."""

    deadlines = [deadline for deadline in (stop, _episode_session_close(active)) if deadline]
    return min(deadlines) if deadlines else None


def _episode_session_close(active: Mapping[str, object]) -> datetime | None:
    session_day = _session_day(active.get("session_id"))
    if session_day is None:
        opened_at = _time(active.get("opened_at"))
        if opened_at is not None:
            session_day = opened_at.astimezone(ET).date()
    if session_day is None:
        return None
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    if session is None:
        return None
    return _utc(session.close_at)


def _mark_episode_degraded(
    active: dict[str, object],
    *,
    reason: str,
    now: datetime,
    close_pending: bool,
) -> bool:
    """Persist non-terminal data health while preserving the first exit trigger."""

    transitioned = active.get("health_status") != "degraded"
    active["status"] = "close_pending" if close_pending else "active"
    active["health_status"] = "degraded"
    active["health_reason"] = f"{reason}_exit_bid_unavailable" if close_pending else reason
    if transitioned or _time(active.get("health_since")) is None:
        active["health_since"] = _utc(now).isoformat()
    if transitioned:
        active.pop("health_recovered_at", None)
    active["last_health_observed_at"] = _utc(now).isoformat()
    if close_pending:
        if _time(active.get("close_pending_since")) is None:
            active["close_pending_since"] = _utc(now).isoformat()
        if not str(active.get("close_pending_reason") or "").strip():
            active["close_pending_reason"] = reason
        if not str(active.get("close_pending_action") or "").strip():
            active["close_pending_action"] = _terminal_exit_action(reason)
    return transitioned


def _recover_episode_health(active: dict[str, object], *, now: datetime) -> bool:
    """Clear transient degradation after all data needed by the observation recovers."""

    recovered = active.get("health_status") == "degraded"
    active["status"] = "active"
    active["health_status"] = "healthy"
    if recovered:
        active["health_recovered_at"] = _utc(now).isoformat()
    active.pop("health_reason", None)
    active.pop("health_since", None)
    active.pop("last_health_observed_at", None)
    active.pop("close_pending_since", None)
    active.pop("close_pending_reason", None)
    active.pop("close_pending_action", None)
    return recovered


def _lifecycle_transition(
    active: dict[str, object],
    current: Mapping[str, object],
    *,
    exit_reason: str | None,
    action: str | None,
    now: datetime,
) -> dict[str, object]:
    """Apply health/exit monotonicity and return persistence-ready evidence."""

    reason = str(exit_reason or "market_data_unavailable")
    if action in {"exit_or_verify", "close_pending"}:
        transitioned = _mark_episode_degraded(
            active,
            reason=reason,
            now=now,
            close_pending=action == "close_pending",
        )
        audit = None
        if transitioned:
            audit = {
                "event": "virtual_degraded",
                "episode_id": active.get("episode_id"),
                "health_status": active.get("health_status"),
                "health_since": active.get("health_since"),
                "health_reason": active.get("health_reason"),
                **_event_contract(active, block_reasons=(reason,)),
            }
        return {"kind": "degraded", "audit": audit}
    if action == "censor":
        censored = {
            **active,
            **_event_contract(active, block_reasons=(str(exit_reason or "exit_price_unavailable"),)),
            "status": "censored",
            "censored_at": now.isoformat(),
            "censor_reason": exit_reason,
            "censor_detail": "exit_bid_unavailable",
            "censor_snapshot": current or None,
            "exit_bid": None,
            "pnl_status": "censored",
        }
        return {"kind": "censored", "episode": censored}
    pending_context = {
        field: active.get(field)
        for field in ("close_pending_since", "close_pending_reason", "close_pending_action")
        if active.get(field) is not None
    }
    recovered = _recover_episode_health(active, now=now)
    recovery_audit = None
    if recovered:
        recovery_audit = {
            "event": "virtual_health_recovered",
            "episode_id": active.get("episode_id"),
            "health_status": active.get("health_status"),
            "health_recovered_at": active.get("health_recovered_at"),
            **_event_contract(active, block_reasons=()),
        }
    return {
        "kind": "ready",
        "pending_exit_context": pending_context,
        "audit": recovery_audit,
    }


def _record_due_horizons(
    storage: StorageSettings,
    active: dict[str, object],
    current: Mapping[str, object],
    *,
    now: datetime,
) -> None:
    opened = _time(active.get("opened_at"))
    if opened is None:
        return
    elapsed = (now - opened).total_seconds() / 60.0
    horizons = dict(active.get("horizon_outcomes") or {})
    entry_mid = _number(active.get("entry_mid"))
    current_mid = _number(current.get("mid"))
    for minutes in (5, 15, 30):
        key = str(minutes)
        if key in horizons or elapsed < minutes or not entry_mid or current_mid is None:
            continue
        row = {
            "horizon_minutes": minutes,
            "observed_at": now.isoformat(),
            "end_return_fraction": current_mid / entry_mid - 1.0,
            "mfe_fraction": active.get("mfe_fraction"),
            "mae_fraction": active.get("mae_fraction"),
            "delta": current.get("delta"),
            "gamma_per_point": current.get("gamma_per_point"),
            "color_gamma_per_minute": current.get("color_gamma_per_minute"),
            "speed_gamma_per_point": current.get("speed_gamma_per_point"),
            "theta_per_minute": current.get("theta_per_minute"),
            "vanna_delta_per_vol_point": current.get("vanna_delta_per_vol_point"),
        }
        horizons[key] = row
        _append_audit(
            storage,
            now,
            {
                "event": "virtual_horizon_outcome",
                "episode_id": active.get("episode_id"),
                "contract_id": active.get("contract_id"),
                **_event_contract(active, block_reasons=()),
                **row,
            },
        )
    active["horizon_outcomes"] = horizons


def _gth_spread_contract_ids(
    spread: Mapping[str, object],
    *,
    session_date: str,
) -> tuple[str, str] | None:
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    if (
        spread.get("right") != "C"
        or long_strike is None
        or short_strike is None
        or long_strike <= 0
        or short_strike <= long_strike
        or not (long_strike / 5.0).is_integer()
        or not (short_strike / 5.0).is_integer()
    ):
        return None
    expiry = session_date.replace("-", "")
    if len(expiry) != 8 or not expiry.isdigit():
        return None
    long_contract = InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=long_strike,
        right="C",
        trading_class="SPXW",
    ).canonical_id
    short_contract = InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=short_strike,
        right="C",
        trading_class="SPXW",
    ).canonical_id
    return long_contract, short_contract


def _contract_snapshot(
    latest: LatestState, contract_id: str, *, now: datetime
) -> dict[str, object]:
    quote = latest.best_quote(contract_id)
    if quote is None or quote.mid is None or quote.quote_time is None:
        return {}
    inputs, _quality = inputs_from_quote(quote, as_of=now)
    if inputs is None:
        return {}
    reference = calculate_contract_reference(inputs)
    source_at = quote.quote_time
    transport_at = quote.last_update_at or quote.received_at
    return {
        "at": now.isoformat(),
        "mid": quote.mid,
        "bid": quote.bid,
        "ask": quote.ask,
        "provider": quote.provider.value,
        "source_at": source_at.isoformat() if source_at is not None else None,
        "transport_at": transport_at.isoformat(),
        "iv": inputs.iv,
        "underlier": inputs.spot,
        "delta": reference.delta,
        "gamma_per_point": reference.gamma_per_point,
        "color_gamma_per_minute": reference.color_gamma_per_minute,
        "speed_gamma_per_point": reference.speed_gamma_per_point,
        "theta_per_minute": reference.theta_per_minute,
        "vanna_delta_per_vol_point": reference.vanna_delta_per_vol_point,
        "quality": reference.quality.to_dict(),
    }


def _action_underlier_snapshot(
    latest: LatestState,
    *,
    instrument_id: str,
    now: datetime,
    max_quote_age_seconds: float,
    future_tolerance_seconds: float,
) -> tuple[dict[str, object], list[str]]:
    """Return one fresh action-time underlier with a matching field clock."""

    quote = latest.best_quote(instrument_id)
    if quote is None:
        return {}, [f"action_underlier_unavailable:{instrument_id}"]
    if (
        quote.bid is not None
        and quote.mid is not None
        and quote.ask is not None
        and 0 < quote.bid <= quote.mid <= quote.ask
        and quote.quote_time is not None
    ):
        price = float(quote.mid)
        price_kind = "mid"
        source_at = quote.quote_time
    elif quote.last is not None and quote.last > 0 and quote.trade_time is not None:
        price = float(quote.last)
        price_kind = "last"
        source_at = quote.trade_time
    else:
        return {}, [f"action_underlier_source_time_unavailable:{instrument_id}"]
    transport_at = quote.last_update_at or quote.received_at
    source_age = (_utc(now) - _utc(source_at)).total_seconds()
    transport_age = (_utc(now) - _utc(transport_at)).total_seconds()
    tolerance = max(0.0, future_tolerance_seconds)
    reasons: list[str] = []
    if source_age < -tolerance:
        reasons.append(f"action_underlier_source_in_future:{instrument_id}")
    elif source_age > max_quote_age_seconds:
        reasons.append(f"action_underlier_source_stale:{instrument_id}")
    if transport_age < -tolerance:
        reasons.append(f"action_underlier_transport_in_future:{instrument_id}")
    elif transport_age > max_quote_age_seconds:
        reasons.append(f"action_underlier_transport_stale:{instrument_id}")
    if reasons:
        return {}, reasons
    use = configured_quote_use_decision(quote, as_of=_utc(now))
    if not use.pricing_allowed:
        return {}, [f"action_underlier_not_pricing_allowed:{instrument_id}"]
    return (
        {
            "instrument_id": instrument_id,
            "price": price,
            "price_kind": price_kind,
            "provider": quote.provider.value,
            "source_at": _utc(source_at).isoformat(),
            "transport_at": _utc(transport_at).isoformat(),
            "source_age_seconds": source_age,
            "transport_age_seconds": transport_age,
        },
        [],
    )


def _active_snapshot_impl(
    latest: LatestState,
    active: Mapping[str, object],
    *,
    now: datetime,
    policy: MarketFeatureSettings,
    spread_snapshot,
) -> dict[str, object]:
    """Build one active mark while preserving legacy GTH exact-spread semantics."""

    if active.get("position_type") == "call_debit_spread":
        snapshot = spread_snapshot(
            latest,
            long_contract_id=str(active.get("long_contract_id") or ""),
            short_contract_id=str(active.get("short_contract_id") or ""),
            now=now,
            max_quote_age_seconds=policy.trade_quote_max_age_seconds,
            max_quote_skew_seconds=policy.provider_sync_tolerance_seconds,
            required_provider=(
                "ibkr" if active.get("source_kind") == "gth_dip_reclaim_call" else None
            ),
        )
        if active.get("source_kind") == "gth_dip_reclaim_call":
            session_date = str(active.get("session_id") or "")
            reference = _gth_chain_reference(
                latest,
                now=now,
                expiry=session_date.replace("-", ""),
                policy=policy,
            )
            if reference is not None:
                snapshot["chain_implied_spx"] = reference
        return snapshot
    return _contract_snapshot(latest, str(active.get("contract_id") or ""), now=now)


def _spx_reference(
    latest: LatestState,
    current: Mapping[str, object],
    *,
    active: Mapping[str, object] | None = None,
    now: datetime | None = None,
) -> float | None:
    for field in ("chain_implied_spx", "action_spx_underlier"):
        reference = current.get(field)
        if isinstance(reference, Mapping):
            price = _number(reference.get("price"))
            if reference.get("kind") == "chain_implied_spx" and price is not None:
                return price
    if (
        isinstance(active, Mapping)
        and active.get("source_kind") == "gth_dip_reclaim_call"
        and not DEFAULT_MARKET_CALENDAR.is_rth_open(now or latest.as_of)
    ):
        return None
    return _direct_reference(latest, "index:SPX", as_of=now)


def _gth_chain_reference(
    latest: LatestState,
    *,
    now: datetime,
    expiry: str,
    policy: MarketFeatureSettings,
) -> dict[str, object] | None:
    if not expiry:
        return None
    return actionable_chain_implied_reference(
        latest,
        expiry=expiry,
        as_of=now,
        required_provider=Provider.IBKR,
        max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
        max_leg_skew_seconds=policy.provider_sync_tolerance_seconds,
        min_pair_count=policy.gth_manual_candidate_min_parity_pairs,
        max_dispersion_points=policy.gth_manual_candidate_max_parity_dispersion_points,
        max_pair_interval_points=policy.gth_manual_candidate_max_parity_interval_points,
    )


def _direct_reference(
    latest: LatestState,
    instrument_id: str,
    *,
    as_of: datetime,
) -> float | None:
    return actionable_live_price(latest, instrument_id, as_of=as_of)
