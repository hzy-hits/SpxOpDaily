"""Session-level structural path retained above individual wall/flip events."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.models import (
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.marketdata import as_utc
from spx_spark.config import StorageSettings
from spx_spark.settings.market_features import MarketFeatureSettings


class SessionEpisodePhase(str, Enum):
    OBSERVING = "observing"
    TREND_EXTENSION = "trend_extension"
    STRUCTURE_BREAK = "structure_break"
    EXTREME = "extreme"
    RECLAIM_PENDING = "reclaim_pending"
    V_REVERSAL_CONFIRMED = "v_reversal_confirmed"
    RECOVERY = "recovery"
    REVERSAL_INVALIDATED = "reversal_invalidated"


def record_session_episode_transition(
    storage: StorageSettings,
    previous: Mapping[str, object] | None,
    current: Mapping[str, object],
    *,
    now: datetime,
) -> bool:
    """Persist only phase changes; the full current path remains in the projection."""

    if previous and previous.get("session_id") == current.get("session_id"):
        if previous.get("phase") == current.get("phase"):
            return False
    path = (
        Path(storage.data_root)
        / "features"
        / "session_episodes"
        / f"date={current.get('session_id') or as_utc(now).date().isoformat()}"
        / "events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "event": "session_episode_transition",
        "record_key": "|".join(
            (
                str(current.get("episode_id") or current.get("session_id") or ""),
                str(current.get("phase_at") or ""),
            )
        ),
        **dict(current),
    }
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(
            descriptor,
            (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode(),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return True


def advance_session_episode(
    previous: Mapping[str, object] | None,
    *,
    session_id: str,
    now: datetime,
    spot: float | None,
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    """Advance one session episode while preserving the ordered structural path."""

    now = as_utc(now)
    if not policy.session_episode_enabled:
        return {
            "schema_version": 1,
            "session_id": session_id,
            "phase": "disabled",
            "updated_at": now.isoformat(),
        }
    parsed_spot = _number(spot)
    state = dict(previous or {})
    if state.get("session_id") != session_id:
        state = _initial_state(session_id, parsed_spot, now=now)
    if parsed_spot is None:
        state.update(
            {
                "updated_at": now.isoformat(),
                "quality": "spot_unavailable",
                "automatic_ordering": False,
            }
        )
        return state

    prior_spot = _number(state.get("current_spot"))
    prior_high = _number(state.get("session_high")) or parsed_spot
    prior_low = _number(state.get("session_low")) or parsed_spot
    state["session_high"] = max(prior_high, parsed_spot)
    state["session_low"] = min(prior_low, parsed_spot)
    state["current_spot"] = parsed_spot
    state["updated_at"] = now.isoformat()
    state["quality"] = "ready"
    state["automatic_ordering"] = False
    state["broker_order_state"] = "not_connected"
    state["current_evidence"] = _evidence(market, options)
    phase = _phase(state)
    levels = _levels(options.structure)

    if phase in {SessionEpisodePhase.OBSERVING, SessionEpisodePhase.TREND_EXTENSION}:
        broken = _crossed_level(
            prior_spot,
            parsed_spot,
            levels,
            buffer=policy.session_break_buffer_points,
        )
        if broken is not None:
            direction, level_kind, level = broken
            state.update(
                {
                    "episode_id": _episode_id(session_id, direction, level_kind, now),
                    "break_direction": direction,
                    "reversal_direction": "up" if direction == "down" else "down",
                    "break_level_kind": level_kind,
                    "break_level": level,
                    "break_at": now.isoformat(),
                    "break_spot": parsed_spot,
                    "extreme_spot": parsed_spot,
                    "extreme_at": now.isoformat(),
                    "pre_break_high": prior_high,
                    "pre_break_low": prior_low,
                    "frozen_levels": levels,
                    "reclaim_at": None,
                }
            )
            _transition(
                state,
                SessionEpisodePhase.STRUCTURE_BREAK,
                "structural_level_crossed",
                now=now,
            )
        elif _is_extension(state, parsed_spot, options, policy=policy):
            open_spot = _number(state.get("session_open")) or parsed_spot
            state["extension_direction"] = "up" if parsed_spot >= open_spot else "down"
            _transition(
                state,
                SessionEpisodePhase.TREND_EXTENSION,
                "session_move_extended",
                now=now,
            )
        return state

    break_direction = str(state.get("break_direction") or "")
    break_level = _number(state.get("break_level"))
    if break_direction not in {"up", "down"} or break_level is None:
        _transition(
            state,
            SessionEpisodePhase.OBSERVING,
            "episode_identity_unavailable",
            now=now,
        )
        return state

    _update_extreme(state, parsed_spot, now=now)
    if phase in {SessionEpisodePhase.STRUCTURE_BREAK, SessionEpisodePhase.EXTREME}:
        if _reclaimed(
            parsed_spot,
            break_level,
            break_direction=break_direction,
            buffer=policy.session_break_buffer_points,
        ):
            state["reclaim_at"] = now.isoformat()
            state["reclaim_spot"] = parsed_spot
            _transition(
                state,
                SessionEpisodePhase.RECLAIM_PENDING,
                "broken_level_reclaimed",
                now=now,
            )
        elif _extreme_extension(state) >= policy.session_extreme_extension_points:
            _transition(
                state,
                SessionEpisodePhase.EXTREME,
                "break_extended_to_session_extreme",
                now=now,
            )
        return state

    if phase is SessionEpisodePhase.RECLAIM_PENDING:
        if _reclaim_lost(
            parsed_spot,
            break_level,
            break_direction=break_direction,
            buffer=policy.session_break_buffer_points,
        ):
            state["reclaim_at"] = None
            _transition(
                state,
                SessionEpisodePhase.EXTREME,
                "reclaim_failed",
                now=now,
            )
            return state
        reclaim_at = _time(state.get("reclaim_at"))
        held = (now - reclaim_at).total_seconds() if reclaim_at is not None else 0.0
        state["reclaim_hold_seconds"] = max(held, 0.0)
        if held >= policy.session_reclaim_hold_seconds and _cross_asset_reversal(
            market,
            break_direction=break_direction,
        ):
            state["reversal_confirmed_at"] = now.isoformat()
            state["reversal_evidence"] = _evidence(market, options)
            _transition(
                state,
                SessionEpisodePhase.V_REVERSAL_CONFIRMED,
                "reclaim_held_with_cross_asset_confirmation",
                now=now,
            )
        return state

    if phase in {
        SessionEpisodePhase.V_REVERSAL_CONFIRMED,
        SessionEpisodePhase.RECOVERY,
    }:
        if _reclaim_lost(
            parsed_spot,
            break_level,
            break_direction=break_direction,
            buffer=policy.session_break_buffer_points,
        ):
            _transition(
                state,
                SessionEpisodePhase.REVERSAL_INVALIDATED,
                "confirmed_reclaim_lost",
                now=now,
            )
            return state
        recovery_ratio = _recovery_ratio(state, parsed_spot)
        state["recovery_ratio"] = recovery_ratio
        if recovery_ratio is not None and recovery_ratio >= policy.session_recovery_ratio:
            _transition(
                state,
                SessionEpisodePhase.RECOVERY,
                "session_excursion_recovered",
                now=now,
            )
    return state


def _initial_state(
    session_id: str,
    spot: float | None,
    *,
    now: datetime,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "session_id": session_id,
        "phase": SessionEpisodePhase.OBSERVING.value,
        "phase_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "session_open": spot,
        "session_high": spot,
        "session_low": spot,
        "current_spot": spot,
        "transition_history": [],
        "quality": "ready" if spot is not None else "spot_unavailable",
        "automatic_ordering": False,
        "broker_order_state": "not_connected",
    }


def _transition(
    state: dict[str, object],
    phase: SessionEpisodePhase,
    reason: str,
    *,
    now: datetime,
) -> None:
    previous = str(state.get("phase") or SessionEpisodePhase.OBSERVING.value)
    if previous == phase.value:
        return
    state["phase"] = phase.value
    state["phase_at"] = now.isoformat()
    state["transition_reason"] = reason
    history = [
        dict(item) for item in state.get("transition_history") or [] if isinstance(item, Mapping)
    ]
    history.append(
        {
            "at": now.isoformat(),
            "previous_phase": previous,
            "phase": phase.value,
            "reason": reason,
            "spot": state.get("current_spot"),
            "break_level": state.get("break_level"),
        }
    )
    state["transition_history"] = history[-100:]


def _levels(structure: Mapping[str, object]) -> dict[str, float]:
    result: dict[str, float] = {}
    for key in ("put_wall", "call_wall"):
        value = _number(structure.get(key))
        if value is not None:
            result[key] = value
    flip = structure.get("flip_zone")
    if isinstance(flip, list | tuple) and len(flip) >= 2:
        low = _number(flip[0])
        high = _number(flip[1])
        if low is not None:
            result["flip_low"] = low
        if high is not None:
            result["flip_high"] = high
    return result


def _crossed_level(
    previous: float | None,
    current: float,
    levels: Mapping[str, float],
    *,
    buffer: float,
) -> tuple[str, str, float] | None:
    if previous is None or current == previous:
        return None
    if current < previous:
        rows = [
            (kind, level)
            for kind in ("flip_low", "put_wall")
            if (level := levels.get(kind)) is not None
            and previous >= level - buffer
            and current < level - buffer
        ]
        if rows:
            kind, level = max(rows, key=lambda item: item[1])
            return "down", kind, level
    else:
        rows = [
            (kind, level)
            for kind in ("flip_high", "call_wall")
            if (level := levels.get(kind)) is not None
            and previous <= level + buffer
            and current > level + buffer
        ]
        if rows:
            kind, level = min(rows, key=lambda item: item[1])
            return "up", kind, level
    return None


def _is_extension(
    state: Mapping[str, object],
    spot: float,
    options: OptionStructureFrame,
    *,
    policy: MarketFeatureSettings,
) -> bool:
    open_spot = _number(state.get("session_open"))
    if open_spot is None:
        return False
    expected_move = _number(options.volatility.get("expected_move_points_0dte")) or 0.0
    threshold = max(
        policy.session_extreme_extension_points,
        expected_move * 0.25,
    )
    return abs(spot - open_spot) >= threshold


def _update_extreme(state: dict[str, object], spot: float, *, now: datetime) -> None:
    direction = str(state.get("break_direction") or "")
    prior = _number(state.get("extreme_spot"))
    extreme = (
        min(prior, spot)
        if direction == "down" and prior is not None
        else max(prior, spot)
        if direction == "up" and prior is not None
        else spot
    )
    if extreme != prior:
        state["extreme_spot"] = extreme
        state["extreme_at"] = now.isoformat()


def _extreme_extension(state: Mapping[str, object]) -> float:
    level = _number(state.get("break_level"))
    extreme = _number(state.get("extreme_spot"))
    if level is None or extreme is None:
        return 0.0
    return level - extreme if state.get("break_direction") == "down" else extreme - level


def _reclaimed(
    spot: float,
    level: float,
    *,
    break_direction: str,
    buffer: float,
) -> bool:
    return spot >= level + buffer if break_direction == "down" else spot <= level - buffer


def _reclaim_lost(
    spot: float,
    level: float,
    *,
    break_direction: str,
    buffer: float,
) -> bool:
    return spot < level - buffer if break_direction == "down" else spot > level + buffer


def _cross_asset_reversal(
    market: MinuteMarketFrame,
    *,
    break_direction: str,
) -> bool:
    reversal_sign = 1.0 if break_direction == "down" else -1.0
    one = _number(market.es.get("return_1m_points"))
    five = _number(market.es.get("return_5m_points"))
    fifteen = _number(market.es.get("return_15m_points"))
    cross = str(market.cross_asset.get("es_spy_direction_confirmation_15m") or "")
    return bool(
        cross == "confirmed"
        and one is not None
        and five is not None
        and fifteen is not None
        and one * reversal_sign > 0
        and five * reversal_sign > 0
        and fifteen * reversal_sign > 0
    )


def _recovery_ratio(state: Mapping[str, object], spot: float) -> float | None:
    extreme = _number(state.get("extreme_spot"))
    if extreme is None:
        return None
    if state.get("break_direction") == "down":
        anchor = _number(state.get("pre_break_high"))
        denominator = anchor - extreme if anchor is not None else None
        value = spot - extreme
    else:
        anchor = _number(state.get("pre_break_low"))
        denominator = extreme - anchor if anchor is not None else None
        value = extreme - spot
    if denominator is None or denominator <= 0:
        return None
    return max(0.0, min(value / denominator, 1.0))


def _evidence(
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
) -> dict[str, object]:
    volume = options.exposure.get("volume_weighted")
    volume = volume if isinstance(volume, Mapping) else {}
    gamma_ratio = _number(volume.get("net_gamma_ratio"))
    if gamma_ratio is None:
        gamma_ratio = _number(options.structure.get("net_gamma_ratio"))
    return {
        "es_return_1m_points": market.es.get("return_1m_points"),
        "es_return_5m_points": market.es.get("return_5m_points"),
        "es_return_15m_points": market.es.get("return_15m_points"),
        "es_spy_confirmation_15m": market.cross_asset.get("es_spy_direction_confirmation_15m"),
        "atm_iv_change_5m": options.volatility.get("atm_iv_change_5m"),
        "atm_iv_change_15m": options.volatility.get("atm_iv_change_15m"),
        "net_gamma_ratio_proxy": gamma_ratio,
        "gamma_sign_convention": options.exposure.get("sign_convention"),
        "dealer_position_sign": options.exposure.get("dealer_position_sign"),
    }


def _episode_id(
    session_id: str,
    direction: str,
    level_kind: str,
    now: datetime,
) -> str:
    token = f"{session_id}|{direction}|{level_kind}|{now.isoformat()}"
    return "session-episode:" + hashlib.sha256(token.encode()).hexdigest()[:20]


def _phase(state: Mapping[str, object]) -> SessionEpisodePhase:
    try:
        return SessionEpisodePhase(str(state.get("phase") or "observing"))
    except ValueError:
        return SessionEpisodePhase.OBSERVING


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return as_utc(datetime.fromisoformat(value))
    except ValueError:
        return None
