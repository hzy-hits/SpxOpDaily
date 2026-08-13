"""Causal source validation for one-shot GTH trend-transition entries."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Mapping

from spx_spark.application.market_features.virtual_strategy_support import (
    _number,
    _time,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import InstrumentId, Provider
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.strategy_contract import policy_version


ES_TREND_SOURCE_MODES = frozenset({"trend", "trend_advance"})
ADVANCE_SOURCE_KIND = "gth_es_trend_advance"
TRANSITION_SOURCE_KIND = "gth_es_trend_transition"


def is_es_trend_source(source_mode: str) -> bool:
    return source_mode in ES_TREND_SOURCE_MODES


def resolve_gth_manual_source(
    level_decision: Mapping[str, object],
    trend_state: Mapping[str, object],
    *,
    now: datetime,
    ttl_seconds: float,
    max_source_lag_seconds: float,
) -> tuple[str, Mapping[str, object], str, str | None, int, list[str], str | None]:
    """Prefer a fresh session-advance, then a valid transition, then the confirmed-level path."""

    advance_event, advance_reasons = current_gth_trend_advance(
        trend_state,
        now=now,
        ttl_seconds=ttl_seconds,
        max_source_lag_seconds=max_source_lag_seconds,
    )
    if advance_event is not None and not advance_reasons:
        event_id = str(advance_event["source_event_id"])
        return "trend_advance", advance_event, event_id, ADVANCE_SOURCE_KIND, 0, [], None
    trend_event, trend_reasons = current_gth_trend_transition(
        trend_state,
        now=now,
        ttl_seconds=ttl_seconds,
        max_source_lag_seconds=max_source_lag_seconds,
    )
    if trend_event is not None and not trend_reasons:
        event_id = str(trend_event["source_event_id"])
        return "trend", trend_event, event_id, TRANSITION_SOURCE_KIND, 0, [], None
    level_expiry = _time(level_decision.get("expires_at"))
    level_ready = bool(
        level_decision.get("formal_signal") is True
        and str(level_decision.get("phase") or "") == "confirmed"
        and level_decision.get("quality_ok") is True
        and level_expiry is not None
        and level_expiry > now
    )
    if level_ready:
        event_id = str(level_decision.get("event_id") or "")
        generation = level_decision.get("reentry_generation", 0)
        generation = (
            generation
            if isinstance(generation, int) and not isinstance(generation, bool)
            else 0
        )
        return (
            "level",
            level_decision,
            event_id,
            "gth_confirmed_level_path",
            max(generation, 0),
            [],
            None,
        )
    if trend_event is not None:
        event_id = str(trend_event["source_event_id"])
        return (
            "trend",
            trend_event,
            event_id,
            "gth_es_trend_transition",
            0,
            trend_reasons,
            None,
        )
    level_absence_reasons = _level_source_absence_reasons(level_decision, now)
    level_tombstone_id = (
        str(level_decision.get("event_id") or "") or None
        if level_absence_reasons
        else None
    )
    return (
        "none",
        {},
        "",
        None,
        0,
        list(dict.fromkeys([*trend_reasons, *level_absence_reasons])),
        level_tombstone_id,
    )


def _level_source_absence_reasons(
    level_decision: Mapping[str, object],
    now: datetime,
) -> list[str]:
    """Describe an explicit level lifecycle end without treating an empty frame as one."""

    if not str(level_decision.get("event_id") or ""):
        return []
    reasons: list[str] = []
    phase = str(level_decision.get("phase") or "")
    if phase == "invalidated":
        reasons.append("level_source_invalidated")
    elif phase == "expired":
        reasons.append("level_source_expired")
    elif phase != "confirmed":
        reasons.append("level_source_not_confirmed")
    if level_decision.get("formal_signal") is not True:
        reasons.append("level_source_formal_signal_absent")
    if level_decision.get("quality_ok") is not True:
        reasons.append("level_source_quality_invalid")
    expiry = _time(level_decision.get("expires_at"))
    if expiry is None:
        reasons.append("level_source_expiry_unavailable")
    elif expiry <= now:
        reasons.append("level_source_expired")
    return list(dict.fromkeys(reasons))


def manual_source_path_fields(
    source_mode: str,
    level_decision: Mapping[str, object],
    source: Mapping[str, object],
) -> tuple[str, str, str]:
    if is_es_trend_source(source_mode):
        return "breakout", str(source.get("direction") or ""), "trend"
    return (
        str(level_decision.get("thesis") or ""),
        str(level_decision.get("direction") or ""),
        str(level_decision.get("level_kind") or ""),
    )


def source_policy_fields(source_mode: str) -> dict[str, str]:
    if source_mode == "trend_advance":
        return {
            "directional_source": "confirmed_gth_trend_advance.v1",
            "source_priority": "fresh_advance_then_level_then_transition",
        }
    if source_mode == "trend":
        return {
            "directional_source": "confirmed_gth_trend_transition.v1",
            "source_priority": "fresh_advance_then_level_then_transition",
        }
    return {
        "directional_source": "confirmed_frozen_level_path.v2",
        "breakout_crossing": "inside_to_outside_required",
        "breakout_extension": "outside_retest_zone_before_return_required",
        "breakout_retest": "required",
    }


def build_candidate_policy_version(
    source_mode: str,
    policy: MarketFeatureSettings,
    *,
    contract_version: str,
    spread_widths: tuple[float, float, float],
) -> str:
    spread_min, spread_default, spread_max = spread_widths
    return policy_version(
        contract_version,
        {
            "quote_max_age_seconds": policy.gth_manual_candidate_quote_max_age_seconds,
            "ttl_seconds": policy.gth_manual_candidate_ttl_seconds,
            **source_policy_fields(source_mode),
            "max_debit_fraction": policy.gth_manual_candidate_max_debit_fraction,
            "max_net_spread_fraction": policy.gth_manual_candidate_max_net_spread_fraction,
            "min_parity_pairs": policy.gth_manual_candidate_min_parity_pairs,
            "target_room_buffer_points": policy.gth_manual_candidate_target_room_buffer_points,
            "expiry_payoff_ratio_diagnostic_floor": (
                policy.gth_manual_candidate_min_reward_risk
            ),
            "operator_edge_authority": "validated_first_touch_time_stop_net_pnl",
            "negative_play_stats_veto_enabled": policy.gth_negative_play_stats_veto_enabled,
            "play_stats_min_samples": policy.play_stats_min_samples,
            "invalidation_buffer_points": policy.trade_invalidation_buffer_points,
            "time_stop_minutes": policy.trade_time_stop_minutes,
            "spread_width_points": {
                "min": spread_min,
                "default": spread_default,
                "max": spread_max,
            },
        },
    )


def current_gth_trend_transition(
    trend_state: Mapping[str, object],
    *,
    now: datetime,
    ttl_seconds: float,
    max_source_lag_seconds: float,
) -> tuple[dict[str, object] | None, list[str]]:
    """Return one fresh confirmed transition, never a static regime or continuation."""

    raw_event = trend_state.get("last_transition")
    if not isinstance(raw_event, Mapping):
        return None, []
    expected_session = f"{DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()}:gth"
    if str(trend_state.get("session_id") or "") != expected_session:
        return None, ["trend_transition_session_mismatch"]
    # The ES trend machine admits only source/transport-fresh LIVE quotes and
    # prefers IBKR during GTH, but it intentionally falls back to fresh Schwab
    # ES when the IBKR base line has a short gap.  Directional provenance and
    # executable option provenance are separate: the candidate still requires
    # fresh IBKR SPXW parity plus both exact spread legs below.
    transition_provider = str(raw_event.get("provider") or "").lower()
    if transition_provider not in {Provider.IBKR.value, Provider.SCHWAB.value}:
        return None, ["trend_transition_provider_unsupported"]

    source = _normalize_transition(raw_event, expected_session=expected_session)
    if source is None:
        return None, ["trend_transition_unavailable"]
    expected_regime = "bullish" if source["direction"] == "up" else "bearish"
    if trend_state.get("regime") != expected_regime or trend_state.get(
        "transition_sequence"
    ) != source.get("sequence"):
        return None, ["trend_transition_state_mismatch"]

    at = _time(source.get("at"))
    source_at = _time(source.get("source_at"))
    if at is None or source_at is None:
        return None, ["trend_transition_timestamp_unavailable"]
    source_lag = (at - source_at).total_seconds()
    if source_lag < -1.0 or source_lag > max_source_lag_seconds:
        return None, ["trend_transition_source_stale_at_confirmation"]
    age = (now - at).total_seconds()
    if age < -1.0:
        return None, ["trend_transition_in_future"]
    if age > ttl_seconds:
        return source, ["source_signal_expired"]
    return source, []


def current_gth_trend_advance(
    trend_state: Mapping[str, object],
    *,
    now: datetime,
    ttl_seconds: float,
    max_source_lag_seconds: float,
) -> tuple[dict[str, object] | None, list[str]]:
    """Return one fresh session-advance or continuation m1, never a regime flip."""

    expected_session = f"{DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()}:gth"
    if str(trend_state.get("session_id") or "") != expected_session:
        return None, []
    candidates: list[Mapping[str, object]] = []
    raw_advance = trend_state.get("last_advance")
    if isinstance(raw_advance, Mapping):
        candidates.append(raw_advance)
    raw_continuation = trend_state.get("last_continuation")
    if (
        isinstance(raw_continuation, Mapping)
        and raw_continuation.get("event_type") == "continuation"
        and raw_continuation.get("signal_stage") == "entry_advisory"
    ):
        candidates.append(raw_continuation)
    for raw_event in candidates:
        source, reasons = _validate_advance_event(
            raw_event,
            expected_session=expected_session,
            now=now,
            ttl_seconds=ttl_seconds,
            max_source_lag_seconds=max_source_lag_seconds,
        )
        if source is not None and not reasons:
            return source, []
    return None, []


def _validate_advance_event(
    raw_event: Mapping[str, object],
    *,
    expected_session: str,
    now: datetime,
    ttl_seconds: float,
    max_source_lag_seconds: float,
) -> tuple[dict[str, object] | None, list[str]]:
    provider = str(raw_event.get("provider") or "").lower()
    if provider not in {Provider.IBKR.value, Provider.SCHWAB.value}:
        return None, ["trend_advance_provider_unsupported"]
    source = _normalize_advance(raw_event, expected_session=expected_session)
    if source is None:
        return None, ["trend_advance_unavailable"]
    at = _time(source.get("at"))
    source_at = _time(source.get("source_at"))
    if at is None or source_at is None:
        return None, ["trend_advance_timestamp_unavailable"]
    source_lag = (at - source_at).total_seconds()
    if source_lag < -1.0 or source_lag > max_source_lag_seconds:
        return None, ["trend_advance_source_stale_at_confirmation"]
    age = (now - at).total_seconds()
    if age < -1.0:
        return None, ["trend_advance_in_future"]
    if age > ttl_seconds:
        return source, ["source_signal_expired"]
    return source, []


def trend_transition_expiry(
    source: Mapping[str, object],
    *,
    ttl_seconds: float,
) -> datetime | None:
    at = _time(source.get("at"))
    return at + timedelta(seconds=ttl_seconds) if at is not None else None


def manual_source_expiry(
    source_mode: str,
    source: Mapping[str, object],
    *,
    ttl_seconds: float,
) -> datetime | None:
    if source_mode == "level":
        return _time(source.get("expires_at"))
    return trend_transition_expiry(source, ttl_seconds=ttl_seconds)


def trend_anchor_geometry(
    source: Mapping[str, object],
    parity: Mapping[str, object],
    es_reference: Mapping[str, object],
    *,
    invalidation_buffer_points: float,
    target_distance_points: float,
) -> dict[str, float] | None:
    anchor_es = _number(source.get("price"))
    current_es = _number(es_reference.get("price"))
    current_parity = _number(parity.get("price"))
    direction = str(source.get("direction") or "")
    if (
        anchor_es is None
        or current_es is None
        or current_parity is None
        or direction not in {"up", "down"}
    ):
        return None
    basis_points = current_es - current_parity
    anchor_spx = anchor_es - basis_points
    if direction == "up":
        invalidation_spx = anchor_spx - invalidation_buffer_points
        target_spx = anchor_spx + target_distance_points
    else:
        invalidation_spx = anchor_spx + invalidation_buffer_points
        target_spx = anchor_spx - target_distance_points
    return {
        "anchor_es": anchor_es,
        "anchor_spx": anchor_spx,
        "basis_points": basis_points,
        "invalidation_spx": invalidation_spx,
        "target_spx": target_spx,
    }


def candidate_geometry_context(
    source_mode: str,
    source: Mapping[str, object],
    level_decision: Mapping[str, object],
    levels: Mapping[str, object],
    level_kind: str,
    parity: Mapping[str, object] | None,
    es_reference: Mapping[str, object] | None,
    *,
    invalidation_buffer_points: float,
    target_distance_points: float,
) -> tuple[dict[str, object], list[str]]:
    selection_spx = (
        _number(parity.get("price"))
        if is_es_trend_source(source_mode) and parity is not None
        else _number(level_decision.get("level")) or _number(levels.get(level_kind))
    )
    reasons = [] if selection_spx is not None else ["trigger_level_unavailable"]
    if es_reference is None:
        reasons.append("direct_es_invalidation_unavailable")
    basis = (
        float(es_reference["price"]) - float(parity["price"])
        if is_es_trend_source(source_mode) and es_reference is not None and parity is not None
        else _number(level_decision.get("es_basis_points"))
    )
    if basis is None:
        reasons.append("es_basis_unavailable")
    trend_geometry = (
        trend_anchor_geometry(
            source,
            parity,
            es_reference,
            invalidation_buffer_points=invalidation_buffer_points,
            target_distance_points=target_distance_points,
        )
        if is_es_trend_source(source_mode) and parity is not None and es_reference is not None
        else None
    )
    if is_es_trend_source(source_mode) and trend_geometry is None:
        reasons.append("trend_anchor_geometry_unavailable")
    trigger_level = (
        float(trend_geometry["anchor_spx"])
        if trend_geometry is not None
        else selection_spx
    )
    return {
        "selection_spx": selection_spx,
        "trigger_level": trigger_level,
        "basis_points": basis,
        "trend_geometry": trend_geometry,
    }, reasons


def confirmation_baseline(
    source_mode: str,
    source: Mapping[str, object],
    level_decision: Mapping[str, object],
    parity: Mapping[str, object],
    trend_geometry: Mapping[str, object] | None,
) -> dict[str, object]:
    return {
        "at": level_decision.get("phase_at") if source_mode == "level" else source.get("at"),
        "parity_spx": (
            float(trend_geometry["anchor_spx"])
            if trend_geometry is not None
            else float(parity["price"])
        ),
        "es": float(trend_geometry["anchor_es"]) if trend_geometry is not None else None,
        "semantics": (
            "causal_trend_transition_coordinate"
            if trend_geometry is not None
            else "state_machine_confirmation_coordinate"
        ),
    }


def candidate_trigger_coordinate(
    level_decision: Mapping[str, object],
    source: Mapping[str, object],
    trigger_level: float,
    trend_geometry: Mapping[str, object] | None,
) -> dict[str, object]:
    if trend_geometry is None:
        raw = level_decision.get("trigger_coordinate")
        return dict(raw) if isinstance(raw, Mapping) else {}
    return {
        "kind": "chain_implied_spx_from_es_transition_anchor",
        "instrument_id": "synthetic:SPXW_PARITY",
        "observed_value": trigger_level,
        "target_value": trigger_level,
        "basis_points": float(trend_geometry["basis_points"]),
        "as_of": source.get("at"),
    }


def spxw_contract_id(expiry: str, strike: float, right: str) -> str:
    return InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=strike,
        right=right,
        trading_class="SPXW",
    ).canonical_id


def _normalize_transition(
    event: Mapping[str, object],
    *,
    expected_session: str,
) -> dict[str, object] | None:
    session_id = str(event.get("session_id") or "")
    event_id = str(event.get("event_id") or "")
    sequence = event.get("sequence")
    provider = str(event.get("provider") or "")
    price = _number(event.get("price"))
    regime = str(event.get("to_regime") or "")
    if (
        session_id != expected_session
        or regime not in {"bullish", "bearish"}
        or not isinstance(sequence, int)
        or sequence <= 0
        or event_id != f"globex-trend:{session_id}:{sequence}:{regime}"
        or not provider
        or price is None
        or price <= 0
        or event.get("automatic_ordering") is not False
        or event.get("event_type") != "transition"
        or event.get("operator_action") != "observe_only"
    ):
        return None
    return {
        **event,
        "source_event_id": event_id,
        "source_event_type": "transition",
        "direction": "up" if regime == "bullish" else "down",
    }


def _normalize_advance(
    event: Mapping[str, object],
    *,
    expected_session: str,
) -> dict[str, object] | None:
    session_id = str(event.get("session_id") or "")
    event_id = str(event.get("event_id") or "")
    provider = str(event.get("provider") or "")
    price = _number(event.get("price"))
    direction = str(event.get("direction") or "")
    event_type = str(event.get("event_type") or "")
    if (
        session_id != expected_session
        or direction not in {"up", "down"}
        or event_type not in {"advance", "continuation"}
        or event.get("signal_stage") != "entry_advisory"
        or not event_id
        or not provider
        or price is None
        or price <= 0
        or event.get("automatic_ordering") is not False
        or event.get("operator_action")
        not in {"evaluate_call_setup", "evaluate_put_setup"}
    ):
        return None
    return {
        **event,
        "source_event_id": event_id,
        "source_event_type": event_type,
        "direction": direction,
    }
