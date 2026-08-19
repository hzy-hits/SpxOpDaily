"""Candidate enumeration for strategy-decision competition."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.analytics.options.pricing import usable_delta
from spx_spark.analytics.options.strategy_payoff import (
    butterfly_economics,
    conservative_butterfly_bbo,
    conservative_vertical_bbo,
    debit_vertical_reach_reasons,
    vertical_economics,
    vertical_width_path_reasons,
)
from spx_spark.application.market_features.market import quote_source_at
from spx_spark.application.market_features.session_quote_selection import provider_quote
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    StrategyPolicy,
    hmm_owns_trend_direction,
    pin_blocks_directional_spreads,
    pin_look_trade_widths,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import InstrumentId, Provider
from spx_spark.storage import LatestState

WIDTHS: tuple[float, ...] = (5.0, 10.0, 15.0, 20.0)
GTH_WIDTH_SCAN = "GTH_WIDTH_SCAN"
GTH_DELTA_SCAN = "GTH_DELTA_SCAN"
GTH_ATM_PIN = "GTH_ATM_PIN"
PREAVERAGE15_PULLBACK = "PREAVERAGE15_PULLBACK"
WALL_BREAKOUT_HAZARD = "WALL_BREAKOUT_HAZARD"
_EXPIRED_GTH_REASONS = {
    "source_signal_expired",
    "strategy_event_expired",
    "gth_dip_reclaim_signal_expired",
    "gth_reclaim_too_old",
    "gth_manual_candidate_ttl_elapsed",
    "spread_exit_at_elapsed",
}


def enumerate_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    """Enumerate all currently supported manual-action strategy candidates."""

    now = _utc(now)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in (
        *_vertical_candidates(payload, facts, regime, latest, now=now, policy=policy),
        *_butterfly_candidates(payload, facts, regime, latest, now=now, policy=policy),
    ):
        candidate_id = str(row.get("candidate_id") or "")
        if candidate_id and candidate_id not in seen:
            rows.append(row)
            seen.add(candidate_id)
    rows.sort(key=lambda row: float(row.get("selection_score") or 0.0), reverse=True)
    return rows


def candidate_generation_reasons(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[str]:
    """Return legacy-compatible reasons when enumeration yields no rows."""

    if DEFAULT_MARKET_CALENDAR.is_rth_open(_utc(now)):
        if str(regime.get("terminal_state") or "") == "PIN_STABLE":
            butterfly_reasons = _capability_reasons(facts, "butterfly")
            if butterfly_reasons:
                return butterfly_reasons
            if not _map(payload.get("option_structure_frame")).get("front_expiry"):
                return ["butterfly_expiry_unavailable"]
            return ["butterfly_three_leg_bbo_unavailable"]
        capability_reasons = _capability_reasons(facts, "vertical")
        if capability_reasons:
            return capability_reasons
        _, reasons = _rth_evidences(
            payload, facts, regime, latest, now=now, policy=policy
        )
        if reasons:
            return reasons
        frame = _map(payload.get("option_structure_frame"))
        if not frame.get("front_expiry"):
            return ["vertical_expiry_unavailable"]
        return ["vertical_exact_two_leg_quote_unavailable"]
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(_utc(now)):
        _, evidence_reasons = _gth_evidence(facts)
        reasons = list(evidence_reasons)
        if not _number(_map(facts.get("spot")).get("spx")):
            reasons.append("spx_price_unavailable")
        frame = _map(payload.get("option_structure_frame"))
        if not (frame.get("front_expiry") or payload.get("expiry")):
            reasons.append("vertical_expiry_unavailable")
        return list(dict.fromkeys(reasons or ["gth_width_scan_no_fresh_quote"]))
    return ["session_not_open_for_spxw_strategy"]


def resolve_geometry(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    direction: str | None,
    trigger: float | None,
) -> tuple[float | None, float | None, str | None]:
    """Resolve target/stop geometry without reintroducing selector-local rules."""

    intent = _map(payload.get("trade_intent"))
    geometry = _map(intent.get("confirmation_geometry"))
    target = _number(geometry.get("target_spx"))
    stop = _number(intent.get("invalidation_spx"))
    if target is not None and stop is not None:
        return target, stop, "confirmation_geometry"

    fallback_target, fallback_stop = _facts_wall_ladder_geometry(facts, direction, trigger)
    return (
        target if target is not None else fallback_target,
        stop if stop is not None else fallback_stop,
        "confirmation_geometry" if target is not None else "facts_wall_ladder_fallback",
    )


def _vertical_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        evidences, _ = _rth_evidences(
            payload, facts, regime, latest, now=now, policy=policy
        )
        if not evidences:
            return []
        if _capability_reasons(facts, "vertical") and not any(
            row.get("setup_kind") == PREAVERAGE15_PULLBACK for row in evidences
        ):
            return []
        rows = []
        for evidence in evidences:
            if evidence.get("setup_kind") == PREAVERAGE15_PULLBACK:
                row = _rth_preaverage_vertical(
                    evidence, payload, facts, latest, now=now, policy=policy
                )
                if row:
                    rows.append(row)
                continue
            if _map(evidence.get("long")) and _map(evidence.get("short")):
                rows.append(
                    _vertical_candidate_from_evidence(
                        evidence, facts, now=now, policy=policy
                    )
                )
            rows.extend(
                _rth_width_verticals(
                    evidence, payload, facts, latest, now=now, policy=policy
                )
            )
        return [row for row in rows if row]
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        rows: list[dict[str, Any]] = []
        evidence, _ = _gth_evidence(facts)
        if evidence:
            row = _vertical_candidate_from_evidence(
                evidence, facts, now=now, policy=_gth_quote_policy(policy)
            )
            if row:
                rows.append(row)
        rows.extend(
            _gth_width_verticals(payload, facts, latest, now=now, policy=policy)
        )
        return [row for row in rows if row]
    return []


def _rth_preaverage_vertical(
    evidence: Mapping[str, Any],
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    expiry = str(_map(payload.get("option_structure_frame")).get("front_expiry") or "")
    direction = str(evidence.get("direction") or "")
    right = "C" if direction == "UP" else "P" if direction == "DOWN" else ""
    lane_policy = replace(policy, quote_max_age_seconds=5.0)
    long_strike = nearest_abs_delta_strike(
        latest,
        expiry,
        right,
        target_abs_delta=0.60,
        now=now,
        policy=lane_policy,
        providers=(Provider.SCHWAB,),
        max_greeks_age_seconds=5.0,
    )
    if not expiry or long_strike is None:
        return {}
    short_strike = long_strike + 15.0 if right == "C" else long_strike - 15.0
    legs = _session_option_legs(
        latest,
        expiry,
        ((long_strike, right), (short_strike, right)),
        now=now,
        policy=lane_policy,
        providers=(Provider.SCHWAB,),
    )
    if not legs or any(
        (_number(leg.get("ask")) or 0.0) <= 0.0
        or ((_number(leg.get("ask")) or 0.0) - (_number(leg.get("bid")) or 0.0))
        / (_number(leg.get("ask")) or 1.0)
        > 0.05
        for leg in legs
    ):
        return {}
    return _vertical_candidate_from_evidence(
        {
            **dict(evidence),
            "long": legs[0],
            "short": legs[1],
            "source": "rth_schwab_preaverage15_pullback",
            "geometry_source": "preaverage_local_scale_first_passage",
        },
        facts,
        now=now,
        policy=lane_policy,
    )


def _rth_width_verticals(
    evidence: Mapping[str, Any],
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    frame = _map(payload.get("option_structure_frame"))
    expiry = str(frame.get("front_expiry") or _expiry_from_legs(evidence) or "")
    direction = str(evidence.get("direction") or "")
    right = "C" if direction == "UP" else "P" if direction == "DOWN" else ""
    if not expiry or right not in {"C", "P"}:
        return []
    anchors = {
        _round_to_strike(_number(evidence.get("trigger_level"))),
        _round_to_strike(_number(_map(facts.get("spot")).get("spx"))),
    }
    rows = []
    for long_strike in sorted(value for value in anchors if value is not None):
        for width in WIDTHS:
            short_strike = long_strike + width if right == "C" else long_strike - width
            if vertical_width_path_reasons(
                long_strike=long_strike,
                short_strike=short_strike,
                right=right,
                target=_number(evidence.get("target_spx")),
                remaining_expected_move=_number(
                    _map(facts.get("volatility")).get("expected_move_points")
                ),
            ):
                continue
            legs = _rth_option_legs(
                latest,
                expiry,
                ((long_strike, right), (short_strike, right)),
                now=now,
                policy=policy,
            )
            if not legs:
                continue
            long, short = legs
            provider = str(long.get("provider") or "")
            row = _vertical_candidate_from_evidence(
                {
                    **dict(evidence),
                    "long": long,
                    "short": short,
                    "source": f"rth_{provider}_width_enumeration",
                },
                facts,
                now=now,
                policy=policy,
            )
            if row:
                rows.append(row)
    return rows


def _gth_width_verticals(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    frame = _map(payload.get("option_structure_frame"))
    expiry = str(frame.get("front_expiry") or payload.get("expiry") or "")
    spot = _round_to_strike(_number(_map(facts.get("spot")).get("spx")))
    if not expiry or spot is None:
        return []
    quote_policy = _gth_quote_policy(policy)
    remaining = _number(_map(facts.get("volatility")).get("expected_move_points"))
    rows: list[dict[str, Any]] = []
    for direction, right in (("UP", "C"), ("DOWN", "P")):
        target, stop = _facts_wall_ladder_geometry(facts, direction, spot)
        for long_strike in _gth_vertical_long_strikes(
            latest,
            expiry,
            right,
            spot,
            now=now,
            policy=quote_policy,
        ):
            near_spot = abs(long_strike - spot) <= 5.0
            for width in policy.gth_widths:
                short_strike = long_strike + width if right == "C" else long_strike - width
                if near_spot:
                    if target is None or stop is None:
                        continue
                    if vertical_width_path_reasons(
                        long_strike=long_strike,
                        short_strike=short_strike,
                        right=right,
                        target=target,
                        remaining_expected_move=remaining,
                    ):
                        continue
                    setup_kind = GTH_WIDTH_SCAN
                    row_target, row_stop = target, stop
                    geometry_source = "facts_wall_ladder_fallback"
                else:
                    setup_kind = GTH_DELTA_SCAN
                    row_target, row_stop = short_strike, stop
                    geometry_source = "gth_delta_anchor"
                    if row_stop is None:
                        continue
                    if debit_vertical_reach_reasons(
                        spot=spot,
                        long_strike=long_strike,
                        short_strike=short_strike,
                        right=right,
                        remaining_expected_move=remaining,
                    ):
                        continue
                legs = _session_option_legs(
                    latest,
                    expiry,
                    ((long_strike, right), (short_strike, right)),
                    now=now,
                    policy=quote_policy,
                    providers=(Provider.IBKR, Provider.SCHWAB),
                )
                if not legs:
                    continue
                long, short = legs
                if setup_kind == GTH_DELTA_SCAN and _long_delta_above_scan_cap(
                    long, policy
                ):
                    continue
                row = _vertical_candidate_from_evidence(
                    {
                        "setup_kind": setup_kind,
                        "setup_state": "ENTRY_WINDOW_OPEN",
                        "direction": direction,
                        "trigger_level": long_strike,
                        "target_spx": row_target,
                        "invalidation_spx": row_stop,
                        "long": long,
                        "short": short,
                        "source": f"gth_{long.get('provider')}_width_enumeration",
                        "geometry_source": geometry_source,
                    },
                    facts,
                    now=now,
                    policy=quote_policy,
                )
                if row:
                    rows.append(row)
    return rows


def _gth_vertical_long_strikes(
    latest: LatestState,
    expiry: str,
    right: str,
    spot: float,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[float]:
    anchors = {
        value
        for offset in policy.gth_long_offsets
        if (value := _round_to_strike(spot + offset)) is not None
    }
    for target_delta in policy.gth_delta_targets:
        strike = nearest_abs_delta_strike(
            latest,
            expiry,
            right,
            target_abs_delta=target_delta,
            now=now,
            policy=policy,
            providers=(Provider.IBKR, Provider.SCHWAB),
            max_abs_delta=target_delta,
        )
        if strike is not None:
            anchors.add(strike)
    if right == "C":
        return sorted(value for value in anchors if value >= spot - 10.0)
    return sorted(value for value in anchors if value <= spot + 10.0)


def _vertical_candidate_from_evidence(
    evidence: Mapping[str, Any] | None,
    facts: Mapping[str, Any],
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    if not evidence:
        return {}
    long, short = _map(evidence.get("long")), _map(evidence.get("short"))
    right = str(long.get("right") or "").upper()
    strikes = (_number(long.get("strike")), _number(short.get("strike")))
    strategy_type = f"{'CALL' if right == 'C' else 'PUT'}_DEBIT_VERTICAL"
    bbo = conservative_vertical_bbo(
        long,
        short,
        now=now,
        max_quote_age_seconds=policy.quote_max_age_seconds,
        max_source_skew_seconds=policy.quote_max_skew_seconds,
    )
    economics: dict[str, Any] = {}
    if bbo.get("status") == "ready" and None not in strikes and right in {"C", "P"}:
        try:
            economics = vertical_economics(
                long_strike=float(strikes[0]),
                short_strike=float(strikes[1]),
                net_debit=float(bbo["ask"]),
                right=right,
            )
        except ValueError:
            economics = {}
    expiry = _expiry_from_contract(long.get("contract_id")) or _expiry_from_contract(short.get("contract_id"))
    candidate_id = _candidate_id(
        facts.get("session_date"),
        strategy_type,
        expiry,
        [value for value in strikes if value is not None],
        right,
    )
    quote_valid = _quote_valid_until((long, short), now=now, policy=policy)
    opportunity_valid = now + timedelta(seconds=policy.opportunity_ttl_seconds)
    if source_valid := _time(evidence.get("valid_until")):
        opportunity_valid = min(opportunity_valid, source_valid)
    identity = {
        "session_date": facts.get("session_date"),
        "candidate_id": candidate_id,
        "long_contract_id": long.get("contract_id"),
        "short_contract_id": short.get("contract_id"),
        "signal_at": evidence.get("signal_at"),
    }
    return {
        "candidate_id": candidate_id,
        "strategy_type": strategy_type,
        **{
            key: evidence.get(key)
            for key in (
                "setup_kind",
                "setup_variant",
                "setup_state",
                "direction",
                "trigger_level",
                "target_spx",
                "invalidation_spx",
                "source",
                "geometry_source",
                "signal_at",
                "evidence_contract_hash",
                "authorization_policy",
                "evidence_status",
                "local_scale_points",
                "impulse_15m_points",
                "pullback_points",
                "resume_1m_points",
                "hazard_probability",
                "hazard_probabilities",
                "hazard_features",
                "hazard_oos",
            )
        },
        "right": right,
        "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
        "long": dict(long),
        "short": dict(short),
        "quote": bbo,
        "economics": economics,
        "selection_score": _vertical_selection_score(economics, bbo),
        "quote_valid_until": quote_valid.isoformat() if quote_valid else now.isoformat(),
        "opportunity_valid_until": opportunity_valid.isoformat(),
        "automatic_ordering": False,
        "manual_action_only": True,
    }


def _momentum_clarity_block(
    direction: str,
    regime: Mapping[str, Any],
    facts: Mapping[str, Any],
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> str | None:
    """Block unclear first prints, weak same-way adds, and unconfirmed flips."""

    committed = str(facts.get("session_committed_direction") or "").upper()
    hmm_trend = hmm_owns_trend_direction(regime)
    if committed in {"UP", "DOWN"} and committed != direction:
        if hmm_trend != direction:
            return "es_volume_momentum_flip_needs_hmm_trend"
    if committed in {"UP", "DOWN"} and committed == direction:
        path = _map(facts.get("path"))
        ret5 = _number(path.get("return_5m_points"))
        atr = _number(path.get("atr_5m"))
        if hmm_trend != direction:
            return "es_volume_momentum_add_needs_new_impulse"
        if ret5 is None or atr is None or atr <= 0:
            return "es_volume_momentum_add_needs_new_impulse"
        if abs(ret5) / atr < policy.es_momentum_add_min_return_5m_atr:
            return "es_volume_momentum_add_needs_new_impulse"
    if hmm_trend is not None and hmm_trend != direction:
        return "es_volume_momentum_hmm_opposes"
    return None


def _rth_evidences(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> tuple[list[dict[str, Any]], list[str]]:
    del now
    bases: list[dict[str, Any]] = []
    reasons: list[str] = []
    setup_facts = [_map(row) for row in facts.get("rth_setups") or ()]
    momentum_setups = [
        row for row in setup_facts if str(row.get("setup_kind") or "") == "ES_VOLUME_MOMENTUM"
    ]
    preaverage_setups = [
        row
        for row in setup_facts
        if str(row.get("setup_kind") or "") == PREAVERAGE15_PULLBACK
    ]
    wall_hazard_setups = [
        row
        for row in setup_facts
        if str(row.get("setup_kind") or "") == WALL_BREAKOUT_HAZARD
    ]
    pin_blocks = pin_blocks_directional_spreads(regime)
    if pin_blocks:
        reasons.append("directional_spread_blocked_by_pin_watch")
    clarity_blocks: list[str] = []
    for setup in momentum_setups:
        if setup.get("state") != "ENTRY_WINDOW_OPEN":
            continue
        direction = _direction(setup.get("direction"))
        if not direction:
            continue
        if pin_blocks:
            continue
        clarity = _momentum_clarity_block(direction, regime, facts, policy)
        if clarity:
            clarity_blocks.append(clarity)
            continue
        bases.append(
            {
                "setup_kind": "ES_VOLUME_MOMENTUM",
                "setup_variant": setup.get("setup_variant") or "ES_PACE_1M5M",
                "setup_state": setup.get("state"),
                "direction": direction,
                "trigger_level": _number(setup.get("trigger_level")),
                "source": setup.get("source") or "es_volume_momentum",
            }
        )
    for setup in preaverage_setups:
        direction = _direction(setup.get("direction"))
        if setup.get("state") != "ENTRY_WINDOW_OPEN" or not direction or pin_blocks:
            continue
        bases.append(
            {
                **dict(setup),
                "setup_kind": PREAVERAGE15_PULLBACK,
                "setup_state": setup.get("state"),
                "direction": direction,
                "source": "rth_preaverage15_pullback",
            }
        )
    for setup in wall_hazard_setups:
        direction = _direction(setup.get("direction"))
        if setup.get("state") != "ENTRY_WINDOW_OPEN" or not direction or pin_blocks:
            continue
        bases.append(
            {
                **dict(setup),
                "setup_kind": WALL_BREAKOUT_HAZARD,
                "setup_state": setup.get("state"),
                "direction": direction,
                "source": "rth_wall_breakout_hazard",
            }
        )
    if not bases and not pin_blocks:
        if clarity_blocks:
            reasons.append(clarity_blocks[0])
        elif not momentum_setups and not wall_hazard_setups:
            reasons.append("es_volume_momentum_unavailable")
        else:
            blocked = [
                str(row.get("blocked_by") or row.get("reason") or "")
                for row in momentum_setups
            ]
            states = {str(row.get("state") or "") for row in momentum_setups}
            if "es_volume_momentum_too_late" in blocked or "ENTRY_TOO_LATE" in states:
                reasons.append("es_volume_momentum_too_late")
            elif any(code in blocked for code in (
                "es_volume_not_elevated",
                "es_volume_momentum_direction_flat",
                "es_volume_momentum_not_aligned",
                "es_volume_momentum_too_weak",
                "es_volume_momentum_unevaluable",
                "es_volume_unavailable",
            )):
                reasons.append(next(
                    code
                    for code in blocked
                    if code
                ))
            else:
                reasons.append("es_volume_momentum_unavailable")
    evidences: list[dict[str, Any]] = []
    for base in bases:
        direction = str(base["direction"])
        trigger_level = _number(base.get("trigger_level"))
        target = _number(base.get("target_spx"))
        stop = _number(base.get("invalidation_spx"))
        geometry_source = base.get("geometry_source")
        if target is None or stop is None:
            target, stop, geometry_source = resolve_geometry(
                payload, facts, direction, trigger_level
            )
        if target is None or stop is None:
            reasons.append("vertical_target_or_invalidation_unavailable")
            continue
        evidence = {
            **base,
            "target_spx": target,
            "invalidation_spx": stop,
            "geometry_source": geometry_source,
        }
        if evidence.get("setup_kind") == WALL_BREAKOUT_HAZARD:
            evidences.append(evidence)
            continue
        spread_source = (
            "call_skew_spread_shadow" if direction == "UP" else "put_skew_spread_shadow"
        )
        shadow = _map(payload.get(spread_source))
        spread = _map(shadow.get("candidate"))
        if shadow.get("status") != "candidate" or not spread:
            spread = _intent_spread(payload.get("trade_intent"), latest)
            spread_source = "legacy_trade_intent_trigger_only"
        if spread:
            expected_right = "C" if direction == "UP" else "P"
            if any(
                str(_map(spread.get(key)).get("right") or "").upper()
                != expected_right
                for key in ("long", "short")
            ):
                spread = {}
        if not spread:
            spread = _confirmed_trigger_spread(facts, direction)
            spread_source = "rth_confirmed_trigger_exact_spread_snapshot"
        if spread:
            evidence.update(
                {
                    "long": _map(spread.get("long")),
                    "short": _map(spread.get("short")),
                    "spread_source": spread_source,
                }
            )
            if evidence.get("source") == "confirmed_level_decision":
                evidence["source"] = spread_source
        evidences.append(evidence)
    return evidences, list(dict.fromkeys(reasons))


def _gth_evidence(facts: Mapping[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    reasons: list[str] = []
    sources = (
        (
            "gth_level_manual_candidate",
            _map(facts.get("gth_evidence")),
            "gth_confirmed_level_candidate_unavailable",
        ),
        (
            "gth_dip_reclaim_evidence",
            _map(facts.get("gth_dip_reclaim_evidence")),
            "gth_dip_reclaim_evidence_unavailable",
        ),
    )
    for source, evidence, unavailable_reason in sources:
        evidence_reasons = list(map(str, evidence.get("block_reasons") or ()))
        eligible = (
            evidence.get("manual_action_eligible") is True
            or evidence.get("selector_evidence_eligible") is True
        )
        if evidence.get("status") not in {"manual_ready", "selector_candidate"} or not eligible:
            live_reasons = [
                reason
                for reason in evidence_reasons
                if reason not in _EXPIRED_GTH_REASONS
            ]
            if live_reasons:
                reasons.extend(live_reasons)
            else:
                # An expired leftover is not a live setup. Keep expiry codes
                # for audit, but do not let them starve the whole GTH session
                # as the desk primary reason.
                reasons.append(unavailable_reason)
                reasons.extend(evidence_reasons)
            continue
        path_kind = str(evidence.get("path_kind") or "")
        if path_kind.startswith("trend_transition_"):
            reasons.append("trend_background_cannot_authorize_entry")
            continue
        direction = _direction(evidence.get("direction"))
        if not direction:
            reasons.append("gth_candidate_direction_unavailable")
            continue
        target = _number(evidence.get("target_spx"))
        stop = _number(evidence.get("invalidation_spx"))
        if target is None or stop is None:
            reasons.extend(
                ["gth_spx_target_or_invalidation_unavailable", *evidence_reasons]
            )
            continue
        snapshot = _map(evidence.get("exact_spread_snapshot"))
        setup = (
            "FAILED_BREAK_RECLAIM"
            if any(token in path_kind for token in ("rejection", "reclaim", "dip"))
            else "TREND_PULLBACK"
        )
        return {
            "setup_kind": setup,
            "direction": direction,
            "trigger_level": _number(evidence.get("trigger_level")),
            "target_spx": target,
            "invalidation_spx": stop,
            "long": _gth_leg(snapshot.get("long"), evidence.get("long_contract_id")),
            "short": _gth_leg(snapshot.get("short"), evidence.get("short_contract_id")),
            "valid_until": evidence.get("valid_until"),
            "source": source,
            "geometry_source": source,
            "source_block_reasons": evidence_reasons,
            "edge_authority": evidence.get("edge_authority"),
            "edge_authority_reason": evidence.get("edge_authority_reason"),
        }, []
    return None, list(dict.fromkeys(reasons))


def _butterfly_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        return []
    if not DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return []
    if _capability_reasons(facts, "butterfly"):
        return []
    frame = _map(payload.get("option_structure_frame"))
    expiry = str(frame.get("front_expiry") or "")
    if not expiry:
        return []
    rows: list[dict[str, Any]] = []
    if regime.get("terminal_state") == "PIN_STABLE":
        pin = _map(regime.get("pin"))
        for ranked in pin.get("top_centers") or ():
            center = _number(_map(ranked).get("center"))
            if center is None:
                continue
            mass = _map(_map(facts.get("structure")).get("q_local_mass_5pt"))
            for width in pin_look_trade_widths(
                facts.get("minutes_to_close"), center, mass, policy
            ):
                for right in ("C", "P"):
                    row = _butterfly_candidate(
                        facts,
                        latest,
                        expiry,
                        center=center,
                        width=width,
                        right=right,
                        now=now,
                        policy=policy,
                        source="stable_pin_butterfly",
                        setup_kind="STABLE_PIN",
                        direction="NEUTRAL",
                        thesis_direction="NEUTRAL",
                        payoff_shape="PIN_CONCENTRATED",
                        manual_authority_eligible=True,
                        selection_prior=float(_map(ranked).get("score") or 0.0),
                        pin=pin,
                        geometry_source=None,
                    )
                    if row:
                        rows.append(row)
    trigger = _map(facts.get("trigger"))
    direction = _direction(trigger.get("direction"))
    if trigger.get("phase") == "confirmed" and direction:
        target, stop, geometry_source = resolve_geometry(payload, facts, direction, _number(trigger.get("level")))
        center = _round_to_strike(target)
        if center is not None and stop is not None:
            for width in WIDTHS:
                for right in ("C", "P"):
                    row = _butterfly_candidate(
                        facts,
                        latest,
                        expiry,
                        center=center,
                        width=width,
                        right=right,
                        now=now,
                        policy=policy,
                        source="directional_confirmation_butterfly",
                        setup_kind="CONFIRMATION_TARGET_PIN",
                        direction="NEUTRAL",
                        thesis_direction=direction,
                        payoff_shape="TARGET_CONCENTRATED",
                        # Research alternative only: butterfly hard gates do not yet
                        # cover anti-chase / ATR band / center-migration checks, so a
                        # directional thesis must not gain manual authority here.
                        manual_authority_eligible=False,
                        selection_prior=0.0,
                        pin={},
                        geometry_source=geometry_source,
                        target_spx=center,
                        invalidation_spx=stop,
                    )
                    if row:
                        rows.append(row)
    return rows


def _butterfly_candidate(
    facts: Mapping[str, Any],
    latest: LatestState,
    expiry: str,
    *,
    center: float,
    width: float,
    right: str,
    now: datetime,
    policy: StrategyPolicy,
    source: str,
    setup_kind: str,
    direction: str,
    thesis_direction: str,
    payoff_shape: str,
    manual_authority_eligible: bool,
    selection_prior: float,
    pin: Mapping[str, Any],
    geometry_source: str | None,
    target_spx: float | None = None,
    invalidation_spx: float | list[float] | None = None,
    providers: Sequence[Provider] = (Provider.SCHWAB, Provider.IBKR),
) -> dict[str, Any]:
    legs = _session_option_legs(
        latest,
        expiry,
        tuple((strike, right) for strike in (center - width, center, center + width)),
        now=now,
        policy=policy,
        providers=providers,
    )
    if not legs:
        return {}
    quote = conservative_butterfly_bbo(
        *legs,
        now=now,
        max_quote_age_seconds=policy.quote_max_age_seconds,
        max_source_skew_seconds=policy.quote_max_skew_seconds,
    )
    economics: dict[str, Any] = {}
    if quote.get("status") == "ready":
        try:
            economics = butterfly_economics(center=center, width=width, net_debit=float(quote["ask"]))
        except ValueError:
            economics = {}
    strategy_type = f"{'CALL' if right == 'C' else 'PUT'}_BUTTERFLY"
    strikes = [center - width, center, center + width]
    candidate_id = _candidate_id(facts.get("session_date"), strategy_type, expiry, strikes, right)
    quote_valid = _quote_valid_until(legs, now=now, policy=policy)
    identity = (facts.get("session_date"), candidate_id, *(leg["contract_id"] for leg in legs))
    score = selection_prior + _butterfly_selection_score(economics, quote, width)
    return {
        "candidate_id": candidate_id,
        "strategy_type": strategy_type,
        "setup_kind": setup_kind,
        "direction": direction,
        "thesis_direction": thesis_direction,
        "payoff_shape": payoff_shape,
        "manual_authority_eligible": manual_authority_eligible,
        "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
        "target_spx": target_spx if target_spx is not None else center,
        "invalidation_spx": invalidation_spx if invalidation_spx is not None else [center - width, center + width],
        "center": center,
        "width": width,
        "right": right,
        "legs": legs,
        "quote": quote,
        "economics": economics,
        "selection_score": round(score, 4),
        "pin": dict(pin),
        "quote_valid_until": quote_valid.isoformat() if quote_valid else now.isoformat(),
        "opportunity_valid_until": (now + timedelta(seconds=policy.opportunity_ttl_seconds)).isoformat(),
        "source": source,
        "geometry_source": geometry_source,
        "automatic_ordering": False,
        "manual_action_only": True,
    }


def _intent_spread(value: object, latest: LatestState) -> Mapping[str, Any]:
    intent = _map(value)
    if intent.get("status") != "trade_ready":
        return {}
    parts = str(intent.get("contract_id") or "").split(":")
    if len(parts) < 6 or parts[-1] not in {"C", "P"}:
        return {}
    try:
        strike = float(parts[-2])
    except ValueError:
        return {}
    expiry, right = parts[-3], parts[-1]
    short_strike = strike + 10.0 if right == "C" else strike - 10.0
    long = _option_leg(latest, expiry, strike, right)
    short = _option_leg(latest, expiry, short_strike, right)
    return {"long": long, "short": short} if long and short else {}


def _confirmed_trigger_spread(facts: Mapping[str, Any], direction: str) -> Mapping[str, Any]:
    evidence = _map(facts.get("gth_evidence"))
    if _direction(evidence.get("direction")) != direction:
        return {}
    snapshot = _map(evidence.get("exact_spread_snapshot"))
    long = _gth_leg(snapshot.get("long"), evidence.get("long_contract_id"))
    short = _gth_leg(snapshot.get("short"), evidence.get("short_contract_id"))
    expected_right = "C" if direction == "UP" else "P"
    if any(
        not str(leg.get("contract_id") or "").startswith("option:SPX:SPXW:")
        or leg.get("right") != expected_right
        for leg in (long, short)
    ):
        return {}
    return {"long": long, "short": short}


def _facts_wall_ladder_geometry(
    facts: Mapping[str, Any], direction: str | None, trigger: float | None
) -> tuple[float | None, float | None]:
    spot, structure = _number(_map(facts.get("spot")).get("spx")), _map(facts.get("structure"))
    if spot is None or direction not in {"UP", "DOWN"}:
        return None, None
    raw_target = _number(
        structure.get("call_wall" if direction == "UP" else "put_wall")
    )
    target = (
        raw_target
        if raw_target is not None
        and ((direction == "UP" and raw_target > spot) or (direction == "DOWN" and raw_target < spot))
        else None
    )
    levels = [
        _number(structure.get("put_wall")),
        *_flip_values(structure.get("flip_zone")),
        _number(structure.get("zero_gamma")),
        _number(structure.get("call_wall")),
        trigger,
    ]
    if direction == "UP":
        stop = max((value for value in levels if value is not None and value < spot), default=None)
    else:
        stop = min((value for value in levels if value is not None and value > spot), default=None)
    return target, stop


def _option_leg(
    latest: LatestState,
    expiry: str,
    strike: float,
    right: str,
    *,
    provider: Provider | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    contract_id = InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=strike,
        right=right,
        trading_class="SPXW",
    ).canonical_id
    quote = (
        provider_quote(latest, contract_id, provider=provider, now=now)
        if provider is not None and now is not None
        else latest.best_quote(contract_id)
    )
    if quote is None:
        return {}
    delta = None
    implied_vol = None
    if quote.greeks is not None:
        delta = quote.greeks.delta
        implied_vol = quote.greeks.implied_vol
    return {
        "contract_id": contract_id,
        "strike": strike,
        "right": right,
        "provider": quote.provider.value,
        "bid": quote.bid,
        "ask": quote.ask,
        "delta": delta,
        "implied_vol": implied_vol,
        "source_at": quote_source_at(quote).isoformat(),
    }


def _rth_option_legs(
    latest: LatestState,
    expiry: str,
    contracts: tuple[tuple[float, str], ...],
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    return _session_option_legs(
        latest,
        expiry,
        contracts,
        now=now,
        policy=policy,
        providers=(Provider.SCHWAB, Provider.IBKR),
    )


def _session_option_legs(
    latest: LatestState,
    expiry: str,
    contracts: tuple[tuple[float, str], ...],
    *,
    now: datetime,
    policy: StrategyPolicy,
    providers: Sequence[Provider],
) -> list[dict[str, Any]]:
    for provider in providers:
        legs = [
            _option_leg(
                latest,
                expiry,
                strike,
                right,
                provider=provider,
                now=now,
            )
            for strike, right in contracts
        ]
        times = [_time(leg.get("source_at")) for leg in legs]
        if (
            all(legs)
            and all(_number(leg.get("bid")) is not None for leg in legs)
            and all(_number(leg.get("ask")) is not None for leg in legs)
            and all(source_at is not None for source_at in times)
            and all(
                0.0 <= (now - source_at).total_seconds() <= policy.quote_max_age_seconds
                for source_at in times
                if source_at is not None
            )
            and (
                max(source_at for source_at in times if source_at is not None)
                - min(source_at for source_at in times if source_at is not None)
            ).total_seconds()
            <= policy.quote_max_skew_seconds
        ):
            return legs
    return []


def nearest_abs_delta_strike(
    latest: LatestState,
    expiry: str,
    right: str,
    *,
    target_abs_delta: float,
    now: datetime,
    policy: StrategyPolicy,
    providers: Sequence[Provider],
    max_distance: float = 0.08,
    min_abs_delta: float | None = None,
    max_abs_delta: float | None = None,
    max_greeks_age_seconds: float | None = None,
) -> float | None:
    """Return the strike whose |delta| is closest to target among fresh quotes.

    When ``max_abs_delta`` is set, richer strikes above that cap are ignored so
    a 20Δ target means 20Δ or the next strike below it, never 21–25Δ.
    """

    wanted = str(right or "").upper()
    floor = 0.0 if min_abs_delta is None else float(min_abs_delta)
    ceiling = None if max_abs_delta is None else float(max_abs_delta)
    for provider in providers:
        best_strike: float | None = None
        best_distance: float | None = None
        for quote in latest.quotes:
            instrument = quote.instrument
            if (
                quote.provider is not provider
                or instrument.expiry != expiry
                or str(getattr(instrument.right, "value", instrument.right) or "").upper()
                != wanted
            ):
                continue
            source_at = quote_source_at(quote)
            if source_at is None:
                continue
            age = (now - source_at).total_seconds()
            if age < 0.0 or age > policy.quote_max_age_seconds:
                continue
            delta = usable_delta(quote)
            if delta is None:
                continue
            if max_greeks_age_seconds is not None:
                raw = _map(quote.raw)
                greeks_at = _time(raw.get("greeks_observed_at")) or source_at
                greeks_provider = str(raw.get("greeks_provider") or provider.value)
                greeks_age = (now - greeks_at).total_seconds()
                if (
                    greeks_provider != provider.value
                    or greeks_age < 0.0
                    or greeks_age > max_greeks_age_seconds
                ):
                    continue
            abs_delta = abs(delta)
            if abs_delta < floor:
                continue
            if ceiling is not None and abs_delta > ceiling:
                continue
            distance = abs(abs_delta - target_abs_delta)
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_strike = _round_to_strike(instrument.strike)
        if best_strike is not None and best_distance is not None and best_distance <= max_distance:
            return best_strike
    return None


def _long_delta_above_scan_cap(long: Mapping[str, Any], policy: StrategyPolicy) -> bool:
    cap = max(policy.gth_delta_targets) if policy.gth_delta_targets else 0.20
    delta = _number(long.get("delta"))
    return delta is None or abs(delta) > cap


def _gth_quote_policy(policy: StrategyPolicy) -> StrategyPolicy:
    return replace(
        policy,
        quote_max_age_seconds=policy.gth_quote_max_age_seconds,
        quote_max_skew_seconds=policy.gth_quote_max_skew_seconds,
    )


def _gth_leg(value: object, contract_id: object) -> dict[str, Any]:
    leg, parts = dict(_map(value)), str(contract_id or "").split(":")
    if len(parts) >= 2:
        try:
            leg["strike"] = float(parts[-2])
        except ValueError:
            pass
        leg["right"] = parts[-1].upper()
    leg["contract_id"] = contract_id
    return leg


def _quote_valid_until(
    legs: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]],
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> datetime | None:
    times = [_time(leg.get("source_at")) for leg in legs]
    if any(value is None for value in times):
        return None
    return min(value for value in times if value) + timedelta(seconds=policy.quote_max_age_seconds)


def _vertical_selection_score(economics: Mapping[str, Any], quote: Mapping[str, Any]) -> float:
    loss = _number(economics.get("max_loss_points"))
    gain = _number(economics.get("max_gain_points"))
    if loss is None or gain is None or loss <= 0.0:
        return 0.0
    spread = abs(float(quote.get("ask", 0.0)) - float(quote.get("bid", 0.0)))
    return round(gain / loss - 0.05 * spread / loss, 4)


def _butterfly_selection_score(
    economics: Mapping[str, Any], quote: Mapping[str, Any], width: float
) -> float:
    loss = _number(economics.get("max_loss_points"))
    gain = _number(economics.get("max_gain_points"))
    if loss is None or gain is None or loss <= 0.0:
        return 0.0
    spread = abs(float(quote.get("ask", 0.0)) - float(quote.get("bid", 0.0)))
    return min(gain / loss, 3.0) * 0.05 - 0.01 * width / 5.0 - 0.02 * spread / loss


def _candidate_id(
    session_date: object,
    strategy_type: str,
    expiry: str | None,
    strikes: list[float],
    right: str,
) -> str:
    return _hash((session_date, strategy_type, expiry, [round(float(value), 4) for value in strikes], right))[:16]


def _expiry_from_legs(evidence: Mapping[str, Any]) -> str | None:
    long = _map(evidence.get("long"))
    short = _map(evidence.get("short"))
    return _expiry_from_contract(long.get("contract_id")) or _expiry_from_contract(short.get("contract_id"))


def _expiry_from_contract(value: object) -> str | None:
    parts = str(value or "").split(":")
    return parts[-3] if len(parts) >= 6 else None


def _round_to_strike(value: float | None) -> float | None:
    return round(float(value) / 5.0) * 5.0 if value is not None else None


def _flip_values(value: object) -> list[float | None]:
    if isinstance(value, (list, tuple)):
        return [_number(item) for item in value[:2]]
    mapped = _map(value)
    return [_number(mapped.get("low")), _number(mapped.get("high"))] if mapped else []


def _direction(value: object) -> str | None:
    normalized = str(value or "").upper()
    return normalized if normalized in {"UP", "DOWN"} else None


def _capability_reasons(facts: Mapping[str, Any], strategy: str) -> list[str]:
    capability = _map(_map(facts.get("capabilities")).get(strategy))
    if not capability or capability.get("ready") is True:
        return []
    return list(map(str, capability.get("reasons") or (f"{strategy}_capability_unavailable",)))


def _hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, (str, datetime)):
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("strategy decision time must be timezone-aware")
    return value.astimezone(timezone.utc)
