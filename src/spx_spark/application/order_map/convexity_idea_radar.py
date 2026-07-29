"""Two-sided, non-executable context for discretionary 0DTE convexity ideas.

The radar does not decide direction and does not claim that an option is
mispriced.  It packages the deterministic facts a human or LLM needs to ask a
better question: which market-priced assumption would have to be wrong for a
Call or Put expression to have asymmetric value before the 13:00 ET exit?
"""

from __future__ import annotations

from datetime import datetime, time, timezone
from math import ceil
from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.convexity_idea_inputs import (
    build_volatility_context,
)
from spx_spark.application.order_map.convexity_path_modifier import select_rolling_path_modifier
from spx_spark.application.order_map.convexity_opportunity_board import (
    build_dense_opportunity_board,
)
from spx_spark.application.order_map.convexity_idea_quality import (
    build_quality_summary,
    build_wall_probability_context,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET


SCHEMA_VERSION = "convexity_idea_radar.v1"
ANALYSIS_START_ET = time(9, 45)
HARD_EXIT_ET = time(13, 0)
_LEVEL_KEYS = ("put_wall", "flip_low", "flip_high", "call_wall")


def attach_convexity_idea_radar(
    payload: dict[str, Any],
    *,
    now: datetime,
) -> None:
    """Attach one compact research packet after all other projections exist."""

    payload["convexity_idea_radar"] = build_convexity_idea_radar(payload, now=now)


def build_convexity_idea_radar(
    payload: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    mandate = _mandate(now)
    levels = _level_map(payload)
    spot, spot_source = _spot(payload)
    lower_test = _boundary(levels, side="lower", spot=spot)
    upper_test = _boundary(levels, side="upper", spot=spot)
    destination = _destination_map(payload, now=now, mandate=mandate)
    market_state = _market_state(payload, levels=levels, now=now)
    wall_probabilities = build_wall_probability_context(
        payload,
        mandate=mandate,
        now=now,
    )
    call_evidence = _option_evidence(payload, right="C", level=_number(lower_test.get("level")))
    put_evidence = _option_evidence(payload, right="P", level=_number(upper_test.get("level")))
    lower_put_evidence = _option_evidence(
        payload,
        right="P",
        level=_number(lower_test.get("level")),
    )
    upper_call_evidence = _option_evidence(
        payload,
        right="C",
        level=_number(upper_test.get("level")),
    )
    quality = build_quality_summary(
        payload,
        destination=destination,
        mandate=mandate,
    )

    if mandate["phase"] == "hard_exit_reached":
        status = "closed"
    elif mandate["phase"] in {"closed_gap", "outside_strategy_window"}:
        status = "inactive"
    elif mandate["phase"] == "rth_warmup":
        status = "warming"
    elif not levels:
        status = "unavailable"
    elif (
        quality["status"] == "degraded"
        or destination["status"] == "unavailable"
        or wall_probabilities["status"] != "ready"
    ):
        status = "partial"
    elif mandate["phase"] == "gth_preparation":
        status = "preparation"
    else:
        status = "ready"

    hypotheses = [
        _hypothesis(
            scenario="lower_rejection_call",
            boundary=lower_test,
            right="C",
            direction="up",
            required_path="REJECTED→RETEST→CONFIRMED",
            falsifier="lower_boundary_accepted_below",
            option_evidence=call_evidence,
            idea_generation_allowed=bool(mandate["new_idea_generation_allowed"]),
        ),
        _hypothesis(
            scenario="lower_acceptance_put",
            boundary=lower_test,
            right="P",
            direction="down",
            required_path="ACCEPTED→RETEST→CONFIRMED",
            falsifier="lower_boundary_reclaimed",
            option_evidence=lower_put_evidence,
            idea_generation_allowed=bool(mandate["new_idea_generation_allowed"]),
        ),
        _hypothesis(
            scenario="upper_rejection_put",
            boundary=upper_test,
            right="P",
            direction="down",
            required_path="REJECTED→RETEST→CONFIRMED",
            falsifier="upper_boundary_accepted_above",
            option_evidence=put_evidence,
            idea_generation_allowed=bool(mandate["new_idea_generation_allowed"]),
        ),
        _hypothesis(
            scenario="upper_acceptance_call",
            boundary=upper_test,
            right="C",
            direction="up",
            required_path="ACCEPTED→RETEST→CONFIRMED",
            falsifier="upper_boundary_lost",
            option_evidence=upper_call_evidence,
            idea_generation_allowed=bool(mandate["new_idea_generation_allowed"]),
        ),
    ]
    volatility_context = build_volatility_context(
        payload,
        market_state=market_state,
    )
    boundary_tests = {
        "lower": lower_test,
        "upper": upper_test,
        "active_event": _active_event(payload),
        "risk_neutral_wall_probabilities": wall_probabilities,
    }
    opportunity_board = build_dense_opportunity_board(
        mandate=mandate,
        market_state=market_state,
        boundary_tests=boundary_tests,
        option_evidence={
            "call": _preferred_evidence(call_evidence, upper_call_evidence),
            "put": _preferred_evidence(put_evidence, lower_put_evidence),
        },
        volatility_context=volatility_context,
        data_quality=quality,
        hypotheses=hypotheses,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "as_of": _utc(now).isoformat(),
        "status": status,
        "mode": "discretionary_convexity_decision_support",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
        "mandate": mandate,
        "spot": {"price": _rounded(spot), "source": spot_source},
        "gth_prior": {
            "status": (
                "live_context_not_frozen"
                if mandate["phase"] == "gth_preparation"
                else "unavailable"
            ),
            "frozen_at": None,
            "reason": (
                "point_in_time_0925_gth_prior_not_yet_persisted"
                if mandate["phase"] != "gth_preparation"
                else "current_gth_context_may_still_change_before_rth"
            ),
        },
        "destination_map": destination,
        "market_state": market_state,
        "volatility_context": volatility_context,
        "levels": {key: _rounded(value) for key, value in levels.items()},
        "boundary_tests": boundary_tests,
        "option_evidence": {
            "call": call_evidence,
            "put": put_evidence,
        },
        "hypotheses": hypotheses,
        "opportunity_board": opportunity_board,
        "tensions": _tensions(payload, market_state=market_state),
        "data_quality": quality,
        "semantics": {
            "destination_distribution": (
                "option_implied_risk_neutral_terminal_distribution_not_physical_forecast"
            ),
            "mispricing": (
                "only_observed_local_skew_edge_may_be_named_evidence_never_proven_mispricing"
            ),
            "llm_role": (
                "rank_and_criticize_hypotheses_using_supplied_facts_never_invent_prices_or_flow"
            ),
            "moving_average_regime": (
                "read_only_confluence_never_direction_or_entry_authority;"
                "cross_alone_cannot_generate_call_or_put"
            ),
        },
    }


def _mandate(now: datetime) -> dict[str, Any]:
    at = _utc(now)
    local = at.astimezone(ET)
    trading_date = DEFAULT_MARKET_CALENDAR.spx_session_date_for(
        at,
        retain_completed=False,
    )
    in_session_window = trading_date is not None
    target_date = trading_date or DEFAULT_MARKET_CALENDAR.research_expiry(at)
    session = DEFAULT_MARKET_CALENDAR.session(target_date)
    if session is None:
        return {
            "instrument": "SPXW_0DTE_long_convexity",
            "sides": ["call", "put"],
            "analysis_start_et": "09:45",
            "hard_exit_et": "13:00",
            "hard_exit_at": None,
            "minutes_to_hard_exit": None,
            "phase": "calendar_unavailable",
            "new_idea_generation_allowed": False,
        }
    window = DEFAULT_MARKET_CALENDAR.spx_session_window(session.trading_date)
    segment = window.segment_at(local) if window is not None and in_session_window else None
    hard_exit = datetime.combine(session.trading_date, HARD_EXIT_ET, tzinfo=ET)
    hard_exit = min(hard_exit, session.close_at)
    analysis_start = datetime.combine(
        session.trading_date,
        ANALYSIS_START_ET,
        tzinfo=ET,
    )
    analysis_start = min(analysis_start, hard_exit)
    if segment == "gth":
        phase = "gth_preparation"
    elif segment == "closed_gap":
        phase = "closed_gap"
    elif analysis_start <= local < hard_exit:
        phase = "rth_active"
    elif session.open_at <= local < analysis_start:
        phase = "rth_warmup"
    elif segment == "rth" and local >= hard_exit:
        phase = "hard_exit_reached"
    else:
        phase = "outside_strategy_window"
    remaining = (
        max(ceil((hard_exit - local).total_seconds() / 60.0), 0)
        if phase
        in {
            "gth_preparation",
            "closed_gap",
            "rth_warmup",
            "rth_active",
            "hard_exit_reached",
        }
        else None
    )
    return {
        "instrument": "SPXW_0DTE_long_convexity",
        "sides": ["call", "put"],
        "trading_date": session.trading_date.isoformat(),
        "session_segment": segment,
        "analysis_start_et": "09:45",
        "analysis_start_at": analysis_start.isoformat(),
        "hard_exit_et": "13:00",
        "hard_exit_at": hard_exit.isoformat(),
        "terminal_time_et": session.close_at.strftime("%H:%M"),
        "terminal_at": session.close_at.isoformat(),
        "minutes_to_hard_exit": remaining,
        "horizon_to_1300_min": remaining,
        "phase": phase,
        "new_idea_generation_allowed": phase in {"gth_preparation", "rth_active"},
        "position_must_be_flat": phase == "hard_exit_reached",
        "automatic_ordering": False,
    }


def _destination_map(
    payload: Mapping[str, Any],
    *,
    now: datetime,
    mandate: Mapping[str, Any],
) -> dict[str, Any]:
    density = payload.get("rn_density")
    source = "order_map_front_0dte_rn_density"
    source_expiry = str(payload.get("expiry") or "")
    density_expiry = ""
    source_as_of = payload.get("as_of")
    expected_move = _number(payload.get("expected_move_points"))
    if not isinstance(density, Mapping):
        frame = _mapping(payload.get("option_structure_frame"))
        density = frame.get("density")
        source = "option_structure_frame_density"
        source_expiry = str(frame.get("front_expiry") or "")
        source_as_of = frame.get("as_of")
        volatility = _mapping(frame.get("volatility"))
        expected_move = _number(volatility.get("expected_move_points_0dte"))
    row = _mapping(density)
    density_expiry = str(row.get("expiry") or "")
    source_as_of = row.get("as_of") or source_as_of
    quality = str(row.get("quality") or "unavailable")
    p10 = _number(row.get("p10"))
    median = _number(row.get("median"))
    p90 = _number(row.get("p90"))
    expected_expiry = str(mandate.get("trading_date") or "").replace("-", "")
    payload_expiry = str(payload.get("expiry") or "")
    observed_at = _datetime(source_as_of)
    age_seconds = (
        (_utc(now) - observed_at).total_seconds() if observed_at is not None else None
    )
    phase = str(mandate.get("phase") or "")
    max_age_seconds = 90.0 if phase == "gth_preparation" else 15.0
    gate_reasons: list[str] = []
    if phase not in {"gth_preparation", "rth_warmup", "rth_active"}:
        gate_reasons.append("strategy_session_inactive")
    if not expected_expiry or source_expiry != expected_expiry:
        gate_reasons.append("density_expiry_mismatch")
    if density_expiry and density_expiry != source_expiry:
        gate_reasons.append("density_payload_expiry_identity_mismatch")
    if payload_expiry != expected_expiry:
        gate_reasons.append("payload_expiry_mismatch")
    if observed_at is None:
        gate_reasons.append("density_as_of_missing")
    elif age_seconds is not None and (
        age_seconds < -2.0 or age_seconds > max_age_seconds
    ):
        gate_reasons.append("density_stale_or_future")
    complete = all(value is not None for value in (p10, median, p90))
    if not complete:
        gate_reasons.append("density_percentiles_incomplete")
    identity_and_freshness_ok = not gate_reasons
    if identity_and_freshness_ok:
        status = "ready" if quality in {"ok", "ready", "live"} else "degraded"
    else:
        status = "unavailable"
        p10 = median = p90 = None
        expected_move = None
    p25 = _number(row.get("p25")) if identity_and_freshness_ok else None
    p75 = _number(row.get("p75")) if identity_and_freshness_ok else None
    prob_below = (
        _number(row.get("prob_below_put_wall"))
        if identity_and_freshness_ok
        else None
    )
    prob_above = (
        _number(row.get("prob_above_call_wall"))
        if identity_and_freshness_ok
        else None
    )
    day_move = _mapping(payload.get("day_move"))
    terminal_time = str(mandate.get("terminal_time_et") or "")
    return {
        "status": status,
        "source": source,
        "expiry": source_expiry or None,
        "expected_expiry": expected_expiry or None,
        "as_of": observed_at.isoformat() if observed_at is not None else None,
        "age_seconds": _rounded(age_seconds),
        "maximum_age_seconds": max_age_seconds,
        "gate_reasons": gate_reasons,
        "quality": quality,
        "p10": _rounded(p10),
        "p25": _rounded(p25),
        "median": _rounded(median),
        "p75": _rounded(p75),
        "p90": _rounded(p90),
        "prob_below_put_wall": _rounded(prob_below, 4),
        "prob_above_call_wall": _rounded(prob_above, 4),
        "expected_move_points_to_settlement": _rounded(expected_move),
        "gth_expected_move_used_fraction": _rounded(
            _number(day_move.get("em_used_fraction")), 4
        ),
        "probability_semantics": "risk_neutral_terminal_not_physical",
        "terminal_time_et": terminal_time or None,
        "strategy_exit_time_et": "13:00",
        "mark_1300_proxy": {
            "status": "unavailable",
            "reason": (
                "requires_causal_intraday_variance_scaling_and_option_repricing;"
                "do_not_relabel_1600_terminal_density"
            ),
        },
    }


def _market_state(
    payload: Mapping[str, Any],
    *,
    levels: Mapping[str, float],
    now: datetime,
) -> dict[str, Any]:
    shadow = _mapping(payload.get("spring_gamma_v3_shadow"))
    state = _mapping(shadow.get("rth_market_state"))
    quality = _mapping(state.get("Q"))
    volatility = _mapping(state.get("V"))
    availability = _mapping(state.get("input_availability"))
    direction = _mapping(shadow.get("direction"))
    lineage = _mapping(state.get("input_lineage"))
    diagnostics = _mapping(lineage.get("diagnostics"))
    same_time = _mapping(diagnostics.get("same_time_range"))
    current_path = _mapping(diagnostics.get("rolling_path_percentiles"))
    fallback = _mapping(payload.get("spring_gamma_v3_path_fallback"))
    rolling_path = select_rolling_path_modifier(
        current=current_path,
        fallback=fallback,
        shadow=shadow,
        now=now,
    )
    return {
        "state": state.get("state"),
        "status": state.get("status"),
        "D": _number(state.get("D")),
        "efficiency_ratio": _number(quality.get("efficiency_ratio")),
        "vwap_cross_count": quality.get("vwap_cross_count"),
        "same_time_range_ratio": _number(volatility.get("same_time_range_ratio")),
        "breadth_above_vwap": _market_breadth(state),
        "direction_components": state.get("direction_components"),
        "input_available_count": availability.get("available_count"),
        "input_required_count": availability.get("required_count"),
        "current_range_points": _number(same_time.get("current_range_points")),
        "same_time_median_range_points": _number(same_time.get("median_range_points")),
        "rolling_path_percentiles": rolling_path,
        "uncalibrated_direction": {
            "decision": direction.get("decision"),
            "diagnostic_es_direction": direction.get("diagnostic_es_direction"),
            "p_up": _rounded(_number(direction.get("p_up")), 4),
            "p_down": _rounded(_number(direction.get("p_down")), 4),
            "composite_score": _rounded(_number(direction.get("composite_score")), 4),
            "calibration_status": direction.get("calibration_status")
            or shadow.get("calibration_status"),
        },
        "moving_averages": _moving_average_context(
            state,
            payload,
            levels=levels,
        ),
        "action_authority": "none",
    }


def _preferred_evidence(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
) -> Mapping[str, Any]:
    rank = {
        "observed_local_skew_edge": 2,
        "not_observed": 1,
        "unknown": 0,
    }
    return max(
        (first, second),
        key=lambda row: rank.get(str(row.get("edge_status") or "unknown"), 0),
    )


def _level_map(payload: Mapping[str, Any]) -> dict[str, float]:
    decision = _mapping(payload.get("level_decision"))
    sources = [
        _mapping(decision.get("levels")),
        _mapping(_mapping(_mapping(payload.get("spring_gamma_v3_shadow")).get(
            "wall_probability"
        )).get("stable_levels")),
        _mapping(_mapping(payload.get("option_structure_frame")).get("structure")),
    ]
    result: dict[str, float] = {}
    for source in sources:
        for key in _LEVEL_KEYS:
            value = _number(source.get(key))
            if value is not None and key not in result:
                result[key] = value
        flip = source.get("flip_zone")
        if isinstance(flip, list | tuple) and len(flip) >= 2:
            low = _number(flip[0])
            high = _number(flip[1])
            if low is not None and high is not None:
                result.setdefault("flip_low", min(low, high))
                result.setdefault("flip_high", max(low, high))
    return result


def _spot(payload: Mapping[str, Any]) -> tuple[float | None, str | None]:
    for key, source_key in (
        ("underlier", "source"),
        ("context_reference", "source"),
        ("research_reference", "source"),
    ):
        row = _mapping(payload.get(key))
        value = _number(row.get("price"))
        if value is not None:
            return value, str(row.get(source_key) or key)
    decision = _mapping(payload.get("level_decision"))
    value = _number(decision.get("spot"))
    if value is not None:
        return value, str(decision.get("spot_source") or "level_decision")
    path = _mapping(
        _mapping(_mapping(payload.get("spring_gamma_v3_shadow")).get("wall_probability")).get(
            "path"
        )
    )
    value = _number(path.get("underlier"))
    return (value, "wall_probability_path") if value is not None else (None, None)


def _boundary(
    levels: Mapping[str, float],
    *,
    side: str,
    spot: float | None,
) -> dict[str, Any]:
    candidates = [(name, value) for name, value in levels.items()]
    if not candidates:
        return {
            "status": "unavailable",
            "name": None,
            "names": [],
            "level": None,
            "distance_points": None,
            "side": side,
        }
    if spot is None:
        fallback_names = (
            ("put_wall", "flip_low")
            if side == "lower"
            else ("flip_high", "call_wall")
        )
        filtered = [row for row in candidates if row[0] in fallback_names]
        if not filtered:
            return {
                "status": "unavailable",
                "name": None,
                "names": [],
                "level": None,
                "distance_points": None,
                "side": side,
            }
        name, level = (
            max(filtered, key=lambda item: item[1])
            if side == "lower"
            else min(filtered, key=lambda item: item[1])
        )
        distance = None
    else:
        filtered = [
            row
            for row in candidates
            if (row[1] <= spot if side == "lower" else row[1] >= spot)
        ]
        if not filtered:
            return {
                "status": "unavailable",
                "name": None,
                "names": [],
                "level": None,
                "distance_points": None,
                "side": side,
                "reason": f"no_structure_level_on_{side}_side_of_spot",
            }
        name, level = (
            max(filtered, key=lambda item: item[1])
            if side == "lower"
            else min(filtered, key=lambda item: item[1])
        )
        distance = level - spot
    names_at_level = [
        candidate_name
        for candidate_name, candidate_level in candidates
        if candidate_level == level
    ]
    return {
        "status": "available",
        "name": name,
        "names": names_at_level,
        "level": _rounded(level),
        "distance_points": _rounded(distance),
        "side": side,
    }


def _option_evidence(
    payload: Mapping[str, Any],
    *,
    right: str,
    level: float | None,
) -> dict[str, Any]:
    shadow_key = "call_skew_spread_shadow" if right == "C" else "put_skew_spread_shadow"
    shadow = _mapping(payload.get(shadow_key))
    candidate = _mapping(shadow.get("candidate"))
    shadow_status = str(shadow.get("status") or "unavailable")
    if shadow_status == "candidate" and candidate:
        edge_status = "observed_local_skew_edge"
    elif shadow_status == "no_candidate":
        edge_status = "not_observed"
    else:
        edge_status = "unknown"
    references = [
        row
        for row in payload.get("candidates") or []
        if isinstance(row, Mapping) and str(row.get("right") or "") == right
    ]
    reference = (
        min(
            references,
            key=lambda row: abs((_number(row.get("level")) or 0.0) - (level or 0.0)),
        )
        if references
        else {}
    )
    greek_scores = _mapping(_mapping(payload.get("greek_decision")).get("contract_scores"))
    greek = _mapping(greek_scores.get(str(reference.get("contract_id") or "")))
    return {
        "right": right,
        "edge_status": edge_status,
        "skew_shadow_status": shadow_status,
        "skew_shadow_reason": shadow.get("reason"),
        "vertical": _compact_vertical(candidate) if candidate else None,
        "contract_reference": _compact_contract(reference, greek=greek),
        "claim_allowed": (
            "observed_local_skew_edge_only"
            if edge_status == "observed_local_skew_edge"
            else "no_mispricing_claim"
        ),
    }


def _compact_vertical(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy": candidate.get("strategy"),
        "long": _compact_leg(candidate.get("long")),
        "short": _compact_leg(candidate.get("short")),
        "executable_debit": candidate.get("executable_debit"),
        "fair_debit": candidate.get("fair_debit"),
        "edge_points": candidate.get("edge_points"),
        "iv_fit": {
            key: _mapping(candidate.get("iv_fit")).get(key)
            for key in (
                "fair_short_iv",
                "observed_short_iv",
                "short_iv_richness_vol_points",
                "required_richness_vol_points",
                "fit_mad_vol_points",
            )
        },
        "defined_risk": candidate.get("defined_risk"),
        "net_greeks": candidate.get("net_greeks"),
        "execution": candidate.get("execution"),
    }


def _compact_leg(value: object) -> dict[str, Any] | None:
    row = _mapping(value)
    if not row:
        return None
    return {
        key: row.get(key)
        for key in (
            "contract_id",
            "strike",
            "right",
            "provider",
            "bid",
            "ask",
            "bid_size",
            "ask_size",
            "iv",
            "delta",
            "gamma",
            "source_at",
        )
    }


def _compact_contract(
    value: Mapping[str, Any],
    *,
    greek: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not value:
        return None
    return {
        key: value.get(key)
        for key in (
            "contract_id",
            "play",
            "level",
            "strike",
            "right",
            "current_mid",
            "prob_touch",
            "projection_range_low",
            "projection_range_high",
            "execution_quote_status",
            "execution_quote_source_age_seconds",
            "execution_quote_spread_bps",
        )
    } | {
        "greek_mode": greek.get("mode"),
        "theta_15m_loss_fraction": greek.get("theta_15m_loss_fraction"),
        "iv_down_3vol_loss_fraction": greek.get("iv_down_3vol_loss_fraction"),
        "confidence_adjustment": greek.get("confidence_adjustment"),
    }


def _hypothesis(
    *,
    scenario: str,
    boundary: Mapping[str, Any],
    right: str,
    direction: str,
    required_path: str,
    falsifier: str,
    option_evidence: Mapping[str, Any],
    idea_generation_allowed: bool,
) -> dict[str, Any]:
    available = boundary.get("status") == "available"
    return {
        "scenario": scenario,
        "status": (
            "available"
            if available and idea_generation_allowed
            else "closed"
            if not idea_generation_allowed
            else "unavailable"
        ),
        "direction": direction,
        "option_right": right,
        "boundary_name": boundary.get("name"),
        "boundary_level": boundary.get("level"),
        "required_path": required_path,
        "falsifier": falsifier,
        "edge_status": option_evidence.get("edge_status"),
        "contract_reference": option_evidence.get("contract_reference"),
        "action_authority": "none",
    }


def _active_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(payload.get("level_decision"))
    return {
        key: decision.get(key)
        for key in (
            "event_id",
            "phase",
            "thesis",
            "direction",
            "level_kind",
            "level",
            "formal_signal",
            "quality_ok",
            "quality_reason",
            "expires_at",
        )
    }


def _tensions(
    payload: Mapping[str, Any],
    *,
    market_state: Mapping[str, Any],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    score = _number(market_state.get("D"))
    direction = _mapping(market_state.get("uncalibrated_direction"))
    diagnostic = str(direction.get("diagnostic_es_direction") or "")
    if score is not None and (
        (score > 0 and diagnostic == "down") or (score < 0 and diagnostic == "up")
    ):
        result.append(
            {
                "kind": "direction_model_conflict",
                "D": score,
                "diagnostic_es_direction": diagnostic,
            }
        )
    components = _mapping(market_state.get("direction_components"))
    structure = _number(components.get("market_structure"))
    price_vwap = _number(components.get("price_vs_vwap"))
    if structure is not None and price_vwap is not None and structure * price_vwap < 0:
        result.append(
            {
                "kind": "market_structure_vs_vwap_conflict",
                "market_structure_score": structure,
                "price_vs_vwap_score": price_vwap,
            }
        )
    er = _number(market_state.get("efficiency_ratio"))
    if market_state.get("state") == "UNCERTAIN" and er is not None and er > 0.65:
        result.append(
            {
                "kind": "clean_path_but_classification_uncertain",
                "efficiency_ratio": er,
                "input_available_count": market_state.get("input_available_count"),
                "input_required_count": market_state.get("input_required_count"),
            }
        )
    em_used = _number(_mapping(payload.get("day_move")).get("em_used_fraction"))
    if em_used is not None and em_used >= 0.70:
        result.append({"kind": "gth_expected_move_largely_consumed", "fraction": em_used})
    return result


def _moving_average_context(
    state: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    levels: Mapping[str, float],
) -> dict[str, Any] | None:
    lineage = _mapping(state.get("input_lineage"))
    diagnostics = _mapping(lineage.get("diagnostics"))
    moving = _mapping(diagnostics.get("moving_averages"))
    if not moving:
        intent = _mapping(payload.get("trade_intent"))
        moving = _mapping(intent.get("moving_average_context"))
    if not moving:
        return {
            "status": "unavailable",
            "ma200_structure_confluence": _ma200_structure_confluence(
                {},
                levels=levels,
            ),
            "action_authority": "none",
            "actionable": False,
        }
    result = {
        key: moving.get(key)
        for key in (
            "status",
            "price",
            "sma20",
            "sma50",
            "sma200",
            "atr_5m",
            "distance_to_sma20_points",
            "distance_to_sma50_points",
            "distance_to_sma200_points",
            "distance_to_sma50_atr",
            "distance_to_sma200_atr",
            "ma50_slope_3_atr",
            "ma50_slope_6_atr",
            "ma200_slope_3_atr",
            "ma200_slope_6_atr",
            "ma50_ma200_spread_points",
            "ma50_ma200_spread_atr",
            "spread_change_3_atr",
            "cross_direction",
            "bars_since_cross",
            "cross_persistent_2_bars",
            "cross_fresh",
            "regime_state",
            "regime_direction",
            "same_direction_convexity",
            "thresholds",
            "relation",
            "spx_equivalent_sma20",
            "spx_equivalent_sma50",
            "spx_equivalent_sma200",
            "spx_projection_near_line",
            "spx_projection_near_line_tolerance_points",
            "contract_identity",
            "basis_contract_identity_matches_sma",
            "action_authority",
        )
    }
    result["ma200_structure_confluence"] = _ma200_structure_confluence(
        moving,
        levels=levels,
    )
    result["action_authority"] = "none"
    result["actionable"] = False
    return result


def _ma200_structure_confluence(
    moving: Mapping[str, Any],
    *,
    levels: Mapping[str, float],
) -> dict[str, Any]:
    projected_ma200 = _number(moving.get("spx_equivalent_sma200"))
    atr = _number(moving.get("atr_5m"))
    base = {
        "source": "spx_equivalent_sma200_basis_projection",
        "nearest_kind": None,
        "nearest_level": None,
        "distance_points": None,
        "distance_atr": None,
        "decision_zone_threshold_atr": 0.5,
        "decision_zone": None,
        "direction_authority": "none",
        "entry_trigger": False,
        "action_authority": "none",
    }
    if projected_ma200 is None:
        return {
            **base,
            "status": "unavailable",
            "reason": "spx_equivalent_sma200_unavailable",
            "interpretation": "no_confluence_claim",
        }
    if atr is None or atr <= 0:
        return {
            **base,
            "status": "unavailable",
            "reason": "atr_5m_unavailable",
            "interpretation": "no_confluence_claim",
        }
    candidates = [
        (kind, level)
        for kind, raw_level in levels.items()
        if (level := _number(raw_level)) is not None
    ]
    if not candidates:
        return {
            **base,
            "status": "unavailable",
            "reason": "stable_levels_unavailable",
            "interpretation": "no_confluence_claim",
        }
    kind, level = min(
        candidates,
        key=lambda item: (abs(item[1] - projected_ma200), item[0]),
    )
    distance_points = abs(level - projected_ma200)
    distance_atr = distance_points / atr
    decision_zone = distance_atr <= 0.5
    return {
        **base,
        "status": "ready",
        "reason": None,
        "nearest_kind": kind,
        "nearest_level": _rounded(level),
        "distance_points": _rounded(distance_points),
        "distance_atr": _rounded(distance_atr),
        "decision_zone": decision_zone,
        "interpretation": (
            "wait_for_wall_or_flip_acceptance_or_rejection"
            if decision_zone
            else "location_context_only"
        ),
    }


def _market_breadth(state: Mapping[str, Any]) -> float | None:
    value = _number(state.get("breadth_above_vwap"))
    if value is not None:
        return value
    components = _mapping(state.get("direction_components"))
    raw = _number(components.get("breadth_above_vwap_ratio"))
    if raw is not None:
        return raw
    lineage = _mapping(state.get("input_lineage"))
    values = _mapping(lineage.get("values"))
    return _number(values.get("breadth_above_vwap"))


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return finite_float(value)


def _rounded(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        try:
            return _utc(value)
        except ValueError:
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("convexity idea radar requires an aware clock")
    return value.astimezone(timezone.utc)
