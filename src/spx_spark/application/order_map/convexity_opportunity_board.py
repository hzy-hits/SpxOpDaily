"""Dense, non-executable opportunity ranks for the convexity radar.

The board deliberately emits Call, Put, and volatility/range observations on
every eligible status cycle.  It ranks supplied facts without converting a
small historical sample, a wall test, or an option-skew residual into trading
authority.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from spx_spark.analytics.options.pricing import finite_float


SCHEMA_VERSION = "convexity_opportunity_board.v0"
_ACTIVE_EVENT_PHASES = {
    "accepted",
    "rejected",
    "retest",
    "testing",
    "confirmed",
}


def preferred_option_evidence(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Select the stronger observed side-level option context."""

    rank = {"observed_local_skew_edge": 2, "not_observed": 1, "unknown": 0}
    return max(
        (first, second),
        key=lambda row: rank.get(str(row.get("edge_status") or "unknown"), 0),
    )


def build_dense_opportunity_board(
    *,
    mandate: Mapping[str, Any],
    market_state: Mapping[str, Any],
    boundary_tests: Mapping[str, Any],
    option_evidence: Mapping[str, Any],
    volatility_context: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    hypotheses: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return three stable shadow rows without relaxing execution controls."""

    phase = str(mandate.get("phase") or "")
    observation_allowed = phase in {"gth_preparation", "rth_active"}
    path = _mapping(market_state.get("rolling_path_percentiles"))
    gth = _mapping(market_state.get("gth_observation"))
    call = _directional_lane(
        side="call",
        direction="up",
        observation_allowed=observation_allowed,
        phase=phase,
        market_state=market_state,
        boundary_tests=boundary_tests,
        evidence=_mapping(option_evidence.get("call")),
        data_quality=data_quality,
        hypotheses=hypotheses,
        path=path,
        gth=gth,
    )
    put = _directional_lane(
        side="put",
        direction="down",
        observation_allowed=observation_allowed,
        phase=phase,
        market_state=market_state,
        boundary_tests=boundary_tests,
        evidence=_mapping(option_evidence.get("put")),
        data_quality=data_quality,
        hypotheses=hypotheses,
        path=path,
        gth=gth,
    )
    vol_range = _volatility_range_lane(
        observation_allowed=observation_allowed,
        market_state=market_state,
        volatility_context=volatility_context,
        data_quality=data_quality,
        path=path,
    )
    ranked = sorted(
        (call, put, vol_range),
        key=lambda row: (
            -int(row.get("priority_score") or 0),
            str(row.get("lane") or ""),
        ),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "observing"
            if observation_allowed
            else "closed"
            if phase == "hard_exit_reached"
            else "inactive"
        ),
        "sample_policy": {
            "minimum_prior_sessions": 5,
            "target_prior_sessions": 20,
            "below_target_behavior": "emit_shrunk_low_confidence_shadow",
            "execution_gate_affected": False,
        },
        "path_percentiles": path,
        "gth_observation": gth,
        "lanes": {
            "call": call,
            "put": put,
            "vol_range": vol_range,
        },
        "rank_order": [str(row["lane"]) for row in ranked],
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }


def _directional_lane(
    *,
    side: str,
    direction: str,
    observation_allowed: bool,
    phase: str,
    market_state: Mapping[str, Any],
    boundary_tests: Mapping[str, Any],
    evidence: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    hypotheses: Sequence[Mapping[str, Any]],
    path: Mapping[str, Any],
    gth: Mapping[str, Any],
) -> dict[str, Any]:
    quality_status = str(data_quality.get("status") or "unknown")
    quality_reasons = [
        str(reason) for reason in data_quality.get("reasons") or []
    ][:4]
    if not observation_allowed:
        return {
            "lane": side,
            "status": "closed",
            "direction": direction,
            "priority": "WATCH",
            "priority_score": 0,
            "score_semantics": "transparent_shadow_rank_0_to_10_not_probability",
            "data_quality_status": quality_status,
            "wall_signal": "CLOSED",
            "gth_signal": "CLOSED",
            "trigger_paths": [],
            "edge_status": "not_evaluated",
            "structure_rank": [],
            "sample_confidence": "unavailable",
            "evidence_sessions": 0,
            "score_contributions": [],
            "execution": {
                "eligible": False,
                "block_reasons": [
                    "strategy_window_inactive",
                    *quality_reasons,
                    "dense_shadow_no_execution_authority",
                ],
            },
            "action_authority": "none",
            "actionable": False,
            "automatic_ordering": False,
        }

    contributions: list[dict[str, Any]] = []
    score = 0
    sign = 1 if direction == "up" else -1
    gth_mode = phase == "gth_preparation"
    gth_signal = _gth_signal(gth, side=side) if gth_mode else "NOT_APPLICABLE"
    if gth_mode:
        gth_points, gth_contributions = _gth_direction_points(
            gth,
            side=side,
            sign=sign,
        )
        score += gth_points
        contributions.extend(gth_contributions)
    d_score = _number(market_state.get("D"))
    if d_score is not None and d_score * sign > 0:
        points = min(max(int(abs(d_score) // 3), 1), 3)
        score += points
        contributions.append(
            {"feature": "direction_score", "points": points, "value": d_score}
        )

    path_rank_usable = (
        str(path.get("status") or "") in {"ready", "provisional"}
        and (_number(path.get("sample_count")) or 0) >= 5
        and _number(_mapping(path.get("dip")).get("shrunk_percentile")) is not None
        and _number(_mapping(path.get("rally")).get("shrunk_percentile")) is not None
    )
    path_bias = _number(path.get("signed_path_bias")) if path_rank_usable else None
    if path_bias is not None and path_bias * sign >= 0.15:
        points = 2 if abs(path_bias) >= 0.35 else 1
        if str(path.get("input_quality") or "strict") != "strict":
            points = 1
        score += points
        contributions.append(
            {
                "feature": "rolling_path_bias",
                "points": points,
                "value": path_bias,
                "input_quality": path.get("input_quality") or "strict",
            }
        )

    breadth = _number(market_state.get("breadth_above_vwap"))
    if breadth is not None and (
        (direction == "up" and breadth >= 0.55)
        or (direction == "down" and breadth <= 0.45)
    ):
        score += 1
        contributions.append(
            {"feature": "breadth_alignment", "points": 1, "value": breadth}
        )

    efficiency = _number(market_state.get("efficiency_ratio"))
    if (
        efficiency is not None
        and efficiency >= 0.45
        and d_score is not None
        and d_score * sign > 0
    ):
        score += 1
        contributions.append(
            {"feature": "path_efficiency", "points": 1, "value": efficiency}
        )

    event = _mapping(boundary_tests.get("active_event"))
    event_phase = str(event.get("phase") or "").lower()
    event_direction = str(event.get("direction") or "").lower()
    event_kind = str(event.get("level_kind") or "").lower()
    event_quality_ok = event.get("quality_ok") is True
    formal = event.get("formal_signal") is True
    event_rank_eligible = not gth_mode or event.get("rank_eligible") is True
    put_wall_breakdown_disabled = (
        side == "put"
        and event_rank_eligible
        and event_direction == "down"
        and event_kind == "put_wall"
        and event_phase in _ACTIVE_EVENT_PHASES
    )
    if (
        event_rank_eligible
        and event_quality_ok
        and not put_wall_breakdown_disabled
        and event_direction == direction
        and event_phase in _ACTIVE_EVENT_PHASES
    ):
        points = 3 if formal and event_phase == "confirmed" else 1
        score += points
        contributions.append(
            {
                "feature": "wall_lifecycle",
                "points": points,
                "value": event_phase,
            }
        )

    skew_rank_eligible = not gth_mode or evidence.get("rank_eligible") is True
    if (
        skew_rank_eligible
        and evidence.get("edge_status") == "observed_local_skew_edge"
    ):
        score += 1
        contributions.append(
            {
                "feature": "side_level_skew_context",
                "points": 1,
                "value": "not_boundary_specific",
            }
        )
    elif gth_mode and (
        event.get("rank_gate_reasons") or evidence.get("rank_gate_reasons")
    ):
        contributions.append(
            {
                "feature": "gth_stale_structure_modifiers_blocked",
                "points": 0,
                "value": {
                    "active_event": list(event.get("rank_gate_reasons") or []),
                    "skew": list(evidence.get("rank_gate_reasons") or []),
                },
            }
        )

    moving = _mapping(market_state.get("moving_averages"))
    if (
        moving.get("regime_state") == "TREND_EXTENDED"
        and str(moving.get("regime_direction") or "") == direction
    ):
        score = max(score - 2, 0)
        contributions.append(
            {
                "feature": "trend_extended_do_not_chase",
                "points": -2,
                "value": True,
            }
        )

    gth_direct_es_ready = (
        gth_mode and _mapping(gth.get("trend")).get("status") == "ready"
    )
    if quality_status == "degraded" and not gth_direct_es_ready:
        score = min(score, 2)
        contributions.append(
            {
                "feature": "degraded_data_priority_cap",
                "points": 0,
                "value": True,
            }
        )
    elif quality_status == "degraded" and gth_direct_es_ready:
        contributions.append(
            {
                "feature": "optional_option_data_degraded_gth_rank_retained",
                "points": 0,
                "value": True,
            }
        )
    if put_wall_breakdown_disabled:
        score = min(score, 2)
        contributions.append(
            {
                "feature": "put_wall_breakdown_disabled",
                "points": 0,
                "value": True,
            }
        )
    score = min(score, 10)
    relevant = [
        dict(row)
        for row in hypotheses
        if str(row.get("option_right") or "").upper()
        == ("C" if side == "call" else "P")
        and not (
            side == "put"
            and str(row.get("scenario") or "") == "lower_acceptance_put"
            and str(row.get("boundary_name") or "") == "put_wall"
        )
    ]
    available = any(row.get("status") == "available" for row in relevant) or (
        gth_mode and gth.get("status") == "ready"
    )
    if available:
        status = "observed"
    else:
        status = "unavailable"
    wall_signal = _wall_signal(
        event if event_rank_eligible else {},
        direction=direction,
        hypotheses=relevant,
        put_wall_breakdown_disabled=put_wall_breakdown_disabled,
    )
    effective_evidence = (
        evidence
        if skew_rank_eligible
        else {
            **dict(evidence),
            "edge_status": "unknown",
            "vertical": None,
        }
    )
    structures = (
        ["put_wall_breakdown_disabled"]
        if put_wall_breakdown_disabled
        else _directional_structures(
            side=side,
            evidence=effective_evidence,
            d_score=d_score,
            efficiency=efficiency,
        )
    )
    return {
        "lane": side,
        "status": status,
        "direction": direction,
        "priority": _priority(score),
        "priority_score": score,
        "score_semantics": "transparent_shadow_rank_0_to_10_not_probability",
        "data_quality_status": quality_status,
        "wall_signal": wall_signal,
        "gth_signal": gth_signal,
        "trigger_paths": [
            {
                "scenario": row.get("scenario"),
                "required_path": row.get("required_path"),
                "falsifier": row.get("falsifier"),
                "boundary_name": row.get("boundary_name"),
                "boundary_level": row.get("boundary_level"),
            }
            for row in relevant
        ],
        "edge_status": effective_evidence.get("edge_status") or "unknown",
        "structure_rank": structures,
        "sample_confidence": path.get("confidence") or "unavailable",
        "evidence_sessions": path.get("sample_count") or 0,
        "score_contributions": contributions,
        "execution": {
            "eligible": False,
            "block_reasons": [
                *quality_reasons,
                "dense_shadow_no_execution_authority",
            ],
        },
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }


def _volatility_range_lane(
    *,
    observation_allowed: bool,
    market_state: Mapping[str, Any],
    volatility_context: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    path: Mapping[str, Any],
) -> dict[str, Any]:
    quality_status = str(data_quality.get("status") or "unknown")
    quality_reasons = [
        str(reason) for reason in data_quality.get("reasons") or []
    ][:4]
    if not observation_allowed:
        return {
            "lane": "vol_range",
            "status": "closed",
            "volatility_signal": "CLOSED",
            "priority": "WATCH",
            "priority_score": 0,
            "score_semantics": "environment_rank_not_option_mispricing_probability",
            "data_quality_status": quality_status,
            "edge_status": "not_evaluated",
            "structure_rank": [],
            "atm_iv_0dte": None,
            "atm_iv_1dte": None,
            "same_time_range_ratio": None,
            "efficiency_ratio": None,
            "sample_confidence": "unavailable",
            "evidence_sessions": 0,
            "execution": {
                "eligible": False,
                "block_reasons": [
                    "strategy_window_inactive",
                    "remaining_vol_edge_not_calibrated",
                    "dense_shadow_no_execution_authority",
                    *quality_reasons,
                ],
            },
            "action_authority": "none",
            "actionable": False,
            "automatic_ordering": False,
        }

    ratio = _number(market_state.get("same_time_range_ratio"))
    efficiency = _number(market_state.get("efficiency_ratio"))
    d_score = _number(market_state.get("D"))
    crosses = market_state.get("vwap_cross_count")
    if (
        ratio is not None
        and ratio < 0.75
        and d_score is not None
        and abs(d_score) <= 2
    ):
        signal = "COMPRESSION"
        score = 4
        structures = [
            "long_straddle_if_remaining_vol_underpriced",
            "1dte_defined_risk_iron_condor_if_tails_overpriced",
        ]
    elif ratio is not None and ratio > 1.25 and efficiency is not None and efficiency < 0.25:
        signal = "HIGH_VOL_CHOP"
        score = 4
        structures = ["avoid_directional_long_premium", "no_trade_until_path_cleans"]
    elif ratio is not None and ratio > 1.25 and efficiency is not None and efficiency >= 0.45:
        signal = "TREND_EXPANSION"
        score = 3
        structures = ["directional_long_premium_watch", "defined_risk_debit_spread_watch"]
    else:
        signal = "MIXED_OR_UNCALIBRATED"
        score = 1
        structures = ["no_structure_until_remaining_vol_edge"]
    if isinstance(crosses, int) and crosses >= 3 and signal == "COMPRESSION":
        score = min(score + 1, 10)
    if quality_status == "degraded":
        score = min(score, 2)
    return {
        "lane": "vol_range",
        "status": "observed",
        "volatility_signal": signal,
        "priority": _priority(score),
        "priority_score": score,
        "score_semantics": "environment_rank_not_option_mispricing_probability",
        "data_quality_status": quality_status,
        "edge_status": "requires_remaining_vol_and_tail_pricing_edge",
        "structure_rank": structures,
        "atm_iv_0dte": volatility_context.get("atm_iv_0dte"),
        "atm_iv_1dte": volatility_context.get("atm_iv_1dte"),
        "same_time_range_ratio": ratio,
        "efficiency_ratio": efficiency,
        "sample_confidence": path.get("confidence") or "unavailable",
        "evidence_sessions": path.get("sample_count") or 0,
        "execution": {
            "eligible": False,
            "block_reasons": [
                "remaining_vol_edge_not_calibrated",
                "dense_shadow_no_execution_authority",
                *quality_reasons,
            ],
        },
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }


def _wall_signal(
    event: Mapping[str, Any],
    *,
    direction: str,
    hypotheses: Sequence[Mapping[str, Any]],
    put_wall_breakdown_disabled: bool,
) -> str:
    if put_wall_breakdown_disabled:
        return "DISABLED_UNSUPPORTED:put_wall"
    raw_phase = str(event.get("phase") or "")
    phase = raw_phase.upper()
    event_direction = str(event.get("direction") or "").lower()
    if (
        event_direction == direction
        and raw_phase.lower() in _ACTIVE_EVENT_PHASES
    ):
        kind = str(event.get("level_kind") or "-")
        return f"{phase}:{kind}"
    scenarios = [
        str(row.get("scenario") or "")
        for row in hypotheses
        if row.get("status") == "available"
    ]
    return "WATCH:" + "|".join(scenarios) if scenarios else "UNAVAILABLE"


def _directional_structures(
    *,
    side: str,
    evidence: Mapping[str, Any],
    d_score: float | None,
    efficiency: float | None,
) -> list[str]:
    vertical = _mapping(evidence.get("vertical"))
    strategy = str(vertical.get("strategy") or "")
    if evidence.get("edge_status") == "observed_local_skew_edge" and strategy:
        fallback = "long_call_watch" if side == "call" else "long_put_watch"
        return [strategy, fallback]
    long_option = "long_call_watch" if side == "call" else "long_put_watch"
    debit = "call_debit_spread_watch" if side == "call" else "put_debit_spread_watch"
    directional = (
        d_score is not None
        and d_score * (1 if side == "call" else -1) >= 6
        and efficiency is not None
        and efficiency >= 0.45
    )
    return [long_option, debit] if directional else [debit, long_option]


def _gth_direction_points(
    gth: Mapping[str, Any],
    *,
    side: str,
    sign: int,
) -> tuple[int, list[dict[str, Any]]]:
    contributions: list[dict[str, Any]] = []
    points = 0
    trend = _mapping(gth.get("trend"))
    if trend.get("status") == "ready":
        regime = str(trend.get("regime") or "")
        aligned = (side == "call" and regime == "bullish") or (
            side == "put" and regime == "bearish"
        )
        if aligned:
            points += 2
            contributions.append(
                {"feature": "gth_direct_es_regime", "points": 2, "value": regime}
            )
        aligned_returns = sum(
            1
            for field in (
                "return_15m_points",
                "return_60m_points",
                "return_180m_points",
            )
            if (value := _number(trend.get(field))) is not None and value * sign > 0
        )
        if aligned_returns:
            return_points = min(aligned_returns, 2)
            points += return_points
            contributions.append(
                {
                    "feature": "gth_multi_horizon_alignment",
                    "points": return_points,
                    "value": aligned_returns,
                }
            )
    if side == "call":
        dip = _mapping(gth.get("dip_reclaim_call"))
        if dip.get("status") == "active":
            quality_pass = dip.get("entry_quality_verdict") == "pass"
            dip_points = 2 if quality_pass else 0
            points += dip_points
            contributions.append(
                {
                    "feature": "gth_dip_reclaim_call",
                    "points": dip_points,
                    "value": "aligned" if quality_pass else "observation_only",
                }
            )
        manual = _mapping(gth.get("manual_candidate"))
        if (
            manual.get("status") == "manual_ready"
            and manual.get("manual_action_eligible") is True
        ):
            points += 1
            contributions.append(
                {
                    "feature": "gth_exact_manual_candidate",
                    "points": 1,
                    "value": "manual_ready_separate_lane",
                }
            )
    return points, contributions


def _gth_signal(gth: Mapping[str, Any], *, side: str) -> str:
    labels: list[str] = []
    trend = _mapping(gth.get("trend"))
    regime = str(trend.get("regime") or "")
    if side == "call":
        manual = _mapping(gth.get("manual_candidate"))
        dip = _mapping(gth.get("dip_reclaim_call"))
        if (
            manual.get("status") == "manual_ready"
            and manual.get("manual_action_eligible") is True
        ):
            labels.append("MANUAL_READY_SEPARATE_CARD")
        if dip.get("status") == "active":
            labels.append(
                "DIP_RECLAIM_ALIGNED"
                if dip.get("entry_quality_verdict") == "pass"
                else "DIP_RECLAIM_OBSERVATION"
            )
        if trend.get("status") == "ready":
            labels.append(
                "TREND_BULLISH"
                if regime == "bullish"
                else "COUNTER_TREND"
                if regime == "bearish"
                else "TREND_NEUTRAL"
            )
    elif trend.get("status") == "ready":
        labels.append(
            "TREND_BEARISH"
            if regime == "bearish"
            else "COUNTER_TREND"
            if regime == "bullish"
            else "TREND_NEUTRAL"
        )
    return "|".join(labels) if labels else "WATCH"


def _priority(score: int) -> str:
    if score >= 6:
        return "HIGH"
    if score >= 3:
        return "MEDIUM"
    return "WATCH"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return finite_float(value)
