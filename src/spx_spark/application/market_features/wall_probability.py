"""Risk-neutral wall probabilities and a non-executable 0DTE/1DTE shadow.

This module deliberately has no direction or action authority.  The wall path
always comes from the exact, current 0DTE structure.  A next-expiry chain may
only be selected as an expression tenor; it never supplies or changes a wall.

Probabilities reuse :func:`probability_for_level`.  Only live, two-sided
quotes carrying a real implied volatility are supplied to that function, so
its delta fallback cannot enter this shadow.  The resulting N(d2) terminal
probability and 2x-reflection touch heuristic are risk-neutral diagnostics,
not physical forecasts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from enum import Enum

from spx_spark.analytics.options.chain import (
    is_spxw_option,
    median_strike_step,
    pair_by_strike,
)
from spx_spark.analytics.options.models import OptionsMap
from spx_spark.analytics.options.pricing import (
    finite_float,
    option_iv,
    usable_delta,
)
from spx_spark.analytics.options.probability import probability_for_level
from spx_spark.application.market_features.models import OptionStructureFrame
from spx_spark.application.market_features.wall_probability_policy import (
    GTH_MAX_INPUT_FRAME_AGE_SECONDS,
    GTH_MAX_LIVE_QUOTE_AGE_SECONDS,
    MIN_IV_COVERAGE,
    RTH_MAX_INPUT_FRAME_AGE_SECONDS,
    RTH_MAX_LIVE_QUOTE_AGE_SECONDS,
    expiry_close as _expiry_close,
    input_freshness as _input_freshness,
    live_two_sided as _live_two_sided,
    quote_age_seconds as _quote_age_seconds,
    summary_tenor as _summary_tenor,
    tenor_eligibility as _tenor_eligibility,
    tenor_market_snapshot as _tenor_market_snapshot,
    tenor_plan_by_horizon as _tenor_plan_by_horizon,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import (
    OptionRight,
    Quote,
    as_utc,
)


SCHEMA_VERSION = "wall_probability_tenor_shadow.v1"
POLICY_STATUS = "policy_prior_not_backtested"
PROBABILITY_SEMANTICS = "risk_neutral_not_physical"
TOUCH_PROBABILITY_SEMANTICS = (
    "zero_drift_2x_terminal_reflection_heuristic_not_calibrated_or_physical"
)
DEFAULT_HORIZONS = (15, 30, 60)

__all__ = [
    "build_wall_probability_tenor_shadow",
    "build_wall_probability_shadow",
]


def build_wall_probability_tenor_shadow(
    *,
    options_map: OptionsMap | Mapping[str, object],
    grouped_quotes: Mapping[str, Sequence[Quote]],
    option_frame: OptionStructureFrame | Mapping[str, object],
    direction: str,
    now: datetime,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
) -> dict[str, object]:
    """Return one fail-closed wall-probability and tenor shadow record.

    The caller supplies an already grouped option chain.  ``direction`` is an
    upstream observation (``up``/``down``/``abstain``); it is used only to
    evaluate whether each expression tenor has a live quote on the relevant
    option side.  It is never created, reversed, or promoted here.
    """

    at = as_utc(now)
    local = at.astimezone(ET)
    rth_open = DEFAULT_MARKET_CALENDAR.is_rth_open(at)
    gth_open = DEFAULT_MARKET_CALENDAR.is_spx_gth_open(at)
    session = "rth" if rth_open else "gth" if gth_open else "closed"
    quote_max_age_seconds = (
        RTH_MAX_LIVE_QUOTE_AGE_SECONDS
        if rth_open
        else GTH_MAX_LIVE_QUOTE_AGE_SECONDS
    )
    input_max_age_seconds = (
        RTH_MAX_INPUT_FRAME_AGE_SECONDS
        if rth_open
        else GTH_MAX_INPUT_FRAME_AGE_SECONDS
    )
    options = _mapping(options_map)
    frame = _mapping(option_frame)
    structure = _mapping(frame.get("structure"))
    normalized_direction = str(direction or "").strip().lower()
    normalized_horizons, horizon_error = _horizons(horizons)
    expected_front_day = DEFAULT_MARKET_CALENDAR.research_expiry(at)
    expected_front = expected_front_day.strftime("%Y%m%d")
    expected_next = DEFAULT_MARKET_CALENDAR.next_trading_day(
        expected_front_day
    ).strftime("%Y%m%d")
    front_row, next_row = _expiry_rows(options)
    map_front = str(front_row.get("expiry") or "")
    frame_front = str(frame.get("front_expiry") or "")
    frame_next = str(frame.get("next_expiry") or "")
    map_next = str(next_row.get("expiry") or "")
    next_expiry = frame_next or map_next
    front_expiry_contract_valid = (
        map_front == expected_front and frame_front == expected_front
    )
    next_expiry_contract_valid = (
        map_next == expected_next and frame_next == expected_next
    )
    spot = _underlier(options, structure)
    stable_levels, level_error, flip_definition = _stable_levels(structure)
    input_freshness = {
        "session": session,
        "maximum_age_seconds": input_max_age_seconds,
        "options_map": _input_freshness(
            options.get("as_of", options.get("created_at")),
            now=at,
            label="options_map",
            max_age_seconds=input_max_age_seconds,
        ),
        "option_frame": _input_freshness(
            frame.get("as_of"),
            now=at,
            label="option_frame",
            max_age_seconds=input_max_age_seconds,
        ),
    }

    probability_gate_reasons: list[str] = []
    overall_gate_reasons: list[str] = []
    if normalized_direction not in {"up", "down", "abstain"}:
        overall_gate_reasons.append("invalid_direction")
    elif normalized_direction == "abstain":
        overall_gate_reasons.append("direction_abstain")
    if horizon_error:
        overall_gate_reasons.append(horizon_error)
    if not rth_open:
        overall_gate_reasons.append("rth_required_for_tenor_prior")
    if not (rth_open or gth_open):
        probability_gate_reasons.append("spx_option_session_required")
    if not front_expiry_contract_valid:
        probability_gate_reasons.append("front_exact_expiry_mismatch")
    for freshness in (
        input_freshness["options_map"],
        input_freshness["option_frame"],
    ):
        if freshness["status"] != "fresh":
            probability_gate_reasons.append(str(freshness["reason"]))
    if _token(frame.get("quality")) not in {"ready", "ok", "live"}:
        probability_gate_reasons.append("option_frame_not_live_ready")
    if structure.get("frozen") is True or "frozen" in str(
        structure.get("source") or ""
    ).lower():
        probability_gate_reasons.append("front_structure_frozen")
    if spot is None or spot <= 0:
        probability_gate_reasons.append("underlier_unavailable")
    if level_error:
        probability_gate_reasons.append(level_error)

    right = _direction_right(normalized_direction)
    front_quotes = _quotes_for_expiry(grouped_quotes, map_front)
    next_quotes = _quotes_for_expiry(grouped_quotes, next_expiry)
    if not map_front or not front_quotes:
        probability_gate_reasons.append("front_chain_unavailable")
    eligibility_0dte = _tenor_eligibility(
        expiry=map_front or None,
        expected_expiry=expected_front,
        expiry_contract_valid=front_expiry_contract_valid,
        tenor="0DTE",
        quotes=front_quotes,
        right=right,
        now=at,
        max_quote_age_seconds=quote_max_age_seconds,
    )
    eligibility_1dte = _tenor_eligibility(
        expiry=next_expiry or None,
        expected_expiry=expected_next,
        expiry_contract_valid=next_expiry_contract_valid,
        tenor="1DTE",
        quotes=next_quotes,
        right=right,
        now=at,
        max_quote_age_seconds=quote_max_age_seconds,
    )
    if rth_open and not eligibility_0dte["eligible"]:
        overall_gate_reasons.append(
            "front_live_bid_ask_iv_coverage_insufficient"
        )
        if eligibility_0dte["iv_coverage_ratio"] < MIN_IV_COVERAGE:
            overall_gate_reasons.append(
                "real_iv_required_delta_fallback_rejected"
            )

    eligibility_by_tenor = {
        "0DTE": eligibility_0dte,
        "1DTE": eligibility_1dte,
    }
    probability_gate_passed = not probability_gate_reasons
    overall_gate_reasons.extend(probability_gate_reasons)
    overall_gate_passed = not overall_gate_reasons
    tenor_by_horizon = _tenor_plan_by_horizon(
        local=local,
        horizons=normalized_horizons,
        eligibility=eligibility_by_tenor,
        front_required=rth_open and eligibility_0dte["eligible"] is True,
        prior_available=rth_open,
    )

    probabilities: dict[str, dict[str, dict[str, object]]] = {}
    probability_reasons: dict[str, list[str]] = {
        f"{horizon}m": [] for horizon in normalized_horizons
    }
    if probability_gate_passed and spot is not None:
        probabilities, probability_reasons = _wall_probabilities(
            levels=stable_levels,
            underlier=spot,
            quotes=front_quotes,
            expiry=map_front,
            now=at,
            horizons=normalized_horizons,
            max_quote_age_seconds=quote_max_age_seconds,
        )

    horizon_status: dict[str, dict[str, object]] = {}
    directional_targets: dict[str, dict[str, object]] = {}
    available_horizons: list[str] = []
    probability_available_horizons: list[str] = []
    for horizon in normalized_horizons:
        key = f"{horizon}m"
        tenor_row = tenor_by_horizon[key]
        local_reasons = list(tenor_row.get("reasons") or [])
        local_reasons.extend(probability_reasons.get(key, []))
        if not overall_gate_passed:
            local_reasons.extend(overall_gate_reasons)
            tenor_row["selected_tenor"] = None
            tenor_row["selected_expiry"] = None
            tenor_row["fallback_used"] = False
        probability_rows = probabilities.get(key, {})
        all_levels_available = bool(probability_rows) and all(
            row.get("status") == "available" for row in probability_rows.values()
        )
        if all_levels_available:
            probability_available_horizons.append(key)
        directional_target = _directional_target(
            direction=normalized_direction,
            underlier=spot,
            levels=stable_levels,
            probabilities=probability_rows,
            horizon=horizon,
        )
        target_calculated = directional_target["status"] == "available"
        if not all_levels_available and not probability_reasons.get(key):
            local_reasons.append("wall_probability_unavailable")
        if not target_calculated:
            local_reasons.append(str(directional_target["reason"]))
        if tenor_row.get("selected_tenor") is None:
            local_reasons.append("expression_tenor_unavailable")
        local_reasons = list(dict.fromkeys(reason for reason in local_reasons if reason))
        available = (
            overall_gate_passed
            and all_levels_available
            and target_calculated
            and tenor_row.get("selected_tenor") is not None
            and not local_reasons
        )
        directional_target["usable"] = available
        directional_target["usable_scope"] = "shadow_diagnostic_only"
        directional_target["diagnostic_usable"] = available
        directional_target["execution_usable"] = False
        directional_target["action_authority"] = "none"
        directional_target["action"] = "none"
        if not available and target_calculated:
            directional_target["status"] = "unavailable"
            directional_target["reason"] = "horizon_gate_unavailable"
            directional_target["diagnostic_values_retained"] = True
        else:
            directional_target["diagnostic_values_retained"] = False
        directional_targets[key] = directional_target
        tenor_row["status"] = "available" if available else "unavailable"
        tenor_row["unavailable_reasons"] = [] if available else local_reasons
        horizon_status[key] = {
            "status": "available" if available else "unavailable",
            "wall_probability_available": all_levels_available,
            "directional_target_available": available,
            "expression_tenor_available": tenor_row.get("selected_tenor") is not None,
            "reasons": [] if available else local_reasons,
        }
        if available:
            available_horizons.append(key)

    reasons = list(overall_gate_reasons)
    if not available_horizons:
        reasons.append("no_horizon_with_wall_probability_and_tenor")
        reasons.extend(
            reason
            for row in horizon_status.values()
            for reason in row["reasons"]
        )
    reasons = list(dict.fromkeys(reason for reason in reasons if reason))
    abstain = not available_horizons
    preferred_tenor = _summary_tenor(
        [
            str(row["preferred_tenor"])
            for row in tenor_by_horizon.values()
            if row["preferred_tenor"] is not None
        ]
    )
    selected_tenor = _summary_tenor(
        [
            str(tenor_by_horizon[key]["selected_tenor"])
            for key in available_horizons
        ]
    )
    fallback_used = any(
        tenor_by_horizon[key]["fallback_used"] is True for key in available_horizons
    )

    warnings = [
        PROBABILITY_SEMANTICS,
        TOUCH_PROBABILITY_SEMANTICS,
        POLICY_STATUS,
    ]
    if eligibility_1dte["eligible"] is not True:
        warnings.append("next_expiry_expression_unavailable")
    if fallback_used:
        warnings.append("preferred_tenor_unavailable_fallback_used")

    selected_expiry = (
        map_front
        if selected_tenor == "0DTE"
        else next_expiry
        if selected_tenor == "1DTE"
        else None
    )
    tenor_market = _tenor_market_snapshot(
        front_row=front_row,
        next_row=next_row,
        volatility=_mapping(frame.get("volatility")),
        eligibility=eligibility_by_tenor,
        front_contract_valid=front_expiry_contract_valid,
        next_contract_valid=next_expiry_contract_valid,
    )
    probability_status = (
        "ready"
        if len(probability_available_horizons) == len(normalized_horizons)
        and normalized_horizons
        else "partial"
        if probability_available_horizons
        else "unavailable"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "as_of": at.isoformat(),
        "mode": "shadow",
        "status": "abstain" if abstain else "ready",
        "direction": normalized_direction
        if normalized_direction in {"up", "down", "abstain"}
        else "abstain",
        "direction_authority": "none",
        "action_authority": "none",
        "action": "none",
        "actionable": False,
        "automatic_ordering": False,
        "execution_status": "not_executable",
        "policy_status": POLICY_STATUS,
        "session": session,
        "probability_status": probability_status,
        "probability_semantics": PROBABILITY_SEMANTICS,
        "touch_probability_semantics": TOUCH_PROBABILITY_SEMANTICS,
        "input_freshness": input_freshness,
        "expiry_contract": {
            "0DTE": {
                "expected_expiry": expected_front,
                "options_map_expiry": map_front or None,
                "option_frame_expiry": frame_front or None,
                "valid": front_expiry_contract_valid,
            },
            "1DTE": {
                "expected_expiry": expected_next,
                "options_map_expiry": map_next or None,
                "option_frame_expiry": frame_next or None,
                "valid": next_expiry_contract_valid,
            },
        },
        "path": {
            "tenor": "0DTE",
            "expiry": map_front or None,
            "expected_expiry": expected_front,
            "underlier": spot,
            "wall_source": "stable_front_0dte_structure",
            "flip_definition": flip_definition,
            "exact_expiry_required": True,
            "live_quotes_required": True,
            "maximum_live_quote_age_seconds": quote_max_age_seconds,
            "frozen_structure_allowed": False,
            "probability_model": "risk_neutral_nd2_front_0dte_fixed_iv_window",
            "iv_policy": (
                "current_live_0dte_anchor_iv_held_fixed_per_level_across_horizons"
            ),
        },
        "stable_levels": stable_levels,
        "wall_probabilities": probabilities,
        "directional_targets": directional_targets,
        "horizon_status": horizon_status,
        "tenor_shadow": {
            "cutoff_et": "13:00",
            "policy": (
                "prefer_1dte_only_when_planned_exit_at_or_before_13_et"
            ),
            "policy_status": POLICY_STATUS,
            "selection_authority": "none",
            "execution_usable": False,
            "expression_only": True,
            "tenor_basis": "next_trading_expiry",
            "wall_path_always_0dte": True,
            "preferred_tenor": preferred_tenor,
            "selected_tenor": selected_tenor,
            "selected_expiry": selected_expiry,
            "fallback_used": fallback_used,
            "by_horizon": tenor_by_horizon,
            "market": tenor_market,
            "eligibility": eligibility_by_tenor,
        },
        "horizons_minutes": list(normalized_horizons),
        "probability_available_horizons": probability_available_horizons,
        "available_horizons": available_horizons,
        "abstain": abstain,
        "abstain_reasons": reasons if abstain else [],
        "warnings": list(dict.fromkeys(warnings)),
    }


def build_wall_probability_shadow(
    *,
    options_map: OptionsMap | Mapping[str, object],
    grouped_quotes: Mapping[str, Sequence[Quote]],
    option_frame: OptionStructureFrame | Mapping[str, object],
    direction: str,
    now: datetime,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
) -> dict[str, object]:
    """Compatibility alias for the combined wall/tenor shadow builder."""

    return build_wall_probability_tenor_shadow(
        options_map=options_map,
        grouped_quotes=grouped_quotes,
        option_frame=option_frame,
        direction=direction,
        now=now,
        horizons=horizons,
    )


def _wall_probabilities(
    *,
    levels: Mapping[str, float],
    underlier: float,
    quotes: list[Quote],
    expiry: str,
    now: datetime,
    horizons: tuple[int, ...],
    max_quote_age_seconds: float,
) -> tuple[
    dict[str, dict[str, dict[str, object]]],
    dict[str, list[str]],
]:
    # Supplying only these quotes makes probability_for_level's delta fallback
    # unreachable: every possible anchor has a real positive IV.
    probability_quotes = [
        quote
        for quote in quotes
        if _live_two_sided(
            quote,
            now=now,
            max_age_seconds=max_quote_age_seconds,
        )
        and option_iv(quote) is not None
    ]
    pairs = pair_by_strike(probability_quotes)
    strikes = sorted(pairs)
    strike_step = median_strike_step(strikes)
    result: dict[str, dict[str, dict[str, object]]] = {}
    reasons: dict[str, list[str]] = {}
    expiry_close = _expiry_close(expiry)
    local_now = now.astimezone(ET)
    for horizon in horizons:
        horizon_key = f"{horizon}m"
        result[horizon_key] = {}
        reasons[horizon_key] = []
        planned_exit = local_now + timedelta(minutes=horizon)
        holding_window_valid = (
            expiry_close is not None and planned_exit <= expiry_close
        )
        if not holding_window_valid:
            reason = (
                "expiry_session_unavailable"
                if expiry_close is None
                else "holding_window_crosses_expiry"
            )
            reasons[horizon_key].append(reason)
            for level_name, level in levels.items():
                right = (
                    OptionRight.CALL if level >= underlier else OptionRight.PUT
                )
                result[horizon_key][level_name] = {
                    "status": "unavailable",
                    "level": level,
                    "terminal_beyond_probability": None,
                    "touch_probability_2x_reflection": None,
                    "prob_close_beyond": None,
                    "prob_touch": None,
                    "source_strike": None,
                    "source_right": right.value,
                    "source_delta": None,
                    "source_iv": None,
                    "source_quote_age_seconds": None,
                    "horizon_minutes": horizon,
                    "tau_years": None,
                    "planned_exit_at": planned_exit.isoformat(),
                    "expiry_close_at": (
                        expiry_close.isoformat() if expiry_close else None
                    ),
                    "holding_window_valid": False,
                    "method": "risk_neutral_nd2_and_2x_reflection",
                    "probability_semantics": PROBABILITY_SEMANTICS,
                    "touch_probability_semantics": TOUCH_PROBABILITY_SEMANTICS,
                    "delta_role": "anchor_selection_only_not_probability",
                    "delta_fallback_allowed": False,
                    "reason": reason,
                }
            continue

        tau_years = horizon / (365.0 * 24.0 * 60.0)
        for level_name, level in levels.items():
            terminal, touch, source_strike, source_delta = probability_for_level(
                level,
                underlier=underlier,
                pairs=pairs,
                strike_step=strike_step,
                tau_years=tau_years,
            )
            right = OptionRight.CALL if level >= underlier else OptionRight.PUT
            source_quote = (
                (pairs.get(source_strike) or {}).get(right)
                if source_strike is not None
                else None
            )
            source_iv = option_iv(source_quote)
            source_quote_age = (
                _quote_age_seconds(source_quote, now=now)
                if source_quote is not None
                else None
            )
            valid = all(
                value is not None
                for value in (terminal, touch, source_strike, source_delta, source_iv)
            )
            reason = None
            if not valid:
                reason = _probability_unavailable_reason(
                    level=level,
                    underlier=underlier,
                    pairs=pairs,
                    strike_step=strike_step,
                    source_strike=source_strike,
                    source_iv=source_iv,
                )
                reasons[horizon_key].extend(
                    [reason, f"{level_name}:{reason}"]
                )
            elif not (
                0.0 <= float(terminal) <= 1.0 and 0.0 <= float(touch) <= 1.0
            ):
                reason = "probability_out_of_bounds"
                reasons[horizon_key].extend(
                    [reason, f"{level_name}:{reason}"]
                )
                valid = False
            result[horizon_key][level_name] = {
                "status": "available" if valid else "unavailable",
                "level": level,
                "terminal_beyond_probability": terminal if valid else None,
                "touch_probability_2x_reflection": touch if valid else None,
                "prob_close_beyond": terminal if valid else None,
                "prob_touch": touch if valid else None,
                "source_strike": source_strike if valid else None,
                "source_right": right.value,
                "source_delta": source_delta if valid else None,
                "source_iv": source_iv if valid else None,
                "source_quote_age_seconds": (
                    source_quote_age if valid else None
                ),
                "horizon_minutes": horizon,
                "tau_years": tau_years,
                "planned_exit_at": planned_exit.isoformat(),
                "expiry_close_at": expiry_close.isoformat(),
                "holding_window_valid": True,
                "method": "risk_neutral_nd2_and_2x_reflection",
                "probability_semantics": PROBABILITY_SEMANTICS,
                "touch_probability_semantics": TOUCH_PROBABILITY_SEMANTICS,
                "delta_role": "anchor_selection_only_not_probability",
                "delta_fallback_allowed": False,
                "reason": reason,
            }
        reasons[horizon_key] = list(dict.fromkeys(reasons[horizon_key]))
    return result, reasons


def _directional_target(
    *,
    direction: str,
    underlier: float | None,
    levels: Mapping[str, float],
    probabilities: Mapping[str, Mapping[str, object]],
    horizon: int,
) -> dict[str, object]:
    base = {
        "status": "unavailable",
        "horizon_minutes": horizon,
        "direction": direction if direction in {"up", "down"} else "abstain",
        "direction_source": "upstream_input_no_probability_inference",
        "level_name": None,
        "level": None,
        "distance_points": None,
        "signed_distance_points": None,
        "terminal_beyond_probability": None,
        "touch_probability_2x_reflection": None,
        "prob_close_beyond": None,
        "prob_touch": None,
        "method": None,
        "probability_semantics": PROBABILITY_SEMANTICS,
        "touch_probability_semantics": TOUCH_PROBABILITY_SEMANTICS,
        "usable_scope": "shadow_diagnostic_only",
        "diagnostic_usable": False,
        "execution_usable": False,
        "action_authority": "none",
        "action": "none",
        "reason": None,
    }
    if direction not in {"up", "down"} or underlier is None:
        return {**base, "reason": "directional_target_input_unavailable"}
    candidates = []
    for name, level in levels.items():
        allowed_name = (
            name == "call_wall" or name.startswith("flip")
            if direction == "up"
            else name == "put_wall" or name.startswith("flip")
        )
        on_directional_side = (
            level >= underlier if direction == "up" else level <= underlier
        )
        if allowed_name and on_directional_side:
            distance = (
                level - underlier if direction == "up" else underlier - level
            )
            candidates.append((distance, level, name))
    if not candidates:
        return {**base, "reason": "directional_stable_level_unavailable"}
    distance, level, name = min(candidates, key=lambda item: (item[0], item[1]))
    probability = probabilities.get(name) or {}
    if probability.get("status") != "available":
        return {
            **base,
            "level_name": name,
            "level": level,
            "distance_points": distance,
            "signed_distance_points": level - underlier,
            "reason": str(
                probability.get("reason") or "wall_probability_unavailable"
            ),
        }
    return {
        **base,
        "status": "available",
        "level_name": name,
        "level": level,
        "distance_points": distance,
        "signed_distance_points": level - underlier,
        "terminal_beyond_probability": probability.get(
            "terminal_beyond_probability"
        ),
        "touch_probability_2x_reflection": probability.get(
            "touch_probability_2x_reflection"
        ),
        "prob_close_beyond": probability.get("prob_close_beyond"),
        "prob_touch": probability.get("prob_touch"),
        "method": probability.get("method"),
        "reason": None,
    }


def _stable_levels(
    structure: Mapping[str, object],
) -> tuple[dict[str, float], str | None, str | None]:
    put_wall = finite_float(
        structure.get("put_wall", structure.get("stable_put_wall"))
    )
    call_wall = finite_float(
        structure.get("call_wall", structure.get("stable_call_wall"))
    )
    flip = structure.get("flip_zone", structure.get("stable_flip_zone"))
    levels: dict[str, float] = {}
    flip_definition: str | None = None
    if put_wall is not None and put_wall > 0:
        levels["put_wall"] = put_wall

    flip_values: list[float] = []
    if isinstance(flip, Sequence) and not isinstance(flip, (str, bytes)):
        flip_values = [
            value
            for item in list(flip)[:2]
            if (value := finite_float(item)) is not None and value > 0
        ]
    elif (value := finite_float(flip)) is not None and value > 0:
        flip_values = [value]
    if len(flip_values) == 2:
        low, high = sorted(flip_values)
        levels["flip_low"] = low
        levels["flip_high"] = high
        flip_definition = "stable_flip_zone_boundaries"
    elif len(flip_values) == 1:
        levels["flip"] = flip_values[0]
        flip_definition = "stable_flip_level"
    else:
        zero_gamma = finite_float(structure.get("zero_gamma"))
        if zero_gamma is not None and zero_gamma > 0:
            levels["flip"] = zero_gamma
            flip_definition = "current_zero_gamma_fallback"

    if call_wall is not None and call_wall > 0:
        levels["call_wall"] = call_wall
    if "put_wall" not in levels or "call_wall" not in levels:
        return levels, "stable_wall_incomplete", flip_definition
    if not any(name.startswith("flip") for name in levels):
        return levels, "stable_flip_unavailable", flip_definition
    return levels, None, flip_definition


def _expiry_rows(
    options: Mapping[str, object],
) -> tuple[Mapping[str, object], Mapping[str, object]]:
    rows = [
        row
        for item in _sequence(options.get("expiries"))
        if (row := _mapping(item))
    ]
    return (
        rows[0] if rows else {},
        rows[1] if len(rows) > 1 else {},
    )


def _underlier(
    options: Mapping[str, object], structure: Mapping[str, object]
) -> float | None:
    underlier = options.get("underlier")
    if isinstance(underlier, Mapping):
        value = finite_float(underlier.get("price"))
    else:
        value = finite_float(getattr(underlier, "price", underlier))
    return value if value is not None else finite_float(structure.get("underlier"))


def _quotes_for_expiry(
    grouped_quotes: Mapping[str, Sequence[Quote]], expiry: str
) -> list[Quote]:
    if not expiry:
        return []
    return [
        quote
        for quote in grouped_quotes.get(expiry, ())
        if isinstance(quote, Quote)
        and quote.instrument.expiry == expiry
        and is_spxw_option(quote)
    ]


def _probability_unavailable_reason(
    *,
    level: float,
    underlier: float,
    pairs: Mapping[float, Mapping[OptionRight, Quote]],
    strike_step: float,
    source_strike: float | None,
    source_iv: float | None,
) -> str:
    if source_strike is not None and source_iv is None:
        return "real_iv_required_delta_fallback_rejected"
    if source_strike is not None:
        return "probability_calculation_unavailable"
    right = OptionRight.CALL if level >= underlier else OptionRight.PUT
    side = [
        (strike, quote)
        for strike, pair in pairs.items()
        if (quote := pair.get(right)) is not None
    ]
    if not side:
        return "real_iv_anchor_unavailable"
    delta_side = [
        (strike, quote)
        for strike, quote in side
        if usable_delta(quote) is not None
    ]
    if not delta_side:
        return "delta_anchor_unavailable"
    nearest_distance = min(abs(strike - level) for strike, _quote in delta_side)
    if nearest_distance > 2 * strike_step:
        return "level_anchor_out_of_range"
    return "probability_calculation_unavailable"


def _direction_right(direction: str) -> OptionRight | None:
    if direction == "up":
        return OptionRight.CALL
    if direction == "down":
        return OptionRight.PUT
    return None


def _horizons(values: Sequence[int]) -> tuple[tuple[int, ...], str | None]:
    parsed: list[int] = []
    for value in values:
        if isinstance(value, bool):
            return (), "invalid_horizons"
        try:
            minutes = int(value)
        except (TypeError, ValueError):
            return (), "invalid_horizons"
        if minutes <= 0 or float(value) != minutes:
            return (), "invalid_horizons"
        if minutes not in parsed:
            parsed.append(minutes)
    if not parsed:
        return (), "invalid_horizons"
    return tuple(parsed), None


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
        if isinstance(payload, Mapping):
            return payload
    return {}


def _sequence(value: object) -> Sequence[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return value
    return ()


def _token(value: object) -> str:
    if isinstance(value, Enum):
        value = value.value
    return str(value or "").strip().lower()
