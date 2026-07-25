"""Pure, fail-closed five-minute market-state scoring.

The kernel consumes exactly eight already-derived variables.  In particular,
``price_vs_vwap`` and ``opening_range_state`` carry the two-close confirmation
state; this module does not infer missing bars or substitute proxy inputs.
"""

from __future__ import annotations

import math
from datetime import datetime, time
from enum import Enum

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET


SCHEMA_VERSION = "market_state_5m.v1"
RULE_VERSION = "market_state_5m_eight_variable_rules.v2"
EARLIEST_CLASSIFICATION_ET = time(9, 45)

TREND_UP = "TREND_UP"
TREND_DOWN = "TREND_DOWN"
LOW_VOL_RANGE = "LOW_VOL_RANGE"
HIGH_VOL_CHOP = "HIGH_VOL_CHOP"
LOW_VOL_PIN = "LOW_VOL_PIN"
UNCERTAIN = "UNCERTAIN"


class PriceVsVwap(str, Enum):
    ABOVE_CONFIRMED = "ABOVE_CONFIRMED"
    ABOVE = "ABOVE"
    AROUND_OR_CROSS = "AROUND_OR_CROSS"
    BELOW = "BELOW"
    BELOW_CONFIRMED = "BELOW_CONFIRMED"


class OpeningRangeState(str, Enum):
    ABOVE_ORH_CONFIRMED = "ABOVE_ORH_CONFIRMED"
    BREAKOUT_ABOVE_ORH = "BREAKOUT_ABOVE_ORH"
    INSIDE = "INSIDE"
    BREAKDOWN_BELOW_ORL = "BREAKDOWN_BELOW_ORL"
    BELOW_ORL_CONFIRMED = "BELOW_ORL_CONFIRMED"


class MarketStructure(str, Enum):
    HH_HL = "HH_HL"
    HH_ONLY = "HH_ONLY"
    HL_ONLY = "HL_ONLY"
    OVERLAP = "OVERLAP"
    LH_ONLY = "LH_ONLY"
    LL_ONLY = "LL_ONLY"
    LH_LL = "LH_LL"


_PRICE_SCORES = {
    PriceVsVwap.ABOVE_CONFIRMED.value: 2,
    PriceVsVwap.ABOVE.value: 1,
    PriceVsVwap.AROUND_OR_CROSS.value: 0,
    PriceVsVwap.BELOW.value: -1,
    PriceVsVwap.BELOW_CONFIRMED.value: -2,
}
_OPENING_RANGE_SCORES = {
    OpeningRangeState.ABOVE_ORH_CONFIRMED.value: 2,
    OpeningRangeState.BREAKOUT_ABOVE_ORH.value: 1,
    OpeningRangeState.INSIDE.value: 0,
    OpeningRangeState.BREAKDOWN_BELOW_ORL.value: -1,
    OpeningRangeState.BELOW_ORL_CONFIRMED.value: -2,
}
_STRUCTURE_SCORES = {
    MarketStructure.HH_HL.value: 2,
    MarketStructure.HH_ONLY.value: 1,
    MarketStructure.HL_ONLY.value: 1,
    MarketStructure.OVERLAP.value: 0,
    MarketStructure.LH_ONLY.value: -1,
    MarketStructure.LL_ONLY.value: -1,
    MarketStructure.LH_LL.value: -2,
}
_REQUIRED_FIELDS = (
    "price_vs_vwap",
    "vwap_slope",
    "opening_range_state",
    "market_structure",
    "efficiency_ratio",
    "vwap_cross_count",
    "same_time_range_ratio",
    "breadth_above_vwap",
)
_DIRECTIONAL_FIELDS = tuple(
    field for field in _REQUIRED_FIELDS if field != "same_time_range_ratio"
)

__all__ = [
    "HIGH_VOL_CHOP",
    "LOW_VOL_PIN",
    "LOW_VOL_RANGE",
    "MarketStructure",
    "OpeningRangeState",
    "PriceVsVwap",
    "TREND_DOWN",
    "TREND_UP",
    "UNCERTAIN",
    "score_market_state_5m",
    "score_five_minute_market_state",
]


def score_market_state_5m(
    *,
    now: datetime,
    price_vs_vwap: str | PriceVsVwap | None,
    vwap_slope: float | None,
    opening_range_state: str | OpeningRangeState | None,
    market_structure: str | MarketStructure | None,
    efficiency_ratio: float | None,
    vwap_cross_count: int | None,
    same_time_range_ratio: float | None,
    breadth_above_vwap: float | None,
) -> dict[str, object]:
    """Score D/Q/V and classify one five-minute market state.

    ``vwap_slope`` is already normalized as
    ``(VWAP_t - VWAP_t-3) / ATR_5m``. ``same_time_range_ratio`` is the current
    range divided by the median same-time range of the prior 20 sessions.
    """

    price_token, price_error = _category(
        price_vs_vwap,
        _PRICE_SCORES,
        field="price_vs_vwap",
    )
    opening_token, opening_error = _category(
        opening_range_state,
        _OPENING_RANGE_SCORES,
        field="opening_range_state",
    )
    structure_token, structure_error = _category(
        market_structure,
        _STRUCTURE_SCORES,
        field="market_structure",
    )
    slope, slope_error = _number(vwap_slope, field="vwap_slope")
    efficiency, efficiency_error = _bounded_number(
        efficiency_ratio,
        field="efficiency_ratio",
        lower=0.0,
        upper=1.0,
    )
    crosses, crosses_error = _nonnegative_integer(
        vwap_cross_count,
        field="vwap_cross_count",
    )
    range_ratio, range_error = _bounded_number(
        same_time_range_ratio,
        field="same_time_range_ratio",
        lower=0.0,
    )
    breadth, breadth_error = _bounded_number(
        breadth_above_vwap,
        field="breadth_above_vwap",
        lower=0.0,
        upper=1.0,
    )
    values = {
        "price_vs_vwap": price_token,
        "vwap_slope": slope,
        "opening_range_state": opening_token,
        "market_structure": structure_token,
        "efficiency_ratio": efficiency,
        "vwap_cross_count": crosses,
        "same_time_range_ratio": range_ratio,
        "breadth_above_vwap": breadth,
    }
    errors = {
        "price_vs_vwap": price_error,
        "vwap_slope": slope_error,
        "opening_range_state": opening_error,
        "market_structure": structure_error,
        "efficiency_ratio": efficiency_error,
        "vwap_cross_count": crosses_error,
        "same_time_range_ratio": range_error,
        "breadth_above_vwap": breadth_error,
    }
    availability = {
        name: {
            "available": errors[name] is None,
            "value": values[name],
            "reason": errors[name],
        }
        for name in _REQUIRED_FIELDS
    }
    complete = all(row["available"] is True for row in availability.values())
    time_error, as_of, local_clock = _time_gate(now)

    component_scores: dict[str, int | None] = {
        "price_vs_vwap": (
            _PRICE_SCORES[price_token] if price_token is not None else None
        ),
        "vwap_slope": _vwap_slope_score(slope) if slope is not None else None,
        "opening_range_state": (
            _OPENING_RANGE_SCORES[opening_token]
            if opening_token is not None
            else None
        ),
        "market_structure": (
            _STRUCTURE_SCORES[structure_token]
            if structure_token is not None
            else None
        ),
        "breadth_above_vwap": (
            _breadth_score(breadth) if breadth is not None else None
        ),
    }
    direction_score = (
        sum(int(value) for value in component_scores.values())
        if all(value is not None for value in component_scores.values())
        else None
    )
    quality = _quality(efficiency, crosses)
    volatility = _volatility(range_ratio)
    directional_complete = all(
        availability[name]["available"] is True for name in _DIRECTIONAL_FIELDS
    )
    range_missing = range_error == "same_time_range_ratio_missing"
    can_classify_direction = (
        directional_complete
        and time_error is None
        and range_error in {None, "same_time_range_ratio_missing"}
    )
    can_classify_volatility = complete and time_error is None
    classification_tier = (
        "complete"
        if can_classify_volatility
        else "directional_provisional"
        if can_classify_direction and range_missing
        else "unavailable"
    )
    pin_proxy_candidate = bool(
        can_classify_volatility
        and direction_score is not None
        and abs(direction_score) <= 2
        and efficiency is not None
        and efficiency < 0.20
        and range_ratio is not None
        and range_ratio < 0.70
        and component_scores["price_vs_vwap"] == 0
        and component_scores["opening_range_state"] == 0
        and crosses is not None
        and crosses >= 2
    )
    state, state_reasons = _classify(
        direction_score,
        efficiency,
        crosses,
        range_ratio,
        can_classify_direction=can_classify_direction,
        can_classify_volatility=can_classify_volatility,
        pin_proxy_candidate=pin_proxy_candidate,
    )
    if (
        state in {TREND_UP, TREND_DOWN}
        and classification_tier == "directional_provisional"
    ):
        state_reasons.append(
            "directional_state_provisional_without_same_time_range_ratio"
        )
    gate_reasons = [
        reason
        for reason in (
            time_error,
            *(errors[name] for name in _REQUIRED_FIELDS),
        )
        if reason is not None
    ]
    reasons = list(dict.fromkeys([*gate_reasons, *state_reasons]))
    return {
        "schema_version": SCHEMA_VERSION,
        "rule_version": RULE_VERSION,
        "as_of": as_of,
        "as_of_et": local_clock,
        "state": state,
        "market_state": state,
        "D": direction_score,
        "Q": quality,
        "V": volatility,
        "direction_components": component_scores,
        "pin_proxy_candidate": pin_proxy_candidate,
        "pin_confirmation": (
            "proxy_unconfirmed" if pin_proxy_candidate else "not_applicable"
        ),
        "low_vol_pin_emission_allowed": False,
        "input_availability": {
            "required_count": len(_REQUIRED_FIELDS),
            "available_count": sum(
                row["available"] is True for row in availability.values()
            ),
            "complete": complete,
            "fields": availability,
        },
        "classification_tier": classification_tier,
        "status": (
            "ready"
            if state != UNCERTAIN and classification_tier == "complete"
            else "provisional"
            if state in {TREND_UP, TREND_DOWN}
            and classification_tier == "directional_provisional"
            else "uncertain"
        ),
        "reasons": reasons,
        "action_authority": "none",
        "actionable": False,
    }


score_five_minute_market_state = score_market_state_5m


def _classify(
    direction_score: int | None,
    efficiency: float | None,
    crosses: int | None,
    range_ratio: float | None,
    *,
    can_classify_direction: bool,
    can_classify_volatility: bool,
    pin_proxy_candidate: bool,
) -> tuple[str, list[str]]:
    if (
        not can_classify_direction
        or direction_score is None
        or efficiency is None
        or crosses is None
    ):
        return UNCERTAIN, ["classification_gate_failed"]
    if direction_score >= 6 and efficiency > 0.45 and crosses <= 2:
        return TREND_UP, [
            "direction_score_at_least_6",
            "efficiency_ratio_above_0_45",
            "vwap_cross_count_at_most_2",
        ]
    if direction_score <= -6 and efficiency > 0.45 and crosses <= 2:
        return TREND_DOWN, [
            "direction_score_at_most_minus_6",
            "efficiency_ratio_above_0_45",
            "vwap_cross_count_at_most_2",
        ]
    if not can_classify_volatility or range_ratio is None:
        return UNCERTAIN, ["volatility_classification_gate_failed"]
    if efficiency < 0.25 and range_ratio > 1.25:
        return HIGH_VOL_CHOP, [
            "efficiency_ratio_below_0_25",
            "same_time_range_ratio_above_1_25",
        ]
    if pin_proxy_candidate:
        return LOW_VOL_RANGE, [
            "low_vol_pin_proxy_unconfirmed_classified_as_range",
            "integer_strike_and_atm_straddle_confirmation_unavailable",
        ]
    if abs(direction_score) <= 2 and efficiency < 0.25 and range_ratio < 0.75:
        return LOW_VOL_RANGE, [
            "absolute_direction_score_at_most_2",
            "efficiency_ratio_below_0_25",
            "same_time_range_ratio_below_0_75",
        ]
    return UNCERTAIN, ["no_state_rule_matched"]


def _quality(
    efficiency: float | None,
    crosses: int | None,
) -> dict[str, object]:
    quality = None
    reason = "quality_inputs_unavailable"
    if efficiency is not None and crosses is not None:
        if efficiency < 0.25 or crosses >= 3:
            quality, reason = "chop", "er_below_0_25_or_crosses_at_least_3"
        elif efficiency > 0.65 and crosses <= 1:
            quality, reason = "high", "er_above_0_65_and_crosses_at_most_1"
        elif 0.45 <= efficiency <= 0.65 and crosses <= 2:
            quality, reason = "trend", "er_0_45_to_0_65_and_crosses_at_most_2"
        else:
            quality, reason = "mixed", "quality_rules_mixed"
    return {
        "quality": quality,
        "efficiency_ratio": efficiency,
        "vwap_cross_count": crosses,
        "reason": reason,
        "numeric_composite": None,
    }


def _volatility(range_ratio: float | None) -> dict[str, object]:
    state = None
    if range_ratio is not None:
        state = (
            "extreme"
            if range_ratio > 1.75
            else "high"
            if range_ratio > 1.25
            else "normal"
            if range_ratio >= 0.75
            else "low"
        )
    return {
        "state": state,
        "same_time_range_ratio": range_ratio,
        "baseline": "past_20_session_same_time_median",
    }


def _vwap_slope_score(value: float) -> int:
    if value > 0.30:
        return 2
    if value >= 0.05:
        return 1
    if value < -0.30:
        return -2
    if value <= -0.05:
        return -1
    return 0


def _breadth_score(value: float) -> int:
    if value > 0.65:
        return 2
    if value > 0.55:
        return 1
    if value >= 0.45:
        return 0
    if value >= 0.35:
        return -1
    return -2


def _category(
    value: str | Enum | None,
    scores: dict[str, int],
    *,
    field: str,
) -> tuple[str | None, str | None]:
    if value is None:
        return None, f"{field}_missing"
    token = str(value.value if isinstance(value, Enum) else value).strip().upper()
    if not token:
        return None, f"{field}_missing"
    if token not in scores:
        return None, f"{field}_invalid"
    return token, None


def _number(
    value: object,
    *,
    field: str,
) -> tuple[float | None, str | None]:
    if value is None:
        return None, f"{field}_missing"
    if isinstance(value, bool):
        return None, f"{field}_invalid"
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None, f"{field}_invalid"
    if not math.isfinite(parsed):
        return None, f"{field}_invalid"
    return parsed, None


def _bounded_number(
    value: object,
    *,
    field: str,
    lower: float,
    upper: float | None = None,
) -> tuple[float | None, str | None]:
    parsed, reason = _number(value, field=field)
    if reason is not None or parsed is None:
        return None, reason
    if parsed < lower or (upper is not None and parsed > upper):
        return None, f"{field}_out_of_range"
    return parsed, None


def _nonnegative_integer(
    value: object,
    *,
    field: str,
) -> tuple[int | None, str | None]:
    parsed, reason = _number(value, field=field)
    if reason is not None or parsed is None:
        return None, reason
    if parsed < 0 or not parsed.is_integer():
        return None, f"{field}_invalid"
    return int(parsed), None


def _time_gate(now: datetime) -> tuple[str | None, str | None, str | None]:
    if (
        not isinstance(now, datetime)
        or now.tzinfo is None
        or now.utcoffset() is None
    ):
        return "now_timezone_missing", None, None
    local = now.astimezone(ET)
    reason = (
        "outside_rth_session"
        if not DEFAULT_MARKET_CALENDAR.is_rth_open(now)
        else "before_0945_et"
        if local.time().replace(tzinfo=None) < EARLIEST_CLASSIFICATION_ET
        else None
    )
    return reason, now.isoformat(), local.isoformat()
