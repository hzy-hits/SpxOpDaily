"""Read-only ES RTH moving-average context and SPX coordinate projection."""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc


SPX_MA_PROXY_NEAR_LINE_POINTS = 4.25
MA_NEAR_SIDE_ATR = 0.10
MA_SLOPE_EPSILON_ATR = 0.02
MA50_EXTENSION_ATR = 2.00
MA200_EXTENSION_ATR = 4.00
MA_FRESH_CROSS_MAX_BARS = 6
MA_CROSS_PERSISTENCE_BARS = 2
RTH_ATR_PERIODS = 14
RTH_ATR_MIN_PERIODS = 6
RTH_ATR_METHOD = (
    "simple_mean_session_aware_rth_true_range_excludes_overnight_gap_no_gap_fill"
)


def rth_atr_5m(
    bars: Sequence[Mapping[str, object]],
    *,
    periods: int = RTH_ATR_PERIODS,
    minimum_periods: int = RTH_ATR_MIN_PERIODS,
) -> tuple[float | None, dict[str, object]]:
    """Return causal ATR from closed RTH bars without overnight gap returns."""

    all_rth = sorted(
        (bar for bar in bars if bar.get("segment") == "rth"),
        key=lambda row: str(row.get("bar_start") or ""),
    )
    latest_identity = all_rth[-1].get("contract_identity") if all_rth else None
    rth = (
        [
            bar
            for bar in all_rth
            if isinstance(latest_identity, str)
            and latest_identity
            and bar.get("contract_identity") == latest_identity
        ]
        if latest_identity
        else []
    )
    true_ranges: list[float] = []
    previous: Mapping[str, object] | None = None
    session_count = 0
    last_session: object = None
    reset_count = 0
    for current in rth:
        start = _parse_at(current.get("bar_start"))
        high = _number(current.get("high"))
        low = _number(current.get("low"))
        if (
            current.get("quality") != "ok"
            or start is None
            or high is None
            or low is None
        ):
            true_ranges = []
            previous = None
            reset_count += 1
            continue
        current_session = current.get("trading_date_et")
        if current_session != last_session:
            session_count += 1
            last_session = current_session

        prior_close: float | None = None
        if previous is not None:
            previous_start = _parse_at(previous.get("bar_start"))
            same_session = previous.get("trading_date_et") == current_session
            continuous = bool(
                previous_start is not None
                and (
                    (
                        same_session
                        and start == previous_start + timedelta(minutes=5)
                        and current.get("gap_before") is not True
                    )
                    or (
                        not same_session
                        and _valid_session_boundary(
                            previous,
                            current,
                            previous_start=previous_start,
                            current_start=start,
                        )
                    )
                )
            )
            if not continuous:
                true_ranges = []
                reset_count += 1
            elif same_session:
                prior_close = _number(previous.get("close"))
        intrabar_range = high - low
        true_range = (
            max(intrabar_range, abs(high - prior_close), abs(low - prior_close))
            if prior_close is not None
            else intrabar_range
        )
        true_ranges.append(true_range)
        previous = current

    window = true_ranges[-periods:]
    value = statistics.fmean(window) if len(window) >= minimum_periods else None
    return value, {
        "value": value,
        "periods_used": len(window),
        "target_periods": periods,
        "minimum_periods": minimum_periods,
        "method": RTH_ATR_METHOD,
        "session": "rth",
        "overnight_gap_included": False,
        "contract_identity": latest_identity,
        "rth_bar_count": len(rth),
        "session_count": session_count,
        "continuity_reset_count": reset_count,
    }


def moving_average_diagnostics(
    bars: Sequence[Mapping[str, object]],
    *,
    atr_5m: float | None = None,
) -> dict[str, object]:
    """Compute closed-bar RTH moving-average context without filling gaps."""

    all_rth = sorted(
        (bar for bar in bars if bar.get("segment") == "rth"),
        key=lambda row: str(row.get("bar_start") or ""),
    )
    latest = all_rth[-1] if all_rth else {}
    contract_identity = latest.get("contract_identity")
    rth = (
        [
            bar
            for bar in all_rth
            if isinstance(contract_identity, str)
            and contract_identity
            and bar.get("contract_identity") == contract_identity
        ]
        if contract_identity
        else []
    )

    def window(period: int, *, offset: int = 0) -> tuple[float | None, str | None]:
        end = len(rth) - offset
        if end < period:
            return None, f"fewer_than_{period + offset}_rth_bars"
        selected = rth[end - period : end]
        error = _window_error(selected, period=period)
        if error is not None:
            return None, error
        closes = [_number(bar.get("close")) for bar in selected]
        if any(value is None for value in closes):
            return None, f"sma{period}_close_missing"
        return statistics.fmean(value for value in closes if value is not None), None

    sma20, error20 = window(20)
    sma50, error50 = window(50)
    sma200, error200 = window(200)
    sma50_3, error50_3 = window(50, offset=3)
    sma50_6, error50_6 = window(50, offset=6)
    sma200_3, error200_3 = window(200, offset=3)
    sma200_6, error200_6 = window(200, offset=6)
    last = rth[-1] if rth else latest
    price = _number(last.get("close"))
    computed_atr, atr_diagnostics = rth_atr_5m(bars)
    override_atr = _number(atr_5m)
    atr = override_atr if override_atr is not None else computed_atr
    if atr is not None and atr <= 0:
        atr = None

    distance50 = price - sma50 if price is not None and sma50 is not None else None
    distance200 = price - sma200 if price is not None and sma200 is not None else None
    spread = sma50 - sma200 if sma50 is not None and sma200 is not None else None
    distance50_atr = _ratio(distance50, atr)
    distance200_atr = _ratio(distance200, atr)
    spread_atr = _ratio(spread, atr)
    ma50_slope_3_atr = _change_atr(sma50, sma50_3, atr)
    ma50_slope_6_atr = _change_atr(sma50, sma50_6, atr)
    ma200_slope_3_atr = _change_atr(sma200, sma200_3, atr)
    ma200_slope_6_atr = _change_atr(sma200, sma200_6, atr)
    spread_3 = (
        sma50_3 - sma200_3
        if sma50_3 is not None and sma200_3 is not None
        else None
    )
    spread_change_3_atr = _change_atr(spread, spread_3, atr)
    (
        cross_direction,
        bars_since_cross,
        cross_persistent_2_bars,
        cross_fresh,
    ) = _cross_diagnostics(rth)
    regime_state, regime_direction, same_direction_convexity = _ma_regime(
        distance50_atr=distance50_atr,
        distance200_atr=distance200_atr,
        spread_atr=spread_atr,
        ma50_slope_3_atr=ma50_slope_3_atr,
        ma50_slope_6_atr=ma50_slope_6_atr,
        ma200_slope_3_atr=ma200_slope_3_atr,
        ma200_slope_6_atr=ma200_slope_6_atr,
        spread_change_3_atr=spread_change_3_atr,
    )
    errors = _unique(
        reason
        for reason in (
            error20,
            error50,
            error200,
            error50_3,
            error50_6,
            error200_3,
            error200_6,
            "atr_5m_unavailable" if atr is None else None,
        )
        if reason
    )
    regime_ready = regime_state is not None
    return {
        "status": (
            "ready"
            if not errors and regime_ready
            else "partial"
            if sma20 is not None
            else "warming"
        ),
        "timeframe": "5m",
        "session": "rth",
        "price": round(price, 2) if price is not None else None,
        "sma20": round(sma20, 2) if sma20 is not None else None,
        "sma50": round(sma50, 2) if sma50 is not None else None,
        "sma200": round(sma200, 2) if sma200 is not None else None,
        "distance_to_sma20_points": (
            round(price - sma20, 2) if price is not None and sma20 is not None else None
        ),
        "distance_to_sma50_points": (
            round(distance50, 2) if distance50 is not None else None
        ),
        "distance_to_sma200_points": (
            round(distance200, 2) if distance200 is not None else None
        ),
        "atr_5m": round(atr, 2) if atr is not None else None,
        "atr_5m_source": (
            "explicit_test_or_research_override"
            if override_atr is not None
            else "shared_session_aware_rth_atr"
        ),
        "atr_5m_method": (
            "explicit_override"
            if override_atr is not None
            else atr_diagnostics["method"]
        ),
        "atr_5m_periods_used": (
            None if override_atr is not None else atr_diagnostics["periods_used"]
        ),
        "atr_5m_overnight_gap_included": False,
        "distance_to_sma50_atr": _rounded(distance50_atr),
        "distance_to_sma200_atr": _rounded(distance200_atr),
        "ma50_ma200_spread_points": round(spread, 2) if spread is not None else None,
        "ma50_ma200_spread_atr": _rounded(spread_atr),
        "ma50_slope_3_atr": _rounded(ma50_slope_3_atr),
        "ma50_slope_6_atr": _rounded(ma50_slope_6_atr),
        "ma200_slope_3_atr": _rounded(ma200_slope_3_atr),
        "ma200_slope_6_atr": _rounded(ma200_slope_6_atr),
        "spread_change_3_atr": _rounded(spread_change_3_atr),
        "cross_direction": cross_direction,
        "bars_since_cross": bars_since_cross,
        "cross_persistent_2_bars": cross_persistent_2_bars,
        "cross_fresh": cross_fresh,
        "regime_state": regime_state,
        "regime_direction": regime_direction,
        "same_direction_convexity": same_direction_convexity,
        "thresholds": {
            "near_side_atr": MA_NEAR_SIDE_ATR,
            "slope_epsilon_atr": MA_SLOPE_EPSILON_ATR,
            "extension_distance_to_ma50_atr": MA50_EXTENSION_ATR,
            "extension_distance_to_ma200_atr": MA200_EXTENSION_ATR,
            "fresh_cross_max_bars": MA_FRESH_CROSS_MAX_BARS,
            "cross_persistence_bars": MA_CROSS_PERSISTENCE_BARS,
        },
        "relation": _moving_average_relation(price, sma20, sma50),
        "latest_bar_end": last.get("bar_end"),
        "rth_bar_count": len(rth),
        "contract_identity": contract_identity,
        "reasons": (errors if contract_identity else ["es_contract_identity_unavailable", *errors]),
        "method": (
            "simple_mean_of_closed_ok_rth_5m_bars_no_gap_fill;"
            f"atr={RTH_ATR_METHOD}"
        ),
        "action_authority": "none",
    }


def project_spx_equivalent_moving_averages(
    moving_averages: Mapping[str, object],
    *,
    es_spx_basis_points: float | None,
    basis_contract_identity: str | None,
) -> dict[str, object]:
    """Project ES SMA levels into SPX coordinates using one synchronized basis."""

    payload = dict(moving_averages)
    basis = _number(es_spx_basis_points)
    moving_identity = payload.get("contract_identity")
    identity_matches = bool(
        isinstance(moving_identity, str)
        and moving_identity
        and isinstance(basis_contract_identity, str)
        and basis_contract_identity
        and moving_identity == basis_contract_identity
    )
    qualified_basis = basis if identity_matches else None
    sma20 = _number(payload.get("sma20"))
    sma50 = _number(payload.get("sma50"))
    sma200 = _number(payload.get("sma200"))
    distance20 = _number(payload.get("distance_to_sma20_points"))
    distance50 = _number(payload.get("distance_to_sma50_points"))
    distance200 = _number(payload.get("distance_to_sma200_points"))
    near_line = bool(
        qualified_basis is not None
        and any(
            distance is not None and abs(distance) <= SPX_MA_PROXY_NEAR_LINE_POINTS
            for distance in (distance20, distance50, distance200)
        )
    )
    payload.update(
        {
            "es_spx_basis_points": round(basis, 2) if basis is not None else None,
            "basis_contract_identity": basis_contract_identity,
            "basis_contract_identity_matches_sma": identity_matches,
            "spx_equivalent_sma20": (
                round(sma20 - qualified_basis, 2)
                if sma20 is not None and qualified_basis is not None
                else None
            ),
            "spx_equivalent_sma50": (
                round(sma50 - qualified_basis, 2)
                if sma50 is not None and qualified_basis is not None
                else None
            ),
            "spx_equivalent_sma200": (
                round(sma200 - qualified_basis, 2)
                if sma200 is not None and qualified_basis is not None
                else None
            ),
            "projection_method": (
                "es_sma_minus_synchronized_current_basis_not_cash_spx_sma"
                if qualified_basis is not None
                else (
                    "unavailable_without_synchronized_basis"
                    if basis is None
                    else "unavailable_basis_contract_identity_mismatch"
                )
            ),
            "spx_projection_near_line": near_line,
            "spx_projection_near_line_tolerance_points": SPX_MA_PROXY_NEAR_LINE_POINTS,
        }
    )
    return payload


def _window_error(
    selected: Sequence[Mapping[str, object]],
    *,
    period: int,
) -> str | None:
    if any(bar.get("quality") != "ok" for bar in selected):
        return f"sma{period}_contains_non_ok_bar"
    for previous, current in zip(selected, selected[1:]):
        previous_start = _parse_at(previous.get("bar_start"))
        current_start = _parse_at(current.get("bar_start"))
        if previous_start is None or current_start is None:
            return f"sma{period}_timestamp_invalid"
        same_session = previous.get("trading_date_et") == current.get("trading_date_et")
        if same_session and (
            current_start != previous_start + timedelta(minutes=5)
            or current.get("gap_before") is True
        ):
            return f"sma{period}_rth_gap"
        if not same_session and not _valid_session_boundary(
            previous,
            current,
            previous_start=previous_start,
            current_start=current_start,
        ):
            return f"sma{period}_rth_session_boundary_gap"
    return None


def _cross_diagnostics(
    rth: Sequence[Mapping[str, object]],
) -> tuple[str | None, int | None, bool | None, bool | None]:
    tail = _continuous_ok_tail(rth)
    if len(tail) < 201:
        return None, None, None, None
    closes = [_number(bar.get("close")) for bar in tail]
    if any(value is None for value in closes):
        return None, None, None, None
    values = [value for value in closes if value is not None]
    spreads: list[float] = []
    endpoints: list[int] = []
    for end in range(200, len(values) + 1):
        sma50 = statistics.fmean(values[end - 50 : end])
        sma200 = statistics.fmean(values[end - 200 : end])
        spreads.append(sma50 - sma200)
        endpoints.append(end - 1)

    latest_direction: str | None = None
    latest_endpoint: int | None = None
    for prior, current, endpoint in zip(spreads, spreads[1:], endpoints[1:]):
        if prior <= 0 < current:
            latest_direction = "golden"
            latest_endpoint = endpoint
        elif prior >= 0 > current:
            latest_direction = "death"
            latest_endpoint = endpoint
    if latest_direction is None or latest_endpoint is None:
        return None, None, None, None
    age = endpoints[-1] - latest_endpoint
    side = 1 if latest_direction == "golden" else -1
    persistent = len(spreads) >= MA_CROSS_PERSISTENCE_BARS and all(
        spread * side > 0 for spread in spreads[-MA_CROSS_PERSISTENCE_BARS:]
    )
    return (
        latest_direction,
        age,
        persistent,
        age <= MA_FRESH_CROSS_MAX_BARS,
    )


def _continuous_ok_tail(
    rows: Sequence[Mapping[str, object]],
) -> list[Mapping[str, object]]:
    tail: list[Mapping[str, object]] = []
    for current in rows:
        if current.get("quality") != "ok" or _number(current.get("close")) is None:
            tail = []
            continue
        if tail:
            previous = tail[-1]
            previous_start = _parse_at(previous.get("bar_start"))
            current_start = _parse_at(current.get("bar_start"))
            if (
                previous_start is None
                or current_start is None
                or not _continuous_pair(
                    previous,
                    current,
                    previous_start=previous_start,
                    current_start=current_start,
                )
            ):
                tail = []
        tail.append(current)
    return tail


def _continuous_pair(
    previous: Mapping[str, object],
    current: Mapping[str, object],
    *,
    previous_start: datetime,
    current_start: datetime,
) -> bool:
    if previous.get("trading_date_et") == current.get("trading_date_et"):
        return bool(
            current_start == previous_start + timedelta(minutes=5)
            and current.get("gap_before") is not True
        )
    return _valid_session_boundary(
        previous,
        current,
        previous_start=previous_start,
        current_start=current_start,
    )


def _ma_regime(
    *,
    distance50_atr: float | None,
    distance200_atr: float | None,
    spread_atr: float | None,
    ma50_slope_3_atr: float | None,
    ma50_slope_6_atr: float | None,
    ma200_slope_3_atr: float | None,
    ma200_slope_6_atr: float | None,
    spread_change_3_atr: float | None,
) -> tuple[str | None, str | None, str | None]:
    inputs = (
        distance50_atr,
        distance200_atr,
        spread_atr,
        ma50_slope_3_atr,
        ma50_slope_6_atr,
        ma200_slope_3_atr,
        ma200_slope_6_atr,
        spread_change_3_atr,
    )
    if any(value is None for value in inputs):
        return None, None, None
    assert distance50_atr is not None
    assert distance200_atr is not None
    assert spread_atr is not None
    assert ma50_slope_3_atr is not None
    assert ma50_slope_6_atr is not None
    assert ma200_slope_3_atr is not None
    assert ma200_slope_6_atr is not None
    assert spread_change_3_atr is not None

    aligned_up = bool(
        distance50_atr >= MA_NEAR_SIDE_ATR
        and spread_atr >= MA_NEAR_SIDE_ATR
        and ma50_slope_3_atr >= MA_SLOPE_EPSILON_ATR
        and ma50_slope_6_atr >= MA_SLOPE_EPSILON_ATR
        and ma200_slope_3_atr >= MA_SLOPE_EPSILON_ATR
        and ma200_slope_6_atr >= MA_SLOPE_EPSILON_ATR
    )
    aligned_down = bool(
        distance50_atr <= -MA_NEAR_SIDE_ATR
        and spread_atr <= -MA_NEAR_SIDE_ATR
        and ma50_slope_3_atr <= -MA_SLOPE_EPSILON_ATR
        and ma50_slope_6_atr <= -MA_SLOPE_EPSILON_ATR
        and ma200_slope_3_atr <= -MA_SLOPE_EPSILON_ATR
        and ma200_slope_6_atr <= -MA_SLOPE_EPSILON_ATR
    )
    if aligned_up or aligned_down:
        direction = "up" if aligned_up else "down"
        sign = 1.0 if aligned_up else -1.0
        extended = bool(
            sign * distance50_atr >= MA50_EXTENSION_ATR
            or sign * distance200_atr >= MA200_EXTENSION_ATR
        )
        return (
            "TREND_EXTENDED" if extended else "TREND_ALIGNED",
            direction,
            "do_not_chase" if extended else "confluence_only",
        )

    transition_up = bool(
        spread_atr <= -MA_NEAR_SIDE_ATR
        and distance50_atr >= MA_NEAR_SIDE_ATR
        and ma50_slope_3_atr >= MA_SLOPE_EPSILON_ATR
        and spread_change_3_atr >= MA_SLOPE_EPSILON_ATR
    )
    transition_down = bool(
        spread_atr >= MA_NEAR_SIDE_ATR
        and distance50_atr <= -MA_NEAR_SIDE_ATR
        and ma50_slope_3_atr <= -MA_SLOPE_EPSILON_ATR
        and spread_change_3_atr <= -MA_SLOPE_EPSILON_ATR
    )
    if transition_up or transition_down:
        return (
            "REGIME_TRANSITION",
            "up" if transition_up else "down",
            "wait_for_wall_confirmation",
        )
    return "MIXED", None, "wait_for_wall_confirmation"


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _change_atr(
    current: float | None,
    previous: float | None,
    atr: float | None,
) -> float | None:
    if current is None or previous is None:
        return None
    return _ratio(current - previous, atr)


def _rounded(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _unique(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if isinstance(value, str) and value not in result:
            result.append(value)
    return result


def _valid_session_boundary(
    previous: Mapping[str, object],
    current: Mapping[str, object],
    *,
    previous_start: datetime,
    current_start: datetime,
) -> bool:
    previous_day = _date(previous.get("trading_date_et"))
    current_day = _date(current.get("trading_date_et"))
    previous_session = (
        DEFAULT_MARKET_CALENDAR.session(previous_day) if previous_day is not None else None
    )
    current_session = (
        DEFAULT_MARKET_CALENDAR.session(current_day) if current_day is not None else None
    )
    return bool(
        previous_day is not None
        and current_day is not None
        and previous_session is not None
        and current_session is not None
        and DEFAULT_MARKET_CALENDAR.next_trading_day(previous_day) == current_day
        and previous_start == previous_session.close_at - timedelta(minutes=5)
        and current_start == current_session.open_at
    )


def _moving_average_relation(
    price: float | None,
    sma20: float | None,
    sma50: float | None,
) -> str | None:
    if price is None or sma20 is None or sma50 is None:
        return None
    if price > sma20 > sma50:
        return "bullish_stack"
    if price < sma20 < sma50:
        return "bearish_stack"
    if price > max(sma20, sma50):
        return "price_above_both_mixed_order"
    if price < min(sma20, sma50):
        return "price_below_both_mixed_order"
    return "price_between_sma20_sma50"


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _parse_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return as_utc(parsed)


def _date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


__all__ = [
    "moving_average_diagnostics",
    "project_spx_equivalent_moving_averages",
    "rth_atr_5m",
]
