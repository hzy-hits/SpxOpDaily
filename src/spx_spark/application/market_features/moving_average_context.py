"""Read-only ES RTH moving-average context and SPX coordinate projection."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc


SPX_MA_PROXY_NEAR_LINE_POINTS = 4.25


def moving_average_diagnostics(
    bars: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Compute read-only 5-minute RTH SMA20/50 without filling missing bars."""

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

    def window(period: int) -> tuple[float | None, str | None]:
        if len(rth) < period:
            return None, f"fewer_than_{period}_rth_bars"
        selected = rth[-period:]
        if any(bar.get("quality") != "ok" for bar in selected):
            return None, f"sma{period}_contains_non_ok_bar"
        for previous, current in zip(selected, selected[1:]):
            previous_start = _parse_at(previous.get("bar_start"))
            current_start = _parse_at(current.get("bar_start"))
            if previous_start is None or current_start is None:
                return None, f"sma{period}_timestamp_invalid"
            same_session = previous.get("trading_date_et") == current.get("trading_date_et")
            if same_session and (
                current_start != previous_start + timedelta(minutes=5)
                or current.get("gap_before") is True
            ):
                return None, f"sma{period}_rth_gap"
            if not same_session and not _valid_session_boundary(
                previous,
                current,
                previous_start=previous_start,
                current_start=current_start,
            ):
                return None, f"sma{period}_rth_session_boundary_gap"
        closes = [_number(bar.get("close")) for bar in selected]
        if any(value is None for value in closes):
            return None, f"sma{period}_close_missing"
        return statistics.fmean(value for value in closes if value is not None), None

    sma20, error20 = window(20)
    sma50, error50 = window(50)
    last = rth[-1] if rth else latest
    price = _number(last.get("close"))
    errors = [reason for reason in (error20, error50) if reason]
    return {
        "status": "ready" if not errors else "partial" if sma20 is not None else "warming",
        "timeframe": "5m",
        "session": "rth",
        "price": round(price, 2) if price is not None else None,
        "sma20": round(sma20, 2) if sma20 is not None else None,
        "sma50": round(sma50, 2) if sma50 is not None else None,
        "distance_to_sma20_points": (
            round(price - sma20, 2) if price is not None and sma20 is not None else None
        ),
        "distance_to_sma50_points": (
            round(price - sma50, 2) if price is not None and sma50 is not None else None
        ),
        "relation": _moving_average_relation(price, sma20, sma50),
        "latest_bar_end": last.get("bar_end"),
        "rth_bar_count": len(rth),
        "contract_identity": contract_identity,
        "reasons": (errors if contract_identity else ["es_contract_identity_unavailable", *errors]),
        "method": "simple_mean_of_closed_ok_rth_5m_bars_no_gap_fill",
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
    distance20 = _number(payload.get("distance_to_sma20_points"))
    distance50 = _number(payload.get("distance_to_sma50_points"))
    near_line = bool(
        qualified_basis is not None
        and any(
            distance is not None and abs(distance) <= SPX_MA_PROXY_NEAR_LINE_POINTS
            for distance in (distance20, distance50)
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
]
