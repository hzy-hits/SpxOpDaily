"""Freshness and tenor policy helpers for the wall-probability shadow."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta

from spx_spark.analytics.options.pricing import finite_float, option_iv
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    MarketDataQuality,
    OptionRight,
    Quote,
    as_utc,
    quality_from_market_data_type,
)


MIN_LIVE_BID_ASK_COVERAGE = 0.60
MIN_IV_COVERAGE = 0.60
RTH_MAX_LIVE_QUOTE_AGE_SECONDS = 15.0
GTH_MAX_LIVE_QUOTE_AGE_SECONDS = 90.0
RTH_MAX_INPUT_FRAME_AGE_SECONDS = 15.0
GTH_MAX_INPUT_FRAME_AGE_SECONDS = 90.0
TENOR_CUTOFF_ET = time(13, 0)


def input_freshness(
    value: object,
    *,
    now: datetime,
    label: str,
    max_age_seconds: float,
) -> dict[str, object]:
    observed_at = _datetime(value)
    if observed_at is None:
        return {
            "status": "unavailable",
            "observed_at": None,
            "age_seconds": None,
            "maximum_age_seconds": max_age_seconds,
            "reason": f"{label}_timestamp_missing",
        }
    age = (as_utc(now) - observed_at).total_seconds()
    if age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
        status = "unavailable"
        reason = f"{label}_future_timestamp"
    elif age > max_age_seconds:
        status = "stale"
        reason = f"{label}_stale"
    else:
        status = "fresh"
        reason = None
    return {
        "status": status,
        "observed_at": observed_at.isoformat(),
        "age_seconds": age,
        "maximum_age_seconds": max_age_seconds,
        "reason": reason,
    }


def live_two_sided(
    quote: Quote,
    *,
    now: datetime,
    max_age_seconds: float,
) -> bool:
    if quote.quality is not MarketDataQuality.LIVE:
        return False
    feed_mode = quality_from_market_data_type(quote.market_data_type)
    if feed_mode is not None and feed_mode is not MarketDataQuality.LIVE:
        return False
    age = quote_age_seconds(quote, now=now)
    return bool(
        _two_sided_values(quote)
        and age is not None
        and -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        <= age
        <= max_age_seconds
    )


def quote_age_seconds(quote: Quote, *, now: datetime) -> float | None:
    source_at = quote.quote_time or quote.trade_time or quote.received_at
    transport_at = quote.last_update_at or quote.received_at
    try:
        source_age = (as_utc(now) - as_utc(source_at)).total_seconds()
        transport_age = (as_utc(now) - as_utc(transport_at)).total_seconds()
    except (AttributeError, TypeError, ValueError):
        return None
    # The older clock controls freshness. A materially future timestamp in
    # either clock must still fail closed, so preserve that negative value.
    if (
        source_age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        or transport_age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
    ):
        return min(source_age, transport_age)
    return max(source_age, transport_age)


def tenor_eligibility(
    *,
    expiry: str | None,
    expected_expiry: str,
    expiry_contract_valid: bool,
    tenor: str,
    quotes: list[Quote],
    right: OptionRight | None,
    now: datetime,
    max_quote_age_seconds: float,
) -> dict[str, object]:
    relevant = [
        quote
        for quote in quotes
        if right is None or quote.instrument.right is right
    ]
    live = [quote for quote in relevant if quote.quality is MarketDataQuality.LIVE]
    quality_live_bid_ask = [
        quote for quote in live if _two_sided_values(quote)
    ]
    live_bid_ask = [
        quote
        for quote in quality_live_bid_ask
        if live_two_sided(
            quote,
            now=now,
            max_age_seconds=max_quote_age_seconds,
        )
    ]
    live_bid_ask_iv = [
        quote for quote in live_bid_ask if option_iv(quote) is not None
    ]
    quote_count = len(relevant)
    bid_ask_ratio = len(live_bid_ask) / quote_count if quote_count else 0.0
    iv_ratio = len(live_bid_ask_iv) / len(live_bid_ask) if live_bid_ask else 0.0
    reasons: list[str] = []
    if not expiry:
        reasons.append("expiry_unavailable")
    elif not expiry_contract_valid or expiry != expected_expiry:
        reasons.append(f"{tenor.lower()}_exact_expiry_mismatch")
    if right is None:
        reasons.append("direction_required")
    if quote_count == 0:
        reasons.append("directional_quotes_unavailable")
    if bid_ask_ratio < MIN_LIVE_BID_ASK_COVERAGE:
        reasons.append("live_bid_ask_coverage_insufficient")
        quality_bid_ask_ratio = (
            len(quality_live_bid_ask) / quote_count if quote_count else 0.0
        )
        if quality_bid_ask_ratio >= MIN_LIVE_BID_ASK_COVERAGE:
            reasons.append("quote_freshness_insufficient")
    if iv_ratio < MIN_IV_COVERAGE:
        reasons.append("real_iv_coverage_insufficient")
    eligible = not reasons
    return {
        "tenor": tenor,
        "expiry": expiry if expiry_contract_valid else None,
        "observed_expiry": expiry,
        "expected_expiry": expected_expiry,
        "expiry_contract_valid": expiry_contract_valid,
        "right": right.value if right else None,
        "quote_count": quote_count,
        "live_quote_count": len(live),
        "quality_live_bid_ask_count": len(quality_live_bid_ask),
        "live_bid_ask_count": len(live_bid_ask),
        "live_bid_ask_iv_count": len(live_bid_ask_iv),
        "live_bid_ask_coverage_ratio": bid_ask_ratio,
        "iv_coverage_ratio": iv_ratio,
        "minimum_live_bid_ask_coverage_ratio": MIN_LIVE_BID_ASK_COVERAGE,
        "minimum_iv_coverage_ratio": MIN_IV_COVERAGE,
        "maximum_live_quote_age_seconds": max_quote_age_seconds,
        "maximum_observed_quote_age_seconds": _maximum_quote_age(
            relevant,
            now=now,
        ),
        "eligible": eligible,
        "reasons": reasons,
    }


def tenor_plan_by_horizon(
    *,
    local: datetime,
    horizons: tuple[int, ...],
    eligibility: Mapping[str, Mapping[str, object]],
    front_required: bool,
    prior_available: bool,
) -> dict[str, dict[str, object]]:
    cutoff = datetime.combine(local.date(), TENOR_CUTOFF_ET, tzinfo=ET)
    result: dict[str, dict[str, object]] = {}
    for horizon in horizons:
        planned_exit = local + timedelta(minutes=horizon)
        preferred = (
            ("1DTE" if planned_exit <= cutoff else "0DTE")
            if prior_available
            else None
        )
        holding = {
            tenor: _holding_window(
                expiry=str(eligibility[tenor].get("expiry") or ""),
                planned_exit=planned_exit,
            )
            for tenor in ("0DTE", "1DTE")
        }
        selected, fallback_used = (
            _select_tenor(
                preferred,
                eligibility,
                holding=holding,
                front_required=front_required,
            )
            if preferred is not None
            else (None, False)
        )
        selected_expiry = (
            eligibility[selected].get("expiry") if selected is not None else None
        )
        local_reasons: list[str] = []
        if not prior_available:
            local_reasons.append("rth_required_for_tenor_prior")
        elif selected is None:
            holding_failures = [
                row["reason"]
                for tenor, row in holding.items()
                if eligibility[tenor].get("eligible") is True
                and row["holding_window_valid"] is not True
            ]
            local_reasons.extend(str(reason) for reason in holding_failures)
            if not holding_failures:
                local_reasons.append("no_expression_tenor_with_live_bid_ask_iv")
        selection_reason = "preferred_available"
        if not prior_available:
            selection_reason = "rth_tenor_prior_unavailable"
        elif fallback_used and preferred is not None:
            preferred_holding = holding[preferred]
            selection_reason = (
                "preferred_holding_window_crosses_expiry_fallback"
                if preferred_holding["holding_window_valid"] is not True
                else "preferred_quote_coverage_unavailable_fallback"
            )
        elif selected is None:
            selection_reason = "no_eligible_tenor"
        result[f"{horizon}m"] = {
            "horizon_minutes": horizon,
            "planned_exit_at": planned_exit.isoformat(),
            "planned_exit_at_or_before_cutoff": (
                planned_exit <= cutoff if prior_available else None
            ),
            "crosses_13_et_cutoff": (
                planned_exit > cutoff if prior_available else None
            ),
            "preferred_tenor": preferred,
            "selected_tenor": selected,
            "selected_expiry": selected_expiry,
            "fallback_used": fallback_used,
            "selection_reason": selection_reason,
            "holding_windows": holding,
            "reasons": list(dict.fromkeys(local_reasons)),
        }
    return result


def summary_tenor(values: Sequence[str]) -> str | None:
    unique = set(values)
    if not unique:
        return None
    return next(iter(unique)) if len(unique) == 1 else "mixed"


def expiry_close(expiry: str) -> datetime | None:
    try:
        day = datetime.strptime(expiry, "%Y%m%d").date()
    except (TypeError, ValueError):
        return None
    session = DEFAULT_MARKET_CALENDAR.session(day)
    return session.close_at if session is not None else None


def tenor_market_snapshot(
    *,
    front_row: Mapping[str, object],
    next_row: Mapping[str, object],
    volatility: Mapping[str, object],
    eligibility: Mapping[str, Mapping[str, object]],
    front_contract_valid: bool,
    next_contract_valid: bool,
) -> dict[str, object]:
    front_atm_iv = (
        _first_number(
            front_row.get("atm_iv"),
            volatility.get("atm_iv_0dte"),
        )
        if front_contract_valid
        else None
    )
    next_atm_iv = (
        _first_number(
            next_row.get("atm_iv"),
            volatility.get("atm_iv_1dte"),
        )
        if next_contract_valid
        else None
    )
    term_gap = (
        _first_number(volatility.get("term_gap"))
        if front_contract_valid and next_contract_valid
        else None
    )
    if term_gap is None and front_atm_iv is not None and next_atm_iv is not None:
        term_gap = front_atm_iv - next_atm_iv

    def tenor_row(
        row: Mapping[str, object],
        *,
        tenor: str,
        atm_iv: float | None,
        expected_move_fallback: object = None,
    ) -> dict[str, object]:
        quote_coverage = eligibility[tenor]
        map_coverage = _mapping(row.get("coverage"))
        contract_valid = quote_coverage.get("expiry_contract_valid") is True
        coverage = {
            "directional_quote_count": quote_coverage.get("quote_count"),
            "directional_live_quote_count": quote_coverage.get(
                "live_quote_count"
            ),
            "directional_live_bid_ask_count": quote_coverage.get(
                "live_bid_ask_count"
            ),
            "directional_live_bid_ask_iv_count": quote_coverage.get(
                "live_bid_ask_iv_count"
            ),
            "directional_live_bid_ask_ratio": quote_coverage.get(
                "live_bid_ask_coverage_ratio"
            ),
            "directional_iv_ratio": quote_coverage.get("iv_coverage_ratio"),
            "map_total": _integer(map_coverage.get("total")),
            "map_live": _integer(map_coverage.get("live")),
            "map_with_bid_ask": _integer(map_coverage.get("with_bid_ask")),
            "map_with_iv": _integer(map_coverage.get("with_iv")),
        }
        if not contract_valid:
            coverage = {key: None for key in coverage}
        return {
            "tenor": tenor,
            "expiry": (
                quote_coverage.get("expiry") if contract_valid else None
            ),
            "expected_expiry": quote_coverage.get("expected_expiry"),
            "expiry_contract_valid": contract_valid,
            "atm_iv": atm_iv,
            "expected_move_points": _first_number(
                row.get("expected_move_points"),
                expected_move_fallback,
            )
            if contract_valid
            else None,
            "expected_move_pct": (
                finite_float(row.get("expected_move_pct"))
                if contract_valid
                else None
            ),
            "coverage": coverage,
        }

    return {
        "0DTE": tenor_row(
            front_row,
            tenor="0DTE",
            atm_iv=front_atm_iv,
            expected_move_fallback=volatility.get("expected_move_points_0dte"),
        ),
        "1DTE": tenor_row(
            next_row,
            tenor="1DTE",
            atm_iv=next_atm_iv,
        ),
        "term_gap_0dte_minus_1dte": term_gap,
        "term_gap_definition": "atm_iv_0dte_minus_atm_iv_1dte",
    }


def _holding_window(*, expiry: str, planned_exit: datetime) -> dict[str, object]:
    close = expiry_close(expiry)
    valid = close is not None and planned_exit <= close
    return {
        "expiry": expiry or None,
        "expiry_close_at": close.isoformat() if close else None,
        "holding_window_valid": valid,
        "minutes_before_expiry_at_planned_exit": (
            (close - planned_exit).total_seconds() / 60.0 if close else None
        ),
        "reason": (
            None
            if valid
            else "expiry_session_unavailable"
            if close is None
            else "holding_window_crosses_expiry"
        ),
    }


def _select_tenor(
    preferred: str,
    eligibility: Mapping[str, Mapping[str, object]],
    *,
    holding: Mapping[str, Mapping[str, object]],
    front_required: bool,
) -> tuple[str | None, bool]:
    if not front_required:
        return None, False

    def selectable(tenor: str) -> bool:
        return bool(
            eligibility[tenor].get("eligible") is True
            and holding[tenor].get("holding_window_valid") is True
        )

    if selectable(preferred):
        return preferred, False
    fallback = "0DTE" if preferred == "1DTE" else "1DTE"
    if selectable(fallback):
        return fallback, True
    return None, False


def _two_sided_values(quote: Quote) -> bool:
    bid = finite_float(quote.bid)
    ask = finite_float(quote.ask)
    return bool(
        bid is not None
        and ask is not None
        and bid >= 0.0
        and ask > 0.0
        and ask >= bid
    )


def _maximum_quote_age(
    quotes: Sequence[Quote],
    *,
    now: datetime,
) -> float | None:
    ages = [
        age
        for quote in quotes
        if (age := quote_age_seconds(quote, now=now)) is not None
    ]
    return max(ages) if ages else None


def _first_number(*values: object) -> float | None:
    for value in values:
        parsed = finite_float(value)
        if parsed is not None:
            return parsed
    return None


def _integer(value: object) -> int | None:
    parsed = finite_float(value)
    return int(parsed) if parsed is not None else None


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return as_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}
