"""ATM / option replan helpers for the IBKR stream."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.ibkr.atm_reference import ReferenceQuote
from spx_spark.ibkr.stream.models import OptionSubscriptionPlan
from spx_spark.ibkr.verifier import VerifyRow, first_present, midpoint
from spx_spark.marketdata import parse_timestamp


def should_replan(
    plan: OptionSubscriptionPlan | None,
    atm_reference: float | None,
    *,
    replan_drift_points: float,
    today_expiry: str,
) -> bool:
    if atm_reference is None:
        return False
    if plan is None:
        return True
    if plan.expiry != today_expiry:
        return True
    return abs(atm_reference - plan.atm_strike) >= replan_drift_points


def estimate_spy_reference(rows: list[VerifyRow]) -> float | None:
    by_label = {row.label: row for row in rows}
    spy = by_label.get("stock:SPY")
    # Align with estimate_atm_reference: only explicitly fresh, non-delayed
    # rows may anchor a new option plan; stale/unknown rows fail closed.
    if spy is None or spy.stale is not False or spy.market_data_type in {3, 4}:
        return None
    price = first_present(spy.market_price, spy.last, midpoint(spy.bid, spy.ask), spy.close)
    if price:
        return price
    return None


def reference_quote_from_row(
    row: VerifyRow | None,
    *,
    contract: str | None = None,
    as_of: datetime | None = None,
) -> ReferenceQuote | None:
    if row is None:
        return None
    # Source-time synchronization matters for ES/SPX basis; transport-time
    # last_update_at can make unrelated source ticks look simultaneous.
    observed_at = parse_timestamp(row.ticker_time)
    decision_at = as_of or datetime.now(tz=timezone.utc)
    observed_in_future = bool(
        observed_at is not None
        and (observed_at - decision_at.astimezone(timezone.utc)).total_seconds() > 5.0
    )
    if row.market_data_type in {3, 4}:
        freshness = "delayed"
    elif row.market_data_type == 2:
        freshness = "frozen"
    elif row.market_data_type == 1:
        if observed_in_future:
            freshness = "unknown"
        elif row.stale is False and observed_at is not None:
            freshness = "fresh"
        elif row.stale is True:
            freshness = "stale"
        else:
            freshness = "unknown"
    else:
        freshness = "unknown"
    live_value = first_present(row.last, midpoint(row.bid, row.ask))
    if freshness == "fresh" and live_value is None:
        freshness = "close_only"
    reference_value = (
        first_present(row.close)
        if freshness == "stale"
        else live_value if live_value is not None else first_present(row.close)
    )
    return ReferenceQuote(
        value=reference_value,
        observed_at=observed_at,
        freshness=freshness,
        contract=contract,
    )

