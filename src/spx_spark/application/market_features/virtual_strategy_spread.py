"""Exact two-leg snapshot calculations for virtual debit spreads."""

from __future__ import annotations

from datetime import datetime
from typing import Mapping

from spx_spark.application.market_features.virtual_strategy_support import (
    _contract_snapshot,
    _number,
    _time,
)
from spx_spark.storage import LatestState


def spread_snapshot(
    latest: LatestState,
    *,
    long_contract_id: str,
    short_contract_id: str,
    now: datetime,
    max_quote_age_seconds: float,
    max_quote_skew_seconds: float,
    required_provider: str | None = None,
    contract_snapshot=_contract_snapshot,
) -> dict[str, object]:
    """Mark a 1x/-1x debit spread from two simultaneously usable leg snapshots."""

    snapshot, _reasons = spread_snapshot_decision(
        latest,
        long_contract_id=long_contract_id,
        short_contract_id=short_contract_id,
        now=now,
        max_quote_age_seconds=max_quote_age_seconds,
        max_quote_skew_seconds=max_quote_skew_seconds,
        required_provider=required_provider,
        contract_snapshot=contract_snapshot,
    )
    return snapshot


def spread_snapshot_decision(
    latest: LatestState,
    *,
    long_contract_id: str,
    short_contract_id: str,
    now: datetime,
    max_quote_age_seconds: float,
    max_quote_skew_seconds: float,
    required_provider: str | None = None,
    contract_snapshot=_contract_snapshot,
) -> tuple[dict[str, object], list[str]]:
    """Return an exact two-leg snapshot or stable, auditable rejection reasons."""

    if not long_contract_id or not short_contract_id:
        return {}, ["spread_contract_id_unavailable"]
    long = contract_snapshot(latest, long_contract_id, now=now)
    short = contract_snapshot(latest, short_contract_id, now=now)
    missing_reasons = []
    if not long:
        missing_reasons.append("long_leg_quote_unavailable")
    if not short:
        missing_reasons.append("short_leg_quote_unavailable")
    if missing_reasons:
        return {}, missing_reasons
    long_provider = str(long.get("provider") or "")
    short_provider = str(short.get("provider") or "")
    if not long_provider or not short_provider:
        return {}, ["spread_leg_provider_unavailable"]
    if long_provider != short_provider:
        return {}, ["spread_leg_provider_mismatch"]
    if required_provider and long_provider != required_provider:
        return {}, ["spread_provider_not_ibkr"]
    long_bid = _number(long.get("bid"))
    long_mid = _number(long.get("mid"))
    long_ask = _number(long.get("ask"))
    short_bid = _number(short.get("bid"))
    short_mid = _number(short.get("mid"))
    short_ask = _number(short.get("ask"))
    if (
        long_bid is None
        or long_mid is None
        or long_ask is None
        or short_bid is None
        or short_mid is None
        or short_ask is None
        or not 0 <= long_bid <= long_mid <= long_ask
        or not 0 <= short_bid <= short_mid <= short_ask
    ):
        return {}, ["spread_leg_nbbo_invalid"]
    long_source_at = _time(long.get("source_at"))
    short_source_at = _time(short.get("source_at"))
    if long_source_at is None or short_source_at is None:
        return {}, ["spread_leg_source_time_unavailable"]
    long_transport_at = _time(long.get("transport_at"))
    short_transport_at = _time(short.get("transport_at"))
    if long_transport_at is None or short_transport_at is None:
        return {}, ["spread_leg_transport_time_unavailable"]
    long_age = (now - long_source_at).total_seconds()
    short_age = (now - short_source_at).total_seconds()
    long_transport_age = (now - long_transport_at).total_seconds()
    short_transport_age = (now - short_transport_at).total_seconds()
    source_skew = abs((long_source_at - short_source_at).total_seconds())
    transport_skew = abs((long_transport_at - short_transport_at).total_seconds())
    time_reasons: list[str] = []
    if long_age < -1.0:
        time_reasons.append("long_leg_quote_in_future")
    elif long_age > max_quote_age_seconds:
        time_reasons.append("long_leg_quote_stale")
    if short_age < -1.0:
        time_reasons.append("short_leg_quote_in_future")
    elif short_age > max_quote_age_seconds:
        time_reasons.append("short_leg_quote_stale")
    if long_transport_age < -1.0:
        time_reasons.append("long_leg_transport_in_future")
    elif long_transport_age > max_quote_age_seconds:
        time_reasons.append("long_leg_transport_stale")
    if short_transport_age < -1.0:
        time_reasons.append("short_leg_transport_in_future")
    elif short_transport_age > max_quote_age_seconds:
        time_reasons.append("short_leg_transport_stale")
    if source_skew > max_quote_skew_seconds:
        time_reasons.append("spread_leg_source_timestamp_skew")
    if transport_skew > max_quote_skew_seconds:
        time_reasons.append("spread_leg_transport_timestamp_skew")
    if time_reasons:
        return {}, time_reasons
    net_bid = long_bid - short_ask
    net_mid = long_mid - short_mid
    net_ask = long_ask - short_bid
    if net_mid <= 0 or net_ask <= 0 or not net_bid <= net_mid <= net_ask:
        return {}, ["spread_net_debit_invalid"]

    long_quality = long.get("quality") if isinstance(long.get("quality"), Mapping) else {}
    short_quality = short.get("quality") if isinstance(short.get("quality"), Mapping) else {}
    quality_ok = long_quality.get("status") == "ok" and short_quality.get("status") == "ok"
    if not quality_ok:
        return {}, ["spread_leg_quality_blocked"]
    result: dict[str, object] = {
        "at": now.isoformat(),
        "mid": net_mid,
        "bid": net_bid,
        "ask": net_ask,
        "iv": long.get("iv"),
        "underlier": long.get("underlier"),
        "long_quote_age_seconds": long_age,
        "short_quote_age_seconds": short_age,
        "long_transport_age_seconds": long_transport_age,
        "short_transport_age_seconds": short_transport_age,
        "leg_source_skew_seconds": source_skew,
        "leg_transport_skew_seconds": transport_skew,
        "quality": {
            "status": "ok",
            "long": dict(long_quality),
            "short": dict(short_quality),
        },
        "long": long,
        "short": short,
    }
    for field in (
        "delta",
        "gamma_per_point",
        "color_gamma_per_minute",
        "speed_gamma_per_point",
        "theta_per_minute",
        "vanna_delta_per_vol_point",
    ):
        result[field] = spread_quote_value(long.get(field), short.get(field))
    return result, []


def spread_quote_value(long_value: object, short_value: object) -> float | None:
    long_number = _number(long_value)
    short_number = _number(short_value)
    if long_number is None or short_number is None:
        return None
    return long_number - short_number
