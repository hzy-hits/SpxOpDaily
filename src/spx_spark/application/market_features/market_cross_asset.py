"""Cross-asset and four-cash-index features from normalized minute samples."""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timedelta
from typing import Any

from spx_spark.marketdata import as_utc
from spx_spark.settings.market_features import MarketFeatureSettings


CASH_INDEX_INSTRUMENTS = (
    "index:SPX",
    "index:NDX",
    "index:DJI",
    "index:RUT",
)
GLOBEX_INDEX_INSTRUMENTS = (
    "future:ES",
    "future:NQ",
    "future:YM",
    "future:RTY",
)
GLOBEX_CASH_ROLE_MAP = {
    "index:SPX": "future:ES",
    "index:NDX": "future:NQ",
    "index:DJI": "future:YM",
    "index:RUT": "future:RTY",
}
RTH_CASH_CLOSED_SEGMENTS = frozenset({"rth", "maintenance"})
RETURN_INSTRUMENTS = (
    "future:ES",
    "future:NQ",
    "future:YM",
    "future:RTY",
    "equity:SPY",
    "equity:QQQ",
    "equity:RSP",
    *CASH_INDEX_INSTRUMENTS,
)


def cross_asset_features(
    samples: list[dict[str, Any]],
    *,
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    returns = {
        instrument_id: {
            f"return_{minutes}m_pct": _percent_return(
                _instrument_points(samples, instrument_id), now, minutes
            )
            for minutes in (5, 15, 60)
        }
        for instrument_id in RETURN_INSTRUMENTS
    }
    latest = samples[-1] if samples else {}
    cash_index = _cash_index_features(latest, returns=returns, policy=policy)
    globex_index = _globex_index_features(latest, returns=returns, policy=policy)
    for instrument_id in (*cash_index["missing_instruments"], *globex_index["missing_instruments"]):
        returns[instrument_id] = {
            "return_5m_pct": None,
            "return_15m_pct": None,
            "return_60m_pct": None,
        }
    cross_index = _session_cross_index(
        latest.get("segment"),
        cash_index=cash_index,
        globex_index=globex_index,
    )

    es = _instrument(latest, "future:ES")
    spx = _instrument(latest, "index:SPX")
    basis = None
    basis_source_skew = None
    if es and spx:
        es_at = _parse_at(es.get("source_at"))
        spx_at = _parse_at(spx.get("source_at"))
        if es_at and spx_at:
            basis_source_skew = abs((es_at - spx_at).total_seconds())
            if basis_source_skew <= policy.provider_sync_tolerance_seconds:
                es_price, spx_price = _number(es.get("price")), _number(spx.get("price"))
                if es_price is not None and spx_price is not None:
                    basis = es_price - spx_price
    basis_history: list[float] = []
    for row in samples:
        row_es, row_spx = _instrument(row, "future:ES"), _instrument(row, "index:SPX")
        if not row_es or not row_spx:
            continue
        es_price, spx_price = _number(row_es.get("price")), _number(row_spx.get("price"))
        es_at, spx_at = _parse_at(row_es.get("source_at")), _parse_at(row_spx.get("source_at"))
        if (
            es_price is not None
            and spx_price is not None
            and es_at is not None
            and spx_at is not None
            and abs((es_at - spx_at).total_seconds()) <= policy.provider_sync_tolerance_seconds
        ):
            basis_history.append(es_price - spx_price)
    providers = (
        latest.get("es_by_provider") if isinstance(latest.get("es_by_provider"), dict) else {}
    )
    divergence = _provider_divergence(
        providers.get("schwab"),
        providers.get("ibkr"),
        policy=policy,
    )
    previous_provider = None
    for row in reversed(samples[:-1]):
        quote = _instrument(row, "future:ES")
        if quote and quote.get("provider"):
            previous_provider = quote["provider"]
            break
    current_provider = es.get("provider") if es else None
    es_15 = returns["future:ES"]["return_15m_pct"]
    spy_15 = returns["equity:SPY"]["return_15m_pct"]
    rolling_basis = statistics.median(basis_history) if basis_history else None
    return {
        "returns": returns,
        "cash_index": cash_index,
        "globex_index": globex_index,
        "cross_index": cross_index,
        "es_spx_basis_points": basis,
        "es_spx_basis_rolling_median": rolling_basis,
        "es_spx_basis_deviation_points": _difference(basis, rolling_basis),
        "basis_source_skew_seconds": basis_source_skew,
        "es_spy_direction_confirmation_15m": direction_confirmation(es_15, spy_15),
        "relative_strength_15m": {
            "qqq_minus_spy_pct": _difference(returns["equity:QQQ"]["return_15m_pct"], spy_15),
            "rsp_minus_spy_pct": _difference(returns["equity:RSP"]["return_15m_pct"], spy_15),
        },
        "es_provider_divergence": divergence,
        "selected_es_provider": current_provider,
        "source_switch": (
            {"from": previous_provider, "to": current_provider}
            if previous_provider and current_provider and previous_provider != current_provider
            else None
        ),
    }


def _cash_index_features(
    latest: dict[str, Any],
    *,
    returns: dict[str, dict[str, float | None]],
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    cash_session_open = latest.get("segment") == "rth"
    basket = _relative_basket_features(
        latest,
        returns=returns,
        policy=policy,
        instruments=CASH_INDEX_INSTRUMENTS,
        session_open=cash_session_open,
        reason_prefix="cash_index",
        closed_reason="cash_index_cash_session_closed",
        anchor_id="index:SPX",
        relative_key="relative_to_spx_15m_bps",
        semantics="observed_cash_index_price_regime_not_market_maker_behavior",
    )
    basket.update(
        {
            "cash_session": "rth",
            "cash_session_open": cash_session_open,
        }
    )
    return basket


def _globex_index_features(
    latest: dict[str, Any],
    *,
    returns: dict[str, dict[str, float | None]],
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    segment = latest.get("segment")
    globex_session_open = segment not in RTH_CASH_CLOSED_SEGMENTS
    closed_reason = (
        "globex_index_rth_uses_cash"
        if segment == "rth"
        else "globex_index_session_closed"
    )
    basket = _relative_basket_features(
        latest,
        returns=returns,
        policy=policy,
        instruments=GLOBEX_INDEX_INSTRUMENTS,
        session_open=globex_session_open,
        reason_prefix="globex_index",
        closed_reason=closed_reason,
        anchor_id="future:ES",
        relative_key="relative_to_es_15m_bps",
        semantics="observed_globex_futures_relative_to_es_not_cash_index",
    )
    basket.update(
        {
            "globex_session": segment,
            "globex_session_open": globex_session_open,
            "calibration": "percent_return_minus_es",
            "role_map": dict(GLOBEX_CASH_ROLE_MAP),
        }
    )
    return basket


def _session_cross_index(
    segment: object,
    *,
    cash_index: dict[str, Any],
    globex_index: dict[str, Any],
) -> dict[str, Any]:
    if segment == "rth":
        selected = cash_index
        source = "cash_index"
        anchor = "index:SPX"
        relative_key = "relative_to_spx_15m_bps"
        session_open = cash_index.get("cash_session_open") is True
    else:
        selected = globex_index
        source = "globex_index"
        anchor = "future:ES"
        relative_key = "relative_to_es_15m_bps"
        session_open = globex_index.get("globex_session_open") is True
    return {
        "source": source,
        "status": selected.get("status"),
        "anchor": anchor,
        "session_open": session_open,
        "required_instruments": list(selected.get("required_instruments") or ()),
        "missing_instruments": list(selected.get("missing_instruments") or ()),
        "relative_to_anchor_15m_bps": dict(selected.get(relative_key) or {}),
        "dispersion_15m_bps": selected.get("dispersion_15m_bps"),
        "breadth_15m": selected.get("breadth_15m"),
        "reason_codes": list(selected.get("reason_codes") or ()),
        "semantics": selected.get("semantics"),
        "calibration": selected.get("calibration"),
        "role_map": dict(selected.get("role_map") or {}),
    }


def _relative_basket_features(
    latest: dict[str, Any],
    *,
    returns: dict[str, dict[str, float | None]],
    policy: MarketFeatureSettings,
    instruments: tuple[str, ...],
    session_open: bool,
    reason_prefix: str,
    closed_reason: str,
    anchor_id: str,
    relative_key: str,
    semantics: str,
) -> dict[str, Any]:
    observations: dict[str, dict[str, object]] = {}
    missing: list[str] = []
    source_times: list[datetime] = []
    for instrument_id in instruments:
        quote = _instrument(latest, instrument_id)
        source_at = _parse_at(quote.get("source_at")) if quote else None
        price = _number(quote.get("price")) if quote else None
        if quote is None or source_at is None or price is None:
            missing.append(instrument_id)
            observations[instrument_id] = {
                "status": "missing",
                "price": None,
                "reference_close": None,
                "price_kind": None,
                "provider": None,
                "quality": "missing",
                "source_at": None,
                "missing_reason": (
                    "fresh_quote_unavailable"
                    if quote is None
                    else "price_or_source_timestamp_unavailable"
                ),
            }
            continue
        source_times.append(source_at)
        observations[instrument_id] = {
            "status": "available",
            "price": price,
            "reference_close": _number(quote.get("reference_close")),
            "price_kind": quote.get("price_kind"),
            "provider": quote.get("provider"),
            "quality": quote.get("quality"),
            "source_at": source_at.isoformat(),
            "missing_reason": None,
        }
    source_skew = (
        (max(source_times) - min(source_times)).total_seconds() if len(source_times) >= 2 else None
    )
    synchronized = (
        session_open
        and not missing
        and source_skew is not None
        and source_skew <= policy.provider_sync_tolerance_seconds
    )
    return_15m = {
        instrument_id: (
            None if instrument_id in missing else returns[instrument_id]["return_15m_pct"]
        )
        for instrument_id in instruments
    }
    complete_returns = all(value is not None for value in return_15m.values())
    usable = synchronized and complete_returns
    anchor_return = return_15m[anchor_id] if usable else None
    relative = {
        instrument_id: (
            (float(value) - float(anchor_return)) * 10_000.0
            if value is not None and anchor_return is not None
            else None
        )
        for instrument_id, value in return_15m.items()
    }
    return_bps = [float(value) * 10_000.0 for value in return_15m.values() if value is not None]
    reason_codes = [f"{reason_prefix}_missing:{instrument_id}" for instrument_id in missing]
    if not session_open:
        reason_codes.append(closed_reason)
    elif not missing and not synchronized:
        reason_codes.append(f"{reason_prefix}_source_skew_exceeded")
    if synchronized and not complete_returns:
        reason_codes.append(f"{reason_prefix}_15m_history_incomplete")
    return {
        "status": "ready" if usable else "degraded",
        "required_instruments": list(instruments),
        "observations": observations,
        "missing_instruments": missing,
        "complete": not missing,
        "synchronized": synchronized,
        "source_skew_seconds": source_skew,
        "source_skew_limit_seconds": policy.provider_sync_tolerance_seconds,
        "return_15m_available_count": len(return_bps),
        relative_key: relative,
        "dispersion_15m_bps": statistics.pstdev(return_bps) if usable else None,
        "breadth_15m": (
            {
                "up_count": sum(value > 0.0 for value in return_bps),
                "down_count": sum(value < 0.0 for value in return_bps),
                "flat_count": sum(math.isclose(value, 0.0) for value in return_bps),
            }
            if usable
            else None
        ),
        "reason_codes": sorted(reason_codes),
        "semantics": semantics,
    }


def direction_confirmation(first: float | None, second: float | None) -> str:
    if first is None or second is None:
        return "unavailable"
    if math.isclose(first, 0.0) or math.isclose(second, 0.0):
        return "neutral"
    return "confirmed" if first * second > 0 else "divergent"


def _provider_divergence(
    schwab: object,
    ibkr: object,
    *,
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    if not isinstance(schwab, dict) or not isinstance(ibkr, dict):
        return {"available": False, "price_points": None, "source_skew_seconds": None}
    schwab_at, ibkr_at = _parse_at(schwab.get("source_at")), _parse_at(ibkr.get("source_at"))
    schwab_price, ibkr_price = _number(schwab.get("price")), _number(ibkr.get("price"))
    if None in {schwab_at, ibkr_at, schwab_price, ibkr_price}:
        return {"available": False, "price_points": None, "source_skew_seconds": None}
    skew = abs((schwab_at - ibkr_at).total_seconds())  # type: ignore[operator]
    return {
        "available": skew <= policy.provider_sync_tolerance_seconds,
        "price_points": (
            schwab_price - ibkr_price  # type: ignore[operator]
            if skew <= policy.provider_sync_tolerance_seconds
            else None
        ),
        "source_skew_seconds": skew,
    }


def _instrument(row: dict[str, Any], instrument_id: str) -> dict[str, Any] | None:
    instruments = row.get("instruments")
    if not isinstance(instruments, dict):
        return None
    quote = instruments.get(instrument_id)
    return quote if isinstance(quote, dict) else None


def _instrument_points(
    samples: list[dict[str, Any]], instrument_id: str
) -> list[tuple[datetime, float, dict[str, Any]]]:
    points: list[tuple[datetime, float, dict[str, Any]]] = []
    for row in samples:
        at = _parse_at(row.get("at"))
        quote = _instrument(row, instrument_id)
        price = _number(quote.get("price")) if quote else None
        if at is not None and price is not None and quote is not None:
            points.append((at, price, quote))
    return points


def _return(
    points: list[tuple[datetime, float, dict[str, Any]]],
    now: datetime,
    minutes: int,
) -> float | None:
    if not points:
        return None
    target = as_utc(now) - timedelta(minutes=minutes)
    reference = _point_before(points, target)
    if reference is None:
        return None
    tolerance = max(90.0, minutes * 12.0)
    if (target - reference[0]).total_seconds() > tolerance:
        return None
    return points[-1][1] - reference[1]


def _percent_return(
    points: list[tuple[datetime, float, dict[str, Any]]],
    now: datetime,
    minutes: int,
) -> float | None:
    delta = _return(points, now, minutes)
    reference = _point_before(points, as_utc(now) - timedelta(minutes=minutes))
    if delta is None or reference is None or reference[1] == 0:
        return None
    return delta / reference[1]


def _point_before(points: list[Any], target: datetime) -> Any | None:
    candidates = [point for point in points if point[0] <= target]
    return max(candidates, key=lambda point: point[0]) if candidates else None


def _difference(first: float | None, second: float | None) -> float | None:
    return first - second if first is not None and second is not None else None


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return as_utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


__all__ = [
    "CASH_INDEX_INSTRUMENTS",
    "GLOBEX_CASH_ROLE_MAP",
    "GLOBEX_INDEX_INSTRUMENTS",
    "cross_asset_features",
    "direction_confirmation",
]
