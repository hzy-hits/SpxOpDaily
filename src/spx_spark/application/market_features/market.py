"""Pure normalized-sample and minute-market-frame calculations."""

from __future__ import annotations

import math
import statistics
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from spx_spark.application.globex_trend.service import globex_session_id
from spx_spark.application.market_features.models import (
    FrameQuality,
    MarketSessionSegment,
    MinuteMarketFrame,
)
from spx_spark.config import NY_TZ
from spx_spark.marketdata import MarketDataQuality, Provider, Quote, as_utc
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState


TRACKED_INSTRUMENTS = (
    "future:ES",
    "index:SPX",
    "equity:SPY",
    "equity:QQQ",
    "equity:RSP",
    "index:VIX",
    "index:VIX1D",
    "index:VIX3M",
    "index:VVIX",
    "index:SKEW",
)


def normalized_market_sample(
    state: LatestState,
    *,
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    now = as_utc(now)
    instruments: dict[str, dict[str, Any]] = {}
    for instrument_id in TRACKED_INSTRUMENTS:
        quote = freshest_quote(state.quotes, instrument_id=instrument_id, now=now, policy=policy)
        if quote is not None:
            instruments[instrument_id] = normalized_quote(quote)

    es_by_provider: dict[str, dict[str, Any]] = {}
    for provider in (Provider.SCHWAB, Provider.IBKR):
        quote = freshest_quote(
            state.quotes,
            instrument_id="future:ES",
            now=now,
            policy=policy,
            provider=provider,
        )
        if quote is not None:
            es_by_provider[provider.value] = normalized_quote(quote)
    return {
        "at": now.isoformat(),
        "session_id": globex_session_id(now),
        "segment": session_segment(now, policy=policy),
        "instruments": instruments,
        "es_by_provider": es_by_provider,
    }


def freshest_quote(
    quotes: tuple[Quote, ...],
    *,
    instrument_id: str,
    now: datetime,
    policy: MarketFeatureSettings,
    provider: Provider | None = None,
) -> Quote | None:
    eligible: list[Quote] = []
    for quote in quotes:
        if quote.instrument.canonical_id != instrument_id:
            continue
        if provider is not None and quote.provider is not provider:
            continue
        if quote.quality is not MarketDataQuality.LIVE or quote.effective_price is None:
            continue
        source_at = quote_source_at(quote)
        transport_at = as_utc(quote.last_update_at or quote.received_at)
        if (
            max(
                (as_utc(now) - source_at).total_seconds(),
                (as_utc(now) - transport_at).total_seconds(),
            )
            > policy.max_quote_age_seconds
        ):
            continue
        eligible.append(quote)
    if not eligible:
        return None
    priority = {Provider.SCHWAB: 1, Provider.IBKR: 0}
    return max(
        eligible,
        key=lambda quote: (quote_source_at(quote), -priority.get(quote.provider, 9)),
    )


def quote_source_at(quote: Quote) -> datetime:
    return as_utc(quote.quote_time or quote.trade_time or quote.last_update_at or quote.received_at)


def normalized_quote(quote: Quote) -> dict[str, Any]:
    return {
        "price": quote.effective_price,
        "provider": quote.provider.value,
        "source_at": quote_source_at(quote).isoformat(),
        "transport_at": as_utc(quote.last_update_at or quote.received_at).isoformat(),
        "bid": quote.bid,
        "ask": quote.ask,
        "bid_size": quote.bid_size,
        "ask_size": quote.ask_size,
        "volume": quote.volume,
        "quality": quote.quality.value,
    }


def merge_minute_sample(
    samples: list[dict[str, Any]],
    sample: dict[str, Any],
    *,
    now: datetime,
    policy: MarketFeatureSettings,
) -> list[dict[str, Any]]:
    current_minute = as_utc(now).replace(second=0, microsecond=0)
    retained = [row for row in samples if isinstance(row, dict)]
    if retained:
        last_at = _parse_at(retained[-1].get("at"))
        if last_at is not None and last_at.replace(second=0, microsecond=0) == current_minute:
            retained[-1] = sample
        elif (
            last_at is None
            or (as_utc(now) - last_at).total_seconds() >= policy.sample_interval_seconds
        ):
            retained.append(sample)
    else:
        retained.append(sample)
    cutoff = as_utc(now) - timedelta(hours=policy.retention_hours)
    return [row for row in retained if (_parse_at(row.get("at")) or cutoff) >= cutoff]


def build_minute_market_frame(
    samples: list[dict[str, Any]],
    *,
    now: datetime,
    expected_move_points: float | None,
    atm_iv: float | None,
    structural_levels: dict[str, Any] | None,
    volume_baselines: dict[str, Any] | None,
    policy: MarketFeatureSettings,
) -> MinuteMarketFrame:
    now = as_utc(now)
    session_id = globex_session_id(now)
    session_samples = [row for row in samples if row.get("session_id") == session_id]
    es_points = _instrument_points(session_samples, "future:ES")
    latest = es_points[-1] if es_points else None
    price = latest[1] if latest else None
    gth_open_at = _spx_gth_open_at(session_id)
    gth_points = [point for point in es_points if point[0] >= gth_open_at]
    gth_open = gth_points[0] if gth_points else None
    gth_open_price = gth_open[1] if gth_open else None
    gth_move_points = (
        price - gth_open_price if price is not None and gth_open_price is not None else None
    )
    returns = {
        f"return_{minutes}m_points": _return(es_points, now, minutes)
        for minutes in (1, 5, 15, 60, 180)
    }
    prices = [point[1] for point in es_points]
    high = max(prices) if prices else None
    low = min(prices) if prices else None
    overnight_points = [
        point
        for row, point in _rows_with_points(session_samples, "future:ES")
        if row.get("segment") in {"asia", "europe", "us_premarket"}
    ]
    overnight_prices = [point[1] for point in overnight_points]
    overnight_high = max(overnight_prices) if overnight_prices else None
    overnight_low = min(overnight_prices) if overnight_prices else None
    overnight_range = (
        overnight_high - overnight_low
        if overnight_high is not None and overnight_low is not None
        else None
    )
    volume = volume_features(
        session_samples,
        now=now,
        baselines=volume_baselines or {},
        required_sessions=policy.volume_baseline_sessions,
    )
    ranges = {
        "overnight": _range_payload(overnight_points, price),
        "asia": _segment_range(session_samples, "asia", price),
        "europe": _segment_range(session_samples, "europe", price),
        "us_premarket": _segment_range(session_samples, "us_premarket", price),
    }
    expected_move_used = (
        overnight_range / expected_move_points
        if overnight_range is not None and expected_move_points and expected_move_points > 0
        else None
    )
    cross_asset = cross_asset_features(session_samples, now=now, policy=policy)
    volatility = volatility_features(session_samples, now=now, atm_iv=atm_iv)
    quality = (
        FrameQuality.READY
        if price is not None and len(es_points) >= 5
        else (FrameQuality.DEGRADED if price is not None else FrameQuality.UNAVAILABLE)
    )
    es = {
        "price": price,
        "provider": latest[2].get("provider") if latest else None,
        "observed_at": latest[0].isoformat() if latest else None,
        "source_at": latest[2].get("source_at") if latest else None,
        "transport_at": latest[2].get("transport_at") if latest else None,
        **returns,
        "session_high": high,
        "session_low": low,
        "distance_from_high_points": price - high
        if price is not None and high is not None
        else None,
        "distance_from_low_points": price - low if price is not None and low is not None else None,
        "trend_efficiency_60m": trend_efficiency(es_points, now=now, minutes=60),
        "trend_efficiency_180m": trend_efficiency(es_points, now=now, minutes=180),
        **swing_structure(es_points, now=now),
        "overnight_range_points": overnight_range,
        "overnight_expected_move_used": expected_move_used,
        "gth_open_at": gth_open[0].isoformat() if gth_open else None,
        "gth_open_price": gth_open_price,
        "gth_move_points": gth_move_points,
        "gth_expected_move_used": (
            abs(gth_move_points) / expected_move_points
            if gth_move_points is not None and expected_move_points and expected_move_points > 0
            else None
        ),
        "vwap": volume.get("session_vwap"),
        "vwap_distance_points": (
            price - volume["session_vwap"]
            if price is not None and isinstance(volume.get("session_vwap"), int | float)
            else None
        ),
        "vwap_slope_15m_points": volume.get("vwap_slope_15m_points"),
        "key_level_holds": key_level_holds(
            es_points,
            now=now,
            levels=structural_levels or {},
        ),
    }
    frame_id = f"market:{session_id}:{now.strftime('%Y%m%dT%H%M')}"
    return MinuteMarketFrame(
        schema_version=1,
        frame_id=frame_id,
        session_id=session_id,
        as_of=now,
        quality=quality,
        es=es,
        session_ranges=ranges,
        volume=volume,
        cross_asset=cross_asset,
        volatility=volatility,
        diagnostics={
            "sample_count": len(session_samples),
            "segment": session_segment(now, policy=policy),
        },
    )


def _spx_gth_open_at(session_id: str) -> datetime:
    """Return the 20:15 ET SPX GTH open for a Globex business-date id."""

    business_date = date.fromisoformat(session_id)
    return datetime.combine(
        business_date - timedelta(days=1),
        time(20, 15),
        tzinfo=NY_TZ,
    ).astimezone(timezone.utc)


def session_segment(at: datetime, *, policy: MarketFeatureSettings | None = None) -> str:
    policy = policy or MarketFeatureSettings()
    local = as_utc(at).astimezone(NY_TZ)
    clock = local.time().replace(tzinfo=None)
    if clock >= time(18) or clock < time.fromisoformat(policy.asia_end_et):
        return MarketSessionSegment.ASIA.value
    if clock < time.fromisoformat(policy.europe_end_et):
        return MarketSessionSegment.EUROPE.value
    if clock < time.fromisoformat(policy.premarket_end_et):
        return MarketSessionSegment.US_PREMARKET.value
    if clock < time.fromisoformat(policy.rth_end_et):
        return MarketSessionSegment.RTH.value
    if clock < time.fromisoformat(policy.curb_end_et):
        return MarketSessionSegment.CURB.value
    return MarketSessionSegment.MAINTENANCE.value


def volume_features(
    samples: list[dict[str, Any]],
    *,
    now: datetime,
    baselines: dict[str, Any],
    required_sessions: int,
) -> dict[str, Any]:
    providers = {
        str(provider)
        for row in samples
        for provider in (
            row.get("es_by_provider", {}).keys()
            if isinstance(row.get("es_by_provider"), dict)
            else ()
        )
    }
    by_provider = {provider: _volume_points(samples, provider=provider) for provider in providers}
    session_provider = (
        max(by_provider, key=lambda provider: len(by_provider[provider])) if by_provider else None
    )
    current_quote = _instrument(samples[-1], "future:ES") if samples else None
    current_provider = str(current_quote.get("provider")) if current_quote else None
    recent_provider = (
        current_provider
        if current_provider in by_provider and len(by_provider[current_provider]) >= 2
        else session_provider
    )
    points = by_provider.get(recent_provider, _volume_points(samples))
    session_points = by_provider.get(session_provider, points)
    deltas: dict[str, float | None] = {}
    window_points: dict[int, tuple[Any, Any] | None] = {}
    for minutes in (1, 5, 15):
        pair = _complete_volume_window(points, minutes=minutes)
        window_points[minutes] = pair
        reference, current = pair if pair is not None else (None, None)
        delta = None
        if reference and current and current[1] >= reference[1]:
            delta = current[1] - reference[1]
        deltas[f"volume_delta_{minutes}m"] = delta
    vwap = _volume_weighted_price(session_points)
    overnight_points = _volume_points(
        [row for row in samples if row.get("segment") in {"asia", "europe", "us_premarket"}],
        provider=session_provider,
    )
    overnight_vwap = _volume_weighted_price(overnight_points)
    recent = _window_points(points, now=now, minutes=15)
    earlier = _window_points(points, now=now - timedelta(minutes=15), minutes=15)
    recent_vwap = _volume_weighted_price(recent)
    earlier_vwap = _volume_weighted_price(earlier)
    pace = deltas["volume_delta_5m"] / 5 if deltas["volume_delta_5m"] is not None else None
    slot = as_utc(now).astimezone(NY_TZ).strftime("%H:%M")
    history = baselines.get(slot)
    current_session_id = str(samples[-1].get("session_id")) if samples else ""
    history_values = [
        float(item["pace"])
        for item in history or []
        if isinstance(item, dict)
        and item.get("session_id") != current_session_id
        and isinstance(item.get("pace"), int | float)
    ]
    percentile = None
    if pace is not None and len(history_values) >= required_sessions:
        percentile = sum(value <= pace for value in history_values) / len(history_values)
    five_minute_window = window_points[5]
    price_delta = (
        five_minute_window[1][2] - five_minute_window[0][2]
        if five_minute_window is not None
        else None
    )
    alignment = _alignment(price_delta, deltas["volume_delta_5m"])
    return {
        **deltas,
        "pace_5m_per_minute": pace,
        "pace_percentile_20_sessions": percentile,
        "pace_baseline_sample_count": len(history_values),
        "pace_baseline_ready": len(history_values) >= required_sessions,
        "session_vwap": vwap,
        "overnight_vwap": overnight_vwap,
        "vwap_slope_15m_points": (
            recent_vwap - earlier_vwap
            if recent_vwap is not None and earlier_vwap is not None
            else None
        ),
        "price_volume_alignment_5m": alignment,
        "price_volume_alignment_reason_5m": (
            None if alignment != "unavailable" else "insufficient_synchronized_window"
        ),
        "session_reset_detected": any(cur[1] < prev[1] for prev, cur in zip(points, points[1:])),
        "recent_volume_provider": recent_provider,
        "session_vwap_provider": session_provider,
    }


def cross_asset_features(
    samples: list[dict[str, Any]],
    *,
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    returns: dict[str, dict[str, float | None]] = {}
    for instrument_id in ("future:ES", "equity:SPY", "equity:QQQ", "equity:RSP"):
        points = _instrument_points(samples, instrument_id)
        returns[instrument_id] = {
            f"return_{minutes}m_pct": _percent_return(points, now, minutes)
            for minutes in (5, 15, 60)
        }
    latest = samples[-1] if samples else {}
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
    schwab, ibkr = providers.get("schwab"), providers.get("ibkr")
    divergence = _provider_divergence(schwab, ibkr, policy=policy)
    previous_provider = None
    for row in reversed(samples[:-1]):
        quote = _instrument(row, "future:ES")
        if quote and quote.get("provider"):
            previous_provider = quote["provider"]
            break
    current_provider = es.get("provider") if es else None
    es_15 = returns["future:ES"]["return_15m_pct"]
    spy_15 = returns["equity:SPY"]["return_15m_pct"]
    confirmation = _direction_confirmation(es_15, spy_15)
    rolling_basis = statistics.median(basis_history) if basis_history else None
    return {
        "returns": returns,
        "es_spx_basis_points": basis,
        "es_spx_basis_rolling_median": rolling_basis,
        "es_spx_basis_deviation_points": _difference(basis, rolling_basis),
        "basis_source_skew_seconds": basis_source_skew,
        "es_spy_direction_confirmation_15m": confirmation,
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


def volatility_features(
    samples: list[dict[str, Any]], *, now: datetime, atm_iv: float | None
) -> dict[str, Any]:
    latest = samples[-1] if samples else {}
    values = {
        key.split(":", 1)[1].lower(): _number((_instrument(latest, key) or {}).get("price"))
        for key in ("index:VIX", "index:VIX1D", "index:VIX3M", "index:VVIX", "index:SKEW")
    }
    vix, vix1d, vix3m = values.get("vix"), values.get("vix1d"), values.get("vix3m")
    vix_return = _percent_return(_instrument_points(samples, "index:VIX"), now, 15)
    vvix_return = _percent_return(_instrument_points(samples, "index:VVIX"), now, 15)
    es_realized = realized_volatility(_instrument_points(samples, "future:ES"), now=now, minutes=60)
    return {
        **values,
        "vix1d_vix_ratio": vix1d / vix if vix1d and vix else None,
        "vix_vix3m_ratio": vix / vix3m if vix and vix3m else None,
        "vix_vvix_direction_confirmation_15m": _direction_confirmation(vix_return, vvix_return),
        "vix_return_15m_pct": vix_return,
        "vvix_return_15m_pct": vvix_return,
        "skew_change_60m": _return(_instrument_points(samples, "index:SKEW"), now, 60),
        "es_realized_vol_60m_annualized": es_realized,
        "atm_iv_minus_es_realized_vol": (
            atm_iv - es_realized if atm_iv is not None and es_realized is not None else None
        ),
    }


def realized_volatility(
    points: list[tuple[datetime, float, dict[str, Any]]],
    *,
    now: datetime,
    minutes: int,
) -> float | None:
    window = [point for point in points if point[0] >= as_utc(now) - timedelta(minutes=minutes)]
    if len(window) < 10:
        return None
    log_returns = [
        math.log(current[1] / previous[1])
        for previous, current in zip(window, window[1:])
        if previous[1] > 0 and current[1] > 0
    ]
    if len(log_returns) < 9:
        return None
    return statistics.stdev(log_returns) * math.sqrt(252 * 23 * 60)


def key_level_holds(
    points: list[tuple[datetime, float, dict[str, Any]]],
    *,
    now: datetime,
    levels: dict[str, Any],
) -> dict[str, Any]:
    if not points:
        return {}
    current = points[-1][1]
    result: dict[str, Any] = {}
    for key in ("put_wall", "zero_gamma", "call_wall"):
        level = _number(levels.get(key))
        if level is None:
            continue
        side = "above" if current >= level else "below"
        row: dict[str, Any] = {"level": level, "side": side}
        for minutes in (1, 3, 5):
            window = [
                point[1] for point in points if point[0] >= as_utc(now) - timedelta(minutes=minutes)
            ]
            row[f"holds_{minutes}m"] = (
                all(price >= level for price in window)
                if side == "above" and len(window) >= minutes
                else all(price < level for price in window)
                if side == "below" and len(window) >= minutes
                else None
            )
        result[key] = row
    return result


def trend_efficiency(
    points: list[tuple[datetime, float, dict[str, Any]]],
    *,
    now: datetime,
    minutes: int,
) -> float | None:
    window = [point for point in points if point[0] >= as_utc(now) - timedelta(minutes=minutes)]
    if len(window) < 2:
        return None
    path = sum(abs(cur[1] - prev[1]) for prev, cur in zip(window, window[1:]))
    return abs(window[-1][1] - window[0][1]) / path if path > 0 else 0.0


def swing_structure(
    points: list[tuple[datetime, float, dict[str, Any]]], *, now: datetime
) -> dict[str, Any]:
    cutoff = as_utc(now) - timedelta(minutes=60)
    window = [point for point in points if point[0] >= cutoff]
    if len(window) < 4:
        return {"lower_high_60m": None, "higher_low_60m": None}
    midpoint = len(window) // 2
    prior, recent = window[:midpoint], window[midpoint:]
    prior_high, recent_high = max(p[1] for p in prior), max(p[1] for p in recent)
    prior_low, recent_low = min(p[1] for p in prior), min(p[1] for p in recent)
    return {
        "lower_high_60m": recent_high < prior_high,
        "higher_low_60m": recent_low > prior_low,
        "prior_swing_high": prior_high,
        "recent_swing_high": recent_high,
        "prior_swing_low": prior_low,
        "recent_swing_low": recent_low,
    }


def update_volume_baselines(
    baselines: dict[str, Any],
    frame: MinuteMarketFrame,
    *,
    max_sessions: int,
) -> dict[str, Any]:
    pace = frame.volume.get("pace_5m_per_minute")
    if not isinstance(pace, int | float):
        return baselines
    slot = frame.as_of.astimezone(NY_TZ).strftime("%H:%M")
    entries = [dict(item) for item in baselines.get(slot, []) if isinstance(item, dict)]
    entries = [item for item in entries if item.get("session_id") != frame.session_id]
    entries.append({"session_id": frame.session_id, "pace": float(pace)})
    baselines[slot] = entries[-max_sessions:]
    return baselines


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


def _rows_with_points(
    samples: list[dict[str, Any]], instrument_id: str
) -> list[tuple[dict[str, Any], tuple[datetime, float, dict[str, Any]]]]:
    result = []
    for row in samples:
        points = _instrument_points([row], instrument_id)
        if points:
            result.append((row, points[0]))
    return result


def _return(
    points: list[tuple[datetime, float, dict[str, Any]]], now: datetime, minutes: int
) -> float | None:
    if not points:
        return None
    reference = _point_before(points, as_utc(now) - timedelta(minutes=minutes))
    if reference is None:
        return None
    tolerance = max(90.0, minutes * 12.0)
    target = as_utc(now) - timedelta(minutes=minutes)
    if (target - reference[0]).total_seconds() > tolerance:
        return None
    return points[-1][1] - reference[1]


def _percent_return(
    points: list[tuple[datetime, float, dict[str, Any]]], now: datetime, minutes: int
) -> float | None:
    delta = _return(points, now, minutes)
    reference = _point_before(points, as_utc(now) - timedelta(minutes=minutes))
    if delta is None or reference is None or reference[1] == 0:
        return None
    return delta / reference[1]


def _point_before(points: list[Any], target: datetime) -> Any | None:
    candidates = [point for point in points if point[0] <= target]
    return max(candidates, key=lambda point: point[0]) if candidates else None


def _window_points(points: list[Any], *, now: datetime, minutes: int) -> list[Any]:
    end = as_utc(now)
    start = end - timedelta(minutes=minutes)
    return [point for point in points if start <= point[0] <= end]


def _volume_weighted_price(points: list[tuple[datetime, float, float]]) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for previous, current in zip(points, points[1:]):
        delta = current[1] - previous[1]
        if delta <= 0:
            continue
        numerator += current[2] * delta
        denominator += delta
    return numerator / denominator if denominator > 0 else None


def _volume_points(
    samples: list[dict[str, Any]], *, provider: str | None = None
) -> list[tuple[datetime, float, float]]:
    points: list[tuple[datetime, float, float]] = []
    for row in samples:
        at = _parse_at(row.get("at"))
        quote = None
        if provider is not None:
            provider_quotes = row.get("es_by_provider")
            candidate = provider_quotes.get(provider) if isinstance(provider_quotes, dict) else None
            if isinstance(candidate, dict):
                quote = candidate
            else:
                selected = _instrument(row, "future:ES")
                if selected and selected.get("provider") == provider:
                    quote = selected
        else:
            quote = _instrument(row, "future:ES")
        volume = _number(quote.get("volume")) if quote else None
        price = _number(quote.get("price")) if quote else None
        if at is not None and volume is not None and price is not None:
            points.append((at, volume, price))
    return points


def _complete_volume_window(
    points: list[tuple[datetime, float, float]], *, minutes: int
) -> tuple[tuple[datetime, float, float], tuple[datetime, float, float]] | None:
    """Return synchronized cumulative-volume and price endpoints for one window."""
    if not points:
        return None
    current = points[-1]
    target = current[0] - timedelta(minutes=minutes)
    reference = _point_before(points, target)
    if reference is None:
        return None
    tolerance = max(90.0, minutes * 12.0)
    if (target - reference[0]).total_seconds() > tolerance:
        return None
    return reference, current


def _range_payload(
    points: list[tuple[datetime, float, dict[str, Any]]], price: float | None
) -> dict[str, Any]:
    prices = [point[1] for point in points]
    high, low = (max(prices), min(prices)) if prices else (None, None)
    return {
        "high": high,
        "low": low,
        "range_points": high - low if high is not None and low is not None else None,
        "distance_from_high_points": price - high
        if price is not None and high is not None
        else None,
        "distance_from_low_points": price - low if price is not None and low is not None else None,
        "sample_count": len(points),
    }


def _segment_range(
    samples: list[dict[str, Any]], segment: str, price: float | None
) -> dict[str, Any]:
    points = [
        point
        for row, point in _rows_with_points(samples, "future:ES")
        if row.get("segment") == segment
    ]
    return _range_payload(points, price)


def _provider_divergence(
    schwab: object, ibkr: object, *, policy: MarketFeatureSettings
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
        "price_points": schwab_price - ibkr_price
        if skew <= policy.provider_sync_tolerance_seconds
        else None,  # type: ignore[operator]
        "source_skew_seconds": skew,
    }


def _direction_confirmation(first: float | None, second: float | None) -> str:
    if first is None or second is None:
        return "unavailable"
    if math.isclose(first, 0.0) or math.isclose(second, 0.0):
        return "neutral"
    return "confirmed" if first * second > 0 else "divergent"


def _alignment(price_delta: float | None, volume_delta: float | None) -> str:
    if price_delta is None or volume_delta is None:
        return "unavailable"
    if abs(price_delta) < 0.5:
        return "volume_without_price_progress" if volume_delta > 0 else "flat"
    return "price_volume_aligned" if volume_delta > 0 else "price_without_volume_confirmation"


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
    return float(value) if isinstance(value, int | float) and math.isfinite(value) else None
