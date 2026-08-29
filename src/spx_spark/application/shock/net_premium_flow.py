"""Captured SPXW aggressor-flow proxy, rollups, and divergence observations."""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from spx_spark.alert_model import Alert
from spx_spark.application.shock.models import (
    NET_PREMIUM_BEARISH_DIVERGENCE_KIND,
    NET_PREMIUM_BULLISH_DIVERGENCE_KIND,
    PriceSample,
    _parse_datetime,
)
from spx_spark.marketdata import (
    InstrumentType,
    MarketDataQuality,
    OptionRight,
    Provider,
    Quote,
    as_utc,
)


_ET = ZoneInfo("America/New_York")
_FLOW_STATE_KEY = "captured_net_premium_divergence"
_POLICY_VERSION = "captured_option_flow.v4"
_FLOW_IMAGE_URL = "https://spx.zh3nyu.com/flow/latest.png"
_LOOKBACK_MINUTES = 15
_CONFIRM_MINUTES = 5
_COOLDOWN_SECONDS = 15 * 60
_MIN_COVERAGE = 0.10
_MEDIUM_COVERAGE = 0.15
_MAX_INSIDE_SHARE = 0.50
_MAX_QUOTE_AGE_SECONDS = 15.0
_MAX_TRADE_LAG_SECONDS = 5.0
# Keep one complete RTH scalar tape for the public flow chart.  Per-strike
# payloads are still stripped after the much shorter window below, so this
# does not retain a full-chain history in hot state.
_HISTORY_MINUTES = 7 * 60
_STRIKE_HISTORY_MINUTES = 15
_SEEN_RETENTION_MINUTES = 45
_START_ET = time(10, 0)
_LATE_MANAGEMENT_START_ET = time(13, 30)
_END_ET = time(15, 30)
_IMBALANCE_THRESHOLD = 0.05
_ROLLING_WINDOWS = (1, 5, 15, 30)
_SNAPSHOT_STRIKE_LIMIT = 20
_PREMIUM_FIELDS = (
    "call_buy",
    "call_sell",
    "call_unknown",
    "put_buy",
    "put_sell",
    "put_unknown",
)
_TOTAL_FIELDS = (
    *_PREMIUM_FIELDS,
    "call_net",
    "put_net",
    "classified_premium",
    "captured_premium",
    "classified_size",
    "captured_size",
    "unknown_size",
    "volume_delta",
    "print_count",
    "classified_print_count",
    "unknown_print_count",
)
_STRIKE_STATE_FIELDS = (
    *_PREMIUM_FIELDS,
    "classified_print_count",
    "unknown_print_count",
)
_AGGREGATE_METADATA_FIELDS = (
    "spx",
    "atm_iv",
    "atm_straddle",
    "started_at",
    "updated_at",
    "minute_count",
)


def _minute_start(value: datetime) -> datetime:
    return as_utc(value).replace(second=0, microsecond=0)


def _number(value: object) -> float:
    return float(value) if isinstance(value, int | float) and math.isfinite(value) else 0.0


def _optional_number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and math.isfinite(value) else None


def _strike_number(value: object) -> float:
    try:
        strike = float(value)
    except (TypeError, ValueError):
        return 0.0
    return strike if math.isfinite(strike) else 0.0


def _decode_strikes(raw: object) -> dict[str, dict[str, object]]:
    if not isinstance(raw, dict):
        return {}
    strikes: dict[str, dict[str, object]] = {}
    for raw_key, raw_value in raw.items():
        key = str(raw_key)
        if isinstance(raw_value, dict):
            row = dict(raw_value)
        elif isinstance(raw_value, list):
            row = {
                field: _number(raw_value[index])
                for index, field in enumerate(_STRIKE_STATE_FIELDS)
                if index < len(raw_value) and _number(raw_value[index]) != 0
            }
        else:
            continue
        row["strike"] = _number(row.get("strike")) or _strike_number(key)
        strikes[key] = row
    return strikes


def _new_aggregate(raw: object = None, *, with_strikes: bool = False) -> dict[str, object]:
    aggregate = dict(raw) if isinstance(raw, dict) else {}
    packed_totals = aggregate.pop("totals", None)
    if isinstance(packed_totals, list):
        for index, field in enumerate(_TOTAL_FIELDS):
            aggregate[field] = _number(packed_totals[index]) if index < len(packed_totals) else 0.0
    for field in _TOTAL_FIELDS:
        aggregate.setdefault(field, 0.0)
    if with_strikes:
        aggregate["strikes"] = _decode_strikes(aggregate.get("strikes"))
    return aggregate


def _trim_trailing_zeroes(values: list[float]) -> list[float]:
    while values and values[-1] == 0:
        values.pop()
    return values


def _pack_aggregate(aggregate: dict[str, object]) -> dict[str, object]:
    packed = {field: aggregate[field] for field in _AGGREGATE_METADATA_FIELDS if field in aggregate}
    packed["totals"] = _trim_trailing_zeroes(
        [_number(aggregate.get(field)) for field in _TOTAL_FIELDS]
    )
    raw_strikes = aggregate.get("strikes")
    if isinstance(raw_strikes, dict):
        packed_strikes: dict[str, list[float]] = {}
        for key, raw_row in raw_strikes.items():
            if not isinstance(raw_row, dict):
                continue
            values = _trim_trailing_zeroes(
                [_number(raw_row.get(field)) for field in _STRIKE_STATE_FIELDS]
            )
            if values:
                packed_strikes[str(key)] = values
        if packed_strikes:
            packed["strikes"] = packed_strikes
    return packed


def _decode_seen_options(raw: object) -> dict[str, dict[str, object]]:
    if not isinstance(raw, dict):
        return {}
    seen: dict[str, dict[str, object]] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            seen[str(key)] = dict(value)
        elif isinstance(value, list):
            seen[str(key)] = {
                "received_at": value[0] if value else None,
                "volume": value[1] if len(value) > 1 else None,
                "fingerprint": value[2] if len(value) > 2 else None,
            }
    return seen


def _pack_seen_options(
    seen: dict[str, dict[str, object]],
) -> dict[str, list[object]]:
    return {
        key: [
            value.get("received_at"),
            value.get("volume"),
            value.get("fingerprint"),
        ]
        for key, value in seen.items()
    }


def _persist_working_state(
    tape: dict[str, object],
    *,
    minutes: dict[str, dict[str, object]],
    seen: dict[str, dict[str, object]],
    session: dict[str, object],
) -> None:
    tape["minutes"] = {key: _pack_aggregate(value) for key, value in minutes.items()}
    tape["seen_options"] = _pack_seen_options(seen)
    tape["session"] = _pack_aggregate(session)


def _bucket(minutes: dict[str, dict[str, object]], minute_at: datetime) -> dict[str, object]:
    key = _minute_start(minute_at).isoformat()
    value = _new_aggregate(minutes.get(key), with_strikes=True)
    minutes[key] = value
    return value


def _strike_bucket(aggregate: dict[str, object], strike: float) -> dict[str, object]:
    raw_strikes = aggregate.get("strikes")
    strikes = raw_strikes if isinstance(raw_strikes, dict) else {}
    key = f"{strike:g}"
    raw_row = strikes.get(key)
    row = dict(raw_row) if isinstance(raw_row, dict) else {}
    row["strike"] = strike
    strikes[key] = row
    aggregate["strikes"] = strikes
    return row


def _add(aggregate: dict[str, object], field: str, value: float) -> None:
    aggregate[field] = _number(aggregate.get(field)) + value


def _record_volume(aggregate: dict[str, object], *, volume_delta: float) -> None:
    if volume_delta <= 0:
        return
    _add(aggregate, "volume_delta", volume_delta)


def _record_trade(
    aggregate: dict[str, object],
    *,
    strike: float,
    right: OptionRight,
    side: str,
    premium: float,
    size: float,
) -> None:
    prefix = "call" if right is OptionRight.CALL else "put"
    _add(aggregate, f"{prefix}_{side}", premium)
    _add(aggregate, "captured_premium", premium)
    _add(aggregate, "captured_size", size)
    _add(aggregate, "print_count", 1.0)
    strike_row = _strike_bucket(aggregate, strike)
    _add(strike_row, f"{prefix}_{side}", premium)
    if side == "unknown":
        _add(aggregate, "unknown_size", size)
        _add(aggregate, "unknown_print_count", 1.0)
        _add(strike_row, "unknown_print_count", 1.0)
        return
    signed = premium if side == "buy" else -premium
    _add(aggregate, f"{prefix}_net", signed)
    _add(aggregate, "classified_premium", premium)
    _add(aggregate, "classified_size", size)
    _add(aggregate, "classified_print_count", 1.0)
    _add(strike_row, "classified_print_count", 1.0)


def _option_expiry_matches_session(quote: Quote, session_date: str) -> bool:
    expiry = str(quote.instrument.expiry or "").replace("-", "")
    return expiry == session_date.replace("-", "")


def _fresh_schwab_zero_dte_options(
    quotes: tuple[Quote, ...], *, decision_at: datetime, session_date: str
) -> tuple[Quote, ...]:
    latest_by_instrument: dict[str, Quote] = {}
    for quote in quotes:
        if (
            quote.provider is not Provider.SCHWAB
            or quote.quality is not MarketDataQuality.LIVE
            or quote.sampling_mode != "schwab_stream"
            or quote.instrument.instrument_type is not InstrumentType.OPTION
            or (quote.instrument.underlier or quote.instrument.symbol) != "SPX"
            or quote.instrument.right not in {OptionRight.CALL, OptionRight.PUT}
            or quote.instrument.strike is None
            or not _option_expiry_matches_session(quote, session_date)
        ):
            continue
        received_at = as_utc(quote.received_at)
        age = (decision_at - received_at).total_seconds()
        if age < -5.0 or age > _MAX_QUOTE_AGE_SECONDS:
            continue
        instrument_id = quote.instrument.canonical_id
        previous = latest_by_instrument.get(instrument_id)
        if previous is None or as_utc(previous.received_at) < received_at:
            latest_by_instrument[instrument_id] = quote
    return tuple(latest_by_instrument.values())


def _last_trade_fingerprint(quote: Quote) -> str | None:
    if (
        quote.trade_time is None
        or quote.last is None
        or quote.last <= 0
        or quote.last_size is None
        or quote.last_size <= 0
    ):
        return None
    return "|".join(
        (
            as_utc(quote.trade_time).isoformat(),
            f"{float(quote.last):.10g}",
            f"{float(quote.last_size):.10g}",
        )
    )


def _combine(rows: list[dict[str, object]], *, with_strikes: bool = False) -> dict[str, object]:
    result = _new_aggregate(with_strikes=with_strikes)
    for row in rows:
        for field in _TOTAL_FIELDS:
            _add(result, field, _number(row.get(field)))
        if not with_strikes:
            continue
        raw_strikes = row.get("strikes")
        if not isinstance(raw_strikes, dict):
            continue
        for raw in raw_strikes.values():
            if not isinstance(raw, dict):
                continue
            strike = _number(raw.get("strike"))
            if strike <= 0:
                continue
            target = _strike_bucket(result, strike)
            for field in _STRIKE_STATE_FIELDS:
                _add(target, field, _number(raw.get(field)))
    return result


def _quality(aggregate: dict[str, object]) -> tuple[str, float, float, float]:
    volume_delta = _number(aggregate.get("volume_delta"))
    classified_size = _number(aggregate.get("classified_size"))
    captured_size = _number(aggregate.get("captured_size"))
    unknown_size = _number(aggregate.get("unknown_size"))
    coverage = classified_size / volume_delta if volume_delta > 0 else 0.0
    capture_ratio = captured_size / volume_delta if volume_delta > 0 else 0.0
    inside_share = unknown_size / captured_size if captured_size > 0 else 0.0
    if volume_delta <= 0 or classified_size <= 0:
        quality = "unavailable"
    elif coverage < _MIN_COVERAGE or inside_share > _MAX_INSIDE_SHARE:
        quality = "low_coverage"
    elif coverage < _MEDIUM_COVERAGE:
        quality = "low"
    else:
        quality = "medium"
    return quality, coverage, capture_ratio, inside_share


def _summary(
    aggregate: dict[str, object], *, minutes: int, price_rows: list[dict[str, object]] | None = None
) -> dict[str, object]:
    quality, coverage, capture_ratio, inside_share = _quality(aggregate)
    call_net = _number(aggregate.get("call_net"))
    put_net = _number(aggregate.get("put_net"))
    directional = call_net - put_net
    classified_premium = _number(aggregate.get("classified_premium"))
    imbalance = directional / classified_premium if classified_premium > 0 else 0.0
    summary: dict[str, object] = {
        "minutes": minutes,
        **{field: _number(aggregate.get(field)) for field in _PREMIUM_FIELDS},
        "call_net": call_net,
        "put_net": put_net,
        "directional_net": directional,
        "classified_premium": classified_premium,
        "captured_premium": _number(aggregate.get("captured_premium")),
        "classified_size": _number(aggregate.get("classified_size")),
        "captured_size": _number(aggregate.get("captured_size")),
        "volume_delta": _number(aggregate.get("volume_delta")),
        "classified_print_count": int(_number(aggregate.get("classified_print_count"))),
        "unknown_print_count": int(_number(aggregate.get("unknown_print_count"))),
        "flow_imbalance": imbalance,
        "coverage": coverage,
        "capture_ratio": capture_ratio,
        "inside_share": inside_share,
        "quality": quality,
    }
    if price_rows:
        prices = [_number(row.get("spx")) for row in price_rows if _number(row.get("spx")) > 0]
        if prices:
            summary.update(
                {
                    "spx_start": prices[0],
                    "spx_end": prices[-1],
                    "spx_high": max(prices),
                    "spx_low": min(prices),
                    "spx_change": prices[-1] - prices[0],
                }
            )
    return summary


def _strike_rows(aggregate: dict[str, object]) -> list[dict[str, object]]:
    raw_strikes = aggregate.get("strikes")
    if not isinstance(raw_strikes, dict):
        return []
    rows: list[dict[str, object]] = []
    for raw in raw_strikes.values():
        if not isinstance(raw, dict):
            continue
        strike = _number(raw.get("strike"))
        call_buy = _number(raw.get("call_buy"))
        call_sell = _number(raw.get("call_sell"))
        call_unknown = _number(raw.get("call_unknown"))
        put_buy = _number(raw.get("put_buy"))
        put_sell = _number(raw.get("put_sell"))
        put_unknown = _number(raw.get("put_unknown"))
        classified = call_buy + call_sell + put_buy + put_sell
        captured = classified + call_unknown + put_unknown
        call_net = call_buy - call_sell
        put_net = put_buy - put_sell
        directional = call_net - put_net
        if strike <= 0 or captured <= 0:
            continue
        rows.append(
            {
                "strike": strike,
                **{field: _number(raw.get(field)) for field in _PREMIUM_FIELDS},
                "call_net": call_net,
                "put_net": put_net,
                "directional_net": directional,
                "classified_premium": classified,
                "flow_imbalance": directional / classified if classified > 0 else 0.0,
                "classified_print_count": int(_number(raw.get("classified_print_count"))),
                "unknown_print_count": int(_number(raw.get("unknown_print_count"))),
            }
        )
    return sorted(rows, key=lambda row: float(row["strike"]))


def _top_strikes(rows: list[dict[str, object]], *, bullish: bool) -> list[dict[str, object]]:
    selected = [
        row
        for row in rows
        if (
            (bullish and _number(row.get("directional_net")) > 0)
            or (not bullish and _number(row.get("directional_net")) < 0)
        )
        and _number(row.get("classified_premium")) > 0
    ]
    return sorted(
        selected,
        key=lambda row: _number(row.get("directional_net")),
        reverse=bullish,
    )[:3]


def _usable(summary: dict[str, object]) -> bool:
    return str(summary.get("quality") or "") in {"low", "medium"}


def _flow_side(summary: dict[str, object]) -> str | None:
    if not _usable(summary):
        return None
    imbalance = _number(summary.get("flow_imbalance"))
    if imbalance >= _IMBALANCE_THRESHOLD:
        return "bullish"
    if imbalance <= -_IMBALANCE_THRESHOLD:
        return "bearish"
    return "mixed"


def _sentiment(rolling_15m: dict[str, object], session: dict[str, object]) -> str:
    if _number(rolling_15m.get("minutes")) < 5 or _number(session.get("minutes")) < 5:
        return "LOW_COVERAGE"
    rolling_side, session_side = _flow_side(rolling_15m), _flow_side(session)
    if rolling_side is None or session_side is None:
        return "LOW_COVERAGE"
    if rolling_side == session_side == "bullish":
        return "BULLISH_CONFIRM"
    if rolling_side == session_side == "bearish":
        return "BEARISH_CONFIRM"
    if rolling_side == "bullish" and session_side == "bearish":
        return "FLOW_REVERSAL_UP"
    if rolling_side == "bearish" and session_side == "bullish":
        return "FLOW_REVERSAL_DOWN"
    return "MIXED"


def _format_dollars(value: object) -> str:
    number = _number(value)
    sign = "+" if number >= 0 else "-"
    absolute = abs(number)
    if absolute >= 1_000_000:
        return f"{sign}${absolute / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{sign}${absolute / 1_000:.0f}K"
    return f"{sign}${absolute:.0f}"


def _format_top(rows: object) -> str:
    if not isinstance(rows, list) or not rows:
        return "-"
    return "/".join(f"{_number(row.get('strike')):g}" for row in rows if isinstance(row, dict))


def _desk_summary(snapshot: dict[str, object]) -> str:
    rollups = snapshot.get("rollups")
    rolling = rollups.get("15m") if isinstance(rollups, dict) else None
    session = snapshot.get("session")
    strike_window = "strike_flow_5m" if snapshot.get("divergence") != "NONE" else "strike_flow_15m"
    strikes = snapshot.get(strike_window)
    if not isinstance(rolling, dict) or not isinstance(session, dict):
        return "0DTE捕获流 unavailable"
    quality = str(rolling.get("quality") or "unavailable")
    if quality in {"unavailable", "low_coverage"}:
        return f"0DTE捕获流 unavailable（覆盖 {_number(rolling.get('coverage')):.0%}）"
    bullish = strikes.get("bullish_top") if isinstance(strikes, dict) else None
    bearish = strikes.get("bearish_top") if isinstance(strikes, dict) else None
    return (
        f"0DTE捕获流 15m C {_format_dollars(rolling.get('call_net'))} / "
        f"P {_format_dollars(rolling.get('put_net'))} / "
        f"Dir {_format_dollars(rolling.get('directional_net'))} · "
        f"全日 {_format_dollars(session.get('directional_net'))} · "
        f"覆盖 {_number(rolling.get('coverage')):.0%} {quality} · "
        f"{snapshot.get('sentiment') or 'MIXED'} · "
        f"熊流 {_format_top(bearish)} / 牛流 {_format_top(bullish)}"
    )


def _snapshot(
    rows: list[tuple[datetime, dict[str, object]]], session: dict[str, object]
) -> dict[str, object]:
    rollups: dict[str, dict[str, object]] = {}
    for window in _ROLLING_WINDOWS:
        selected = rows[-window:]
        buckets = [row for _, row in selected]
        rollups[f"{window}m"] = _summary(
            _combine(buckets), minutes=len(buckets), price_rows=buckets
        )
    selected_15m = [row for _, row in rows[-15:]]
    selected_5m = [row for _, row in rows[-5:]]
    strike_5m = _strike_rows(_combine(selected_5m, with_strikes=True))
    strike_15m = _strike_rows(_combine(selected_15m, with_strikes=True))
    strike_session = _strike_rows(session)
    session_summary = _summary(
        session,
        minutes=int(_number(session.get("minute_count"))),
    )
    previous_5m = [row for _, row in rows[-10:-5]]
    previous_summary = _summary(_combine(previous_5m), minutes=len(previous_5m))
    current_5m = rollups["5m"]

    def strike_projection(strike_rows: list[dict[str, object]]) -> dict[str, object]:
        ranked = sorted(
            strike_rows,
            key=lambda row: abs(_number(row.get("directional_net"))),
            reverse=True,
        )[:_SNAPSHOT_STRIKE_LIMIT]
        return {
            "row_count": len(strike_rows),
            "rows": sorted(ranked, key=lambda row: _number(row.get("strike"))),
            "bullish_top": _top_strikes(strike_rows, bullish=True),
            "bearish_top": _top_strikes(strike_rows, bullish=False),
        }

    snapshot = {
        "schema_version": _POLICY_VERSION,
        "rollups": rollups,
        "session": session_summary,
        "flow_acceleration_5m": _number(current_5m.get("flow_imbalance"))
        - _number(previous_summary.get("flow_imbalance")),
        "sentiment": _sentiment(rollups["15m"], session_summary),
        "divergence": "NONE",
        "strike_flow_5m": strike_projection(strike_5m),
        "strike_flow_15m": strike_projection(strike_15m),
        "strike_flow_session": strike_projection(strike_session),
        "volatility_15m": _volatility_15m(rows),
    }
    snapshot["desk_summary"] = _desk_summary(snapshot)
    return snapshot


def _volatility_15m(rows: list[tuple[datetime, dict[str, object]]]) -> dict[str, object]:
    """Compare exact causal ATM observations 15 minutes apart."""

    unavailable: dict[str, object] = {"quality": "unavailable", "atm_iv_change": None,
                                      "atm_straddle_decay": None, "contracted": False}
    if not rows:
        return unavailable
    current_at, current = rows[-1]
    target_at = current_at - timedelta(minutes=15)
    prior = next((row for at, row in rows if at == target_at), None)
    if prior is None:
        return unavailable
    current_iv = _optional_number(current.get("atm_iv"))
    prior_iv = _optional_number(prior.get("atm_iv"))
    current_straddle = _optional_number(current.get("atm_straddle"))
    prior_straddle = _optional_number(prior.get("atm_straddle"))
    values = (current_iv, prior_iv, current_straddle, prior_straddle)
    if any(value is None or value <= 0 for value in values):
        return unavailable
    assert all(value is not None for value in values)
    iv_change = current_iv - prior_iv
    straddle_decay = (prior_straddle - current_straddle) / prior_straddle
    return {
        "quality": "ready",
        "observed_at": current_at.isoformat(),
        "atm_iv": current_iv,
        "atm_iv_change": iv_change,
        "atm_straddle": current_straddle,
        "atm_straddle_decay": straddle_decay,
        "contracted": iv_change < 0 and straddle_decay > 0,
    }


def _retain_active_divergence(
    snapshot: dict[str, object], tape: dict[str, object], *, decision_at: datetime
) -> None:
    raw = tape.get("last_divergence")
    divergence = dict(raw) if isinstance(raw, dict) else {}
    expires_at = _parse_datetime(divergence.get("expires_at"))
    direction = str(divergence.get("direction") or "")
    if expires_at is None or decision_at > expires_at or direction not in {"BEARISH", "BULLISH"}:
        return
    snapshot["divergence"] = direction
    snapshot["sentiment"] = f"{direction}_DIVERGENCE"
    snapshot["desk_summary"] = _desk_summary(snapshot)


def _divergence_alert(
    *,
    kind: str,
    session_date: str,
    decision_at: datetime,
    signal_minute: datetime,
    current_spx: float,
    extreme: float,
    pretrend_points: float,
    rejection_points: float,
    recent: dict[str, object],
    session: dict[str, object],
    volatility: dict[str, object],
) -> Alert:
    bearish = kind == NET_PREMIUM_BEARISH_DIVERGENCE_KIND
    direction = "bearish" if bearish else "bullish"
    signal_time_et = as_utc(signal_minute).astimezone(_ET).time()
    late_contraction = signal_time_et >= _LATE_MANAGEMENT_START_ET and (
        volatility.get("contracted") is True
    )
    stamp = as_utc(signal_minute).strftime("%H%M")
    event_id = f"spx_net_premium_{direction}_divergence:{session_date.replace('-', '')}:{stamp}"
    label = "熊背离" if bearish else "牛背离"
    management_label = "止盈/减仓提醒" if late_contraction else "离场提醒"
    title = f"SPX {label}{management_label}：局部{'新高' if bearish else '新低'}失败，方向净权利金转{'空' if bearish else '多'}"
    path = (
        f"SPX 近 5 分钟冲出 {extreme:.1f} 后回落至 {current_spx:.1f}"
        if bearish
        else f"SPX 近 5 分钟下探 {extreme:.1f} 后回升至 {current_spx:.1f}"
    )
    if late_contraction:
        action = (
            "持有 Call/多头盈利仓只止盈或减仓；不追 Put、不反手、不生成新方向交易"
            if bearish
            else "持有 Put/空头盈利仓只止盈或减仓；不追 Call、不反手、不生成新方向交易"
        )
        authority = "take_profit_reduce_only"
        position_action = "take_profit_or_reduce_long" if bearish else "take_profit_or_reduce_short"
    else:
        action = (
            "持有 Call/多头方向仓应离场；无对应持仓不追 Put、不反手"
            if bearish
            else "持有 Put/空头方向仓应离场；无对应持仓不追 Call、不反手"
        )
        authority = "conditional_exit_alert"
        position_action = "exit_long_direction" if bearish else "exit_short_direction"
    volatility_detail = ""
    if late_contraction:
        volatility_detail = (
            f"13:30 ET 后 ATM IV 15m {_number(volatility.get('atm_iv_change')):+.2%}、"
            f"跨式衰减 {_number(volatility.get('atm_straddle_decay')):.1%}；"
        )
    return Alert(
        severity="high",
        kind=kind,
        instrument_id="index:SPX",
        title=title,
        detail=(
            f"{path}；捕获的 0DTE 成交代理显示 "
            f"Call 净权利金 {_format_dollars(recent.get('call_net'))}、"
            f"Put 净权利金 {_format_dollars(recent.get('put_net'))}、"
            f"方向净值 {_format_dollars(recent.get('directional_net'))}，"
            f"可分类成交量覆盖约 {_number(recent.get('coverage')):.1%}。"
            f"此前同向推进 {pretrend_points:.1f} 点，本次拒绝 {rejection_points:.1f} 点。"
            f"{volatility_detail}"
            f"这是{label}条件式人工离场提醒：{action}。"
            f"资金流图 {_FLOW_IMAGE_URL}"
        ),
        provider=Provider.SCHWAB.value,
        quality=MarketDataQuality.LIVE.value,
        value=current_spx,
        threshold=extreme,
        research_only=False,
        source_gate=f"captured_net_premium_proxy_{direction}_divergence_v4",
        dedup_group=f"{event_id}:observe",
        event_id=event_id,
        source_at=as_utc(decision_at).isoformat(),
        cooldown_seconds=float(_COOLDOWN_SECONDS),
        audit_context={
            "policy_version": _POLICY_VERSION,
            "authority": authority,
            "execution_eligible": False,
            "automatic_ordering": False,
            "new_entry_eligible": False,
            "reverse_eligible": False,
            "direction": direction,
            "position_action": position_action,
            "pre_divergence_trend": "up" if bearish else "down",
            "pretrend_points": pretrend_points,
            "rejection_points": rejection_points,
            "late_contraction": late_contraction,
            "atm_iv_change_15m": volatility.get("atm_iv_change"),
            "atm_straddle_decay_15m": volatility.get("atm_straddle_decay"),
            "signal_minute": as_utc(signal_minute).isoformat(),
            "call_net_premium_dollars": recent.get("call_net"),
            "put_net_premium_dollars": recent.get("put_net"),
            "directional_net_premium_dollars": recent.get("directional_net"),
            "flow_imbalance": recent.get("flow_imbalance"),
            "at_touch_volume_coverage": recent.get("coverage"),
            "session_directional_net_premium_dollars": session.get("directional_net"),
            "classification": "last_at_ask_buy_last_at_bid_sell_inside_unknown",
            "known_limit": "schwab_l1_captured_last_trade_proxy_not_full_opra_tape_or_bto_stc",
        },
    )


def advance_captured_option_flow(
    state: dict[str, object],
    sample: PriceSample,
    *,
    quotes: tuple[Quote, ...],
    decision_at: datetime,
    session_date: str,
    atm_iv: float | None = None,
    atm_straddle: float | None = None,
) -> tuple[dict[str, object], list[Alert]]:
    """Advance causal flow facts and emit position-management divergences."""

    state = dict(state)
    decision_at = as_utc(decision_at)
    raw_tape = state.get(_FLOW_STATE_KEY)
    tape = dict(raw_tape) if isinstance(raw_tape, dict) else {}
    raw_minutes = tape.get("minutes")
    minutes = (
        {
            str(key): _new_aggregate(value, with_strikes=True)
            for key, value in raw_minutes.items()
            if isinstance(value, dict)
        }
        if isinstance(raw_minutes, dict)
        else {}
    )
    seen = _decode_seen_options(tape.get("seen_options"))
    session = _new_aggregate(tape.get("session"), with_strikes=True)
    session.setdefault("started_at", decision_at.isoformat())

    current_bucket = _bucket(minutes, sample.at)
    current_bucket["spx"] = float(sample.spx)
    if atm_iv is not None and math.isfinite(atm_iv) and atm_iv > 0:
        current_bucket["atm_iv"] = float(atm_iv)
    if atm_straddle is not None and math.isfinite(atm_straddle) and atm_straddle > 0:
        current_bucket["atm_straddle"] = float(atm_straddle)

    for quote in _fresh_schwab_zero_dte_options(
        quotes, decision_at=decision_at, session_date=session_date
    ):
        instrument_id = quote.instrument.canonical_id
        prior_raw = seen.get(instrument_id)
        prior = dict(prior_raw) if isinstance(prior_raw, dict) else None
        received_at = as_utc(quote.received_at)
        received_iso = received_at.isoformat()
        if prior is not None and prior.get("received_at") == received_iso:
            continue
        strike = float(quote.instrument.strike or 0.0)
        volume = _number(quote.volume) if quote.volume is not None and quote.volume >= 0 else None
        prior_volume_raw = prior.get("volume") if prior is not None else None
        prior_volume = (
            _number(prior_volume_raw)
            if isinstance(prior_volume_raw, int | float) and prior_volume_raw >= 0
            else None
        )
        volume_delta = (
            max(float(volume) - prior_volume, 0.0)
            if volume is not None and prior_volume is not None
            else 0.0
        )
        fingerprint = _last_trade_fingerprint(quote)
        previous_fingerprint = str(prior.get("fingerprint") or "") if prior else ""
        seen[instrument_id] = {
            "received_at": received_iso,
            "volume": volume,
            "fingerprint": fingerprint,
        }
        quote_bucket = _bucket(minutes, received_at)
        _record_volume(quote_bucket, volume_delta=volume_delta)
        _record_volume(session, volume_delta=volume_delta)

        if prior is None or fingerprint is None or fingerprint == previous_fingerprint:
            continue
        trade_at = as_utc(quote.trade_time) if quote.trade_time is not None else None
        trade_lag = (received_at - trade_at).total_seconds() if trade_at is not None else math.inf
        if (
            trade_lag < 0
            or trade_lag > _MAX_TRADE_LAG_SECONDS
            or quote.bid is None
            or quote.ask is None
            or quote.last is None
            or quote.last_size is None
            or quote.bid < 0
            or quote.ask <= quote.bid
            or quote.last <= 0
            or quote.last_size <= 0
        ):
            continue
        side = (
            "buy" if quote.last >= quote.ask else "sell" if quote.last <= quote.bid else "unknown"
        )
        premium = float(quote.last) * float(quote.last_size) * 100.0
        for target in (quote_bucket, session):
            _record_trade(
                target,
                strike=strike,
                right=quote.instrument.right,
                side=side,
                premium=premium,
                size=float(quote.last_size),
            )

    history_cutoff = decision_at - timedelta(minutes=_HISTORY_MINUTES)
    minutes = {
        key: value
        for key, value in minutes.items()
        if (parsed := _parse_datetime(key)) is not None and parsed >= history_cutoff
    }
    strike_history_cutoff = _minute_start(decision_at) - timedelta(minutes=_STRIKE_HISTORY_MINUTES)
    for key, value in minutes.items():
        minute_at = _parse_datetime(key)
        if minute_at is not None and minute_at < strike_history_cutoff:
            value.pop("strikes", None)
    seen_cutoff = decision_at - timedelta(minutes=_SEEN_RETENTION_MINUTES)
    seen = {
        key: value
        for key, value in seen.items()
        if (parsed := _parse_datetime(value.get("received_at"))) is not None
        and parsed >= seen_cutoff
    }
    session["updated_at"] = decision_at.isoformat()
    tape.update({"schema_version": _POLICY_VERSION, "updated_at": decision_at.isoformat()})

    completed_minute = _minute_start(decision_at) - timedelta(minutes=1)
    last_evaluated = _parse_datetime(tape.get("last_evaluated_minute"))
    if last_evaluated is not None and last_evaluated >= completed_minute:
        _persist_working_state(tape, minutes=minutes, seen=seen, session=session)
        state[_FLOW_STATE_KEY] = tape
        return state, []
    tape["last_evaluated_minute"] = completed_minute.isoformat()
    session["minute_count"] = _number(session.get("minute_count")) + 1.0

    rows: list[tuple[datetime, dict[str, object]]] = []
    for key, row in minutes.items():
        minute_at = _parse_datetime(key)
        if minute_at is not None and minute_at <= completed_minute and _number(row.get("spx")) > 0:
            rows.append((minute_at, row))
    rows.sort(key=lambda item: item[0])
    snapshot = _snapshot(rows, session)
    _retain_active_divergence(snapshot, tape, decision_at=decision_at)
    alerts: list[Alert] = []
    required = _LOOKBACK_MINUTES + _CONFIRM_MINUTES
    window = rows[-required:]
    signal_time_et = completed_minute.astimezone(_ET).time()
    contiguous = len(window) == required and all(
        (window[index][0] - window[index - 1][0]).total_seconds() == 60
        for index in range(1, len(window))
    )
    if contiguous and _START_ET <= signal_time_et <= _END_ET:
        prior, recent = window[:_LOOKBACK_MINUTES], window[_LOOKBACK_MINUTES:]
        previous_flow = _summary(_combine([row for _, row in prior[-5:]]), minutes=5)
        recent_rows = [row for _, row in recent]
        recent_flow = _summary(_combine(recent_rows), minutes=5, price_rows=recent_rows)
        prior_high = max(_number(row.get("spx")) for _, row in prior)
        recent_high = max(_number(row.get("spx")) for _, row in recent)
        prior_low = min(_number(row.get("spx")) for _, row in prior)
        recent_low = min(_number(row.get("spx")) for _, row in recent)
        current_spx = _number(recent[-1][1].get("spx"))
        recent_imbalance = _number(recent_flow.get("flow_imbalance"))
        previous_imbalance = _number(previous_flow.get("flow_imbalance"))
        bearish = (
            _usable(recent_flow)
            and recent_high > prior_high
            and current_spx < recent_high
            and _number(recent_flow.get("directional_net")) < 0
            and recent_imbalance < previous_imbalance
        )
        bullish = (
            _usable(recent_flow)
            and recent_low < prior_low
            and current_spx > recent_low
            and _number(recent_flow.get("directional_net")) > 0
            and recent_imbalance > previous_imbalance
        )
        direction_key = "BEARISH" if bearish else "BULLISH" if bullish else ""
        raw_alert_times = tape.get("last_alert_at_by_direction")
        alert_times = dict(raw_alert_times) if isinstance(raw_alert_times, dict) else {}
        last_alert_at = _parse_datetime(alert_times.get(direction_key))
        last_divergence = tape.get("last_divergence")
        if last_alert_at is None and isinstance(last_divergence, dict):
            if str(last_divergence.get("direction") or "") == direction_key:
                last_alert_at = _parse_datetime(tape.get("last_alert_at"))
        cooldown_active = (
            last_alert_at is not None
            and (decision_at - last_alert_at).total_seconds() < _COOLDOWN_SECONDS
        )
        if (bearish or bullish) and not cooldown_active:
            kind = (
                NET_PREMIUM_BEARISH_DIVERGENCE_KIND
                if bearish
                else NET_PREMIUM_BULLISH_DIVERGENCE_KIND
            )
            snapshot["sentiment"] = "BEARISH_DIVERGENCE" if bearish else "BULLISH_DIVERGENCE"
            snapshot["divergence"] = "BEARISH" if bearish else "BULLISH"
            snapshot["desk_summary"] = _desk_summary(snapshot)
            extreme = recent_high if bearish else recent_low
            prior_origin = _number(prior[0][1].get("spx"))
            pretrend_points = max(
                (extreme - prior_origin) if bearish else (prior_origin - extreme),
                0.0,
            )
            rejection_points = abs(current_spx - extreme)
            raw_volatility = snapshot.get("volatility_15m")
            volatility = dict(raw_volatility) if isinstance(raw_volatility, dict) else {}
            alert = _divergence_alert(
                kind=kind,
                session_date=session_date,
                decision_at=decision_at,
                signal_minute=completed_minute,
                current_spx=current_spx,
                extreme=extreme,
                pretrend_points=pretrend_points,
                rejection_points=rejection_points,
                recent=recent_flow,
                session=snapshot["session"],
                volatility=volatility,
            )
            alerts.append(alert)
            tape["last_alert_at"] = decision_at.isoformat()
            alert_times[direction_key] = decision_at.isoformat()
            tape["last_alert_at_by_direction"] = alert_times
            tape["last_alert_event_id"] = alert.event_id
            tape["last_divergence"] = {
                "direction": "BEARISH" if bearish else "BULLISH",
                "signal_at": completed_minute.isoformat(),
                "expires_at": (decision_at + timedelta(seconds=_COOLDOWN_SECONDS)).isoformat(),
                "event_id": alert.event_id,
            }
            raw_events = tape.get("divergence_events")
            events = (
                [dict(row) for row in raw_events if isinstance(row, dict)]
                if isinstance(raw_events, list)
                else []
            )
            events.append(
                {
                    "direction": "BEARISH" if bearish else "BULLISH",
                    "signal_at": completed_minute.isoformat(),
                    "spx": current_spx,
                    "extreme_spx": extreme,
                    "pretrend_points": pretrend_points,
                    "rejection_points": rejection_points,
                    "management_mode": alert.audit_context.get("authority")
                    if alert.audit_context
                    else None,
                    "atm_iv_change_15m": volatility.get("atm_iv_change"),
                    "atm_straddle_decay_15m": volatility.get("atm_straddle_decay"),
                    "directional_net": recent_flow.get("directional_net"),
                    "coverage": recent_flow.get("coverage"),
                    "event_id": alert.event_id,
                }
            )
            tape["divergence_events"] = events[-20:]

    session_summary = snapshot["session"]
    tape["snapshot"] = snapshot
    tape["desk_summary"] = snapshot["desk_summary"]
    tape["coverage"] = session_summary.get("coverage") if isinstance(session_summary, dict) else 0.0
    tape["cumulative_classified_size"] = session.get("classified_size")
    tape["cumulative_volume_delta"] = session.get("volume_delta")
    _persist_working_state(tape, minutes=minutes, seen=seen, session=session)
    state[_FLOW_STATE_KEY] = tape
    return state, alerts
