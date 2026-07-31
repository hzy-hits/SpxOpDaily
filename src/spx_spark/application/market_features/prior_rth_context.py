"""Causal prior-RTH trajectory context for the following GTH session.

This module deliberately has no execution authority.  It preserves the
completed cash-session path and annotates later GTH plans with location-aware
chase risk instead of adding another global trade gate.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path

from spx_spark.application.market_features.state import load_json, save_json
from spx_spark.application.market_features.spx_standardized import (
    load_standardized_spx_samples,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.storage import LatestState


SCHEMA_VERSION = "prior_rth_context.v1"
MINUTE_COVERAGE_READY = 0.95
MINUTE_COVERAGE_PARTIAL = 0.70
SESSION_EDGE_TOLERANCE = timedelta(minutes=10)
SHOCK_RETURN_FRACTION = 0.01
EXTREME_LOCATION_FRACTION = 0.20
TAIL_WINDOW_MINUTES = 30


def prior_rth_context_path(data_root: str | Path) -> Path:
    return Path(data_root) / "latest" / "prior_rth_context.json"


def build_prior_rth_context(
    samples: Sequence[Mapping[str, object]],
    *,
    now: datetime,
    official_close: float | None = None,
) -> dict[str, object]:
    """Summarize the RTH session immediately preceding ``now``."""

    now = as_utc(now)
    trading_date = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    prior_date = DEFAULT_MARKET_CALENDAR.previous_trading_day(trading_date)
    session = DEFAULT_MARKET_CALENDAR.session(prior_date)
    base: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "status": "unavailable",
        "as_of": now.isoformat(),
        "for_trading_date": trading_date.isoformat(),
        "session_date": prior_date.isoformat(),
        "action_authority": "none",
        "execution_gate": False,
        "reasons": [],
    }
    if session is None:
        return {**base, "reasons": ["prior_rth_session_unavailable"]}

    points: list[tuple[datetime, float, float | None]] = []
    for sample in samples:
        at = _time(sample.get("at"))
        if at is None or at < session.open_at or at > session.close_at:
            continue
        instruments = sample.get("instruments")
        instruments = instruments if isinstance(instruments, Mapping) else {}
        spx = instruments.get("index:SPX")
        spx = spx if isinstance(spx, Mapping) else {}
        price = _number(spx.get("price"))
        if price is None:
            continue
        points.append((at, price, _number(spx.get("reference_close"))))
    points.sort(key=lambda item: item[0])
    if not points:
        return {**base, "reasons": ["prior_rth_spx_path_unavailable"]}

    unique_minutes = {
        at.replace(second=0, microsecond=0)
        for at, _price, _reference_close in points
    }
    expected_minutes = max(
        int((session.close_at - session.open_at).total_seconds() // 60),
        1,
    )
    coverage = min(len(unique_minutes) / expected_minutes, 1.0)
    open_lag = points[0][0] - session.open_at
    close_lag = session.close_at - points[-1][0]
    quality_reasons: list[str] = []
    if coverage < MINUTE_COVERAGE_READY:
        quality_reasons.append("prior_rth_minute_coverage_low")
    if open_lag > SESSION_EDGE_TOLERANCE:
        quality_reasons.append("prior_rth_open_missing")
    if close_lag > SESSION_EDGE_TOLERANCE:
        quality_reasons.append("prior_rth_close_missing")

    prices = [item[1] for item in points]
    open_price = prices[0]
    sampled_close = prices[-1]
    high = max(prices)
    low = min(prices)
    session_range = high - low
    close_price = _credible_official_close(
        official_close,
        sampled_close=sampled_close,
        high=high,
        low=low,
    )
    reference_close = next(
        (
            reference
            for _at, _price, reference in points
            if reference is not None and reference > 0
        ),
        None,
    )
    return_points = (
        close_price - reference_close if reference_close is not None else None
    )
    return_fraction = (
        return_points / reference_close
        if return_points is not None and reference_close
        else None
    )
    close_location = (
        min(max((close_price - low) / session_range, 0.0), 1.0)
        if session_range > 0
        else 0.5
    )
    open_to_close = close_price - open_price
    tail_start = session.close_at - timedelta(minutes=TAIL_WINDOW_MINUTES)
    tail_reference = _point_at_or_before(points, tail_start)
    tail_return_points = (
        close_price - tail_reference[1] if tail_reference is not None else None
    )
    tail_return_fraction = (
        tail_return_points / tail_reference[1]
        if tail_return_points is not None and tail_reference and tail_reference[1]
        else None
    )
    path_efficiency = _path_efficiency(prices)
    shock_direction = (
        "down"
        if return_fraction is not None and return_fraction <= -SHOCK_RETURN_FRACTION
        else "up"
        if return_fraction is not None and return_fraction >= SHOCK_RETURN_FRACTION
        else "none"
    )
    close_zone = (
        "lower"
        if close_location <= EXTREME_LOCATION_FRACTION
        else "upper"
        if close_location >= 1.0 - EXTREME_LOCATION_FRACTION
        else "middle"
    )
    path_class = _path_class(
        return_fraction=return_fraction,
        shock_direction=shock_direction,
        close_zone=close_zone,
        open_to_close=open_to_close,
        tail_return_points=tail_return_points,
    )
    ready = not quality_reasons and reference_close is not None
    if reference_close is None:
        quality_reasons.append("prior_reference_close_unavailable")
    return {
        **base,
        "status": "ready" if ready else "partial",
        "source": "normalized_spx_minute_samples",
        "session_open_at": session.open_at.isoformat(),
        "session_close_at": session.close_at.isoformat(),
        "sample_count": len(points),
        "minute_coverage": round(coverage, 6),
        "open": round(open_price, 4),
        "high": round(high, 4),
        "low": round(low, 4),
        "close": round(close_price, 4),
        "sampled_close": round(sampled_close, 4),
        "reference_close": (
            round(reference_close, 4) if reference_close is not None else None
        ),
        "range_points": round(session_range, 4),
        "return_points": (
            round(return_points, 4) if return_points is not None else None
        ),
        "return_fraction": (
            round(return_fraction, 8) if return_fraction is not None else None
        ),
        "open_to_close_points": round(open_to_close, 4),
        "close_location_fraction": round(close_location, 6),
        "tail_window_minutes": TAIL_WINDOW_MINUTES,
        "tail_return_points": (
            round(tail_return_points, 4)
            if tail_return_points is not None
            else None
        ),
        "tail_return_fraction": (
            round(tail_return_fraction, 8)
            if tail_return_fraction is not None
            else None
        ),
        "path_efficiency": (
            round(path_efficiency, 6) if path_efficiency is not None else None
        ),
        "shock_direction": shock_direction,
        "close_zone": close_zone,
        "path_class": path_class,
        "reasons": list(dict.fromkeys(quality_reasons)),
    }


def process_prior_rth_context(
    data_root: str | Path,
    samples: Sequence[Mapping[str, object]],
    latest: LatestState,
    *,
    now: datetime,
) -> dict[str, object]:
    """Load the durable prior session, or build it once while samples exist."""

    path = prior_rth_context_path(data_root)
    current = load_json(path)
    trading_date = DEFAULT_MARKET_CALENDAR.research_expiry(as_utc(now))
    prior_date = DEFAULT_MARKET_CALENDAR.previous_trading_day(trading_date)
    if (
        current.get("session_date") == prior_date.isoformat()
        and current.get("status") == "ready"
    ):
        return current
    quote = latest.best_quote("index:SPX")
    official_close = _number(quote.close) if quote is not None else None
    canonical_samples = load_standardized_spx_samples(data_root)
    source_samples = canonical_samples or list(samples)
    built = build_prior_rth_context(
        source_samples,
        now=now,
        official_close=official_close,
    )
    if (
        built.get("status") == "partial"
        and _number_signed(built.get("minute_coverage")) is not None
        and float(built["minute_coverage"]) < MINUTE_COVERAGE_PARTIAL
    ):
        built = {
            **built,
            "status": "unavailable",
            "reasons": list(
                dict.fromkeys(
                    [
                        *(str(item) for item in built.get("reasons") or ()),
                        "prior_rth_minute_coverage_unusable",
                    ]
                )
            ),
        }
    if built.get("status") in {"ready", "partial"}:
        save_json(path, built)
        return built
    if current.get("session_date") == prior_date.isoformat():
        return current
    save_json(path, built)
    return built


def prior_session_signal_view(
    context: Mapping[str, object] | None,
    *,
    direction: str | None = None,
    gth_position_fraction: float | None = None,
) -> dict[str, object]:
    """Return a bounded operator view and same-direction chase diagnosis."""

    if not isinstance(context, Mapping) or context.get("status") not in {
        "ready",
        "partial",
    }:
        return {
            "status": "unavailable",
            "action_authority": "none",
            "execution_gate": False,
            "chase_risk": "unknown",
        }
    shock_direction = str(context.get("shock_direction") or "none")
    close_zone = str(context.get("close_zone") or "middle")
    parsed_position = _bounded_fraction(gth_position_fraction)
    parsed_direction = direction if direction in {"up", "down"} else None
    same_direction = bool(
        parsed_direction is not None and parsed_direction == shock_direction
    )
    at_directional_extreme = bool(
        parsed_position is not None
        and (
            (parsed_direction == "down" and parsed_position <= 0.15)
            or (parsed_direction == "up" and parsed_position >= 0.85)
        )
    )
    near_directional_extreme = bool(
        parsed_position is not None
        and (
            (parsed_direction == "down" and parsed_position <= 0.30)
            or (parsed_direction == "up" and parsed_position >= 0.70)
        )
    )
    chase_risk = (
        "high"
        if same_direction and at_directional_extreme
        else "elevated"
        if same_direction and near_directional_extreme
        else "normal"
    )
    return {
        "status": str(context.get("status")),
        "session_date": context.get("session_date"),
        "return_fraction": context.get("return_fraction"),
        "return_points": context.get("return_points"),
        "close_location_fraction": context.get("close_location_fraction"),
        "tail_return_fraction": context.get("tail_return_fraction"),
        "shock_direction": shock_direction,
        "close_zone": close_zone,
        "path_class": context.get("path_class"),
        "gth_position_fraction": parsed_position,
        "same_direction_as_prior_shock": same_direction,
        "chase_risk": chase_risk,
        "action_authority": "none",
        "execution_gate": False,
        "semantics": "context_and_ordering_only_not_edge_or_execution_gate",
    }


def prior_session_operator_line(view: Mapping[str, object] | None) -> str:
    if not isinstance(view, Mapping) or view.get("status") == "unavailable":
        return "前日  走势上下文不可用；不据此补方向"
    return_fraction = _number_signed(view.get("return_fraction"))
    close_location = _bounded_fraction(view.get("close_location_fraction"))
    tail_return = _number_signed(view.get("tail_return_fraction"))
    change = f"{return_fraction:+.2%}" if return_fraction is not None else "-"
    location = (
        f"收于日内区间 {close_location:.0%}"
        if close_location is not None
        else "收盘位置未知"
    )
    tail = f"尾盘30m {tail_return:+.2%}" if tail_return is not None else "尾盘未知"
    chase = {
        "high": "本票同向追单风险高",
        "elevated": "本票同向追单风险偏高",
        "normal": "不构成同向极值追单",
        "unknown": "当前追单位置未知",
    }.get(str(view.get("chase_risk") or ""), "仅作位置参考")
    return f"前日  {change} · {location} · {tail}；{chase}"


def gth_position_fraction(market_es: Mapping[str, object] | None) -> float | None:
    if not isinstance(market_es, Mapping):
        return None
    price = _number(market_es.get("price"))
    high = _number(market_es.get("session_high"))
    low = _number(market_es.get("session_low"))
    if price is None or high is None or low is None or high <= low:
        return None
    return min(max((price - low) / (high - low), 0.0), 1.0)


def _path_class(
    *,
    return_fraction: float | None,
    shock_direction: str,
    close_zone: str,
    open_to_close: float,
    tail_return_points: float | None,
) -> str:
    if shock_direction == "down" and close_zone == "lower":
        return "shock_down_close_low"
    if shock_direction == "up" and close_zone == "upper":
        return "shock_up_close_high"
    if open_to_close < 0 and close_zone == "lower":
        return "trend_down_close_low"
    if open_to_close > 0 and close_zone == "upper":
        return "trend_up_close_high"
    if (
        return_fraction is not None
        and return_fraction < 0
        and tail_return_points is not None
        and tail_return_points > 0
    ):
        return "down_day_late_recovery"
    if (
        return_fraction is not None
        and return_fraction > 0
        and tail_return_points is not None
        and tail_return_points < 0
    ):
        return "up_day_late_fade"
    return "balanced_or_reversal"


def _credible_official_close(
    value: float | None,
    *,
    sampled_close: float,
    high: float,
    low: float,
) -> float:
    parsed = _number(value)
    if parsed is None:
        return sampled_close
    tolerance = max(5.0, (high - low) * 0.10)
    return parsed if abs(parsed - sampled_close) <= tolerance else sampled_close


def _path_efficiency(prices: Sequence[float]) -> float | None:
    if len(prices) < 2:
        return None
    gross = sum(abs(current - previous) for previous, current in zip(prices, prices[1:]))
    return abs(prices[-1] - prices[0]) / gross if gross > 0 else 0.0


def _point_at_or_before(
    points: Sequence[tuple[datetime, float, float | None]],
    target: datetime,
) -> tuple[datetime, float, float | None] | None:
    candidates = [item for item in points if item[0] <= target]
    return candidates[-1] if candidates else None


def _time(value: object) -> datetime | None:
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
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _number_signed(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _bounded_fraction(value: object) -> float | None:
    parsed = _number_signed(value)
    return parsed if parsed is not None and 0.0 <= parsed <= 1.0 else None


__all__ = [
    "SCHEMA_VERSION",
    "build_prior_rth_context",
    "gth_position_fraction",
    "prior_rth_context_path",
    "prior_session_operator_line",
    "prior_session_signal_view",
    "process_prior_rth_context",
]
