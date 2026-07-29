"""Causal same-clock ranks for the observed 30-minute ES path."""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any


ROLLING_PATH_WINDOW_BARS = 6
MIN_ROLLING_PATH_PERCENTILE_SESSIONS = 5
TARGET_ROLLING_PATH_PERCENTILE_SESSIONS = 20
MIN_DEGRADED_BAR_SAMPLES = 30
MAX_DEGRADED_BAR_SAMPLE_GAP_SECONDS = 45.0
MAX_DEGRADED_BAR_EDGE_GAP_SECONDS = 30.0


def observed_rolling_path_state(
    bars: Sequence[Mapping[str, object]],
    *,
    strict_atr: float | None,
) -> dict[str, object]:
    """Build a dense Shadow path without weakening strict market-state inputs.

    A mildly partial bar remains usable only when it contains broad observed
    coverage and sits in a truly contiguous five-minute sequence. No missing
    price is filled and the degraded input can never claim more than low
    confidence.
    """

    tail = _observed_contiguous_tail(bars)
    window = tail[-ROLLING_PATH_WINDOW_BARS:]
    partial_count = sum(bar.get("quality") == "partial" for bar in window)
    if len(window) < ROLLING_PATH_WINDOW_BARS:
        return {
            "status": "warming",
            "reason": "rolling_path_requires_six_contiguous_observed_bars",
            "window_minutes": ROLLING_PATH_WINDOW_BARS * 5,
            "observed_bar_count": len(window),
            "input_quality": "unavailable",
            "partial_bar_count": partial_count,
        }

    atr = _number(strict_atr)
    atr_source = "strict_session_local_rth_atr"
    if atr is None or atr <= 0:
        atr = _observed_atr(tail[-14:])
        atr_source = "degraded_observed_rth_true_range"
    if atr is None or atr <= 0:
        return {
            "status": "warming",
            "reason": "rolling_path_atr_unavailable",
            "window_minutes": ROLLING_PATH_WINDOW_BARS * 5,
            "observed_bar_count": len(window),
            "input_quality": "degraded" if partial_count else "strict",
            "partial_bar_count": partial_count,
        }

    close = float(window[-1]["close"])
    high = max(float(bar["high"]) for bar in window)
    low = min(float(bar["low"]) for bar in window)
    dip_points = max(high - close, 0.0)
    rally_points = max(close - low, 0.0)
    latest_end = _datetime(window[-1].get("bar_end"))
    return {
        "status": "ready",
        "window_minutes": ROLLING_PATH_WINDOW_BARS * 5,
        "observed_bar_count": len(window),
        "latest_bar_end": latest_end.isoformat() if latest_end is not None else None,
        "close": round(close, 6),
        "rolling_high": round(high, 6),
        "rolling_low": round(low, 6),
        "dip_points": round(dip_points, 6),
        "rally_points": round(rally_points, 6),
        "atr_5m": round(atr, 6),
        "atr_source": atr_source,
        "dip_atr": round(dip_points / atr, 6),
        "rally_atr": round(rally_points / atr, 6),
        "input_quality": "degraded" if partial_count else "strict",
        "partial_bar_count": partial_count,
        "degraded_bar_policy": {
            "minimum_samples": MIN_DEGRADED_BAR_SAMPLES,
            "maximum_internal_gap_seconds": MAX_DEGRADED_BAR_SAMPLE_GAP_SECONDS,
            "maximum_edge_gap_seconds": MAX_DEGRADED_BAR_EDGE_GAP_SECONDS,
            "missing_prices_filled": False,
        },
    }


def rank_rolling_path_percentiles(
    *,
    current: Mapping[str, object],
    slot_et: str | None,
    baselines: Mapping[str, object],
    trading_date: date,
) -> dict[str, object]:
    """Rank one observed path against strictly earlier same-clock sessions.

    Five prior sessions are sufficient for a low-confidence shadow rank.  The
    rank is linearly shrunk toward 50% until twenty sessions are present, so a
    small sample remains visible without masquerading as a calibrated tail
    probability.
    """

    if current.get("status") != "ready":
        return _unavailable_result(current)
    if not slot_et:
        return _unavailable_result(
            current,
            status="unavailable",
            reason="rolling_path_slot_unavailable",
        )

    history: dict[date, tuple[float, float]] = {}
    rows = _mapping(baselines.get("slots")).get(slot_et)
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        observed_date = _date(row.get("trading_date_et"))
        if observed_date is None or observed_date >= trading_date:
            continue
        dip = _number(row.get("dip_atr_30m"))
        rally = _number(row.get("rally_atr_30m"))
        if dip is not None and dip >= 0 and rally is not None and rally >= 0:
            history[observed_date] = (dip, rally)

    ranked: dict[str, dict[str, object]] = {}
    sample_counts: list[int] = []
    paired_history = sorted(history.items(), key=lambda item: item[0])[
        -TARGET_ROLLING_PATH_PERCENTILE_SESSIONS:
    ]
    for name, current_field, tuple_index in (
        ("dip", "dip_atr", 0),
        ("rally", "rally_atr", 1),
    ):
        values = [
            pair[tuple_index]
            for _, pair in paired_history
        ]
        count = len(values)
        sample_counts.append(count)
        raw = (
            empirical_percentile(values, float(current[current_field]))
            if count >= MIN_ROLLING_PATH_PERCENTILE_SESSIONS
            else None
        )
        weight = min(count / TARGET_ROLLING_PATH_PERCENTILE_SESSIONS, 1.0)
        shrunk = 0.5 + weight * (raw - 0.5) if raw is not None else None
        ranked[name] = {
            "value_atr": current[current_field],
            "raw_percentile": round(raw, 6) if raw is not None else None,
            "shrunk_percentile": round(shrunk, 6) if shrunk is not None else None,
            "sample_count": count,
        }

    sample_count = min(sample_counts, default=0)
    status, historical_confidence = _sample_status(sample_count)
    input_quality = str(current.get("input_quality") or "strict")
    confidence = historical_confidence
    confidence_cap_reason = None
    if input_quality == "degraded" and historical_confidence != "unavailable":
        status = "provisional"
        confidence = "low"
        confidence_cap_reason = "mild_partial_bar_observed_shadow_only"
    dip_rank = _number(ranked["dip"].get("shrunk_percentile"))
    rally_rank = _number(ranked["rally"].get("shrunk_percentile"))
    return {
        **current,
        "status": status,
        "slot_et": slot_et,
        "sample_count": sample_count,
        "minimum_sessions": MIN_ROLLING_PATH_PERCENTILE_SESSIONS,
        "target_sessions": TARGET_ROLLING_PATH_PERCENTILE_SESSIONS,
        "confidence": confidence,
        "historical_sample_confidence": historical_confidence,
        "confidence_cap_reason": confidence_cap_reason,
        "dip": ranked["dip"],
        "rally": ranked["rally"],
        "signed_path_bias": (
            round(rally_rank - dip_rank, 6)
            if dip_rank is not None and rally_rank is not None
            else None
        ),
        "shrinkage": "linear_to_50pct_until_20_prior_sessions",
        "probability_semantics": "historical_rank_not_forward_probability",
        "action_authority": "none",
    }


def _observed_contiguous_tail(
    bars: Sequence[Mapping[str, object]],
) -> list[Mapping[str, object]]:
    tail: list[Mapping[str, object]] = []
    previous_start: datetime | None = None
    for bar in sorted(bars, key=lambda row: str(row.get("bar_start") or "")):
        start = _datetime(bar.get("bar_start"))
        if start is None or not _observed_bar_usable(bar):
            tail = []
            previous_start = None
            continue
        continuous = previous_start is not None and start == previous_start + timedelta(
            minutes=5
        )
        if continuous and tail:
            prior_identity = str(tail[-1].get("contract_identity") or "")
            current_identity = str(bar.get("contract_identity") or "")
            continuous = bool(prior_identity and prior_identity == current_identity)
        if continuous and bar.get("gap_before") is True:
            continuous = bool(tail and tail[-1].get("quality") == "partial")
        if (
            continuous
            and bar.get("quality") == "partial"
            and any(item.get("quality") == "partial" for item in tail)
        ):
            continuous = False
        tail = [*tail, bar] if continuous else [bar]
        previous_start = start
    return tail


def _observed_bar_usable(bar: Mapping[str, object]) -> bool:
    start = _datetime(bar.get("bar_start"))
    end = _datetime(bar.get("bar_end"))
    if (
        start is None
        or end is None
        or end != start + timedelta(minutes=5)
    ):
        return False
    if any(_number(bar.get(field)) is None for field in ("open", "high", "low", "close")):
        return False
    if bar.get("quality") == "ok":
        return True
    if bar.get("quality") != "partial":
        return False
    sample_count = _number(bar.get("sample_count"))
    sample_gap = _number(bar.get("max_sample_gap_seconds"))
    leading_gap = _number(bar.get("leading_edge_gap_seconds"))
    trailing_gap = _number(bar.get("trailing_edge_gap_seconds"))
    return bool(
        sample_count is not None
        and sample_count >= MIN_DEGRADED_BAR_SAMPLES
        and sample_gap is not None
        and 0 <= sample_gap <= MAX_DEGRADED_BAR_SAMPLE_GAP_SECONDS
        and leading_gap is not None
        and 0 <= leading_gap <= MAX_DEGRADED_BAR_EDGE_GAP_SECONDS
        and trailing_gap is not None
        and 0 <= trailing_gap <= MAX_DEGRADED_BAR_EDGE_GAP_SECONDS
        and bar.get("gap_before") is not True
        and bar.get("contract_identity_ambiguous") is not True
        and bool(str(bar.get("contract_identity") or ""))
    )


def _observed_atr(bars: Sequence[Mapping[str, object]]) -> float | None:
    if len(bars) < ROLLING_PATH_WINDOW_BARS:
        return None
    true_ranges: list[float] = []
    previous: Mapping[str, object] | None = None
    for bar in bars:
        high = _number(bar.get("high"))
        low = _number(bar.get("low"))
        if high is None or low is None:
            return None
        prior_close = _number(previous.get("close")) if previous is not None else None
        true_ranges.append(
            max(high - low, abs(high - prior_close), abs(low - prior_close))
            if prior_close is not None
            else high - low
        )
        previous = bar
    return statistics.fmean(true_ranges)


def empirical_percentile(values: Sequence[float], current: float) -> float:
    """Return a bounded mid-rank with half an observation at each endpoint."""

    less = sum(value < current for value in values)
    equal = sum(value == current for value in values)
    return (less + 0.5 * equal + 0.5) / (len(values) + 1.0)


def _unavailable_result(
    current: Mapping[str, object],
    *,
    status: str | None = None,
    reason: str | None = None,
) -> dict[str, object]:
    return {
        **current,
        **({"status": status} if status else {}),
        **({"reason": reason} if reason else {}),
        "sample_count": 0,
        "minimum_sessions": MIN_ROLLING_PATH_PERCENTILE_SESSIONS,
        "target_sessions": TARGET_ROLLING_PATH_PERCENTILE_SESSIONS,
        "confidence": "unavailable",
        "probability_semantics": "historical_rank_not_forward_probability",
        "action_authority": "none",
    }


def _sample_status(sample_count: int) -> tuple[str, str]:
    if sample_count >= TARGET_ROLLING_PATH_PERCENTILE_SESSIONS:
        return "ready", "high"
    if sample_count >= 10:
        return "provisional", "medium"
    if sample_count >= MIN_ROLLING_PATH_PERCENTILE_SESSIONS:
        return "provisional", "low"
    return "warming", "unavailable"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if number == number and abs(number) != float("inf") else None


def _date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value or ""))
    except ValueError:
        return None


def _datetime(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None
