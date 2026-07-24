"""Provider-neutral ES five-minute OHLC sampling state.

The hot market-feature worker observes the freshest live ES quote every five
seconds.  This module turns those observations into non-overlapping 5-minute
bars without filling gaps or replaying duplicate provider timestamps.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.application.market_features.market import session_segment
from spx_spark.config import NY_TZ
from spx_spark.marketdata import as_utc
from spx_spark.settings.market_features import MarketFeatureSettings


SCHEMA_VERSION = "es_5m_bar_state.v1"
INTERVAL_SECONDS = 300
MAX_CLOSED_BARS = 432
MIN_OK_SAMPLES = 20
MAX_OK_GAP_SECONDS = 30.0
MAX_EDGE_GAP_SECONDS = 30.0
FUTURE_TOLERANCE_SECONDS = 5.0


def advance_es_bar_state(
    previous: Mapping[str, object] | None,
    sample: Mapping[str, object],
    *,
    now: datetime,
    policy: MarketFeatureSettings | None = None,
) -> dict[str, object]:
    """Advance the ES bar state with one fresh normalized market sample.

    Duplicate and out-of-order source timestamps are ignored. Missing buckets
    are never synthesized; the next real observation carries ``gap_before``.
    """

    at = as_utc(now)
    state = _valid_state(previous)
    quote = _es_quote(sample)
    source_at = _parse_at(quote.get("source_at")) if quote else None
    price = _number(quote.get("price")) if quote else None
    provider = str(quote.get("provider") or "unknown") if quote else "unknown"
    rejection = _rejection_reason(
        source_at=source_at,
        price=price,
        now=at,
        last_source_at=_parse_at(state.get("last_source_at")),
    )
    if rejection is not None:
        return _with_diagnostic(state, at=at, rejection=rejection)

    assert source_at is not None
    assert price is not None
    bar_start = _bucket_start(source_at)
    current = _mapping(state.get("current_bar"))
    closed = _bar_rows(state.get("closed_bars"))
    segment = session_segment(source_at, policy=policy)

    if current and _parse_at(current.get("bar_start")) == bar_start:
        current = _add_observation(
            current,
            source_at=source_at,
            price=price,
            provider=provider,
        )
    else:
        gap_before = False
        if current:
            finalized = _finalize_bar(current)
            closed = _upsert_closed(closed, finalized)
            previous_start = _parse_at(current.get("bar_start"))
            gap_before = bool(
                previous_start is None
                or bar_start > previous_start + timedelta(seconds=INTERVAL_SECONDS)
                or finalized.get("quality") != "ok"
            )
        current = _new_bar(
            bar_start=bar_start,
            source_at=source_at,
            price=price,
            provider=provider,
            segment=segment,
            gap_before=gap_before,
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "interval_seconds": INTERVAL_SECONDS,
        "updated_at": at.isoformat(),
        "last_source_at": source_at.isoformat(),
        "last_provider": provider,
        "current_bar": current,
        "closed_bars": closed[-MAX_CLOSED_BARS:],
        "diagnostics": {
            "last_rejection": None,
            "closed_bar_count": len(closed[-MAX_CLOSED_BARS:]),
            "sampling": "fresh_live_es_source_timestamps_no_gap_fill",
            "min_ok_samples": MIN_OK_SAMPLES,
            "max_ok_gap_seconds": MAX_OK_GAP_SECONDS,
            "max_edge_gap_seconds": MAX_EDGE_GAP_SECONDS,
        },
    }


def completed_es_bars(
    state: Mapping[str, object] | None,
) -> list[dict[str, object]]:
    """Return validated completed bars in chronological order."""

    return _bar_rows(_valid_state(state).get("closed_bars"))


def _valid_state(previous: Mapping[str, object] | None) -> dict[str, object]:
    if not isinstance(previous, Mapping) or previous.get("schema_version") != SCHEMA_VERSION:
        return {
            "schema_version": SCHEMA_VERSION,
            "interval_seconds": INTERVAL_SECONDS,
            "updated_at": None,
            "last_source_at": None,
            "last_provider": None,
            "current_bar": {},
            "closed_bars": [],
            "diagnostics": {
                "last_rejection": None,
                "closed_bar_count": 0,
                "sampling": "fresh_live_es_source_timestamps_no_gap_fill",
                "min_ok_samples": MIN_OK_SAMPLES,
                "max_ok_gap_seconds": MAX_OK_GAP_SECONDS,
                "max_edge_gap_seconds": MAX_EDGE_GAP_SECONDS,
            },
        }
    return dict(previous)


def _es_quote(sample: Mapping[str, object]) -> Mapping[str, object]:
    instruments = sample.get("instruments")
    if not isinstance(instruments, Mapping):
        return {}
    quote = instruments.get("future:ES")
    return quote if isinstance(quote, Mapping) else {}


def _rejection_reason(
    *,
    source_at: datetime | None,
    price: float | None,
    now: datetime,
    last_source_at: datetime | None,
) -> str | None:
    if source_at is None:
        return "es_source_timestamp_missing"
    if price is None or price <= 0:
        return "es_price_missing_or_invalid"
    if source_at > now + timedelta(seconds=FUTURE_TOLERANCE_SECONDS):
        return "es_source_timestamp_future"
    if last_source_at is not None and source_at <= last_source_at:
        return "es_source_timestamp_duplicate_or_out_of_order"
    return None


def _with_diagnostic(
    state: dict[str, object],
    *,
    at: datetime,
    rejection: str,
) -> dict[str, object]:
    result = dict(state)
    diagnostics = dict(_mapping(result.get("diagnostics")))
    diagnostics["last_rejection"] = rejection
    diagnostics["last_rejection_at"] = at.isoformat()
    diagnostics["closed_bar_count"] = len(_bar_rows(result.get("closed_bars")))
    result["diagnostics"] = diagnostics
    return result


def _new_bar(
    *,
    bar_start: datetime,
    source_at: datetime,
    price: float,
    provider: str,
    segment: str,
    gap_before: bool,
) -> dict[str, object]:
    return {
        "bar_start": bar_start.isoformat(),
        "bar_end": (bar_start + timedelta(seconds=INTERVAL_SECONDS)).isoformat(),
        "interval_seconds": INTERVAL_SECONDS,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "sample_count": 1,
        "first_source_at": source_at.isoformat(),
        "last_source_at": source_at.isoformat(),
        "max_sample_gap_seconds": 0.0,
        "provider_counts": {provider: 1},
        "provider": provider,
        "segment": segment,
        "trading_date_et": source_at.astimezone(NY_TZ).date().isoformat(),
        "gap_before": gap_before,
        "quality": "open",
    }


def _add_observation(
    current: Mapping[str, object],
    *,
    source_at: datetime,
    price: float,
    provider: str,
) -> dict[str, object]:
    result = dict(current)
    last_source_at = _parse_at(result.get("last_source_at"))
    gap = (
        max((source_at - last_source_at).total_seconds(), 0.0)
        if last_source_at is not None
        else 0.0
    )
    result["high"] = max(float(result["high"]), price)
    result["low"] = min(float(result["low"]), price)
    result["close"] = price
    result["sample_count"] = int(result.get("sample_count") or 0) + 1
    result["last_source_at"] = source_at.isoformat()
    result["max_sample_gap_seconds"] = max(
        float(result.get("max_sample_gap_seconds") or 0.0),
        gap,
    )
    counts = {
        str(key): int(value)
        for key, value in _mapping(result.get("provider_counts")).items()
        if isinstance(value, int | float)
    }
    counts[provider] = counts.get(provider, 0) + 1
    result["provider_counts"] = counts
    result["provider"] = max(counts, key=counts.get)
    return result


def _finalize_bar(current: Mapping[str, object]) -> dict[str, object]:
    result = dict(current)
    sample_count = int(result.get("sample_count") or 0)
    max_gap = _number(result.get("max_sample_gap_seconds"))
    start = _parse_at(result.get("bar_start"))
    end = _parse_at(result.get("bar_end"))
    first = _parse_at(result.get("first_source_at"))
    last = _parse_at(result.get("last_source_at"))
    leading_gap = (
        (first - start).total_seconds()
        if first is not None and start is not None
        else None
    )
    trailing_gap = (
        (end - last).total_seconds()
        if end is not None and last is not None
        else None
    )
    result["leading_edge_gap_seconds"] = leading_gap
    result["trailing_edge_gap_seconds"] = trailing_gap
    result["quality"] = (
        "ok"
        if sample_count >= MIN_OK_SAMPLES
        and max_gap is not None
        and max_gap <= MAX_OK_GAP_SECONDS
        and leading_gap is not None
        and 0 <= leading_gap <= MAX_EDGE_GAP_SECONDS
        and trailing_gap is not None
        and 0 <= trailing_gap <= MAX_EDGE_GAP_SECONDS
        else "partial"
    )
    return result


def _upsert_closed(
    rows: list[dict[str, object]],
    bar: dict[str, object],
) -> list[dict[str, object]]:
    start = str(bar.get("bar_start") or "")
    result = [row for row in rows if str(row.get("bar_start") or "") != start]
    result.append(bar)
    return sorted(result, key=lambda row: str(row.get("bar_start") or ""))


def _bar_rows(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rows = [
        dict(row)
        for row in value
        if isinstance(row, Mapping)
        and _parse_at(row.get("bar_start")) is not None
        and int(row.get("interval_seconds") or 0) == INTERVAL_SECONDS
        and all(_number(row.get(key)) is not None for key in ("open", "high", "low", "close"))
    ]
    return sorted(rows, key=lambda row: str(row.get("bar_start") or ""))


def _bucket_start(at: datetime) -> datetime:
    stamp = int(as_utc(at).timestamp())
    return datetime.fromtimestamp(
        stamp - stamp % INTERVAL_SECONDS,
        tz=timezone.utc,
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if number == number and abs(number) != float("inf") else None


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


__all__ = [
    "INTERVAL_SECONDS",
    "SCHEMA_VERSION",
    "advance_es_bar_state",
    "completed_es_bars",
]
