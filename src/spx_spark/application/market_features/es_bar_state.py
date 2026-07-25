"""Provider-neutral ES five-minute OHLC sampling state.

The hot market-feature worker observes the freshest live ES quote every five
seconds.  This module turns those observations into non-overlapping 5-minute
bars without filling gaps or replaying duplicate provider timestamps.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.application.market_features.market import session_segment
from spx_spark.config import NY_TZ
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.settings.market_features import MarketFeatureSettings


SCHEMA_VERSION = "es_5m_bar_state.v1"
INTERVAL_SECONDS = 300
MAX_CLOSED_BARS = 432
MAX_RTH_MA_BARS = 320
MIN_RTH_MA_SEED_BARS = 206
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
    contract_identity = _contract_identity(quote)
    rejection = (
        "es_contract_identity_provider_conflict"
        if _provider_contract_identity_conflict(sample)
        else _rejection_reason(
            source_at=source_at,
            price=price,
            now=at,
            last_source_at=_parse_at(state.get("last_source_at")),
        )
    )
    if rejection is not None:
        return _with_diagnostic(state, at=at, rejection=rejection)

    assert source_at is not None
    assert price is not None
    previous_contract_identity = str(state.get("contract_identity") or "")
    if (
        contract_identity is not None
        and previous_contract_identity
        and contract_identity != previous_contract_identity
    ):
        state = _valid_state(None)
        diagnostics = dict(_mapping(state.get("diagnostics")))
        diagnostics.update(
            {
                "contract_reset_at": at.isoformat(),
                "contract_reset_from": previous_contract_identity,
                "contract_reset_to": contract_identity,
            }
        )
        state["diagnostics"] = diagnostics
    bar_start = _bucket_start(source_at)
    current = _mapping(state.get("current_bar"))
    closed = _bar_rows(state.get("closed_bars"))
    rth_ma_history = _merge_bars(
        _rth_ma_rows(state.get("rth_ma_history")),
        (_compact_rth_bar(row) for row in closed if row.get("segment") == "rth"),
    )[-MAX_RTH_MA_BARS:]
    segment = session_segment(source_at, policy=policy)

    if current and _parse_at(current.get("bar_start")) == bar_start:
        current = _add_observation(
            current,
            source_at=source_at,
            price=price,
            provider=provider,
            contract_identity=contract_identity,
        )
    else:
        gap_before = False
        if current:
            finalized = _finalize_bar(current)
            closed = _upsert_closed(closed, finalized)
            if finalized.get("segment") == "rth":
                rth_ma_history = _merge_bars(
                    rth_ma_history,
                    [_compact_rth_bar(finalized)],
                )[-MAX_RTH_MA_BARS:]
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
            contract_identity=contract_identity,
            contract_identity_ambiguous=bool(
                previous_contract_identity and contract_identity is None
            ),
        )

    diagnostics = {
        **_mapping(state.get("diagnostics")),
        "last_rejection": None,
        "closed_bar_count": len(closed[-MAX_CLOSED_BARS:]),
        "rth_ma_bar_count": len(rth_ma_history),
        "sampling": "fresh_live_es_source_timestamps_no_gap_fill",
        "min_ok_samples": MIN_OK_SAMPLES,
        "max_ok_gap_seconds": MAX_OK_GAP_SECONDS,
        "max_edge_gap_seconds": MAX_EDGE_GAP_SECONDS,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "interval_seconds": INTERVAL_SECONDS,
        "updated_at": at.isoformat(),
        "last_source_at": source_at.isoformat(),
        "last_provider": provider,
        "contract_identity": contract_identity or previous_contract_identity or None,
        "current_bar": current,
        "closed_bars": closed[-MAX_CLOSED_BARS:],
        "rth_ma_history": rth_ma_history,
        "diagnostics": diagnostics,
    }


def completed_es_bars(
    state: Mapping[str, object] | None,
) -> list[dict[str, object]]:
    """Return validated completed bars in chronological order."""

    valid = _valid_state(state)
    return _merge_bars(
        _rth_ma_rows(valid.get("rth_ma_history")),
        _bar_rows(valid.get("closed_bars")),
    )


def seed_rth_ma_history(
    previous: Mapping[str, object] | None,
    bars: Iterable[Mapping[str, object]],
    *,
    contract_identity: str,
    now: datetime,
    source: str = "ibkr_historical_exact_contract",
    promote_contract_identity: bool = False,
) -> dict[str, object]:
    """Warm the bounded MA history from one explicitly qualified ES contract.

    This does not synthesize bars or alter the active live bucket. Every seed
    row must be a closed, continuous, exact-contract RTH observation. Existing
    live full bars remain the preferred representation only when their quality
    and contract identity are equally verified.
    """

    identity = contract_identity.strip()
    if not identity:
        raise ValueError("rth_ma_seed_contract_identity_missing")
    at = as_utc(now)
    if (
        not isinstance(previous, Mapping)
        or previous.get("schema_version") != SCHEMA_VERSION
        or previous.get("interval_seconds") != INTERVAL_SECONDS
    ):
        raise ValueError("rth_ma_seed_state_schema_invalid")
    state = dict(previous)
    active_identity = state.get("contract_identity")
    if (
        isinstance(active_identity, str)
        and active_identity
        and active_identity != identity
    ):
        raise ValueError("rth_ma_seed_contract_identity_mismatch")

    raw_rows = list(bars)
    if not raw_rows:
        raise ValueError("rth_ma_seed_empty")
    validated: list[dict[str, object]] = []
    starts: set[str] = set()
    for raw in raw_rows:
        row = _validated_seed_bar(
            raw,
            contract_identity=identity,
            now=at,
        )
        row["ma_history_source"] = source
        start = str(row["bar_start"])
        if start in starts:
            raise ValueError("rth_ma_seed_duplicate_bar")
        starts.add(start)
        validated.append(row)
    selected = sorted(validated, key=lambda row: str(row["bar_start"]))[
        -MAX_RTH_MA_BARS:
    ]
    if len(selected) < MIN_RTH_MA_SEED_BARS:
        raise ValueError("rth_ma_seed_insufficient_bars")
    _validate_seed_continuity(selected)

    existing: list[dict[str, object]] = []
    for value in _rth_ma_rows(state.get("rth_ma_history")):
        if value.get("contract_identity") != identity:
            continue
        end = _parse_at(value.get("bar_end"))
        if end is not None and end > at:
            raise ValueError("rth_ma_existing_history_future")
        if (
            value.get("quality") != "ok"
            or value.get("contract_identity_ambiguous") is True
        ):
            continue
        existing.append(
            _validated_seed_bar(
                value,
                contract_identity=identity,
                now=at,
            )
        )
    rth_ma_history = _merge_bars(selected, existing)[-MAX_RTH_MA_BARS:]
    if len(rth_ma_history) < MIN_RTH_MA_SEED_BARS:
        raise ValueError("rth_ma_seed_insufficient_merged_bars")
    _validate_seed_continuity(rth_ma_history)

    diagnostics = dict(_mapping(state.get("diagnostics")))
    diagnostics.update(
        {
            "rth_ma_bar_count": len(rth_ma_history),
            "rth_ma_seeded_at": at.isoformat(),
            "rth_ma_seed_source": source,
            "rth_ma_seed_contract_identity": identity,
            "rth_ma_seed_first_bar_start": rth_ma_history[0]["bar_start"],
            "rth_ma_seed_last_bar_end": rth_ma_history[-1]["bar_end"],
        }
    )
    return {
        **state,
        "contract_identity": (
            identity if promote_contract_identity else state.get("contract_identity")
        ),
        "rth_ma_history": rth_ma_history,
        "diagnostics": diagnostics,
    }


def _valid_state(previous: Mapping[str, object] | None) -> dict[str, object]:
    if not isinstance(previous, Mapping) or previous.get("schema_version") != SCHEMA_VERSION:
        return {
            "schema_version": SCHEMA_VERSION,
            "interval_seconds": INTERVAL_SECONDS,
            "updated_at": None,
            "last_source_at": None,
            "last_provider": None,
            "contract_identity": None,
            "current_bar": {},
            "closed_bars": [],
            "rth_ma_history": [],
            "diagnostics": {
                "last_rejection": None,
                "closed_bar_count": 0,
                "rth_ma_bar_count": 0,
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


def _contract_identity(
    selected: Mapping[str, object],
) -> str | None:
    direct = selected.get("contract_identity")
    return direct if isinstance(direct, str) and direct else None


def _provider_contract_identity_conflict(sample: Mapping[str, object]) -> bool:
    providers = sample.get("es_by_provider")
    if not isinstance(providers, Mapping):
        return False
    identities = {
        identity
        for value in providers.values()
        if isinstance(value, Mapping)
        and isinstance((identity := value.get("contract_identity")), str)
        and identity
    }
    return len(identities) > 1


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
    diagnostics["rth_ma_bar_count"] = len(_rth_ma_rows(result.get("rth_ma_history")))
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
    contract_identity: str | None,
    contract_identity_ambiguous: bool,
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
        "contract_identity": contract_identity,
        "contract_identity_ambiguous": contract_identity_ambiguous,
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
    contract_identity: str | None,
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
    current_identity = result.get("contract_identity")
    current_known = isinstance(current_identity, str) and bool(current_identity)
    incoming_known = isinstance(contract_identity, str) and bool(contract_identity)
    if (
        result.get("contract_identity_ambiguous") is True
        or (current_known != incoming_known)
        or (current_known and contract_identity != current_identity)
    ):
        result["contract_identity"] = None
        result["contract_identity_ambiguous"] = True
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
        (first - start).total_seconds() if first is not None and start is not None else None
    )
    trailing_gap = (end - last).total_seconds() if end is not None and last is not None else None
    result["leading_edge_gap_seconds"] = leading_gap
    result["trailing_edge_gap_seconds"] = trailing_gap
    result["quality"] = (
        "ok"
        if sample_count >= MIN_OK_SAMPLES
        and max_gap is not None
        and max_gap <= MAX_OK_GAP_SECONDS
        and result.get("contract_identity_ambiguous") is not True
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


def _merge_bars(
    older: Iterable[Mapping[str, object]],
    newer: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Merge chronological bars by start time, preferring the newer row."""

    by_start: dict[str, dict[str, object]] = {}
    for row in (*older, *newer):
        start = str(row.get("bar_start") or "")
        if start:
            by_start[start] = dict(row)
    return [by_start[start] for start in sorted(by_start)]


def _compact_rth_bar(row: Mapping[str, object]) -> dict[str, object]:
    """Keep only fields required by RTH MA/ATR calculations."""

    fields = (
        "bar_start",
        "bar_end",
        "interval_seconds",
        "open",
        "high",
        "low",
        "close",
        "quality",
        "gap_before",
        "segment",
        "trading_date_et",
        "contract_identity",
        "contract_identity_ambiguous",
        "ma_history_source",
    )
    return {field: row.get(field) for field in fields}


def _validated_seed_bar(
    value: Mapping[str, object],
    *,
    contract_identity: str,
    now: datetime,
) -> dict[str, object]:
    row = _compact_rth_bar(value)
    start = _parse_at(row.get("bar_start"))
    end = _parse_at(row.get("bar_end"))
    if (
        start is None
        or end is None
        or end != start + timedelta(seconds=INTERVAL_SECONDS)
        or end > now
    ):
        raise ValueError("rth_ma_seed_timestamp_invalid")
    if row.get("segment") != "rth" or row.get("quality") != "ok":
        raise ValueError("rth_ma_seed_not_closed_ok_rth")
    if (
        row.get("contract_identity") != contract_identity
        or row.get("contract_identity_ambiguous") is True
    ):
        raise ValueError("rth_ma_seed_bar_contract_identity_mismatch")
    local = start.astimezone(NY_TZ)
    session = DEFAULT_MARKET_CALENDAR.session(local.date())
    if session is None or not session.open_at <= local < session.close_at:
        raise ValueError("rth_ma_seed_outside_rth")
    if row.get("trading_date_et") != local.date().isoformat():
        raise ValueError("rth_ma_seed_trading_date_mismatch")
    prices = {
        key: _number(row.get(key))
        for key in ("open", "high", "low", "close")
    }
    if any(value is None for value in prices.values()):
        raise ValueError("rth_ma_seed_price_invalid")
    open_px = float(prices["open"])
    high = float(prices["high"])
    low = float(prices["low"])
    close = float(prices["close"])
    if low <= 0 or high < max(open_px, close) or low > min(open_px, close):
        raise ValueError("rth_ma_seed_ohlc_invalid")
    return row


def _validate_seed_continuity(rows: list[dict[str, object]]) -> None:
    for previous, current in zip(rows, rows[1:], strict=False):
        previous_start = _parse_at(previous.get("bar_start"))
        current_start = _parse_at(current.get("bar_start"))
        if previous_start is None or current_start is None:
            raise ValueError("rth_ma_seed_timestamp_invalid")
        previous_day = str(previous.get("trading_date_et") or "")
        current_day = str(current.get("trading_date_et") or "")
        if previous_day == current_day:
            if (
                current_start != previous_start + timedelta(minutes=5)
                or current.get("gap_before") is True
            ):
                raise ValueError("rth_ma_seed_intraday_gap")
            continue
        try:
            prior_date = datetime.fromisoformat(previous_day).date()
            next_date = datetime.fromisoformat(current_day).date()
        except ValueError as exc:
            raise ValueError("rth_ma_seed_trading_date_invalid") from exc
        prior_session = DEFAULT_MARKET_CALENDAR.session(prior_date)
        next_session = DEFAULT_MARKET_CALENDAR.session(next_date)
        if (
            prior_session is None
            or next_session is None
            or previous_start.astimezone(NY_TZ)
            != prior_session.close_at - timedelta(minutes=5)
            or current_start.astimezone(NY_TZ) != next_session.open_at
            or DEFAULT_MARKET_CALENDAR.next_trading_day(prior_date) != next_date
        ):
            raise ValueError("rth_ma_seed_session_boundary_gap")


def _rth_ma_rows(value: object) -> list[dict[str, object]]:
    return [
        row
        for row in _bar_rows(value)
        if row.get("segment") == "rth"
    ]


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
    "MAX_CLOSED_BARS",
    "MAX_RTH_MA_BARS",
    "MIN_RTH_MA_SEED_BARS",
    "SCHEMA_VERSION",
    "advance_es_bar_state",
    "completed_es_bars",
    "seed_rth_ma_history",
]
