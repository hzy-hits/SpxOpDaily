"""Causal Schwab SPX/ES reference loading for replay Session Surface."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any

import duckdb

from spx_spark.ibkr.atm_reference import (
    BasisState,
    EsSpxBasisTracker,
    ReferenceQuote,
)
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import ReplaySourceError
from spx_spark.surface_replay_session_models import (
    SESSION_SURFACE_BASIS_MAX_SKEW_SECONDS,
    SESSION_SURFACE_REFERENCE_MAX_AGE_SECONDS,
    SessionSurfaceBuildCache,
    SessionSurfaceWindow,
    _finite,
    _iso,
    _SPXObservation,
    session_surface_window,
)
from spx_spark.surface_replay_trend import TrendContext


BasisPayloadBuilder = Callable[..., dict[str, object]]
BasisLoader = Callable[[TrendContext], dict[str, object] | None]
SPXLoader = Callable[[TrendContext], tuple[_SPXObservation, ...]]
ConnectFactory = Callable[[], Any]


def basis_payload(
    state: BasisState,
    *,
    known_at: datetime,
    frozen_at: datetime,
) -> dict[str, object]:
    return {
        "value": state.median,
        "method": "frozen_previous_rth_median",
        "provider": "schwab",
        "es_contract": state.es_contract,
        "contract_expiry": None,
        "sample_count": state.sample_count,
        "window_start_at": (
            _iso(state.sample_window_start) if state.sample_window_start is not None else None
        ),
        "window_end_at": _iso(state.observed_at) if state.observed_at else None,
        "known_at": _iso(known_at),
        "frozen_at": _iso(frozen_at),
    }


def load_previous_rth_basis(
    context: TrendContext,
    *,
    payload_builder: BasisPayloadBuilder = basis_payload,
    connect_factory: ConnectFactory = duckdb.connect,
) -> dict[str, object] | None:
    """Rebuild a frozen ES-SPX basis only from the preceding RTH lake rows."""

    window = session_surface_window(context.session_date)
    query = """
        WITH source AS MATERIALIZED (
            SELECT
                instrument_id,
                provider_symbol,
                source_at,
                received_at,
                GREATEST(
                    received_at,
                    COALESCE(source_at, received_at),
                    COALESCE(quote_time, received_at),
                    COALESCE(trade_time, received_at),
                    COALESCE(last_update_at, received_at)
                ) AS known_at,
                mark,
                filename AS source_file,
                file_row_number AS source_row
            FROM read_parquet(
                ?,
                union_by_name=true,
                filename=true,
                file_row_number=true
            )
            WHERE provider = 'schwab'
              AND instrument_id IN ('future:ES', 'index:SPX')
              AND source_at >= ?::TIMESTAMPTZ
              AND source_at < ?::TIMESTAMPTZ
              AND received_at IS NOT NULL
              AND quality = 'live'
              AND error IS NULL
              AND mark IS NOT NULL
              AND isfinite(mark)
              AND mark > 0
        ),
        eligible AS (
            SELECT *
            FROM source
            WHERE known_at <= ?::TIMESTAMPTZ
        ),
        es AS (
            SELECT * EXCLUDE (rank)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY source_at
                    ORDER BY known_at, received_at, source_file, source_row
                ) AS rank
                FROM eligible
                WHERE instrument_id = 'future:ES'
                  AND provider_symbol IS NOT NULL
                  AND provider_symbol <> ''
            )
            WHERE rank = 1
        ),
        spx AS (
            SELECT * EXCLUDE (rank)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY source_at
                    ORDER BY known_at, received_at, source_file, source_row
                ) AS rank
                FROM eligible
                WHERE instrument_id = 'index:SPX'
            )
            WHERE rank = 1
        )
        SELECT
            es.mark,
            es.source_at,
            es.known_at,
            es.provider_symbol,
            spx.mark,
            spx.source_at,
            spx.known_at
        FROM es ASOF JOIN spx
          ON es.source_at >= spx.source_at
        WHERE es.source_at - spx.source_at <= (
            ?::DOUBLE * INTERVAL '1 second'
        )
        ORDER BY es.source_at, es.known_at
    """
    connection = connect_factory()
    try:
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET threads=1")
        rows = connection.execute(
            query,
            [
                [str(path) for path in context.source_paths],
                as_utc(window.previous_rth_open),
                as_utc(window.previous_rth_end),
                as_utc(window.previous_rth_end),
                SESSION_SURFACE_BASIS_MAX_SKEW_SECONDS,
            ],
        ).fetchall()
    finally:
        connection.close()
    tracker = EsSpxBasisTracker()
    latest_known: datetime | None = None
    for (
        raw_es,
        raw_es_at,
        raw_es_known,
        raw_contract,
        raw_spx,
        raw_spx_at,
        raw_spx_known,
    ) in rows:
        es = _finite(raw_es)
        spx = _finite(raw_spx)
        if (
            es is None
            or spx is None
            or not isinstance(raw_es_at, datetime)
            or not isinstance(raw_spx_at, datetime)
            or not isinstance(raw_es_known, datetime)
            or not isinstance(raw_spx_known, datetime)
            or not isinstance(raw_contract, str)
            or not raw_contract
        ):
            continue
        es_at = as_utc(raw_es_at)
        spx_at = as_utc(raw_spx_at)
        pair_known = max(as_utc(raw_es_known), as_utc(raw_spx_known))
        latest_known = pair_known if latest_known is None else max(latest_known, pair_known)
        tracker.observe(
            spx=ReferenceQuote(spx, spx_at, "fresh"),
            es=ReferenceQuote(es, es_at, "fresh", contract=raw_contract),
            is_rth=True,
            trading_date=window.previous_rth_open.date(),
        )
    state = tracker.state
    if (
        state is None
        or not state.is_qualified
        or state.trading_date != window.previous_rth_open.date()
        or latest_known is None
        or latest_known > as_utc(window.previous_rth_end)
    ):
        return None
    # This policy clock is not a response-finished/acceptance clock.
    return payload_builder(
        state,
        known_at=latest_known,
        frozen_at=as_utc(window.previous_rth_end),
    )


def load_spx_session(
    context: TrendContext,
    *,
    basis_loader: BasisLoader = load_previous_rth_basis,
    connect_factory: ConnectFactory = duckdb.connect,
) -> tuple[_SPXObservation, ...]:
    window = session_surface_window(context.session_date)
    basis = basis_loader(context)
    query = """
        WITH source AS MATERIALIZED (
            SELECT
                provider,
                instrument_id,
                provider_symbol,
                source_at,
                received_at,
                GREATEST(
                    received_at,
                    COALESCE(source_at, received_at),
                    COALESCE(quote_time, received_at),
                    COALESCE(trade_time, received_at),
                    COALESCE(last_update_at, received_at)
                ) AS known_at,
                mark,
                quality,
                error,
                filename AS source_file,
                file_row_number AS source_row
            FROM read_parquet(
                ?,
                union_by_name=true,
                filename=true,
                file_row_number=true
            )
            WHERE provider = 'schwab'
              AND instrument_id IN ('index:SPX', 'future:ES')
              AND source_at >= ?::TIMESTAMPTZ
              AND source_at < ?::TIMESTAMPTZ
              AND received_at IS NOT NULL
        ),
        eligible AS (
            SELECT *
            FROM source
            WHERE known_at <= ?::TIMESTAMPTZ
        )
        SELECT
            provider,
            instrument_id,
            provider_symbol,
            source_at,
            known_at,
            received_at,
            mark,
            quality,
            error,
            source_file,
            source_row
        FROM eligible
        ORDER BY source_at, known_at, source_file, source_row
    """
    connection = connect_factory()
    try:
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET threads=1")
        rows = connection.execute(
            query,
            [
                [str(path) for path in context.source_paths],
                as_utc(window.session_start),
                as_utc(window.session_end),
                as_utc(window.session_end),
            ],
        ).fetchall()
    finally:
        connection.close()
    observations: list[_SPXObservation] = []
    previous: datetime | None = None
    for (
        raw_provider,
        raw_instrument,
        raw_provider_symbol,
        raw_source,
        raw_known,
        raw_received,
        raw_price,
        raw_quality,
        raw_error,
        raw_source_file,
        raw_source_row,
    ) in rows:
        if (
            not isinstance(raw_source, datetime)
            or not isinstance(raw_known, datetime)
            or not isinstance(raw_received, datetime)
        ):
            raise ReplaySourceError("session_surface_spx_clock_invalid")
        source_at = as_utc(raw_source)
        known_at = as_utc(raw_known)
        received_at = as_utc(raw_received)
        raw_mark = _finite(raw_price)
        kind = window.segment_kind(source_at)
        is_gth = kind == "gth" and raw_instrument == "future:ES"
        is_rth = kind == "rth" and raw_instrument == "index:SPX"
        if not (is_gth or is_rth):
            continue
        if is_gth:
            basis_value = _finite(basis.get("value")) if basis is not None else None
            price = (
                raw_mark - basis_value if raw_mark is not None and basis_value is not None else None
            )
            method = "es_basis_inferred_spx"
            instrument_id = "future:ES"
            observation_basis: Mapping[str, Any] | None = basis
            contract_matches = basis_value is not None and raw_provider_symbol == basis.get(
                "es_contract"
            )
        else:
            price = raw_mark
            method = "direct_index_spx"
            instrument_id = "index:SPX"
            observation_basis = None
            contract_matches = True
        usable = (
            raw_quality == "live"
            and raw_error is None
            and contract_matches
            and price is not None
            and price > 0
        )
        valid_until = source_at + timedelta(seconds=SESSION_SURFACE_REFERENCE_MAX_AGE_SECONDS)
        if (
            known_at < source_at
            or known_at < received_at
            or (previous is not None and source_at < previous)
        ):
            raise ReplaySourceError("session_surface_spx_contract_invalid")
        observations.append(
            _SPXObservation(
                source_at=source_at,
                known_at=known_at,
                received_at=received_at,
                price=price,
                source_file=str(raw_source_file),
                source_row=int(raw_source_row),
                method=method,
                provider=str(raw_provider),
                instrument_id=instrument_id,
                valid_until=valid_until,
                basis=observation_basis,
                usable=usable,
                error=str(raw_error) if raw_error is not None else None,
            )
        )
        previous = source_at
    if not observations:
        raise ReplaySourceError("session_surface_spx_unavailable")
    return tuple(observations)


def causal_spx(
    context: TrendContext,
    *,
    as_of: datetime,
    build_cache: SessionSurfaceBuildCache,
    spx_loader: SPXLoader = load_spx_session,
) -> tuple[
    tuple[_SPXObservation, ...],
    _SPXObservation,
    _SPXObservation | None,
]:
    key = (context.source_fingerprint, context.session_date.isoformat())
    observations = build_cache.get_spx(key)
    if observations is None:
        observations = spx_loader(context)
        build_cache.put_spx(key, observations)
    cutoff = as_utc(as_of)
    eligible_all = tuple(
        row for row in observations if row.source_at <= cutoff and row.known_at <= cutoff
    )
    eligible = tuple(
        row
        for row in eligible_all
        if row.usable and row.price is not None and math.isfinite(row.price)
    )
    if not eligible:
        raise ReplaySourceError("session_surface_causal_spx_unavailable")
    anchor = min(
        eligible,
        key=lambda row: (
            row.known_at,
            row.received_at,
            row.source_at,
            row.source_file,
            row.source_row,
        ),
    )
    by_source: dict[datetime, _SPXObservation] = {}
    for row in eligible:
        previous = by_source.get(row.source_at)
        if previous is None or (
            row.known_at,
            row.received_at,
            row.source_file,
            row.source_row,
        ) < (
            previous.known_at,
            previous.received_at,
            previous.source_file,
            previous.source_row,
        ):
            by_source[row.source_at] = row
    selected = tuple(by_source[key] for key in sorted(by_source))
    latest_state = max(
        eligible_all,
        key=lambda row: (
            row.known_at,
            row.received_at,
            row.source_at,
            row.source_file,
            row.source_row,
        ),
    )
    current = (
        latest_state
        if latest_state.usable
        and latest_state.price is not None
        and latest_state.valid_until is not None
        and cutoff < as_utc(latest_state.valid_until)
        else None
    )
    return selected, anchor, current


def candles(
    observations: tuple[_SPXObservation, ...],
    buckets: tuple[tuple[datetime, datetime], ...],
    *,
    as_of: datetime,
    session_window: SessionSurfaceWindow | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    cutoff = as_utc(as_of)
    rows: list[dict[str, object]] = []
    missing: list[dict[str, object]] = []
    observation_index = 0
    for start, end in buckets:
        if start > cutoff:
            break
        knowledge_cutoff = min(cutoff, end)
        values: list[_SPXObservation] = []
        while observation_index < len(observations):
            observation = observations[observation_index]
            if observation.source_at < start:
                observation_index += 1
                continue
            if observation.source_at >= end or observation.source_at > cutoff:
                break
            # Completed candles are append-only by their bucket-end knowledge
            # clock. A unique event learned after the bucket ended must not
            # backfill or rewrite that historical OHLC bar at a later playhead.
            if observation.known_at > knowledge_cutoff:
                observation_index += 1
                continue
            values.append(observation)
            observation_index += 1
        if not values:
            session_kind = (
                session_window.segment_kind(start) if session_window is not None else None
            )
            missing.append(
                {
                    "start_at": _iso(start),
                    "end_at": _iso(min(end, cutoff) if cutoff < end else end),
                    "reason": (
                        "scheduled_closed_gap"
                        if session_kind == "closed_gap"
                        else "spx_coordinate_reference_unavailable"
                    ),
                    "component": "candles",
                    "session_kind": session_kind,
                }
            )
            continue
        prices = [row.price for row in values]
        last = values[-1]
        rows.append(
            {
                "start_at": _iso(start),
                "end_at": _iso(end),
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "sample_count": len(prices),
                "complete": end <= cutoff,
                "source_at": _iso(last.source_at),
                "known_at": _iso(max(row.known_at for row in values)),
                "accepted_at": None,
                "valid_until": (_iso(last.valid_until) if last.valid_until is not None else None),
                "quality": "event_sampled",
                "session_kind": (
                    session_window.segment_kind(start)
                    if session_window is not None
                    else ("gth" if last.method == "es_basis_inferred_spx" else "rth")
                ),
                "reference_method": last.method,
                "reference_provider": last.provider,
                "reference_instrument_id": last.instrument_id,
                "basis_value": (
                    _finite(last.basis.get("value")) if isinstance(last.basis, Mapping) else None
                ),
                "basis_observed_at": (
                    last.basis.get("window_end_at") if isinstance(last.basis, Mapping) else None
                ),
                "render_style": (
                    "inferred_dashed" if last.method == "es_basis_inferred_spx" else "direct_solid"
                ),
            }
        )
    return rows, [row for row in missing if row["start_at"] != row["end_at"]]


__all__ = (
    "basis_payload",
    "candles",
    "causal_spx",
    "load_previous_rth_basis",
    "load_spx_session",
)
