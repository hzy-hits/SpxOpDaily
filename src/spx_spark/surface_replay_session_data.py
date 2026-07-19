"""Causal source parsing and fixed-grid calculations for session-surface replay."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

import duckdb

from spx_spark.features.exposure_surface import (
    SurfaceContract,
    SurfaceGridConfig,
    build_exposure_surface,
)
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import (
    REPLAY_POLICY_VERSION,
    ReplaySourceError,
    replay_id,
)
from spx_spark.surface_replay_session_models import (
    MAX_SINGLE_BUILD_EVALUATIONS,
    SESSION_SURFACE_POLICY_VERSION,
    SESSION_SURFACE_PRICE_EXTENT_POINTS,
    FrameLoader,
    SessionSurfaceBuildCache,
    _clock,
    _finite,
    _FrameState,
    _iso,
    _KernelColumn,
    _list,
    _mapping,
    _METRIC_TO_OUTPUT,
    _nonnegative,
    _SHA256_RE,
    _SPXObservation,
)
from spx_spark.surface_replay_trend import TrendContext


def _load_spx_session(context: TrendContext) -> tuple[_SPXObservation, ...]:
    query = """
        WITH source AS MATERIALIZED (
            SELECT
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
              AND instrument_id = 'index:SPX'
              AND source_at >= ?::TIMESTAMPTZ
              AND source_at < ?::TIMESTAMPTZ
              AND received_at IS NOT NULL
              AND mark IS NOT NULL
              AND isfinite(mark)
              AND mark > 0
              AND quality = 'live'
              AND error IS NULL
        ),
        eligible AS (
            SELECT *
            FROM source
            WHERE known_at <= ?::TIMESTAMPTZ
        )
        SELECT source_at, known_at, received_at, mark, source_file, source_row
        FROM eligible
        ORDER BY source_at, known_at, source_file, source_row
    """
    connection = duckdb.connect()
    try:
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET threads=1")
        rows = connection.execute(
            query,
            [
                [str(path) for path in context.source_paths],
                as_utc(context.open_at),
                as_utc(context.close_at),
                as_utc(context.close_at),
            ],
        ).fetchall()
    finally:
        connection.close()
    observations: list[_SPXObservation] = []
    previous: datetime | None = None
    for (
        raw_source,
        raw_known,
        raw_received,
        raw_price,
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
        price = _finite(raw_price)
        if (
            price is None
            or price <= 0
            or known_at < source_at
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
            )
        )
        previous = source_at
    if not observations:
        raise ReplaySourceError("session_surface_spx_unavailable")
    return tuple(observations)


def _causal_spx(
    context: TrendContext,
    *,
    as_of: datetime,
    build_cache: SessionSurfaceBuildCache,
) -> tuple[tuple[_SPXObservation, ...], _SPXObservation]:
    key = (context.source_fingerprint, context.session_date.isoformat())
    observations = build_cache.get_spx(key)
    if observations is None:
        observations = _load_spx_session(context)
        build_cache.put_spx(key, observations)
    cutoff = as_utc(as_of)
    eligible = tuple(
        row
        for row in observations
        if row.source_at <= cutoff and row.known_at <= cutoff
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
    return selected, anchor


def _candles(
    observations: tuple[_SPXObservation, ...],
    buckets: tuple[tuple[datetime, datetime], ...],
    *,
    as_of: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    cutoff = as_utc(as_of)
    rows: list[dict[str, object]] = []
    missing: list[dict[str, object]] = []
    observation_index = 0
    for start, end in buckets:
        if start > cutoff:
            break
        values: list[_SPXObservation] = []
        while observation_index < len(observations):
            observation = observations[observation_index]
            if observation.source_at < start:
                observation_index += 1
                continue
            if observation.source_at >= end or observation.source_at > cutoff:
                break
            values.append(observation)
            observation_index += 1
        if not values:
            missing.append(
                {
                    "start_at": _iso(start),
                    "end_at": _iso(min(end, cutoff) if cutoff < end else end),
                    "reason": "spx_event_samples_unavailable",
                    "component": "candles",
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
                "quality": "event_sampled",
            }
        )
    return rows, [row for row in missing if row["start_at"] != row["end_at"]]


def _contracts_and_strikes(
    surface: Mapping[str, Any],
    *,
    expiry: str,
) -> tuple[tuple[SurfaceContract, ...], tuple[Mapping[str, Any], ...]]:
    raw_rows = _list(
        surface.get("strike_ladder"),
        code="session_surface_strike_ladder_invalid",
    )
    contracts: list[SurfaceContract] = []
    strike_rows: list[Mapping[str, Any]] = []
    previous_strike: float | None = None
    for raw_row in raw_rows:
        row = _mapping(raw_row, code="session_surface_strike_row_invalid")
        strike = _finite(row.get("strike"))
        if (
            strike is None
            or strike <= 0
            or (previous_strike is not None and strike <= previous_strike)
        ):
            raise ReplaySourceError("session_surface_strike_order_invalid")
        for side, right in (("call", "C"), ("put", "P")):
            raw_leg = row.get(side)
            if raw_leg is None:
                continue
            leg = _mapping(raw_leg, code="session_surface_strike_leg_invalid")
            contracts.append(
                SurfaceContract(
                    expiry=expiry,
                    strike=strike,
                    right=right,
                    iv=_finite(leg.get("iv")),
                    open_interest=_nonnegative(leg.get("open_interest")),
                    volume=_nonnegative(leg.get("volume")),
                )
            )
        strike_rows.append(dict(row))
        previous_strike = strike
    if not contracts or not strike_rows:
        raise ReplaySourceError("session_surface_contracts_unavailable")
    return tuple(contracts), tuple(strike_rows)


def _parse_role_frame(
    context: TrendContext,
    *,
    requested: datetime,
    frame: Mapping[str, Any],
    role: str,
) -> _FrameState:
    requested = as_utc(requested)
    expected = {
        "kind": "spxw_surface_dashboard_replay",
        "mode": "replay",
        "policy_version": REPLAY_POLICY_VERSION,
        "session_date": context.session_date.isoformat(),
        "requested_as_of": requested.isoformat(),
        "projection_policy_sha256": context.projection_policy_sha256,
        "frozen": True,
        "automatic_ordering": False,
    }
    if any(frame.get(key) != value for key, value in expected.items()):
        raise ReplaySourceError("session_surface_frame_contract_invalid")
    artifact_hash = frame.get("artifact_sha256")
    if not isinstance(artifact_hash, str) or not _SHA256_RE.fullmatch(artifact_hash):
        raise ReplaySourceError("session_surface_frame_hash_invalid")
    source = _mapping(frame.get("source"), code="session_surface_frame_source_invalid")
    if (
        source.get("lookahead_rows_selected") != 0
        or source.get("availability_clock_available") is not False
        or source.get("point_in_time_confidence") != "bounded_not_proven"
    ):
        raise ReplaySourceError("session_surface_frame_pit_contract_invalid")
    expiries = _list(frame.get("expiries"), code="session_surface_expiries_invalid")
    selected = [
        value
        for value in expiries
        if isinstance(value, Mapping) and value.get("role") == role
    ]
    if len(selected) != 1:
        raise ReplaySourceError("session_surface_frame_role_invalid")
    expiry_row = selected[0]
    expiry = expiry_row.get("expiry")
    if not isinstance(expiry, str) or not re.fullmatch(r"\d{8}", expiry):
        raise ReplaySourceError("session_surface_expiry_invalid")
    expiry_close = _clock(
        expiry_row.get("expiry_close"),
        code="session_surface_expiry_close_invalid",
    )
    surface = _mapping(
        expiry_row.get("surface"),
        code="session_surface_frame_surface_invalid",
    )
    surface_as_of = _clock(
        surface.get("as_of"),
        code="session_surface_frame_surface_clock_invalid",
    )
    reference_spot = _finite(surface.get("reference_spot"))
    if surface_as_of != requested or reference_spot is None or reference_spot <= 0:
        raise ReplaySourceError("session_surface_frame_surface_contract_invalid")
    contracts, strike_rows = _contracts_and_strikes(surface, expiry=expiry)
    raw_warnings = expiry_row.get("warnings")
    warnings = tuple(str(value) for value in raw_warnings) if isinstance(raw_warnings, list) else ()
    quality = str(expiry_row.get("quality") or "unavailable")
    return _FrameState(
        at=requested,
        valid_until=requested,
        artifact_sha256=artifact_hash,
        expiry=expiry,
        expiry_close=expiry_close,
        reference_spot=reference_spot,
        contracts=contracts,
        strike_rows=strike_rows,
        quality=quality,
        warnings=warnings,
    )


def _causal_frames(
    context: TrendContext,
    *,
    as_of: datetime,
    role: str,
    frame_loader: FrameLoader,
    build_cache: SessionSurfaceBuildCache,
) -> tuple[_FrameState, ...]:
    cutoff = as_utc(as_of)
    requested_frames = tuple(
        as_utc(value) for value in context.frames if as_utc(value) <= cutoff
    )
    parsed: list[_FrameState] = []
    for requested in requested_frames:
        cache_key = (context.source_fingerprint, role, replay_id(requested))
        row = build_cache.get_frame(cache_key)
        if row is None:
            row = _parse_role_frame(
                context,
                requested=requested,
                frame=frame_loader(requested),
                role=role,
            )
            build_cache.put_frame(cache_key, row)
        parsed.append(row)
    if any(right.at <= left.at for left, right in zip(parsed, parsed[1:])):
        raise ReplaySourceError("session_surface_timeline_invalid")
    expiry_values = {row.expiry for row in parsed}
    if len(expiry_values) > 1:
        raise ReplaySourceError("session_surface_expiry_changed")
    resolved: list[_FrameState] = []
    for index, row in enumerate(parsed):
        next_at = parsed[index + 1].at if index + 1 < len(parsed) else None
        valid_until = min(
            row.at + timedelta(minutes=context.frame_minutes),
            row.expiry_close,
            as_utc(context.close_at),
            next_at if next_at is not None else as_utc(context.close_at),
        )
        if row.quality not in {"ready", "degraded", "ok"}:
            valid_until = row.at
        resolved.append(
            _FrameState(
                at=row.at,
                valid_until=valid_until,
                artifact_sha256=row.artifact_sha256,
                expiry=row.expiry,
                expiry_close=row.expiry_close,
                reference_spot=row.reference_spot,
                contracts=row.contracts,
                strike_rows=row.strike_rows,
                quality=row.quality,
                warnings=row.warnings,
            )
        )
    return tuple(resolved)


def _fixed_price_grid(spot: float, *, step: float) -> tuple[float, ...]:
    anchor = math.floor((spot / step) + 0.5) * step
    steps_each_side = int(round(SESSION_SURFACE_PRICE_EXTENT_POINTS / step))
    values = tuple(
        anchor + offset * step
        for offset in range(
            -steps_each_side,
            steps_each_side + 1,
        )
        if anchor + offset * step > 0
    )
    if len(values) < 2:
        raise ReplaySourceError("session_surface_price_grid_invalid")
    return values


def _kernel_columns_uncached(
    frame: _FrameState,
    *,
    price_grid: tuple[float, ...],
    offsets: tuple[float, ...],
    weighting: str,
) -> tuple[_KernelColumn, ...]:
    if not offsets:
        return ()
    evaluations = len(frame.contracts) * len(price_grid) * len(offsets) * 3
    max_time_points = max(24, len(offsets))
    max_cells = max(1_944, len(price_grid) * len(offsets))
    if evaluations <= MAX_SINGLE_BUILD_EVALUATIONS:
        chunks = (offsets,)
    else:
        per_offset = max(len(frame.contracts) * len(price_grid) * 3, 1)
        chunk_size = max(1, min(24, MAX_SINGLE_BUILD_EVALUATIONS // per_offset))
        chunks = tuple(
            offsets[index : index + chunk_size]
            for index in range(0, len(offsets), chunk_size)
        )
        max_time_points = max(24, chunk_size)
        max_cells = max(1_944, len(price_grid) * chunk_size)
    columns: list[_KernelColumn] = []
    for chunk in chunks:
        price_step = price_grid[1] - price_grid[0]
        config = SurfaceGridConfig(
            spot_step_points=price_step,
            spot_steps_each_side=(len(price_grid) - 1) // 2,
            max_time_points=max_time_points,
            max_cells=max_cells,
            max_contract_cell_evaluations=MAX_SINGLE_BUILD_EVALUATIONS,
        )
        surface = build_exposure_surface(
            frame.contracts,
            spot=frame.reference_spot,
            as_of=frame.at,
            expiry_close=frame.expiry_close,
            spot_points=price_grid,
            time_offsets_minutes=chunk,
            config=config,
        )
        for time_slice in surface.time_slices:
            weighting_slice = time_slice.weightings[weighting]
            columns.append(
                _KernelColumn(
                    metrics={
                        key: tuple(value)
                        for key, value in weighting_slice.metrics.to_dict().items()
                    },
                    zero_ridge=weighting_slice.zero_ridge_spot,
                    quality=weighting_slice.quality,
                    warnings=tuple(weighting_slice.warnings),
                )
            )
    if len(columns) != len(offsets):
        raise ReplaySourceError("session_surface_kernel_shape_invalid")
    return tuple(columns)


def _kernel_columns(
    frame: _FrameState,
    *,
    price_grid: tuple[float, ...],
    offsets: tuple[float, ...],
    weighting: str,
    build_cache: SessionSurfaceBuildCache,
) -> tuple[_KernelColumn, ...]:
    key: tuple[object, ...] = (
        frame.artifact_sha256,
        weighting,
        price_grid,
        offsets,
        SESSION_SURFACE_POLICY_VERSION,
    )
    cached = build_cache.get_kernel(key)
    if cached is not None:
        return cached
    built = _kernel_columns_uncached(
        frame,
        price_grid=price_grid,
        offsets=offsets,
        weighting=weighting,
    )
    build_cache.put_kernel(key, built)
    return built


def _frame_for_historical_end(
    frames: tuple[_FrameState, ...],
    end: datetime,
) -> _FrameState | None:
    candidates = [
        row
        for row in frames
        if (row.known_at or row.at) <= end < row.valid_until
    ]
    return candidates[-1] if candidates else None


def _current_frame(
    frames: tuple[_FrameState, ...],
    as_of: datetime,
) -> _FrameState | None:
    cutoff = as_utc(as_of)
    candidates = [
        row
        for row in frames
        if (row.known_at or row.at) <= cutoff < row.valid_until
    ]
    return candidates[-1] if candidates else None


def _missing_column(
    *,
    reason: str,
    price_points: int,
) -> tuple[dict[str, object], dict[str, list[float | None]], None]:
    return (
        {
            "kind": "missing",
            "quality": "unavailable",
            "source_at": None,
            "valid_until": None,
            "reason": reason,
        },
        {metric: [None] * price_points for metric in _METRIC_TO_OUTPUT},
        None,
    )


def _surface_payload(
    frames: tuple[_FrameState, ...],
    buckets: tuple[tuple[datetime, datetime], ...],
    *,
    as_of: datetime,
    price_grid: tuple[float, ...],
    weighting: str,
    build_cache: SessionSurfaceBuildCache,
) -> tuple[
    list[dict[str, object]],
    dict[str, list[list[float | None]]],
    list[float | None],
    list[dict[str, float] | None],
    list[dict[str, float] | None],
]:
    cutoff = as_utc(as_of)
    current = _current_frame(frames, cutoff)
    historical_frames = {
        row.artifact_sha256: row
        for _start, end in buckets
        if end <= cutoff
        for row in [_frame_for_historical_end(frames, end)]
        if row is not None
    }
    historical_values = {
        key: _kernel_columns(
            frame,
            price_grid=price_grid,
            offsets=(0.0,),
            weighting=weighting,
            build_cache=build_cache,
        )[0]
        for key, frame in historical_frames.items()
    }
    projection_by_end: dict[datetime, _KernelColumn] = {}
    if current is not None:
        future_ends = tuple(end for _start, end in buckets if end > current.at)
        offsets = tuple((end - current.at).total_seconds() / 60.0 for end in future_ends)
        projected = _kernel_columns(
            current,
            price_grid=price_grid,
            offsets=offsets,
            weighting=weighting,
            build_cache=build_cache,
        )
        projection_by_end = dict(zip(future_ends, projected, strict=True))

    columns: list[dict[str, object]] = []
    matrices = {metric: [] for metric in _METRIC_TO_OUTPUT}
    zero_ridges: list[float | None] = []
    positive_peaks: list[dict[str, float] | None] = []
    negative_troughs: list[dict[str, float] | None] = []
    for _start, end in buckets:
        if end <= cutoff:
            frame = _frame_for_historical_end(frames, end)
            if frame is None:
                column, values, zero = _missing_column(
                    reason="validated_surface_unavailable_at_bucket_end",
                    price_points=len(price_grid),
                )
            else:
                kernel = historical_values[frame.artifact_sha256]
                if kernel.quality == "unavailable" or not any(
                    value is not None for value in kernel.metrics["signed_gamma"]
                ):
                    column, values, zero = _missing_column(
                        reason=(kernel.warnings[0] if kernel.warnings else "kernel_unavailable"),
                        price_points=len(price_grid),
                    )
                    column["source_at"] = _iso(frame.at)
                    column["valid_until"] = _iso(frame.valid_until)
                else:
                    column = {
                        "kind": "historical",
                        "quality": "ready" if kernel.quality == "ok" else "degraded",
                        "source_at": _iso(frame.at),
                        "valid_until": _iso(frame.valid_until),
                        "reason": None,
                        "source_frame_sha256": frame.artifact_sha256,
                        "minutes_forward": 0.0,
                    }
                    values = {
                        metric: list(kernel.metrics[metric]) for metric in _METRIC_TO_OUTPUT
                    }
                    zero = kernel.zero_ridge
        elif current is None:
            column, values, zero = _missing_column(
                reason="current_validated_frame_unavailable_for_projection",
                price_points=len(price_grid),
            )
        else:
            kernel = projection_by_end.get(end)
            if kernel is None or kernel.quality == "unavailable" or not any(
                value is not None for value in (
                    kernel.metrics["signed_gamma"] if kernel is not None else ()
                )
            ):
                reason = (
                    kernel.warnings[0]
                    if kernel is not None and kernel.warnings
                    else "projection_kernel_unavailable"
                )
                column, values, zero = _missing_column(
                    reason=reason,
                    price_points=len(price_grid),
                )
                column["source_at"] = _iso(current.at)
                column["valid_until"] = _iso(current.valid_until)
            else:
                minutes_forward = (end - current.at).total_seconds() / 60.0
                column = {
                    "kind": "projection",
                    "quality": "ready" if kernel.quality == "ok" else "degraded",
                    "source_at": _iso(current.at),
                    "valid_until": _iso(current.valid_until),
                    "reason": None,
                    "source_frame_sha256": current.artifact_sha256,
                    "scenario_at": _iso(end),
                    "minutes_forward": minutes_forward,
                    "scenario_semantics": "fixed_iv_oi_volume_tau_decay_not_forecast",
                }
                values = {
                    metric: list(kernel.metrics[metric]) for metric in _METRIC_TO_OUTPUT
                }
                zero = kernel.zero_ridge
        columns.append(column)
        for metric in _METRIC_TO_OUTPUT:
            matrices[metric].append(values[metric])
        zero_ridges.append(zero)
        signed_gamma = values["signed_gamma"]
        positive = [
            (price, value)
            for price, value in zip(price_grid, signed_gamma, strict=True)
            if value is not None and value > 0
        ]
        negative = [
            (price, value)
            for price, value in zip(price_grid, signed_gamma, strict=True)
            if value is not None and value < 0
        ]
        if positive:
            peak_price, peak_value = max(positive, key=lambda row: row[1])
            positive_peaks.append({"price": peak_price, "value": peak_value})
        else:
            positive_peaks.append(None)
        if negative:
            trough_price, trough_value = min(negative, key=lambda row: row[1])
            negative_troughs.append(
                {"price": trough_price, "value": trough_value}
            )
        else:
            negative_troughs.append(None)
    return columns, matrices, zero_ridges, positive_peaks, negative_troughs


def _strike_value(
    row: Mapping[str, Any] | None,
    *,
    weighting: str,
) -> tuple[float | None, float | None, str]:
    if row is None:
        return None, None, "unavailable"
    raw_weightings = row.get("weightings")
    selected = (
        raw_weightings.get(weighting)
        if isinstance(raw_weightings, Mapping)
        else None
    )
    metrics = selected.get("metrics") if isinstance(selected, Mapping) else None
    proxy = metrics.get("signed_gamma") if isinstance(metrics, Mapping) else None
    oi_values: list[float] = []
    for side in ("call", "put"):
        leg = row.get(side)
        if isinstance(leg, Mapping):
            value = _nonnegative(leg.get("open_interest"))
            if value is not None:
                oi_values.append(value)
    quality = str(selected.get("quality") or row.get("quality") or "unavailable") if isinstance(selected, Mapping) else str(row.get("quality") or "unavailable")
    return _finite(proxy), math.fsum(oi_values) if oi_values else None, quality


def _strike_profile(
    frames: tuple[_FrameState, ...],
    *,
    as_of: datetime,
    weighting: str,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    baseline = frames[0] if frames else None
    current = _current_frame(frames, as_of)

    def indexed(frame: _FrameState | None) -> dict[float, Mapping[str, Any]]:
        if frame is None:
            return {}
        return {
            float(row["strike"]): row
            for row in frame.strike_rows
            if _finite(row.get("strike")) is not None
        }

    baseline_rows = indexed(baseline)
    current_rows = indexed(current)
    strikes = sorted(set(baseline_rows) | set(current_rows))
    output: list[dict[str, object]] = []
    for strike in strikes:
        current_proxy, current_oi, current_quality = _strike_value(
            current_rows.get(strike),
            weighting=weighting,
        )
        baseline_proxy, baseline_oi, baseline_quality = _strike_value(
            baseline_rows.get(strike),
            weighting=weighting,
        )
        qualities = {current_quality, baseline_quality}
        quality = (
            "ready"
            if qualities <= {"ok", "ready"}
            else "unavailable"
            if qualities == {"unavailable"}
            else "degraded"
        )
        output.append(
            {
                "strike": strike,
                "current_proxy": current_proxy,
                "first_validated_proxy": baseline_proxy,
                "current_open_interest": current_oi,
                "first_validated_open_interest": baseline_oi,
                "quality": quality,
            }
        )
    metadata = {
        "baseline_label": "first_validated",
        "baseline_at": _iso(baseline.at) if baseline is not None else None,
        "current_at": _iso(current.at) if current is not None else None,
        "exact_sod_available": False,
        "missing_join_value": None,
        "proxy_metric": "signed_gamma",
    }
    return output, metadata


def _robust_domain(matrix: list[list[float | None]]) -> dict[str, object]:
    values = sorted(
        abs(value)
        for row in matrix
        for value in row
        if value is not None and math.isfinite(value)
    )
    if values:
        position = (len(values) - 1) * 0.98
        lower = math.floor(position)
        upper = math.ceil(position)
        fraction = position - lower
        robust = values[lower] + (values[upper] - values[lower]) * fraction
        raw_values = [
            value
            for row in matrix
            for value in row
            if value is not None and math.isfinite(value)
        ]
        raw_min = min(raw_values)
        raw_max = max(raw_values)
    else:
        robust = 0.0
        raw_min = None
        raw_max = None
    return {
        "quantile": 0.98,
        "max_abs": robust,
        "domain": [-robust, robust],
        "raw_min": raw_min,
        "raw_max": raw_max,
        "sample_count": len(values),
        "tooltip_values": "raw_unclipped_matrix_values",
    }


def _surface_missing_ranges(
    buckets: tuple[tuple[datetime, datetime], ...],
    columns: list[dict[str, object]],
) -> list[dict[str, object]]:
    ranges: list[dict[str, object]] = []
    active_start: datetime | None = None
    active_end: datetime | None = None
    active_reason: str | None = None
    for (start, end), column in zip(buckets, columns, strict=True):
        reason = str(column.get("reason") or "surface_unavailable")
        if column.get("kind") != "missing":
            if active_start is not None and active_end is not None:
                ranges.append(
                    {
                        "start_at": _iso(active_start),
                        "end_at": _iso(active_end),
                        "reason": active_reason,
                        "component": "surface",
                    }
                )
            active_start = active_end = None
            active_reason = None
            continue
        if active_start is None or active_reason != reason or active_end != start:
            if active_start is not None and active_end is not None:
                ranges.append(
                    {
                        "start_at": _iso(active_start),
                        "end_at": _iso(active_end),
                        "reason": active_reason,
                        "component": "surface",
                    }
                )
            active_start = start
            active_reason = reason
        active_end = end
    if active_start is not None and active_end is not None:
        ranges.append(
            {
                "start_at": _iso(active_start),
                "end_at": _iso(active_end),
                "reason": active_reason,
                "component": "surface",
            }
        )
    return ranges

__all__ = (
    "_candles",
    "_causal_frames",
    "_causal_spx",
    "_fixed_price_grid",
    "_robust_domain",
    "_strike_profile",
    "_surface_missing_ranges",
    "_surface_payload",
)
