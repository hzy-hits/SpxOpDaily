"""Compatibility facade and fixed-grid calculations for Session Surface replay."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import duckdb

from spx_spark.features.exposure_surface import (
    SurfaceContract,
    SurfaceGridConfig,
    build_exposure_surface,
)
from spx_spark.ibkr.atm_reference import BasisState
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import ReplaySourceError
from spx_spark.surface_replay_session_frames import (
    causal_frames as _causal_frames_impl,
    contracts_and_strikes as _contracts_and_strikes_impl,
    gth_frame_hash as _gth_frame_hash_impl,
    gth_reference_at as _gth_reference_at_impl,
    load_gth_frames as _load_gth_frames_impl,
    parse_role_frame as _parse_role_frame_impl,
)
from spx_spark.surface_replay_session_models import (
    MAX_SINGLE_BUILD_EVALUATIONS,
    SESSION_SURFACE_POLICY_VERSION,
    SESSION_SURFACE_PRICE_EXTENT_POINTS,
    FrameLoader,
    SessionSurfaceBuildCache,
    SessionSurfaceWindow,
    _finite,
    _FrameState,
    _iso,
    _KernelColumn,
    _METRIC_TO_OUTPUT,
    _nonnegative,
    _SPXObservation,
)
from spx_spark.surface_replay_session_reference import (
    basis_payload as _basis_payload_impl,
    candles as _candles_impl,
    causal_spx as _causal_spx_impl,
    load_previous_rth_basis as _load_previous_rth_basis_impl,
    load_spx_session as _load_spx_session_impl,
)
from spx_spark.surface_replay_trend import TrendContext


# The wrappers intentionally resolve collaborators from this module at call time.
# Existing tests and callers can continue monkeypatching this facade after the
# source/query implementations have moved to focused modules.
def _basis_payload(
    state: BasisState,
    *,
    known_at: datetime,
    frozen_at: datetime,
) -> dict[str, object]:
    return _basis_payload_impl(state, known_at=known_at, frozen_at=frozen_at)


def _load_previous_rth_basis(context: TrendContext) -> dict[str, object] | None:
    return _load_previous_rth_basis_impl(
        context,
        payload_builder=_basis_payload,
        connect_factory=duckdb.connect,
    )


def _load_spx_session(context: TrendContext) -> tuple[_SPXObservation, ...]:
    return _load_spx_session_impl(
        context,
        basis_loader=_load_previous_rth_basis,
        connect_factory=duckdb.connect,
    )


def _causal_spx(
    context: TrendContext,
    *,
    as_of: datetime,
    build_cache: SessionSurfaceBuildCache,
) -> tuple[
    tuple[_SPXObservation, ...],
    _SPXObservation,
    _SPXObservation | None,
]:
    return _causal_spx_impl(
        context,
        as_of=as_of,
        build_cache=build_cache,
        spx_loader=_load_spx_session,
    )


def _candles(
    observations: tuple[_SPXObservation, ...],
    buckets: tuple[tuple[datetime, datetime], ...],
    *,
    as_of: datetime,
    session_window: SessionSurfaceWindow | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    return _candles_impl(
        observations,
        buckets,
        as_of=as_of,
        session_window=session_window,
    )


def _contracts_and_strikes(
    surface: Mapping[str, Any],
    *,
    expiry: str,
) -> tuple[tuple[SurfaceContract, ...], tuple[Mapping[str, Any], ...]]:
    return _contracts_and_strikes_impl(surface, expiry=expiry)


def _parse_role_frame(
    context: TrendContext,
    *,
    requested: datetime,
    frame: Mapping[str, Any],
    role: str,
) -> _FrameState:
    return _parse_role_frame_impl(
        context,
        requested=requested,
        frame=frame,
        role=role,
        contracts_parser=_contracts_and_strikes,
    )


def _gth_reference_at(
    observations: tuple[_SPXObservation, ...],
    at: datetime,
) -> _SPXObservation | None:
    return _gth_reference_at_impl(observations, at)


def _gth_frame_hash(
    *,
    expiry: str,
    at: datetime,
    reference: _SPXObservation,
    rows: list[tuple[object, ...]],
) -> str:
    return _gth_frame_hash_impl(
        expiry=expiry,
        at=at,
        reference=reference,
        rows=rows,
    )


def _load_gth_frames(
    context: TrendContext,
    *,
    as_of: datetime,
    role: str,
    reference_observations: tuple[_SPXObservation, ...],
) -> tuple[_FrameState, ...]:
    return _load_gth_frames_impl(
        context,
        as_of=as_of,
        role=role,
        reference_observations=reference_observations,
        reference_selector=_gth_reference_at,
        hash_builder=_gth_frame_hash,
        connect_factory=duckdb.connect,
        surface_builder=build_exposure_surface,
    )


def _causal_frames(
    context: TrendContext,
    *,
    as_of: datetime,
    role: str,
    frame_loader: FrameLoader,
    build_cache: SessionSurfaceBuildCache,
    reference_observations: tuple[_SPXObservation, ...] = (),
) -> tuple[_FrameState, ...]:
    return _causal_frames_impl(
        context,
        as_of=as_of,
        role=role,
        frame_loader=frame_loader,
        build_cache=build_cache,
        reference_observations=reference_observations,
        gth_loader=_load_gth_frames,
        frame_parser=_parse_role_frame,
    )


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
            offsets[index : index + chunk_size] for index in range(0, len(offsets), chunk_size)
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
    candidates = [row for row in frames if (row.known_at or row.at) <= end < row.valid_until]
    return candidates[-1] if candidates else None


def _current_frame(
    frames: tuple[_FrameState, ...],
    as_of: datetime,
) -> _FrameState | None:
    cutoff = as_utc(as_of)
    candidates = [row for row in frames if (row.known_at or row.at) <= cutoff < row.valid_until]
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
    session_window: SessionSurfaceWindow | None = None,
    projection_allowed: bool = True,
) -> tuple[
    list[dict[str, object]],
    dict[str, list[list[float | None]]],
    list[float | None],
    list[dict[str, float] | None],
    list[dict[str, float] | None],
]:
    cutoff = as_utc(as_of)
    current = _current_frame(frames, cutoff) if projection_allowed else None
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
    for start, end in buckets:
        session_kind = session_window.segment_kind(start) if session_window is not None else None
        if session_kind == "closed_gap":
            column, values, zero = _missing_column(
                reason="scheduled_closed_gap",
                price_points=len(price_grid),
            )
        elif end <= cutoff:
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
                        "quality": ("ready" if kernel.quality == "ok" else "degraded"),
                        "source_at": _iso(frame.at),
                        "valid_until": _iso(frame.valid_until),
                        "reason": None,
                        "source_frame_sha256": frame.artifact_sha256,
                        "minutes_forward": 0.0,
                        "session_kind": session_kind or frame.session_kind,
                        "source_session_kind": frame.session_kind,
                        "surface_provider": frame.surface_provider,
                        "reference_method": frame.reference_method,
                    }
                    values = {metric: list(kernel.metrics[metric]) for metric in _METRIC_TO_OUTPUT}
                    zero = kernel.zero_ridge
        elif current is None:
            column, values, zero = _missing_column(
                reason="current_validated_frame_unavailable_for_projection",
                price_points=len(price_grid),
            )
        else:
            kernel = projection_by_end.get(end)
            if (
                kernel is None
                or kernel.quality == "unavailable"
                or not any(
                    value is not None
                    for value in (kernel.metrics["signed_gamma"] if kernel is not None else ())
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
                    "scenario_semantics": ("fixed_iv_oi_volume_tau_decay_not_forecast"),
                    "session_kind": session_kind or current.session_kind,
                    "source_session_kind": current.session_kind,
                    "surface_provider": current.surface_provider,
                    "reference_method": current.reference_method,
                }
                values = {metric: list(kernel.metrics[metric]) for metric in _METRIC_TO_OUTPUT}
                zero = kernel.zero_ridge
        column.setdefault("session_kind", session_kind)
        column.setdefault("source_session_kind", None)
        if column.get("kind") == "missing":
            column["source_session_kind"] = None
        column.setdefault(
            "surface_provider",
            ("ibkr" if session_kind == "gth" else "schwab" if session_kind == "rth" else None),
        )
        column.setdefault(
            "reference_method",
            (
                "es_basis_inferred_spx"
                if session_kind == "gth"
                else "direct_index_spx"
                if session_kind == "rth"
                else None
            ),
        )
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
            negative_troughs.append({"price": trough_price, "value": trough_value})
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
    selected = raw_weightings.get(weighting) if isinstance(raw_weightings, Mapping) else None
    metrics = selected.get("metrics") if isinstance(selected, Mapping) else None
    proxy = metrics.get("signed_gamma") if isinstance(metrics, Mapping) else None
    oi_values: list[float] = []
    for side in ("call", "put"):
        leg = row.get(side)
        if isinstance(leg, Mapping):
            value = _nonnegative(leg.get("open_interest"))
            if value is not None:
                oi_values.append(value)
    quality = (
        str(selected.get("quality") or row.get("quality") or "unavailable")
        if isinstance(selected, Mapping)
        else str(row.get("quality") or "unavailable")
    )
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
        abs(value) for row in matrix for value in row if value is not None and math.isfinite(value)
    )
    if values:
        position = (len(values) - 1) * 0.98
        lower = math.floor(position)
        upper = math.ceil(position)
        fraction = position - lower
        robust = values[lower] + (values[upper] - values[lower]) * fraction
        raw_values = [
            value for row in matrix for value in row if value is not None and math.isfinite(value)
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
