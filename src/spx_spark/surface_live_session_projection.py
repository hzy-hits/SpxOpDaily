"""Lock-free materialization of one live SPXW Session Surface response."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta
from typing import Any

from spx_spark.features.exposure_surface import METRIC_UNITS
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.surface_artifact import canonical_sha256
from spx_spark.surface_live_session_models import (
    LIVE_COORDINATE,
    LIVE_PRICE_EXTENT_POINTS,
    LIVE_SESSION_KIND,
    LIVE_SESSION_MODE,
    LIVE_SESSION_POLICY_VERSION,
    LIVE_TRADING_CLASS,
    LiveSelector,
    LiveSessionError,
    finite,
    frame_state,
    iso,
    mapping,
    parse_clock,
    signed_payload,
)
from spx_spark.surface_replay_session_data import (
    _fixed_price_grid,
    _robust_domain,
    _strike_profile,
    _surface_missing_ranges,
    _surface_payload,
)
from spx_spark.surface_replay_session_models import (
    SessionSurfaceBuildCache,
    _METRIC_TO_OUTPUT,
)


def _buckets(start: datetime, close: datetime) -> tuple[tuple[datetime, datetime], ...]:
    rows: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < close:
        end = min(cursor + timedelta(minutes=5), close)
        rows.append((cursor, end))
        cursor = end
    return tuple(rows)


def _frame_payload_index(
    role: str,
    runtime: Mapping[str, Any],
    boundaries: tuple[Mapping[str, Any], ...],
) -> dict[str, Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for key in ("baseline_by_role", "candidate_by_role"):
        values = runtime.get(key)
        if isinstance(values, Mapping) and isinstance(values.get(role), Mapping):
            rows.append(values[role])
    for boundary in boundaries:
        values = boundary.get("frame_by_role")
        if isinstance(values, Mapping) and isinstance(values.get(role), Mapping):
            rows.append(values[role])
    return {
        str(row["artifact_sha256"]): row
        for row in rows
        if isinstance(row.get("artifact_sha256"), str)
    }


def _all_frames(
    role: str,
    runtime: Mapping[str, Any],
    boundaries: tuple[Mapping[str, Any], ...],
) -> tuple[Any, ...]:
    values = _frame_payload_index(role, runtime, boundaries).values()
    return tuple(sorted((frame_state(row) for row in values), key=lambda row: row.at))


def _role_candidate(role: str, runtime: Mapping[str, Any]) -> Mapping[str, Any] | None:
    candidates = runtime.get("candidate_by_role")
    row = candidates.get(role) if isinstance(candidates, Mapping) else None
    return row if isinstance(row, Mapping) else None


def _valid_candidate(
    role: str,
    runtime: Mapping[str, Any],
    cutoff: datetime,
) -> Mapping[str, Any] | None:
    row = _role_candidate(role, runtime)
    if row is None:
        return None
    accepted = parse_clock(row.get("accepted_at"), code="live_candidate_accepted_invalid")
    valid_until = parse_clock(row.get("valid_until"), code="live_candidate_lease_invalid")
    return row if accepted <= cutoff < valid_until else None


def _missing_column(price_count: int, reason: str) -> tuple[dict[str, object], dict[str, list[None]]]:
    return (
        {
            "kind": "missing",
            "quality": "unavailable",
            "source_at": None,
            "known_at": None,
            "accepted_at": None,
            "valid_until": None,
            "reason": reason,
        },
        {metric: [None] * price_count for metric in _METRIC_TO_OUTPUT},
    )


def _historical_prefix(
    *,
    role: str,
    weighting: str,
    buckets: tuple[tuple[datetime, datetime], ...],
    cutoff: datetime,
    price_count: int,
    boundaries: tuple[Mapping[str, Any], ...],
) -> tuple[
    list[dict[str, object]],
    dict[str, list[list[float | None]]],
    list[float | None],
    list[dict[str, float] | None],
    list[dict[str, float] | None],
]:
    by_end = {row.get("end_at"): row for row in boundaries}
    columns: list[dict[str, object]] = []
    metrics: dict[str, list[list[float | None]]] = {
        metric: [] for metric in _METRIC_TO_OUTPUT
    }
    zero: list[float | None] = []
    positive: list[dict[str, float] | None] = []
    negative: list[dict[str, float] | None] = []
    for _start, end in buckets:
        if end > cutoff:
            column, values = _missing_column(price_count, "future_projection_not_merged")
            columns.append(column)
            for metric in _METRIC_TO_OUTPUT:
                metrics[metric].append(values[metric])
            zero.append(None)
            positive.append(None)
            negative.append(None)
            continue
        boundary = by_end.get(iso(end))
        frozen_by_role = boundary.get("frozen_columns") if isinstance(boundary, Mapping) else None
        frozen_by_weight = (
            frozen_by_role.get(role) if isinstance(frozen_by_role, Mapping) else None
        )
        frozen = (
            frozen_by_weight.get(weighting)
            if isinstance(frozen_by_weight, Mapping)
            else None
        )
        if not isinstance(frozen, Mapping):
            column, values = _missing_column(
                price_count,
                "validated_surface_unavailable_at_bucket_end",
            )
            columns.append(column)
            for metric in _METRIC_TO_OUTPUT:
                metrics[metric].append(values[metric])
            zero.append(None)
            positive.append(None)
            negative.append(None)
            continue
        if frozen.get("quality") == "unavailable":
            column, values = _missing_column(
                price_count,
                "frozen_kernel_unavailable",
            )
            columns.append(column)
            for metric in _METRIC_TO_OUTPUT:
                metrics[metric].append(values[metric])
            zero.append(None)
            positive.append(None)
            negative.append(None)
            continue
        raw_metrics = mapping(frozen.get("metrics"), code="live_frozen_metrics_invalid")
        parsed: dict[str, list[float | None]] = {}
        for metric in _METRIC_TO_OUTPUT:
            values = raw_metrics.get(metric)
            if not isinstance(values, list) or len(values) != price_count:
                raise LiveSessionError("live_frozen_column_shape_invalid")
            parsed[metric] = [finite(value) if value is not None else None for value in values]
        if not any(value is not None for value in parsed["signed_gamma"]):
            column, values = _missing_column(price_count, "frozen_kernel_unavailable")
            columns.append(column)
            for metric in _METRIC_TO_OUTPUT:
                metrics[metric].append(values[metric])
            zero.append(None)
            positive.append(None)
            negative.append(None)
            continue
        column = {
            "kind": "historical",
            "quality": str(frozen.get("quality") or "degraded"),
            "source_at": frozen.get("source_at"),
            "known_at": frozen.get("known_at"),
            "accepted_at": frozen.get("accepted_at"),
            "valid_until": frozen.get("valid_until"),
            "reason": None,
            "source_frame_sha256": frozen.get("source_frame_sha256"),
            "model_as_of": frozen.get("model_as_of"),
            "scenario_at": frozen.get("scenario_at"),
            "minutes_forward": frozen.get("minutes_forward"),
        }
        columns.append(column)
        for metric in _METRIC_TO_OUTPUT:
            metrics[metric].append(parsed[metric])
        zero.append(finite(frozen.get("zero_ridge")))
        raw_positive = frozen.get("gamma_positive_peak")
        raw_negative = frozen.get("gamma_negative_trough")
        positive.append(dict(raw_positive) if isinstance(raw_positive, Mapping) else None)
        negative.append(dict(raw_negative) if isinstance(raw_negative, Mapping) else None)
    return columns, metrics, zero, positive, negative


def _merge_projection(
    historical: tuple[Any, ...],
    projected: tuple[Any, ...],
    *,
    buckets: tuple[tuple[datetime, datetime], ...],
    cutoff: datetime,
    projection_allowed: bool,
) -> tuple[Any, ...]:
    h_columns, h_metrics, h_zero, h_positive, h_negative = historical
    p_columns, p_metrics, p_zero, p_positive, p_negative = projected
    columns: list[dict[str, object]] = []
    metrics = {metric: [] for metric in _METRIC_TO_OUTPUT}
    zero: list[float | None] = []
    positive: list[dict[str, float] | None] = []
    negative: list[dict[str, float] | None] = []
    for index, (_start, end) in enumerate(buckets):
        use_projection = end > cutoff and projection_allowed
        columns.append(dict(p_columns[index] if use_projection else h_columns[index]))
        for metric in _METRIC_TO_OUTPUT:
            values = p_metrics[metric][index] if use_projection else h_metrics[metric][index]
            metrics[metric].append(list(values))
        zero.append(p_zero[index] if use_projection else h_zero[index])
        positive.append(p_positive[index] if use_projection else h_positive[index])
        negative.append(p_negative[index] if use_projection else h_negative[index])
    return columns, metrics, zero, positive, negative


def _sample_candle(
    samples: list[Mapping[str, Any]],
    *,
    start: datetime,
    end: datetime,
    cutoff: datetime,
) -> dict[str, object] | None:
    selected: list[tuple[datetime, datetime, float, str]] = []
    for sample in samples:
        source = parse_clock(sample.get("source_at"), code="live_sample_source_invalid")
        accepted = parse_clock(sample.get("accepted_at"), code="live_sample_accepted_invalid")
        price = finite(sample.get("price"))
        if price is not None and price > 0 and start <= source < end and accepted <= cutoff:
            selected.append((source, accepted, price, str(sample.get("provider") or "")))
    if not selected:
        return None
    selected.sort(key=lambda row: (row[0], row[1]))
    prices = [row[2] for row in selected]
    return {
        "start_at": iso(start),
        "end_at": iso(end),
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "sample_count": len(prices),
        "complete": False,
        "source_at": iso(selected[-1][0]),
        "known_at": iso(max(row[1] for row in selected)),
        "quality": "event_sampled",
        "providers": sorted({row[3] for row in selected if row[3]}),
    }


def _candles(
    boundaries: tuple[Mapping[str, Any], ...],
    runtime: Mapping[str, Any],
    *,
    buckets: tuple[tuple[datetime, datetime], ...],
    cutoff: datetime,
    allow_partial: bool,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_end = {row.get("end_at"): row for row in boundaries}
    raw_samples = runtime.get("candle_samples")
    samples = [row for row in raw_samples if isinstance(row, Mapping)] if isinstance(raw_samples, list) else []
    output: list[dict[str, object]] = []
    missing: list[dict[str, object]] = []
    for start, end in buckets:
        if start > cutoff:
            break
        boundary = by_end.get(iso(end))
        candle = boundary.get("candle") if isinstance(boundary, Mapping) else None
        if isinstance(candle, Mapping):
            output.append(dict(candle))
            continue
        partial = (
            _sample_candle(samples, start=start, end=end, cutoff=cutoff)
            if allow_partial and end > cutoff
            else None
        )
        if partial is not None:
            output.append(partial)
        else:
            missing.append(
                {
                    "start_at": iso(start),
                    "end_at": iso(min(end, cutoff)),
                    "reason": "spx_event_samples_unavailable",
                    "component": "candles",
                }
            )
    return output, [row for row in missing if row["start_at"] != row["end_at"]]


def _combined_live_clocks(
    role_frame: Mapping[str, Any] | None,
    spot: Mapping[str, Any] | None,
) -> tuple[datetime | None, datetime | None, datetime | None, Mapping[str, Any] | None]:
    if role_frame is None:
        if not isinstance(spot, Mapping):
            return None, None, None, None
        # A selector can be absent while other roles have already frozen the
        # shared session prefix. Preserve a coherent root availability clock,
        # but never expose the spot as selector-valid without a role frame.
        return (
            parse_clock(spot.get("source_as_of"), code="live_spot_model_clock_invalid"),
            parse_clock(spot.get("accepted_at"), code="live_spot_accepted_invalid"),
            parse_clock(spot.get("valid_until"), code="live_spot_lease_invalid"),
            None,
        )
    source = parse_clock(role_frame.get("model_as_of"), code="live_role_source_clock_invalid")
    accepted = parse_clock(role_frame.get("accepted_at"), code="live_role_accepted_invalid")
    valid = parse_clock(role_frame.get("valid_until"), code="live_role_lease_invalid")
    selected_spot = None
    if isinstance(spot, Mapping):
        spot_source = parse_clock(spot.get("source_as_of"), code="live_spot_model_clock_invalid")
        spot_accepted = parse_clock(spot.get("accepted_at"), code="live_spot_accepted_invalid")
        spot_valid = parse_clock(spot.get("valid_until"), code="live_spot_lease_invalid")
        combined_source = max(source, spot_source)
        combined_accepted = max(accepted, spot_accepted)
        combined_valid = min(valid, spot_valid)
        if combined_source <= combined_accepted < combined_valid:
            source, accepted, valid = combined_source, combined_accepted, combined_valid
            selected_spot = spot
    return source, accepted, valid, selected_spot


def build_live_session_surface(
    *,
    selector: LiveSelector,
    request_as_of: datetime,
    active_date: date,
    manifest: Mapping[str, Any],
    runtime: Mapping[str, Any],
    boundaries: tuple[Mapping[str, Any], ...],
    build_cache: SessionSurfaceBuildCache,
    finished_at: Callable[[], datetime],
) -> dict[str, object]:
    """Build outside the accumulator lock, then gate dynamics at completion."""

    request_clock = as_utc(request_as_of)
    start = parse_clock(manifest.get("session_start"), code="live_manifest_start_invalid")
    close = parse_clock(manifest.get("session_end"), code="live_manifest_end_invalid")
    if request_clock < start:
        raise LiveSessionError("live_session_not_started")
    cutoff = min(request_clock, close)
    anchor = mapping(manifest.get("anchor"), code="live_manifest_anchor_invalid")
    anchor_spot = finite(anchor.get("grid_anchor"))
    if anchor_spot is None or anchor_spot <= 0:
        raise LiveSessionError("live_anchor_invalid")
    buckets = _buckets(start, close)
    price_grid = _fixed_price_grid(anchor_spot, step=selector.price_step)
    role_frame = _role_candidate(selector.role, runtime)
    current_frame = _valid_candidate(selector.role, runtime, cutoff)
    current_frames = (frame_state(current_frame),) if current_frame is not None else ()
    historical = _historical_prefix(
        role=selector.role,
        weighting=selector.weighting,
        buckets=buckets,
        cutoff=cutoff,
        price_count=len(price_grid),
        boundaries=boundaries,
    )
    projected = _surface_payload(
        current_frames,
        buckets,
        as_of=cutoff,
        price_grid=price_grid,
        weighting=selector.weighting,
        build_cache=build_cache,
    )
    raw_spot = runtime.get("latest_spot")
    spot = raw_spot if isinstance(raw_spot, Mapping) else None
    source_as_of, accepted_at, valid_until, selected_spot = _combined_live_clocks(
        role_frame,
        spot,
    )
    dynamic_candidate = (
        current_frame is not None
        and selected_spot is not None
        and source_as_of is not None
        and accepted_at is not None
        and valid_until is not None
        and source_as_of <= accepted_at <= cutoff < valid_until
        and cutoff < close
    )
    columns, metric_values, zero_ridges, positive_peaks, negative_troughs = _merge_projection(
        historical,
        projected,
        buckets=buckets,
        cutoff=cutoff,
        projection_allowed=dynamic_candidate,
    )
    frame_payloads = _frame_payload_index(selector.role, runtime, boundaries)
    for column in columns:
        if column.get("kind") == "missing":
            column["source_at"] = None
            column["known_at"] = None
            column["accepted_at"] = None
            column["valid_until"] = None
            column.pop("source_frame_sha256", None)
            continue
        source_hash = column.get("source_frame_sha256")
        source = frame_payloads.get(source_hash) if isinstance(source_hash, str) else None
        if source is not None and column.get("kind") == "projection":
            column["source_at"] = source.get("source_at")
            column["known_at"] = source.get("known_at")
            column["accepted_at"] = source.get("accepted_at")
    frames = _all_frames(selector.role, runtime, boundaries)
    strike_rows, strike_metadata = _strike_profile(
        frames,
        as_of=cutoff,
        weighting=selector.weighting,
    )
    candles, candle_missing = _candles(
        boundaries,
        runtime,
        buckets=buckets,
        cutoff=cutoff,
        allow_partial=dynamic_candidate,
    )
    color_domains = {
        output_name: _robust_domain(metric_values[metric])
        for metric, output_name in _METRIC_TO_OUTPUT.items()
    }
    # Lease validity is sampled only after all expensive kernel, strike,
    # candle and color-domain work.  If computation crossed the exclusive
    # boundary, rebuild the cheap presentation layer from frozen history.
    finished = max(as_utc(finished_at()), request_clock)
    dynamic_valid = (
        dynamic_candidate
        and valid_until is not None
        and finished < valid_until
        and finished < close
    )
    if not dynamic_valid:
        columns, metric_values, zero_ridges, positive_peaks, negative_troughs = (
            _merge_projection(
                historical,
                projected,
                buckets=buckets,
                cutoff=cutoff,
                projection_allowed=False,
            )
        )
        for column in columns:
            if column.get("kind") == "missing":
                column["source_at"] = None
                column["known_at"] = None
                column["accepted_at"] = None
                column["valid_until"] = None
                column.pop("source_frame_sha256", None)
        for row in strike_rows:
            row["current_proxy"] = None
            row["current_open_interest"] = None
        strike_metadata["current_at"] = None
        selected_spot = None
        candles, candle_missing = _candles(
            boundaries,
            runtime,
            buckets=buckets,
            cutoff=cutoff,
            allow_partial=False,
        )
        color_domains = {
            output_name: _robust_domain(metric_values[metric])
            for metric, output_name in _METRIC_TO_OUTPUT.items()
        }
    historical_available = any(row.get("kind") == "historical" for row in columns)
    closed = finished >= close
    if closed:
        live_status = "closed"
    elif dynamic_valid and selected_spot is not None:
        live_status = "ready"
    elif valid_until is not None and finished >= valid_until:
        live_status = "lease_expired"
    elif accepted_at is None:
        live_status = "initializing"
    elif historical_available:
        live_status = "degraded"
    else:
        live_status = "unavailable"
    status = "ready" if live_status == "ready" else "degraded" if historical_available else "unavailable"
    boundary_hashes = [
        str(row["artifact_sha256"])
        for row in boundaries
        if isinstance(row.get("artifact_sha256"), str)
    ]
    matrices = {
        output_name: metric_values[metric]
        for metric, output_name in _METRIC_TO_OUTPUT.items()
    }
    providers: set[str] = set()
    for row in frame_payloads.values():
        raw = row.get("providers")
        if isinstance(raw, list):
            providers.update(str(value) for value in raw if str(value))
    if selected_spot is not None and selected_spot.get("provider"):
        providers.add(str(selected_spot["provider"]))
    provider_values = sorted(providers)
    expiry_index = 0 if selector.role == "front" else 1
    expiry = DEFAULT_MARKET_CALENDAR.research_expiries(start)[expiry_index].strftime("%Y%m%d")
    frozen_through = runtime.get("history_frozen_through")
    payload: dict[str, object] = {
        "schema_version": 1,
        "kind": LIVE_SESSION_KIND,
        "policy_version": LIVE_SESSION_POLICY_VERSION,
        "mode": LIVE_SESSION_MODE,
        "status": status,
        "live_status": live_status,
        "session_date": active_date.isoformat(),
        "session_start": iso(start),
        "session_end": iso(close),
        "source_as_of": iso(source_as_of) if source_as_of else None,
        "accepted_at": iso(accepted_at) if accepted_at else None,
        "as_of": iso(cutoff),
        "created_at": iso(finished),
        "server_time": iso(finished),
        "valid_until": iso(valid_until) if valid_until else None,
        "history_frozen_through": frozen_through,
        "accumulator_started_at": manifest.get("accumulator_started_at"),
        "expiry": expiry,
        "role": selector.role,
        "weighting": selector.weighting,
        "coordinate": LIVE_COORDINATE,
        "provider": provider_values[0] if len(provider_values) == 1 else "mixed" if provider_values else "unavailable",
        "providers": provider_values,
        "trading_class": LIVE_TRADING_CLASS,
        "bucket_minutes": selector.bucket_minutes,
        "price_step": selector.price_step,
        "price_grid": list(price_grid),
        "price_grid_policy": {
            "anchor": anchor_spot,
            "anchor_source": "first_accepted_direct_index_spx",
            "anchor_source_at": anchor.get("source_at"),
            "anchor_known_at": anchor.get("accepted_at"),
            "extent_points_each_side": LIVE_PRICE_EXTENT_POINTS,
            "current_spot_in_grid": (
                price_grid[0] <= float(selected_spot["price"]) <= price_grid[-1]
                if selected_spot is not None
                else None
            ),
            "out_of_grid_policy": "retain_fixed_grid_surface_out_of_view",
            "interpolation": "none_rebuilt_from_accepted_strike_ladder",
        },
        "time_buckets": [
            {"start_at": iso(bucket_start), "end_at": iso(bucket_end)}
            for bucket_start, bucket_end in buckets
        ],
        "surface_columns": columns,
        **matrices,
        "zero_ridges": zero_ridges,
        "gamma_positive_peaks": positive_peaks,
        "gamma_negative_troughs": negative_troughs,
        "candles": candles,
        "strike_profile": strike_rows,
        "strike_profile_metadata": strike_metadata,
        "spot": selected_spot.get("price") if selected_spot else None,
        "spot_source_at": selected_spot.get("source_at") if selected_spot else None,
        "spot_known_at": selected_spot.get("accepted_at") if selected_spot else None,
        "color_domains": color_domains,
        "metric_units": dict(METRIC_UNITS),
        "availability": {
            "projection_available": dynamic_valid,
            "current_strike_profile_available": dynamic_valid,
            "current_spot_available": dynamic_valid and selected_spot is not None,
            "historical_surface_available": historical_available,
        },
        "capabilities": {
            "proxy_position_available": True,
            "participant_position_available": False,
            "open_close_available": False,
            "signed_flow_available": False,
            "dealer_position_sign_available": False,
            "strict_point_in_time_available": True,
            "known_clock_no_lookahead": True,
            "event_sampled_spx_ohlc_available": True,
            "official_spx_ohlc_available": False,
            "exact_sod_available": False,
            "first_validated_baseline_available": bool(frames),
            "projection_is_model_scenario": True,
            "historical_surface_is_model_proxy": True,
            "gth_available": False,
        },
        "missing_ranges": [*_surface_missing_ranges(buckets, columns), *candle_missing],
        "provenance": {
            "session_surface_policy_version": LIVE_SESSION_POLICY_VERSION,
            "availability_clock_available": True,
            "availability_clock": "accepted_at",
            "point_in_time_confidence": "observed_live",
            "lookahead_rows_selected": 0,
            "per_leg_availability_clock_available": False,
            "source_as_of": iso(source_as_of) if source_as_of else None,
            "known_limitations": [
                "per_leg_response_finished_at_unavailable",
                "dealer_position_sign_unknown",
                "spx_ohlc_is_event_sampled_not_official",
            ],
            "historical_selection": "immutable_boundary_derived_column",
            "projection_selection": "latest_selector_lease_valid_accepted_frame",
            "frozen_boundary_artifact_sha256": boundary_hashes,
            "frozen_history_prefix_sha256": canonical_sha256(boundary_hashes),
            "boundary_tip_sha256": runtime.get("boundary_tip_sha256"),
            "source_snapshot_sha256": (
                role_frame.get("source_snapshot_sha256")
                if isinstance(role_frame, Mapping)
                else None
            ),
            "automatic_ordering": False,
        },
        "automatic_ordering": False,
    }
    return signed_payload(payload)


__all__ = ("build_live_session_surface",)
