"""Causal session-wide SPX candles and fixed-grid SPXW exposure replay."""

from __future__ import annotations

import hmac
import json
import math
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from spx_spark.features.exposure_surface import (
    METRIC_UNITS,
    VECTORIZED_CALCULATION_ENGINE,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.surface_dashboard_replay import (
    QUOTE_LAKE_DATASET,
    REPLAY_POLICY_VERSION,
    ReplaySourceError,
    _canonical_sha256,
    replay_id,
)
from spx_spark.surface_replay_session_data import (
    _candles,
    _causal_frames,
    _causal_spx,
    _fixed_price_grid,
    _robust_domain,
    _strike_profile,
    _surface_missing_ranges,
    _surface_payload,
)
from spx_spark.surface_replay_session_models import (
    MAX_SESSION_SURFACE_CACHE_ARTIFACT_BYTES,
    ReplaySessionSurfaceBusyError,
    ReplaySessionSurfaceCacheError,
    SESSION_SURFACE_BUCKET_MINUTES,
    SESSION_SURFACE_BUCKET_OPTIONS,
    SESSION_SURFACE_CACHE_VERSION,
    SESSION_SURFACE_GTH_QUOTE_MAX_AGE_SECONDS,
    SESSION_SURFACE_KIND,
    SESSION_SURFACE_LOCK_TIMEOUT_SECONDS,
    SESSION_SURFACE_MODE,
    SESSION_SURFACE_POLICY_VERSION,
    SESSION_SURFACE_PRICE_EXTENT_POINTS,
    SESSION_SURFACE_PRICE_STEP,
    SESSION_SURFACE_PRICE_STEP_OPTIONS,
    SESSION_SURFACE_REFERENCE_MAX_AGE_SECONDS,
    SESSION_SURFACE_SCHEMA_VERSION,
    FrameLoader,
    FingerprintLoader,
    SessionSurfaceBuildCache,
    SessionSurfaceSelector,
    SessionSurfaceWindow,
    _cache_clock,
    _finite,
    _iso,
    _METRIC_TO_OUTPUT,
    _session_buckets,
    _SHA256_RE,
    _SPXObservation,
    session_surface_window,
)
from spx_spark.surface_replay_trend import (
    TrendContext,
    _assert_source_fingerprint,
    _source_files,
    _source_hashes,
)


def _reference_payload(
    *,
    current: _SPXObservation | None,
    observations: tuple[_SPXObservation, ...],
    cutoff: datetime,
    window: SessionSurfaceWindow,
) -> dict[str, object]:
    session_kind = window.segment_kind(cutoff)
    if current is not None:
        return {
            "coordinate": "SPX",
            "price": current.price,
            "method": current.method,
            "provider": current.provider,
            "instrument_id": current.instrument_id,
            "source_at": _iso(current.source_at),
            "known_at": _iso(current.known_at),
            "accepted_at": None,
            "valid_until": (
                _iso(current.valid_until)
                if current.valid_until is not None
                else None
            ),
            "quality": "ready",
            "missing_reason": None,
            "basis": dict(current.basis) if current.basis is not None else None,
            "render_style": (
                "inferred_dashed"
                if current.method == "es_basis_inferred_spx"
                else "direct_solid"
            ),
        }
    return {
        "coordinate": "SPX",
        "price": None,
        "method": None,
        "provider": None,
        "instrument_id": None,
        "source_at": None,
        "known_at": None,
        "accepted_at": None,
        "valid_until": None,
        "quality": "unavailable",
        "missing_reason": (
            "scheduled_closed_gap"
            if session_kind == "closed_gap"
            else "fresh_coordinate_reference_unavailable"
        ),
        "basis": None,
        "render_style": None,
    }


def session_surface_cache_path(
    context: TrendContext,
    *,
    as_of: datetime,
    selector: SessionSurfaceSelector,
) -> Path:
    lookback = format(context.lookback_seconds, ".15g").replace(".", "p")
    step = format(selector.price_step, ".15g").replace(".", "p")
    return (
        context.data_root
        / "published"
        / "spxw-surface"
        / "session-surface-cache"
        / f"policy={SESSION_SURFACE_POLICY_VERSION.rsplit('.', maxsplit=1)[-1]}"
        / f"contract={SESSION_SURFACE_CACHE_VERSION}"
        / f"frame={context.frame_minutes}m"
        / f"bucket={selector.bucket_minutes}m"
        / f"step={step}"
        / f"lookback={lookback}s"
        / f"projection={context.projection_policy_sha256}"
        / f"source={context.source_fingerprint}"
        / f"timeline={context.timeline_sha256}"
        / f"role={selector.role}"
        / f"weighting={selector.weighting}"
        / f"{replay_id(as_of)}.json"
    )


def build_session_surface_artifact(
    *,
    context: TrendContext,
    as_of: datetime,
    selector: SessionSurfaceSelector,
    frame_loader: FrameLoader,
    current_source_fingerprint: FingerprintLoader,
    build_cache: SessionSurfaceBuildCache,
    verified_source_hashes: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Build one response whose observed inputs are all bounded by ``as_of``."""

    cutoff = as_utc(as_of)
    window = session_surface_window(context.session_date)
    session_start = as_utc(window.session_start)
    session_end = as_utc(window.session_end)
    if not session_start <= cutoff <= session_end:
        raise ReplaySourceError("session_surface_at_outside_session")
    _assert_source_fingerprint(context, current_source_fingerprint)
    if verified_source_hashes is None:
        source_hashes_before = _source_hashes(context)
    else:
        expected_source_files = set(_source_files(context))
        if set(verified_source_hashes) != expected_source_files or any(
            not isinstance(value, str) or not _SHA256_RE.fullmatch(value)
            for value in verified_source_hashes.values()
        ):
            raise ReplaySourceError("session_surface_verified_source_hashes_invalid")
        source_hashes_before = dict(verified_source_hashes)
    observations, anchor_observation, current_observation = _causal_spx(
        context,
        as_of=cutoff,
        build_cache=build_cache,
    )
    active_session_kind = window.segment_kind(cutoff)
    expected_reference_method = (
        "es_basis_inferred_spx"
        if active_session_kind == "gth"
        else "direct_index_spx"
        if active_session_kind == "rth"
        else None
    )
    if (
        current_observation is not None
        and current_observation.method != expected_reference_method
    ):
        current_observation = None
    frames = _causal_frames(
        context,
        as_of=cutoff,
        role=selector.role,
        frame_loader=frame_loader,
        build_cache=build_cache,
        reference_observations=observations,
    )
    buckets = _session_buckets(context, bucket_minutes=selector.bucket_minutes)
    spot = current_observation.price if current_observation is not None else None
    price_grid_anchor_spot = anchor_observation.price
    price_grid = _fixed_price_grid(price_grid_anchor_spot, step=selector.price_step)
    (
        columns,
        metric_values,
        zero_ridges,
        gamma_positive_peaks,
        gamma_negative_troughs,
    ) = _surface_payload(
        frames,
        buckets,
        as_of=cutoff,
        price_grid=price_grid,
        weighting=selector.weighting,
        build_cache=build_cache,
        session_window=window,
        projection_allowed=current_observation is not None,
    )
    candle_rows, candle_missing = _candles(
        observations,
        buckets,
        as_of=cutoff,
        session_window=window,
    )
    strike_rows, strike_metadata = _strike_profile(
        frames,
        as_of=cutoff,
        weighting=selector.weighting,
    )
    source_hashes_after = (
        _source_hashes(context)
        if verified_source_hashes is None
        else dict(verified_source_hashes)
    )
    if source_hashes_before != source_hashes_after:
        raise ReplaySourceError("session_surface_source_changed_during_build")
    _assert_source_fingerprint(context, current_source_fingerprint)

    research_expiries = DEFAULT_MARKET_CALENDAR.research_expiries(window.session_start)
    expiry_index = 0 if selector.role == "front" else 1
    if len(research_expiries) <= expiry_index:
        raise ReplaySourceError("session_surface_expiry_unavailable")
    expiry = research_expiries[expiry_index].strftime("%Y%m%d")
    if frames and any(row.expiry != expiry for row in frames):
        raise ReplaySourceError("session_surface_expiry_contract_mismatch")
    matrices = {
        output_name: metric_values[metric]
        for metric, output_name in _METRIC_TO_OUTPUT.items()
    }
    missing_ranges = [
        *_surface_missing_ranges(buckets, columns),
        *candle_missing,
    ]
    selected_hashes = [row.artifact_sha256 for row in frames]
    gth_reference_available = any(
        row.method == "es_basis_inferred_spx" for row in observations
    )
    gth_surface_available = any(row.session_kind == "gth" for row in frames)
    gth_data_available = gth_reference_available and gth_surface_available
    payload: dict[str, object] = {
        "schema_version": SESSION_SURFACE_SCHEMA_VERSION,
        "kind": SESSION_SURFACE_KIND,
        "policy_version": SESSION_SURFACE_POLICY_VERSION,
        "mode": SESSION_SURFACE_MODE,
        "session_date": context.session_date.isoformat(),
        "session_start": _iso(session_start),
        "session_end": _iso(session_end),
        "as_of": _iso(cutoff),
        "expiry": expiry,
        "role": selector.role,
        "weighting": selector.weighting,
        "coordinate": "SPX",
        "provider": "mixed",
        "providers": {
            "gth_surface": "ibkr",
            "gth_reference": "schwab",
            "rth_surface": "schwab",
            "rth_reference": "schwab",
        },
        "session_segments": list(window.segments()),
        "trading_class": "SPXW",
        "bucket_minutes": selector.bucket_minutes,
        "price_step": selector.price_step,
        "price_grid": list(price_grid),
        "price_grid_policy": {
            "anchor": math.floor((price_grid_anchor_spot / selector.price_step) + 0.5)
            * selector.price_step,
            "anchor_source": "first_causal_session_spot",
            "anchor_source_at": _iso(anchor_observation.source_at),
            "anchor_known_at": _iso(anchor_observation.known_at),
            "extent_points_each_side": SESSION_SURFACE_PRICE_EXTENT_POINTS,
            "steps_each_side": int(
                round(SESSION_SURFACE_PRICE_EXTENT_POINTS / selector.price_step)
            ),
            "current_spot_in_grid": (
                price_grid[0] <= spot <= price_grid[-1]
                if spot is not None
                else False
            ),
            "observed_spot_range_in_grid": (
                price_grid[0] <= min(row.price for row in observations)
                and max(row.price for row in observations) <= price_grid[-1]
            ),
            "out_of_grid_policy": "retain_fixed_grid_surface_out_of_view",
            "interpolation": "none_rebuilt_from_each_frame_strike_ladder",
        },
        "time_buckets": [
            {"start_at": _iso(start), "end_at": _iso(end)}
            for start, end in buckets
        ],
        "surface_columns": columns,
        **matrices,
        "zero_ridges": zero_ridges,
        "gamma_positive_peaks": gamma_positive_peaks,
        "gamma_negative_troughs": gamma_negative_troughs,
        "candles": candle_rows,
        "candle_policy": {
            "kind": "event_sampled_spx_coordinate_reference_ohlc",
            "price_field": "direct_mark_or_es_minus_frozen_basis",
            "market_clock": "source_at",
            "official_consolidated_ohlc": False,
            "current_partial_allowed": True,
        },
        "strike_profile": strike_rows,
        "strike_profile_metadata": strike_metadata,
        "spot": spot,
        "spot_source_at": (
            _iso(current_observation.source_at)
            if current_observation is not None
            else None
        ),
        "spot_known_at": (
            _iso(current_observation.known_at)
            if current_observation is not None
            else None
        ),
        "reference": _reference_payload(
            current=current_observation,
            observations=observations,
            cutoff=cutoff,
            window=window,
        ),
        "color_domains": {
            output_name: _robust_domain(metric_values[metric])
            for metric, output_name in _METRIC_TO_OUTPUT.items()
        },
        "metric_units": dict(METRIC_UNITS),
        "capabilities": {
            "proxy_position_available": True,
            "participant_position_available": False,
            "open_close_available": False,
            "signed_flow_available": False,
            "dealer_position_sign_available": False,
            "strict_point_in_time_available": False,
            "known_clock_no_lookahead": True,
            "event_sampled_spx_ohlc_available": True,
            "official_spx_ohlc_available": False,
            "exact_sod_available": False,
            "first_validated_baseline_available": bool(frames),
            "projection_is_model_scenario": True,
            "historical_surface_is_model_proxy": True,
            "gth_available": gth_data_available,
            "gth_data_available": gth_data_available,
            "gth_contract_declared": True,
        },
        "missing_ranges": missing_ranges,
        "provenance": {
            "frame_policy_version": REPLAY_POLICY_VERSION,
            "timeline_policy_version": context.timeline_policy_version,
            "session_surface_policy_version": SESSION_SURFACE_POLICY_VERSION,
            "calculation_engine": VECTORIZED_CALCULATION_ENGINE,
            "numeric_reduction": (
                "extended_precision_or_fsum_signed_float64_gross"
            ),
            "projection_policy": dict(context.projection_policy),
            "projection_policy_sha256": context.projection_policy_sha256,
            "timeline_sha256": context.timeline_sha256,
            "source_fingerprint": context.source_fingerprint,
            "dataset": QUOTE_LAKE_DATASET,
            "source_files": list(_source_files(context)),
            "parquet_file_sha256": source_hashes_before,
            "source_files_verified_unchanged_during_build": True,
            "causal_frame_count": len(frames),
            "causal_frame_artifact_sha256": selected_hashes,
            "cutoff_fields": [
                "received_at",
                "source_at",
                "quote_time",
                "trade_time",
                "last_update_at",
            ],
            "lookahead_rows_selected": 0,
            "availability_clock_available": False,
            "availability_clock": "unavailable",
            "point_in_time_confidence": "bounded_not_proven",
            "known_limitations": [
                "response_finished_at_unavailable",
                "received_at_is_cycle_started_at",
                "dealer_position_sign_unknown",
                "spx_ohlc_is_event_sampled_not_official",
                "gth_surface_is_ibkr_oi_volume_proxy",
                "gth_reference_is_schwab_es_minus_frozen_previous_rth_basis",
                "legacy_frame_incomplete_expiry_projection_is_missing",
            ],
            "reference_max_age_seconds": SESSION_SURFACE_REFERENCE_MAX_AGE_SECONDS,
            "gth_surface_quote_max_age_seconds": (
                SESSION_SURFACE_GTH_QUOTE_MAX_AGE_SECONDS
            ),
            "gth_surface_quote_age_semantics": (
                "analytical_exposure_proxy_not_trade_exact_leg_gate"
            ),
            "historical_selection": (
                "latest_causal_validated_frame_with_minutes_forward_0_valid_at_bucket_end"
            ),
            "spx_dedupe_rule": (
                "earliest_known_at_then_received_at_then_source_file_position_per_source_at"
            ),
            "projection_selection": (
                "single_current_causal_frame_fixed_iv_oi_volume_tau_decay_to_session_close"
            ),
            "cache_policy": "source_timeline_selector_as_of_immutable_file_plus_bounded_lru",
        },
    }
    payload["artifact_sha256"] = _canonical_sha256(payload)
    return payload


def load_cached_session_surface(
    *,
    path: Path,
    context: TrendContext,
    as_of: datetime,
    selector: SessionSurfaceSelector,
    current_source_fingerprint: FingerprintLoader,
    verified_source_hashes: Mapping[str, str] | None = None,
) -> dict[str, object]:
    _assert_source_fingerprint(context, current_source_fingerprint)
    try:
        stat = path.stat()
        if stat.st_size <= 0 or stat.st_size > MAX_SESSION_SURFACE_CACHE_ARTIFACT_BYTES:
            raise ReplaySessionSurfaceCacheError("session_surface_cache_size_invalid")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except ReplaySessionSurfaceCacheError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplaySessionSurfaceCacheError("session_surface_cache_unreadable") from exc
    if not isinstance(payload, dict):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_contract_invalid")
    expected = {
        "schema_version": SESSION_SURFACE_SCHEMA_VERSION,
        "kind": SESSION_SURFACE_KIND,
        "policy_version": SESSION_SURFACE_POLICY_VERSION,
        "mode": SESSION_SURFACE_MODE,
        "session_date": context.session_date.isoformat(),
        "session_start": _iso(context.open_at),
        "session_end": _iso(context.close_at),
        "as_of": _iso(as_of),
        "role": selector.role,
        "weighting": selector.weighting,
        "bucket_minutes": selector.bucket_minutes,
        "price_step": selector.price_step,
        "coordinate": "SPX",
        "provider": "mixed",
        "trading_class": "SPXW",
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_contract_invalid")
    window = session_surface_window(context.session_date)
    if payload.get("session_segments") != list(window.segments()):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_segments_invalid")
    stored_hash = payload.get("artifact_sha256")
    if not isinstance(stored_hash, str) or not _SHA256_RE.fullmatch(stored_hash):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_hash_invalid")
    unsigned = dict(payload)
    unsigned.pop("artifact_sha256", None)
    if not hmac.compare_digest(stored_hash, _canonical_sha256(unsigned)):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_hash_mismatch")
    provenance = payload.get("provenance")
    if not isinstance(provenance, Mapping):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_provenance_invalid")
    if (
        provenance.get("source_fingerprint") != context.source_fingerprint
        or provenance.get("timeline_sha256") != context.timeline_sha256
        or provenance.get("projection_policy_sha256")
        != context.projection_policy_sha256
        or provenance.get("lookahead_rows_selected") != 0
        or provenance.get("point_in_time_confidence") != "bounded_not_proven"
        or provenance.get("calculation_engine") != VECTORIZED_CALCULATION_ENGINE
        or provenance.get("numeric_reduction")
        != "extended_precision_or_fsum_signed_float64_gross"
    ):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_provenance_invalid")
    expected_hashes = provenance.get("parquet_file_sha256")
    if verified_source_hashes is None:
        current_hashes = _source_hashes(context)
    else:
        expected_source_files = set(_source_files(context))
        if set(verified_source_hashes) != expected_source_files or any(
            not isinstance(value, str) or not _SHA256_RE.fullmatch(value)
            for value in verified_source_hashes.values()
        ):
            raise ReplaySessionSurfaceCacheError(
                "session_surface_cache_verified_source_hashes_invalid"
            )
        current_hashes = dict(verified_source_hashes)
    if not isinstance(expected_hashes, Mapping) or set(expected_hashes) != set(current_hashes):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_source_invalid")
    for name, actual in current_hashes.items():
        expected_hash = expected_hashes.get(name)
        if (
            not isinstance(expected_hash, str)
            or not _SHA256_RE.fullmatch(expected_hash)
            or not hmac.compare_digest(expected_hash, actual)
        ):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_source_hash_mismatch")
    cutoff = as_utc(as_of)
    expected_buckets = [
        {"start_at": _iso(start), "end_at": _iso(end)}
        for start, end in _session_buckets(
            context,
            bucket_minutes=selector.bucket_minutes,
        )
    ]
    time_buckets = payload.get("time_buckets")
    price_grid = payload.get("price_grid")
    columns = payload.get("surface_columns")
    candles = payload.get("candles")
    strike_profile = payload.get("strike_profile")
    zero_ridges = payload.get("zero_ridges")
    positive_peaks = payload.get("gamma_positive_peaks")
    negative_troughs = payload.get("gamma_negative_troughs")
    provenance_hashes = provenance.get("causal_frame_artifact_sha256")
    if (
        time_buckets != expected_buckets
        or not isinstance(price_grid, list)
        or len(price_grid)
        != int(round(2 * SESSION_SURFACE_PRICE_EXTENT_POINTS / selector.price_step))
        + 1
        or not isinstance(columns, list)
        or len(columns) != len(expected_buckets)
        or not isinstance(candles, list)
        or not isinstance(strike_profile, list)
        or not isinstance(zero_ridges, list)
        or len(zero_ridges) != len(expected_buckets)
        or not isinstance(positive_peaks, list)
        or len(positive_peaks) != len(expected_buckets)
        or not isinstance(negative_troughs, list)
        or len(negative_troughs) != len(expected_buckets)
        or not isinstance(provenance_hashes, list)
    ):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_shape_invalid")
    resolved_prices = [_finite(value) for value in price_grid]
    if any(value is None or value <= 0 for value in resolved_prices) or any(
        not math.isclose(
            float(right) - float(left),
            selector.price_step,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        for left, right in zip(resolved_prices, resolved_prices[1:], strict=False)
    ):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_price_grid_invalid")
    matrices: dict[str, list[Any]] = {}
    for metric, output_name in _METRIC_TO_OUTPUT.items():
        matrix = payload.get(output_name)
        if not isinstance(matrix, list) or len(matrix) != len(expected_buckets):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_shape_invalid")
        for row in matrix:
            if not isinstance(row, list) or len(row) != len(price_grid):
                raise ReplaySessionSurfaceCacheError("session_surface_cache_shape_invalid")
            for value in row:
                if value is not None and _finite(value) is None:
                    raise ReplaySessionSurfaceCacheError(
                        "session_surface_cache_matrix_invalid"
                    )
                if metric == "gross_gamma" and value is not None and float(value) < 0:
                    raise ReplaySessionSurfaceCacheError(
                        "session_surface_cache_matrix_invalid"
                    )
        matrices[metric] = matrix
    for index, (bucket, column) in enumerate(
        zip(expected_buckets, columns, strict=True)
    ):
        if not isinstance(column, Mapping):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_shape_invalid")
        kind = column.get("kind")
        if kind not in {"historical", "projection", "missing"}:
            raise ReplaySessionSurfaceCacheError("session_surface_cache_column_invalid")
        source_at = column.get("source_at")
        source_clock = _cache_clock(source_at) if source_at is not None else None
        if source_clock is not None and source_clock > cutoff:
            raise ReplaySessionSurfaceCacheError("session_surface_cache_lookahead")
        bucket_end = _cache_clock(bucket["end_at"])
        if kind == "historical" and (
            source_clock is None
            or source_clock > bucket_end
            or bucket_end > cutoff
        ):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_lookahead")
        if kind == "projection" and (
            source_clock is None or bucket_end <= cutoff
        ):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_lookahead")
        if kind == "missing" and (
            any(
                value is not None
                for metric in _METRIC_TO_OUTPUT
                for value in matrices[metric][index]
            )
            or zero_ridges[index] is not None
            or positive_peaks[index] is not None
            or negative_troughs[index] is not None
        ):
            raise ReplaySessionSurfaceCacheError(
                "session_surface_cache_missing_not_null"
            )
        for extremum in (positive_peaks[index], negative_troughs[index]):
            if extremum is None:
                continue
            if not isinstance(extremum, Mapping) or (
                _finite(extremum.get("price")) is None
                or _finite(extremum.get("value")) is None
            ):
                raise ReplaySessionSurfaceCacheError(
                    "session_surface_cache_extremum_invalid"
                )
    for candle in candles:
        if not isinstance(candle, Mapping):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_shape_invalid")
        start_at = _cache_clock(candle.get("start_at"))
        end_at = _cache_clock(candle.get("end_at"))
        source_at = candle.get("source_at")
        known_at = candle.get("known_at")
        if candle.get("accepted_at") is not None:
            raise ReplaySessionSurfaceCacheError(
                "session_surface_cache_acceptance_clock_invalid"
            )
        if (
            source_at is not None
            and _cache_clock(source_at) > cutoff
        ) or (
            known_at is not None
            and _cache_clock(known_at) > cutoff
        ) or (
            start_at > cutoff
            or end_at <= start_at
            or (candle.get("complete") is True and end_at > cutoff)
        ):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_lookahead")
        prices = [_finite(candle.get(name)) for name in ("open", "high", "low", "close")]
        sample_count = candle.get("sample_count")
        if (
            any(value is None for value in prices)
            or isinstance(sample_count, bool)
            or not isinstance(sample_count, int)
            or sample_count <= 0
            or not isinstance(candle.get("complete"), bool)
        ):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_candle_invalid")
    reference = payload.get("reference")
    if not isinstance(reference, Mapping) or reference.get("accepted_at") is not None:
        raise ReplaySessionSurfaceCacheError("session_surface_cache_reference_invalid")
    reference_quality = reference.get("quality")
    if reference_quality == "ready":
        spot_source_at = _cache_clock(payload.get("spot_source_at"))
        spot_known_at = _cache_clock(payload.get("spot_known_at"))
        if (
            spot_source_at > cutoff
            or spot_known_at > cutoff
            or _finite(payload.get("spot")) is None
            or _finite(reference.get("price")) != _finite(payload.get("spot"))
            or _cache_clock(reference.get("source_at")) != spot_source_at
            or _cache_clock(reference.get("known_at")) != spot_known_at
        ):
            raise ReplaySessionSurfaceCacheError("session_surface_cache_lookahead")
    elif reference_quality == "unavailable":
        if any(
            value is not None
            for value in (
                payload.get("spot"),
                payload.get("spot_source_at"),
                payload.get("spot_known_at"),
                reference.get("price"),
                reference.get("source_at"),
                reference.get("known_at"),
                reference.get("valid_until"),
            )
        ):
            raise ReplaySessionSurfaceCacheError(
                "session_surface_cache_reference_invalid"
            )
    else:
        raise ReplaySessionSurfaceCacheError("session_surface_cache_reference_invalid")
    strike_metadata = payload.get("strike_profile_metadata")
    if not isinstance(strike_metadata, Mapping):
        raise ReplaySessionSurfaceCacheError("session_surface_cache_strike_invalid")
    for key in ("baseline_at", "current_at"):
        value = strike_metadata.get(key)
        if value is not None and _cache_clock(value) > cutoff:
            raise ReplaySessionSurfaceCacheError("session_surface_cache_lookahead")
    capabilities = payload.get("capabilities")
    required_capabilities = {
        "proxy_position_available": True,
        "participant_position_available": False,
        "open_close_available": False,
        "signed_flow_available": False,
        "strict_point_in_time_available": False,
        "known_clock_no_lookahead": True,
        "official_spx_ohlc_available": False,
        "exact_sod_available": False,
    }
    if not isinstance(capabilities, Mapping) or any(
        capabilities.get(key) is not value
        for key, value in required_capabilities.items()
    ) or not isinstance(capabilities.get("gth_available"), bool) or (
        capabilities.get("gth_data_available")
        is not capabilities.get("gth_available")
    ) or capabilities.get("gth_contract_declared") is not True:
        raise ReplaySessionSurfaceCacheError(
            "session_surface_cache_capabilities_invalid"
        )
    _assert_source_fingerprint(context, current_source_fingerprint)
    return payload


def materialize_session_surface(
    *,
    context: TrendContext,
    as_of: datetime,
    selector: SessionSurfaceSelector,
    frame_loader: FrameLoader,
    current_source_fingerprint: FingerprintLoader,
    build_cache: SessionSurfaceBuildCache,
    verified_source_hashes: Mapping[str, str] | None = None,
) -> dict[str, object]:
    destination = session_surface_cache_path(
        context,
        as_of=as_of,
        selector=selector,
    )
    if destination.is_file():
        return load_cached_session_surface(
            path=destination,
            context=context,
            as_of=as_of,
            selector=selector,
            current_source_fingerprint=current_source_fingerprint,
            verified_source_hashes=verified_source_hashes,
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with exclusive_state_lock(
            destination,
            timeout_seconds=SESSION_SURFACE_LOCK_TIMEOUT_SECONDS,
        ):
            if destination.is_file():
                return load_cached_session_surface(
                    path=destination,
                    context=context,
                    as_of=as_of,
                    selector=selector,
                    current_source_fingerprint=current_source_fingerprint,
                    verified_source_hashes=verified_source_hashes,
                )
            payload = build_session_surface_artifact(
                context=context,
                as_of=as_of,
                selector=selector,
                frame_loader=frame_loader,
                current_source_fingerprint=current_source_fingerprint,
                build_cache=build_cache,
                verified_source_hashes=verified_source_hashes,
            )
            atomic_write_json_secure(destination, payload)
            return load_cached_session_surface(
                path=destination,
                context=context,
                as_of=as_of,
                selector=selector,
                current_source_fingerprint=current_source_fingerprint,
                verified_source_hashes=verified_source_hashes,
            )
    except TimeoutError as exc:
        raise ReplaySessionSurfaceBusyError("session_surface_generation_locked") from exc


__all__ = (
    "SESSION_SURFACE_BUCKET_MINUTES",
    "SESSION_SURFACE_BUCKET_OPTIONS",
    "SESSION_SURFACE_CACHE_VERSION",
    "SESSION_SURFACE_KIND",
    "SESSION_SURFACE_LOCK_TIMEOUT_SECONDS",
    "SESSION_SURFACE_MODE",
    "SESSION_SURFACE_POLICY_VERSION",
    "SESSION_SURFACE_PRICE_STEP",
    "SESSION_SURFACE_PRICE_STEP_OPTIONS",
    "SESSION_SURFACE_SCHEMA_VERSION",
    "ReplaySessionSurfaceBusyError",
    "ReplaySessionSurfaceCacheError",
    "SessionSurfaceBuildCache",
    "SessionSurfaceSelector",
    "build_session_surface_artifact",
    "load_cached_session_surface",
    "materialize_session_surface",
    "session_surface_cache_path",
)
