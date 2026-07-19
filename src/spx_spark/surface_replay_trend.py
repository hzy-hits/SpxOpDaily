"""Compact, source-bound intraday SPX and gamma replay artifacts."""

from __future__ import annotations

import hmac
import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from spx_spark.marketdata import as_utc
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.surface_dashboard_replay import (
    QUOTE_LAKE_DATASET,
    REPLAY_POLICY_VERSION,
    ReplaySourceError,
    _canonical_sha256,
    _sha256,
    replay_id,
)


TREND_SCHEMA_VERSION = 1
TREND_KIND = "spxw_intraday_gamma_replay"
TREND_MODE = "replay"
TREND_POLICY_VERSION = "spxw_surface_replay_trend.v1"
FRAME_POLICY_VERSION = REPLAY_POLICY_VERSION
MAX_TREND_CACHE_ARTIFACT_BYTES = 64 * 1024 * 1024

TREND_ROLES = frozenset({"front", "next"})
TREND_WEIGHTINGS = frozenset({"oi_weighted", "volume_weighted"})
TREND_METRICS = frozenset({"signed_gamma", "gross_gamma", "charm", "vanna"})

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")


class ReplayTrendCacheError(RuntimeError):
    """A cached trend artifact failed its source or self-hash contract."""


class ReplayTrendBusyError(RuntimeError):
    """Another process is materializing the same trend artifact."""


@dataclass(frozen=True, slots=True)
class TrendSelector:
    role: str
    weighting: str
    metric: str

    def __post_init__(self) -> None:
        if self.role not in TREND_ROLES:
            raise ValueError("unsupported trend role")
        if self.weighting not in TREND_WEIGHTINGS:
            raise ValueError("unsupported trend weighting")
        if self.metric not in TREND_METRICS:
            raise ValueError("unsupported trend metric")

    def to_dict(self) -> dict[str, str]:
        return {
            "role": self.role,
            "weighting": self.weighting,
            "metric": self.metric,
        }


@dataclass(frozen=True, slots=True)
class TrendContext:
    data_root: Path
    session_date: date
    open_at: datetime
    close_at: datetime
    close_grace_elapsed_at: datetime
    close_grace_policy: str
    close_grace_seconds: int
    frame_minutes: int
    lookback_seconds: float
    timeline_policy_version: str
    projection_policy: Mapping[str, object]
    projection_policy_sha256: str
    source_paths: tuple[Path, ...]
    source_fingerprint: str
    frames: tuple[datetime, ...]

    @property
    def timeline_sha256(self) -> str:
        return _canonical_sha256([replay_id(value) for value in self.frames])


FrameLoader = Callable[[datetime], dict[str, object]]
FingerprintLoader = Callable[[], str]


def trend_cache_path(
    context: TrendContext,
    selector: TrendSelector,
) -> Path:
    """Return the immutable policy, source, timeline, and selector namespace."""

    lookback = format(context.lookback_seconds, ".15g").replace(".", "p")
    return (
        context.data_root
        / "published"
        / "spxw-surface"
        / "trend-cache"
        / "policy=v1"
        / f"frame={context.frame_minutes}m"
        / f"lookback={lookback}s"
        / f"projection={context.projection_policy_sha256}"
        / f"source={context.source_fingerprint}"
        / f"timeline={context.timeline_sha256}"
        / f"role={selector.role}"
        / f"weighting={selector.weighting}"
        / f"metric={selector.metric}"
        / f"{context.session_date.isoformat()}.json"
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("trend clocks must be timezone-aware")
    return as_utc(value)


def _epoch_ms(value: datetime) -> int:
    return int(_aware_utc(value).timestamp() * 1000)


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_clock(value: object, *, code: str) -> datetime:
    if not isinstance(value, str):
        raise ReplaySourceError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReplaySourceError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplaySourceError(code)
    return as_utc(parsed)


def _source_files(context: TrendContext) -> tuple[str, ...]:
    values: list[str] = []
    for path in context.source_paths:
        try:
            values.append(str(path.relative_to(context.data_root)))
        except ValueError as exc:
            raise ReplaySourceError("trend_source_path_outside_data_root") from exc
    if not values or len(values) != len(set(values)):
        raise ReplaySourceError("trend_source_files_invalid")
    return tuple(values)


def _source_hashes(context: TrendContext) -> dict[str, str]:
    relative = _source_files(context)
    try:
        return {
            name: _sha256(path)
            for name, path in zip(relative, context.source_paths, strict=True)
        }
    except OSError as exc:
        raise ReplaySourceError("trend_source_hash_unavailable") from exc


def _assert_source_fingerprint(
    context: TrendContext,
    current_source_fingerprint: FingerprintLoader,
) -> None:
    try:
        current = current_source_fingerprint()
    except OSError as exc:
        raise ReplaySourceError("trend_source_fingerprint_unavailable") from exc
    if not hmac.compare_digest(current, context.source_fingerprint):
        raise ReplaySourceError("trend_source_context_changed")


def _load_spx_series(context: TrendContext) -> dict[str, object]:
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
                quality,
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
              AND source_at <= ?::TIMESTAMPTZ
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
        ),
        ranked AS (
            SELECT
                *,
                count(*) OVER (PARTITION BY source_at) AS duplicate_count,
                row_number() OVER (
                    PARTITION BY source_at
                    ORDER BY
                        known_at DESC,
                        received_at DESC,
                        source_file DESC,
                        source_row DESC
                ) AS source_rank
            FROM eligible
        )
        SELECT
            epoch_ms(source_at)::BIGINT AS source_at_ms,
            epoch_ms(known_at)::BIGINT AS known_at_ms,
            mark,
            quality,
            duplicate_count
        FROM ranked
        WHERE source_rank = 1
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
                _aware_utc(context.open_at),
                _aware_utc(context.close_at),
                _aware_utc(context.close_at),
            ],
        ).fetchall()
    finally:
        connection.close()
    if len(rows) < 2:
        raise ReplaySourceError("trend_spx_series_unavailable")

    base_source_at_ms = int(rows[0][0])
    source_offsets: list[int] = []
    known_offsets: list[int] = []
    prices: list[float] = []
    raw_row_count = 0
    duplicate_groups = 0
    previous_source_ms: int | None = None
    previous_known_ms: int | None = None
    for raw_source_ms, raw_known_ms, raw_price, _quality, raw_duplicate_count in rows:
        source_ms = int(raw_source_ms)
        known_ms = int(raw_known_ms)
        price = _finite(raw_price)
        duplicate_count = int(raw_duplicate_count)
        if (
            price is None
            or known_ms < source_ms
            or (previous_source_ms is not None and source_ms <= previous_source_ms)
            or (previous_known_ms is not None and known_ms < previous_known_ms)
        ):
            raise ReplaySourceError("trend_spx_series_contract_invalid")
        source_offsets.append(source_ms - base_source_at_ms)
        known_offsets.append(known_ms - base_source_at_ms)
        prices.append(price)
        raw_row_count += duplicate_count
        duplicate_groups += int(duplicate_count > 1)
        previous_source_ms = source_ms
        previous_known_ms = known_ms
    return {
        "point_count": len(rows),
        "raw_row_count": raw_row_count,
        "duplicate_source_at_group_count": duplicate_groups,
        "base_source_at_ms": base_source_at_ms,
        "source_offset_ms": source_offsets,
        "known_at_offset_ms": known_offsets,
        "price": prices,
        "price_field": "mark",
        "market_clock": "source_at",
        "source_at_resolution": "milliseconds",
        "known_at_rule": "max_recorded_clocks",
        "known_at_is_availability_clock": False,
        "dedupe_rule": (
            "latest_known_at_then_received_at_then_source_file_position_per_source_at"
        ),
    }


def _mapping(value: object, *, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReplaySourceError(code)
    return value


def _list(value: object, *, code: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReplaySourceError(code)
    return value


def _quality(
    *,
    values: list[float | None],
    statuses: tuple[str, ...],
) -> str:
    if not any(value is not None for value in values):
        return "unavailable"
    if any(value in {"unavailable", "invalid"} for value in statuses):
        return "unavailable"
    if any(value is None for value in values):
        return "degraded"
    if any(value not in {"ok", "ready"} for value in statuses):
        return "degraded"
    return "ready"


def _warning_values(*values: object) -> list[str]:
    warnings: set[str] = set()
    for value in values:
        if isinstance(value, list):
            warnings.update(str(item) for item in value if str(item))
    return sorted(warnings)


def _extract_keyframe(
    *,
    context: TrendContext,
    selector: TrendSelector,
    requested: datetime,
    frame: Mapping[str, Any],
    shared_offsets: tuple[float, ...] | None,
    metric_unit: str | None,
) -> tuple[dict[str, object], tuple[float, ...], str, datetime]:
    requested = as_utc(requested)
    expected = {
        "kind": "spxw_surface_dashboard_replay",
        "mode": "replay",
        "policy_version": FRAME_POLICY_VERSION,
        "session_date": context.session_date.isoformat(),
        "requested_as_of": requested.isoformat(),
        "projection_policy_sha256": context.projection_policy_sha256,
        "frozen": True,
        "automatic_ordering": False,
    }
    if any(frame.get(key) != value for key, value in expected.items()):
        raise ReplaySourceError("trend_frame_contract_invalid")
    frame_hash = frame.get("artifact_sha256")
    if not isinstance(frame_hash, str) or not _SHA256_RE.fullmatch(frame_hash):
        raise ReplaySourceError("trend_frame_hash_invalid")
    source = _mapping(frame.get("source"), code="trend_frame_source_invalid")
    if (
        source.get("lookahead_rows_selected") != 0
        or source.get("availability_clock_available") is not False
        or source.get("point_in_time_confidence") != "bounded_not_proven"
    ):
        raise ReplaySourceError("trend_frame_point_in_time_contract_invalid")

    expiries = _list(frame.get("expiries"), code="trend_frame_expiries_invalid")
    selected = [
        value
        for value in expiries
        if isinstance(value, Mapping) and value.get("role") == selector.role
    ]
    if len(selected) != 1:
        raise ReplaySourceError("trend_frame_role_invalid")
    expiry = selected[0]
    expiry_text = expiry.get("expiry")
    if not isinstance(expiry_text, str) or not re.fullmatch(r"\d{8}", expiry_text):
        raise ReplaySourceError("trend_frame_expiry_invalid")
    expiry_close = _parse_clock(
        expiry.get("expiry_close"),
        code="trend_frame_expiry_close_invalid",
    )
    surface = _mapping(expiry.get("surface"), code="trend_frame_surface_invalid")
    reference_spot = _finite(surface.get("reference_spot"))
    raw_spots = _list(surface.get("spot_grid"), code="trend_frame_spot_grid_invalid")
    spots = [_finite(value) for value in raw_spots]
    if reference_spot is None or not spots or any(value is None for value in spots):
        raise ReplaySourceError("trend_frame_spot_grid_invalid")
    resolved_spots = [float(value) for value in spots if value is not None]
    offsets = tuple(round(value - reference_spot, 9) for value in resolved_spots)
    if any(right <= left for left, right in zip(offsets, offsets[1:])):
        raise ReplaySourceError("trend_frame_spot_grid_invalid")
    if shared_offsets is not None and offsets != shared_offsets:
        raise ReplaySourceError("trend_frame_relative_spot_grid_changed")

    slices = _list(surface.get("time_slices"), code="trend_frame_slices_invalid")
    current_slices = [
        value
        for value in slices
        if isinstance(value, Mapping) and _finite(value.get("minutes_forward")) == 0.0
    ]
    if len(current_slices) != 1:
        raise ReplaySourceError("trend_frame_current_slice_invalid")
    current_slice = current_slices[0]
    weightings = _mapping(
        current_slice.get("weightings"),
        code="trend_frame_weightings_invalid",
    )
    weighting = _mapping(
        weightings.get(selector.weighting),
        code="trend_frame_weighting_invalid",
    )
    metrics = _mapping(weighting.get("metrics"), code="trend_frame_metrics_invalid")
    raw_values = _list(
        metrics.get(selector.metric),
        code="trend_frame_metric_values_invalid",
    )
    values: list[float | None] = []
    for value in raw_values:
        if value is None:
            values.append(None)
            continue
        parsed = _finite(value)
        if parsed is None:
            raise ReplaySourceError("trend_frame_metric_values_invalid")
        values.append(parsed)
    if len(values) != len(offsets):
        raise ReplaySourceError("trend_frame_metric_values_invalid")

    metric_units = _mapping(
        surface.get("metric_units"),
        code="trend_frame_metric_units_invalid",
    )
    current_unit = metric_units.get(selector.metric)
    if not isinstance(current_unit, str) or not current_unit:
        raise ReplaySourceError("trend_frame_metric_units_invalid")
    if metric_unit is not None and current_unit != metric_unit:
        raise ReplaySourceError("trend_frame_metric_unit_changed")

    zero_ridge = (
        weighting.get("zero_ridge_spot")
        if selector.metric == "signed_gamma"
        else None
    )
    if zero_ridge is not None:
        zero_ridge = _finite(zero_ridge)
        if zero_ridge is None:
            raise ReplaySourceError("trend_frame_zero_ridge_invalid")
    statuses = (
        str(expiry.get("quality") or "unavailable"),
        str(current_slice.get("quality") or "unavailable"),
        str(weighting.get("quality") or "unavailable"),
    )
    quality = _quality(values=values, statuses=statuses)
    warnings = _warning_values(
        expiry.get("warnings"),
        surface.get("warnings"),
        current_slice.get("warnings"),
        weighting.get("warnings"),
    )
    session_open_ms = _epoch_ms(context.open_at)
    keyframe = {
        "at": requested.isoformat(),
        "at_offset_ms": _epoch_ms(requested) - session_open_ms,
        "valid_until": requested.isoformat(),
        "valid_until_offset_ms": _epoch_ms(requested) - session_open_ms,
        "expiry": expiry_text,
        "reference_spot": reference_spot,
        "values": values,
        "zero_ridge_spot": zero_ridge,
        "quality": quality,
        "warnings": warnings,
        "frame_artifact_sha256": frame_hash,
    }
    return keyframe, offsets, current_unit, expiry_close


def _append_gap(
    gaps: list[dict[str, object]],
    *,
    context: TrendContext,
    start: datetime,
    end: datetime,
    reason: str,
) -> None:
    start = as_utc(start)
    end = as_utc(end)
    if end <= start:
        return
    open_ms = _epoch_ms(context.open_at)
    gaps.append(
        {
            "start_at": start.isoformat(),
            "end_at": end.isoformat(),
            "start_offset_ms": _epoch_ms(start) - open_ms,
            "end_offset_ms": _epoch_ms(end) - open_ms,
            "reason": reason,
        }
    )


def _load_keyframes(
    context: TrendContext,
    selector: TrendSelector,
    frame_loader: FrameLoader,
) -> dict[str, object]:
    if not context.frames:
        raise ReplaySourceError("trend_timeline_empty")
    frames = tuple(as_utc(value) for value in context.frames)
    if tuple(sorted(set(frames))) != frames:
        raise ReplaySourceError("trend_timeline_invalid")
    keyframes: list[dict[str, object]] = []
    expiry_closes: list[datetime] = []
    shared_offsets: tuple[float, ...] | None = None
    metric_unit: str | None = None
    for requested in frames:
        frame = frame_loader(requested)
        keyframe, offsets, current_unit, expiry_close = _extract_keyframe(
            context=context,
            selector=selector,
            requested=requested,
            frame=frame,
            shared_offsets=shared_offsets,
            metric_unit=metric_unit,
        )
        shared_offsets = offsets
        metric_unit = current_unit
        keyframes.append(keyframe)
        expiry_closes.append(expiry_close)
    if shared_offsets is None or metric_unit is None:
        raise ReplaySourceError("trend_surface_profiles_unavailable")

    gaps: list[dict[str, object]] = []
    session_open = as_utc(context.open_at)
    session_close = as_utc(context.close_at)
    first_at = frames[0]
    _append_gap(
        gaps,
        context=context,
        start=session_open,
        end=first_at,
        reason="surface_keyframe_unavailable_before_first",
    )
    for index, (requested, keyframe, expiry_close) in enumerate(
        zip(frames, keyframes, expiry_closes, strict=True)
    ):
        next_at = frames[index + 1] if index + 1 < len(frames) else session_close
        maximum = min(
            requested + timedelta(minutes=context.frame_minutes),
            next_at,
            expiry_close,
            session_close,
        )
        valid_until = requested if keyframe["quality"] == "unavailable" else maximum
        keyframe["valid_until"] = valid_until.isoformat()
        keyframe["valid_until_offset_ms"] = _epoch_ms(valid_until) - _epoch_ms(session_open)
        _append_gap(
            gaps,
            context=context,
            start=valid_until,
            end=next_at,
            reason=(
                "surface_keyframe_unavailable"
                if keyframe["quality"] == "unavailable"
                else "surface_keyframe_validity_elapsed"
            ),
        )
    return {
        "cadence": "catalog_timeline_keyframes",
        "frame_count": len(keyframes),
        "shared_relative_spot_offsets": list(shared_offsets),
        "metric_unit": metric_unit,
        "validity_rule": (
            "min(next_keyframe_at, at_plus_frame_interval, expiry_close, session_close); "
            "unavailable_at_at"
        ),
        "interpolation": "none",
        "higher_frequency_candidate_upgrade": False,
        "keyframes": keyframes,
        "gaps": gaps,
    }


def build_trend_artifact(
    *,
    context: TrendContext,
    selector: TrendSelector,
    frame_loader: FrameLoader,
    current_source_fingerprint: FingerprintLoader,
) -> dict[str, object]:
    """Build one deterministic trend strip from verified catalog keyframes."""

    _assert_source_fingerprint(context, current_source_fingerprint)
    source_hashes_before = _source_hashes(context)
    spx = _load_spx_series(context)
    surface = _load_keyframes(context, selector, frame_loader)
    source_hashes_after = _source_hashes(context)
    if source_hashes_before != source_hashes_after:
        raise ReplaySourceError("trend_source_files_changed_during_build")
    _assert_source_fingerprint(context, current_source_fingerprint)
    projection_policy = dict(context.projection_policy)
    if _canonical_sha256(projection_policy) != context.projection_policy_sha256:
        raise ReplaySourceError("trend_projection_policy_hash_invalid")
    source_files = list(_source_files(context))
    payload: dict[str, object] = {
        "schema_version": TREND_SCHEMA_VERSION,
        "kind": TREND_KIND,
        "mode": TREND_MODE,
        "policy_version": TREND_POLICY_VERSION,
        "frame_policy_version": FRAME_POLICY_VERSION,
        "timeline_policy_version": context.timeline_policy_version,
        "session_date": context.session_date.isoformat(),
        "provider": "schwab",
        "coordinate": "SPX",
        "trading_class": "SPXW",
        **selector.to_dict(),
        "projection_policy": projection_policy,
        "projection_policy_sha256": context.projection_policy_sha256,
        "source_fingerprint": context.source_fingerprint,
        "timeline_sha256": context.timeline_sha256,
        "open_at": as_utc(context.open_at).isoformat(),
        "close_at": as_utc(context.close_at).isoformat(),
        "frame_interval_minutes": context.frame_minutes,
        "lookback_seconds": context.lookback_seconds,
        "session_close_grace_elapsed": True,
        "session_close_grace_elapsed_at": as_utc(
            context.close_grace_elapsed_at
        ).isoformat(),
        "session_close_grace_policy": context.close_grace_policy,
        "session_close_grace_seconds": context.close_grace_seconds,
        "availability_proven": False,
        "availability_clock": "unavailable",
        "point_in_time_confidence": "bounded_not_proven",
        "data_finalization_proven": False,
        "source": {
            "dataset": QUOTE_LAKE_DATASET,
            "source_files": source_files,
            "parquet_file_sha256": source_hashes_before,
            "source_files_verified_unchanged_during_build": True,
            "source_fingerprint": context.source_fingerprint,
            "cutoff_fields": [
                "received_at",
                "source_at",
                "quote_time",
                "trade_time",
                "last_update_at",
            ],
            "availability_clock_available": False,
            "availability_clock": "unavailable",
            "point_in_time_confidence": "bounded_not_proven",
            "known_limitations": [
                "response_finished_at_unavailable",
                "received_at_is_cycle_started_at",
            ],
            "spx": spx,
        },
        "surface": surface,
    }
    payload["artifact_sha256"] = _canonical_sha256(payload)
    return payload


def _validate_parallel_spx(
    payload: Mapping[str, object],
    *,
    context: TrendContext,
) -> None:
    source = payload.get("source")
    if not isinstance(source, Mapping):
        raise ReplayTrendCacheError("trend_cache_source_contract_invalid")
    spx = source.get("spx")
    if not isinstance(spx, Mapping):
        raise ReplayTrendCacheError("trend_cache_spx_contract_invalid")
    if (
        spx.get("price_field") != "mark"
        or spx.get("market_clock") != "source_at"
        or spx.get("known_at_rule") != "max_recorded_clocks"
        or spx.get("known_at_is_availability_clock") is not False
    ):
        raise ReplayTrendCacheError("trend_cache_spx_contract_invalid")
    arrays = [spx.get(key) for key in ("source_offset_ms", "known_at_offset_ms", "price")]
    if not all(isinstance(value, list) for value in arrays):
        raise ReplayTrendCacheError("trend_cache_spx_contract_invalid")
    lengths = {len(value) for value in arrays if isinstance(value, list)}
    point_count = spx.get("point_count")
    base_ms = spx.get("base_source_at_ms")
    raw_row_count = spx.get("raw_row_count")
    duplicate_groups = spx.get("duplicate_source_at_group_count")
    if (
        isinstance(point_count, bool)
        or not isinstance(point_count, int)
        or point_count < 2
        or lengths != {point_count}
        or isinstance(base_ms, bool)
        or not isinstance(base_ms, int)
        or isinstance(raw_row_count, bool)
        or not isinstance(raw_row_count, int)
        or raw_row_count < point_count
        or isinstance(duplicate_groups, bool)
        or not isinstance(duplicate_groups, int)
        or not 0 <= duplicate_groups <= point_count
    ):
        raise ReplayTrendCacheError("trend_cache_spx_contract_invalid")
    source_offsets, known_offsets, prices = arrays
    open_ms = _epoch_ms(context.open_at)
    close_ms = _epoch_ms(context.close_at)
    previous_source: int | None = None
    previous_known: int | None = None
    for source_offset, known_offset, price in zip(
        source_offsets,
        known_offsets,
        prices,
        strict=True,
    ):
        resolved_price = _finite(price)
        if (
            isinstance(source_offset, bool)
            or not isinstance(source_offset, int)
            or isinstance(known_offset, bool)
            or not isinstance(known_offset, int)
            or source_offset < 0
            or known_offset < source_offset
            or base_ms + source_offset < open_ms
            or base_ms + source_offset > close_ms
            or base_ms + known_offset > close_ms
            or resolved_price is None
            or resolved_price <= 0
            or (previous_source is not None and source_offset <= previous_source)
            or (previous_known is not None and known_offset < previous_known)
        ):
            raise ReplayTrendCacheError("trend_cache_spx_contract_invalid")
        previous_source = source_offset
        previous_known = known_offset


def load_cached_trend(
    *,
    path: Path,
    context: TrendContext,
    selector: TrendSelector,
    current_source_fingerprint: FingerprintLoader,
) -> dict[str, object]:
    """Read and revalidate a cached artifact before ETag comparison."""

    _assert_source_fingerprint(context, current_source_fingerprint)
    try:
        stat = path.stat()
        if stat.st_size <= 0 or stat.st_size > MAX_TREND_CACHE_ARTIFACT_BYTES:
            raise ReplayTrendCacheError("trend_cache_size_invalid")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except ReplayTrendCacheError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayTrendCacheError("trend_cache_unreadable") from exc
    if not isinstance(payload, dict):
        raise ReplayTrendCacheError("trend_cache_contract_invalid")
    expected = {
        "schema_version": TREND_SCHEMA_VERSION,
        "kind": TREND_KIND,
        "mode": TREND_MODE,
        "policy_version": TREND_POLICY_VERSION,
        "frame_policy_version": FRAME_POLICY_VERSION,
        "timeline_policy_version": context.timeline_policy_version,
        "session_date": context.session_date.isoformat(),
        "provider": "schwab",
        "coordinate": "SPX",
        "trading_class": "SPXW",
        **selector.to_dict(),
        "projection_policy_sha256": context.projection_policy_sha256,
        "source_fingerprint": context.source_fingerprint,
        "timeline_sha256": context.timeline_sha256,
        "open_at": as_utc(context.open_at).isoformat(),
        "close_at": as_utc(context.close_at).isoformat(),
        "frame_interval_minutes": context.frame_minutes,
        "lookback_seconds": context.lookback_seconds,
        "session_close_grace_elapsed": True,
        "session_close_grace_elapsed_at": as_utc(
            context.close_grace_elapsed_at
        ).isoformat(),
        "session_close_grace_policy": context.close_grace_policy,
        "session_close_grace_seconds": context.close_grace_seconds,
        "availability_proven": False,
        "availability_clock": "unavailable",
        "point_in_time_confidence": "bounded_not_proven",
        "data_finalization_proven": False,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ReplayTrendCacheError("trend_cache_contract_invalid")
    projection_policy = payload.get("projection_policy")
    if not isinstance(projection_policy, Mapping) or (
        _canonical_sha256(dict(projection_policy)) != context.projection_policy_sha256
        or dict(projection_policy) != dict(context.projection_policy)
    ):
        raise ReplayTrendCacheError("trend_cache_projection_policy_invalid")
    stored_hash = payload.get("artifact_sha256")
    if not isinstance(stored_hash, str) or not _SHA256_RE.fullmatch(stored_hash):
        raise ReplayTrendCacheError("trend_cache_hash_invalid")
    unsigned = dict(payload)
    unsigned.pop("artifact_sha256", None)
    if not hmac.compare_digest(stored_hash, _canonical_sha256(unsigned)):
        raise ReplayTrendCacheError("trend_cache_hash_mismatch")
    source = payload.get("source")
    if not isinstance(source, Mapping):
        raise ReplayTrendCacheError("trend_cache_source_contract_invalid")
    relative_files = list(_source_files(context))
    if (
        source.get("source_files") != relative_files
        or source.get("source_fingerprint") != context.source_fingerprint
        or source.get("source_files_verified_unchanged_during_build") is not True
    ):
        raise ReplayTrendCacheError("trend_cache_source_contract_invalid")
    expected_hashes = source.get("parquet_file_sha256")
    if not isinstance(expected_hashes, Mapping) or set(expected_hashes) != set(relative_files):
        raise ReplayTrendCacheError("trend_cache_source_contract_invalid")
    current_hashes = _source_hashes(context)
    for name in relative_files:
        value = expected_hashes.get(name)
        if (
            not isinstance(value, str)
            or not _SHA256_RE.fullmatch(value)
            or not hmac.compare_digest(value, current_hashes[name])
        ):
            raise ReplayTrendCacheError("trend_cache_source_hash_mismatch")
    _validate_parallel_spx(payload, context=context)
    surface = payload.get("surface")
    if not isinstance(surface, Mapping):
        raise ReplayTrendCacheError("trend_cache_surface_contract_invalid")
    keyframes = surface.get("keyframes")
    offsets = surface.get("shared_relative_spot_offsets")
    if (
        not isinstance(keyframes, list)
        or len(keyframes) != len(context.frames)
        or surface.get("frame_count") != len(context.frames)
        or not isinstance(offsets, list)
        or not offsets
    ):
        raise ReplayTrendCacheError("trend_cache_surface_contract_invalid")
    for requested, keyframe in zip(context.frames, keyframes, strict=True):
        if not isinstance(keyframe, Mapping):
            raise ReplayTrendCacheError("trend_cache_surface_contract_invalid")
        if keyframe.get("at") != as_utc(requested).isoformat():
            raise ReplayTrendCacheError("trend_cache_timeline_mismatch")
        values = keyframe.get("values")
        frame_hash = keyframe.get("frame_artifact_sha256")
        if (
            not isinstance(values, list)
            or len(values) != len(offsets)
            or not isinstance(frame_hash, str)
            or not _SHA256_RE.fullmatch(frame_hash)
        ):
            raise ReplayTrendCacheError("trend_cache_surface_contract_invalid")
        try:
            valid_until = _parse_clock(
                keyframe.get("valid_until"),
                code="trend_cache_valid_until_invalid",
            )
        except ReplaySourceError as exc:
            raise ReplayTrendCacheError("trend_cache_valid_until_invalid") from exc
        maximum = as_utc(requested) + timedelta(minutes=context.frame_minutes)
        if valid_until < as_utc(requested) or valid_until > maximum:
            raise ReplayTrendCacheError("trend_cache_valid_until_invalid")
    _assert_source_fingerprint(context, current_source_fingerprint)
    return payload


def materialize_trend(
    *,
    context: TrendContext,
    selector: TrendSelector,
    frame_loader: FrameLoader,
    current_source_fingerprint: FingerprintLoader,
) -> dict[str, object]:
    """Read or atomically publish one immutable trend cache artifact."""

    destination = trend_cache_path(context, selector)
    if destination.is_file():
        return load_cached_trend(
            path=destination,
            context=context,
            selector=selector,
            current_source_fingerprint=current_source_fingerprint,
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with exclusive_state_lock(destination, timeout_seconds=0.0):
            if destination.is_file():
                return load_cached_trend(
                    path=destination,
                    context=context,
                    selector=selector,
                    current_source_fingerprint=current_source_fingerprint,
                )
            payload = build_trend_artifact(
                context=context,
                selector=selector,
                frame_loader=frame_loader,
                current_source_fingerprint=current_source_fingerprint,
            )
            atomic_write_json_secure(destination, payload)
            return load_cached_trend(
                path=destination,
                context=context,
                selector=selector,
                current_source_fingerprint=current_source_fingerprint,
            )
    except TimeoutError as exc:
        raise ReplayTrendBusyError("trend_generation_locked") from exc
