"""Causal replay frame parsing and IBKR GTH SPXW quote loading."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any

import duckdb

from spx_spark.features.exposure_surface import (
    SurfaceContract,
    SurfaceGridConfig,
    build_exposure_surface,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import (
    REPLAY_POLICY_VERSION,
    ReplaySourceError,
    replay_id,
)
from spx_spark.surface_replay_session_models import (
    SESSION_SURFACE_GTH_QUOTE_MAX_AGE_SECONDS,
    SESSION_SURFACE_POLICY_VERSION,
    FrameLoader,
    SessionSurfaceBuildCache,
    _clock,
    _finite,
    _FrameState,
    _iso,
    _list,
    _mapping,
    _nonnegative,
    _SHA256_RE,
    _SPXObservation,
    session_surface_window,
)
from spx_spark.surface_replay_trend import TrendContext


ConnectFactory = Callable[[], Any]
ContractsParser = Callable[..., tuple[tuple[SurfaceContract, ...], tuple[Mapping[str, Any], ...]]]
FrameHashBuilder = Callable[..., str]
ReferenceSelector = Callable[[tuple[_SPXObservation, ...], datetime], _SPXObservation | None]
SurfaceBuilder = Callable[..., Any]


def contracts_and_strikes(
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


def parse_role_frame(
    context: TrendContext,
    *,
    requested: datetime,
    frame: Mapping[str, Any],
    role: str,
    contracts_parser: ContractsParser = contracts_and_strikes,
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
        value for value in expiries if isinstance(value, Mapping) and value.get("role") == role
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
    contracts, strike_rows = contracts_parser(surface, expiry=expiry)
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


def gth_reference_at(
    observations: tuple[_SPXObservation, ...],
    at: datetime,
) -> _SPXObservation | None:
    cutoff = as_utc(at)
    candidates = [
        row
        for row in observations
        if row.method == "es_basis_inferred_spx"
        and row.source_at <= cutoff
        and row.known_at <= cutoff
        and row.valid_until is not None
        and cutoff < as_utc(row.valid_until)
    ]
    return candidates[-1] if candidates else None


def gth_frame_hash(
    *,
    expiry: str,
    at: datetime,
    reference: _SPXObservation,
    rows: list[tuple[object, ...]],
) -> str:
    encoded = json.dumps(
        {
            "policy_version": SESSION_SURFACE_POLICY_VERSION,
            "expiry": expiry,
            "at": _iso(at),
            "reference_source_at": _iso(reference.source_at),
            "reference_known_at": _iso(reference.known_at),
            "basis": dict(reference.basis or {}),
            "rows": rows,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_gth_frames(
    context: TrendContext,
    *,
    as_of: datetime,
    role: str,
    reference_observations: tuple[_SPXObservation, ...],
    reference_selector: ReferenceSelector = gth_reference_at,
    hash_builder: FrameHashBuilder = gth_frame_hash,
    connect_factory: ConnectFactory = duckdb.connect,
    surface_builder: SurfaceBuilder = build_exposure_surface,
) -> tuple[_FrameState, ...]:
    """Build causal IBKR SPXW frames on the fixed GTH bucket clock."""

    window = session_surface_window(context.session_date)
    cutoff = min(as_utc(as_of), as_utc(window.gth_end))
    if cutoff <= as_utc(window.session_start):
        return ()
    research_expiries = DEFAULT_MARKET_CALENDAR.research_expiries(window.session_start)
    expiry_index = 0 if role == "front" else 1
    if len(research_expiries) <= expiry_index:
        return ()
    expiry_date = research_expiries[expiry_index]
    expiry = expiry_date.strftime("%Y%m%d")
    interval_text = f"{context.frame_minutes} minutes"
    query = """
        WITH source AS MATERIALIZED (
            SELECT
                instrument_id,
                source_at,
                received_at,
                GREATEST(
                    received_at,
                    COALESCE(source_at, received_at),
                    COALESCE(quote_time, received_at),
                    COALESCE(trade_time, received_at),
                    COALESCE(last_update_at, received_at)
                ) AS known_at,
                strike,
                "right" AS option_right,
                implied_vol,
                open_interest,
                volume,
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
            WHERE provider = 'ibkr'
              AND trading_class = 'SPXW'
              AND expiry = ?::DATE
              AND source_at >= ?::TIMESTAMPTZ
              AND source_at < ?::TIMESTAMPTZ
              AND received_at IS NOT NULL
        ),
        eligible AS (
            SELECT *
            FROM source
            WHERE known_at <= ?::TIMESTAMPTZ
        ),
        historical_ranked AS (
            SELECT
                TIME_BUCKET(
                    CAST(? AS INTERVAL),
                    known_at,
                    ?::TIMESTAMPTZ
                ) + CAST(? AS INTERVAL) AS frame_end,
                *,
                row_number() OVER (
                    PARTITION BY
                        TIME_BUCKET(
                            CAST(? AS INTERVAL),
                            known_at,
                            ?::TIMESTAMPTZ
                        ),
                        instrument_id
                    ORDER BY known_at DESC, received_at DESC, source_file DESC, source_row DESC
                ) AS rank
            FROM eligible
        ),
        historical AS (
            SELECT * EXCLUDE (rank)
            FROM historical_ranked
            WHERE rank = 1
              AND frame_end <= ?::TIMESTAMPTZ
              AND frame_end <= ?::TIMESTAMPTZ
        ),
        current_ranked AS (
            SELECT
                ?::TIMESTAMPTZ AS frame_end,
                *,
                row_number() OVER (
                    PARTITION BY instrument_id
                    ORDER BY known_at DESC, received_at DESC, source_file DESC, source_row DESC
                ) AS rank
            FROM eligible
        ),
        current_frame AS (
            SELECT * EXCLUDE (rank)
            FROM current_ranked
            WHERE rank = 1
              AND ?::TIMESTAMPTZ < ?::TIMESTAMPTZ
        )
        SELECT * FROM historical
        UNION ALL BY NAME
        SELECT * FROM current_frame
        ORDER BY frame_end, strike, option_right, instrument_id
    """
    paths = [str(path) for path in context.source_paths]
    connection = connect_factory()
    try:
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET threads=1")
        rows = connection.execute(
            query,
            [
                paths,
                expiry_date,
                as_utc(window.session_start),
                as_utc(window.gth_end),
                cutoff,
                interval_text,
                as_utc(window.session_start),
                interval_text,
                interval_text,
                as_utc(window.session_start),
                cutoff,
                as_utc(window.gth_end),
                cutoff,
                cutoff,
                as_utc(window.gth_end),
            ],
        ).fetchall()
    finally:
        connection.close()

    grouped: dict[datetime, list[tuple[object, ...]]] = {}
    for row in rows:
        raw_end = row[0]
        if isinstance(raw_end, datetime):
            grouped.setdefault(as_utc(raw_end), []).append(tuple(row[1:]))

    expiry_close = as_utc(window.session_end)
    built: list[_FrameState] = []
    for frame_end, frame_rows in sorted(grouped.items()):
        deduped_rows: dict[str, tuple[object, ...]] = {}
        for raw in frame_rows:
            instrument_id = str(raw[0])
            previous = deduped_rows.get(instrument_id)
            raw_known = raw[3]
            previous_known = previous[3] if previous is not None else None
            if previous is None or (
                isinstance(raw_known, datetime)
                and (
                    not isinstance(previous_known, datetime)
                    or as_utc(raw_known) > as_utc(previous_known)
                )
            ):
                deduped_rows[instrument_id] = raw
        frame_rows = list(deduped_rows.values())
        reference = reference_selector(reference_observations, frame_end)
        if reference is None:
            continue
        contracts: list[SurfaceContract] = []
        hash_rows: list[tuple[object, ...]] = []
        source_valid_until: list[datetime] = (
            [as_utc(reference.valid_until)] if reference.valid_until else []
        )
        latest_known = reference.known_at
        for raw in frame_rows:
            (
                _instrument_id,
                raw_source,
                _raw_received,
                raw_known,
                raw_strike,
                raw_right,
                raw_iv,
                raw_oi,
                raw_volume,
                raw_quality,
                raw_error,
                raw_source_file,
                raw_source_row,
            ) = raw
            if not isinstance(raw_source, datetime) or not isinstance(raw_known, datetime):
                continue
            source_at = as_utc(raw_source)
            known_at = as_utc(raw_known)
            valid_until = source_at + timedelta(seconds=SESSION_SURFACE_GTH_QUOTE_MAX_AGE_SECONDS)
            if (
                raw_quality != "live"
                or raw_error is not None
                or known_at > frame_end
                or not frame_end < valid_until
            ):
                continue
            strike = _finite(raw_strike)
            iv = _finite(raw_iv)
            right = str(raw_right or "").upper()
            if strike is None or strike <= 0 or iv is None or iv <= 0 or right not in {"C", "P"}:
                continue
            contract = SurfaceContract(
                expiry=expiry,
                strike=strike,
                right=right,
                iv=iv,
                open_interest=_nonnegative(raw_oi),
                volume=_nonnegative(raw_volume),
            )
            if contract.open_interest is None and contract.volume is None:
                continue
            contracts.append(contract)
            latest_known = max(latest_known, known_at)
            source_valid_until.append(valid_until)
            hash_rows.append(
                (
                    str(_instrument_id),
                    _iso(source_at),
                    _iso(known_at),
                    strike,
                    right,
                    iv,
                    contract.open_interest,
                    contract.volume,
                    str(raw_source_file),
                    int(raw_source_row),
                )
            )
        if not contracts or not source_valid_until:
            continue
        artifact_hash = hash_builder(
            expiry=expiry,
            at=frame_end,
            reference=reference,
            rows=hash_rows,
        )
        config = SurfaceGridConfig(
            spot_step_points=5.0,
            spot_steps_each_side=0,
            max_spot_points=1,
            max_time_points=1,
            max_cells=1,
            max_contract_cell_evaluations=max(3, len(contracts) * 3),
        )
        surface = surface_builder(
            tuple(contracts),
            spot=reference.price,
            as_of=frame_end,
            expiry_close=expiry_close,
            spot_points=(reference.price,),
            time_offsets_minutes=(0.0,),
            config=config,
        ).to_dict()
        strike_ladder = surface.get("strike_ladder")
        if not isinstance(strike_ladder, list) or not strike_ladder:
            continue
        quality = str(surface.get("quality") or "degraded")
        raw_warnings = surface.get("warnings")
        warnings = (
            tuple(str(value) for value in raw_warnings) if isinstance(raw_warnings, list) else ()
        )
        built.append(
            _FrameState(
                at=frame_end,
                known_at=latest_known,
                valid_until=min(source_valid_until),
                artifact_sha256=artifact_hash,
                expiry=expiry,
                expiry_close=expiry_close,
                reference_spot=reference.price,
                contracts=tuple(contracts),
                strike_rows=tuple(dict(row) for row in strike_ladder if isinstance(row, Mapping)),
                quality="ready" if quality == "ok" else quality,
                warnings=warnings,
                session_kind="gth",
                surface_provider="ibkr",
                reference_method="es_basis_inferred_spx",
            )
        )
    by_clock: dict[datetime, _FrameState] = {}
    for frame in built:
        by_clock[frame.at] = frame
    return tuple(by_clock[key] for key in sorted(by_clock))


def causal_frames(
    context: TrendContext,
    *,
    as_of: datetime,
    role: str,
    frame_loader: FrameLoader,
    build_cache: SessionSurfaceBuildCache,
    reference_observations: tuple[_SPXObservation, ...] = (),
    gth_loader: Callable[..., tuple[_FrameState, ...]] = load_gth_frames,
    frame_parser: Callable[..., _FrameState] = parse_role_frame,
) -> tuple[_FrameState, ...]:
    cutoff = as_utc(as_of)
    window = session_surface_window(context.session_date)
    gth_cutoff = min(cutoff, as_utc(window.gth_end))
    requested_frames = tuple(as_utc(value) for value in context.frames if as_utc(value) <= cutoff)
    cached_gth = build_cache.get_gth_frames(
        source_fingerprint=context.source_fingerprint,
        role=role,
        cutoff=gth_cutoff,
    )
    if cached_gth is not None:
        gth_frames = cached_gth or ()
    else:
        gth_frames = tuple(gth_loader(
            context,
            as_of=cutoff,
            role=role,
            reference_observations=reference_observations,
        ))
        if any(
            row.session_kind != "gth"
            or row.at < as_utc(window.session_start)
            or row.at > gth_cutoff
            or (row.known_at or row.at) > gth_cutoff
            for row in gth_frames
        ) or any(
            right.at <= left.at for left, right in zip(gth_frames, gth_frames[1:])
        ):
            raise ReplaySourceError("session_surface_gth_timeline_invalid")
        build_cache.put_gth_frames(
            source_fingerprint=context.source_fingerprint,
            role=role,
            covered_until=gth_cutoff,
            frames=gth_frames,
        )
    parsed: list[_FrameState] = list(gth_frames)
    for requested in requested_frames:
        cache_key = (context.source_fingerprint, role, replay_id(requested))
        row = build_cache.get_frame(cache_key)
        if row is None:
            try:
                frame = frame_loader(requested)
            except ReplaySourceError as exc:
                # The legacy replay artifact requires both expiries even though
                # this endpoint requests one role.  If either expiry is not
                # model-viable, preserve the fixed canvas and leave this source
                # interval Missing.  Integrity, clock, and cache failures still
                # propagate and fail the whole response.
                if str(exc) != "replay_front_next_projection_incomplete":
                    raise
                continue
            row = frame_parser(
                context,
                requested=requested,
                frame=frame,
                role=role,
            )
            build_cache.put_frame(cache_key, row)
        parsed.append(row)
    parsed.sort(key=lambda row: row.at)
    if any(right.at <= left.at for left, right in zip(parsed, parsed[1:])):
        raise ReplaySourceError("session_surface_timeline_invalid")
    expiry_values = {row.expiry for row in parsed}
    if len(expiry_values) > 1:
        raise ReplaySourceError("session_surface_expiry_changed")
    resolved: list[_FrameState] = []
    for index, row in enumerate(parsed):
        next_at = parsed[index + 1].at if index + 1 < len(parsed) else None
        source_valid_until = (
            row.valid_until if row.session_kind == "gth" else as_utc(context.close_at)
        )
        valid_until = min(
            row.at + timedelta(minutes=context.frame_minutes),
            source_valid_until,
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
                known_at=row.known_at,
                session_kind=row.session_kind,
                surface_provider=row.surface_provider,
                reference_method=row.reference_method,
            )
        )
    return tuple(resolved)


__all__ = (
    "causal_frames",
    "contracts_and_strikes",
    "gth_frame_hash",
    "gth_reference_at",
    "load_gth_frames",
    "parse_role_frame",
)
