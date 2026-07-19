from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

import spx_spark.surface_replay_session_data as session_data
from spx_spark.surface_dashboard_replay import (
    QUOTE_LAKE_DATASET,
    ReplaySourceError,
    _canonical_sha256,
)
from spx_spark.surface_replay_service import ReplayCacheError, ReplayCatalog
from spx_spark.surface_replay_session_frames import causal_frames
from spx_spark.surface_replay_session_models import (
    SessionSurfaceBuildCache,
    _FrameState,
    session_surface_window,
)
from test_surface_dashboard_replay import _row, storage_settings, write_quote_partition


SESSION_DATE = date(2026, 7, 17)
GTH_AT = datetime(2026, 7, 17, 0, 25, tzinfo=timezone.utc)


def _replace(
    row: tuple[object, ...],
    *,
    provider: str | None = None,
    provider_symbol: str | None = None,
    quality: str | None = None,
    error: str | None = None,
) -> tuple[object, ...]:
    values = list(row)
    if provider is not None:
        values[2] = provider
    if provider_symbol is not None:
        values[11] = provider_symbol
    if quality is not None:
        values[20] = quality
    values[44] = error
    return tuple(values)


def _write_partition(
    *,
    template: Path,
    destination: Path,
    rows: list[tuple[object, ...]],
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE quotes AS SELECT * FROM read_parquet(?, hive_partitioning=false) WHERE FALSE",
            [str(template)],
        )
        placeholders = ",".join("?" for _ in rows[0])
        connection.executemany(f"INSERT INTO quotes VALUES ({placeholders})", rows)
        connection.execute("COPY quotes TO ? (FORMAT PARQUET)", [str(destination)])
    finally:
        connection.close()


def _catalog_with_gth(
    tmp_path: Path,
    *,
    include_basis: bool = True,
    current_es_error: bool = False,
    include_future_rows: bool = True,
) -> ReplayCatalog:
    template = write_quote_partition(tmp_path)
    lake = tmp_path / "data" / QUOTE_LAKE_DATASET

    if include_basis:
        basis_rows: list[tuple[object, ...]] = []
        basis_start = datetime(2026, 7, 16, 19, 58, 20, tzinfo=timezone.utc)
        for index in range(7):
            spx_at = basis_start + timedelta(seconds=index * 10)
            es_at = spx_at + timedelta(seconds=1)
            basis_rows.append(
                _row(
                    instrument_id="index:SPX",
                    symbol="SPX",
                    instrument_type="index",
                    received_at=spx_at,
                    bid=7499.75 + index,
                    ask=7500.25 + index,
                    mark=7500.0 + index,
                )
            )
            basis_rows.append(
                _replace(
                    _row(
                        instrument_id="future:ES",
                        symbol="ES",
                        instrument_type="future",
                        received_at=es_at,
                        bid=7543.75 + index,
                        ask=7544.25 + index,
                        mark=7544.0 + index,
                    ),
                    provider_symbol="/ESU26",
                )
            )
        _write_partition(
            template=template,
            destination=(
                lake
                / "date=2026-07-16"
                / "provider=schwab"
                / "hour=19"
                / "quotes.parquet"
            ),
            rows=basis_rows,
        )

    reference_rows = [
        _replace(
            _row(
                instrument_id="future:ES",
                symbol="ES",
                instrument_type="future",
                received_at=GTH_AT - timedelta(seconds=10),
                bid=7539.75,
                ask=7540.25,
                mark=7540.0,
            ),
            provider_symbol="/ESU26",
        )
    ]
    if not current_es_error:
        reference_rows.append(
            _replace(
                _row(
                    instrument_id="future:ES",
                    symbol="ES",
                    instrument_type="future",
                    received_at=GTH_AT - timedelta(seconds=2),
                    bid=7540.75,
                    ask=7541.25,
                    mark=7541.0,
                ),
                provider_symbol="/ESU26",
            )
        )
    else:
        reference_rows.append(
            _replace(
                _row(
                    instrument_id="future:ES",
                    symbol="ES",
                    instrument_type="future",
                    received_at=GTH_AT - timedelta(seconds=1),
                    bid=7540.75,
                    ask=7541.25,
                    mark=7541.0,
                ),
                provider_symbol="/ESU26",
                quality="stale",
                error="10197:no_market_data_during_competing_session",
            )
        )
    if include_future_rows:
        reference_rows.append(
            _replace(
                _row(
                    instrument_id="future:ES",
                    symbol="ES",
                    instrument_type="future",
                    received_at=GTH_AT + timedelta(minutes=1),
                    bid=7999.75,
                    ask=8000.25,
                    mark=8000.0,
                ),
                provider_symbol="/ESU26",
            )
        )
        reference_rows.append(
            _replace(
                _row(
                    instrument_id="future:ES",
                    symbol="ES",
                    instrument_type="future",
                    received_at=datetime(
                        2026, 7, 17, 13, 24, 59, tzinfo=timezone.utc
                    ),
                    bid=7499.75,
                    ask=7500.25,
                    mark=7500.0,
                ),
                provider_symbol="/ESU26",
            )
        )
    _write_partition(
        template=template,
        destination=(
            lake
            / "date=2026-07-17"
            / "provider=schwab"
            / "hour=00"
            / "quotes.parquet"
        ),
        rows=reference_rows,
    )

    option_rows: list[tuple[object, ...]] = []
    for strike_index, strike in enumerate(range(7460, 7520, 10)):
        for right in ("C", "P"):
            option_rows.append(
                _replace(
                    _row(
                        instrument_id=(
                            f"option:SPX:SPXW:20260717:{strike}:{right}"
                        ),
                        symbol="SPX",
                        instrument_type="option",
                        received_at=GTH_AT - timedelta(seconds=10),
                        expiry="20260717",
                        strike=float(strike),
                        right=right,
                        bid=10.0 + strike_index,
                        ask=10.5 + strike_index,
                        mark=10.25 + strike_index,
                        implied_vol=0.18 + strike_index * 0.002,
                        open_interest=100.0 + strike_index * 10,
                        volume=20.0 + strike_index,
                    ),
                    provider="ibkr",
                )
            )
    if include_future_rows:
        option_rows.append(
            _replace(
                _row(
                    instrument_id="option:SPX:SPXW:20260717:9000:C",
                    symbol="SPX",
                    instrument_type="option",
                    received_at=GTH_AT + timedelta(minutes=1),
                    expiry="20260717",
                    strike=9000.0,
                    right="C",
                    bid=1.0,
                    ask=1.5,
                    mark=1.25,
                    implied_vol=0.9,
                    open_interest=9_000_000.0,
                    volume=9_000_000.0,
                ),
                provider="ibkr",
            )
        )
    _write_partition(
        template=template,
        destination=(
            lake
            / "date=2026-07-17"
            / "provider=ibkr"
            / "hour=00"
            / "quotes.parquet"
        ),
        rows=option_rows,
    )
    settings = storage_settings(tmp_path)
    return ReplayCatalog(data_root=settings.data_root, storage_settings=settings)


def test_v2_timeline_keeps_legacy_frames_and_adds_fixed_surface_playheads(
    tmp_path: Path,
) -> None:
    catalog = _catalog_with_gth(tmp_path)
    payload = catalog.timeline_payload(SESSION_DATE)

    assert payload["open_at"] == "2026-07-17T13:30:00+00:00"
    assert payload["surface_open_at"] == "2026-07-17T00:15:00+00:00"
    assert payload["surface_close_at"] == "2026-07-17T20:00:00+00:00"
    assert payload["surface_provider"] == "mixed"
    assert payload["surface_frame_count"] == 237
    assert len(payload["frames"]) > 0
    surface_frames = payload["surface_frames"]
    assert {row["session_kind"] for row in surface_frames} == {
        "gth",
        "closed_gap",
        "rth",
    }
    assert sum(row["session_kind"] == "gth" for row in surface_frames) == 158
    assert sum(row["session_kind"] == "closed_gap" for row in surface_frames) == 1
    assert sum(row["session_kind"] == "rth" for row in surface_frames) == 78
    assert all(
        row["status"] == "scheduled_missing"
        for row in surface_frames
        if row["session_kind"] == "closed_gap"
    )
    hash_body = [
        {
            "at": row["at"],
            "session_kind": row["session_kind"],
            "status": row["status"],
        }
        for row in surface_frames
    ]
    assert payload["surface_timeline_sha256"] == _canonical_sha256(hash_body)


def test_v2_gth_surface_uses_ibkr_chain_and_causal_schwab_es_basis(
    tmp_path: Path,
) -> None:
    catalog = _catalog_with_gth(tmp_path)
    payload = catalog.session_surface(
        SESSION_DATE,
        at=GTH_AT,
        role="front",
        weighting="oi_weighted",
        bucket_minutes=5,
        price_step=5.0,
    )

    assert payload["schema_version"] == 2
    assert payload["provider"] == "mixed"
    assert len(payload["time_buckets"]) == 237
    reference = payload["reference"]
    assert reference["method"] == "es_basis_inferred_spx"
    assert reference["provider"] == "schwab"
    assert reference["instrument_id"] == "future:ES"
    assert reference["accepted_at"] is None
    assert reference["basis"]["provider"] == "schwab"
    assert reference["basis"]["method"] == "frozen_previous_rth_median"
    assert reference["basis"]["es_contract"] == "/ESU26"
    assert reference["basis"]["contract_expiry"] is None
    assert reference["basis"]["known_at"] < reference["basis"]["frozen_at"]
    assert reference["source_at"] <= payload["as_of"]
    assert reference["known_at"] <= payload["as_of"]
    assert payload["capabilities"]["gth_data_available"] is True
    assert payload["capabilities"]["gth_complete_chain_available"] is False
    assert (
        "gth_contract_universe_completeness_unproven"
        in payload["provenance"]["known_limitations"]
    )

    historical = [
        row for row in payload["surface_columns"] if row["kind"] == "historical"
    ]
    assert historical
    assert all(row["surface_provider"] == "ibkr" for row in historical)
    assert all(row["source_session_kind"] == "gth" for row in historical)
    assert all(row["quality"] == "degraded" for row in historical)
    rth_projections = [
        row
        for row in payload["surface_columns"]
        if row["kind"] == "projection" and row["session_kind"] == "rth"
    ]
    assert rth_projections
    assert all(row["source_session_kind"] == "gth" for row in rth_projections)
    assert all(row["surface_provider"] == "ibkr" for row in rth_projections)
    assert all(row["quality"] == "degraded" for row in rth_projections)
    assert all(
        row["reference_method"] == "es_basis_inferred_spx"
        for row in rth_projections
    )
    gap_index = next(
        index
        for index, row in enumerate(payload["surface_columns"])
        if row["session_kind"] == "closed_gap"
    )
    assert payload["surface_columns"][gap_index]["kind"] == "missing"
    assert payload["surface_columns"][gap_index]["reason"] == "scheduled_closed_gap"
    assert all(value is None for value in payload["gamma_surface"][gap_index])
    assert payload["provenance"]["lookahead_rows_selected"] == 0
    assert payload["provenance"]["gth_surface_quote_max_age_seconds"] == 30.0
    assert payload["provenance"]["reference_max_age_seconds"] == 5.0
    strike_metadata = payload["strike_profile_metadata"]
    assert strike_metadata["baseline_at"] is None
    assert strike_metadata["baseline_session_kind"] is None
    assert strike_metadata["baseline_surface_provider"] is None
    assert strike_metadata["baseline_reference_method"] is None
    assert strike_metadata["baseline_unavailable_reason"] == (
        "gth_contract_universe_completeness_unproven"
    )
    assert strike_metadata["current_session_kind"] == "gth"
    assert strike_metadata["current_surface_provider"] == "ibkr"
    assert payload["capabilities"]["first_validated_baseline_available"] is False
    assert all(
        row["first_validated_proxy"] is None
        and row["first_validated_open_interest"] is None
        for row in payload["strike_profile"]
    )
    assert max(payload["price_grid"]) < 8000.0
    assert all(row.get("source_at") is None or row["source_at"] <= payload["as_of"] for row in payload["surface_columns"])


def test_v2_gth_cache_rejects_partial_chain_baseline_claim(tmp_path: Path) -> None:
    catalog = _catalog_with_gth(tmp_path)
    catalog.session_surface(
        SESSION_DATE,
        at=GTH_AT,
        role="front",
        weighting="oi_weighted",
        bucket_minutes=5,
        price_step=5.0,
    )
    cache_files = list(
        (
            catalog.data_root
            / "published"
            / "spxw-surface"
            / "session-surface-cache"
        ).rglob("*.json")
    )
    assert len(cache_files) == 1
    path = cache_files[0]
    payload = json.loads(path.read_text(encoding="utf-8"))
    metadata = payload["strike_profile_metadata"]
    metadata["baseline_at"] = metadata["current_at"]
    metadata["baseline_session_kind"] = "gth"
    metadata["baseline_surface_provider"] = "ibkr"
    metadata["baseline_reference_method"] = "es_basis_inferred_spx"
    metadata["baseline_unavailable_reason"] = None
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = _canonical_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="session_surface_cache_strike_invalid"):
        catalog.session_surface(
            SESSION_DATE,
            at=GTH_AT,
            role="front",
            weighting="oi_weighted",
            bucket_minutes=5,
            price_step=5.0,
        )


def test_v2_gth_missing_basis_fails_closed_without_chain_coordinate(
    tmp_path: Path,
) -> None:
    catalog = _catalog_with_gth(tmp_path, include_basis=False)

    with pytest.raises(ReplaySourceError, match="session_surface_causal_spx_unavailable"):
        catalog.session_surface(
            SESSION_DATE,
            at=GTH_AT,
            role="front",
            weighting="oi_weighted",
            bucket_minutes=5,
            price_step=5.0,
        )


def test_v2_gth_10197_or_expired_es_blocks_reference_and_projection(
    tmp_path: Path,
) -> None:
    catalog = _catalog_with_gth(tmp_path, current_es_error=True)
    payload = catalog.session_surface(
        SESSION_DATE,
        at=GTH_AT,
        role="front",
        weighting="oi_weighted",
        bucket_minutes=5,
        price_step=5.0,
    )

    assert payload["reference"] == {
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
        "missing_reason": "fresh_coordinate_reference_unavailable",
        "basis": None,
        "render_style": None,
    }
    assert payload["spot"] is None
    assert payload["capabilities"]["gth_data_available"] is False
    assert not any(row["kind"] == "projection" for row in payload["surface_columns"])
    assert all(
        row["source_session_kind"] is None
        for row in payload["surface_columns"]
        if row["kind"] == "missing"
    )


def test_v2_0925_et_boundary_forces_closed_gap_missing(
    tmp_path: Path,
) -> None:
    catalog = _catalog_with_gth(tmp_path)
    boundary = datetime(2026, 7, 17, 13, 25, tzinfo=timezone.utc)
    payload = catalog.session_surface(
        SESSION_DATE,
        at=boundary,
        role="front",
        weighting="oi_weighted",
        bucket_minutes=5,
        price_step=5.0,
    )

    assert payload["reference"]["quality"] == "unavailable"
    assert payload["reference"]["method"] is None
    assert payload["reference"]["provider"] is None
    assert payload["spot"] is None
    assert not any(row["kind"] == "projection" for row in payload["surface_columns"])
    gap = next(
        row
        for row in payload["surface_columns"]
        if row["session_kind"] == "closed_gap"
    )
    assert gap["kind"] == "missing"
    assert gap["reason"] == "scheduled_closed_gap"
    assert payload["strike_profile"] == []
    strike_metadata = payload["strike_profile_metadata"]
    assert strike_metadata["current_at"] is None
    assert strike_metadata["current_session_kind"] is None
    assert strike_metadata["current_surface_provider"] is None
    assert strike_metadata["current_reference_method"] is None
    assert strike_metadata["baseline_at"] is None
    assert strike_metadata["baseline_session_kind"] is None
    assert strike_metadata["baseline_surface_provider"] is None
    assert strike_metadata["baseline_reference_method"] is None
    assert strike_metadata["baseline_unavailable_reason"] is None
    assert payload["capabilities"]["first_validated_baseline_available"] is False

    cached = catalog.session_surface(
        SESSION_DATE,
        at=boundary,
        role="front",
        weighting="oi_weighted",
        bucket_minutes=5,
        price_step=5.0,
    )
    assert cached == payload


def _cached_gth_frame(at: datetime, index: int) -> _FrameState:
    return _FrameState(
        at=at,
        known_at=at,
        valid_until=at + timedelta(minutes=5),
        artifact_sha256=f"{index:064x}",
        expiry="20260717",
        expiry_close=datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc),
        reference_spot=7500.0,
        contracts=(),
        strike_rows=(),
        quality="ready",
        warnings=(),
        session_kind="gth",
        surface_provider="ibkr",
        reference_method="es_basis_inferred_spx",
    )


def _cache_test_context(source_fingerprint: str) -> SimpleNamespace:
    window = session_surface_window(SESSION_DATE)
    return SimpleNamespace(
        session_date=SESSION_DATE,
        source_fingerprint=source_fingerprint,
        frames=(),
        frame_minutes=5,
        close_at=window.session_end,
    )


def test_gth_close_seed_reuses_only_causal_early_prefix_without_rescan() -> None:
    window = session_surface_window(SESSION_DATE)
    clocks = (
        datetime(2026, 7, 17, 0, 20, tzinfo=timezone.utc),
        datetime(2026, 7, 17, 0, 25, tzinfo=timezone.utc),
        datetime(2026, 7, 17, 0, 30, tzinfo=timezone.utc),
    )
    seeded_frames = tuple(
        _cached_gth_frame(clock, index)
        for index, clock in enumerate(clocks, start=1)
    )
    context = _cache_test_context("source-a")
    cache = SessionSurfaceBuildCache()
    loader_calls: list[datetime] = []

    def close_loader(*_args: object, **kwargs: object) -> tuple[_FrameState, ...]:
        loader_calls.append(kwargs["as_of"])
        return seeded_frames

    close_rows = causal_frames(
        context,
        as_of=window.session_end,
        role="front",
        frame_loader=lambda _requested: pytest.fail("unexpected RTH frame load"),
        build_cache=cache,
        gth_loader=close_loader,
    )
    assert tuple(row.at for row in close_rows) == clocks
    assert loader_calls == [window.session_end]

    def unexpected_loader(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[_FrameState, ...]:
        pytest.fail("covered early GTH prefix rescanned")

    arbitrary_early = GTH_AT + timedelta(minutes=2)
    early_rows = causal_frames(
        context,
        as_of=arbitrary_early,
        role="front",
        frame_loader=lambda _requested: pytest.fail("unexpected RTH frame load"),
        build_cache=cache,
        gth_loader=unexpected_loader,
    )

    assert tuple(row.at for row in early_rows) == clocks[:2]
    assert all((row.known_at or row.at) <= arbitrary_early for row in early_rows)
    assert all(row.at <= arbitrary_early for row in early_rows)


def test_close_seed_is_reused_across_surface_selectors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _catalog_with_gth(tmp_path)
    original_loader = session_data._load_gth_frames
    loader_calls: list[datetime] = []

    def counted_loader(*args: object, **kwargs: object) -> tuple[_FrameState, ...]:
        loader_calls.append(kwargs["as_of"])
        return original_loader(*args, **kwargs)

    monkeypatch.setattr(session_data, "_load_gth_frames", counted_loader)
    close = session_surface_window(SESSION_DATE).session_end
    catalog.session_surface(
        SESSION_DATE,
        at=close,
        role="front",
        weighting="volume_weighted",
        bucket_minutes=5,
        price_step=2.5,
    )
    assert loader_calls == [close]

    early = catalog.session_surface(
        SESSION_DATE,
        at=GTH_AT,
        role="front",
        weighting="oi_weighted",
        bucket_minutes=5,
        price_step=5.0,
    )

    assert loader_calls == [close]
    assert all(
        row["source_at"] is None or row["source_at"] <= early["as_of"]
        for row in early["surface_columns"]
    )
    assert early["provenance"]["lookahead_rows_selected"] == 0


def test_gth_frame_cache_isolated_by_source_and_role_and_strictly_bounded() -> None:
    clock = datetime(2026, 7, 17, 0, 20, tzinfo=timezone.utc)
    frame = _cached_gth_frame(clock, 1)
    cache = SessionSurfaceBuildCache(
        max_gth_frame_entries=2,
        max_gth_contexts=1,
    )
    cache.put_gth_frames(
        source_fingerprint="source-a",
        role="front",
        covered_until=GTH_AT,
        frames=(frame,),
    )

    scopes: list[tuple[str, str]] = []

    def scoped_loader(
        context: SimpleNamespace,
        *,
        role: str,
        **_kwargs: object,
    ) -> tuple[_FrameState, ...]:
        scopes.append((context.source_fingerprint, role))
        return (frame,)

    for context, role in (
        (_cache_test_context("source-b"), "front"),
        (_cache_test_context("source-a"), "next"),
    ):
        rows = causal_frames(
            context,
            as_of=GTH_AT,
            role=role,
            frame_loader=lambda _requested: pytest.fail("unexpected RTH frame load"),
            build_cache=cache,
            gth_loader=scoped_loader,
        )
        assert tuple(row.at for row in rows) == (clock,)

    assert scopes == [("source-b", "front"), ("source-a", "next")]
    assert len(cache._gth_frames) <= cache.max_gth_frame_entries
    assert len(cache._gth_coverage) <= cache.max_gth_contexts
