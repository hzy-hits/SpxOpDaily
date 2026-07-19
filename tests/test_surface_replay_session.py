from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import threading
import time

import duckdb
import pytest

import spx_spark.surface_dashboard_replay as replay_module
import spx_spark.surface_replay_service as service_module
import spx_spark.surface_replay_session as session_module
import spx_spark.surface_replay_session_data as session_data_module
from spx_spark.surface_replay_session_models import _FrameState
from spx_spark.surface_replay_service import (
    ReplayAPI,
    ReplayCacheError,
    ReplayCatalog,
    ReplayRequestError,
)
from spx_spark.surface_replay_session import (
    SESSION_SURFACE_CACHE_VERSION,
    SESSION_SURFACE_KIND,
    SESSION_SURFACE_POLICY_VERSION,
)
from test_surface_dashboard_replay import AS_OF, storage_settings, write_quote_partition
from test_surface_replay_service import EVENT_AS_OF


@pytest.fixture
def catalog(tmp_path: Path) -> ReplayCatalog:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    return ReplayCatalog(data_root=settings.data_root, storage_settings=settings)


def _target(
    *,
    at: str = "2026-07-17T18:30:00Z",
    role: str = "front",
    weighting: str = "oi_weighted",
    bucket_minutes: int = 5,
    price_step: float = 5,
) -> str:
    return (
        "/api/v1/replay/sessions/2026-07-17/session-surface"
        f"?at={at}&role={role}&weighting={weighting}"
        f"&bucket_minutes={bucket_minutes}&price_step={price_step}"
    )


def _surface(catalog: ReplayCatalog, **kwargs: object) -> dict[str, object]:
    return ReplayAPI(catalog).dispatch("GET", _target(**kwargs)).payload


def test_session_surface_endpoint_builds_frontend_contract(
    catalog: ReplayCatalog,
) -> None:
    response = ReplayAPI(catalog).dispatch("GET", _target())
    payload = response.payload

    assert payload["schema_version"] == 2
    assert payload["kind"] == SESSION_SURFACE_KIND == "spxw_session_surface"
    assert payload["policy_version"] == SESSION_SURFACE_POLICY_VERSION
    assert payload["mode"] == "replay"
    assert payload["session_date"] == "2026-07-17"
    assert payload["session_start"] == "2026-07-17T00:15:00+00:00"
    assert payload["session_end"] == "2026-07-17T20:00:00+00:00"
    assert payload["as_of"] == AS_OF.isoformat()
    assert payload["expiry"] == "20260717"
    assert payload["role"] == "front"
    assert payload["weighting"] == "oi_weighted"
    assert payload["coordinate"] == "SPX"
    assert payload["provider"] == "mixed"
    assert payload["providers"] == {
        "gth_surface": "ibkr",
        "gth_reference": "schwab",
        "rth_surface": "schwab",
        "rth_reference": "schwab",
    }
    assert [row["kind"] for row in payload["session_segments"]] == [
        "gth",
        "closed_gap",
        "rth",
    ]
    assert payload["trading_class"] == "SPXW"
    assert payload["bucket_minutes"] == 5
    assert payload["price_step"] == 5.0
    assert payload["spot"] == 7462.0

    buckets = payload["time_buckets"]
    price_grid = payload["price_grid"]
    columns = payload["surface_columns"]
    assert len(buckets) == 237
    assert buckets[0] == {
        "start_at": "2026-07-17T00:15:00+00:00",
        "end_at": "2026-07-17T00:20:00+00:00",
    }
    assert buckets[-1]["end_at"] == payload["session_end"]
    assert len(price_grid) == 41
    assert all(right - left == 5.0 for left, right in zip(price_grid, price_grid[1:]))
    assert payload["price_grid_policy"]["anchor_source"] == (
        "first_causal_session_spot"
    )
    assert len(columns) == len(buckets)
    for key in (
        "gamma_surface",
        "gross_gamma_surface",
        "charm_surface",
        "vanna_surface",
    ):
        matrix = payload[key]
        assert len(matrix) == len(buckets)
        assert all(len(row) == len(price_grid) for row in matrix)
    assert len(payload["zero_ridges"]) == len(buckets)
    assert len(payload["gamma_positive_peaks"]) == len(buckets)
    assert len(payload["gamma_negative_troughs"]) == len(buckets)
    strike_metadata = payload["strike_profile_metadata"]
    assert strike_metadata["baseline_label"] == (
        "first_validated_same_segment_provider"
    )
    assert strike_metadata["baseline_unavailable_reason"] is None
    assert strike_metadata["comparison_semantics"] == (
        "snapshot_state_not_position_or_flow"
    )
    assert strike_metadata["baseline_session_kind"] == "rth"
    assert strike_metadata["baseline_surface_provider"] == "schwab"
    assert strike_metadata["baseline_reference_method"] == "direct_index_spx"
    assert strike_metadata["current_session_kind"] == "rth"
    assert strike_metadata["current_surface_provider"] == "schwab"
    assert strike_metadata["current_reference_method"] == "direct_index_spx"
    assert payload["strike_profile_metadata"]["exact_sod_available"] is False
    assert payload["strike_profile"]
    assert {
        "current_proxy",
        "first_validated_proxy",
        "current_open_interest",
        "first_validated_open_interest",
    } <= set(payload["strike_profile"][0])

    capabilities = payload["capabilities"]
    assert capabilities["proxy_position_available"] is True
    assert capabilities["participant_position_available"] is False
    assert capabilities["open_close_available"] is False
    assert capabilities["signed_flow_available"] is False
    assert capabilities["strict_point_in_time_available"] is False
    assert capabilities["known_clock_no_lookahead"] is True
    assert capabilities["gth_contract_declared"] is True
    assert capabilities["gth_complete_chain_available"] is False
    assert capabilities["gth_data_available"] is False
    assert payload["provenance"]["point_in_time_confidence"] == (
        "bounded_not_proven"
    )
    assert payload["provenance"]["lookahead_rows_selected"] == 0
    assert payload["provenance"]["projection_selection"].startswith(
        "single_current_causal_frame"
    )
    assert (
        "gth_contract_universe_completeness_unproven"
        in payload["provenance"]["known_limitations"]
    )

    artifact_hash = payload.pop("artifact_sha256")
    assert artifact_hash == replay_module._canonical_sha256(payload)
    payload["artifact_sha256"] = artifact_hash
    assert ("Cache-Control", "private, no-cache") in response.headers
    assert ("ETag", f'"{artifact_hash}"') in response.headers


def test_session_surface_is_causal_and_never_loads_future_frame(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    future_frame = AS_OF + timedelta(minutes=5)
    original_frame = catalog._ensure_materialized_frame
    loaded: list[object] = []
    monkeypatch.setattr(
        catalog,
        "viable_frames",
        lambda _session_date: (EVENT_AS_OF, future_frame),
    )

    def causal_frame(requested: object) -> dict[str, object]:
        loaded.append(requested)
        assert requested <= AS_OF
        return original_frame(requested)

    monkeypatch.setattr(catalog, "_ensure_materialized_frame", causal_frame)

    payload = _surface(catalog)

    assert loaded == [EVENT_AS_OF]
    assert payload["spot"] == 7462.0
    assert payload["spot_source_at"] <= payload["as_of"]
    assert payload["spot_known_at"] <= payload["as_of"]
    for column in payload["surface_columns"]:
        assert column["source_at"] is None or column["source_at"] <= payload["as_of"]
    for candle in payload["candles"]:
        assert candle["source_at"] <= payload["as_of"]
        assert candle["known_at"] <= payload["as_of"]
    assert payload["provenance"]["causal_frame_count"] == 1
    assert len(payload["provenance"]["causal_frame_artifact_sha256"]) == 1
    strike_metadata = payload["strike_profile_metadata"]
    assert strike_metadata["baseline_at"] <= strike_metadata["current_at"]
    assert strike_metadata["current_at"] <= payload["as_of"]
    assert (
        strike_metadata["baseline_session_kind"],
        strike_metadata["baseline_surface_provider"],
        strike_metadata["baseline_reference_method"],
    ) == (
        strike_metadata["current_session_kind"],
        strike_metadata["current_surface_provider"],
        strike_metadata["current_reference_method"],
    )


def test_session_surface_marks_incomplete_legacy_frame_missing(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        catalog,
        "viable_frames",
        lambda _session_date: (EVENT_AS_OF,),
    )

    def incomplete_frame(_requested: object) -> dict[str, object]:
        raise replay_module.ReplaySourceError(
            "replay_front_next_projection_incomplete"
        )

    monkeypatch.setattr(catalog, "_ensure_materialized_frame", incomplete_frame)

    payload = _surface(catalog)

    assert payload["provenance"]["causal_frame_count"] == 0
    assert (
        "legacy_frame_incomplete_expiry_projection_is_missing"
        in payload["provenance"]["known_limitations"]
    )
    rth_columns = [
        row
        for row in payload["surface_columns"]
        if row["session_kind"] == "rth"
    ]
    assert rth_columns
    assert all(row["kind"] == "missing" for row in rth_columns)
    assert all(
        all(value is None for value in row)
        for row in payload["gamma_surface"]
    )


def test_session_surface_candles_are_aligned_and_current_bar_can_be_partial(
    catalog: ReplayCatalog,
) -> None:
    complete = _surface(catalog)["candles"]

    assert len(complete) == 1
    assert complete[0] == {
        **complete[0],
        "start_at": "2026-07-17T18:25:00+00:00",
        "end_at": "2026-07-17T18:30:00+00:00",
        "open": 7459.0,
        "high": 7462.0,
        "low": 7459.0,
        "close": 7462.0,
        "sample_count": 2,
        "complete": True,
        "source_at": "2026-07-17T18:29:59+00:00",
        "known_at": "2026-07-17T18:29:59+00:00",
        "accepted_at": None,
        "reference_method": "direct_index_spx",
        "reference_provider": "schwab",
        "reference_instrument_id": "index:SPX",
        "render_style": "direct_solid",
        "session_kind": "rth",
        "quality": "event_sampled",
    }

    partial = _surface(
        catalog,
        at="2026-07-17T18:29:58Z",
    )["candles"]
    assert len(partial) == 1
    assert partial[0]["start_at"] == "2026-07-17T18:25:00+00:00"
    assert partial[0]["end_at"] == "2026-07-17T18:30:00+00:00"
    assert partial[0]["open"] == partial[0]["high"] == 7459.0
    assert partial[0]["low"] == partial[0]["close"] == 7459.0
    assert partial[0]["sample_count"] == 1
    assert partial[0]["complete"] is False
    assert partial[0]["source_at"] == "2026-07-17T18:29:55+00:00"
    assert partial[0]["known_at"] == "2026-07-17T18:29:55+00:00"
    assert partial[0]["accepted_at"] is None


def test_session_surface_missing_columns_are_null_not_zero(
    catalog: ReplayCatalog,
) -> None:
    payload = _surface(catalog)
    matrices = [
        payload["gamma_surface"],
        payload["gross_gamma_surface"],
        payload["charm_surface"],
        payload["vanna_surface"],
    ]

    missing_indexes = [
        index
        for index, column in enumerate(payload["surface_columns"])
        if column["kind"] == "missing"
    ]
    assert missing_indexes
    for index in missing_indexes:
        assert payload["zero_ridges"][index] is None
        assert payload["gamma_positive_peaks"][index] is None
        assert payload["gamma_negative_troughs"][index] is None
        assert all(
            all(value is None for value in matrix[index])
            for matrix in matrices
        )
    assert any(
        value is not None
        for index, column in enumerate(payload["surface_columns"])
        if column["kind"] != "missing"
        for value in payload["gamma_surface"][index]
    )
    available_index = next(
        index
        for index, column in enumerate(payload["surface_columns"])
        if column["kind"] != "missing"
    )
    gamma = payload["gamma_surface"][available_index]
    price_grid = tuple(payload["price_grid"])
    assert payload["gamma_positive_peaks"][available_index] == (
        session_data_module._interior_local_extremum(
            price_grid,
            gamma,
            positive=True,
        )
    )
    assert payload["gamma_negative_troughs"][available_index] == (
        session_data_module._interior_local_extremum(
            price_grid,
            gamma,
            positive=False,
        )
    )


def test_session_surface_extrema_reject_grid_edges_and_require_local_turn() -> None:
    prices = (100.0, 105.0, 110.0, 115.0, 120.0)

    assert session_data_module._interior_local_extremum(
        prices,
        [9.0, 7.0, 5.0, 3.0, 1.0],
        positive=True,
    ) is None
    assert session_data_module._interior_local_extremum(
        prices,
        [-1.0, -3.0, -5.0, -7.0, -9.0],
        positive=False,
    ) is None
    assert session_data_module._interior_local_extremum(
        prices,
        [1.0, 4.0, 2.0, -5.0, -1.0],
        positive=True,
    ) == {"price": 105.0, "value": 4.0}
    assert session_data_module._interior_local_extremum(
        prices,
        [1.0, 4.0, 2.0, -5.0, -1.0],
        positive=False,
    ) == {"price": 115.0, "value": -5.0}
    assert session_data_module._interior_local_extremum(
        prices,
        [9.0, 2.0, 6.0, 1.0, 10.0],
        positive=True,
    ) == {"price": 110.0, "value": 6.0}
    assert session_data_module._interior_local_extremum(
        prices,
        [-10.0, -2.0, -6.0, -1.0, -9.0],
        positive=False,
    ) == {"price": 110.0, "value": -6.0}


def test_strike_profile_baseline_is_first_same_segment_provider_snapshot() -> None:
    def frame(
        *,
        at: datetime,
        valid_until: datetime,
        proxy: float,
        open_interest: float,
        session_kind: str,
        provider: str,
        reference_method: str,
        quality: str = "ready",
    ) -> _FrameState:
        return _FrameState(
            at=at,
            valid_until=valid_until,
            artifact_sha256=f"{int(proxy):064d}"[-64:],
            expiry="20260717",
            expiry_close=AS_OF + timedelta(hours=2),
            reference_spot=7460.0,
            contracts=(),
            strike_rows=(
                {
                    "strike": 7460.0,
                    "call": {"open_interest": open_interest},
                    "put": None,
                    "quality": "ok",
                    "weightings": {
                        "oi_weighted": {
                            "quality": "ok",
                            "metrics": {"signed_gamma": proxy},
                        }
                    },
                },
            ),
            quality=quality,
            warnings=(),
            session_kind=session_kind,
            surface_provider=provider,
            reference_method=reference_method,
        )

    gth = frame(
        at=AS_OF - timedelta(hours=12),
        valid_until=AS_OF - timedelta(hours=11, minutes=55),
        proxy=100.0,
        open_interest=10.0,
        session_kind="gth",
        provider="ibkr",
        reference_method="es_basis_inferred_spx",
    )
    unavailable_rth = frame(
        at=AS_OF - timedelta(hours=5),
        valid_until=AS_OF - timedelta(hours=4, minutes=55),
        proxy=150.0,
        open_interest=15.0,
        session_kind="rth",
        provider="schwab",
        reference_method="direct_index_spx",
        quality="unavailable",
    )
    zero_lifetime_rth = frame(
        at=AS_OF - timedelta(hours=4, minutes=30),
        valid_until=AS_OF - timedelta(hours=4, minutes=30),
        proxy=175.0,
        open_interest=17.5,
        session_kind="rth",
        provider="schwab",
        reference_method="direct_index_spx",
    )
    first_rth = frame(
        at=AS_OF - timedelta(hours=4),
        valid_until=AS_OF - timedelta(hours=3, minutes=55),
        proxy=200.0,
        open_interest=20.0,
        session_kind="rth",
        provider="schwab",
        reference_method="direct_index_spx",
    )
    current_rth = frame(
        at=AS_OF - timedelta(seconds=10),
        valid_until=AS_OF + timedelta(minutes=4),
        proxy=300.0,
        open_interest=30.0,
        session_kind="rth",
        provider="schwab",
        reference_method="direct_index_spx",
    )

    rows, metadata = session_data_module._strike_profile(
        (current_rth, first_rth, zero_lifetime_rth, unavailable_rth, gth),
        as_of=AS_OF,
        weighting="oi_weighted",
        active_session_kind="rth",
    )

    assert rows == [
        {
            "strike": 7460.0,
            "current_proxy": 300.0,
            "first_validated_proxy": 200.0,
            "current_open_interest": 30.0,
            "first_validated_open_interest": 20.0,
            "quality": "ready",
        }
    ]
    assert metadata == {
        "baseline_label": "first_validated_same_segment_provider",
        "baseline_unavailable_reason": None,
        "baseline_at": first_rth.at.isoformat(),
        "current_at": current_rth.at.isoformat(),
        "baseline_session_kind": "rth",
        "baseline_surface_provider": "schwab",
        "baseline_reference_method": "direct_index_spx",
        "current_session_kind": "rth",
        "current_surface_provider": "schwab",
        "current_reference_method": "direct_index_spx",
        "comparison_semantics": "snapshot_state_not_position_or_flow",
        "exact_sod_available": False,
        "missing_join_value": None,
        "proxy_metric": "signed_gamma",
    }


def test_session_surface_price_grid_is_stable_across_adjacent_playheads(
    catalog: ReplayCatalog,
) -> None:
    earlier = _surface(catalog, at="2026-07-17T18:29:58Z")
    later = _surface(catalog)

    assert earlier["spot"] == 7459.0
    assert later["spot"] == 7462.0
    assert earlier["price_grid"] == later["price_grid"]
    assert earlier["price_grid_policy"]["anchor"] == 7460.0
    assert later["price_grid_policy"]["anchor"] == 7460.0


def test_completed_candle_is_frozen_against_late_same_source_revision(
    tmp_path: Path,
) -> None:
    source = write_quote_partition(tmp_path)
    replacement = source.with_name("quotes.replacement.parquet")
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE replay_source AS SELECT * FROM read_parquet(?)",
            [str(source)],
        )
        connection.execute(
            """
            INSERT INTO replay_source
            SELECT * REPLACE (
                ?::TIMESTAMPTZ AS received_at,
                ?::TIMESTAMPTZ AS last_update_at,
                7000.0 AS last,
                7000.0 AS mark
            )
            FROM read_parquet(?)
            WHERE instrument_id = 'index:SPX'
              AND source_at = ?::TIMESTAMPTZ
            LIMIT 1
            """,
            [
                AS_OF + timedelta(seconds=30),
                AS_OF + timedelta(seconds=30),
                str(source),
                AS_OF - timedelta(seconds=5),
            ],
        )
        connection.execute(
            "COPY replay_source TO ? (FORMAT PARQUET)",
            [str(replacement)],
        )
    finally:
        connection.close()
    replacement.replace(source)
    settings = storage_settings(tmp_path)
    replay_catalog = ReplayCatalog(
        data_root=settings.data_root,
        storage_settings=settings,
    )

    before = _surface(replay_catalog)["candles"]
    after = _surface(
        replay_catalog,
        at="2026-07-17T18:31:00Z",
    )["candles"]

    before_completed = next(
        row for row in before if row["start_at"] == "2026-07-17T18:25:00+00:00"
    )
    after_completed = next(
        row for row in after if row["start_at"] == "2026-07-17T18:25:00+00:00"
    )
    assert after_completed == before_completed
    assert after_completed["low"] == 7459.0
    assert replay_catalog.session_surface(
        AS_OF.date(),
        at=AS_OF + timedelta(minutes=1),
        role="front",
        weighting="oi_weighted",
        bucket_minutes=5,
        price_step=5.0,
    )["provenance"]["spx_dedupe_rule"].startswith("earliest_known_at")


def test_completed_candle_rejects_unique_event_known_after_bucket_end(
    tmp_path: Path,
) -> None:
    source = write_quote_partition(tmp_path)
    replacement = source.with_name("quotes.replacement.parquet")
    normal_source_at = AS_OF - timedelta(seconds=4)
    normal_known_at = AS_OF - timedelta(seconds=3)
    late_source_at = AS_OF - timedelta(seconds=3)
    late_known_at = AS_OF + timedelta(seconds=30)
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE replay_source AS SELECT * FROM read_parquet(?)",
            [str(source)],
        )
        connection.execute(
            """
            INSERT INTO replay_source
            SELECT * REPLACE (
                ?::TIMESTAMPTZ AS received_at,
                ?::TIMESTAMPTZ AS source_at,
                ?::TIMESTAMPTZ AS quote_time,
                ?::TIMESTAMPTZ AS last_update_at,
                7470.0 AS last,
                7470.0 AS mark
            )
            FROM read_parquet(?)
            WHERE instrument_id = 'index:SPX'
            LIMIT 1
            """,
            [
                normal_known_at,
                normal_source_at,
                normal_source_at,
                normal_known_at,
                str(source),
            ],
        )
        connection.execute(
            """
            INSERT INTO replay_source
            SELECT * REPLACE (
                ?::TIMESTAMPTZ AS received_at,
                ?::TIMESTAMPTZ AS source_at,
                ?::TIMESTAMPTZ AS quote_time,
                ?::TIMESTAMPTZ AS last_update_at,
                7000.0 AS last,
                7000.0 AS mark
            )
            FROM read_parquet(?)
            WHERE instrument_id = 'index:SPX'
            LIMIT 1
            """,
            [
                late_known_at,
                late_source_at,
                late_source_at,
                late_known_at,
                str(source),
            ],
        )
        connection.execute(
            "COPY replay_source TO ? (FORMAT PARQUET)",
            [str(replacement)],
        )
    finally:
        connection.close()
    replacement.replace(source)
    settings = storage_settings(tmp_path)
    replay_catalog = ReplayCatalog(
        data_root=settings.data_root,
        storage_settings=settings,
    )

    before = _surface(replay_catalog)["candles"]
    after = _surface(
        replay_catalog,
        at="2026-07-17T18:31:00Z",
    )["candles"]
    bucket_start = "2026-07-17T18:25:00+00:00"
    before_completed = next(row for row in before if row["start_at"] == bucket_start)
    after_completed = next(row for row in after if row["start_at"] == bucket_start)

    assert before_completed == after_completed
    assert before_completed["sample_count"] == 3
    assert before_completed["high"] == 7470.0
    assert before_completed["low"] == 7459.0


def test_session_surface_cache_contract_tracks_vectorized_kernel(
    catalog: ReplayCatalog,
) -> None:
    assert SESSION_SURFACE_CACHE_VERSION == 8
    payload = _surface(catalog)
    assert payload["policy_version"] == "spxw_session_surface.v5"
    assert payload["provenance"]["calculation_engine"] == (
        "numpy_vectorized_bs_stable_sum.v1"
    )
    assert payload["provenance"]["numeric_reduction"] == (
        "extended_precision_or_fsum_signed_float64_gross"
    )
    cache_files = list(
        (
            catalog.data_root
            / "published"
            / "spxw-surface"
            / "session-surface-cache"
        ).rglob("2026-07-17T183000Z.json")
    )
    assert len(cache_files) == 1
    assert "policy=v5" in cache_files[0].parts
    assert "contract=8" in cache_files[0].parts


def test_session_surface_cache_rejects_old_numeric_engine(
    catalog: ReplayCatalog,
) -> None:
    _surface(catalog)
    matches = list(
        (
            catalog.data_root
            / "published"
            / "spxw-surface"
            / "session-surface-cache"
        ).rglob("2026-07-17T183000Z.json")
    )
    assert len(matches) == 1
    path = matches[0]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["provenance"]["calculation_engine"] = "python_math_fsum.v1"
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = replay_module._canonical_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="session_surface_cache_provenance_invalid"):
        catalog.session_surface(
            AS_OF.date(),
            at=AS_OF,
            role="front",
            weighting="oi_weighted",
            bucket_minutes=5,
            price_step=5.0,
        )


def test_session_surface_cache_rejects_cross_provider_strike_baseline(
    catalog: ReplayCatalog,
) -> None:
    _surface(catalog)
    matches = list(
        (
            catalog.data_root
            / "published"
            / "spxw-surface"
            / "session-surface-cache"
        ).rglob("2026-07-17T183000Z.json")
    )
    assert len(matches) == 1
    path = matches[0]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["strike_profile_metadata"]["baseline_surface_provider"] = "ibkr"
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = replay_module._canonical_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="session_surface_cache_strike_invalid"):
        catalog.session_surface(
            AS_OF.date(),
            at=AS_OF,
            role="front",
            weighting="oi_weighted",
            bucket_minutes=5,
            price_step=5.0,
        )


def test_session_surface_supports_bounded_10m_and_2_5_point_grid(
    catalog: ReplayCatalog,
) -> None:
    payload = _surface(
        catalog,
        bucket_minutes=10,
        price_step=2.5,
    )

    assert payload["bucket_minutes"] == 10
    assert payload["price_step"] == 2.5
    assert len(payload["time_buckets"]) == 119
    assert len(payload["price_grid"]) == 81
    assert all(
        right - left == 2.5
        for left, right in zip(payload["price_grid"], payload["price_grid"][1:])
    )
    assert len(payload["gamma_surface"]) == 119
    assert all(len(row) == 81 for row in payload["gamma_surface"])


@pytest.mark.parametrize(
    "target",
    [
        (
            "/api/v1/replay/sessions/2026-07-17/session-surface"
            "?at=2026-07-17T18:30:00Z&role=front&weighting=oi_weighted"
        ),
        _target(bucket_minutes=1),
        _target(price_step=1),
        _target(role="all"),
        _target(weighting="dealer"),
        _target() + "&extra=1",
    ],
)
def test_session_surface_rejects_missing_or_unsupported_query(
    catalog: ReplayCatalog,
    target: str,
) -> None:
    with pytest.raises(
        ReplayRequestError,
        match="invalid_(query|session_surface_selector)",
    ):
        ReplayAPI(catalog).dispatch("GET", target)


def test_session_surface_file_cache_reuses_artifact_without_frames(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _surface(catalog)

    def unexpected_frame(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise AssertionError("verified session-surface cache reloaded a frame")

    def unexpected_source_hashes(*_args: object, **_kwargs: object) -> dict[str, str]:
        raise AssertionError("verified session-surface cache rehashed source files")

    monkeypatch.setattr(catalog, "frame", unexpected_frame)
    monkeypatch.setattr(service_module, "_sha256", unexpected_source_hashes)
    monkeypatch.setattr(session_module, "_source_hashes", unexpected_source_hashes)
    second = _surface(catalog)

    assert second["artifact_sha256"] == first["artifact_sha256"]


def test_signed_cache_still_rejects_historical_column_relabeled_projection(
    catalog: ReplayCatalog,
) -> None:
    _surface(catalog)
    matches = list(
        (
            catalog.data_root
            / "published"
            / "spxw-surface"
            / "session-surface-cache"
        ).rglob("2026-07-17T183000Z.json")
    )
    assert len(matches) == 1
    path = matches[0]
    payload = json.loads(path.read_text(encoding="utf-8"))
    historical_index = next(
        index
        for index, column in enumerate(payload["surface_columns"])
        if column["kind"] == "historical"
    )
    payload["surface_columns"][historical_index]["kind"] = "projection"
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = replay_module._canonical_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="session_surface_cache_lookahead"):
        catalog.session_surface(
            AS_OF.date(),
            at=AS_OF,
            role="front",
            weighting="oi_weighted",
            bucket_minutes=5,
            price_step=5.0,
        )


def test_adjacent_session_surface_requests_wait_for_in_process_builder(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog.viable_frames(AS_OF.date())
    first_started = threading.Event()
    release_first = threading.Event()
    calls: list[object] = []
    results: list[dict[str, object]] = []
    errors: list[BaseException] = []

    def controlled_materialize(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs["as_of"])
        if len(calls) == 1:
            first_started.set()
            assert release_first.wait(timeout=2)
        return {"as_of": kwargs["as_of"]}

    monkeypatch.setattr(
        service_module,
        "materialize_session_surface",
        controlled_materialize,
    )

    def request(at: object) -> None:
        try:
            results.append(
                catalog.session_surface(
                    AS_OF.date(),
                    at=at,
                    role="front",
                    weighting="oi_weighted",
                    bucket_minutes=5,
                    price_step=5.0,
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first = threading.Thread(target=request, args=(EVENT_AS_OF,))
    second = threading.Thread(target=request, args=(AS_OF,))
    first.start()
    assert first_started.wait(timeout=2)
    second.start()
    time.sleep(0.05)
    assert len(calls) == 1
    release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not errors
    assert len(results) == 2
    assert calls == [EVENT_AS_OF, AS_OF]
    assert service_module.SESSION_SURFACE_LOCK_TIMEOUT_SECONDS == 15.0
