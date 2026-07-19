from __future__ import annotations

from datetime import timedelta
import json
from pathlib import Path
import threading
import time

import duckdb
import pytest

import spx_spark.surface_dashboard_replay as replay_module
import spx_spark.surface_replay_service as service_module
from spx_spark.surface_replay_service import (
    ReplayAPI,
    ReplayCacheError,
    ReplayCatalog,
    ReplayRequestError,
)
from spx_spark.surface_replay_session import (
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

    assert payload["schema_version"] == 1
    assert payload["kind"] == SESSION_SURFACE_KIND == "spxw_session_surface"
    assert payload["policy_version"] == SESSION_SURFACE_POLICY_VERSION
    assert payload["mode"] == "replay"
    assert payload["session_date"] == "2026-07-17"
    assert payload["session_start"] == "2026-07-17T13:30:00+00:00"
    assert payload["session_end"] == "2026-07-17T20:00:00+00:00"
    assert payload["as_of"] == AS_OF.isoformat()
    assert payload["expiry"] == "20260717"
    assert payload["role"] == "front"
    assert payload["weighting"] == "oi_weighted"
    assert payload["coordinate"] == "SPX"
    assert payload["provider"] == "schwab"
    assert payload["trading_class"] == "SPXW"
    assert payload["bucket_minutes"] == 5
    assert payload["price_step"] == 5.0
    assert payload["spot"] == 7462.0

    buckets = payload["time_buckets"]
    price_grid = payload["price_grid"]
    columns = payload["surface_columns"]
    assert len(buckets) == 78
    assert buckets[0] == {
        "start_at": "2026-07-17T13:30:00+00:00",
        "end_at": "2026-07-17T13:35:00+00:00",
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
    assert payload["strike_profile_metadata"]["baseline_label"] == "first_validated"
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
    assert payload["provenance"]["point_in_time_confidence"] == (
        "bounded_not_proven"
    )
    assert payload["provenance"]["lookahead_rows_selected"] == 0
    assert payload["provenance"]["projection_selection"].startswith(
        "single_current_causal_frame"
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


def test_session_surface_candles_are_aligned_and_current_bar_can_be_partial(
    catalog: ReplayCatalog,
) -> None:
    complete = _surface(catalog)["candles"]

    assert complete == [
        {
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
            "quality": "event_sampled",
        }
    ]

    partial = _surface(
        catalog,
        at="2026-07-17T18:29:58Z",
    )["candles"]
    assert partial == [
        {
            "start_at": "2026-07-17T18:25:00+00:00",
            "end_at": "2026-07-17T18:30:00+00:00",
            "open": 7459.0,
            "high": 7459.0,
            "low": 7459.0,
            "close": 7459.0,
            "sample_count": 1,
            "complete": False,
            "source_at": "2026-07-17T18:29:55+00:00",
            "known_at": "2026-07-17T18:29:55+00:00",
            "quality": "event_sampled",
        }
    ]


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
    positive = [
        (price, value)
        for price, value in zip(payload["price_grid"], gamma, strict=True)
        if value is not None and value > 0
    ]
    negative = [
        (price, value)
        for price, value in zip(payload["price_grid"], gamma, strict=True)
        if value is not None and value < 0
    ]
    peak = max(positive, key=lambda row: row[1])
    trough = min(negative, key=lambda row: row[1])
    assert payload["gamma_positive_peaks"][available_index] == {
        "price": peak[0],
        "value": peak[1],
    }
    assert payload["gamma_negative_troughs"][available_index] == {
        "price": trough[0],
        "value": trough[1],
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
    assert len(payload["time_buckets"]) == 39
    assert len(payload["price_grid"]) == 81
    assert all(
        right - left == 2.5
        for left, right in zip(payload["price_grid"], payload["price_grid"][1:])
    )
    assert len(payload["gamma_surface"]) == 39
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

    monkeypatch.setattr(catalog, "frame", unexpected_frame)
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
