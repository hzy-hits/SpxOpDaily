from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

import spx_spark.surface_dashboard_replay as replay_module
from spx_spark.surface_replay_service import (
    ReplayAPI,
    ReplayCacheError,
    ReplayCatalog,
    ReplayRequestError,
)
from spx_spark.surface_replay_trend import (
    FRAME_POLICY_VERSION,
    TREND_KIND,
    TREND_MODE,
    TREND_POLICY_VERSION,
)
from test_surface_dashboard_replay import AS_OF, storage_settings, write_quote_partition
from test_surface_replay_service import EVENT_AS_OF


@pytest.fixture
def catalog(tmp_path: Path) -> ReplayCatalog:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    return ReplayCatalog(data_root=settings.data_root, storage_settings=settings)


def _trend(catalog: ReplayCatalog) -> dict[str, object]:
    return catalog.trend(
        AS_OF.date(),
        role="front",
        weighting="oi_weighted",
        metric="signed_gamma",
    )


def _trend_cache(catalog: ReplayCatalog) -> Path:
    matches = list(
        (catalog.data_root / "published" / "spxw-surface" / "trend-cache").rglob(
            "2026-07-17.json"
        )
    )
    assert len(matches) == 1
    return matches[0]


def test_trend_builds_compact_source_bound_intraday_artifact(
    catalog: ReplayCatalog,
) -> None:
    payload = _trend(catalog)

    assert payload["schema_version"] == 1
    assert payload["kind"] == TREND_KIND == "spxw_intraday_gamma_replay"
    assert payload["mode"] == TREND_MODE == "replay"
    assert payload["policy_version"] == TREND_POLICY_VERSION
    assert payload["frame_policy_version"] == FRAME_POLICY_VERSION
    assert payload["frame_policy_version"] == "spxw_surface_replay.v3"
    assert payload["role"] == "front"
    assert payload["weighting"] == "oi_weighted"
    assert payload["metric"] == "signed_gamma"
    assert payload["projection_policy"] == catalog.projection_policy
    assert payload["projection_policy_sha256"] == catalog.projection_policy_sha256
    assert payload["session_close_grace_elapsed"] is True
    assert payload["session_close_grace_seconds"] == 7200
    assert payload["availability_proven"] is False
    assert payload["availability_clock"] == "unavailable"
    assert payload["point_in_time_confidence"] == "bounded_not_proven"
    assert payload["data_finalization_proven"] is False

    source = payload["source"]
    assert source["source_fingerprint"] == payload["source_fingerprint"]
    assert source["source_files_verified_unchanged_during_build"] is True
    assert source["availability_clock_available"] is False
    assert source["known_limitations"] == [
        "response_finished_at_unavailable",
        "received_at_is_cycle_started_at",
    ]
    assert set(source["parquet_file_sha256"]) == set(source["source_files"])
    spx = source["spx"]
    assert spx["point_count"] == 4
    assert spx["raw_row_count"] == 4
    assert spx["duplicate_source_at_group_count"] == 0
    assert spx["source_offset_ms"][0] == 0
    assert len(spx["source_offset_ms"]) == len(spx["known_at_offset_ms"])
    assert len(spx["source_offset_ms"]) == len(spx["price"]) == spx["point_count"]
    assert spx["source_offset_ms"] == sorted(spx["source_offset_ms"])
    assert all(
        known >= source_at
        for source_at, known in zip(
            spx["source_offset_ms"],
            spx["known_at_offset_ms"],
            strict=True,
        )
    )
    assert spx["price_field"] == "mark"
    assert spx["market_clock"] == "source_at"
    assert spx["known_at_rule"] == "max_recorded_clocks"
    assert spx["known_at_is_availability_clock"] is False

    surface = payload["surface"]
    assert surface["cadence"] == "catalog_timeline_keyframes"
    assert surface["frame_count"] == 1
    assert len(surface["shared_relative_spot_offsets"]) == 41
    assert surface["interpolation"] == "none"
    assert surface["higher_frequency_candidate_upgrade"] is False
    assert surface["validity_rule"] == (
        "min(next_keyframe_at, at_plus_frame_interval, expiry_close, session_close); "
        "unavailable_at_at"
    )
    keyframe = surface["keyframes"][0]
    assert keyframe["at"] == EVENT_AS_OF.isoformat()
    assert keyframe["valid_until"] == (EVENT_AS_OF + timedelta(minutes=5)).isoformat()
    assert keyframe["expiry"] == "20260717"
    assert keyframe["quality"] == "ready"
    assert len(keyframe["values"]) == len(surface["shared_relative_spot_offsets"])
    assert len(keyframe["frame_artifact_sha256"]) == 64
    assert surface["gaps"][-1]["start_at"] == keyframe["valid_until"]
    assert surface["gaps"][-1]["end_at"] == payload["close_at"]
    assert surface["gaps"][-1]["reason"] == "surface_keyframe_validity_elapsed"

    artifact_hash = payload.pop("artifact_sha256")
    assert artifact_hash == replay_module._canonical_sha256(payload)
    payload["artifact_sha256"] = artifact_hash
    json.dumps(payload, allow_nan=False)

    cache_path = _trend_cache(catalog)
    cache_text = str(cache_path)
    assert "/trend-cache/policy=v1/frame=5m/lookback=15s/" in cache_text
    assert f"/projection={catalog.projection_policy_sha256}/" in cache_text
    assert "/source=" in cache_text
    assert "/timeline=" in cache_text
    assert "/role=front/weighting=oi_weighted/metric=signed_gamma/" in cache_text


def test_trend_cache_reuses_verified_artifact_without_reloading_frames(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _trend(catalog)

    def unexpected_frame(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise AssertionError("cached trend reloaded a replay frame")

    monkeypatch.setattr(catalog, "frame", unexpected_frame)
    second = _trend(catalog)

    assert second["artifact_sha256"] == first["artifact_sha256"]


def test_trend_spx_series_deduplicates_source_clock_deterministically(
    catalog: ReplayCatalog,
) -> None:
    source = next((catalog.data_root / "lake" / "quotes" / "schema=v1").rglob("quotes.parquet"))
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
                received_at + INTERVAL '500 milliseconds' AS received_at,
                received_at + INTERVAL '500 milliseconds' AS last_update_at,
                mark + 0.5 AS last,
                mark + 0.5 AS mark
            )
            FROM read_parquet(?)
            WHERE instrument_id = 'index:SPX'
            ORDER BY source_at
            LIMIT 1
            """,
            [str(source)],
        )
        connection.execute("COPY replay_source TO ? (FORMAT PARQUET)", [str(replacement)])
    finally:
        connection.close()
    replacement.replace(source)

    spx = _trend(catalog)["source"]["spx"]

    assert spx["point_count"] == 4
    assert spx["raw_row_count"] == 5
    assert spx["duplicate_source_at_group_count"] == 1
    assert spx["price"][0] == 7459.5


def test_trend_cache_rejects_self_hash_tampering(catalog: ReplayCatalog) -> None:
    _trend(catalog)
    cache_path = _trend_cache(catalog)
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["source"]["spx"]["price"][0] += 1.0
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="trend_cache_hash_mismatch"):
        _trend(catalog)


def test_trend_cache_rejects_signed_source_contract_tampering(
    catalog: ReplayCatalog,
) -> None:
    _trend(catalog)
    cache_path = _trend_cache(catalog)
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["source_fingerprint"] = "0" * 64
    payload["source"]["source_fingerprint"] = "0" * 64
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = replay_module._canonical_sha256(payload)
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="trend_cache_contract_invalid"):
        _trend(catalog)


def test_api_serves_trend_with_private_etag(catalog: ReplayCatalog) -> None:
    response = ReplayAPI(catalog).dispatch(
        "GET",
        "/api/v1/replay/sessions/2026-07-17/trend"
        "?role=front&weighting=oi_weighted&metric=signed_gamma",
    )

    assert response.payload["kind"] == TREND_KIND
    assert ("Cache-Control", "private, no-cache") in response.headers
    assert (
        "ETag",
        f'"{response.payload["artifact_sha256"]}"',
    ) in response.headers


@pytest.mark.parametrize(
    ("role", "weighting", "metric"),
    [
        ("next", "volume_weighted", "gross_gamma"),
        ("front", "volume_weighted", "charm"),
        ("next", "oi_weighted", "vanna"),
    ],
)
def test_trend_supports_non_default_selector_contracts(
    catalog: ReplayCatalog,
    role: str,
    weighting: str,
    metric: str,
) -> None:
    payload = catalog.trend(
        AS_OF.date(),
        role=role,
        weighting=weighting,
        metric=metric,
    )

    assert (payload["role"], payload["weighting"], payload["metric"]) == (
        role,
        weighting,
        metric,
    )
    assert payload["surface"]["metric_unit"]
    assert payload["surface"]["keyframes"][0]["zero_ridge_spot"] is None


@pytest.mark.parametrize(
    "target",
    [
        (
            "/api/v1/replay/sessions/2026-07-17/trend"
            "?role=front&weighting=oi_weighted"
        ),
        (
            "/api/v1/replay/sessions/2026-07-17/trend"
            "?role=all&weighting=oi_weighted&metric=signed_gamma"
        ),
        (
            "/api/v1/replay/sessions/2026-07-17/trend"
            "?role=front&weighting=oi_weighted&metric=signed_gamma&extra=1"
        ),
        (
            "/api/v1/replay/sessions/2026-07-17/trend"
            "?role=front&role=next&weighting=oi_weighted&metric=signed_gamma"
        ),
    ],
)
def test_api_rejects_missing_duplicate_or_unsupported_trend_selectors(
    catalog: ReplayCatalog,
    target: str,
) -> None:
    with pytest.raises(ReplayRequestError, match="invalid_(query|trend_selector)"):
        ReplayAPI(catalog).dispatch("GET", target)
