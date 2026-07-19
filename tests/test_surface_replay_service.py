from __future__ import annotations

import json
import socket
import stat
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

import spx_spark.surface_replay_service as service_module
from spx_spark.surface_dashboard_replay import (
    REPLAY_POLICY_VERSION,
    ReplaySourceError,
    default_replay_output_path,
)
from spx_spark.surface_replay_service import (
    ReplayAPI,
    ReplayCacheError,
    ReplayCatalog,
    ReplayHTTPServer,
    ReplayRequestError,
    ReplayUnixHTTPServer,
    parse_args,
)
from test_surface_dashboard_replay import AS_OF, storage_settings, write_quote_partition


EVENT_AS_OF = AS_OF - timedelta(seconds=2)


@pytest.fixture
def catalog(tmp_path: Path) -> ReplayCatalog:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    return ReplayCatalog(data_root=settings.data_root, storage_settings=settings)


def _http_get(
    server: ReplayHTTPServer,
    target: str,
    *,
    headers: tuple[tuple[str, str], ...] = (),
) -> tuple[int, dict[str, str], bytes]:
    client = socket.create_connection(server.server_address, timeout=5)
    header_lines = "".join(f"{key}: {value}\r\n" for key, value in headers)
    request = (
        f"GET {target} HTTP/1.1\r\n"
        "Host: localhost\r\n"
        f"{header_lines}"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    with client:
        client.sendall(request)
        response = b""
        while chunk := client.recv(65536):
            response += chunk
    raw_headers, body = response.split(b"\r\n\r\n", 1)
    lines = raw_headers.decode("iso-8859-1").split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    parsed_headers = {
        key.strip().lower(): value.strip()
        for line in lines[1:]
        for key, value in [line.split(":", 1)]
    }
    return status, parsed_headers, body


def test_catalog_discovers_session_and_indexes_only_viable_frames(
    catalog: ReplayCatalog,
) -> None:
    sessions = catalog.discover_sessions()

    assert [item.session_date.isoformat() for item in sessions] == ["2026-07-17"]
    timeline = catalog.timeline_payload(AS_OF.date())
    assert timeline["frame_interval_minutes"] == 5
    assert timeline["frame_count"] == 1
    assert timeline["frames"] == [
        {
            "id": "2026-07-17T182958Z",
            "replay_id": "2026-07-17T182958Z",
            "at": "2026-07-17T18:29:58Z",
            "requested_as_of": "2026-07-17T18:29:58Z",
            "label": "14:29:58 ET",
            "label_et": "14:29:58 ET",
            "cached": False,
            "projection_policy_sha256": catalog.projection_policy_sha256,
            "url": (
                "/api/v1/replay/sessions/2026-07-17/frame"
                "?at=2026-07-17T18:29:58Z"
            ),
            "frame_url": "/api/v1/replay/frames/2026-07-17T182958Z",
        }
    ]
    manifest = (
        catalog.catalog_root
        / "session=2026-07-17"
        / "timeline-5m.json"
    )
    assert stat.S_IMODE(manifest.stat().st_mode) == 0o600


def test_timeline_uses_memory_cache_when_source_fingerprint_is_unchanged(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = catalog.viable_frames(AS_OF.date())

    def unexpected_scan(*_args: object, **_kwargs: object) -> tuple[()]:
        raise AssertionError("timeline was rescanned")

    monkeypatch.setattr(catalog, "_scan_viable_frames", unexpected_scan)

    assert catalog.viable_frames(AS_OF.date()) == expected


def test_timeline_source_scan_uses_configured_lookback_at_session_open(
    tmp_path: Path,
) -> None:
    source = write_quote_partition(tmp_path)
    replacement = source.with_name("quotes.replacement.parquet")
    session_open = datetime(2026, 7, 17, 13, 30, tzinfo=timezone.utc)
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE replay_source AS SELECT * FROM read_parquet(?)",
            [str(source)],
        )
        connection.execute(
            """
            UPDATE replay_source
            SET received_at = ?, source_at = ?, quote_time = ?, last_update_at = ?
            WHERE trading_class = 'SPXW'
            """,
            [session_open + timedelta(seconds=1)] * 4,
        )
        connection.execute(
            """
            UPDATE replay_source
            SET received_at = ?, source_at = ?, quote_time = ?, last_update_at = ?
            WHERE instrument_id = 'index:SPX' AND bid = 7458.0
            """,
            [session_open - timedelta(seconds=40)] * 4,
        )
        connection.execute(
            """
            UPDATE replay_source
            SET received_at = ?, source_at = ?, quote_time = ?, last_update_at = ?
            WHERE instrument_id = 'index:SPX' AND bid <> 7458.0
            """,
            [session_open + timedelta(seconds=2)] * 4,
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
        lookback_seconds=60.0,
    )

    timeline = replay_catalog.timeline_payload(AS_OF.date())

    assert timeline["lookback_seconds"] == 60.0
    assert timeline["frame_count"] == 1
    assert timeline["frames"][0]["at"] == "2026-07-17T13:30:01Z"


def test_frame_builds_policy_scoped_cache_and_reuses_it(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)
    cache_path = catalog._cache_path(EVENT_AS_OF)

    assert payload["policy_version"] == REPLAY_POLICY_VERSION
    assert REPLAY_POLICY_VERSION == "spxw_surface_replay.v3"
    assert payload["source"]["point_in_time_confidence"] == "bounded_not_proven"
    assert cache_path.is_file()
    assert "replay-cache/policy=v3" in str(cache_path)
    assert "/lookback=15s/" in str(cache_path)
    assert f"projection={catalog.projection_policy_sha256}" in str(cache_path)
    assert "/source=" in str(cache_path)
    assert cache_path != default_replay_output_path(catalog.data_root, as_of=EVENT_AS_OF)

    def unexpected_generate(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("cached frame was regenerated")

    monkeypatch.setattr(service_module, "generate_replay", unexpected_generate)
    assert catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)["artifact_sha256"] == payload["artifact_sha256"]


def test_frame_rejects_cache_tampering(catalog: ReplayCatalog) -> None:
    catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)
    cache_path = catalog._cache_path(EVENT_AS_OF)
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["underlier"]["price"] += 1
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="replay_cache_hash_mismatch"):
        catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)


def test_frame_cache_rehashes_current_parquet(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)
    monkeypatch.setattr(service_module, "_sha256", lambda _path: "0" * 64)

    with pytest.raises(ReplayCacheError, match="replay_cache_source_hash_mismatch"):
        catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)


def test_frame_cache_rejects_signed_wrong_lookback(catalog: ReplayCatalog) -> None:
    catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)
    cache_path = catalog._cache_path(EVENT_AS_OF)
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["source"]["lookback_seconds"] = 30.0
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = service_module._canonical_sha256(payload)
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="replay_cache_lookback_mismatch"):
        catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)


def test_frame_cache_rejects_signed_wrong_projection_policy(
    catalog: ReplayCatalog,
) -> None:
    catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)
    cache_path = catalog._cache_path(EVENT_AS_OF)
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["projection_policy_sha256"] = "0" * 64
    payload.pop("artifact_sha256")
    payload["artifact_sha256"] = service_module._canonical_sha256(payload)
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReplayCacheError, match="replay_cache_projection_policy_mismatch"):
        catalog.frame(EVENT_AS_OF.date(), EVENT_AS_OF)


def test_frame_cache_namespace_changes_with_source_stat(catalog: ReplayCatalog) -> None:
    before = catalog._cache_path(EVENT_AS_OF)
    source_paths, _fingerprint = catalog._frame_source_context(EVENT_AS_OF)
    source_paths[0].touch()

    assert catalog._cache_path(EVENT_AS_OF) != before


def test_frame_requires_timeline_membership(catalog: ReplayCatalog) -> None:
    with pytest.raises(ReplayRequestError, match="replay_frame_not_found"):
        catalog.frame(AS_OF.date(), EVENT_AS_OF + timedelta(minutes=5))


def test_api_supports_session_timeline_and_both_frame_routes(
    catalog: ReplayCatalog,
) -> None:
    api = ReplayAPI(catalog)

    sessions = api.dispatch("GET", "/api/v1/replay/sessions")
    assert sessions.payload["default_session"] == "2026-07-17"
    assert sessions.payload["sessions"][0]["timeline_status"] == "on_demand"
    assert sessions.payload["only_close_grace_elapsed_sessions"] is True
    assert sessions.payload["session_close_grace_seconds"] == 7200
    assert sessions.payload["data_finalization_proven"] is False
    assert sessions.payload["projection_policy_sha256"] == catalog.projection_policy_sha256
    assert sessions.payload["sessions"][0]["session_close_grace_elapsed"] is True
    assert sessions.payload["sessions"][0]["data_finalization_proven"] is False

    timeline = api.dispatch(
        "GET",
        "/api/v1/replay/sessions/2026-07-17/timeline?step_minutes=5",
    )
    assert timeline.payload["frame_count"] == 1
    assert timeline.payload["session_close_grace_elapsed"] is True
    assert timeline.payload["data_finalization_proven"] is False
    assert timeline.payload["availability_clock"] == "unavailable"
    assert timeline.payload["frame_validation"] == (
        "known_clock_validation_on_frame_request"
    )
    assert timeline.payload["projection_policy_sha256"] == catalog.projection_policy_sha256
    assert timeline.payload["frames"][0]["projection_policy_sha256"] == (
        catalog.projection_policy_sha256
    )

    query_frame = api.dispatch(
        "GET",
        "/api/v1/replay/sessions/2026-07-17/frame?at=2026-07-17T18:29:58Z",
    )
    id_frame = api.dispatch(
        "GET",
        "/api/v1/replay/frames/2026-07-17T182958Z",
    )
    assert query_frame.payload["artifact_sha256"] == id_frame.payload["artifact_sha256"]
    assert ("ETag", f'"{query_frame.payload["artifact_sha256"]}"') in query_frame.headers
    assert ("Cache-Control", "private, no-cache") in query_frame.headers


@pytest.mark.parametrize(
    ("target", "error"),
    [
        (
            "/api/v1/replay/sessions/2026-07-17/timeline?step_minutes=1",
            "unsupported_step_minutes",
        ),
        (
            "/api/v1/replay/sessions/2026-07-17/frame?at=2026-07-17T18:30:01.001Z",
            "replay_at_subsecond_not_supported",
        ),
        (
            "/api/v1/replay/sessions/2026-07-16/frame?at=2026-07-17T18:30:00Z",
            "replay_at_session_mismatch",
        ),
        ("/api/v1/replay/sessions/../../etc/passwd/timeline", "route_not_found"),
    ],
)
def test_api_rejects_unsupported_or_unsafe_requests(
    catalog: ReplayCatalog,
    target: str,
    error: str,
) -> None:
    with pytest.raises(ReplayRequestError, match=error):
        ReplayAPI(catalog).dispatch("GET", target)


def test_http_server_returns_stable_redacted_source_error(
    catalog: ReplayCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        catalog,
        "frame",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ReplaySourceError("/secret/path")),
    )
    server = ReplayHTTPServer(("127.0.0.1", 0), ReplayAPI(catalog))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = socket.create_connection(server.server_address, timeout=2)
        with client:
            client.sendall(
                b"GET /api/v1/replay/frames/2026-07-17T182958Z HTTP/1.1\r\n"
                b"Host: localhost\r\nConnection: close\r\n\r\n"
            )
            response = b""
            while chunk := client.recv(65536):
                response += chunk
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert b"422 Unprocessable Entity" in response
    assert b'"error":"replay_frame_source_rejected"' in response
    assert b"/secret/path" not in response


def test_frame_etag_revalidation_occurs_after_current_source_validation(
    catalog: ReplayCatalog,
) -> None:
    server = ReplayHTTPServer(("127.0.0.1", 0), ReplayAPI(catalog))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    target = "/api/v1/replay/frames/2026-07-17T182958Z"
    try:
        status, headers, body = _http_get(server, target)
        assert status == 200
        assert body
        first_etag = headers["etag"]
        assert headers["cache-control"] == "private, no-cache"

        status, headers, body = _http_get(
            server,
            target,
            headers=(("If-None-Match", first_etag),),
        )
        assert status == 304
        assert body == b""
        assert headers["etag"] == first_etag
        assert headers["cache-control"] == "private, no-cache"

        source_paths, _fingerprint = catalog._frame_source_context(EVENT_AS_OF)
        source = source_paths[0]
        replacement = source.with_name("quotes.replacement.parquet")
        connection = duckdb.connect()
        try:
            connection.execute(
                "CREATE TABLE replay_source AS SELECT * FROM read_parquet(?)",
                [str(source)],
            )
            connection.execute(
                "UPDATE replay_source SET writer_version = 'test-writer-v2'"
            )
            connection.execute(
                "COPY replay_source TO ? (FORMAT PARQUET)",
                [str(replacement)],
            )
        finally:
            connection.close()
        replacement.replace(source)

        status, headers, body = _http_get(
            server,
            target,
            headers=(("If-None-Match", first_etag),),
        )
        assert status == 200
        assert body
        assert headers["etag"] != first_etag
        assert headers["cache-control"] == "private, no-cache"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_unix_http_server_sets_mode_serves_health_and_cleans_up(
    catalog: ReplayCatalog,
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "runtime" / "replay-api.sock"
    server = ReplayUnixHTTPServer(socket_path, ReplayAPI(catalog), mode=0o660)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        assert stat.S_ISSOCK(socket_path.lstat().st_mode)
        assert stat.S_IMODE(socket_path.lstat().st_mode) == 0o660
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(2)
        client.connect(str(socket_path))
        with client:
            client.sendall(
                b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            response = b""
            while chunk := client.recv(65536):
                response += chunk
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert b"200 OK" in response
    assert b'"service":"spxw-surface-replay"' in response
    assert not socket_path.exists()


def test_unix_http_server_refuses_to_replace_regular_file(
    catalog: ReplayCatalog,
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "replay-api.sock"
    socket_path.write_text("keep", encoding="utf-8")

    with pytest.raises(OSError, match="non-socket"):
        ReplayUnixHTTPServer(socket_path, ReplayAPI(catalog))

    assert socket_path.read_text(encoding="utf-8") == "keep"


def test_cli_accepts_unix_socket() -> None:
    args = parse_args(["--unix-socket", "/tmp/replay-api.sock", "--unix-socket-mode", "0600"])

    assert args.unix_socket == Path("/tmp/replay-api.sock")
    assert args.unix_socket_mode == "0600"


def test_catalog_hides_session_until_close_grace_has_elapsed(tmp_path: Path) -> None:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    before_grace = ReplayCatalog(
        data_root=settings.data_root,
        storage_settings=settings,
        clock=lambda: datetime(2026, 7, 17, 21, 59, 59, tzinfo=timezone.utc),
    )

    assert before_grace.discover_sessions() == ()
    assert before_grace.sessions_payload()["sessions"] == []
    with pytest.raises(ReplayRequestError, match="replay_session_not_found"):
        before_grace.get_session(AS_OF.date())

    after_grace = ReplayCatalog(
        data_root=settings.data_root,
        storage_settings=settings,
        clock=lambda: datetime(2026, 7, 17, 22, 0, tzinfo=timezone.utc),
    )
    assert [item.session_date for item in after_grace.discover_sessions()] == [AS_OF.date()]


def test_http_workers_are_joined_on_shutdown() -> None:
    assert ReplayHTTPServer.daemon_threads is False
    assert ReplayUnixHTTPServer.daemon_threads is False
