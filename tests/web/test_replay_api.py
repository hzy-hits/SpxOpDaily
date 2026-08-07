from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from spx_spark.web.replay_api import create_app


HASH = "a" * 64


class Catalog:
    frame_minutes = 5

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def sessions_payload(self) -> dict[str, object]:
        return {"kind": "sessions", "sessions": []}

    def timeline_payload(self, session_date: date) -> dict[str, object]:
        return {"kind": "timeline", "session": session_date.isoformat()}

    def frame(self, session_date: date, requested: datetime) -> dict[str, object]:
        return {"kind": "frame", "session": session_date.isoformat(),
                "at": requested.isoformat(), "artifact_sha256": HASH}

    def trend(self, session_date: date, **selector: str) -> dict[str, object]:
        return {"kind": "trend", "session": session_date.isoformat(),
                "selector": selector, "artifact_sha256": HASH}

    def session_surface(self, session_date: date, **selector: object) -> dict[str, object]:
        return {"kind": "surface", "session": session_date.isoformat(),
                "selector": {key: str(value) for key, value in selector.items()},
                "artifact_sha256": HASH}


def _client(tmp_path: Path) -> TestClient:
    return TestClient(create_app(Catalog(tmp_path)), raise_server_exceptions=False)


def test_all_replay_routes_keep_paths_status_and_payloads(tmp_path: Path) -> None:
    client = _client(tmp_path)
    requests = (
        ("/healthz", "spxw-surface-replay"),
        ("/api/v1/replay/sessions", "sessions"),
        ("/api/v1/replay/sessions/2026-07-17/timeline?step_minutes=5", "timeline"),
        ("/api/v1/replay/sessions/2026-07-17/trend?role=front&weighting=oi_weighted&metric=signed_gamma", "trend"),
        ("/api/v1/replay/sessions/2026-07-17/session-surface?at=2026-07-17T18%3A29%3A58Z&role=front&weighting=oi_weighted&bucket_minutes=5&price_step=5", "surface"),
        ("/api/v1/replay/sessions/2026-07-17/frame?at=2026-07-17T18%3A29%3A58Z", "frame"),
        ("/api/v1/replay/frames/2026-07-17T182958Z", "frame"),
    )
    for target, marker in requests:
        response = client.get(target)
        assert response.status_code == 200
        assert marker in response.text
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["referrer-policy"] == "no-referrer"
        assert response.headers["content-security-policy"] == "default-src 'none'"


def test_head_etag_revalidation_and_method_contract(tmp_path: Path) -> None:
    client = _client(tmp_path)
    target = "/api/v1/replay/frames/2026-07-17T182958Z"
    first = client.get(target)
    assert first.headers["etag"] == f'"{HASH}"'
    assert first.headers["cache-control"] == "private, no-cache"

    head = client.head(target)
    assert head.status_code == 200 and head.content == b""
    cached = client.get(target, headers={"If-None-Match": f'W/"{HASH}"'})
    assert cached.status_code == 304 and cached.content == b""
    assert cached.headers["etag"] == f'"{HASH}"'

    rejected = client.post(target)
    assert rejected.status_code == 405
    assert rejected.json() == {"error": "method_not_allowed"}
    assert rejected.headers["allow"] == "GET, HEAD"


def test_invalid_queries_retain_stable_public_errors(tmp_path: Path) -> None:
    client = _client(tmp_path)
    assert client.get("/openapi.json").status_code == 404
    assert client.get(
        "/api/v1/replay/sessions/2026-07-17/timeline?step_minutes=1"
    ).json() == {"error": "unsupported_step_minutes"}
    assert client.get(
        "/api/v1/replay/sessions/2026-07-17/frame?at=2026-07-17T18%3A30%3A01.001Z"
    ).json() == {"error": "replay_at_subsecond_not_supported"}
    duplicate = client.get(
        "/api/v1/replay/sessions?x=1&x=2"
    )
    assert duplicate.status_code == 400
    assert duplicate.json() == {"error": "invalid_query"}
