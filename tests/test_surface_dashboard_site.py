from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site" / "spxw-surface"


def read(relative: str) -> str:
    return (SITE / relative).read_text(encoding="utf-8")


def test_site_exposes_live_snapshot_archived_frame_and_read_only_replay_api() -> None:
    nginx = read("nginx.conf")

    assert "location = /api/v1/snapshot" in nginx
    assert "location = /api/v1/replays/2026-07-17T183500Z" in nginx
    assert (
        "alias /usr/share/nginx/data/replays/2026-07-17T183500Z.json;"
        in nginx
    )
    assert "location ^~ /api/v1/replay/" in nginx
    assert "proxy_pass http://unix:/usr/share/nginx/replay-runtime/replay-api.sock:;" in nginx
    assert (
        "proxy_pass http://unix:/usr/share/nginx/replay-runtime/replay-api.sock:/healthz;"
        in nginx
    )
    assert "proxy_hide_header Cache-Control;" in nginx
    assert "private, no-cache" in nginx
    assert "location = /live" in nginx
    assert "location = /replay" in nginx
    assert "location ^~ /api/" in nginx
    assert "location ^~ /data/" in nginx
    assert nginx.count("limit_except GET") >= 2
    assert "autoindex on" not in nginx


def test_frontend_keeps_live_and_replay_clock_contracts_separate() -> None:
    app = read("public/app.js")
    page = read("public/index.html")

    assert 'const REPLAY_SESSIONS_URL = "api/v1/replay/sessions"' in app
    assert 'const LIVE_SESSION_SURFACE_URL = "api/v1/live/session-surface"' in app
    assert "function normalizeReplaySessions(raw)" in app
    assert "function normalizeReplayTimeline(raw, sessionDate, expectedProjectionPolicySha256)" in app
    assert '"surface_frames"' in app
    assert 'raw.surface_provider !== "mixed"' in app
    assert "surfaceTimelineExtended" in app
    assert "app.frames = timeline.surfaceFrames" in app
    assert "app.legacyFrames = timeline.frames" in app
    assert "legacyReplayFrameIndexAtOrBefore" in app
    assert "const frame = legacyReplayFrame();" in app
    assert "legacy_replay_artifact_unavailable_at_cutoff" in app
    assert "async function loadReplayCatalog()" in app
    assert "async function loadReplayTimeline(" in app
    assert "async function loadReplayFrame()" in app
    assert 'current.replace(/\\/(?:index\\.html|live|replay|friday)' in app
    assert "item.at || item.requested_as_of" in app
    assert "item.id) || nonEmptyString(item.replay_id" in app
    assert "item.url) || nonEmptyString(item.frame_url" in app
    assert "function normalizeSnapshot(raw)" in app
    assert "function normalizeReplaySnapshot(raw" in app
    assert 'raw.kind) !== "spxw_surface_dashboard_replay"' in app
    assert 'raw.mode !== "replay"' in app
    assert "replay_must_not_have_valid_until" in app
    assert "replay_must_not_have_live_created_at" in app
    assert "source.cutoff_fields.some" in app
    assert "received_at_and_available_source_clocks_lte_requested_as_of" in app
    assert "source.lookahead_rows_selected !== 0" in app
    assert "source.replay_loader_field_stitching !== false" in app
    assert "source.source_clock_rows_excluded," in app
    assert "droppedAmbiguousCount !== ambiguousTopCount" in app
    assert "source.source_files_verified_unchanged_during_read !== true" in app
    assert 'source.point_in_time_confidence !== "bounded_not_proven"' in app
    assert "source.availability_clock_available !== false" in app
    assert 'pitFieldCount !== pitFieldNames.length' in app
    assert "legacy_replay_pit_contract" not in app
    assert 'crypto.subtle.digest("SHA-256"' in app
    assert "await verifyReplayDigests(raw, expected)" in app
    assert "replay_projection_policy_hash_mismatch" in app
    assert "replay_artifact_hash_mismatch" in app
    assert "finiteNumber(coverage.usable_ratio)" in app
    assert "isObject(expiry?.raw?.coverage)" in app
    assert "AbortController" in app
    assert "requestGeneration" in app
    assert "SESSION_SURFACE_RETRY_DELAYS_MS" in app
    assert "scheduleSessionSurfaceRetry(key)" in app
    assert 'renderSessionSurfaceChrome("unavailable", failure.reason, { retrying: true })' in app
    assert "sessionSurfaceFailureDisposition" in app
    assert "session_surface_timeout_" in app
    assert "Replay · Unavailable · Retrying" in app
    assert "Replay · Scheduled Missing" in app
    assert "Refresh the transport after releasing" in app
    assert "sessionSurfaceFrameIndexFor" in app
    assert "return Math.max(index, 0)" not in app
    assert 'if (app.mode !== "live") return;' in app
    assert "async function refreshLiveSessionSurface()" in app
    assert "LIVE_SESSION_REQUEST_TIMEOUT_MS" in app
    assert "Math.max(POLL_INTERVAL_MS - elapsed, 0)" in app
    assert "live_surface_server_time_header_mismatch" in app
    assert "historicalOnlyLiveSurface" in app
    assert "dom.scenarioDiagnostic.open = false" in app
    assert "else refreshLiveSessionSurface();" in app
    assert "if (!isReplayView()) return;" in app
    assert "HISTORICAL REPLAY" in page
    assert "Frozen" in page
    assert "Not live" in page
    assert "Bounded PIT" in page
    assert "Availability clock missing" in page
    assert 'id="replay-session-filter"' in page
    assert 'id="replay-timeline"' in page
    assert 'id="replay-play"' in page
    assert 'id="replay-previous"' in page
    assert 'id="replay-next"' in page
    assert '<option value="4">4×</option>' in page
    assert '<option value="live">' in page
    assert '<option value="replay">' in page
    assert "2026-07-17T183500Z" not in app
    assert "frame contract not yet verified" in app
    assert 'window.history[push ? "pushState" : "replaceState"]' in app
    assert '"aria-valuetext"' in app
    assert 'raw.only_close_grace_elapsed_sessions !== true' in app
    assert 'item.session_close_grace_elapsed !== true' in app
    assert 'raw.data_finalization_proven !== false' in app
    assert 'raw.frame_validation !== REPLAY_FRAME_VALIDATION' in app
    assert 'raw.availability_clock !== "unavailable"' in app
    assert 'raw.projection_policy_sha256 !== expectedProjectionPolicySha256' in app
    assert 'projectionPolicySha256 !== expectedProjectionPolicySha256' in app
    assert 'throw new Error("missing_expected_replay_projection_policy_hash")' in app
    assert "function resetReplayNavigationState()" in app
    assert "const navigationLocked = app.replayCatalogLoading" in app
    assert "app.frameLoading || !trend || gammaCount < 2" in app
    assert "app.trendLoading || app.frameLoading" in app
    assert 'cache: "no-cache"' in app
    assert "/trend?" not in app
    assert "async function normalizeSessionSurface(raw, expected = {})" in app
    assert "const surface = await normalizeSessionSurface(payload" in app
    assert "function sessionSurfaceRequestDecision(" in app
    assert 'return interrupt ? "interrupt" : "queue"' in app
    assert "session_surface_missing_column_has_values" in app
    assert "session_surface_artifact_hash_mismatch" in app
    assert "source_session_kind" in app
    assert "projected from ${sourceKind}" in app
    assert 'typeof capabilities.gth_available !== "boolean"' in app
    assert 'icon.className = `legend-candle${presentation.inferred ? " inferred" : ""}`' in app
    assert "NOT OFFICIAL SPX OHLC" in app
    assert 'id="reference-chip"' in page
    assert 'id="reference-clock"' in page
    assert 'id="provider-chip"' in page
    assert 'id="position-mode-filter"' in page
    assert "Participant (unavailable)" in page
    assert 'id="cockpit-gamma-base"' in page
    assert 'id="cockpit-strike-base"' in page
    assert 'id="cockpit-charm-base"' in page
    assert 'id="cockpit-live-placeholder"' not in page
    assert "LEGACY ROLLING SNAPSHOT" in page
    assert "verifiedReplayFrameCache" not in app
    assert "@media (max-width: 380px)" in read("public/styles.css")


def test_frontend_strictly_normalizes_compact_trend_fixture() -> None:
    subprocess.run(
        [
            "node",
            str(ROOT / "tests" / "js" / "surface_trend_contract_test.js"),
            str(SITE / "public" / "app.js"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def test_frontend_strictly_normalizes_causal_session_surface_fixture() -> None:
    subprocess.run(
        [
            "node",
            str(ROOT / "tests" / "js" / "session_surface_contract_test.js"),
            str(SITE / "public" / "app.js"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def test_frontend_strictly_normalizes_live_session_surface_and_lease() -> None:
    subprocess.run(
        [
            "node",
            str(ROOT / "tests" / "js" / "live_session_surface_contract_test.js"),
            str(SITE / "public" / "app.js"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def test_frontend_uses_extended_surface_timeline_without_relabeling_legacy_artifacts() -> None:
    subprocess.run(
        [
            "node",
            str(ROOT / "tests" / "js" / "replay_surface_timeline_contract_test.js"),
            str(SITE / "public" / "app.js"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def test_memorable_entry_is_redirect_only_and_loopback_bound() -> None:
    entry = read("entry-nginx.conf")
    compose = read("compose.yaml")

    assert "listen 18084 default_server" in entry
    assert "return 302 https://code.zh3nyu.com/proxy/18082/live;" in entry
    assert (
        "return 302 https://code.zh3nyu.com/proxy/18082/replay$is_args$args;"
        in entry
    )
    assert (
        "return 302 https://code.zh3nyu.com/proxy/18082/replay?date=2026-07-17"
        in entry
    )
    assert "location /" in entry and "return 404;" in entry
    assert '"127.0.0.1:18084:18084"' in compose
    assert ":/usr/share/nginx/replay-runtime:ro" in compose
    assert "no-new-privileges:true" in compose
    assert "cap_drop:" in compose
    assert "read_only: true" in compose
