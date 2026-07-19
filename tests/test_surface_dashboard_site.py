from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site" / "spxw-surface"


def read(relative: str) -> str:
    return (SITE / relative).read_text(encoding="utf-8")


def test_site_exposes_only_live_and_one_exact_replay_api() -> None:
    nginx = read("nginx.conf")

    assert "location = /api/v1/snapshot" in nginx
    assert "location = /api/v1/replays/2026-07-17T183500Z" in nginx
    assert (
        "alias /usr/share/nginx/data/replays/2026-07-17T183500Z.json;"
        in nginx
    )
    assert "location ^~ /api/" in nginx
    assert "location ^~ /data/" in nginx
    assert nginx.count("limit_except GET") >= 2
    assert "autoindex on" not in nginx


def test_frontend_keeps_live_and_replay_clock_contracts_separate() -> None:
    app = read("public/app.js")
    page = read("public/index.html")

    assert 'url: "api/v1/replays/2026-07-17T183500Z"' in app
    assert "function normalizeSnapshot(raw)" in app
    assert "function normalizeReplaySnapshot(raw)" in app
    assert 'raw.kind) !== "spxw_surface_dashboard_replay"' in app
    assert 'raw.mode !== "replay"' in app
    assert "replay_must_not_have_valid_until" in app
    assert "replay_must_not_have_live_created_at" in app
    assert "source.cutoff_fields.some" in app
    assert "received_at_and_available_source_clocks_lte_requested_as_of" in app
    assert "source.lookahead_rows_selected !== 0" in app
    assert "source.replay_loader_field_stitching !== false" in app
    assert "source.source_clock_rows_excluded !== 0" in app
    assert "source.ambiguous_top_instrument_count !== 0" in app
    assert "source.source_files_verified_unchanged_during_read !== true" in app
    assert 'crypto.subtle.digest("SHA-256"' in app
    assert "await verifyReplayDigests(raw)" in app
    assert "replay_projection_policy_hash_mismatch" in app
    assert "replay_artifact_hash_mismatch" in app
    assert "finiteNumber(coverage.usable_ratio)" in app
    assert "isObject(expiry?.raw?.coverage)" in app
    assert "AbortController" in app
    assert "requestGeneration" in app
    assert 'if (app.mode !== "live") return;' in app
    assert "window.setTimeout(refreshSnapshot, POLL_INTERVAL_MS)" in app
    assert "HISTORICAL REPLAY" in page
    assert "Frozen" in page
    assert "Not live" in page
    assert '<option value="live">' in page
    assert '<option value="friday">' in page


def test_memorable_entry_is_redirect_only_and_loopback_bound() -> None:
    entry = read("entry-nginx.conf")
    compose = read("compose.yaml")

    assert "listen 18084 default_server" in entry
    assert "return 302 https://code.zh3nyu.com/proxy/18082/;" in entry
    assert (
        "return 302 https://code.zh3nyu.com/proxy/18082/?view=friday;"
        in entry
    )
    assert "location /" in entry and "return 404;" in entry
    assert '"127.0.0.1:18084:18084"' in compose
    assert "no-new-privileges:true" in compose
    assert "cap_drop:" in compose
    assert "read_only: true" in compose
