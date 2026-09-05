from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.macro_event_calendar import (
    fetch_allowlisted_macro_events,
    refresh_macro_events_if_due,
    runtime_macro_events_path,
)
from spx_spark.macro_event_clock import macro_event_state


FF_FIXTURE = [
    {
        "title": "CPI m/m",
        "country": "USD",
        "date": "2026-08-12T08:30:00-04:00",
        "impact": "High",
    },
    {
        "title": "Core CPI m/m",
        "country": "USD",
        "date": "2026-08-12T08:30:00-04:00",
        "impact": "High",
    },
    {
        "title": "PPI m/m",
        "country": "USD",
        "date": "2026-08-13T08:30:00-04:00",
        "impact": "High",
    },
    {
        "title": "Core PPI m/m",
        "country": "USD",
        "date": "2026-08-13T08:30:00-04:00",
        "impact": "High",
    },
    {
        "title": "FOMC Member Hammack Speaks",
        "country": "USD",
        "date": "2026-08-13T08:15:00-04:00",
        "impact": "Low",
    },
    {
        "title": "Non-Farm Employment Change",
        "country": "USD",
        "date": "2026-08-14T08:30:00-04:00",
        "impact": "High",
    },
]

FOMC_FIXTURE = """
<h4>2026 FOMC Meetings</h4>
<div class="row fomc-meeting">
  <div class="fomc-meeting__month"><strong>July</strong></div>
  <div class="fomc-meeting__date">28-29</div>
</div>
<div class="row fomc-meeting">
  <div class="fomc-meeting__month"><strong>September</strong></div>
  <div class="fomc-meeting__date">15-16*</div>
</div>
<h4>2025 FOMC Meetings</h4>
<div class="row fomc-meeting">
  <div class="fomc-meeting__month"><strong>September</strong></div>
  <div class="fomc-meeting__date">16-17*</div>
</div>
"""


def test_cpi_pre_and_post_event_modes() -> None:
    pre = macro_event_state(
        datetime(2026, 7, 14, 12, 15, tzinfo=timezone.utc),
    )
    post = macro_event_state(
        datetime(2026, 7, 14, 12, 45, tzinfo=timezone.utc),
    )
    normal = macro_event_state(
        datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
    )
    assert pre["mode"] == "pre_event"
    assert pre["entry_allowed"] is False
    assert post["mode"] == "post_event"
    assert post["entry_allowed"] is False  # seed is not a coverage certificate
    assert normal["mode"] == "unavailable"


def test_missing_calendar_fails_closed(tmp_path) -> None:
    state = macro_event_state(
        datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
        path=tmp_path / "missing.toml",
    )

    assert state["mode"] == "unavailable"
    assert state["entry_allowed"] is False
    assert str(state["reason"]).startswith("macro_calendar_unavailable:")


def test_corrupt_calendar_fails_closed(tmp_path) -> None:
    path = tmp_path / "corrupt.toml"
    path.write_text("events = [", encoding="utf-8")

    state = macro_event_state(
        datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
        path=path,
    )

    assert state["mode"] == "unavailable"
    assert state["entry_allowed"] is False


def test_fetch_allowlisted_collapses_cpi_ppi_and_keeps_fomc(monkeypatch) -> None:
    def fake_get(url: str) -> bytes:
        if "ff_calendar" in url:
            return json.dumps(FF_FIXTURE).encode()
        if "fomccalendars" in url:
            return FOMC_FIXTURE.encode()
        raise AssertionError(url)

    monkeypatch.setattr(
        "spx_spark.macro_event_calendar._http_get",
        fake_get,
    )
    events = fetch_allowlisted_macro_events(
        now=datetime(2026, 8, 12, 16, 0, tzinfo=timezone.utc)
    )
    by_id = {row["id"]: row for source in events.values() for row in source["events"]}
    assert "us-cpi-2026-08-12" in by_id
    assert by_id["us-ppi-2026-08-13"]["release_at"].startswith("2026-08-13T08:30:00")
    assert by_id["us-nfp-2026-08-14"]["name"] == "US Employment Situation"
    assert by_id["us-fomc-2026-09-16"]["release_at"].startswith("2026-09-16T14:00:00")
    assert all("Speaks" not in str(row.get("description") or "") for row in by_id.values())


def test_refresh_writes_runtime_overlay_and_feeds_next_event(
    tmp_path: Path, monkeypatch
) -> None:
    def fake_get(url: str) -> bytes:
        if "ff_calendar" in url:
            return json.dumps(FF_FIXTURE).encode()
        return FOMC_FIXTURE.encode()

    monkeypatch.setattr(
        "spx_spark.macro_event_calendar._http_get",
        fake_get,
    )
    seed = tmp_path / "seed.toml"
    seed.write_text(
        'schema_version = 1\ntimezone = "America/New_York"\n'
        "[defaults]\npre_window_minutes = 30\npost_window_minutes = 90\n"
        "[[events]]\nid = \"legacy\"\nname = \"Legacy\"\n"
        'release_at = "2026-01-01T08:30:00-05:00"\nimpact = "high"\n',
        encoding="utf-8",
    )
    now = datetime(2026, 8, 12, 16, 0, tzinfo=timezone.utc)
    first = refresh_macro_events_if_due(tmp_path, now=now)
    assert first["status"] == "refreshed"
    assert runtime_macro_events_path(tmp_path).exists()
    second = refresh_macro_events_if_due(tmp_path, now=now)
    assert second["status"] == "skipped_fresh"

    state = macro_event_state(
        now,
        path=seed,
        data_root=tmp_path,
    )
    assert state["mode"] == "normal"
    assert state["next_event"]["id"] == "us-ppi-2026-08-13"


def test_partial_refresh_retains_failed_source_and_never_renews_its_coverage(tmp_path, monkeypatch):
    from spx_spark import macro_event_calendar as calendar
    now = datetime(2026, 8, 12, 16, tzinfo=timezone.utc)
    monkeypatch.setattr(calendar, "_http_get", lambda url: json.dumps(FF_FIXTURE).encode()
                        if "ff_calendar" in url else FOMC_FIXTURE.encode())
    refresh_macro_events_if_due(tmp_path, now=now)
    original = json.loads(runtime_macro_events_path(tmp_path).read_text())

    def partial(url):
        if "ff_calendar" in url:
            raise TimeoutError("offline")
        return FOMC_FIXTURE.encode()

    monkeypatch.setattr(calendar, "_http_get", partial)
    later = now + timedelta(hours=7)
    result = refresh_macro_events_if_due(tmp_path, now=later)
    saved = json.loads(runtime_macro_events_path(tmp_path).read_text())
    assert result["status"] == "partial_failure"
    assert saved["sources"]["ff_thisweek"]["events"] == original["sources"]["ff_thisweek"]["events"]
    assert saved["sources"]["ff_thisweek"]["refreshed_at"] == now.isoformat()
    assert saved["refreshed_at"] == now.isoformat()
    assert calendar.calendar_coverage(saved, now=later)["status"] == "unknown"
    assert refresh_macro_events_if_due(tmp_path, now=later)["status"] == "retry_deferred"


def test_upcoming_block_wins_over_nearer_post_event_and_empty_seed_is_unknown(tmp_path):
    seed = tmp_path / "seed.toml"
    now = datetime(2026, 8, 12, 13, tzinfo=timezone.utc)
    seed.write_text('events = []\n')
    assert macro_event_state(now, path=seed)["entry_allowed"] is False
    seed.write_text('''[[events]]
id = "released"
release_at = "2026-08-12T12:55:00+00:00"
[[events]]
id = "upcoming"
release_at = "2026-08-12T13:20:00+00:00"
''')
    state = macro_event_state(now, path=seed)
    assert state["mode"] == "pre_event"
    assert state["active_event"]["id"] == "upcoming"
    assert state["entry_allowed"] is False


def test_clock_never_performs_http_and_known_week_can_have_no_allowlisted_events(tmp_path, monkeypatch):
    from spx_spark import macro_event_calendar as calendar
    now = datetime(2026, 8, 12, 16, tzinfo=timezone.utc)
    weekly = [{"title": "Unrelated", "country": "GBP", "impact": "Low", "date": now.isoformat()}]
    monkeypatch.setattr(calendar, "_http_get", lambda url: json.dumps(weekly).encode()
                        if "ff_calendar" in url else FOMC_FIXTURE.encode())
    refresh_macro_events_if_due(tmp_path, now=now)
    def no_network(_url):
        raise AssertionError("clock tried network access")
    monkeypatch.setattr(calendar, "_http_get", no_network)
    state = macro_event_state(now, data_root=tmp_path)
    assert state["mode"] == "normal"
    assert state["entry_allowed"] is True
