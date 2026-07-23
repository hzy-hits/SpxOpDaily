from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.application.market_features import play_outcome_stats
from spx_spark.application.market_features.play_outcome_stats import (
    PlayOutcomeStatsProvider,
    load_play_outcome_stats,
)
from spx_spark.settings.market_features import MarketFeatureSettings


UTC = timezone.utc
NOW = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)


def test_loader_aggregates_touched_outcomes_within_window(tmp_path: Path) -> None:
    rows = [
        _row("level_fade_put", "call_wall", 0.05, days_ago=1),
        _row("level_fade_put", "call_wall", -0.02, days_ago=2),
        _row("level_fade_put", "call_wall", 0.10, days_ago=3),
        _row("level_breakout_call", "put_wall", 0.01, days_ago=1),
    ]
    _write_outcomes(tmp_path, "2026-07-13", rows)

    stats = load_play_outcome_stats(tmp_path, window_days=20, horizon="300", now=NOW)

    fade = stats[("level_fade_put", "call_wall")]
    assert fade.sample_count == 3
    assert fade.winrate == pytest.approx(2 / 3)
    assert fade.avg_return == pytest.approx((0.05 - 0.02 + 0.10) / 3)
    assert fade.median_return == pytest.approx(0.05)
    assert fade.window_days == 20
    assert fade.horizon == "300"
    assert fade.as_of == NOW.isoformat()
    breakout = stats[("level_breakout_call", "put_wall")]
    assert breakout.sample_count == 1
    assert breakout.winrate == pytest.approx(1.0)


def test_loader_filters_window_touched_and_horizon(tmp_path: Path) -> None:
    rows = [
        _row("level_fade_put", "call_wall", 0.05, days_ago=1),
        _row("level_fade_put", "call_wall", 0.05, days_ago=30),  # outside window
        _row("level_fade_put", "call_wall", 0.05, days_ago=-1),  # completed in future
        {**_row("level_fade_put", "call_wall", 0.05, days_ago=1), "touched": False},
        {**_row("level_fade_put", "call_wall", 0.05, days_ago=1), "horizons": {}},
        {
            **_row("level_fade_put", "call_wall", 0.05, days_ago=1),
            "horizons": {"60": {"return_fraction": 0.5}},
        },
        {**_row("level_fade_put", "call_wall", 0.05, days_ago=1), "completed_at": "not-a-date"},
        {**_row("level_fade_put", "call_wall", 0.05, days_ago=1), "play": ""},
        "{not valid json",
        "[1, 2, 3]",
    ]
    _write_outcomes(tmp_path, "2026-07-13", rows)

    stats = load_play_outcome_stats(tmp_path, window_days=20, horizon="300", now=NOW)

    assert set(stats) == {("level_fade_put", "call_wall")}
    assert stats[("level_fade_put", "call_wall")].sample_count == 1


def test_loader_includes_window_boundaries(tmp_path: Path) -> None:
    at_earliest = (NOW - timedelta(days=20)).isoformat()
    rows = [
        {**_row("level_fade_put", "call_wall", 0.05, days_ago=1), "completed_at": at_earliest},
        {**_row("level_fade_put", "call_wall", 0.05, days_ago=1), "completed_at": NOW.isoformat()},
    ]
    _write_outcomes(tmp_path, "2026-07-14", rows)

    stats = load_play_outcome_stats(tmp_path, window_days=20, horizon="300", now=NOW)

    assert stats[("level_fade_put", "call_wall")].sample_count == 2


def test_loader_semantically_deduplicates_regenerated_event_ids(tmp_path: Path) -> None:
    common = {
        "first_touch_at": "2026-07-13T14:35:00+00:00",
        "contract_id": "option:SPX:SPXW:20260713:7550:P",
    }
    rows = [
        {
            **_row("level_fade_put", "call_wall", 0.05, days_ago=1),
            **common,
            "key": "level:first|level_fade_put|contract",
        },
        {
            **_row("level_fade_put", "call_wall", 0.05, days_ago=1),
            **common,
            "key": "level:regenerated|level_fade_put|contract",
        },
    ]
    _write_outcomes(tmp_path, "2026-07-13", rows)

    stats = load_play_outcome_stats(tmp_path, window_days=20, horizon="300", now=NOW)

    assert stats[("level_fade_put", "call_wall")].sample_count == 1


def test_loader_returns_empty_when_store_missing(tmp_path: Path) -> None:
    stats = load_play_outcome_stats(tmp_path / "missing", window_days=20, horizon="300", now=NOW)

    assert stats == {}


def test_provider_returns_none_below_min_samples(tmp_path: Path) -> None:
    _write_outcomes(tmp_path, "2026-07-13", [_row("level_fade_put", "call_wall", 0.05, days_ago=1)])
    strict = PlayOutcomeStatsProvider(
        tmp_path, settings=MarketFeatureSettings(play_stats_min_samples=2)
    )

    assert strict.lookup("level_fade_put", "call_wall") is None

    permissive = PlayOutcomeStatsProvider(
        tmp_path, settings=MarketFeatureSettings(play_stats_min_samples=1)
    )
    stats = permissive.lookup("level_fade_put", "call_wall")
    assert stats is not None
    assert stats.sample_count == 1


def test_provider_caches_within_refresh_ttl(tmp_path: Path) -> None:
    rows = [_row("level_fade_put", "call_wall", 0.05, days_ago=1)]
    _write_outcomes(tmp_path, "2026-07-13", rows)
    provider = PlayOutcomeStatsProvider(
        tmp_path, settings=MarketFeatureSettings(play_stats_min_samples=1)
    )

    first = provider.lookup("level_fade_put", "call_wall")
    rows.append(_row("level_fade_put", "call_wall", 0.10, days_ago=1))
    _write_outcomes(tmp_path, "2026-07-13", rows)
    cached = provider.lookup("level_fade_put", "call_wall")

    assert first is not None and first.sample_count == 1
    assert cached is not None and cached.sample_count == 1

    refreshing = PlayOutcomeStatsProvider(
        tmp_path,
        settings=MarketFeatureSettings(play_stats_min_samples=1, play_stats_refresh_seconds=0.0),
    )
    reloaded = refreshing.lookup("level_fade_put", "call_wall")
    assert reloaded is not None and reloaded.sample_count == 2


def test_provider_reuses_cache_across_process_instances(tmp_path: Path) -> None:
    rows = [_row("level_fade_put", "call_wall", 0.05, days_ago=1)]
    _write_outcomes(tmp_path, "2026-07-13", rows)
    settings = MarketFeatureSettings(play_stats_min_samples=1)

    first = PlayOutcomeStatsProvider(tmp_path, settings=settings)
    assert first.lookup("level_fade_put", "call_wall").sample_count == 1

    rows.append(_row("level_fade_put", "call_wall", 0.10, days_ago=1))
    _write_outcomes(tmp_path, "2026-07-13", rows)
    cached = PlayOutcomeStatsProvider(tmp_path, settings=settings)

    assert cached.lookup("level_fade_put", "call_wall").sample_count == 1


def test_provider_keeps_snapshot_when_source_file_becomes_unreadable(tmp_path: Path) -> None:
    _write_outcomes(tmp_path, "2026-07-13", [_row("level_fade_put", "call_wall", 0.05, days_ago=1)])
    provider = PlayOutcomeStatsProvider(
        tmp_path,
        settings=MarketFeatureSettings(play_stats_min_samples=1, play_stats_refresh_seconds=0.0),
    )
    loaded = provider.lookup("level_fade_put", "call_wall")
    source = tmp_path / "pricing_outcomes/date=2026-07-13/outcomes.jsonl"

    source.chmod(0)
    try:
        assert provider.lookup("level_fade_put", "call_wall") == loaded
    finally:
        source.chmod(0o600)


def test_provider_keeps_snapshot_when_store_disappears(tmp_path: Path) -> None:
    _write_outcomes(
        tmp_path,
        "2026-07-13",
        [_row("level_fade_put", "call_wall", 0.05, days_ago=1)],
    )
    provider = PlayOutcomeStatsProvider(
        tmp_path,
        settings=MarketFeatureSettings(
            play_stats_min_samples=1,
            play_stats_refresh_seconds=0.0,
        ),
    )
    loaded = provider.lookup("level_fade_put", "call_wall")
    (tmp_path / "pricing_outcomes").rename(tmp_path / "pricing_outcomes.offline")

    assert provider.lookup("level_fade_put", "call_wall") == loaded


def test_provider_fail_open_keeps_cache_on_load_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_outcomes(tmp_path, "2026-07-13", [_row("level_fade_put", "call_wall", 0.05, days_ago=1)])
    provider = PlayOutcomeStatsProvider(
        tmp_path,
        settings=MarketFeatureSettings(play_stats_min_samples=1, play_stats_refresh_seconds=0.0),
    )
    loaded = provider.lookup("level_fade_put", "call_wall")
    assert loaded is not None

    def broken(*_args: object, **_kwargs: object) -> dict:
        raise OSError("disk gone")

    monkeypatch.setattr(play_outcome_stats, "load_play_outcome_stats", broken)

    assert provider.lookup("level_fade_put", "call_wall") == loaded

    fresh = PlayOutcomeStatsProvider(
        tmp_path / "missing", settings=MarketFeatureSettings(play_stats_min_samples=1)
    )
    assert fresh.lookup("level_fade_put", "call_wall") is None


def _row(play: str, level_kind: str, return_fraction: float, *, days_ago: float) -> dict:
    return {
        "schema_version": 1,
        "play": play,
        "level_kind": level_kind,
        "touched": True,
        "completed_at": (NOW - timedelta(days=days_ago)).isoformat(),
        "horizons": {"300": {"return_fraction": return_fraction}},
    }


def _write_outcomes(root: Path, day: str, rows: list[object]) -> None:
    path = root / "pricing_outcomes" / f"date={day}" / "outcomes.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [row if isinstance(row, str) else json.dumps(row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
