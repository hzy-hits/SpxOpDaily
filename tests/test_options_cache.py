from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map import options_cache
from spx_spark.storage import LatestState


def test_options_snapshot_reuses_one_state_and_rebuilds_on_new_version(monkeypatch) -> None:
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())
    calls = {"group": 0, "map": 0, "exposure": 0}

    def group(*_args, **_kwargs):
        calls["group"] += 1
        return {}

    def build_map(*_args, **_kwargs):
        calls["map"] += 1
        return object()

    def build_exposure(*_args, **_kwargs):
        calls["exposure"] += 1
        return object()

    monkeypatch.setattr(options_cache, "group_spxw_option_quotes", group)
    monkeypatch.setattr(options_cache, "build_options_map", build_map)
    monkeypatch.setattr(options_cache, "build_exposure_map", build_exposure)

    first = options_cache.options_snapshot(state)
    second = options_cache.options_snapshot(state)
    assert first is second
    assert calls == {"group": 1, "map": 1, "exposure": 1}

    newer = LatestState(
        created_at=now,
        as_of=now + timedelta(seconds=5),
        quotes=(),
        best_quotes=(),
    )
    options_cache.options_snapshot(newer)
    assert calls == {"group": 2, "map": 2, "exposure": 2}
