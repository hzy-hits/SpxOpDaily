from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.schwab.market_data_plan import (
    CadencePolicy,
    collection_profile,
    planner_tick_seconds,
    planned_requests_per_minute,
)
from spx_spark.schwab.request_models import CollectionProfile


def test_profiles_cover_open_normal_burst_and_off_hours() -> None:
    open_window = datetime(2026, 7, 13, 13, 35, tzinfo=timezone.utc)
    midday = datetime(2026, 7, 13, 16, 0, tzinfo=timezone.utc)
    gth = datetime(2026, 7, 13, 2, 0, tzinfo=timezone.utc)
    overnight = datetime(2026, 7, 12, 23, 0, tzinfo=timezone.utc)

    assert collection_profile(open_window) is CollectionProfile.ACTIVE
    assert collection_profile(midday) is CollectionProfile.NORMAL
    assert collection_profile(midday, burst=True) is CollectionProfile.BURST
    assert collection_profile(gth) is CollectionProfile.GTH
    assert collection_profile(overnight) is CollectionProfile.OFF_HOURS
    policy = CadencePolicy()
    assert planned_requests_per_minute(CollectionProfile.OFF_HOURS, policy) == 10
    assert planned_requests_per_minute(CollectionProfile.GTH, policy) == 13
    assert planned_requests_per_minute(CollectionProfile.NORMAL, policy) == 64
    assert planned_requests_per_minute(CollectionProfile.ACTIVE, policy) == 78
    assert planned_requests_per_minute(CollectionProfile.BURST, policy) == 84
    assert planner_tick_seconds(CollectionProfile.OFF_HOURS, policy) == 5.0
    assert planner_tick_seconds(CollectionProfile.GTH, policy) == 5.0
    assert planner_tick_seconds(CollectionProfile.NORMAL, policy) == 2.0 / 3.0
    assert planner_tick_seconds(CollectionProfile.ACTIVE, policy) == 0.5
