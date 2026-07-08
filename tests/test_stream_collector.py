from datetime import datetime, timezone

from spx_spark.config import SamplingSettings
from spx_spark.ibkr.adapter import snapshot_from_rows
from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.ibkr.stream_collector import (
    OptionSubscriptionPlan,
    ReconnectPolicy,
    StreamAction,
    build_option_subscription_plan,
    decide_after_flush,
    merge_cached_option_rows,
    option_spec_label,
    should_replan,
    update_option_cache,
)
from spx_spark.sampling import OptionContractSpec


def make_sampling_settings(**overrides) -> SamplingSettings:
    values = {
        "strike_step": 5,
        "window_points": 200,
        "hot_window_points": 50,
        "group_count": 4,
        "group_interval_seconds": 4,
        "degraded_group_count": 20,
        "degraded_group_interval_seconds": 3,
        "group_strategy": "interleaved",
        "hot_human_cadence_seconds": 8,
        "hot_execution_cadence_seconds": 2,
        "include_next_expiry": False,
        "default_mode": "human_alert",
    }
    values.update(overrides)
    return SamplingSettings(**values)


def test_snapshot_from_rows_can_request_provider_replace():
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    rows = [
        VerifyRow(
            label="index:SPX",
            kind="index",
            symbol="SPX",
            exchange="CBOE",
            market_data_type=1,
            market_price=7524.0,
            ticker_time=now.isoformat(),
        )
    ]

    snapshot = snapshot_from_rows(
        rows,
        received_at=now,
        stale_after_seconds=15.0,
        connected=True,
        authenticated=True,
        latency_ms=12.0,
        replace_provider_quotes=True,
    )

    assert snapshot.metadata["replace_provider_quotes"] is True


def test_option_plan_respects_line_budget_and_keeps_pairs():
    plan = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry=None,
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        max_option_lines=60,
        hot_lane_share=0.7,
    )

    assert len(plan.hot) == 42  # 60 * 0.7 = 42, already an even pair count
    assert len(plan.hot) % 2 == 0
    for rotation in plan.rotations:
        assert 0 < len(rotation) <= 18
    # Hot lane is centered on ATM.
    strikes = {spec.strike for spec in plan.hot}
    assert 7500 in strikes
    assert max(abs(strike - 7500) for strike in strikes) <= 50


def test_option_plan_rotations_exclude_hot_contracts():
    plan = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry=None,
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        max_option_lines=40,
        hot_lane_share=0.5,
    )

    hot_keys = {(spec.expiry, spec.strike, spec.right) for spec in plan.hot}
    for rotation in plan.rotations:
        for spec in rotation:
            assert (spec.expiry, spec.strike, spec.right) not in hot_keys


def test_option_plan_rotations_cover_full_window():
    plan = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry=None,
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        max_option_lines=60,
        hot_lane_share=0.7,
    )

    rotation_strikes = {spec.strike for rotation in plan.rotations for spec in rotation}
    hot_strikes = {spec.strike for spec in plan.hot}
    all_strikes = rotation_strikes | hot_strikes
    assert min(all_strikes) == 7300
    assert max(all_strikes) == 7700


def _option_row(label: str, *, subscribed: bool = True) -> VerifyRow:
    return VerifyRow(label=label, kind="option", symbol="SPX", subscribed=subscribed)


def test_option_cache_carries_rotated_strikes_across_flushes():
    cache: dict[str, tuple[float, VerifyRow]] = {}
    slice_a = [_option_row("option:SPXW:20260708:7350:P"), _option_row("option:SPXW:20260708:7350:C")]
    update_option_cache(cache, slice_a, now_monotonic=100.0, expiry="20260708")

    # Next flush: rotation moved on to another slice; 7350 must still be
    # merged so walls see the whole chain, not the live slice only.
    slice_b = [_option_row("option:SPXW:20260708:7550:P")]
    update_option_cache(cache, slice_b, now_monotonic=105.0, expiry="20260708")
    rows = merge_cached_option_rows(list(slice_b), cache, {"option:SPXW:20260708:7550:P"})
    labels = {row.label for row in rows}
    assert "option:SPXW:20260708:7350:P" in labels
    assert "option:SPXW:20260708:7350:C" in labels
    # No duplicate for the currently subscribed label.
    assert sum(1 for row in rows if row.label == "option:SPXW:20260708:7550:P") == 1


def test_option_cache_evicts_expired_and_rolled_expiry_rows():
    cache: dict[str, tuple[float, VerifyRow]] = {}
    update_option_cache(
        cache,
        [_option_row("option:SPXW:20260708:7350:P")],
        now_monotonic=0.0,
        expiry="20260708",
    )
    # Past TTL -> evicted.
    update_option_cache(cache, [], now_monotonic=901.0, expiry="20260708")
    assert not cache

    update_option_cache(
        cache,
        [_option_row("option:SPXW:20260708:7350:P")],
        now_monotonic=1000.0,
        expiry="20260708",
    )
    # Expiry rollover -> old-expiry rows dropped.
    update_option_cache(cache, [], now_monotonic=1001.0, expiry="20260709")
    assert not cache

    # Unsubscribed rows (failed subscriptions) never enter the cache.
    update_option_cache(
        cache,
        [_option_row("option:SPXW:20260709:7400:P", subscribed=False)],
        now_monotonic=1002.0,
        expiry="20260709",
    )
    assert not cache


def test_should_replan_triggers_on_drift_and_expiry_roll():
    plan = OptionSubscriptionPlan(
        atm_strike=7500,
        expiry="20260706",
        hot=(),
        rotations=(),
    )

    assert should_replan(None, 7500.0, replan_drift_points=10.0, today_expiry="20260706")
    assert not should_replan(plan, 7505.0, replan_drift_points=10.0, today_expiry="20260706")
    assert should_replan(plan, 7512.0, replan_drift_points=10.0, today_expiry="20260706")
    assert should_replan(plan, 7500.0, replan_drift_points=10.0, today_expiry="20260707")
    assert not should_replan(plan, None, replan_drift_points=10.0, today_expiry="20260707")


def test_option_spec_label_matches_snapshot_collector_format():
    spec = OptionContractSpec(expiry="20260706", strike=7500, right="C", lane="hot")

    assert option_spec_label(spec) == "option:SPXW:20260706:7500:C"


def test_decide_after_flush_priorities():
    assert (
        decide_after_flush(connected=True, allowed=True, competing_session=False)
        is StreamAction.CONTINUE
    )
    assert (
        decide_after_flush(connected=True, allowed=True, competing_session=True)
        is StreamAction.CONFLICT_WAIT
    )
    assert (
        decide_after_flush(
            connected=True,
            allowed=True,
            competing_session=False,
            gateway_restart=True,
        )
        is StreamAction.GATEWAY_RESTART
    )
    assert (
        decide_after_flush(connected=False, allowed=True, competing_session=False)
        is StreamAction.RECONNECT
    )
    assert (
        decide_after_flush(connected=True, allowed=False, competing_session=False)
        is StreamAction.POLICY_BLOCKED
    )
    # Competing session wins over disconnect: probe wait, not tight reconnect.
    assert (
        decide_after_flush(connected=False, allowed=True, competing_session=True)
        is StreamAction.CONFLICT_WAIT
    )


def test_reconnect_policy_backs_off_exponentially_and_resets():
    policy = ReconnectPolicy(min_seconds=5.0, max_seconds=60.0)

    assert policy.next_delay() == 5.0
    assert policy.next_delay() == 10.0
    assert policy.next_delay() == 20.0
    assert policy.next_delay() == 40.0
    assert policy.next_delay() == 60.0
    assert policy.next_delay() == 60.0

    policy.reset()
    assert policy.next_delay() == 5.0
