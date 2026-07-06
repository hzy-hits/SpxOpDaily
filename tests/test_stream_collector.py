from __future__ import annotations

from spx_spark.config import SamplingSettings
from spx_spark.ibkr.stream_collector import (
    OptionSubscriptionPlan,
    ReconnectPolicy,
    StreamAction,
    build_option_subscription_plan,
    decide_after_flush,
    option_spec_label,
    should_replan,
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
