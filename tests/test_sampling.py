from spx_spark.config import SamplingSettings
from spx_spark.sampling import build_sampling_plan, build_strikes, round_to_step, split_groups


def make_settings() -> SamplingSettings:
    return SamplingSettings(
        strike_step=5,
        window_points=200,
        hot_window_points=50,
        group_count=4,
        group_interval_seconds=4,
        degraded_group_count=20,
        degraded_group_interval_seconds=3,
        group_strategy="interleaved",
        hot_human_cadence_seconds=8,
        hot_execution_cadence_seconds=2,
        include_next_expiry=True,
        default_mode="human_alert",
    )


def test_build_strikes_for_7500_window():
    strikes = build_strikes(7500, 200, 5)

    assert strikes[0] == 7300
    assert strikes[-1] == 7700
    assert len(strikes) == 81


def test_split_groups_keeps_all_strikes():
    strikes = build_strikes(7500, 200, 5)
    groups = split_groups(strikes, 4)

    assert len(groups) == 4
    assert sum(len(group) for group in groups) == 81
    assert groups[0][0] == 7300
    assert groups[0][1] == 7320
    assert groups[0][-1] == 7700
    assert groups[1][0] == 7305
    assert groups[-1][-1] == 7695


def test_sampling_plan_counts_0dte_and_1dte():
    plan = build_sampling_plan(
        underlier_price=7501.2,
        expiry="20260706",
        next_expiry="20260707",
        mode="human_alert",
        settings=make_settings(),
    )

    assert round_to_step(7501.2, 5) == 7500
    assert plan.atm_strike == 7500
    assert len(plan.expiries) == 2
    # 0DTE keeps the full windows; 1DTE is trimmed to the ATM vicinity
    # (hot: ATM +/- 10 -> 5 strikes, rolling: ATM +/- 30 -> 13 strikes).
    hot_0dte = [spec for spec in plan.hot_lane if spec.expiry == "20260706"]
    hot_1dte = [spec for spec in plan.hot_lane if spec.expiry == "20260707"]
    assert len(hot_0dte) == 42
    assert len(hot_1dte) == 10
    assert plan.hot_contract_count == 52
    rolling_1dte = [
        spec
        for group in plan.rolling_groups
        for spec in group.contracts
        if spec.expiry == "20260707"
    ]
    assert len(rolling_1dte) == 26
    assert plan.rolling_contract_count == 162 + 26
    assert all(abs(spec.strike - 7500) <= 10 for spec in hot_1dte)
    assert all(abs(spec.strike - 7500) <= 30 for spec in rolling_1dte)
    assert len(plan.rolling_groups) == 4
    assert plan.full_scan_seconds == 16


def test_degraded_sampling_plan_uses_twenty_groups():
    plan = build_sampling_plan(
        underlier_price=7500,
        expiry="20260706",
        next_expiry="20260707",
        mode="degraded",
        settings=make_settings(),
    )

    assert len(plan.rolling_groups) == 20
    assert plan.full_scan_seconds == 60
