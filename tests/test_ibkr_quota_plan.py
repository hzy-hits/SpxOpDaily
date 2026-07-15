from __future__ import annotations

from spx_spark.ibkr.stream.quota_plan import IbkrQuotaMode, plan_ibkr_option_allocation


def test_validation_allocation_keeps_twenty_six_line_reserve() -> None:
    plan = plan_ibkr_option_allocation(fallback=False)
    assert plan.mode is IbkrQuotaMode.VALIDATION
    assert (plan.hot_option_lines, plan.rotation_option_lines) == (44, 20)
    assert plan.option_lines == 64
    assert plan.reserve_lines == 26


def test_fallback_allocation_expands_spxw_and_keeps_six_line_reserve() -> None:
    plan = plan_ibkr_option_allocation(fallback=True)
    assert plan.mode is IbkrQuotaMode.FALLBACK
    assert (plan.hot_option_lines, plan.rotation_option_lines) == (56, 28)
    assert plan.option_lines == 84
    assert plan.reserve_lines == 6


def test_small_discovered_capacity_preserves_pair_atomicity() -> None:
    plan = plan_ibkr_option_allocation(discovered_capacity=40, fallback=True)
    assert plan.hot_option_lines % 2 == 0
    assert plan.rotation_option_lines % 2 == 0
    assert plan.reserve_lines >= 6
