from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

from spx_spark.schwab.chain_lane import LaneDisposition, plan_chain_lanes
from spx_spark.schwab.collector_state import CollectorBudgetState
from spx_spark.schwab.market_data_plan import CadencePolicy
from spx_spark.schwab.request_models import CollectionProfile, QuotaMode


NOW = datetime(2026, 7, 13, 16, 0, tzinfo=timezone.utc)


def typed_settings() -> SimpleNamespace:
    return SimpleNamespace(
        cadence=CadencePolicy(),
        wide_chain=SimpleNamespace(
            strike_count_candidates=(80, 100, 120),
            next_expiry_strike_count=40,
        ),
        research_chain=SimpleNamespace(
            enabled=False,
            spx_target_dtes=(7, 30),
            spx_strike_count=10,
            bond_underliers=("TLT", "IEF"),
            bond_target_dte=30,
            bond_strike_count=20,
        ),
    )


def test_planner_grants_by_priority_until_request_budget_is_exhausted() -> None:
    plans = plan_chain_lanes(
        chain_symbols=["SPX", "SPY"],
        current_expiry=date(2026, 7, 13),
        next_expiry=date(2026, 7, 14),
        now=NOW,
        profile=CollectionProfile.NORMAL,
        quota_mode=QuotaMode.NORMAL,
        budget_state=CollectorBudgetState(),
        settings=SimpleNamespace(option_chain_strike_count=10),
        typed_settings=typed_settings(),
        available_requests=1,
    )

    assert [(plan.lane_key, plan.disposition) for plan in plans] == [
        ("SPX:front", LaneDisposition.READY),
        ("SPY:front", LaneDisposition.BUDGET_BLOCKED),
        ("SPX:next", LaneDisposition.BUDGET_BLOCKED),
    ]


def test_planner_pressure_blocks_low_priority_confirmation_lane() -> None:
    plans = plan_chain_lanes(
        chain_symbols=["SPX", "QQQ"],
        current_expiry=date(2026, 7, 13),
        next_expiry=date(2026, 7, 14),
        now=NOW,
        profile=CollectionProfile.NORMAL,
        quota_mode=QuotaMode.PRESSURE,
        budget_state=CollectorBudgetState(),
        settings=SimpleNamespace(option_chain_strike_count=10),
        typed_settings=typed_settings(),
        available_requests=3,
    )

    by_key = {plan.lane_key: plan.disposition for plan in plans}
    assert by_key == {
        "SPX:front": LaneDisposition.READY,
        "QQQ:front": LaneDisposition.QUOTA_BLOCKED,
        "SPX:next": LaneDisposition.READY,
    }


def test_next_expiry_uses_half_front_baseline_width() -> None:
    plans = plan_chain_lanes(
        chain_symbols=["SPX"],
        current_expiry=date(2026, 7, 13),
        next_expiry=date(2026, 7, 14),
        now=NOW,
        profile=CollectionProfile.NORMAL,
        quota_mode=QuotaMode.NORMAL,
        budget_state=CollectorBudgetState(),
        settings=SimpleNamespace(option_chain_strike_count=10),
        typed_settings=typed_settings(),
        available_requests=2,
    )

    by_key = {plan.lane_key: plan.strike_count for plan in plans}
    assert by_key == {"SPX:front": 80, "SPX:next": 40}


def test_normal_profile_adds_bounded_raw_only_research_chains() -> None:
    settings = typed_settings()
    settings.research_chain.enabled = True
    plans = plan_chain_lanes(
        chain_symbols=["SPX"],
        current_expiry=date(2026, 7, 13),
        next_expiry=date(2026, 7, 14),
        now=NOW,
        profile=CollectionProfile.NORMAL,
        quota_mode=QuotaMode.NORMAL,
        budget_state=CollectorBudgetState(),
        settings=SimpleNamespace(option_chain_strike_count=10),
        typed_settings=settings,
        available_requests=6,
    )

    research = {plan.lane_key: plan for plan in plans if plan.raw_only}
    assert {key: (plan.expiry, plan.strike_count) for key, plan in research.items()} == {
        "SPX:research_7d": (date(2026, 7, 20), 10),
        "SPX:research_30d": (date(2026, 8, 12), 10),
        "TLT:research_30d": (date(2026, 8, 21), 20),
        "IEF:research_30d": (date(2026, 8, 21), 20),
    }
    assert all(plan.disposition is LaneDisposition.READY for plan in research.values())


def test_research_chains_skip_active_and_quota_pressure_profiles() -> None:
    settings = typed_settings()
    settings.research_chain.enabled = True
    active = plan_chain_lanes(
        chain_symbols=["SPX"],
        current_expiry=date(2026, 7, 13),
        next_expiry=date(2026, 7, 14),
        now=NOW,
        profile=CollectionProfile.ACTIVE,
        quota_mode=QuotaMode.NORMAL,
        budget_state=CollectorBudgetState(),
        settings=SimpleNamespace(option_chain_strike_count=10),
        typed_settings=settings,
        available_requests=8,
    )
    pressure = plan_chain_lanes(
        chain_symbols=["SPX"],
        current_expiry=date(2026, 7, 13),
        next_expiry=date(2026, 7, 14),
        now=NOW,
        profile=CollectionProfile.NORMAL,
        quota_mode=QuotaMode.PRESSURE,
        budget_state=CollectorBudgetState(),
        settings=SimpleNamespace(option_chain_strike_count=10),
        typed_settings=settings,
        available_requests=8,
    )

    assert not any(plan.raw_only for plan in active)
    assert all(
        plan.disposition is LaneDisposition.QUOTA_BLOCKED
        for plan in pressure
        if plan.raw_only
    )
