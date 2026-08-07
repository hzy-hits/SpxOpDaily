import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from spx_spark.analytics.options.strategy_payoff import (
    butterfly_economics,
    butterfly_payoff,
    conservative_butterfly_bbo,
    conservative_vertical_bbo,
    vertical_economics,
    vertical_payoff,
)
from spx_spark.application.order_map.strategy_regime import assess_regime
from spx_spark.application.order_map.strategy_select import build_strategy_decision
from spx_spark.data_platform.research.strategy_decision_replay import (
    build_vertical_replay_report,
    classify_gth_vertical_record,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings
from spx_spark.storage import LatestState


@given(
    width=st.floats(min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    debit_fraction=st.floats(
        min_value=0.01, max_value=0.99, allow_nan=False, allow_infinity=False
    ),
    long_strike=st.floats(
        min_value=100.0, max_value=10_000.0, allow_nan=False, allow_infinity=False
    ),
)
def test_vertical_risk_is_bounded_and_sums_to_width(
    width: float, debit_fraction: float, long_strike: float
) -> None:
    debit = width * debit_fraction
    economics = vertical_economics(
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    )

    assert economics["max_loss_points"] + economics["max_gain_points"] == pytest.approx(width)
    assert vertical_payoff(
        economics["breakeven_spx"],
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    ) == pytest.approx(0.0, abs=1e-8)
    assert vertical_payoff(
        long_strike - width,
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    ) == pytest.approx(-debit)
    call_low = vertical_payoff(
        long_strike - width / 2,
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    )
    call_mid_left = vertical_payoff(
        long_strike + width * 0.2,
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    )
    call_mid_right = vertical_payoff(
        long_strike + width * 0.3,
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    )
    call_high = vertical_payoff(
        long_strike + width * 1.5,
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    )
    assert call_low == pytest.approx(-debit)
    assert call_mid_right - call_mid_left == pytest.approx(width * 0.1)
    assert call_high == pytest.approx(width - debit)

    put = vertical_economics(
        long_strike=long_strike + width,
        short_strike=long_strike,
        net_debit=debit,
        right="P",
    )
    assert put["max_loss_points"] + put["max_gain_points"] == pytest.approx(width)
    assert vertical_payoff(
        put["breakeven_spx"],
        long_strike=long_strike + width,
        short_strike=long_strike,
        net_debit=debit,
        right="P",
    ) == pytest.approx(0.0, abs=1e-8)


def test_conservative_vertical_bbo_uses_ask_minus_bid_and_rejects_stale_quotes() -> None:
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    long_leg = {
        "bid": 10.0,
        "ask": 10.2,
        "provider": "schwab",
        "source_at": (now - timedelta(seconds=2)).isoformat(),
    }
    short_leg = {
        "bid": 4.0,
        "ask": 4.2,
        "provider": "schwab",
        "source_at": (now - timedelta(seconds=1)).isoformat(),
    }

    bbo = conservative_vertical_bbo(long_leg, short_leg, now=now)
    assert bbo["ask"] == pytest.approx(6.2)
    assert bbo["bid"] == pytest.approx(5.8)

    long_leg["source_at"] = (now - timedelta(seconds=16)).isoformat()
    assert conservative_vertical_bbo(long_leg, short_leg, now=now) == {
        "status": "unavailable",
        "reasons": ["spread_leg_quote_stale", "spread_leg_time_skew_exceeded"],
    }


def test_conservative_vertical_bbo_accepts_zero_bid_without_using_mid() -> None:
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    long_leg = {
        "bid": 0.0,
        "ask": 0.2,
        "provider": "schwab",
        "source_at": now.isoformat(),
    }
    short_leg = {
        "bid": 0.0,
        "ask": 0.1,
        "provider": "schwab",
        "source_at": now.isoformat(),
    }

    assert conservative_vertical_bbo(long_leg, short_leg, now=now)["ask"] == 0.2


@given(
    center=st.floats(min_value=100, max_value=10_000, allow_nan=False, allow_infinity=False),
    width=st.floats(min_value=1, max_value=100, allow_nan=False, allow_infinity=False),
    fraction=st.floats(min_value=0.01, max_value=0.99, allow_nan=False, allow_infinity=False),
)
def test_butterfly_payoff_is_bounded_with_two_breakevens(
    center: float, width: float, fraction: float
) -> None:
    debit = width * fraction
    economics = butterfly_economics(center=center, width=width, net_debit=debit)
    assert economics["max_loss_points"] + economics["max_gain_points"] == pytest.approx(width)
    assert butterfly_payoff(center, center=center, width=width, net_debit=debit) == pytest.approx(width - debit)
    for breakeven in (economics["breakeven_low"], economics["breakeven_high"]):
        assert butterfly_payoff(
            breakeven, center=center, width=width, net_debit=debit
        ) == pytest.approx(0, abs=1e-8)


def test_conservative_butterfly_bbo_uses_three_leg_nbbo_and_rejects_mid_only() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    legs = [
        {"bid": 15.1, "ask": 15.3, "mid": 15.2},
        {"bid": 7.3, "ask": 7.5, "mid": 7.4},
        {"bid": 2.5, "ask": 2.6, "mid": 2.55},
    ]
    for leg in legs:
        leg.update(provider="schwab", source_at=(now - timedelta(seconds=1)).isoformat())
    assert conservative_butterfly_bbo(*legs, now=now)["bid"] == pytest.approx(2.6)
    assert conservative_butterfly_bbo(*legs, now=now)["ask"] == pytest.approx(3.3)
    assert conservative_butterfly_bbo(
        *({"mid": leg["mid"], "provider": "schwab", "source_at": leg["source_at"]} for leg in legs),
        now=now,
    )["status"] == "unavailable"


def test_frozen_pin_cases_migrate_on_aug5_and_rank_7710_on_aug6() -> None:
    aug5 = assess_regime(_frozen_pin_facts("2026-08-05"))
    aug6 = assess_regime(_frozen_pin_facts("2026-08-06"))
    assert aug5["terminal_state"] == "PIN_MIGRATING"
    assert aug6["terminal_state"] == "PIN_STABLE"
    assert [row["center"] for row in aug6["pin"]["top_centers"]][:1] == [7710.0]


def test_stable_pin_produces_manual_7710_call_butterfly() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    decision = build_strategy_decision(_pin_payload(now), _pin_state(now), now)
    assert decision["decision_type"] == "CALL_BUTTERFLY", decision["why_not"]
    assert decision["candidate"]["center"] == 7710.0
    assert decision["candidate"]["width"] == 10.0
    assert decision["execution"]["limit"] == pytest.approx(3.3)
    assert decision["automatic_ordering"] is False


def test_stable_pin_builds_candidate_specific_terminal_range_probability(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    for offset in range(1, 31):
        day = (now.date() - timedelta(days=offset)).isoformat()
        path = tmp_path / "features" / "spx_standardized_samples" / f"date={day}" / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            {"status": "selected", "minute": f"{day}T19:00:00+00:00", "selected": {"price": 7712.0}},
            {"status": "selected", "minute": f"{day}T19:05:00+00:00", "selected": {"price": 7712.0}},
        ]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    payload = _pin_payload(now)
    payload["strategy_distribution_forecast"] = {
        "quality": "unavailable",
        "q_event": {"event": None, "probability": None},
        "p_event": {"event": None, "probability": None},
    }

    decision = build_strategy_decision(
        payload,
        _pin_state(now),
        now,
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
    )

    assert decision["decision_type"] == "CALL_BUTTERFLY", decision["why_not"]
    assert decision["probability_evidence"]["method"] == "physical_terminal_range_bootstrap.v1"
    assert decision["probability_evidence"]["n_effective"] == 30.0
    assert decision["candidate"]["utility"]["conservative_lower_bound"] > 0


def test_rth_vertical_is_manual_candidate_but_late_chase_is_no_trade() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["candidate"]["setup_kind"] == "TREND_PULLBACK"
    assert decision["probability_evidence"] == {
        "q": 0.85, "p_empirical": 0.9, "p_interval_low": 0.8,
        "n_raw": 40, "n_effective": 40.0, "shrinkage_weight": 0.666667,
        "historical_sessions": ["2026-08-04", "2026-08-05"],
    }
    assert decision["candidate"]["utility"]["conservative_lower_bound"] > 0
    assert decision["execution"]["action"] == "MANUAL_LIMIT"
    assert decision["execution"]["limit"] == pytest.approx(3.0)
    assert decision["automatic_ordering"] is False
    assert datetime.fromisoformat(decision["candidate"]["opportunity_valid_until"]) == (
        now + timedelta(minutes=5)
    )

    late = deepcopy(payload)
    late["minute_market_frame"]["es"]["vwap_distance_points"] = 12.0
    late["minute_market_frame"]["es"]["return_15m_points"] = 11.0
    rejected = build_strategy_decision(late, _state(now), now)

    assert rejected["decision_type"] == "NO_TRADE"
    assert rejected["regime"]["entry_state"] == "LATE_CHASE"
    assert "direction_valid_but_entry_too_late" in rejected["why_not"]["reasons"]


def test_rth_confirmed_trigger_reuses_fresh_exact_snapshot_for_pricing_only() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    snapshot = _gth_candidate(now, "lower_rejection_call")
    snapshot.update(
        status="blocked",
        manual_action_eligible=False,
        execution_eligible=False,
        block_reasons=["spx_gth_session_required"],
    )
    payload["gth_level_manual_candidate"] = snapshot

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["candidate"]["source"] == (
        "rth_confirmed_trigger_exact_spread_snapshot"
    )
    assert decision["candidate"]["long"]["contract_id"].endswith(":7710:C")
    assert decision["candidate"]["short"]["contract_id"].endswith(":7720:C")
    assert decision["execution"]["limit"] == pytest.approx(3.0)


def test_sparse_physical_sample_shrinks_to_q_and_utility_can_still_compete() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    forecast = payload["strategy_distribution_forecast"]
    forecast["p_event"].update(probability=0.1, interval_low=0.05, n_raw=2, n_effective=0.0)
    decision = build_strategy_decision(payload, _state(now), now)
    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["probability_evidence"]["shrinkage_weight"] == 0.0
    assert decision["candidate"]["utility"]["event_probability"] == 0.85


def test_candidate_fails_closed_when_utility_and_lower_bound_are_not_positive() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    forecast = payload["strategy_distribution_forecast"]
    forecast["q_event"]["probability"] = 0.2
    forecast["p_event"].update(probability=0.2, interval_low=0.1)
    decision = build_strategy_decision(payload, _state(now), now)
    assert decision["decision_type"] == "NO_TRADE"
    assert "candidate_utility_not_positive" in decision["why_not"]["reasons"]


def test_gth_level_path_can_authorize_manual_candidate_but_trend_background_cannot() -> None:
    now = datetime(2026, 8, 7, 3, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["gth_level_manual_candidate"] = _gth_candidate(now, "upper_acceptance_call")
    payload.pop("call_skew_spread_shadow")

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["candidate"]["source"] == "gth_level_manual_candidate"
    assert decision["action_authority"] == "manual"

    payload["gth_level_manual_candidate"] = _gth_candidate(now, "trend_transition_call")
    rejected = build_strategy_decision(payload, _state(now), now)

    assert rejected["decision_type"] == "NO_TRADE"
    assert "trend_background_cannot_authorize_entry" in rejected["why_not"]["reasons"]


def test_vertical_replay_is_causal_and_compares_four_slippage_levels() -> None:
    at = "2026-08-06T10:52:25+00:00"
    decisions = [
        {
            "opportunity_id": "late",
            "session_date": "2026-08-05",
            "decision_at": at,
            "available_at": "2026-08-06T10:52:24+00:00",
            "new_action": "NO_TRADE",
            "new_reason": "direction_valid_but_entry_too_late",
            "manual_candidate_complete": True,
            "automatic_ordering": False,
        },
        {
            "opportunity_id": "good",
            "session_date": "2026-08-06",
            "decision_at": at,
            "available_at": at,
            "new_action": "TRADE",
            "new_reason": "vertical_entry_quality_passed",
            "manual_candidate_complete": True,
            "automatic_ordering": False,
        },
    ]
    opportunities = [
        _opportunity("late", (-80.0, -100.0, -120.0, -160.0)),
        _opportunity("good", (80.0, 60.0, 40.0, 0.0)),
    ]

    report = build_vertical_replay_report(
        opportunities,
        decisions,
        frozen_cases={"2026-08-05": True, "2026-08-06": True},
        minimum_sessions=2,
    )

    assert report["slippage_grid"] == [0.0, 0.05, 0.1, 0.2]
    assert len(report["legacy_vs_new"]) == 4
    assert report["late_chase_legacy_loss_usd"] == -100.0
    assert report["bootstrap_gate"]["status"] == "pass"


def test_historical_gth_record_uses_same_stop_atr_and_trend_gates() -> None:
    now = datetime(2026, 8, 6, 10, 52, 25, tzinfo=timezone.utc)
    record = {
        **_gth_candidate(now, "upper_acceptance_call"),
        "candidate_id": "gth:7730-call",
        "session_date": "2026-08-06",
        "evaluated_at": now.isoformat(),
        "decision_ask": 15.6,
        "spread_width_points": 40.0,
        "current_parity_spx": 7734.3,
        "trigger_level": 7730.0,
        "target_spx": 7770.0,
        "invalidation_spx": 7722.0,
        "automatic_ordering": False,
    }

    rejected = classify_gth_vertical_record(record, atr_5m=4.27)
    assert rejected["new_action"] == "NO_TRADE"
    assert rejected["new_reason"] == "stop_distance_outside_atr_band"

    record["path_kind"] = "trend_transition_call"
    trend_only = classify_gth_vertical_record(record, atr_5m=20.0)
    assert trend_only["new_action"] == "NO_TRADE"
    assert trend_only["new_reason"] == "trend_background_cannot_authorize_entry"


def _state(now: datetime) -> LatestState:
    observed = now - timedelta(seconds=1)
    return LatestState(created_at=observed, as_of=observed, quotes=(), best_quotes=())


def _decision_payload(now: datetime) -> dict[str, object]:
    observed = now - timedelta(seconds=1)
    long_leg = {
        "contract_id": "option:SPX:SPXW:20260807:7710:C",
        "strike": 7710.0,
        "right": "C",
        "provider": "schwab",
        "bid": 3.8,
        "ask": 4.0,
        "source_at": observed.isoformat(),
    }
    short_leg = {
        "contract_id": "option:SPX:SPXW:20260807:7720:C",
        "strike": 7720.0,
        "right": "C",
        "provider": "schwab",
        "bid": 1.0,
        "ask": 1.2,
        "source_at": observed.isoformat(),
    }
    return {
        "trading_date": "2026-08-07",
        "pricing_allowed": True,
        "underlier": {"price": 7710.0, "source": "index:SPX"},
        "minute_market_frame": {
            "as_of": observed.isoformat(),
            "quality": "ready",
            "es": {
                "price": 7735.0,
                "vwap": 7733.0,
                "vwap_distance_points": 2.0,
                "return_15m_points": 3.0,
                "return_60m_points": 8.0,
                "vwap_slope_15m_points": 0.5,
            },
            "diagnostics": {
                "rth_market_state": {
                    "D": 7.0,
                    "input_lineage": {
                        "values": {
                            "efficiency_ratio": 0.60,
                            "vwap_cross_count": 0,
                            "price_vs_vwap": "above",
                            "breadth_above_vwap": 0.70,
                        },
                        "diagnostics": {"moving_averages": {"atr_5m": 10.0}},
                    },
                }
            },
        },
        "option_structure_frame": {
            "as_of": observed.isoformat(),
            "quality": "ready",
            "l1": {"quality": "ready"},
            "structure": {
                "put_wall": 7680.0,
                "zero_gamma": 7695.0,
                "flip_zone": [7700.0, 7705.0],
                "call_wall": 7730.0,
            },
        },
        "macro_event": {"mode": "normal", "entry_allowed": True},
        "level_decision": {
            "phase": "confirmed",
            "thesis": "breakout",
            "direction": "up",
            "level_kind": "flip_high",
            "level": 7705.0,
            "event_id": "level:7705:up",
        },
        "call_skew_spread_shadow": {
            "status": "candidate",
            "candidate": {"long": long_leg, "short": short_leg},
        },
        "strategy_distribution_forecast": _probability_forecast(now, "terminal_above"),
        "candidates": [],
    }


def _gth_candidate(now: datetime, path_kind: str) -> dict[str, object]:
    observed = now - timedelta(seconds=1)
    long_id = "option:SPX:SPXW:20260807:7710:C"
    short_id = "option:SPX:SPXW:20260807:7720:C"
    return {
        "status": "manual_ready",
        "direction": "up",
        "path_kind": path_kind,
        "manual_action_eligible": True,
        "execution_eligible": False,
        "trigger_level": 7705.0,
        "current_parity_spx": 7710.0,
        "target_spx": 7730.0,
        "invalidation_spx": 7705.0,
        "valid_until": (now + timedelta(minutes=5)).isoformat(),
        "long_contract_id": long_id,
        "short_contract_id": short_id,
        "block_reasons": [],
        "exact_spread_snapshot": {
            "long": {
                "provider": "ibkr",
                "bid": 3.8,
                "ask": 4.0,
                "source_at": observed.isoformat(),
            },
            "short": {
                "provider": "ibkr",
                "bid": 1.0,
                "ask": 1.2,
                "source_at": observed.isoformat(),
            },
        },
    }


def _frozen_pin_facts(day: str) -> dict[str, object]:
    aug6 = day == "2026-08-06"
    return {
        "quality": {"status": "ready"},
        "event": {"state": "normal"},
        "minutes_to_close": 60,
        "path": {
            "direction_score": 0.0, "efficiency_ratio_30m": 0.1429 if aug6 else 0.2432,
            "vwap_crosses_30m": 3.0, "breadth_above_vwap": 0.5, "vwap_slope": 0.0,
            "price_vs_vwap": "above",
            "pin_path_spx": (
                [7710.75, 7709.62, 7712.71, 7718.41, 7715.24, 7709.41, 7712.85,
                 7712.70, 7712.85, 7713.11, 7712.75]
                if aug6 else [7741.36, 7742.71, 7741.63, 7739.13, 7738.26, 7738.47, 7738.94, 7732.72]
            ),
        },
        "value_center": (
            {"spx_15m": 7712.56, "spx_30m": 7712.69, "spx_60m": 7714.18}
            if aug6 else {"spx_15m": 7736.65, "spx_30m": 7737.36, "spx_60m": 7738.68}
        ),
        "volatility": {"vix_return_15m_pct": -0.005 if aug6 else 0.004,
                       "atm_straddle_decay_15m": 0.0448 if aug6 else -0.0123},
        "structure": {
            "q_mode": 7710.0 if aug6 else 7730.0,
            "q_local_mass_5pt": (
                {"7700": 0.0766, "7705": 0.1100, "7710": 0.3033, "7715": 0.05,
                 "7720": 0.1483, "7725": 0.1053}
                if aug6 else {"7725": 0.05, "7730": 0.521, "7735": 0.224, "7740": 0.17}
            ),
            "zero_gamma": 7709.0 if aug6 else 7740.0,
            "flip_zone": [7705.0, 7710.0] if aug6 else [7735.0, 7740.0],
            "put_wall": 7700.0 if aug6 else 7720.0,
            "call_wall": 7720.0 if aug6 else 7760.0,
        },
    }


def _pin_payload(now: datetime) -> dict[str, object]:
    observed = (now - timedelta(seconds=1)).isoformat()
    facts = _frozen_pin_facts("2026-08-06")
    return {
        "trading_date": "2026-08-06", "pricing_allowed": True,
        "underlier": {"price": 7712.94, "source": "index:SPX"},
        "minute_market_frame": {
            "as_of": observed, "quality": "ready", "es": {
                "price": 7739.5, "vwap": 7739.25, "trend_efficiency_30m": 0.1429,
                "vwap_slope_15m_points": 0.0,
                "pin_path_1m": [value + 26.56 for value in facts["path"]["pin_path_spx"]],
            },
            "volume": {"value_centers_es": {"15m": 7739.12, "30m": 7739.25, "60m": 7740.74}},
            "volatility": {"vix_return_15m_pct": -0.005},
            "diagnostics": {"rth_market_state": {"D": 0.0, "input_lineage": {
                "values": {"efficiency_ratio": 0.1429, "vwap_cross_count": 3,
                           "price_vs_vwap": "above", "breadth_above_vwap": 0.5},
                "diagnostics": {"moving_averages": {"atr_5m": 4.6}},
            }}},
        },
        "option_structure_frame": {
            "as_of": observed, "quality": "ready", "front_expiry": "20260806",
            "l1": {"quality": "ready"}, "structure": facts["structure"],
            "density": {"mode": 7710.0, "local_mass_5pt": facts["structure"]["q_local_mass_5pt"]},
            "volatility": {"atm_straddle_decay_15m": 0.0448},
        },
        "macro_event": {"mode": "normal", "entry_allowed": True},
        "strategy_distribution_forecast": _probability_forecast(now, "terminal_between"),
        "candidates": [],
    }


def _probability_forecast(now: datetime, kind: str) -> dict[str, object]:
    event = {"kind": kind, "target_at": (now + timedelta(minutes=5)).isoformat()}
    return {
        "quality": "degraded", "valid_until": (now + timedelta(minutes=5)).isoformat(),
        "q_event": {"event": event, "probability": 0.85},
        "p_event": {"event": event, "probability": 0.9, "interval_low": 0.8,
                    "n_raw": 40, "n_effective": 40.0,
                    "historical_sessions": ["2026-08-04", "2026-08-05"]},
    }


def _pin_state(now: datetime) -> LatestState:
    quotes = tuple(
        Quote(
            instrument=InstrumentId.option("SPX", expiry="20260806", strike=strike,
                                           right="C", trading_class="SPXW"),
            provider=Provider.SCHWAB, received_at=now - timedelta(seconds=1),
            quote_time=now - timedelta(seconds=1), quality=MarketDataQuality.LIVE,
            bid=bid, ask=ask,
        )
        for strike, bid, ask in ((7700, 15.1, 15.3), (7710, 7.3, 7.5), (7720, 2.5, 2.6))
    )
    return LatestState(created_at=now, as_of=now - timedelta(seconds=1), quotes=quotes, best_quotes=quotes)


def _opportunity(opportunity_id: str, pnl: tuple[float, float, float, float]) -> dict:
    return {
        "opportunity_id": opportunity_id,
        "latency_sensitivity": [
            {
                "latency_seconds": 0,
                "cost": {
                    "slippage_sensitivity": [
                        {
                            "per_leg_side_slippage_points": slippage,
                            "net_pnl_usd": net_pnl,
                        }
                        for slippage, net_pnl in zip((0.0, 0.05, 0.1, 0.2), pnl)
                    ]
                },
            }
        ],
    }
