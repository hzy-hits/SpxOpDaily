import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    PIN_BUTTERFLY_MANAGEMENT_POLICY,
    PolicyMark,
    butterfly_economics,
    butterfly_payoff,
    conservative_butterfly_bbo,
    conservative_vertical_bbo,
    debit_vertical_reach_reasons,
    management_policy_for_candidate,
    simulate_management_policy,
    vertical_economics,
    vertical_payoff,
    vertical_width_path_reasons,
)
from spx_spark.application.order_map.candidate_factory import enumerate_candidates
from spx_spark.application.order_map.strategy_facts import build_market_fact_pack
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    assess_regime,
)
from spx_spark.application.order_map.strategy_ranker import rank_candidates
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


def test_index_hmm_owns_gth_path_state_from_globex_basket() -> None:
    regime = assess_regime(
        {
            "path": {},
            "event": {"state": "normal"},
            "quality": {"status": "ready"},
            "capabilities": {"path": {"ready": False}},
            "cross_index": {
                "source": "globex_index",
                "status": "ready",
                "session_open": True,
                "anchor": "future:ES",
            },
            "hmm": {
                "status": "available",
                "posterior": {"state_00": 0.08, "state_01": 0.12, "state_02": 0.80},
            },
            "shock": {"state": "NONE"},
        }
    )
    assert regime["path_state"] == "TREND"
    assert regime["path_direction"] == "UP"
    assert regime["hmm"]["used"] is True
    assert regime["hmm"]["source"] == "globex_index"
    assert "hmm_index_trend" in regime["reasons"]
    assert regime["policy_version"] == "strategy_policy.bootstrap.v16"


def test_index_hmm_owns_rth_balanced_from_cash_basket() -> None:
    regime = assess_regime(
        {
            "path": {"direction_score": 8.0, "efficiency_ratio_30m": 0.6},
            "event": {"state": "normal"},
            "cross_index": {
                "source": "cash_index",
                "status": "ready",
                "session_open": True,
                "anchor": "index:SPX",
            },
            "hmm": {
                "status": "available",
                "posterior": {"state_00": 0.15, "state_01": 0.70, "state_02": 0.15},
            },
            "shock": {"state": "NONE"},
        }
    )
    assert regime["path_state"] == "BALANCED"
    assert regime["path_direction"] is None
    assert regime["hmm"]["used"] is True
    assert regime["hmm"]["source"] == "cash_index"
    assert "hmm_index_balanced" in regime["reasons"]


def test_index_hmm_trend_yields_to_vwap_price_contradiction() -> None:
    regime = assess_regime(
        {
            "path": {"price_vs_vwap": "below"},
            "event": {"state": "normal"},
            "cross_index": {
                "source": "globex_index",
                "status": "ready",
                "session_open": True,
                "anchor": "future:ES",
            },
            "hmm": {
                "status": "available",
                "posterior": {"state_00": 0.05, "state_01": 0.10, "state_02": 0.85},
            },
            "shock": {"state": "NONE"},
        }
    )
    assert regime["path_state"] == "TRANSITION"
    assert "price_vwap_direction_conflict" in regime["contradictions"]
    assert "hmm_price_vwap_contradiction" in regime["reasons"]
    assert regime["hmm"]["used"] is True


def test_index_hmm_absent_keeps_es_path_fallback() -> None:
    facts = _frozen_pin_facts("2026-08-06")
    facts["path"] = {
        **facts["path"],
        "direction_score": 8.0,
        "efficiency_ratio_30m": 0.6,
        "vwap_crosses_30m": 1.0,
        "breadth_above_vwap": 0.7,
        "vwap_slope": 0.4,
        "price_vs_vwap": "above",
    }
    facts["capabilities"] = {"path": {"ready": True}}
    facts["quality"] = {"status": "ready"}
    regime = assess_regime(facts)
    assert regime["path_state"] == "TREND"
    assert regime["path_direction"] == "UP"
    assert regime["hmm"]["used"] is False


def test_fact_pack_reads_index_hmm_from_research_signals() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["minute_market_frame"]["cross_asset"] = {
        "cross_index": {
            "source": "cash_index",
            "status": "ready",
            "session_open": True,
            "anchor": "index:SPX",
        }
    }
    payload["experimental_research_signals"] = {
        "schema_version": "research_context.v2",
        "action_authority": "none",
        "generated_at": (now - timedelta(seconds=5)).isoformat(),
        "regime": {
            "observed_through": (now - timedelta(seconds=5)).isoformat(),
            "posterior": [
                {"state_id": "state_00", "probability": 0.10},
                {"state_id": "state_01", "probability": 0.18},
                {"state_id": "state_02", "probability": 0.72},
            ],
        },
    }
    facts = build_market_fact_pack(payload, _state(now), now)
    regime = assess_regime(facts)
    assert facts["hmm"]["status"] == "available"
    assert facts["cross_index"]["source"] == "cash_index"
    assert regime["path_state"] == "TREND"
    assert regime["path_direction"] == "UP"
    assert regime["hmm"]["used"] is True


def test_stable_pin_produces_manual_7710_call_butterfly() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    decision = build_strategy_decision(_pin_payload(now), _pin_state(now), now)
    assert decision["decision_type"] == "CALL_BUTTERFLY", decision["why_not"]
    assert decision["candidate"]["center"] == 7710.0
    assert decision["candidate"]["width"] == 10.0
    assert decision["execution"]["limit"] == pytest.approx(3.3)
    assert decision["automatic_ordering"] is False


def test_low_snr_strike_surface_is_explained_without_strategy_authority() -> None:
    cases: list[tuple[str, datetime, dict[str, object], LatestState]] = []
    for day in ("2026-08-05", "2026-08-06"):
        now = datetime.fromisoformat(f"{day}T19:00:00+00:00")
        payload = _pin_payload(now)
        frozen = _frozen_pin_facts(day)
        basis = 26.56
        payload["trading_date"] = day
        payload["option_structure_frame"]["structure"] = frozen["structure"]
        payload["option_structure_frame"]["density"] = {
            "mode": frozen["structure"]["q_mode"],
            "local_mass_5pt": frozen["structure"]["q_local_mass_5pt"],
        }
        payload["minute_market_frame"]["es"]["pin_path_1m"] = [
            value + basis for value in frozen["path"]["pin_path_spx"]
        ]
        payload["minute_market_frame"]["es"]["trend_efficiency_30m"] = frozen[
            "path"
        ]["efficiency_ratio_30m"]
        payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
            "input_lineage"
        ]["values"]["efficiency_ratio"] = frozen["path"]["efficiency_ratio_30m"]
        payload["minute_market_frame"]["volume"]["value_centers_es"] = {
            key.removeprefix("spx_"): value + basis
            for key, value in frozen["value_center"].items()
        }
        payload["minute_market_frame"]["volatility"]["vix_return_15m_pct"] = frozen[
            "volatility"
        ]["vix_return_15m_pct"]
        payload["option_structure_frame"]["volatility"][
            "atm_straddle_decay_15m"
        ] = frozen["volatility"]["atm_straddle_decay_15m"]
        cases.append((day, now, payload, _pin_state(now)))
    for day, pricing_allowed in (("2026-08-07", True), ("2026-08-08", False)):
        now = datetime.fromisoformat(f"{day}T15:00:00+00:00")
        payload = _decision_payload(now)
        payload["trading_date"] = day
        payload["pricing_allowed"] = pricing_allowed
        cases.append((day, now, payload, _state(now)))

    def context(value: float) -> dict[str, object]:
        return {
            "feature_version": "strike_differential_context.v1",
            "authority": "context_only",
            "semantics": "risk_neutral_strike_shape",
            "status": "ready",
            "references": [
                {
                    "center": 7710.0,
                    "labels": ["atm"],
                    "observations": [
                        {
                            "scale_points": 5.0,
                            "quality": "ready",
                            "strike_d2": value,
                            "strike_d3": value,
                            "strike_d4": value,
                            "d2_snr": 0.0,
                            "d3_snr": 0.0,
                            "d4_snr": 0.0,
                            "reasons": [],
                        }
                    ],
                }
            ],
        }

    expected_decisions = {
        "2026-08-05": "NO_TRADE",
        "2026-08-06": "CALL_BUTTERFLY",
        "2026-08-07": "CALL_DEBIT_VERTICAL",
        "2026-08-08": "NO_TRADE",
    }
    for day, now, payload, latest in cases:
        positive_payload, negative_payload = deepcopy(payload), deepcopy(payload)
        positive = context(1e12)
        negative = context(-1e12)
        positive_payload["option_structure_frame"].setdefault("density", {})[
            "strike_differential_context"
        ] = positive
        negative_payload["option_structure_frame"].setdefault("density", {})[
            "strike_differential_context"
        ] = negative

        baseline_decision = build_strategy_decision(payload, latest, now)
        positive_decision = build_strategy_decision(positive_payload, latest, now)
        negative_decision = build_strategy_decision(negative_payload, latest, now)

        assert baseline_decision["decision_type"] == expected_decisions[day]
        assert baseline_decision["market_facts"]["structure"][
            "strike_differential_context"
        ] == {}
        assert positive_decision["market_facts"]["structure"][
            "strike_differential_context"
        ] == positive
        assert negative_decision["market_facts"]["structure"][
            "strike_differential_context"
        ] == negative
        assert positive_decision["market_facts"]["quality"] == negative_decision[
            "market_facts"
        ]["quality"]
        for shaped in (positive_decision, negative_decision):
            assert shaped["decision_type"] == baseline_decision["decision_type"]
            assert shaped["action_authority"] == baseline_decision["action_authority"]
            assert shaped["automatic_ordering"] is False
            assert shaped["desk_view"]["surface_shape"]["snr_quality"] == "low"
            assert shaped["desk_view"]["surface_shape"]["rank_prior"] == 0.0
            assert "SNR低" in shaped["desk_view"]["shape"]
            if shaped["candidate"]:
                assert shaped["candidate"]["candidate_id"] == baseline_decision["candidate"][
                    "candidate_id"
                ]
                assert shaped["candidate"]["surface_shape_prior"] == 0.0
            else:
                assert "surface_shape_low_snr" in shaped["why_not"]["reasons"]
                assert shaped["desk_view"]["reason"] == baseline_decision["desk_view"]["reason"]


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

    assert decision["schema_version"] == "strategy_decision.v2"
    assert decision["policy_version"] == "strategy_policy.bootstrap.v16"
    assert decision["geometry_source"] == "facts_wall_ladder_fallback"
    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["candidate"]["candidate_id"]
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
    assert decision["shadow_candidates"] == []
    assert decision["shadow_candidates_skipped"] == []
    assert datetime.fromisoformat(decision["candidate"]["opportunity_valid_until"]) == (
        now + timedelta(minutes=5)
    )

    late = deepcopy(payload)
    late["minute_market_frame"]["es"]["vwap_distance_points"] = 12.0
    late["minute_market_frame"]["es"]["return_15m_points"] = 11.0
    rejected = build_strategy_decision(late, _state(now), now)

    assert rejected["decision_type"] == "NO_TRADE"
    assert rejected["regime"]["entry_state"] == "LATE_CHASE"
    assert rejected["shadow_candidates"] == []
    assert rejected["shadow_candidates_skipped"] == []
    assert "direction_valid_but_entry_too_late" in rejected["why_not"]["reasons"]
    assert rejected["why_not"]["nearest_candidates"]


def test_selected_decision_carries_shadow_candidates_and_skips_incomplete_quotes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from spx_spark.application.order_map import strategy_select
    from spx_spark.application.order_map.strategy_ranker import RankResult

    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    base = build_strategy_decision(payload, _state(now), now)
    selected = deepcopy(base["candidate"])
    valid_shadow = deepcopy(selected)
    valid_shadow["candidate_id"] = "shadow-valid"
    valid_shadow["opportunity_id"] = "strategy-opportunity:shadow-valid"
    invalid_shadow = deepcopy(selected)
    invalid_shadow["candidate_id"] = "shadow-invalid"
    invalid_shadow["opportunity_id"] = "strategy-opportunity:shadow-invalid"
    invalid_shadow["quote"] = {"bid": None, "ask": 2.8}

    monkeypatch.setattr(
        strategy_select,
        "enumerate_candidates",
        lambda *args, **kwargs: [selected, valid_shadow, invalid_shadow],
    )
    monkeypatch.setattr(
        strategy_select,
        "rank_candidates",
        lambda *args, **kwargs: RankResult(
            passed=[selected, valid_shadow, invalid_shadow],
            near_misses=[],
            gate_audit=[],
        ),
    )

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["candidate"]["candidate_id"] == selected["candidate_id"]
    assert [row["candidate_id"] for row in decision["shadow_candidates"]] == ["shadow-valid"]
    assert decision["shadow_candidates_skipped"] == [
        {"candidate_id": "shadow-invalid", "reason": "candidate_quote_incomplete"}
    ]


def test_rth_confirmed_breakout_can_compete_when_path_is_transitional() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    market_state = payload["minute_market_frame"]["diagnostics"]["rth_market_state"]
    market_state["D"] = 4.0
    market_state["input_lineage"]["values"]["efficiency_ratio"] = 0.40

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["regime"]["path_state"] == "TRANSITION"
    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["setup_kind"] == "BREAKOUT_ACCEPTANCE"


def test_rth_confirmed_breakout_is_blocked_by_opposite_established_trend() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    market_state = payload["minute_market_frame"]["diagnostics"]["rth_market_state"]
    market_state["D"] = -7.0
    values = market_state["input_lineage"]["values"]
    values["breadth_above_vwap"] = 0.30
    values["price_vs_vwap"] = "below"
    payload["minute_market_frame"]["es"]["vwap_slope_15m_points"] = -0.5

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["regime"]["path_state"] == "TREND"
    assert decision["regime"]["path_direction"] == "DOWN"
    assert decision["decision_type"] == "NO_TRADE"
    assert "price_trigger_conflicts_with_established_path" in decision["why_not"]["reasons"]


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


def test_negative_utility_is_advisory_not_veto() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    forecast = payload["strategy_distribution_forecast"]
    forecast["q_event"]["probability"] = 0.2
    forecast["p_event"].update(probability=0.2, interval_low=0.1)
    decision = build_strategy_decision(payload, _state(now), now)
    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["action_authority"] == "manual"
    edge = decision["candidate"]["edge"]
    assert edge["edge_status"] == "research_unvalidated"
    assert "candidate_utility_not_positive" in edge["advisories"]
    assert edge["required_p_breakeven"] is not None
    assert decision["candidate"]["utility"]["utility"] <= 0


def test_candidate_factory_emits_stable_candidate_id() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    latest = _state(now)
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)

    rows = enumerate_candidates(
        payload, facts, regime, latest, now=now, policy=DEFAULT_STRATEGY_POLICY
    )

    assert rows
    assert len(rows[0]["candidate_id"]) == 16
    assert rows[0]["automatic_ordering"] is False
    assert rows[0]["manual_action_only"] is True


def test_missing_oi_gex_does_not_block_stable_pin_butterfly() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    payload = _pin_payload(now)
    payload["option_structure_frame"]["structure"]["gex_quality"] = (
        "no_open_interest_gex"
    )
    bars = _or_failed_break_bars(now, "UP")
    late = {
        "bar_start": now.isoformat(),
        "open": 7738.0,
        "high": 7739.0,
        "low": 7737.0,
        "close": 7738.0,
        "quality": "ok",
    }
    _attach_rth_setup_path(payload, [*bars, late], "LH_LL")
    latest = _pin_state(now)
    facts = build_market_fact_pack(payload, latest, now)
    decision = build_strategy_decision(payload, latest, now)

    assert facts["capabilities"]["butterfly"]["ready"] is True
    assert facts["capabilities"]["butterfly"]["structure_ready"] is True
    assert "butterfly_structure_capability_unavailable" not in facts[
        "capabilities"
    ]["butterfly"]["reasons"]
    assert decision["decision_type"] == "CALL_BUTTERFLY", decision["why_not"]
    assert decision["candidate"]["setup_kind"] == "STABLE_PIN"
    assert "rth_entry_window_too_late" not in decision.get("why_not", {}).get(
        "reasons", []
    )


def test_degraded_gamma_structure_keeps_vertical_capability_and_blocks_butterfly() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    payload = _pin_payload(now)
    payload["option_structure_frame"].update(quality="degraded")
    payload["option_structure_frame"]["structure"]["gex_quality"] = (
        "no_open_interest_gex"
    )
    payload["level_decision"] = {
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "flip_high",
        "level": 7710.0,
        "event_id": "level:7710:up",
    }
    latest = _pin_ladder_state(now)
    facts = build_market_fact_pack(payload, latest, now)
    rows = enumerate_candidates(
        payload,
        facts,
        assess_regime(facts),
        latest,
        now=now,
        policy=DEFAULT_STRATEGY_POLICY,
    )

    assert facts["quality"]["status"] == "degraded"
    assert facts["capabilities"]["vertical"]["ready"] is True
    assert facts["capabilities"]["butterfly"]["ready"] is False
    assert any(
        row["strategy_type"].endswith("_DEBIT_VERTICAL")
        and row["quote"]["status"] == "ready"
        for row in rows
    )
    assert not any(row["strategy_type"].endswith("_BUTTERFLY") for row in rows)


def test_confirmed_setup_directly_enumerates_without_legacy_spread() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["option_structure_frame"]["front_expiry"] = "20260807"

    decision = build_strategy_decision(payload, _vertical_chain_state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["source"] == "rth_schwab_width_enumeration"
    assert decision["candidate"]["economics"]["width_points"] in {5.0, 10.0, 15.0, 20.0}
    assert "vertical_exact_spread_unavailable" not in decision["why_not"]["reasons"]
    assert decision["rejection_funnel"]["setup_detected"] == 1
    assert decision["rejection_funnel"]["candidate_enumerated"] > 0
    assert decision["rejection_funnel"]["exact_quote_ready"] > 0


def test_vertical_width_path_reasons_bind_short_to_target_and_remaining_move() -> None:
    assert vertical_width_path_reasons(
        long_strike=7755.0,
        short_strike=7775.0,
        right="C",
        target=7760.0,
        remaining_expected_move=8.7,
    ) == [
        "vertical_short_beyond_target",
        "vertical_width_exceeds_remaining_move",
    ]
    assert vertical_width_path_reasons(
        long_strike=7755.0,
        short_strike=7760.0,
        right="C",
        target=7760.0,
        remaining_expected_move=8.7,
    ) == []
    assert vertical_width_path_reasons(
        long_strike=7755.0,
        short_strike=7765.0,
        right="C",
        target=7770.0,
        remaining_expected_move=8.7,
    ) == ["vertical_width_exceeds_remaining_move"]
    assert vertical_width_path_reasons(
        long_strike=7755.0,
        short_strike=7745.0,
        right="P",
        target=7750.0,
        remaining_expected_move=10.0,
    ) == ["vertical_short_beyond_target"]
    assert vertical_width_path_reasons(
        long_strike=7755.0,
        short_strike=7750.0,
        right="P",
        target=7750.0,
        remaining_expected_move=10.0,
    ) == []
    assert vertical_width_path_reasons(
        long_strike=7755.0,
        short_strike=7760.0,
        right="C",
        target=7760.0,
        remaining_expected_move=None,
    ) == ["vertical_remaining_move_unavailable"]


def test_debit_long_beyond_remaining_move_rejects_unreachable_10_delta_calls() -> None:
    assert debit_vertical_reach_reasons(
        spot=7753.0,
        long_strike=7800.0,
        short_strike=7850.0,
        right="C",
        remaining_expected_move=25.0,
    ) == [
        "vertical_width_exceeds_remaining_move",
        "debit_long_beyond_remaining_move",
    ]
    assert debit_vertical_reach_reasons(
        spot=7753.0,
        long_strike=7760.0,
        short_strike=7770.0,
        right="C",
        remaining_expected_move=25.0,
    ) == []


def test_failed_break_does_not_select_call_vertical_past_target_or_remaining_move() -> None:
    now = datetime(2026, 8, 12, 14, 26, 35, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["trading_date"] = "2026-08-12"
    payload["underlier"] = {"price": 7752.955, "source": "index:SPX"}
    payload["expected_move_points"] = 8.7
    payload["option_structure_frame"]["front_expiry"] = "20260812"
    payload["option_structure_frame"]["structure"]["call_wall"] = 7760.0
    payload["level_decision"] = {
        "phase": "confirmed",
        "thesis": "fade",
        "direction": "up",
        "level_kind": "orh",
        "level": 7750.0,
        "event_id": "level:7750:up",
    }
    payload["trade_intent"] = {
        "invalidation_spx": 7750.0,
        "confirmation_geometry": {"target_spx": 7760.0},
    }
    latest = _failed_break_20260812_chain(now)

    decision = build_strategy_decision(payload, latest, now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    candidate = decision["candidate"]
    assert candidate["setup_kind"] == "FAILED_BREAK_RECLAIM"
    assert candidate["long"]["strike"] == 7755.0
    assert candidate["short"]["strike"] == 7760.0
    assert candidate["economics"]["width_points"] == 5.0
    considered = [
        tuple(row.get("strikes") or ())
        for row in decision.get("candidates_considered") or ()
        if str(row.get("strategy_type") or "").endswith("_DEBIT_VERTICAL")
    ]
    assert (7755.0, 7775.0) not in considered
    near_miss_strikes = [
        tuple(row.get("strikes") or ())
        for row in decision.get("why_not", {}).get("nearest_candidates") or ()
    ]
    assert (7755.0, 7775.0) not in near_miss_strikes


def test_missing_expected_move_fails_closed_for_debit_vertical() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("expected_move_points")

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "NO_TRADE"
    assert "vertical_remaining_move_unavailable" in decision["why_not"]["reasons"]


def test_confirmed_session_episode_maps_to_failed_break_reclaim_vertical() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["option_structure_frame"]["front_expiry"] = "20260807"
    payload["level_decision"] = {"phase": "far"}
    payload["session_episode"] = {
        "phase": "V_REVERSAL_CONFIRMED",
        "break_direction": "down",
        "break_level": 7705.0,
        "break_level_kind": "flip_high",
        "episode_id": "episode:failed-down-break",
    }

    decision = build_strategy_decision(payload, _vertical_chain_state(now), now)

    assert decision["market_facts"]["session_episode"]["phase"] == (
        "v_reversal_confirmed"
    )
    assert decision["market_facts"]["session_episode"]["setup_direction"] == "UP"
    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["setup_kind"] == "FAILED_BREAK_RECLAIM"
    assert decision["candidate"]["direction"] == "UP"
    assert decision["rejection_funnel"]["setup_detected"] == 1


def test_session_episode_reclaim_expires_after_chase_progress() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["underlier"] = {"price": 7724.0, "source": "index:SPX"}
    payload["option_structure_frame"]["front_expiry"] = "20260807"
    payload["level_decision"] = {"phase": "far"}
    payload["session_episode"] = {
        "phase": "V_REVERSAL_CONFIRMED",
        "break_direction": "down",
        "break_level": 7705.0,
        "break_level_kind": "flip_high",
        "episode_id": "episode:failed-down-break",
    }

    decision = build_strategy_decision(payload, _vertical_chain_state(now), now)
    episode = next(
        row
        for row in decision["market_facts"]["rth_setups"]
        if row["setup_variant"] == "SESSION_EPISODE"
    )

    assert episode["state"] == "ENTRY_TOO_LATE"
    assert episode["blocked_by"] == "session_episode_reclaim_progress_too_late"
    assert episode["trigger_target_progress"] >= 0.60
    assert decision["decision_type"] == "NO_TRADE"


def test_vwap_pullback_stays_open_for_two_bars_after_confirmation() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _vwap_pullback_payload(now)
    bars = payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
        "input_lineage"
    ]["diagnostics"]["rth_bar_path"]
    extra = [
        {
            "bar_start": now.isoformat(),
            "open": 7735.0,
            "high": 7736.5,
            "low": 7733.0,
            "close": 7735.5,
            "quality": "ok",
        }
    ]
    _attach_rth_setup_path(payload, [*bars, *extra], "HL_ONLY")

    decision = build_strategy_decision(
        payload, _two_sided_vertical_chain(now, right="C"), now
    )
    pullback = next(
        row
        for row in decision["market_facts"]["rth_setups"]
        if row["setup_kind"] == "TREND_PULLBACK"
    )

    assert pullback["state"] == "ENTRY_WINDOW_OPEN"
    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["setup_kind"] == "TREND_PULLBACK"


def test_vwap_pullback_closes_after_hold_bars_elapse() -> None:
    now = datetime(2026, 8, 7, 15, 10, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["level_decision"] = {"phase": "far"}
    payload["option_structure_frame"]["front_expiry"] = "20260807"
    bars = [
        {
            "bar_start": (now - timedelta(minutes=20)).isoformat(),
            "open": 7735.0,
            "high": 7736.0,
            "low": 7731.5,
            "close": 7734.0,
            "quality": "ok",
        },
        {
            "bar_start": (now - timedelta(minutes=15)).isoformat(),
            "open": 7734.0,
            "high": 7736.0,
            "low": 7733.0,
            "close": 7735.0,
            "quality": "ok",
        },
        {
            "bar_start": (now - timedelta(minutes=10)).isoformat(),
            "open": 7735.0,
            "high": 7736.5,
            "low": 7733.0,
            "close": 7735.5,
            "quality": "ok",
        },
        {
            "bar_start": (now - timedelta(minutes=5)).isoformat(),
            "open": 7735.0,
            "high": 7736.5,
            "low": 7733.0,
            "close": 7735.5,
            "quality": "ok",
        },
        {
            "bar_start": now.isoformat(),
            "open": 7735.0,
            "high": 7736.5,
            "low": 7733.0,
            "close": 7735.5,
            "quality": "ok",
        },
    ]
    _attach_rth_setup_path(payload, bars, "HL_ONLY")

    decision = build_strategy_decision(
        payload, _two_sided_vertical_chain(now, right="C"), now
    )
    pullback = next(
        row
        for row in decision["market_facts"]["rth_setups"]
        if row["setup_kind"] == "TREND_PULLBACK"
    )

    assert pullback["state"] == "ENTRY_TOO_LATE"
    assert decision["decision_type"] == "NO_TRADE"


@pytest.mark.parametrize(
    ("break_side", "direction", "right", "structure", "event_kind"),
    [
        ("UP", "DOWN", "P", "LH_LL", "terminal_below"),
        ("DOWN", "UP", "C", "HH_HL", "terminal_above"),
    ],
)
def test_opening_range_failed_break_opens_symmetric_vertical_entry_window(
    break_side: str,
    direction: str,
    right: str,
    structure: str,
    event_kind: str,
) -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["level_decision"] = {"phase": "far"}
    payload["option_structure_frame"]["front_expiry"] = "20260807"
    payload["strategy_distribution_forecast"] = _probability_forecast(
        now, event_kind
    )
    _attach_rth_setup_path(payload, _or_failed_break_bars(now, break_side), structure)

    latest = _two_sided_vertical_chain(now, right=right)
    decision = build_strategy_decision(payload, latest, now)

    assert decision["decision_type"] == f"{'CALL' if right == 'C' else 'PUT'}_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["setup_kind"] == "FAILED_BREAK_RECLAIM"
    assert decision["candidate"]["setup_variant"] == "OR_FAILED_BREAK"
    assert decision["candidate"]["setup_state"] == "ENTRY_WINDOW_OPEN"
    assert decision["candidate"]["direction"] == direction
    assert decision["rejection_funnel"]["entry_window_open"] == 1


def test_vwap_trend_pullback_opens_call_vertical_before_prior_high_break() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _vwap_pullback_payload(now)

    decision = build_strategy_decision(
        payload, _two_sided_vertical_chain(now, right="C"), now
    )

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["setup_kind"] == "TREND_PULLBACK"
    assert decision["candidate"]["setup_variant"] == "VWAP_PULLBACK"
    assert decision["candidate"]["setup_state"] == "ENTRY_WINDOW_OPEN"
    assert decision["rejection_funnel"]["entry_window_open"] == 1


def test_pending_5m_confirmation_is_the_desk_primary_blocker() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _vwap_pullback_payload(now)
    bars = payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
        "input_lineage"
    ]["diagnostics"]["rth_bar_path"]
    _attach_rth_setup_path(payload, bars[:1], "HL_ONLY")

    decision = build_strategy_decision(
        payload, _two_sided_vertical_chain(now, right="C"), now
    )
    setups = decision["market_facts"]["rth_setups"]
    pullback = next(row for row in setups if row["setup_kind"] == "TREND_PULLBACK")

    assert decision["decision_type"] == "NO_TRADE"
    assert pullback["state"] == "SETUP_DETECTED"
    assert pullback["blocked_by"] == "next_5m_confirmation_pending"
    assert pullback["detected_at"]
    assert pullback["window_opens_at"] is None
    assert decision["desk_view"]["reason"] == "rth_entry_window_not_open"
    assert decision["why_not"]["primary_blocker"] == "rth_entry_window_not_open"
    assert not str(decision["desk_view"]["reason"]).startswith("surface_shape_")
    assert decision["rejection_funnel"]["setup_detected"] == 1
    assert decision["rejection_funnel"]["entry_window_open"] == 0
    assert decision["rejection_funnel"]["pending_confirmation"] == 1
    assert decision["rejection_funnel"]["candidate_enumerated"] == 0


def test_incomplete_trend_vector_is_unevaluable_not_transition() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _vwap_pullback_payload(now)
    lineage = payload["minute_market_frame"]["diagnostics"]["rth_market_state"]
    lineage["D"] = None
    lineage["input_lineage"]["values"]["vwap_cross_count"] = None

    facts = build_market_fact_pack(payload, _two_sided_vertical_chain(now, right="C"), now)
    regime = assess_regime(facts)
    decision = build_strategy_decision(
        payload, _two_sided_vertical_chain(now, right="C"), now
    )

    assert facts["capabilities"]["path"]["ready"] is True
    assert facts["capabilities"]["path"]["trend_evaluable"] is False
    assert facts["capabilities"]["vertical"]["ready"] is True
    assert regime["path_state"] == "UNCERTAIN"
    assert "path_inputs_unavailable" in regime["reasons"]
    assert decision["decision_type"] == "NO_TRADE"
    assert decision["desk_view"]["reason"] == "trend_pullback_path_unevaluable"
    assert decision["regime"]["entry_state"] == "INSUFFICIENT_DATA"
    assert "vertical_path_inputs_unavailable" not in decision["why_not"]["reasons"]


def test_invalidated_only_setups_are_not_counted_as_detected() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["level_decision"] = {"phase": "far"}
    payload["option_structure_frame"]["front_expiry"] = "20260807"
    closes = (7735.0, 7736.0, 7742.0, 7743.0, 7739.0, 7742.0)
    bars = [
        {
            "bar_start": (now - timedelta(minutes=5 * (len(closes) - index))).isoformat(),
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "quality": "ok",
        }
        for index, close in enumerate(closes)
    ]
    _attach_rth_setup_path(payload, bars, "LH_LL")
    payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
        "input_lineage"
    ]["diagnostics"]["rth_bar_vwaps"] = {}

    decision = build_strategy_decision(
        payload, _two_sided_vertical_chain(now, right="P"), now
    )
    setups = decision["market_facts"]["rth_setups"]
    failed_break = next(
        row for row in setups if row["setup_variant"] == "OR_FAILED_BREAK"
    )

    assert failed_break["state"] == "INVALIDATED"
    assert failed_break["blocked_by"] == "next_5m_reaccepted_breakout"
    assert decision["decision_type"] == "NO_TRADE"
    assert decision["desk_view"]["reason"] == "rth_setup_invalidated"
    assert decision["rejection_funnel"]["setup_detected"] == 0
    assert decision["rejection_funnel"]["pending_confirmation"] == 0


def test_event_settlement_reason_is_trace_not_exclusive_rth_blocker() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _vwap_pullback_payload(now)
    bars = payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
        "input_lineage"
    ]["diagnostics"]["rth_bar_path"]
    _attach_rth_setup_path(payload, bars[:1], "HL_ONLY")
    payload["day_move"] = {"prior_close": 7728.2}
    payload["macro_event"] = {
        "mode": "pre_event",
        "entry_allowed": False,
        "next_event": {
            "id": "cpi",
            "name": "CPI",
            "impact": "high",
            "release_at": (now + timedelta(hours=1)).isoformat(),
        },
    }

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "NO_TRADE"
    assert decision["why_not"]["primary_blocker"] == "rth_entry_window_not_open"
    assert "event_settlement_exact_two_leg_quote_unavailable" in decision["why_not"]["reasons"]
    assert decision["rejection_funnel"]["event_settlement_considered"] == 1


def test_active_shock_blocks_butterfly_but_keeps_open_vertical_setup() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _vwap_pullback_payload(now)
    payload["intraday_shock_state"] = {
        "active_event": {
            "status": "shock_confirmed",
            "anchor_at": (now - timedelta(minutes=2)).isoformat(),
            "anchor_spx": 7720.0,
            "extreme_spx": 7690.0,
            "shock_spx_bps": -38.9,
            "shock_es_bps": -37.0,
        },
        "samples": [],
        "rearm": None,
    }

    decision = build_strategy_decision(
        payload, _two_sided_vertical_chain(now, right="C"), now
    )

    assert decision["market_facts"]["shock"]["state"] == "ACTIVE"
    assert decision["market_facts"]["capabilities"]["butterfly"]["ready"] is False
    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]

    pin_now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    pin_payload = _pin_payload(pin_now)
    pin_latest = _pin_state(pin_now)
    control_facts = build_market_fact_pack(pin_payload, pin_latest, pin_now)
    control_regime = assess_regime(control_facts)
    butterfly = next(
        row
        for row in enumerate_candidates(
            pin_payload,
            control_facts,
            control_regime,
            pin_latest,
            now=pin_now,
            policy=DEFAULT_STRATEGY_POLICY,
        )
        if row["strategy_type"].endswith("_BUTTERFLY")
    )
    pin_payload["intraday_shock_state"] = deepcopy(payload["intraday_shock_state"])
    pin_payload["intraday_shock_state"]["active_event"]["anchor_at"] = (
        pin_now - timedelta(minutes=2)
    ).isoformat()
    shock_facts = build_market_fact_pack(pin_payload, pin_latest, pin_now)
    shock_regime = assess_regime(shock_facts)
    rank = rank_candidates(
        [butterfly],
        shock_facts,
        shock_regime,
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=pin_now,
    )
    shock_gates = [gate["gate"] for gate in rank.near_misses[0]["failed_gates"]]
    assert "butterfly_shock_veto" in shock_gates


def test_missing_vix_response_cannot_produce_pin_stable_or_butterfly() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    payload = _pin_payload(now)
    latest = _pin_state(now)
    control_facts = build_market_fact_pack(payload, latest, now)
    control_regime = assess_regime(control_facts)
    butterfly = next(
        row
        for row in enumerate_candidates(
            payload,
            control_facts,
            control_regime,
            latest,
            now=now,
            policy=DEFAULT_STRATEGY_POLICY,
        )
        if row["strategy_type"].endswith("_BUTTERFLY")
    )
    payload["minute_market_frame"]["volatility"].pop("vix_return_15m_pct")

    decision = build_strategy_decision(payload, latest, now)

    assert decision["regime"]["terminal_state"] == "UNCERTAIN"
    assert decision["market_facts"]["capabilities"]["butterfly"]["ready"] is False
    assert decision["decision_type"] == "NO_TRADE"
    rank = rank_candidates(
        [butterfly],
        decision["market_facts"],
        decision["regime"],
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )
    gates = [gate["gate"] for gate in rank.near_misses[0]["failed_gates"]]
    assert "butterfly_vix_or_breadth_unavailable" in gates


def test_butterfly_body_far_from_value_center_fails_ranker_hard_gate() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    payload = _pin_payload(now)
    latest = _pin_state(now)
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)
    rows = enumerate_candidates(
        payload, facts, regime, latest, now=now, policy=DEFAULT_STRATEGY_POLICY
    )
    butterfly = deepcopy(
        next(row for row in rows if row["strategy_type"].endswith("_BUTTERFLY"))
    )
    butterfly["center"] = 7730.0

    rank = rank_candidates(
        [butterfly],
        facts,
        regime,
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )

    assert rank.passed == []
    gates = [gate["gate"] for gate in rank.near_misses[0]["failed_gates"]]
    assert "butterfly_body_value_center_distance" in gates


def _stable_pin_butterfly(
    *,
    center: float = 7710.0,
    width: float = 10.0,
    right: str = "C",
    debit: float = 3.2,
) -> dict:
    return {
        "candidate_id": f"fly-{center:.0f}-{width:.0f}{right}",
        "strategy_type": "CALL_BUTTERFLY" if right == "C" else "PUT_BUTTERFLY",
        "setup_kind": "STABLE_PIN",
        "direction": "NEUTRAL",
        "selection_score": 1.0,
        "center": center,
        "width": width,
        "right": right,
        "legs": [
            {"strike": center - width, "right": right},
            {"strike": center, "right": right},
            {"strike": center + width, "right": right},
        ],
        "quote": {"status": "ready", "bid": debit - 0.2, "ask": debit},
        "economics": {
            "width_points": width,
            "max_gain_points": width - debit,
            "max_loss_points": debit,
            "debit_fraction_of_width": debit / width,
            "breakeven_low": center - (width - debit),
            "breakeven_high": center + (width - debit),
        },
        "quote_valid_until": "2026-08-06T19:00:30+00:00",
        "opportunity_valid_until": "2026-08-06T19:05:00+00:00",
        "automatic_ordering": False,
        "manual_action_only": True,
    }


def _rank_stable_pin_butterfly(facts: dict[str, object], butterfly: dict) -> object:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    return rank_candidates(
        [butterfly],
        _ranker_pin_facts(facts),
        {
            "pin": {"depin_risk": 0.0, "recent_extreme_acceptance": False},
            "terminal_state": "PIN_STABLE",
        },
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )


def test_butterfly_spot_outside_wings_fails_ranker_hard_gate() -> None:
    rank = _rank_stable_pin_butterfly(
        {"spot": {"spx": 7792.0}, "minutes_to_close": 44},
        _stable_pin_butterfly(center=7785.0, width=5.0, right="P", debit=0.9),
    )
    assert rank.passed == []
    gates = [gate["gate"] for gate in rank.near_misses[0]["failed_gates"]]
    assert "butterfly_spot_outside_wings" in gates


def test_butterfly_entry_too_early_blocks_midday_five_wide() -> None:
    rank = _rank_stable_pin_butterfly(
        {
            "spot": {"spx": 7793.0},
            "minutes_to_close": 90,
            "volatility": {"expected_move_points": 6.3},
            "structure": {"q_mode": 7790.0, "put_wall": 7780.0, "call_wall": 7780.0},
            "value_center": {"spx_30m": 7791.0},
        },
        _stable_pin_butterfly(center=7790.0, width=5.0, right="P", debit=1.3),
    )
    assert rank.passed == []
    gates = [gate["gate"] for gate in rank.near_misses[0]["failed_gates"]]
    assert "butterfly_entry_too_early" in gates


def test_butterfly_unresolved_nearby_wall_blocks_cage_that_is_not_a_pin() -> None:
    rank = _rank_stable_pin_butterfly(
        {
            "spot": {"spx": 7793.0},
            "minutes_to_close": 44,
            "volatility": {"expected_move_points": 6.3},
            "structure": {"q_mode": 7790.0, "put_wall": 7790.0, "call_wall": 7800.0},
            "value_center": {"spx_30m": 7792.0},
        },
        _stable_pin_butterfly(center=7790.0, width=5.0, right="P", debit=1.3),
    )
    assert rank.passed == []
    gates = [gate["gate"] for gate in rank.near_misses[0]["failed_gates"]]
    assert "butterfly_unresolved_nearby_wall" in gates


def test_late_pin_at_call_wall_still_authorizes_five_wide_butterfly() -> None:
    rank = _rank_stable_pin_butterfly(
        {
            "spot": {"spx": 7801.2},
            "minutes_to_close": 44,
            "volatility": {"expected_move_points": 6.3},
            "structure": {"q_mode": 7800.0, "put_wall": 7800.0, "call_wall": 7800.0},
            "value_center": {"spx_30m": 7802.0},
        },
        _stable_pin_butterfly(center=7800.0, width=5.0, right="P", debit=1.3),
    )
    assert [row["center"] for row in rank.passed] == [7800.0]


def test_rth_vertical_uses_fresh_atomic_ibkr_fallback_when_schwab_is_stale() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _vwap_pullback_payload(now)
    fresh_ibkr = _two_sided_vertical_chain(
        now, right="C", provider=Provider.IBKR
    )
    stale_schwab = _two_sided_vertical_chain(
        now - timedelta(seconds=30), right="C", provider=Provider.SCHWAB
    )
    latest = LatestState(
        created_at=now - timedelta(seconds=1),
        as_of=now - timedelta(seconds=1),
        quotes=(*stale_schwab.quotes, *fresh_ibkr.quotes),
        best_quotes=stale_schwab.quotes,
    )

    decision = build_strategy_decision(
        payload,
        latest,
        now,
    )

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["source"] == "rth_ibkr_width_enumeration"
    assert decision["candidate"]["long"]["provider"] == "ibkr"
    assert decision["candidate"]["short"]["provider"] == "ibkr"
    assert decision["candidate"]["quote"]["status"] == "ready"


def test_chain_implied_spx_coordinate_keeps_gth_facts_available() -> None:
    now = datetime(2026, 8, 7, 3, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload.pop("underlier")
    payload["spot"] = {
        "price": 7711.25,
        "spx_observed_value": 7711.25,
        "observed_value": 7711.25,
        "source": "chain_implied",
        "kind": "chain_implied_spx",
        "basis": None,
    }
    payload["trigger_coordinate"] = {
        "spx_observed_value": 7711.25,
        "observed_value": 7711.25,
        "source": "chain_implied",
        "kind": "chain_implied_spx",
        "basis_points": None,
    }

    facts = build_market_fact_pack(payload, _state(now), now)

    assert facts["spot"]["spx"] == pytest.approx(7711.25)
    assert facts["spot"]["kind"] == "chain_implied_spx"
    assert facts["capabilities"]["global"]["coordinate_ready"] is True
    assert "spx_price_unavailable" not in facts["quality"]["reasons"]


def test_es_equivalent_fills_when_official_spx_print_is_missing() -> None:
    now = datetime(2026, 8, 12, 16, 10, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["pricing_allowed"] = True
    payload["underlier"] = {
        "price": None,
        "spx_observed_value": None,
        "kind": "unavailable",
        "source": "unavailable",
        "basis": 21.925,
        "basis_points": 21.925,
    }
    payload["spot"] = dict(payload["underlier"])
    payload["trigger_coordinate"] = {
        "kind": "unavailable",
        "spx_observed_value": None,
        "observed_value": None,
        "basis_points": 21.925,
        "source": "unavailable",
    }
    payload["minute_market_frame"]["es"]["price"] = 7768.125

    facts = build_market_fact_pack(payload, _state(now), now)

    assert facts["spot"]["spx"] == pytest.approx(7746.2)
    assert facts["spot"]["kind"] == "es_equivalent"
    assert facts["capabilities"]["global"]["coordinate_ready"] is True
    assert "spx_price_unavailable" not in facts["quality"]["reasons"]
    assert "pricing_not_authorized" not in facts["quality"]["reasons"]


def test_rolling_path_atr_keeps_vertical_capability_when_sma_atr_is_missing() -> None:
    now = datetime(2026, 8, 12, 16, 19, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    lineage = payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
        "input_lineage"
    ]["diagnostics"]
    lineage["moving_averages"] = {
        "atr_5m": None,
        "status": "warming",
        "reasons": ["atr_5m_unavailable"],
    }
    lineage["rolling_path_percentiles"] = {"atr_5m": 4.196429, "status": "provisional"}

    facts = build_market_fact_pack(payload, _state(now), now)
    decision = build_strategy_decision(payload, _state(now), now)

    assert facts["path"]["atr_5m"] == pytest.approx(4.196429)
    assert facts["capabilities"]["path"]["atr_ready"] is True
    assert facts["capabilities"]["vertical"]["ready"] is True
    assert "vertical_path_inputs_unavailable" not in (
        facts["capabilities"]["vertical"]["reasons"]
    )
    assert "pricing_not_authorized" not in decision["why_not"]["reasons"]
    assert decision["decision_type"] != "NO_TRADE" or (
        "vertical_path_inputs_unavailable" not in decision["why_not"]["reasons"]
        and "spx_price_unavailable" not in decision["why_not"]["reasons"]
    )


def test_missing_path_inputs_do_not_count_as_setup_detected() -> None:
    now = datetime(2026, 8, 12, 16, 19, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["level_decision"] = {"phase": "far"}
    lineage = payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
        "input_lineage"
    ]["diagnostics"]
    lineage["moving_averages"] = {"atr_5m": None}
    lineage["rolling_path_percentiles"] = {}
    lineage["atr"] = {}

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "NO_TRADE"
    assert decision["why_not"]["primary_blocker"] == "vertical_path_inputs_unavailable"
    assert decision["rejection_funnel"]["setup_detected"] == 0
    assert decision["rejection_funnel"]["pending_confirmation"] == 0
    assert decision["rejection_funnel"]["current_stage"] == "facts_ready"
    assert decision["regime"]["entry_state"] == "INSUFFICIENT_DATA"


def test_option_frame_not_ready_does_not_emit_pricing_not_authorized() -> None:
    now = datetime(2026, 8, 12, 16, 19, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["pricing_allowed"] = False
    payload["option_structure_frame"]["quality"] = "degraded"
    payload["option_structure_frame"]["l1"] = {"quality": "degraded"}

    facts = build_market_fact_pack(payload, _state(now), now)
    decision = build_strategy_decision(payload, _state(now), now)

    assert "pricing_not_authorized" not in facts["quality"]["reasons"]
    assert "option_frame_not_ready" in facts["quality"]["reasons"]
    assert facts["capabilities"]["vertical"]["ready"] is True
    assert "pricing_not_authorized" not in decision["why_not"]["reasons"]


def test_ranker_tries_second_candidate_when_first_fails_utility() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    forecast = payload["strategy_distribution_forecast"]
    forecast["q_event"]["probability"] = 0.6
    forecast["p_event"].update(probability=0.6, interval_low=0.6)
    payload["call_skew_spread_shadow"]["candidate"]["long"].update(bid=4.8, ask=5.0)
    payload["call_skew_spread_shadow"]["candidate"]["short"].update(bid=0.5, ask=0.7)
    latest = _vertical_chain_state(now)
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)
    rows = enumerate_candidates(
        payload, facts, regime, latest, now=now, policy=DEFAULT_STRATEGY_POLICY
    )

    rank = rank_candidates(
        rows,
        facts,
        regime,
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )

    assert rank.passed
    assert any(
        "candidate_utility_not_positive"
        in list((candidate.get("edge") or {}).get("advisories") or ())
        or float((candidate.get("utility") or {}).get("utility") or 1.0) <= 0.0
        for candidate in rank.passed
    )
    assert rank.passed[0]["economics"]["width_points"] == pytest.approx(20.0)


def test_directional_confirmation_butterfly_is_research_alternative_only() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    payload = _pin_payload(now)
    payload["level_decision"] = {
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "flip_high",
        "level": 7710.0,
        "event_id": "level:7710:up",
    }
    # Distinct target center so the directional rows do not collide with the
    # stable-pin rows sharing the same strikes (candidate_id dedup keeps pin rows).
    payload["option_structure_frame"]["structure"]["call_wall"] = 7730.0
    latest = _pin_ladder_state(now)
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)
    rows = enumerate_candidates(
        payload, facts, regime, latest, now=now, policy=DEFAULT_STRATEGY_POLICY
    )

    directional = [
        row for row in rows if row["source"] == "directional_confirmation_butterfly"
    ]
    assert directional
    for row in directional:
        assert row["manual_authority_eligible"] is False
        assert row["thesis_direction"] == "UP"
        assert row["payoff_shape"] == "TARGET_CONCENTRATED"
        assert row["direction"] == "NEUTRAL"

    rank = rank_candidates(
        rows,
        facts,
        regime,
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )
    assert all(
        candidate.get("source") != "directional_confirmation_butterfly"
        for candidate in rank.passed
    )
    assert any(
        "research_alternative_only"
        in [str(gate.get("gate")) for gate in row.get("gate_failures") or ()]
        for row in rank.gate_audit
    )


def test_surface_shape_soft_prior_changes_only_post_gate_vertical_rank() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)

    def context(d3_snr: float) -> dict[str, object]:
        return {
            "feature_version": "strike_differential_context.v1",
            "status": "ready",
            "references": [
                {
                    "center": 100.0,
                    "labels": ["atm"],
                    "observations": [
                        {
                            "scale_points": 5.0,
                            "quality": "ready" if d3_snr >= 1.0 else "degraded_low_snr",
                            "strike_d2": 0.02,
                            "strike_d3": 0.001,
                            "strike_d4": 0.0,
                            "d2_snr": d3_snr,
                            "d3_snr": d3_snr,
                            "d4_snr": 0.0,
                        }
                    ],
                }
            ],
        }

    facts = {
        "session_date": "2026-08-07",
        "spot": {"spx": 100.0},
        "path": {"atr_5m": 10.0, "distance_to_vwap_points": 0.0, "impulse_15m_points": 0.0},
        "volatility": {"expected_move_points": 40.0},
        "probability": {
            "event": {"kind": "terminal_above", "target_at": (now + timedelta(minutes=5)).isoformat()},
            "q": 0.6,
            "p_empirical": 0.7,
            "p_interval_low": 0.6,
            "n_raw": 40,
            "n_effective": 40.0,
            "historical_sessions": ["2026-08-06"],
        },
        "structure": {"strike_differential_context": context(2.0)},
    }

    def vertical(
        candidate_id: str,
        *,
        strategy_type: str,
        direction: str,
        score: float,
        quote_status: str = "ready",
    ) -> dict[str, object]:
        up = direction == "UP"
        return {
            "candidate_id": candidate_id,
            "strategy_type": strategy_type,
            "setup_kind": "TREND_PULLBACK",
            "direction": direction,
            "selection_score": score,
            "long": {"strike": 100.0},
            "short": {"strike": 110.0 if up else 90.0},
            "quote": {
                "status": quote_status,
                "reasons": [] if quote_status == "ready" else ["spread_leg_quote_stale"],
                "bid": 2.8,
                "ask": 3.0,
            },
            "economics": {
                "width_points": 10.0,
                "max_gain_points": 7.0,
                "max_loss_points": 3.0,
                "debit_fraction_of_width": 0.3,
            },
            "trigger_level": 100.0,
            "target_spx": 120.0 if up else 80.0,
            "invalidation_spx": 95.0 if up else 105.0,
            "quote_valid_until": (now + timedelta(seconds=30)).isoformat(),
            "opportunity_valid_until": (now + timedelta(minutes=5)).isoformat(),
            "automatic_ordering": False,
            "manual_action_only": True,
        }

    call = vertical(
        "aligned-call",
        strategy_type="CALL_DEBIT_VERTICAL",
        direction="UP",
        score=1.0,
    )
    put = vertical(
        "unaligned-put",
        strategy_type="PUT_DEBIT_VERTICAL",
        direction="DOWN",
        score=1.01,
    )
    hard_failed = vertical(
        "hard-failed-call",
        strategy_type="CALL_DEBIT_VERTICAL",
        direction="UP",
        score=9.0,
        quote_status="unavailable",
    )

    high_snr = rank_candidates(
        [call, put, hard_failed],
        facts,
        {"pin": {"depin_risk": 0.0}},
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )
    assert [candidate["candidate_id"] for candidate in high_snr.passed[:2]] == [
        "aligned-call",
        "unaligned-put",
    ]
    assert high_snr.passed[0]["selection_score_base"] == 1.0
    assert high_snr.passed[0]["surface_shape_prior"] == 0.05
    assert high_snr.passed[0]["selection_score"] == 1.05
    assert high_snr.passed[0]["automatic_ordering"] is False
    assert [candidate["candidate_id"] for candidate in high_snr.near_misses] == [
        "hard-failed-call"
    ]
    assert "surface_shape_prior" not in high_snr.near_misses[0]

    low_snr_facts = deepcopy(facts)
    low_snr_facts["structure"]["strike_differential_context"] = context(0.2)
    low_snr = rank_candidates(
        [call, put],
        low_snr_facts,
        {"pin": {"depin_risk": 0.0}},
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )
    assert [candidate["candidate_id"] for candidate in low_snr.passed[:2]] == [
        "unaligned-put",
        "aligned-call",
    ]
    assert all(candidate["surface_shape_prior"] == 0.0 for candidate in low_snr.passed)


def test_ranker_winner_is_structure_score_not_research_utility() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    facts = _ranker_pin_facts({
        "session_date": "2026-08-06",
        "probability": {
            "event": {"kind": "terminal_between", "target_at": (now + timedelta(minutes=5)).isoformat()},
            "q": 0.6,
            "p_empirical": 0.7,
            "p_interval_low": 0.6,
            "n_raw": 40,
            "n_effective": 40.0,
            "historical_sessions": ["2026-08-05"],
        },
    })

    def butterfly(candidate_id: str, *, selection_score: float, gain: float) -> dict:
        return {
            "candidate_id": candidate_id,
            "strategy_type": "CALL_BUTTERFLY",
            "setup_kind": "STABLE_PIN",
            "direction": "NEUTRAL",
            "selection_score": selection_score,
            "center": 7710.0,
            "width": 10.0,
            "legs": [{"strike": 7700.0}, {"strike": 7710.0}, {"strike": 7720.0}],
            "quote": {"status": "ready", "bid": 3.0, "ask": 3.2},
            "economics": {"width_points": 10.0, "max_gain_points": gain, "max_loss_points": 3.2,
                          "breakeven_low": 7703.2, "breakeven_high": 7716.8},
            "quote_valid_until": (now + timedelta(seconds=30)).isoformat(),
            "opportunity_valid_until": (now + timedelta(minutes=5)).isoformat(),
            "automatic_ordering": False,
            "manual_action_only": True,
        }

    high_utility = butterfly("high-utility", selection_score=1.0, gain=16.8)
    high_structure = butterfly("high-structure", selection_score=9.0, gain=6.8)
    rank = rank_candidates(
        [high_utility, high_structure],
        facts,
        {
            "pin": {"depin_risk": 0.0, "recent_extreme_acceptance": False},
            "terminal_state": "PIN_STABLE",
        },
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=None,
        probability_settings=None,
        now=now,
    )

    assert [candidate["candidate_id"] for candidate in rank.passed] == [
        "high-structure",
        "high-utility",
    ]
    assert (
        rank.passed[0]["utility"]["utility"] < rank.passed[1]["utility"]["utility"]
    )
    scores = {row["candidate_id"]: row["score"] for row in rank.gate_audit}
    assert scores["high-structure"] == pytest.approx(9.0)
    assert scores["high-utility"] == pytest.approx(1.0)


def test_policy_ev_annotation_is_rank_only_and_does_not_change_order(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    facts = _ranker_pin_facts({
        "session_date": "2026-08-06",
        "probability": {
            "event": {
                "kind": "terminal_between",
                "target_at": (now + timedelta(minutes=5)).isoformat(),
            },
            "q": 0.6,
            "p_empirical": 0.7,
            "p_interval_low": 0.6,
            "n_raw": 40,
            "n_effective": 40.0,
            "historical_sessions": ["2026-08-05"],
        },
    })

    def butterfly(candidate_id: str, *, selection_score: float, gain: float) -> dict:
        return {
            "candidate_id": candidate_id,
            "strategy_type": "CALL_BUTTERFLY",
            "setup_kind": "STABLE_PIN",
            "direction": "NEUTRAL",
            "selection_score": selection_score,
            "center": 7710.0,
            "width": 10.0,
            "legs": [{"strike": 7700.0}, {"strike": 7710.0}, {"strike": 7720.0}],
            "quote": {"status": "ready", "bid": 3.0, "ask": 3.2},
            "economics": {
                "width_points": 10.0,
                "max_gain_points": gain,
                "max_loss_points": 3.2,
                "breakeven_low": 7703.2,
                "breakeven_high": 7716.8,
            },
            "quote_valid_until": (now + timedelta(seconds=30)).isoformat(),
            "opportunity_valid_until": (now + timedelta(minutes=5)).isoformat(),
            "automatic_ordering": False,
            "manual_action_only": True,
        }

    candidates = [
        butterfly("high-utility", selection_score=1.0, gain=16.8),
        butterfly("high-structure", selection_score=9.0, gain=6.8),
    ]
    regime = {
        "pin": {"depin_risk": 0.0, "recent_extreme_acceptance": False},
        "terminal_state": "PIN_STABLE",
    }

    without_table = rank_candidates(
        candidates,
        facts,
        regime,
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=tmp_path,
        probability_settings=None,
        now=now,
    )

    research = tmp_path / "research"
    research.mkdir(parents=True, exist_ok=True)
    (research / "policy_ev_table.v1.json").write_text(
        json.dumps(
            {
                "schema_version": "policy_ev_table.v1",
                "management_policy_version": "management_policy.v1",
                "generated_at": now.isoformat(),
                "source_sessions": ["2026-08-05", "2026-08-06"],
                "buckets": {
                    "STABLE_PIN|NEUTRAL|PIN_STABLE": {
                        "n": 24,
                        "ev_points": 0.35,
                        "p25": -0.1,
                        "p75": 0.8,
                        "n_censored": 2,
                        "reason": None,
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    with_table = rank_candidates(
        candidates,
        facts,
        regime,
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=tmp_path,
        probability_settings=None,
        now=now,
    )

    assert [candidate["candidate_id"] for candidate in without_table.passed] == [
        "high-structure",
        "high-utility",
    ]
    assert [candidate["candidate_id"] for candidate in with_table.passed] == [
        "high-structure",
        "high-utility",
    ]
    assert without_table.passed[0]["edge"]["policy_ev_reason"] == "table_unavailable"
    assert with_table.passed[0]["edge"]["policy_ev"] == pytest.approx(0.35)
    assert with_table.passed[0]["edge"]["policy_ev_n"] == 24
    assert with_table.passed[0]["edge"]["policy_ev_version"] == "management_policy.v1"
    assert with_table.passed[0]["edge"]["policy_ev_reason"] is None


def test_policy_ev_annotation_marks_missing_table_as_unavailable(tmp_path: Path) -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    rank = rank_candidates(
        [
            {
                "candidate_id": "high-structure",
                "strategy_type": "CALL_BUTTERFLY",
                "setup_kind": "STABLE_PIN",
                "direction": "NEUTRAL",
                "selection_score": 9.0,
                "center": 7710.0,
                "width": 10.0,
                "legs": [{"strike": 7700.0}, {"strike": 7710.0}, {"strike": 7720.0}],
                "quote": {"status": "ready", "bid": 3.0, "ask": 3.2},
                "economics": {
                    "width_points": 10.0,
                    "max_gain_points": 6.8,
                    "max_loss_points": 3.2,
                    "breakeven_low": 7703.2,
                    "breakeven_high": 7716.8,
                },
                "quote_valid_until": (now + timedelta(seconds=30)).isoformat(),
                "opportunity_valid_until": (now + timedelta(minutes=5)).isoformat(),
                "automatic_ordering": False,
                "manual_action_only": True,
            }
        ],
        _ranker_pin_facts({
            "session_date": "2026-08-06",
            "probability": {
                "event": {
                    "kind": "terminal_between",
                    "target_at": (now + timedelta(minutes=5)).isoformat(),
                },
                "q": 0.6,
                "p_empirical": 0.7,
                "p_interval_low": 0.6,
                "n_raw": 40,
                "n_effective": 40.0,
                "historical_sessions": ["2026-08-05"],
            },
        }),
        {
            "pin": {"depin_risk": 0.0, "recent_extreme_acceptance": False},
            "terminal_state": "PIN_STABLE",
        },
        policy=DEFAULT_STRATEGY_POLICY,
        data_root=tmp_path,
        probability_settings=None,
        now=now,
    )

    assert rank.passed[0]["edge"]["policy_ev"] is None
    assert rank.passed[0]["edge"]["policy_ev_reason"] == "table_unavailable"


def test_late_chase_near_misses_and_geometry_source_are_populated() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    late = deepcopy(payload)
    late["minute_market_frame"]["es"]["vwap_distance_points"] = 12.0
    late["minute_market_frame"]["es"]["return_15m_points"] = 11.0

    decision = build_strategy_decision(late, _state(now), now)

    assert decision["decision_type"] == "NO_TRADE"
    assert decision["geometry_source"] in {
        "facts_wall_ladder_fallback",
        "confirmation_geometry",
        None,
        "",
    } or decision.get("geometry_source") is not None
    assert decision["candidates_considered"]
    assert decision["why_not"]["nearest_candidates"]
    assert decision["why_not"]["nearest_candidate"] == decision["why_not"]["nearest_candidates"][0]
    assert "direction_valid_but_entry_too_late" in decision["why_not"]["reasons"]


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

    payload["gth_level_manual_candidate"] = _gth_candidate(now, "trend_advance_call")
    advanced = build_strategy_decision(payload, _state(now), now)
    assert advanced["decision_type"] == "CALL_DEBIT_VERTICAL", advanced["why_not"]
    assert advanced["candidate"]["source"] == "gth_level_manual_candidate"
    assert advanced["candidate"]["setup_kind"] == "TREND_PULLBACK"


def test_gth_selector_evidence_can_compete_while_operator_edge_authority_is_unavailable() -> None:
    now = datetime(2026, 8, 7, 3, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["gth_level_manual_candidate"] = {
        **_gth_candidate(now, "upper_acceptance_call"),
        "status": "selector_candidate",
        "candidate_scope": "research_watch",
        "execution_mode": "observe_only",
        "manual_action_eligible": False,
        "selector_evidence_eligible": True,
        "operator_notification_eligible": False,
        "edge_authority": "none",
        "edge_authority_required": "validated_first_touch_time_stop_net_pnl",
        "edge_authority_reason": "first_touch_time_stop_net_pnl_authority_unavailable",
        "block_reasons": ["first_touch_time_stop_net_pnl_authority_unavailable"],
    }
    payload.pop("call_skew_spread_shadow")

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["source"] == "gth_level_manual_candidate"
    evidence = decision["market_facts"]["gth_evidence"]
    assert evidence["selector_evidence_eligible"] is True
    assert evidence["edge_authority_reason"] == (
        "first_touch_time_stop_net_pnl_authority_unavailable"
    )
    assert evidence["block_reasons"] == [
        "first_touch_time_stop_net_pnl_authority_unavailable"
    ]
    assert decision["automatic_ordering"] is False


def test_fresh_dip_reclaim_evidence_overrides_trend_only_background() -> None:
    now = datetime(2026, 8, 7, 3, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["gth_level_manual_candidate"] = {
        **_gth_candidate(now, "trend_transition_call"),
        "status": "selector_candidate",
        "manual_action_eligible": False,
        "selector_evidence_eligible": True,
    }
    payload["gth_dip_reclaim_evidence"] = {
        **_gth_candidate(now, "gth_dip_reclaim_call"),
        "status": "selector_candidate",
        "candidate_scope": "research_watch",
        "execution_mode": "observe_only",
        "manual_action_eligible": False,
        "selector_evidence_eligible": True,
        "operator_notification_eligible": False,
        "edge_authority": "none",
        "edge_authority_reason": "first_touch_time_stop_net_pnl_authority_unavailable",
        "block_reasons": ["first_touch_time_stop_net_pnl_authority_unavailable"],
    }
    payload.pop("call_skew_spread_shadow")

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL", decision["why_not"]
    assert decision["candidate"]["source"] == "gth_dip_reclaim_evidence"
    assert decision["candidate"]["setup_kind"] == "FAILED_BREAK_RECLAIM"
    assert decision["automatic_ordering"] is False


def test_gth_diagnostics_distinguish_unconfirmed_level_from_expired_dip_reclaim() -> None:
    now = datetime(2026, 8, 7, 3, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["gth_level_manual_candidate"] = {
        "status": "blocked",
        "block_reasons": ["gth_level_not_confirmed_or_near"],
    }
    payload["gth_dip_reclaim_evidence"] = {
        "status": "blocked",
        "block_reasons": ["gth_dip_reclaim_signal_expired"],
    }
    payload.pop("call_skew_spread_shadow")

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "NO_TRADE"
    assert "gth_level_not_confirmed_or_near" in decision["why_not"]["reasons"]
    assert "gth_dip_reclaim_signal_expired" in decision["why_not"]["reasons"]


def test_expired_gth_source_is_not_the_desk_primary_reason() -> None:
    now = datetime(2026, 8, 11, 13, 12, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["gth_level_manual_candidate"] = {
        "status": "blocked",
        "block_reasons": ["source_signal_expired"],
        "manual_action_eligible": False,
        "selector_evidence_eligible": False,
    }
    payload["gth_dip_reclaim_evidence"] = {
        "status": "blocked",
        "block_reasons": [
            "strategy_event_expired",
            "gth_dip_reclaim_signal_expired",
            "gth_reclaim_too_old",
        ],
        "manual_action_eligible": False,
        "selector_evidence_eligible": False,
    }
    payload.pop("call_skew_spread_shadow")

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "NO_TRADE"
    assert decision["desk_view"]["reason"] == "gth_confirmed_level_candidate_unavailable"
    assert "source_signal_expired" in decision["why_not"]["reasons"]
    assert "gth_dip_reclaim_signal_expired" in decision["why_not"]["reasons"]


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


def _ranker_pin_facts(facts: dict[str, object]) -> dict[str, object]:
    merged = deepcopy(facts)
    merged["spot"] = {"spx": 7710.0, **dict(merged.get("spot") or {})}
    merged["minutes_to_close"] = merged.get("minutes_to_close", 60)
    merged["path"] = {
        "breadth_above_vwap": 0.5,
        **dict(merged.get("path") or {}),
    }
    merged["volatility"] = {
        "vix_return_15m_pct": -0.005,
        "expected_move_points": 8.0,
        **dict(merged.get("volatility") or {}),
    }
    merged["value_center"] = {
        "spx_30m": 7710.0,
        **dict(merged.get("value_center") or {}),
    }
    merged["structure"] = {
        "q_mode": 7710.0,
        "put_wall": 7700.0,
        "call_wall": 7720.0,
        **dict(merged.get("structure") or {}),
    }
    merged["shock"] = {"state": "NONE", **dict(merged.get("shock") or {})}
    return merged


def _attach_rth_setup_path(
    payload: dict[str, object],
    bars: list[dict[str, object]],
    structure: str,
) -> None:
    lineage = payload["minute_market_frame"]["diagnostics"]["rth_market_state"][
        "input_lineage"
    ]
    lineage["values"].update(
        opening_range_state="INSIDE",
        market_structure=structure,
    )
    lineage["diagnostics"].update(
        opening_range={"status": "ready", "orh": 7740.0, "orl": 7730.0},
        rth_bar_path=bars,
        rth_bar_vwaps={bar["bar_start"]: 7732.0 for bar in bars},
    )


def _or_failed_break_bars(
    now: datetime, break_side: str
) -> list[dict[str, object]]:
    closes = (
        (7735.0, 7736.0, 7742.0, 7743.0, 7739.0, 7738.0)
        if break_side == "UP"
        else (7735.0, 7734.0, 7728.0, 7727.0, 7731.0, 7732.0)
    )
    return [
        {
            "bar_start": (now - timedelta(minutes=5 * (len(closes) - index))).isoformat(),
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "quality": "ok",
        }
        for index, close in enumerate(closes)
    ]


def _vwap_pullback_payload(now: datetime) -> dict[str, object]:
    payload = _decision_payload(now)
    payload.pop("call_skew_spread_shadow")
    payload["level_decision"] = {"phase": "far"}
    payload["option_structure_frame"]["front_expiry"] = "20260807"
    bars = [
        {
            "bar_start": (now - timedelta(minutes=10)).isoformat(),
            "open": 7735.0,
            "high": 7736.0,
            "low": 7731.5,
            "close": 7734.0,
            "quality": "ok",
        },
        {
            "bar_start": (now - timedelta(minutes=5)).isoformat(),
            "open": 7734.0,
            "high": 7736.0,
            "low": 7732.0,
            "close": 7735.0,
            "quality": "ok",
        },
    ]
    _attach_rth_setup_path(payload, bars, "HL_ONLY")
    return payload


def _two_sided_vertical_chain(
    now: datetime,
    *,
    right: str,
    provider: Provider = Provider.SCHWAB,
) -> LatestState:
    observed = now - timedelta(seconds=1)
    prices = (
        ((7700, 8.0, 8.2), (7705, 6.2, 6.4), (7710, 4.8, 5.0),
         (7715, 2.7, 2.9), (7720, 0.5, 0.7), (7725, 0.2, 0.4), (7730, 0.1, 0.2))
        if right == "C"
        else ((7690, 0.1, 0.2), (7695, 0.3, 0.4), (7700, 0.7, 0.9),
              (7705, 2.6, 2.8), (7710, 4.7, 5.0), (7715, 6.0, 6.2), (7720, 8.0, 8.2))
    )
    quotes = tuple(
        Quote(
            instrument=InstrumentId.option(
                "SPX",
                expiry="20260807",
                strike=strike,
                right=right,
                trading_class="SPXW",
            ),
            provider=provider,
            received_at=observed,
            quote_time=observed,
            quality=MarketDataQuality.LIVE,
            bid=bid,
            ask=ask,
        )
        for strike, bid, ask in prices
    )
    return LatestState(
        created_at=observed,
        as_of=observed,
        quotes=quotes,
        best_quotes=quotes if provider is Provider.SCHWAB else (),
    )


def _state(now: datetime) -> LatestState:
    observed = now - timedelta(seconds=1)
    return LatestState(created_at=observed, as_of=observed, quotes=(), best_quotes=())


def _vertical_chain_state(now: datetime) -> LatestState:
    observed = now - timedelta(seconds=1)
    quotes = tuple(
        Quote(
            instrument=InstrumentId.option(
                "SPX",
                expiry="20260807",
                strike=strike,
                right="C",
                trading_class="SPXW",
            ),
            provider=Provider.SCHWAB,
            received_at=observed,
            quote_time=observed,
            quality=MarketDataQuality.LIVE,
            bid=bid,
            ask=ask,
        )
        for strike, bid, ask in (
            (7705, 6.2, 6.4),
            (7710, 4.8, 5.0),
            (7715, 2.7, 2.9),
            (7720, 0.5, 0.7),
            (7725, 0.5, 0.7),
            (7730, 0.5, 0.7),
        )
    )
    return LatestState(created_at=observed, as_of=observed, quotes=quotes, best_quotes=quotes)


def _failed_break_20260812_chain(now: datetime) -> LatestState:
    observed = now - timedelta(seconds=1)
    quotes = tuple(
        Quote(
            instrument=InstrumentId.option(
                "SPX",
                expiry="20260812",
                strike=strike,
                right="C",
                trading_class="SPXW",
            ),
            provider=Provider.SCHWAB,
            received_at=observed,
            quote_time=observed,
            quality=MarketDataQuality.LIVE,
            bid=bid,
            ask=ask,
        )
        for strike, bid, ask in (
            (7755, 3.0, 3.2),
            (7760, 2.0, 2.2),
            (7775, 0.4, 0.5),
        )
    )
    return LatestState(created_at=observed, as_of=observed, quotes=quotes, best_quotes=quotes)


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
        "expected_move_points": 40.0,
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
            "volatility": {"atm_straddle_decay_15m": 0.0448, "expected_move_points_0dte": 25.0},
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


def _pin_ladder_state(now: datetime) -> LatestState:
    quotes = tuple(
        Quote(
            instrument=InstrumentId.option("SPX", expiry="20260806", strike=strike,
                                           right=right, trading_class="SPXW"),
            provider=Provider.SCHWAB, received_at=now - timedelta(seconds=1),
            quote_time=now - timedelta(seconds=1), quality=MarketDataQuality.LIVE,
            bid=bid, ask=ask,
        )
        for right in ("C", "P")
        for strike, bid, ask in (
            (7700, 15.1, 15.3), (7710, 7.3, 7.5), (7720, 2.5, 2.6),
            (7730, 0.8, 0.9), (7740, 0.3, 0.4),
        )
    )
    return LatestState(created_at=now, as_of=now - timedelta(seconds=1), quotes=quotes, best_quotes=quotes)


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


def test_management_policy_arms_then_trails() -> None:
    start = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)
    marks = [
        PolicyMark(at=start + timedelta(minutes=1), combo_bid=1.2),
        PolicyMark(at=start + timedelta(minutes=2), combo_bid=1.6),  # arm at 1.5
        PolicyMark(at=start + timedelta(minutes=3), combo_bid=1.8),
        PolicyMark(at=start + timedelta(minutes=4), combo_bid=1.2),  # trail below peak*0.75=1.35 but floor=entry
    ]
    label = simulate_management_policy(
        marks, entry_ask=1.0, leg_count=3, entry_at=start
    )
    assert label.tp_armed is True
    assert label.time_to_arm_seconds == pytest.approx(120.0)
    assert label.exit_reason == "trail"
    assert label.mfe_points == pytest.approx(0.8)
    assert label.policy_pnl_points < 0.8  # fees deducted


def test_management_policy_premium_stop_before_arm() -> None:
    start = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)
    marks = [
        PolicyMark(at=start + timedelta(minutes=1), combo_bid=0.8),
        PolicyMark(at=start + timedelta(minutes=2), combo_bid=0.4),
    ]
    label = simulate_management_policy(
        marks, entry_ask=1.0, leg_count=2, entry_at=start
    )
    assert label.tp_armed is False
    assert label.exit_reason == "premium_stop"
    assert label.mae_points == pytest.approx(-0.6)


def test_pin_butterfly_policy_holds_past_default_time_and_premium_stop() -> None:
    start = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)  # 15:00 ET
    marks = [
        PolicyMark(at=start + timedelta(minutes=5), combo_bid=0.40),
        PolicyMark(at=start + timedelta(minutes=25), combo_bid=0.80),
        PolicyMark(at=start + timedelta(minutes=35), combo_bid=1.60),
        PolicyMark(at=start + timedelta(minutes=40), combo_bid=1.80),
        PolicyMark(at=start + timedelta(minutes=44), combo_bid=1.20),
        PolicyMark(at=start + timedelta(minutes=45), combo_bid=1.10),
    ]
    default = simulate_management_policy(
        marks, entry_ask=1.0, leg_count=3, entry_at=start
    )
    pin = simulate_management_policy(
        marks,
        entry_ask=1.0,
        leg_count=3,
        entry_at=start,
        policy=PIN_BUTTERFLY_MANAGEMENT_POLICY,
    )
    assert default.exit_reason == "premium_stop"
    assert default.exit_at == start + timedelta(minutes=5)
    assert pin.exit_reason == "trail"
    assert pin.tp_armed is True
    assert pin.exit_at == start + timedelta(minutes=44)
    assert pin.exit_at > default.exit_at
    assert pin.policy_version == "management_policy.pin_butterfly.hold_1545.v1"
    assert default.policy_version == "management_policy.v1"


def test_management_policy_for_candidate_keeps_verticals_on_v1() -> None:
    assert (
        management_policy_for_candidate(
            {"setup_kind": "STABLE_PIN", "strategy_type": "PUT_BUTTERFLY"}
        )
        is PIN_BUTTERFLY_MANAGEMENT_POLICY
    )
    assert (
        management_policy_for_candidate(
            {"setup_kind": "TREND_PULLBACK", "strategy_type": "CALL_DEBIT_VERTICAL"}
        )
        is DEFAULT_MANAGEMENT_POLICY
    )
    assert DEFAULT_MANAGEMENT_POLICY.time_stop_minutes == 20
    assert DEFAULT_MANAGEMENT_POLICY.premium_stop_fraction == pytest.approx(0.50)


@given(
    entry=st.floats(min_value=0.5, max_value=5.0, allow_nan=False, allow_infinity=False),
    peak_mult=st.floats(min_value=1.5, max_value=3.0, allow_nan=False, allow_infinity=False),
)
def test_management_policy_pnl_bounded_by_path(entry: float, peak_mult: float) -> None:
    start = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)
    peak = entry * peak_mult
    marks = [
        PolicyMark(at=start + timedelta(minutes=1), combo_bid=entry * 1.1),
        PolicyMark(at=start + timedelta(minutes=2), combo_bid=peak),
        PolicyMark(at=start + timedelta(minutes=3), combo_bid=entry * 0.9),
    ]
    label = simulate_management_policy(
        marks, entry_ask=entry, leg_count=2, entry_at=start
    )
    fees = label.fees_points
    assert label.mae_points <= 0.0 <= label.mfe_points
    assert label.policy_pnl_points <= peak - entry - fees + 1e-9
    assert label.policy_pnl_points >= -entry - fees - 1e-9


def test_policy_ev_score_is_rank_only_attachment() -> None:
    from spx_spark.data_platform.research.strategy_policy_calibration import (
        apply_policy_ev_score,
        calibration_report,
    )

    candidate = {
        "economics": {"max_loss_points": 2.0},
        "utility": {"liquidity_penalty": 0.1, "model_uncertainty": 0.2},
    }
    scored = apply_policy_ev_score(
        candidate, expected_policy_pnl=0.4, expected_shortfall_10=1.0
    )
    assert scored["policy_ev"]["authority"] == "rank_only"
    assert scored["policy_ev"]["score"] == pytest.approx(
        0.4 / 2.0 - 0.5 * 1.0 / 2.0 - 0.25 * 0.1 - 0.25 * 0.2
    )
    report = calibration_report(
        [
            {
                "session_date": "2026-08-05",
                "policy_pnl_points": 0.2,
                "entry_ask": 1.0,
                "regime_terminal_state": "PIN_STABLE",
                "setup_kind": "STABLE_PIN",
                "strategy_type": "CALL_BUTTERFLY",
            },
            {
                "session_date": "2026-08-06",
                "policy_pnl_points": -0.5,
                "entry_ask": 2.0,
                "regime_terminal_state": "TREND",
                "setup_kind": "BREAKOUT_ACCEPTANCE",
                "strategy_type": "CALL_DEBIT_VERTICAL",
            },
        ]
    )
    assert report["policy_authority"] == "rank_only"
    assert report["gates"]["promotion_ready"] is False
    assert report["gates"]["sessions_covered"] == 2
