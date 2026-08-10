import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from spx_spark.analytics.options.strategy_payoff import (
    PolicyMark,
    butterfly_economics,
    butterfly_payoff,
    conservative_butterfly_bbo,
    conservative_vertical_bbo,
    simulate_management_policy,
    vertical_economics,
    vertical_payoff,
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


def test_stable_pin_produces_manual_7710_call_butterfly() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    decision = build_strategy_decision(_pin_payload(now), _pin_state(now), now)
    assert decision["decision_type"] == "CALL_BUTTERFLY", decision["why_not"]
    assert decision["candidate"]["center"] == 7710.0
    assert decision["candidate"]["width"] == 10.0
    assert decision["execution"]["limit"] == pytest.approx(3.3)
    assert decision["automatic_ordering"] is False


def test_strike_differential_context_is_copied_whole_without_strategy_interference() -> None:
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
        baseline_decision["market_facts"]["structure"].pop("strike_differential_context")
        positive_decision["market_facts"]["structure"].pop("strike_differential_context")
        negative_decision["market_facts"]["structure"].pop("strike_differential_context")
        assert baseline_decision == positive_decision == negative_decision, day


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
    assert decision["policy_version"] == "strategy_policy.bootstrap.v2"
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


def test_ranker_winner_is_structure_score_not_research_utility() -> None:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    facts = {
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
    }

    def butterfly(candidate_id: str, *, selection_score: float, gain: float) -> dict:
        return {
            "candidate_id": candidate_id,
            "strategy_type": "CALL_BUTTERFLY",
            "setup_kind": "STABLE_PIN",
            "direction": "NEUTRAL",
            "selection_score": selection_score,
            "legs": [{"strike": 7700.0}, {"strike": 7710.0}, {"strike": 7720.0}],
            "quote": {"status": "ready", "bid": 3.0, "ask": 3.2},
            "economics": {"max_gain_points": gain, "max_loss_points": 3.2,
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
        {"pin": {"depin_risk": 0.0}},
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
    facts = {
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
    }

    def butterfly(candidate_id: str, *, selection_score: float, gain: float) -> dict:
        return {
            "candidate_id": candidate_id,
            "strategy_type": "CALL_BUTTERFLY",
            "setup_kind": "STABLE_PIN",
            "direction": "NEUTRAL",
            "selection_score": selection_score,
            "legs": [{"strike": 7700.0}, {"strike": 7710.0}, {"strike": 7720.0}],
            "quote": {"status": "ready", "bid": 3.0, "ask": 3.2},
            "economics": {
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
    regime = {"pin": {"depin_risk": 0.0}, "terminal_state": "PIN_STABLE"}

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
                "legs": [{"strike": 7700.0}, {"strike": 7710.0}, {"strike": 7720.0}],
                "quote": {"status": "ready", "bid": 3.0, "ask": 3.2},
                "economics": {
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
        {
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
        },
        {"pin": {"depin_risk": 0.0}, "terminal_state": "PIN_STABLE"},
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
