from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.analytics.options.strategy_payoff import (
    conservative_iron_condor_bbo,
    iron_condor_economics,
)
from spx_spark.application.order_map.candidate_factory import GTH_WIDTH_SCAN, enumerate_candidates
from spx_spark.application.order_map.iron_condor import (
    IRON_CONDOR_DELTA,
    build_iron_condor_map,
    enumerate_iron_condor_candidates,
    iron_condor_session_state,
)
from spx_spark.application.order_map.operator_status import build_desk_message_sections
from spx_spark.application.order_map.strategy_ranker import rank_candidates
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.application.order_map.strategy_select import build_strategy_decision
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import LatestState

NOW = datetime(2026, 8, 13, 4, 30, tzinfo=timezone.utc)
RTH_NOW = datetime(2026, 8, 13, 14, 0, tzinfo=timezone.utc)
EXPIRY = "20260813"
SPOT = 7750.0


def _option(
    *,
    strike: float,
    right: str,
    bid: float,
    ask: float,
    now: datetime,
    delta: float | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=EXPIRY,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{EXPIRY}:{strike:g}:{right}",
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
        mark=(bid + ask) / 2.0,
        greeks=None if delta is None else OptionGreeks(delta=delta, implied_vol=0.18, gamma=0.01),
    )


def _delta(strike: float, right: str) -> float:
    raw = 0.5 + (SPOT - strike) / 200.0
    call = min(max(raw, 0.01), 0.99)
    return call if right == "C" else call - 1.0


def _price(strike: float, right: str) -> float:
    if right == "P":
        return max(0.35, (strike - (SPOT - 140.0)) * 0.08)
    return max(0.35, ((SPOT + 140.0) - strike) * 0.08)


def _quotes(now: datetime, *, with_greeks: bool) -> tuple[Quote, ...]:
    rows: list[Quote] = []
    for strike in range(int(SPOT - 150), int(SPOT + 155), 5):
        for right in ("C", "P"):
            mid = _price(float(strike), right)
            rows.append(
                _option(
                    strike=float(strike),
                    right=right,
                    bid=round(mid - 0.15, 2),
                    ask=round(mid + 0.15, 2),
                    now=now,
                    delta=_delta(float(strike), right) if with_greeks else None,
                )
            )
    return tuple(rows)


def _state(now: datetime, *, with_greeks: bool = True) -> LatestState:
    quotes = _quotes(now, with_greeks=with_greeks)
    return LatestState(created_at=now, as_of=now, quotes=quotes, best_quotes=quotes)


def _rth_state(
    now: datetime = RTH_NOW,
    *,
    short_quote_bonus: float = 0.75,
) -> LatestState:
    rows = []
    for quote in _quotes(now, with_greeks=True):
        right = str(getattr(quote.instrument.right, "value", quote.instrument.right) or "")
        rich_short = (
            (right == "P" and quote.instrument.strike == 7690.0)
            or (right == "C" and quote.instrument.strike == 7810.0)
        )
        rows.append(
            replace(
                quote,
                provider=Provider.SCHWAB,
                received_at=now,
                quote_time=now,
                bid=(quote.bid or 0.0) + (short_quote_bonus if rich_short else 0.0),
                ask=(quote.ask or 0.0) + (short_quote_bonus if rich_short else 0.0),
            )
        )
    quotes = tuple(rows)
    return LatestState(created_at=now, as_of=now, quotes=quotes, best_quotes=quotes)


def _facts() -> dict[str, object]:
    return {
        "schema_version": "market_fact_pack.v1",
        "decision_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
        "session_date": "2026-08-13",
        "minutes_to_close": 930,
        "session": {"mode": "gth", "legal": True},
        "spot": {"spx": SPOT},
        "path": {},
        "value_center": {},
        "volatility": {"expected_move_points": 40.0},
        "structure": {
            "put_wall": 7680.0,
            "call_wall": 7820.0,
            "flip_zone": [7730.0, 7735.0],
            "zero_gamma": 7740.0,
            "q_mode": SPOT,
            "strike_differential_context": {},
        },
        "event": {"state": "normal", "entry_allowed": True},
        "trigger": {},
        "session_episode": {},
        "rth_setups": [],
        "shock": {"state": "NONE"},
        "gth_evidence": {},
        "gth_dip_reclaim_evidence": {},
        "probability": {},
        "capabilities": {
            "global": {
                "ready": True,
                "session_legal": True,
                "coordinate_ready": True,
                "market_frame_ready": True,
                "macro_entry_allowed": True,
                "provider_advice_allowed": True,
                "reasons": [],
            },
            "vertical": {"ready": False, "reasons": ["vertical_path_inputs_unavailable"]},
            "butterfly": {"ready": False, "reasons": []},
            "path": {"ready": False},
        },
        "quality": {"status": "ready", "reasons": []},
    }


def _rth_facts(
    *,
    atm_iv: float | None = 0.20,
    put_skew: float | None = 0.03,
    call_skew: float | None = 0.02,
) -> dict[str, object]:
    facts = _facts()
    facts.update(
        {
            "decision_at": RTH_NOW.isoformat(),
            "available_at": RTH_NOW.isoformat(),
            "minutes_to_close": 360,
            "session": {"mode": "rth", "legal": True},
            "path": {
                "direction_score": 0.0,
                "efficiency_ratio_30m": 0.20,
                "vwap_crosses_30m": 3.0,
                "price_vs_vwap": "above",
                "breadth_above_vwap": 0.50,
                "vwap_slope": 0.0,
            },
            "volatility": {
                "expected_move_points": 40.0,
                "atm_iv_0dte": atm_iv,
                "put_skew_25d_0dte": put_skew,
                "call_skew_25d_0dte": call_skew,
                "vix1d_return_15m_pct": -0.01,
                "atm_iv_change_5m": -0.001,
                "atm_iv_change_15m": -0.002,
                "atm_straddle_decay_15m": 0.02,
            },
            "structure": {
                **facts["structure"],
                "gamma_state": "positive_gamma_pin",
            },
            "iron_condor_authority": {
                "status": "ready",
                "accepted_count": 0,
            },
        }
    )
    facts["capabilities"]["path"] = {"ready": True}
    return facts


def _payload() -> dict[str, object]:
    return {
        "expiry": EXPIRY,
        "session_phase": {"name": "asia_globex", "name_cn": "亚盘夜盘"},
        "option_structure_frame": {"front_expiry": EXPIRY, "quality": "ready"},
        "macro_event": {"mode": "normal", "entry_allowed": True},
    }


def test_iron_condor_credit_is_shorts_bid_minus_longs_ask() -> None:
    now = NOW
    put_long = {"bid": 1.2, "ask": 1.4, "provider": "ibkr", "source_at": now.isoformat()}
    put_short = {"bid": 8.1, "ask": 8.4, "provider": "ibkr", "source_at": now.isoformat()}
    call_short = {"bid": 7.6, "ask": 7.9, "provider": "ibkr", "source_at": now.isoformat()}
    call_long = {"bid": 1.1, "ask": 1.3, "provider": "ibkr", "source_at": now.isoformat()}

    quote = conservative_iron_condor_bbo(
        put_long, put_short, call_short, call_long, now=now
    )
    economics = iron_condor_economics(
        put_long=7650.0,
        put_short=7700.0,
        call_short=7800.0,
        call_long=7840.0,
        net_credit=float(quote["credit"]),
    )

    assert quote["status"] == "ready"
    assert quote["credit"] == quote["bid"] == 13.0
    assert economics["max_gain_points"] == 13.0
    assert economics["max_loss_points"] == 37.0
    assert 0.15 <= economics["credit_fraction_of_width"] <= 0.55


def test_gth_always_computes_ten_wide_5_20_delta_iron_condor_from_one_minute_quotes() -> None:
    structure = build_iron_condor_map(
        _payload(),
        _facts(),
        _state(NOW),
        now=NOW,
        policy=StrategyPolicy(),
    )
    rows = enumerate_iron_condor_candidates(
        _payload(),
        _facts(),
        _state(NOW),
        now=NOW,
        policy=StrategyPolicy(),
    )

    assert structure["status"] == "ready"
    assert structure["short_abs_delta"] == 0.10
    assert structure["spot_inside_shorts"] is True
    assert structure["wing_width"] == 10.0
    assert structure["economics"]["put_width_points"] == 10.0
    assert structure["economics"]["call_width_points"] == 10.0
    assert structure["economics"]["max_loss_points"] <= 10.0
    assert structure["strikes"] == [7660.0, 7670.0, 7830.0, 7840.0]
    assert structure["surface_decision_modifier"] <= 0.0
    assert structure["surface_attribution"]["authority"] == "structure_risk_only"
    assert [row["short_abs_delta"] for row in structure["variants"]] == [0.10, 0.15, 0.20]
    assert abs(structure["put_short"]["delta"]) <= 0.20
    assert abs(structure["call_short"]["delta"]) <= 0.20
    assert rows
    assert rows[0]["setup_kind"] == IRON_CONDOR_DELTA
    assert rows[0]["strategy_type"] == "IRON_CONDOR"
    assert len(rows[0]["legs"]) == 4


def _state_with_richer_20d(now: datetime) -> LatestState:
    quotes = []
    for quote in _quotes(now, with_greeks=True):
        right = str(getattr(quote.instrument.right, "value", quote.instrument.right) or "").upper()
        if right == "C" and quote.instrument.strike == 7810.0:
            quotes.append(
                replace(quote, greeks=OptionGreeks(delta=0.22, implied_vol=0.18, gamma=0.01))
            )
        elif right == "P" and quote.instrument.strike == 7690.0:
            quotes.append(
                replace(quote, greeks=OptionGreeks(delta=-0.22, implied_vol=0.18, gamma=0.01))
            )
        else:
            quotes.append(quote)
    packed = tuple(quotes)
    return LatestState(created_at=now, as_of=now, quotes=packed, best_quotes=packed)


def test_iron_condor_20d_picks_at_or_below_not_richer_nearest() -> None:
    policy = replace(StrategyPolicy(), iron_condor_short_deltas=(0.20,))
    structure = build_iron_condor_map(
        _payload(),
        _facts(),
        _state_with_richer_20d(NOW),
        now=NOW,
        policy=policy,
    )

    assert max(StrategyPolicy().gth_delta_targets) == 0.20
    assert 0.25 not in StrategyPolicy().gth_delta_targets
    assert structure["status"] == "ready"
    assert structure["short_abs_delta"] == 0.20
    assert structure["strikes"] == [7675.0, 7685.0, 7815.0, 7825.0]
    assert abs(structure["put_short"]["delta"]) <= 0.20
    assert abs(structure["call_short"]["delta"]) <= 0.20


def test_stale_quotes_do_not_build_an_iron_condor() -> None:
    stale = NOW - timedelta(seconds=90)
    structure = build_iron_condor_map(
        _payload(),
        _facts(),
        _state(stale),
        now=NOW,
        policy=StrategyPolicy(),
    )

    assert structure["status"] == "unavailable"
    assert enumerate_iron_condor_candidates(
        _payload(),
        _facts(),
        _state(stale),
        now=NOW,
        policy=StrategyPolicy(),
    ) == []


def test_strategy_decision_always_attaches_iron_condor_map(monkeypatch) -> None:
    facts = _facts()
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.build_market_fact_pack",
        lambda payload, latest, at: facts,
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.assess_regime",
        lambda supplied: {
            "schema_version": "regime_assessment.v1",
            "policy_version": StrategyPolicy().policy_version,
            "path_state": "UNCERTAIN",
            "path_direction": None,
            "terminal_state": "NONE",
            "event_state": "NORMAL",
            "entry_state": "INSUFFICIENT_DATA",
            "confidence": 0.0,
            "reasons": [],
            "contradictions": [],
            "pin": {},
        },
    )

    decision = build_strategy_decision(_payload(), _state(NOW), NOW)

    assert StrategyPolicy().policy_version == "strategy_policy.bootstrap.v61"
    assert decision["policy_version"] == "strategy_policy.bootstrap.v61"
    assert decision["decision_type"] == "NO_TRADE"
    assert decision["action_authority"] == "none"
    assert decision["candidate"] is None
    assert decision["iron_condor_map"]["status"] == "ready"
    assert decision["iron_condor_map"]["setup_kind"] == IRON_CONDOR_DELTA


def test_ready_iron_condor_is_map_only_not_a_human_winner() -> None:
    facts = _facts()
    rows = enumerate_iron_condor_candidates(
        _payload(),
        facts,
        _state(NOW),
        now=NOW,
        policy=StrategyPolicy(),
    )
    ranked = rank_candidates(
        rows,
        facts,
        {
            "schema_version": "regime_assessment.v1",
            "policy_version": StrategyPolicy().policy_version,
            "path_state": "UNCERTAIN",
            "path_direction": None,
            "terminal_state": "NONE",
            "event_state": "NORMAL",
            "entry_state": "INSUFFICIENT_DATA",
            "confidence": 0.0,
            "reasons": [],
            "contradictions": [],
            "pin": {},
        },
        policy=StrategyPolicy(),
        data_root=None,
        probability_settings=None,
        now=NOW,
    )

    assert rows
    assert rows[0]["strategy_type"] == "IRON_CONDOR"
    assert ranked.passed == []
    assert ranked.near_misses
    miss = ranked.near_misses[0]
    assert miss["strategy_type"] == "IRON_CONDOR"
    assert "iron_condor_not_human_authorized" in miss["rejection_reasons"]


def test_rth_human_iron_condor_uses_schwab_per_side_delta_until_1100() -> None:
    from spx_spark.application.order_map.delivery import _render_strategy_candidate

    at_1100 = datetime(2026, 8, 13, 15, 0, tzinfo=timezone.utc)
    rows = enumerate_iron_condor_candidates(
        _payload(),
        _rth_facts(),
        _rth_state(at_1100),
        now=at_1100,
        policy=StrategyPolicy(),
    )

    assert rows[0]["manual_authority_eligible"] is True
    assert rows[0]["put_short_abs_delta"] == 0.20
    assert rows[0]["call_short_abs_delta"] == 0.20
    assert rows[0]["put_short_distance_points"] == 60.0
    assert rows[0]["call_short_distance_points"] == 60.0
    assert {leg["provider"] for leg in rows[0]["legs"]} == {"schwab"}
    assert rows[0]["gamma_risk"] == {
        "status": "ready",
        "version": "iron_condor_gamma_risk.v1",
        "decision_effect": "explanation_only",
        "state": "LOW",
        "net_gamma_per_spx_point": 0.0,
        "delta_shock_10_trader_delta": 0.0,
        "gamma_loss_10_points": 0.0,
        "gcr10": 0.0,
        "gcr20": 0.0,
        "nearest_short_abs_delta": 0.2,
        "entry_gate_applied": False,
    }
    card = _render_strategy_candidate(
        {"market_facts": {"spot": {"spx": SPOT}}}, rows[0]
    )
    assert "逐边卖≤20Δ" in card
    assert "实际选腿 Put 20.0Δ（距SPX 60.0点） · Call 20.0Δ（距SPX 60.0点）" in card
    assert "Gamma风控（解释）：净Γ 0.0000 · 10点Delta冲击 0.0Δ" in card
    assert "GCR10 0.0% · 最近短腿 20.0Δ · LOW" in card

    after_window = at_1100 + timedelta(minutes=1)
    later = enumerate_iron_condor_candidates(
        _payload(),
        _rth_facts(),
        _rth_state(after_window),
        now=after_window,
        policy=StrategyPolicy(),
    )
    assert later[0]["manual_authority_eligible"] is False


def test_rth_iron_condor_gamma_risk_uses_signed_four_leg_gamma() -> None:
    quotes = []
    for quote in _rth_state().quotes:
        gamma = 0.01
        if quote.instrument.strike in {7680.0, 7820.0}:
            gamma = 0.005
        quotes.append(replace(quote, greeks=replace(quote.greeks, gamma=gamma)))
    state = LatestState(
        created_at=RTH_NOW,
        as_of=RTH_NOW,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )

    candidate = enumerate_iron_condor_candidates(
        _payload(),
        _rth_facts(),
        state,
        now=RTH_NOW,
        policy=StrategyPolicy(),
    )[0]

    assert candidate["quote"]["credit"] == 2.5
    assert candidate["gamma_risk"]["net_gamma_per_spx_point"] == -0.01
    assert candidate["gamma_risk"]["delta_shock_10_trader_delta"] == 10.0
    assert candidate["gamma_risk"]["gcr10"] == 0.20
    assert candidate["gamma_risk"]["state"] == "NORMAL"
    assert candidate["gamma_risk"]["entry_gate_applied"] is False


def test_expansion_to_contraction_uses_balanced_23pct_credit_lane() -> None:
    facts = _rth_facts()
    facts["rth_environment"] = {"state": "EXPANSION_TO_CONTRACTION"}
    rows = enumerate_iron_condor_candidates(
        _payload(),
        facts,
        _rth_state(short_quote_bonus=0.65),
        now=RTH_NOW,
        policy=StrategyPolicy(),
    )

    assert rows[0]["quote"]["credit"] == 2.3
    assert rows[0]["minimum_side_credit_share"] >= 0.25
    transition_state = iron_condor_session_state(
        _payload(), facts, rows, now=RTH_NOW
    )
    assert transition_state["status"] == "eligible"
    facts["iron_condor_session_state"] = transition_state
    ranked = rank_candidates(
        rows,
        facts,
        {
            "rth_environment": {
                "state": "EXPANSION_TO_CONTRACTION",
                "status": "ready",
            },
            "path_state": "BALANCED",
            "terminal_state": "NONE",
            "pin": {},
        },
        policy=StrategyPolicy(),
        data_root=None,
        probability_settings=None,
        now=RTH_NOW,
    )
    assert ranked.passed, ranked.near_misses

    balanced_facts = {**facts, "rth_environment": {"state": "VOL_CONTRACTION_BALANCE"}}
    balanced_facts.pop("iron_condor_session_state")
    balanced_state = iron_condor_session_state(
        _payload(), balanced_facts, rows, now=RTH_NOW
    )
    assert balanced_state["status"] == "waiting"


def test_rth_human_iron_condor_does_not_fall_back_to_ibkr_delta_map() -> None:
    ibkr_only = _state(RTH_NOW)
    structure = build_iron_condor_map(
        _payload(),
        _rth_facts(),
        ibkr_only,
        now=RTH_NOW,
        policy=StrategyPolicy(),
    )

    assert structure["status"] == "ready"
    assert structure["provider"] == "ibkr"
    assert enumerate_iron_condor_candidates(
        _payload(),
        _rth_facts(),
        ibkr_only,
        now=RTH_NOW,
        policy=StrategyPolicy(),
    ) == []


def test_rth_iron_condor_locks_first_qualifying_candidate_id() -> None:
    facts = _rth_facts()
    facts["iron_condor_session_state"] = {
        "status": "eligible",
        "candidate_id": "strategy-candidate:locked-first-strikes",
    }
    rows = enumerate_iron_condor_candidates(
        _payload(),
        facts,
        _rth_state(),
        now=RTH_NOW,
        policy=StrategyPolicy(),
    )
    ranked = rank_candidates(
        rows,
        facts,
        {
            "schema_version": "regime_assessment.v1",
            "policy_version": StrategyPolicy().policy_version,
            "path_state": "UNCERTAIN",
            "path_direction": None,
            "terminal_state": "NONE",
            "event_state": "NORMAL",
            "entry_state": "INSUFFICIENT_DATA",
            "confidence": 0.0,
            "reasons": [],
            "contradictions": [],
            "pin": {},
        },
        policy=StrategyPolicy(),
        data_root=None,
        probability_settings=None,
        now=RTH_NOW,
    )

    assert ranked.passed == []
    assert "iron_condor_session_candidate_locked" in ranked.near_misses[0][
        "rejection_reasons"
    ]


def test_rth_iron_condor_reaches_single_strategy_decision_authority(
    monkeypatch, tmp_path
) -> None:
    from spx_spark.application.order_map.strategy_edge_model import (
        apply_strategy_edge_authority,
    )

    facts = _rth_facts()
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.build_market_fact_pack",
        lambda payload, latest, at: facts,
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select._accepted_session_cards",
        lambda session_date: (),
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.apply_strategy_edge_authority",
        apply_strategy_edge_authority,
    )

    decision = build_strategy_decision(
        _payload(), _rth_state(), RTH_NOW, data_root=tmp_path
    )

    assert decision["decision_type"] == "IRON_CONDOR", decision["why_not"]
    assert decision["action_authority"] == "manual"
    assert decision["execution"]["order_type"] == "NET_CREDIT_LIMIT"
    assert decision["execution"]["automatic_ordering"] is False
    assert decision["candidate"]["short_abs_delta"] == 0.20
    assert decision["candidate"]["wing_width"] == 10.0
    assert decision["candidate"]["economics"]["credit_fraction_of_width"] == 0.25
    assert decision["candidate"]["human_surface_gate"]["passed"] is True
    assert "strategy_edge" in decision["candidate"]["edge"], decision["candidate"]["edge"]
    assert decision["candidate"]["edge"]["strategy_edge"]["status"] == (
        "explicit_policy_authority_unvalidated"
    )
    assert decision["targets"][0]["kind"] == "take_profit"
    assert decision["risk"]["management_plan"]["stop_buyback_multiple"] == 3.0

    low_credit = build_strategy_decision(
        _payload(),
        _rth_state(short_quote_bonus=0.70),
        RTH_NOW,
        data_root=tmp_path,
    )
    assert low_credit["decision_type"] == "NO_TRADE"
    assert "iron_condor_credit_fraction" in low_credit["why_not"]["reasons"]


def test_rth_iron_condor_surface_is_advisory_when_high_or_missing(
    monkeypatch, tmp_path
) -> None:
    from spx_spark.application.order_map.strategy_edge_model import (
        apply_strategy_edge_authority,
    )

    facts_by_case = (
        (_rth_facts(atm_iv=0.24), "iron_condor_atm_iv_high"),
        (
            _rth_facts(put_skew=0.05, call_skew=0.02),
            "iron_condor_smile_richness_high",
        ),
        (_rth_facts(atm_iv=None), "iron_condor_surface_gate_unavailable"),
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select._accepted_session_cards",
        lambda session_date: (),
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.apply_strategy_edge_authority",
        apply_strategy_edge_authority,
    )
    for facts, expected_reason in facts_by_case:
        monkeypatch.setattr(
            "spx_spark.application.order_map.strategy_select.build_market_fact_pack",
            lambda payload, latest, at, supplied=facts: supplied,
        )
        decision = build_strategy_decision(
            _payload(), _rth_state(), RTH_NOW, data_root=tmp_path
        )
        assert decision["decision_type"] == "IRON_CONDOR", decision["why_not"]
        context = decision["candidate"]["human_surface_gate"]
        assert context["blocking"] is False
        assert context["decision_effect"] == "explanation_only"
        assert expected_reason in context["reasons"]


def test_rth_iron_condor_surface_warning_does_not_poison_session(
    monkeypatch, tmp_path
) -> None:
    from spx_spark.application.order_map.strategy_edge_model import (
        apply_strategy_edge_authority,
    )

    later = RTH_NOW + timedelta(minutes=1)
    high_facts = _rth_facts(atm_iv=0.24)
    normal_facts = _rth_facts(atm_iv=0.20)
    normal_facts["decision_at"] = later.isoformat()
    normal_facts["available_at"] = later.isoformat()
    pending_facts = iter((high_facts, normal_facts))
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.build_market_fact_pack",
        lambda payload, latest, at: next(pending_facts),
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select._accepted_session_cards",
        lambda session_date: (),
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.apply_strategy_edge_authority",
        apply_strategy_edge_authority,
    )

    first = build_strategy_decision(
        _payload(), _rth_state(), RTH_NOW, data_root=tmp_path
    )
    next_payload = _payload()
    next_payload["previous_strategy_decision"] = first
    second = build_strategy_decision(
        next_payload, _rth_state(later), later, data_root=tmp_path
    )

    first_state = first["market_facts"]["iron_condor_session_state"]
    second_state = second["market_facts"]["iron_condor_session_state"]
    assert first["decision_type"] == "IRON_CONDOR", first["why_not"]
    assert first_state["status"] == "eligible"
    assert first_state["surface_gate"]["atm_iv_0dte"] == 0.24
    assert first_state["surface_gate"]["blocking"] is False
    assert second["decision_type"] == "IRON_CONDOR", second["why_not"]
    assert second_state["status"] == "eligible"
    assert second_state["carried_forward"] is True


def test_gth_desk_map_shows_iron_condor_not_empty_heartbeat() -> None:
    payload = _payload()
    payload["strategy_decision"] = {
        "decision_type": "NO_TRADE",
        "candidate": None,
        "action_authority": "none",
        "execution": {"action": "WAIT"},
        "iron_condor_map": {
            "status": "ready",
            "short_abs_delta": 0.20,
            "wing_width": 10.0,
            "strikes": [7680.0, 7690.0, 7810.0, 7820.0],
            "quote": {"credit": 2.4},
            "economics": {"max_gain_points": 2.4, "max_loss_points": 7.6, "width_points": 10.0},
            "reason": None,
        },
        "rejection_funnel": {"candidate_enumerated": 64, "hard_gate_pass": 0},
        "data_quality": {"status": "ready", "reasons": []},
        "why_not": {
            "reasons": ["max_debit_fraction_exceeded"],
            "nearest_candidate": {
                "strategy_type": "PUT_DEBIT_VERTICAL",
                "long": {"strike": 7730.0},
                "short": {"strike": 7725.0},
            },
        },
    }

    sections = build_desk_message_sections(payload, NOW)

    assert "心跳 · 健康检查" not in sections.desk_view
    assert "心跳 · 非交易卡" not in sections.execution
    assert "可看 ·" not in sections.desk_view
    assert "7730/7725" not in sections.desk_view
    assert "结论  不做" in sections.desk_view
    assert "扫描赢家已推送" not in sections.desk_view
    assert "无过门赢家" not in sections.desk_view
    assert "卖20Δ 10宽 7680/7690/7810/7820 贷记 2.4 最大亏损 7.6" in sections.desk_view
    assert "卖20Δ 10宽 7680/7690/7810/7820 贷记 2.4 最大亏损 7.6" in sections.structure
    assert "扫描中 · 仅人工候选可做" in sections.execution


def test_gth_width_scan_adds_delta_anchors_when_greeks_exist() -> None:
    rows = enumerate_candidates(
        _payload(),
        _facts(),
        {
            "schema_version": "regime_assessment.v1",
            "policy_version": StrategyPolicy().policy_version,
            "path_state": "UNCERTAIN",
            "terminal_state": "NONE",
            "pin": {},
        },
        _state(NOW),
        now=NOW,
        policy=StrategyPolicy(),
    )
    longs = {
        float(row["long"]["strike"])
        for row in rows
        if row.get("setup_kind") in {GTH_WIDTH_SCAN, "GTH_DELTA_SCAN"}
    }
    delta_scan_longs = [
        row for row in rows if row.get("setup_kind") == "GTH_DELTA_SCAN"
    ]

    assert {7700.0, 7800.0}.isdisjoint(longs)
    assert {7745.0, 7750.0, 7755.0} <= longs
    assert len(rows) > 30
    assert all(
        abs(float(row["long"]["delta"])) <= 0.20 + 1e-12 for row in delta_scan_longs
    )
