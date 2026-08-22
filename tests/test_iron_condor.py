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
    assert structure["short_abs_delta"] == 0.20
    assert structure["spot_inside_shorts"] is True
    assert structure["wing_width"] == 10.0
    assert structure["economics"]["put_width_points"] == 10.0
    assert structure["economics"]["call_width_points"] == 10.0
    assert structure["economics"]["max_loss_points"] <= 10.0
    assert structure["strikes"] == [7680.0, 7690.0, 7810.0, 7820.0]
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
    structure = build_iron_condor_map(
        _payload(),
        _facts(),
        _state_with_richer_20d(NOW),
        now=NOW,
        policy=StrategyPolicy(),
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

    assert StrategyPolicy().policy_version == "strategy_policy.bootstrap.v44"
    assert decision["policy_version"] == "strategy_policy.bootstrap.v44"
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
