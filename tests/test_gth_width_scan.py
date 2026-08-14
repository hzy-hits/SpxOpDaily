from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map.candidate_factory import (
    GTH_ATM_PIN,
    GTH_WIDTH_SCAN,
    enumerate_candidates,
)
from spx_spark.application.order_map.operator_status import build_desk_message_sections
from spx_spark.application.order_map.strategy_ranker import rank_candidates
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.application.order_map.strategy_select import build_strategy_decision
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
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
    )


def _quotes(now: datetime) -> tuple[Quote, ...]:
    rows: list[Quote] = []
    for strike in range(int(SPOT - 50), int(SPOT + 55), 5):
        call_debit = max(0.4, (SPOT + 40 - strike) * 0.08)
        put_debit = max(0.4, (strike - (SPOT - 40)) * 0.08)
        rows.append(
            _option(
                strike=float(strike),
                right="C",
                bid=round(call_debit - 0.1, 2),
                ask=round(call_debit + 0.1, 2),
                now=now,
            )
        )
        rows.append(
            _option(
                strike=float(strike),
                right="P",
                bid=round(put_debit - 0.1, 2),
                ask=round(put_debit + 0.1, 2),
                now=now,
            )
        )
    return tuple(rows)


def _state(now: datetime) -> LatestState:
    quotes = _quotes(now)
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


def _regime() -> dict[str, object]:
    return {
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
    }


def _payload() -> dict[str, object]:
    return {
        "expiry": EXPIRY,
        "session_phase": {"name": "asia_globex", "name_cn": "亚盘夜盘"},
        "option_structure_frame": {"front_expiry": EXPIRY, "quality": "ready"},
        "macro_event": {"mode": "normal", "entry_allowed": True},
    }


def test_gth_enumerates_fresh_width_verticals_and_butterflies() -> None:
    rows = enumerate_candidates(
        _payload(),
        _facts(),
        _regime(),
        _state(NOW),
        now=NOW,
        policy=StrategyPolicy(),
    )
    verticals = [row for row in rows if row.get("setup_kind") == GTH_WIDTH_SCAN]
    butterflies = [row for row in rows if row.get("setup_kind") == GTH_ATM_PIN]
    widths = {
        abs(float(row["long"]["strike"]) - float(row["short"]["strike"]))
        for row in verticals
    }

    assert {row["strategy_type"] for row in verticals} == {
        "CALL_DEBIT_VERTICAL",
        "PUT_DEBIT_VERTICAL",
    }
    assert widths <= set(StrategyPolicy().gth_widths)
    assert 5.0 in widths
    assert 50.0 not in widths
    assert butterflies
    assert all(row["quote"]["status"] == "ready" for row in verticals)


def test_gth_ignores_quotes_older_than_one_minute() -> None:
    stale = NOW - timedelta(seconds=90)
    rows = enumerate_candidates(
        _payload(),
        _facts(),
        _regime(),
        _state(stale),
        now=NOW,
        policy=StrategyPolicy(),
    )

    assert rows == []


def test_gth_scan_pushes_only_the_ranked_winner(monkeypatch) -> None:
    facts = _facts()
    regime = _regime()
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.build_market_fact_pack",
        lambda payload, latest, at: facts,
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.strategy_select.assess_regime",
        lambda supplied: regime,
    )
    decision = build_strategy_decision(
        _payload(),
        _state(NOW),
        NOW,
        data_root=None,
        probability_settings=None,
    )
    ranked = rank_candidates(
        enumerate_candidates(
            _payload(),
            facts,
            regime,
            _state(NOW),
            now=NOW,
            policy=StrategyPolicy(),
        ),
        facts,
        regime,
        policy=StrategyPolicy(),
        data_root=None,
        probability_settings=None,
        now=NOW,
    )

    assert StrategyPolicy().policy_version == "strategy_policy.bootstrap.v14"
    assert ranked.passed
    assert decision["action_authority"] == "manual"
    assert decision["decision_type"] in {
        "CALL_DEBIT_VERTICAL",
        "PUT_DEBIT_VERTICAL",
        "CALL_BUTTERFLY",
        "PUT_BUTTERFLY",
    }
    assert decision["candidate"]["setup_kind"] in {GTH_WIDTH_SCAN, GTH_ATM_PIN}
    assert decision["candidate"]["candidate_id"] == ranked.passed[0]["candidate_id"]


def test_gth_desk_map_shows_scan_not_empty_heartbeat_when_a_winner_exists() -> None:
    payload = _payload()
    payload["strategy_decision"] = {
        "decision_type": "CALL_DEBIT_VERTICAL",
        "action_authority": "manual",
        "candidate": {
            "strategy_type": "CALL_DEBIT_VERTICAL",
            "setup_kind": GTH_WIDTH_SCAN,
            "source": "gth_ibkr_width_enumeration",
            "long": {"strike": 7750.0},
            "short": {"strike": 7760.0},
            "opportunity_id": "strategy-opportunity:winner",
        },
        "execution": {"action": "MANUAL_LIMIT"},
        "iron_condor_map": {
            "status": "ready",
            "short_abs_delta": 0.20,
            "wing_width": 10.0,
            "strikes": [7680.0, 7690.0, 7810.0, 7820.0],
            "quote": {"credit": 8.0},
            "economics": {"max_gain_points": 8.0, "max_loss_points": 2.0, "width_points": 10.0},
        },
        "rejection_funnel": {"candidate_enumerated": 40, "hard_gate_pass": 1},
        "data_quality": {"status": "ready", "reasons": []},
        "why_not": {"reasons": []},
    }

    sections = build_desk_message_sections(payload, NOW)

    assert "心跳 · 健康检查" not in sections.desk_view
    assert "最近候选  无" not in sections.desk_view
    assert "可看 ·" not in sections.desk_view
    assert "卖20Δ 10宽 7680/7690/7810/7820 贷记 8 最大亏损 2" in sections.desk_view
    assert "扫描中 · 铁鹰已标位" in sections.execution
