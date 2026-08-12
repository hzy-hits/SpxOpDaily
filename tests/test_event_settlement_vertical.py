from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from spx_spark.application.order_map.candidate_factory import enumerate_candidates
from spx_spark.application.order_map.strategy_ranker import rank_candidates
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.application.order_map.strategy_select import _gate_reasons
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


def _option(
    *,
    expiry: str,
    strike: float,
    right: str,
    bid: float,
    ask: float,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike:g}:{right}",
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
        mark=(bid + ask) / 2.0,
    )


def _state(now: datetime) -> LatestState:
    expiry = "20260812"
    quotes = (
        _option(expiry=expiry, strike=7725.0, right="C", bid=7.9, ask=8.1, now=now),
        _option(expiry=expiry, strike=7730.0, right="C", bid=5.4, ask=5.6, now=now),
        _option(expiry=expiry, strike=7735.0, right="C", bid=3.1, ask=3.3, now=now),
        _option(expiry=expiry, strike=7725.0, right="P", bid=3.8, ask=4.0, now=now),
        _option(expiry=expiry, strike=7730.0, right="P", bid=6.3, ask=6.5, now=now),
        _option(expiry=expiry, strike=7735.0, right="P", bid=8.9, ask=9.1, now=now),
    )
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=quotes,
        best_quotes=quotes,
    )


def _payload() -> dict[str, object]:
    return {
        "expiry": "20260812",
        "day_move": {"prior_close": 7728.2},
        "option_structure_frame": {"front_expiry": "20260812"},
        "macro_event": {
            "mode": "normal",
            "entry_allowed": True,
            "active_event": None,
            "next_event": {
                "id": "us-cpi-2026-08-12",
                "name": "US CPI",
                "impact": "high",
                "release_at": "2026-08-12T08:30:00-04:00",
            },
        },
    }


def test_prior_close_event_view_enumerates_adjacent_call_and_put_verticals() -> None:
    now = datetime(2026, 8, 12, 4, 30, tzinfo=timezone.utc)
    policy = StrategyPolicy()

    rows = enumerate_candidates(
        _payload(),
        {"session_date": "2026-08-12"},
        {},
        _state(now),
        now=now,
        policy=policy,
    )

    event_rows = [
        row for row in rows if row.get("setup_kind") == "EVENT_SETTLEMENT_THRESHOLD"
    ]
    assert len(event_rows) == 4
    assert {row["strategy_type"] for row in event_rows} == {
        "CALL_DEBIT_VERTICAL",
        "PUT_DEBIT_VERTICAL",
    }
    assert {
        (row["long"]["strike"], row["short"]["strike"])
        for row in event_rows
    } == {
        (7725.0, 7730.0),
        (7730.0, 7735.0),
        (7735.0, 7730.0),
        (7730.0, 7725.0),
    }
    assert all(row["manual_authority_eligible"] is True for row in event_rows)
    assert all(row["event_spans_release"] is True for row in event_rows)
    assert all(row["view"]["threshold_level"] == 7728.2 for row in event_rows)
    assert all(row["view"]["evidence_status"] == "thesis_driven_unvalidated" for row in event_rows)
    call_7730_7735 = next(
        row
        for row in event_rows
        if row["strategy_type"] == "CALL_DEBIT_VERTICAL"
        and row["long"]["strike"] == 7730.0
    )
    assert call_7730_7735["quote"]["ask"] == 2.5
    assert call_7730_7735["view"]["market_odds_proxy"] == 0.5
    assert call_7730_7735["probability_event"]["kind"] == "terminal_above"


def test_event_view_can_pass_pre_event_macro_gate_without_path_geometry() -> None:
    now = datetime(2026, 8, 12, 4, 30, tzinfo=timezone.utc)
    policy = StrategyPolicy()
    rows = enumerate_candidates(
        _payload(),
        {"session_date": "2026-08-12"},
        {},
        _state(now),
        now=now,
        policy=policy,
    )
    event_rows = [
        row for row in rows if row.get("setup_kind") == "EVENT_SETTLEMENT_THRESHOLD"
    ]
    facts = {
        "session_date": "2026-08-12",
        "event": {"state": "pre_event", "entry_allowed": False},
        "spot": {"spx": 7727.0},
        "path": {},
        "structure": {"strike_differential_context": {}},
        "shock": {"state": "NONE"},
        "probability": {},
    }
    regime = {"path_state": "UNCERTAIN", "terminal_state": "NONE", "pin": {}}

    ranked = rank_candidates(
        event_rows,
        facts,
        regime,
        policy=policy,
        data_root=None,
        probability_settings=None,
        now=now,
    )

    assert len(ranked.passed) == 4
    assert all(
        row["edge"]["edge_status"] == "thesis_driven_unvalidated"
        for row in ranked.passed
    )
    assert all(
        "candidate_probability_unavailable" in row["edge"]["advisories"]
        for row in ranked.passed
    )

    regular = deepcopy(event_rows[0])
    regular.update(
        {
            "candidate_id": "regular-candidate",
            "setup_kind": "TREND_PULLBACK",
            "event_spans_release": False,
            "target_spx": 7750.0,
            "invalidation_spx": 7715.0,
        }
    )
    blocked = rank_candidates(
        [regular],
        facts,
        regime,
        policy=policy,
        data_root=None,
        probability_settings=None,
        now=now,
    )
    assert not blocked.passed
    assert "macro_entry_not_authorized" in blocked.near_misses[0]["rejection_reasons"]


def test_macro_reason_is_no_longer_a_global_enumeration_veto() -> None:
    reasons = _gate_reasons(
        {
            "capabilities": {
                "global": {
                    "reasons": [
                        "macro_entry_not_authorized",
                        "market_frame_not_ready",
                    ]
                }
            }
        },
        {},
    )

    assert reasons == ["market_frame_not_ready"]


def test_event_view_expires_after_the_release() -> None:
    now = datetime(2026, 8, 12, 13, 0, tzinfo=timezone.utc)

    rows = enumerate_candidates(
        _payload(),
        {"session_date": "2026-08-12"},
        {},
        _state(now),
        now=now,
        policy=StrategyPolicy(),
    )

    assert not any(
        row.get("setup_kind") == "EVENT_SETTLEMENT_THRESHOLD" for row in rows
    )
