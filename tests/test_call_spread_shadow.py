from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.analytics.greeks.black_scholes import bs_delta, bs_gamma, bs_price
from spx_spark.analytics.options.pricing import time_to_expiry_years
from spx_spark.application.order_map.call_spread_shadow import (
    build_call_skew_spread_shadow,
    build_put_skew_spread_shadow,
)
from spx_spark.application.order_map.prompts import (
    _status_writer_payload,
    render_feishu_delivery_text,
    render_status_template,
)
from spx_spark.application.order_map.state import material_changes, payload_fingerprint
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import LatestState


NOW = datetime(2026, 7, 20, 15, 0, tzinfo=timezone.utc)
EXPIRY = "20260720"
SPOT = 7500.0


def _call(
    strike: float,
    iv: float,
    spread: float,
    *,
    now: datetime = NOW,
    bid_size: float = 20.0,
    ask_size: float = 20.0,
    right: str = "C",
) -> Quote:
    tau = time_to_expiry_years(EXPIRY, as_of=now)
    mid = bs_price(SPOT, strike, iv, tau, right)
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=EXPIRY,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{EXPIRY}:{strike}:{right}",
        received_at=now,
        last_update_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        bid=max(mid - spread / 2.0, 0.05),
        ask=mid + spread / 2.0,
        bid_size=bid_size,
        ask_size=ask_size,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=bs_delta(SPOT, strike, iv, tau, right),
            gamma=bs_gamma(SPOT, strike, iv, tau),
            theta=-1.0,
            vega=2.0,
            underlier_price=SPOT,
            model="test_bs",
        ),
    )


def _state(*, missing_short_size: bool = False) -> LatestState:
    rows = [
        _call(7485.0, 0.200, 0.08),
        _call(7490.0, 0.198, 0.09),
        _call(7495.0, 0.196, 0.10),
        _call(7500.0, 0.194, 0.11),
        _call(7505.0, 0.192, 0.12),
        _call(7510.0, 0.190, 0.13),
        _call(
            7515.0,
            0.220,
            0.15,
            bid_size=0.0 if missing_short_size else 5.0,
            ask_size=5.0,
        ),
        _call(7520.0, 0.218, 0.16, bid_size=5.0, ask_size=5.0),
        _call(7525.0, 0.184, 0.50, bid_size=2.0, ask_size=2.0),
        _call(7475.0, 0.184, 0.50, bid_size=2.0, ask_size=2.0, right="P"),
        _call(7480.0, 0.218, 0.16, bid_size=5.0, ask_size=5.0, right="P"),
        _call(7485.0, 0.220, 0.15, bid_size=5.0, ask_size=5.0, right="P"),
        _call(7490.0, 0.190, 0.13, right="P"),
        _call(7495.0, 0.192, 0.12, right="P"),
        _call(7500.0, 0.194, 0.11, right="P"),
        _call(7505.0, 0.196, 0.10, right="P"),
        _call(7510.0, 0.198, 0.09, right="P"),
        _call(7515.0, 0.200, 0.08, right="P"),
    ]
    return LatestState(
        created_at=NOW,
        as_of=NOW,
        quotes=tuple(rows),
        best_quotes=tuple(rows),
    )


def test_selector_finds_positive_executable_call_skew_vertical() -> None:
    shadow = build_call_skew_spread_shadow(
        _state(),
        expiry=EXPIRY,
        spot=SPOT,
        now=NOW,
    )

    assert shadow["status"] == "candidate"
    assert shadow["automatic_ordering"] is False
    candidate = shadow["candidate"]
    assert candidate["strategy"] == "long_call_vertical"
    assert candidate["long"]["strike"] < candidate["short"]["strike"] == 7515.0
    assert candidate["executable_debit"] > 0
    assert candidate["edge_points"] >= 0.10
    assert candidate["iv_fit"]["short_iv_richness_vol_points"] >= 0.5
    assert candidate["iv_fit"]["adjacent_confirmation_strike"] == 7520.0
    assert candidate["defined_risk"]["max_loss_usd"] == (candidate["executable_debit"] * 100)
    assert candidate["execution"] == {
        "order_style": "combo_net_debit_limit_shadow",
        "net_debit_reference": candidate["executable_debit"],
        "leg_orders_prohibited": True,
        "max_leg_time_skew_seconds": 5.0,
        "automatic_ordering": False,
    }


def test_selector_finds_mirrored_positive_executable_put_skew_vertical() -> None:
    shadow = build_put_skew_spread_shadow(
        _state(),
        expiry=EXPIRY,
        spot=SPOT,
        now=NOW,
    )

    assert shadow["status"] == "candidate"
    candidate = shadow["candidate"]
    assert candidate["strategy"] == "long_put_vertical"
    assert candidate["long"]["strike"] > candidate["short"]["strike"] == 7485.0
    assert candidate["long"]["right"] == candidate["short"]["right"] == "P"
    assert candidate["executable_debit"] > 0
    assert candidate["edge_points"] >= 0.10
    assert candidate["iv_fit"]["adjacent_confirmation_strike"] == 7480.0
    assert candidate["net_greeks"]["delta"] < 0
    assert candidate["defined_risk"]["breakeven_spx"] < candidate["long"]["strike"]


def test_selector_rejects_missing_short_liquidity_instead_of_treating_it_as_edge() -> None:
    shadow = build_call_skew_spread_shadow(
        _state(missing_short_size=True),
        expiry=EXPIRY,
        spot=SPOT,
        now=NOW,
    )

    assert shadow["status"] == "no_candidate"
    assert shadow["candidate"] is None
    assert shadow["diagnostics"]["reject_counts"]["bid_size_unavailable"] == 1


def test_selector_is_unavailable_outside_rth() -> None:
    outside_rth = datetime(2026, 7, 20, 11, 0, tzinfo=timezone.utc)
    shadow = build_call_skew_spread_shadow(
        _state(),
        expiry=EXPIRY,
        spot=SPOT,
        now=outside_rth,
    )

    assert shadow["status"] == "unavailable"
    assert shadow["reason"] == "rth_only_live_spxw_required"
    assert shadow["candidate"] is None


def test_order_payload_always_attaches_shadow_contract(monkeypatch) -> None:
    import spx_spark.application.order_map.service as service

    expected = {
        "call_skew_spread_shadow": {
            "status": "unavailable",
            "automatic_ordering": False,
        },
        "put_skew_spread_shadow": {
            "status": "unavailable",
            "automatic_ordering": False,
        },
    }
    monkeypatch.setattr(service, "_build_spread_shadows", lambda *args, **kwargs: expected)
    empty = LatestState(created_at=NOW, as_of=NOW, quotes=(), best_quotes=())

    payload = service.build_order_payload(empty, now=NOW)

    assert payload["call_skew_spread_shadow"] is expected["call_skew_spread_shadow"]
    assert payload["put_skew_spread_shadow"] is expected["put_skew_spread_shadow"]


def test_15_minute_report_keeps_shadow_visible_and_non_actionable() -> None:
    shadow = build_call_skew_spread_shadow(
        _state(),
        expiry=EXPIRY,
        spot=SPOT,
        now=NOW,
    )
    put_shadow = build_put_skew_spread_shadow(
        _state(),
        expiry=EXPIRY,
        spot=SPOT,
        now=NOW,
    )
    payload = {
        "research_only": False,
        "expiry": EXPIRY,
        "session_phase": {"name": "us_open_hour", "name_cn": "美盘上午主战场"},
        "underlier": {"price": SPOT, "source": "index:SPX"},
        "gamma_state": "zero_gamma_transition",
        "level_decision": {"phase": "far"},
        "plan_candidates": [],
        "wall_ladder": {"put_walls": [], "call_walls": []},
        "call_skew_spread_shadow": shadow,
        "put_skew_spread_shadow": put_shadow,
        "warnings": [],
    }

    compact = render_status_template(payload, [], NOW)
    delivery = render_feishu_delivery_text(payload, [], NOW, "决策摘要")
    writer_payload = _status_writer_payload(payload)

    assert "Call Spread Shadow" in compact
    assert "Put Spread Shadow" in compact
    assert "只读" in compact
    assert "## Call / Put Skew Spread Shadow" in delivery
    assert "### Call" in delivery
    assert "### Put" in delivery
    assert "仅组合净借记限价；禁止拆腿" in delivery
    assert writer_payload["call_skew_spread_shadow"]["automatic_ordering"] is False


def test_research_only_report_explains_why_shadow_is_unavailable() -> None:
    outside_rth = datetime(2026, 7, 20, 11, 0, tzinfo=timezone.utc)
    shadow = build_call_skew_spread_shadow(
        _state(),
        expiry=EXPIRY,
        spot=SPOT,
        now=outside_rth,
    )
    put_shadow = build_put_skew_spread_shadow(
        _state(),
        expiry=EXPIRY,
        spot=SPOT,
        now=outside_rth,
    )
    payload = {
        "research_only": True,
        "research_reference": {"price": 7502.0, "source": "future:ES"},
        "pricing_reference": {"gate_state": "missing"},
        "expiry": EXPIRY,
        "beijing_time": "19:00",
        "session_phase": {"name_cn": "美盘前"},
        "gamma_state": "unknown",
        "call_skew_spread_shadow": shadow,
        "put_skew_spread_shadow": put_shadow,
        "warnings": [],
    }

    text = render_status_template(payload, [], outside_rth)

    assert "Call Spread Shadow  不可用" in text
    assert "Put Spread Shadow  不可用" in text
    assert "仅 RTH 实时 SPXW 双边链" in text


def test_selector_rejects_leg_source_time_skew() -> None:
    state = _state()
    rows = list(state.best_quotes)
    rows[6] = replace(rows[6], quote_time=NOW.replace(second=10))
    skewed = replace(state, quotes=tuple(rows), best_quotes=tuple(rows))

    shadow = build_call_skew_spread_shadow(
        skewed,
        expiry=EXPIRY,
        spot=SPOT,
        now=NOW,
    )

    assert shadow["status"] == "no_candidate"
    assert shadow["candidate"] is None


def test_spread_leg_change_is_material_but_quote_noise_is_not() -> None:
    payload = {
        "expiry": EXPIRY,
        "call_skew_spread_shadow": build_call_skew_spread_shadow(
            _state(), expiry=EXPIRY, spot=SPOT, now=NOW
        ),
        "put_skew_spread_shadow": build_put_skew_spread_shadow(
            _state(), expiry=EXPIRY, spot=SPOT, now=NOW
        ),
    }
    previous = payload_fingerprint(payload)
    quote_noise = deepcopy(payload)
    quote_noise["put_skew_spread_shadow"]["candidate"]["edge_points"] += 0.05

    assert material_changes(previous, payload_fingerprint(quote_noise)) == []

    new_legs = deepcopy(payload)
    new_legs["put_skew_spread_shadow"]["candidate"]["short"]["contract_id"] += ":new"
    changes = material_changes(previous, payload_fingerprint(new_legs))

    assert changes == ["Skew Spread Shadow Put 候选更新"]
