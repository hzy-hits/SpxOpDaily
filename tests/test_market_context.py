from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

from spx_spark.market_context import build_market_context
from spx_spark.marketdata import InstrumentId, InstrumentType, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


BJ_TZ = ZoneInfo("Asia/Shanghai")


def make_quote(instrument_id: InstrumentId, price: float, close: float, now: datetime) -> Quote:
    return Quote(
        instrument=instrument_id,
        provider=Provider.IBKR,
        provider_symbol=instrument_id.canonical_id,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=price,
        close=close,
        quote_time=now,
    )


def make_provider_quote(
    instrument_id: InstrumentId,
    price: float,
    close: float,
    now: datetime,
    *,
    provider: Provider,
) -> Quote:
    return Quote(
        instrument=instrument_id,
        provider=provider,
        provider_symbol=instrument_id.canonical_id,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=price,
        close=close,
        quote_time=now,
    )


def test_market_context_includes_vol_and_cross_asset_ratios() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
            make_quote(InstrumentId.index("VIX1D"), 14.0, 13.0, now),
            make_quote(InstrumentId.index("VIX9D"), 16.0, 15.5, now),
            make_quote(InstrumentId.index("VIX"), 18.0, 17.0, now),
            make_quote(InstrumentId.index("VIX3M"), 20.0, 19.5, now),
            make_quote(InstrumentId.equity("SPY"), 750.0, 745.0, now),
            make_quote(InstrumentId.equity("QQQ"), 725.0, 720.0, now),
            make_quote(InstrumentId.equity("HYG"), 80.0, 79.5, now),
            make_quote(InstrumentId.equity("LQD"), 108.0, 108.5, now),
        ),
    )

    context = build_market_context(state)

    entries = {entry["instrument_id"]: entry for entry in context["entries"]}
    assert entries["index:VIX"]["quality"] == "live"
    assert entries["index:NDX"]["quality"] == "missing"
    assert context["derived"]["vix1d_vix9d"] == 14.0 / 16.0
    assert context["derived"]["vix9d_vix"] == 16.0 / 18.0
    assert context["derived"]["vix_vix3m"] == 18.0 / 20.0
    assert context["derived"]["qqq_spy"] == 725.0 / 750.0
    assert context["derived"]["hyg_lqd"] == 80.0 / 108.0


def test_market_context_missing_polymarket_is_research_only(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("POLYMARKET_LATEST_CONTEXT_PATH", str(tmp_path / "missing.json"))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())

    context = build_market_context(state)
    polymarket = context["derived"]["polymarket_context"]

    assert polymarket["state"] == "missing"
    assert polymarket["research_only"] is True
    assert polymarket["human_visible"] is False
    assert polymarket["usage_gate"] == "context_only_no_kelly_no_direct_alert"


def test_market_context_loads_latest_polymarket_context(monkeypatch, tmp_path) -> None:
    path = tmp_path / "polymarket_context.json"
    path.write_text(
        json.dumps(
            {
                "research_only": True,
                "human_visible": False,
                "usage_gate": "context_only_no_kelly_no_direct_alert",
                "market_count": 2,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("POLYMARKET_LATEST_CONTEXT_PATH", str(path))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())

    context = build_market_context(state)

    assert context["derived"]["polymarket_context"]["market_count"] == 2


def test_hyperliquid_proxy_gate_is_unanchored_without_es_or_spx() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
            make_provider_quote(
                InstrumentId(
                    symbol="xyz:SP500",
                    instrument_type=InstrumentType.CRYPTO_PERP,
                ),
                7500.0,
                7480.0,
                now,
                provider=Provider.HYPERLIQUID,
            ),
        ),
    )

    context = build_market_context(state)
    gate = context["derived"]["hyperliquid_spx_proxy"]

    assert gate["state"] == "unanchored_context_only"
    assert gate["usable_for_alert"] is False


def test_delayed_tradfi_quote_is_research_context_not_actionable_anchor() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    delayed_spx = replace(
        make_provider_quote(
            InstrumentId.index("SPX"),
            7500.0,
            7480.0,
            now,
            provider=Provider.IBKR,
        ),
        quality=MarketDataQuality.DELAYED,
        market_data_type=3,
        last_update_at=now,
    )
    proxy = make_provider_quote(
        InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        7501.0,
        7480.0,
        now,
        provider=Provider.HYPERLIQUID,
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(delayed_spx, proxy),
    )

    context = build_market_context(state)
    entries = {entry["instrument_id"]: entry for entry in context["entries"]}
    gate = context["derived"]["hyperliquid_spx_proxy"]

    assert entries["index:SPX"]["research_usable"] is True
    assert entries["index:SPX"]["alert_allowed"] is False
    assert gate["state"] == "unanchored_context_only"
    assert gate["anchor"] is None


def test_hyperliquid_proxy_gate_blocks_wide_basis() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
            make_provider_quote(
                InstrumentId.index("SPX"),
                7500.0,
                7480.0,
                now,
                provider=Provider.IBKR,
            ),
            make_provider_quote(
                InstrumentId.future("ES"),
                7500.0,
                7480.0,
                now,
                provider=Provider.IBKR,
            ),
            make_provider_quote(
                InstrumentId(
                    symbol="xyz:SP500",
                    instrument_type=InstrumentType.CRYPTO_PERP,
                ),
                7600.0,
                7480.0,
                now,
                provider=Provider.HYPERLIQUID,
            ),
        ),
    )

    context = build_market_context(state)
    gate = context["derived"]["hyperliquid_spx_proxy"]

    assert gate["state"] == "basis_blocked"
    assert gate["usable_for_alert"] is False
    assert gate["anchor"] == "index:SPX"
    assert gate["anchor_is_future"] is False


def test_hyperliquid_proxy_gate_prefers_spx_over_es_anchor() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
            make_provider_quote(
                InstrumentId.index("SPX"),
                7500.0,
                7480.0,
                now,
                provider=Provider.IBKR,
            ),
            make_provider_quote(
                InstrumentId.future("ES"),
                7510.0,
                7480.0,
                now,
                provider=Provider.IBKR,
            ),
            make_provider_quote(
                InstrumentId(
                    symbol="xyz:SP500",
                    instrument_type=InstrumentType.CRYPTO_PERP,
                ),
                7567.5,
                7480.0,
                now,
                provider=Provider.HYPERLIQUID,
            ),
        ),
    )

    context = build_market_context(state)
    gate = context["derived"]["hyperliquid_spx_proxy"]

    assert gate["anchor"] == "index:SPX"
    assert gate["anchor_is_future"] is False
    assert gate["state"] == "basis_warn"
    assert gate["warn_bps"] == 50.0
    assert gate["block_bps"] == 100.0


def test_hyperliquid_proxy_gate_uses_futures_thresholds_when_only_es_anchor() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    es_price = 7500.0
    proxy_90bps = es_price * 1.009
    proxy_120bps = es_price * 1.012
    state_90 = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
            make_provider_quote(
                InstrumentId.future("ES"),
                es_price,
                7480.0,
                now,
                provider=Provider.IBKR,
            ),
            make_provider_quote(
                InstrumentId(
                    symbol="xyz:SP500",
                    instrument_type=InstrumentType.CRYPTO_PERP,
                ),
                proxy_90bps,
                7480.0,
                now,
                provider=Provider.HYPERLIQUID,
            ),
        ),
    )
    state_120 = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
            make_provider_quote(
                InstrumentId.future("ES"),
                es_price,
                7480.0,
                now,
                provider=Provider.IBKR,
            ),
            make_provider_quote(
                InstrumentId(
                    symbol="xyz:SP500",
                    instrument_type=InstrumentType.CRYPTO_PERP,
                ),
                proxy_120bps,
                7480.0,
                now,
                provider=Provider.HYPERLIQUID,
            ),
        ),
    )

    gate_90 = build_market_context(state_90)["derived"]["hyperliquid_spx_proxy"]
    gate_120 = build_market_context(state_120)["derived"]["hyperliquid_spx_proxy"]

    assert gate_90["anchor"] == "future:ES"
    assert gate_90["anchor_is_future"] is True
    assert gate_90["state"] == "basis_warn"
    assert gate_90["warn_bps"] == 80.0
    assert gate_90["block_bps"] == 150.0

    assert gate_120["anchor"] == "future:ES"
    assert gate_120["anchor_is_future"] is True
    assert gate_120["state"] == "basis_warn"
    assert gate_120["usable_for_alert"] is False
