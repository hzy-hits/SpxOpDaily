from __future__ import annotations

import json
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


def test_hyperliquid_proxy_gate_blocks_wide_basis() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
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
    assert gate["anchor"] == "future:ES"
