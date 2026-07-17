from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from spx_spark.market_context import _cme_contract_expiry, build_market_context
from spx_spark.marketdata import InstrumentId, InstrumentType, MarketDataQuality, Provider, Quote
from spx_spark.settings import settings_value
from spx_spark.storage import LatestState


BJ_TZ = ZoneInfo("Asia/Shanghai")
SPX_SECTOR_SYMBOLS = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")


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


def test_spx_sector_breadth_creates_symmetric_confirmed_bias() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)

    def context_for(price: float, spy: float, rsp: float) -> dict[str, object]:
        quotes = [
            make_quote(InstrumentId.equity(symbol), price, 100.0, now)
            for symbol in SPX_SECTOR_SYMBOLS
        ]
        quotes.extend(
            (
                make_quote(InstrumentId.equity("SPY"), spy, 750.0, now),
                make_quote(InstrumentId.equity("RSP"), rsp, 200.0, now),
            )
        )
        state = LatestState(
            created_at=now,
            as_of=now,
            quotes=tuple(quotes),
            best_quotes=tuple(quotes),
        )
        return build_market_context(state)["derived"]["spx_sector_breadth"]

    bullish = context_for(101.0, 751.0, 201.0)
    bearish = context_for(99.0, 749.0, 199.0)

    assert bullish["state"] == "usable_confirmed"
    assert bullish["directional_bias"] == "bullish"
    assert bullish["advancing_sector_count"] == 11
    assert bearish["directional_bias"] == "bearish"
    assert bearish["declining_sector_count"] == 11


def test_spx_sector_breadth_fails_closed_without_enough_fresh_sectors() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    quotes = []
    for index, symbol in enumerate(SPX_SECTOR_SYMBOLS):
        quote = make_quote(InstrumentId.equity(symbol), 101.0, 100.0, now)
        if index >= 7:
            quote = replace(
                quote,
                quality=MarketDataQuality.STALE,
                quote_time=now - timedelta(seconds=46),
            )
        quotes.append(quote)
    quotes.extend(
        (
            make_quote(InstrumentId.equity("SPY"), 751.0, 750.0, now),
            make_quote(InstrumentId.equity("RSP"), 201.0, 200.0, now),
        )
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )

    breadth = build_market_context(state)["derived"]["spx_sector_breadth"]

    assert breadth["state"] == "insufficient_fresh_sectors"
    assert breadth["usable_sector_count"] == 7
    assert breadth["directional_bias"] == "neutral_unclear"


def test_spx_sector_breadth_uses_its_configured_forty_five_second_window() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)

    def breadth_at_age(age_seconds: int) -> dict[str, object]:
        quotes = []
        for symbol in (*SPX_SECTOR_SYMBOLS, "SPY", "RSP"):
            close = 750.0 if symbol == "SPY" else 200.0 if symbol == "RSP" else 100.0
            price = close * 1.01
            quote = make_quote(InstrumentId.equity(symbol), price, close, now)
            quotes.append(
                replace(
                    quote,
                    quality=MarketDataQuality.STALE,
                    quote_time=now - timedelta(seconds=age_seconds),
                )
            )
        state = LatestState(
            created_at=now,
            as_of=now,
            quotes=tuple(quotes),
            best_quotes=tuple(quotes),
        )
        return build_market_context(state)["derived"]["spx_sector_breadth"]

    assert breadth_at_age(16)["state"] == "usable_confirmed"
    assert breadth_at_age(46)["state"] == "insufficient_fresh_sectors"


def test_spx_sector_breadth_does_not_claim_spy_rsp_confirmation_when_missing() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    quotes = tuple(
        make_quote(InstrumentId.equity(symbol), 101.0, 100.0, now) for symbol in SPX_SECTOR_SYMBOLS
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=quotes,
        best_quotes=quotes,
    )

    breadth = build_market_context(state)["derived"]["spx_sector_breadth"]

    assert breadth["state"] == "usable_unconfirmed"
    assert breadth["confirmation_state"] == "spy_rsp_missing_or_stale"
    assert breadth["directional_bias"] == "neutral_unclear"


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


def test_hyperliquid_gate_strips_futures_carry_before_basis() -> None:
    assert _cme_contract_expiry("/ESU26") == date(2026, 9, 18)
    assert _cme_contract_expiry("MESZ26") == date(2026, 12, 18)
    assert _cme_contract_expiry("future:ES") is None
    assert _cme_contract_expiry(None) is None

    now = datetime(2026, 7, 17, 5, 0, tzinfo=timezone.utc)
    es = make_provider_quote(
        InstrumentId.future("ES", provider_symbol="/ESU26"),
        7513.0,
        7480.0,
        now,
        provider=Provider.IBKR,
    )
    proxy = make_provider_quote(
        InstrumentId(symbol="xyz:SP500", instrument_type=InstrumentType.CRYPTO_PERP),
        7449.0,
        7480.0,
        now,
        provider=Provider.HYPERLIQUID,
    )
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=(es, proxy))

    gate = build_market_context(state)["derived"]["hyperliquid_spx_proxy"]

    # Raw basis is -85bps (basis_warn); stripping ~63 days of carry leaves
    # about -25bps, which is a fair anchor.
    assert gate["anchor_is_future"] is True
    assert gate["state"] == "basis_ok"
    assert gate["usable_for_alert"] is True
    assert gate["anchor_cash_equivalent"] == pytest.approx(7467.7, abs=0.5)
    assert gate["basis_bps"] == pytest.approx(-25.0, abs=2.0)


def test_hyperliquid_gate_falls_back_to_raw_basis_without_contract_symbol() -> None:
    now = datetime(2026, 7, 17, 5, 0, tzinfo=timezone.utc)
    es = make_provider_quote(
        InstrumentId.future("ES"),
        7513.0,
        7480.0,
        now,
        provider=Provider.IBKR,
    )
    proxy = make_provider_quote(
        InstrumentId(symbol="xyz:SP500", instrument_type=InstrumentType.CRYPTO_PERP),
        7449.0,
        7480.0,
        now,
        provider=Provider.HYPERLIQUID,
    )
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=(es, proxy))

    gate = build_market_context(state)["derived"]["hyperliquid_spx_proxy"]

    assert gate["anchor_is_future"] is True
    assert gate["state"] == "basis_warn"
    assert gate["anchor_cash_equivalent"] is None


def test_hyperliquid_gate_survives_missing_carry_config(monkeypatch) -> None:
    now = datetime(2026, 7, 17, 5, 0, tzinfo=timezone.utc)
    es = make_provider_quote(
        InstrumentId.future("ES", provider_symbol="/ESU26"),
        7513.0,
        7480.0,
        now,
        provider=Provider.IBKR,
    )
    proxy = make_provider_quote(
        InstrumentId(symbol="xyz:SP500", instrument_type=InstrumentType.CRYPTO_PERP),
        7449.0,
        7480.0,
        now,
        provider=Provider.HYPERLIQUID,
    )
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=(es, proxy))

    def _raise_missing(path: str) -> object:
        if path == "hyperliquid.es_carry_annual_rate":
            raise KeyError(path)
        return settings_value(path)

    monkeypatch.setattr("spx_spark.market_context.settings_value", _raise_missing)

    gate = build_market_context(state)["derived"]["hyperliquid_spx_proxy"]

    assert gate["anchor_cash_equivalent"] is None
    assert gate["basis_bps"] == pytest.approx(-85.2, abs=0.5)
    assert gate["state"] == "basis_warn"
