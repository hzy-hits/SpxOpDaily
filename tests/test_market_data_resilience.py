from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
)
from spx_spark.options_map import build_options_map, group_spxw_option_quotes, select_underlier
from spx_spark.storage import LatestState, LatestStateStore


def make_spxw_option(
    *,
    now: datetime,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
    provider: Provider = Provider.IBKR,
    strike: float = 7500.0,
    right: str = "C",
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260706",
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=provider,
        provider_symbol=f"{provider.value}:SPXW:{strike:g}:{right}",
        received_at=now,
        quality=quality,
        mark=10.0,
        quote_time=now,
    )


def test_select_underlier_prefers_es_over_hyperliquid() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    es = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7510.0,
        quote_time=now,
    )
    hyperliquid = Quote(
        instrument=InstrumentId(
            symbol="SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
            provider_symbol="xyz:SP500",
            exchange="xyz",
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7512.0,
        quote_time=now,
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(es, hyperliquid),
        best_quotes=(es, hyperliquid),
    )
    underlier = select_underlier(state)
    assert underlier.source == "future:ES"


def test_build_options_map_drops_ibkr_options_when_feed_unavailable() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    es = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7510.0,
        quote_time=now,
    )
    live_option = make_spxw_option(now=now, quality=MarketDataQuality.LIVE)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(es, live_option),
        best_quotes=(es, live_option),
        provider_states=(
            ProviderState(
                provider=Provider.IBKR,
                status=ProviderStatus.UNAVAILABLE,
                checked_at=now,
                reason="connect failed",
                connected=False,
            ),
        ),
    )
    options_map = build_options_map(state)
    assert options_map.expiries == ()
    assert "IBKR feed unavailable" in " ".join(options_map.warnings)


def test_build_options_map_prefers_ibkr_spxw_over_mock_when_feed_available() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    spx = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7524.0,
        quote_time=now,
    )
    ibkr_option = make_spxw_option(now=now, quality=MarketDataQuality.LIVE)
    mock_option = Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260706",
            strike=7500.0,
            right="C",
            trading_class="SPXW",
        ),
        provider=Provider.MOCK,
        provider_symbol="mock:SPXW",
        received_at=now - timedelta(days=2),
        quality=MarketDataQuality.STALE,
        mark=1.0,
        quote_time=now - timedelta(days=2),
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(spx, ibkr_option, mock_option),
        best_quotes=(spx, ibkr_option, mock_option),
        provider_states=(
            ProviderState(
                provider=Provider.IBKR,
                status=ProviderStatus.AVAILABLE,
                checked_at=now,
                connected=True,
                authenticated=True,
            ),
        ),
    )

    options_map = build_options_map(state)

    assert len(options_map.expiries) == 1
    assert options_map.expiries[0].option_count == 1
    assert options_map.expiries[0].coverage.live == 1
    assert options_map.expiries[0].coverage.stale == 0


def test_group_spxw_selects_each_contract_by_quality_then_configured_provider(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    monkeypatch.setenv(
        "MARKET_DATA_PROVIDER_PRIORITY",
        "schwab,ibkr,hyperliquid,polymarket,internal,mock,unknown",
    )
    same_contract_ibkr = make_spxw_option(now=now, provider=Provider.IBKR)
    same_contract_schwab = make_spxw_option(now=now, provider=Provider.SCHWAB)
    stale_schwab_put = make_spxw_option(
        now=now - timedelta(seconds=30),
        provider=Provider.SCHWAB,
        strike=7495.0,
        right="P",
    )
    live_ibkr_put = make_spxw_option(
        now=now,
        provider=Provider.IBKR,
        strike=7495.0,
        right="P",
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(same_contract_ibkr, same_contract_schwab, stale_schwab_put, live_ibkr_put),
        best_quotes=(same_contract_ibkr, same_contract_schwab, stale_schwab_put, live_ibkr_put),
    )

    grouped = group_spxw_option_quotes(state)
    selected = {
        (quote.instrument.strike, quote.instrument.right.value): quote.provider
        for quote in grouped["20260706"]
    }

    assert selected[(7500.0, "C")] == Provider.SCHWAB
    assert selected[(7495.0, "P")] == Provider.IBKR


def test_stale_ibkr_residue_does_not_exclude_live_schwab_contracts(monkeypatch) -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    monkeypatch.setenv(
        "MARKET_DATA_PROVIDER_PRIORITY",
        "schwab,ibkr,hyperliquid,polymarket,internal,mock,unknown",
    )
    stale_ibkr = make_spxw_option(
        now=now - timedelta(minutes=5),
        provider=Provider.IBKR,
    )
    live_schwab = make_spxw_option(
        now=now,
        provider=Provider.SCHWAB,
        strike=7495.0,
        right="P",
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(stale_ibkr, live_schwab),
        best_quotes=(stale_ibkr, live_schwab),
        provider_states=(
            ProviderState(
                provider=Provider.IBKR,
                status=ProviderStatus.AVAILABLE,
                checked_at=now - timedelta(minutes=5),
                connected=True,
            ),
        ),
    )

    grouped = group_spxw_option_quotes(state)
    selected = grouped["20260706"]

    assert {quote.provider for quote in selected} == {Provider.IBKR, Provider.SCHWAB}
    assert any(
        quote.provider == Provider.IBKR and quote.quality == MarketDataQuality.STALE
        for quote in selected
    )
    assert any(
        quote.provider == Provider.SCHWAB and quote.quality == MarketDataQuality.LIVE
        for quote in selected
    )


def test_purge_provider_quotes_removes_ibkr_rows(tmp_path) -> None:
    from spx_spark.config import StorageSettings

    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    settings = StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data/latest/state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset({"index:SKEW", "index:VVIX"}),
    )
    store = LatestStateStore(settings)
    ibkr = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=now,
    )
    hyperliquid = Quote(
        instrument=InstrumentId(
            symbol="SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
            provider_symbol="xyz:SP500",
            exchange="xyz",
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7510.0,
        quote_time=now,
    )
    store.update((ibkr, hyperliquid), now=now)
    result = store.purge_provider_quotes(Provider.IBKR, now=now)
    state = store.load(now=now)
    assert result.best_quote_count == 1
    assert all(quote.provider != Provider.IBKR for quote in state.best_quotes)
