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
from spx_spark.options_map import build_options_map, select_underlier
from spx_spark.storage import LatestState, LatestStateStore


def make_spxw_option(*, now: datetime, quality: MarketDataQuality = MarketDataQuality.LIVE) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260706",
            strike=7500.0,
            right="C",
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol="SPXW",
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
            strike=7300.0,
            right="P",
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
