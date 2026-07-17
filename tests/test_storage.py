from __future__ import annotations

import json
import multiprocessing
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
    QuoteFreshness,
)
from spx_spark.storage import (
    JsonlQuoteWriter,
    LatestStateStore,
    configured_quote_use_decision,
    degrade_stale_quote,
    prune_expired_option_quotes,
)


def make_storage_settings(
    tmp_path,
    *,
    provider_priority: tuple[str, ...] = ("ibkr", "schwab"),
) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset({"index:SKEW", "index:VVIX"}),
        provider_priority=provider_priority,
    )


def make_quote(
    *,
    provider: Provider,
    quality: MarketDataQuality,
    mark: float,
    received_at: datetime,
    symbol: str = "SPX",
    quote_time: datetime | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.index(symbol),
        provider=provider,
        provider_symbol=f"{provider.value}:{symbol}",
        received_at=received_at,
        quality=quality,
        bid=mark - 0.5,
        ask=mark + 0.5,
        last=mark,
        mark=mark,
        quote_time=quote_time or received_at,
    )


def _concurrent_raw_writer(data_root: str, worker_id: int, row_count: int) -> None:
    settings = StorageSettings(
        data_root=data_root,
        latest_state_path=f"{data_root}/latest/state.json",
        raw_file_name="quotes.jsonl",
        include_raw_payload=True,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
        provider_priority=("schwab", "ibkr"),
    )
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    rows = [
        Quote(
            instrument=InstrumentId.index("SPX"),
            provider=Provider.SCHWAB,
            provider_symbol="$SPX",
            received_at=received_at,
            quality=MarketDataQuality.LIVE,
            mark=7500.0 + row_index / 1000,
            quote_time=received_at,
            raw={
                "worker_id": worker_id,
                "row_index": row_index,
                "padding": "x" * 8192,
            },
        )
        for row_index in range(row_count)
    ]
    JsonlQuoteWriter(settings).write_quotes(rows)


def test_jsonl_writer_partitions_by_provider_date_and_hour(tmp_path):
    settings = make_storage_settings(tmp_path)
    writer = JsonlQuoteWriter(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    quote = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=7500,
        received_at=now,
    )

    result = writer.write_quotes([quote])

    assert result.row_count == 1
    assert len(result.paths) == 1
    path = tmp_path / "data" / "raw" / "provider=schwab" / "date=2026-07-06"
    path = path / "hour=13" / "quotes.jsonl"
    assert result.paths[0] == str(path)
    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["instrument_id"] == "index:SPX"
    assert record["provider"] == "schwab"


def test_jsonl_writer_serializes_concurrent_process_appends(tmp_path) -> None:
    worker_count = 4
    rows_per_worker = 200
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(
            target=_concurrent_raw_writer,
            args=(str(tmp_path / "data"), worker_id, rows_per_worker),
        )
        for worker_id in range(worker_count)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    path = (
        tmp_path
        / "data"
        / "raw"
        / "provider=schwab"
        / "date=2026-07-06"
        / "hour=13"
        / "quotes.jsonl"
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines]

    assert len(records) == worker_count * rows_per_worker
    assert {
        (record["raw"]["worker_id"], record["raw"]["row_index"])
        for record in records
    } == {
        (worker_id, row_index)
        for worker_id in range(worker_count)
        for row_index in range(rows_per_worker)
    }


def test_latest_state_falls_back_from_stale_ibkr_to_live_schwab(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    ibkr = make_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.LIVE,
        mark=7500,
        received_at=now - timedelta(minutes=2),
        quote_time=now - timedelta(minutes=2),
    )
    schwab = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=7501,
        received_at=now,
        quote_time=now,
    )

    result = store.update([ibkr, schwab], now=now)
    state = LatestStateStore(settings).load(now=now)
    best = state.best_quote("index:SPX")

    assert result.provider_quote_count == 2
    assert result.best_quote_count == 1
    assert best is not None
    assert best.provider == Provider.SCHWAB
    assert best.effective_price == 7501
    ibkr_state = [quote for quote in state.quotes if quote.provider == Provider.IBKR][0]
    assert ibkr_state.quality == MarketDataQuality.STALE


def test_latest_state_supports_schwab_primary_with_ibkr_quality_fallback(tmp_path):
    settings = make_storage_settings(
        tmp_path,
        provider_priority=("schwab", "ibkr"),
    )
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)
    schwab = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=6301.0,
        received_at=now,
        quote_time=now,
    )
    ibkr = make_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.LIVE,
        mark=6300.0,
        received_at=now,
        quote_time=now,
    )

    store.update([ibkr, schwab], now=now)
    assert store.load(now=now).best_quote("index:SPX").provider is Provider.SCHWAB

    stale_schwab = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=6299.0,
        received_at=now + timedelta(seconds=20),
        quote_time=now - timedelta(minutes=2),
    )
    fresh_ibkr = make_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.LIVE,
        mark=6302.0,
        received_at=now + timedelta(seconds=20),
        quote_time=now + timedelta(seconds=20),
    )

    store.update([stale_schwab, fresh_ibkr], now=now + timedelta(seconds=20))
    fallback = store.load(now=now + timedelta(seconds=20)).best_quote("index:SPX")
    assert fallback is not None
    assert fallback.provider is Provider.IBKR

    missing_schwab = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        provider_symbol="$SPX",
        received_at=now + timedelta(seconds=30),
        quality=MarketDataQuality.MISSING,
        error="symbol missing from Schwab payload",
    )
    store.update([missing_schwab], now=now + timedelta(seconds=30))
    missing_fallback = store.load(now=now + timedelta(seconds=30)).best_quote("index:SPX")
    assert missing_fallback is not None
    assert missing_fallback.provider is Provider.IBKR


def test_latest_state_keeps_provider_latest_across_updates(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    store.update(
        [
            make_quote(
                provider=Provider.SCHWAB,
                quality=MarketDataQuality.LIVE,
                mark=7501,
                received_at=now,
            )
        ],
        now=now,
    )
    store.update(
        [
            make_quote(
                provider=Provider.IBKR,
                quality=MarketDataQuality.LIVE,
                mark=7502,
                received_at=now + timedelta(seconds=1),
            )
        ],
        now=now + timedelta(seconds=1),
    )

    state = store.load()
    assert len(state.quotes) == 2
    assert state.best_quote("index:SPX").provider == Provider.IBKR


def test_latest_state_can_replace_a_dynamic_provider_quote_set(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    store.update(
        [
            make_quote(
                provider=Provider.POLYMARKET,
                quality=MarketDataQuality.LIVE,
                mark=0.6,
                received_at=now,
                symbol="POLY-OLD",
            ),
            make_quote(
                provider=Provider.IBKR,
                quality=MarketDataQuality.LIVE,
                mark=7500,
                received_at=now,
            ),
        ],
        now=now,
    )

    store.update(
        [
            make_quote(
                provider=Provider.POLYMARKET,
                quality=MarketDataQuality.LIVE,
                mark=0.7,
                received_at=now + timedelta(seconds=30),
                symbol="POLY-NEW",
            )
        ],
        now=now + timedelta(seconds=30),
        replace_providers=(Provider.POLYMARKET,),
    )
    state = store.load(now=now + timedelta(seconds=30))

    quote_ids = {quote.instrument.canonical_id for quote in state.quotes}
    assert "index:POLY-OLD" not in quote_ids
    assert "index:POLY-NEW" in quote_ids
    assert "index:SPX" in quote_ids


def test_latest_state_concurrent_updates_do_not_lose_quotes(tmp_path):
    settings = make_storage_settings(tmp_path)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    symbols = [f"SYM{i}" for i in range(20)]

    def update_one(symbol: str) -> None:
        LatestStateStore(settings).update(
            [
                make_quote(
                    provider=Provider.IBKR,
                    quality=MarketDataQuality.LIVE,
                    mark=7500,
                    received_at=now,
                    symbol=symbol,
                )
            ],
            now=now,
        )

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(update_one, symbols))

    state = LatestStateStore(settings).load(now=now)
    quote_ids = {quote.instrument.canonical_id for quote in state.quotes}
    assert quote_ids == {f"index:{symbol}" for symbol in symbols}


def test_latest_state_round_trips_provider_state(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    provider_state = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="runtime policy blocks IBKR collection",
        connected=False,
        authenticated=None,
        latency_ms=None,
        priority=0,
    )

    store.update([], now=now, provider_states=[provider_state])
    state = LatestStateStore(settings).load(now=now)

    assert len(state.provider_states) == 1
    assert state.provider_states[0].provider == Provider.IBKR
    assert state.provider_states[0].status == ProviderStatus.UNAVAILABLE
    assert state.provider_states[0].connected is False


def test_latest_state_merges_provider_states_across_provider_updates(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    ibkr_state = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="competing session",
        connected=False,
        authenticated=False,
        priority=0,
    )
    hyperliquid_state = ProviderState(
        provider=Provider.HYPERLIQUID,
        status=ProviderStatus.AVAILABLE,
        checked_at=now + timedelta(seconds=30),
        connected=True,
        authenticated=None,
        priority=0,
    )

    store.update([], now=now, provider_states=[ibkr_state])
    store.update(
        [
            make_quote(
                provider=Provider.HYPERLIQUID,
                quality=MarketDataQuality.LIVE,
                mark=7505,
                received_at=now + timedelta(seconds=30),
            )
        ],
        now=now + timedelta(seconds=30),
        provider_states=[hyperliquid_state],
    )
    state = LatestStateStore(settings).load(now=now + timedelta(seconds=30))

    states_by_provider = {item.provider: item for item in state.provider_states}
    assert states_by_provider[Provider.IBKR].status == ProviderStatus.UNAVAILABLE
    assert states_by_provider[Provider.HYPERLIQUID].status == ProviderStatus.AVAILABLE


def test_degrade_stale_quote_does_not_treat_flush_time_as_source_update() -> None:
    now = datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc)
    received_at = now - timedelta(hours=2)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=received_at,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=None,
        trade_time=None,
    )

    degraded = degrade_stale_quote(quote, as_of=now, stale_after_seconds=15.0)

    assert degraded.quality == MarketDataQuality.UNKNOWN


def make_option_quote(*, expiry: str, received_at: datetime) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=7500.0,
            right="C",
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"option:SPXW:{expiry}:7500:C",
        received_at=received_at,
        quality=MarketDataQuality.LIVE,
        mark=10.0,
        quote_time=received_at,
    )


def test_prune_expired_option_quotes_drops_yesterday_keeps_today_and_index() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quotes = (
        make_option_quote(expiry="20260706", received_at=now),
        make_option_quote(expiry="20260707", received_at=now),
        make_option_quote(expiry="20260708", received_at=now),
        make_option_quote(expiry="BAD", received_at=now),
        make_quote(
            provider=Provider.IBKR,
            quality=MarketDataQuality.LIVE,
            mark=7500.0,
            received_at=now,
        ),
    )

    pruned = prune_expired_option_quotes(quotes, now=now)
    expiries = {
        quote.instrument.expiry
        for quote in pruned
        if quote.instrument.instrument_type == InstrumentType.OPTION
    }

    assert "20260706" not in expiries
    assert expiries == {"20260707", "20260708", "BAD"}
    assert any(quote.instrument.instrument_type == InstrumentType.INDEX for quote in pruned)


def test_prune_expired_options_uses_1700_research_rollover() -> None:
    before_roll = datetime(2026, 7, 9, 20, 59, tzinfo=timezone.utc)
    at_roll = datetime(2026, 7, 9, 21, 0, tzinfo=timezone.utc)
    rows = (
        make_option_quote(expiry="20260709", received_at=before_roll),
        make_option_quote(expiry="20260710", received_at=before_roll),
    )

    assert len(prune_expired_option_quotes(rows, now=before_roll)) == 2
    assert [
        quote.instrument.expiry
        for quote in prune_expired_option_quotes(rows, now=at_roll)
    ] == ["20260710"]

    holiday_roll = datetime(2026, 7, 2, 21, 0, tzinfo=timezone.utc)
    holiday_rows = (
        make_option_quote(expiry="20260702", received_at=holiday_roll),
        make_option_quote(expiry="20260706", received_at=holiday_roll),
    )
    assert [
        quote.instrument.expiry
        for quote in prune_expired_option_quotes(holiday_rows, now=holiday_roll)
    ] == ["20260706"]


def test_latest_state_update_prunes_expired_options(tmp_path) -> None:
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    store.update(
        [
            make_option_quote(expiry="20260706", received_at=now),
            make_option_quote(expiry="20260707", received_at=now),
        ],
        now=now,
    )

    state = store.load(now=now)

    expiries = {quote.instrument.expiry for quote in state.quotes}
    assert expiries == {"20260707"}


def test_ibkr_rotated_option_row_uses_rotation_window() -> None:
    received_at = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = make_option_quote(expiry="20260707", received_at=received_at)
    as_of = received_at + timedelta(seconds=20)

    degraded = degrade_stale_quote(
        quote,
        as_of=as_of,
        stale_after_seconds=15.0,
        rotation_stale_after_seconds=45.0,
    )

    assert degraded.quality == MarketDataQuality.LIVE


def test_ibkr_rotated_option_row_stale_beyond_rotation_window() -> None:
    received_at = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = make_option_quote(expiry="20260707", received_at=received_at)
    as_of = received_at + timedelta(seconds=50)

    degraded = degrade_stale_quote(
        quote,
        as_of=as_of,
        stale_after_seconds=15.0,
        rotation_stale_after_seconds=45.0,
    )

    assert degraded.quality == MarketDataQuality.STALE


def test_rotation_window_does_not_apply_to_schwab_options_or_ibkr_indexes() -> None:
    received_at = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    as_of = received_at + timedelta(seconds=20)
    schwab_option = replace(
        make_option_quote(expiry="20260707", received_at=received_at),
        provider=Provider.SCHWAB,
        provider_symbol="schwab:SPXW:20260707:7500:C",
    )
    ibkr_index = make_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        received_at=received_at,
    )

    for quote in (schwab_option, ibkr_index):
        degraded = degrade_stale_quote(
            quote,
            as_of=as_of,
            stale_after_seconds=15.0,
            rotation_stale_after_seconds=45.0,
        )
        assert degraded.quality == MarketDataQuality.STALE


def test_configured_decision_uses_rotation_window_for_ibkr_options(tmp_path) -> None:
    settings = make_storage_settings(tmp_path)
    assert settings.rotation_stale_after_seconds == 45.0
    received_at = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = make_option_quote(expiry="20260707", received_at=received_at)

    fresh_decision = configured_quote_use_decision(
        quote,
        as_of=received_at + timedelta(seconds=20),
        settings=settings,
    )
    stale_decision = configured_quote_use_decision(
        quote,
        as_of=received_at + timedelta(seconds=50),
        settings=settings,
    )

    assert fresh_decision.pricing_allowed
    assert stale_decision.freshness == QuoteFreshness.STALE
    assert not stale_decision.pricing_allowed
