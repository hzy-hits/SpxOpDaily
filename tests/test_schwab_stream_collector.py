from datetime import datetime, timedelta, timezone

from spx_spark.marketdata import MarketDataQuality, Provider
from spx_spark.schwab.stream_collector import SchwabStreamQuoteAssembler


UTC = timezone.utc


def millis(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def test_stream_assembler_merges_sparse_equity_deltas() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    assembler = SchwabStreamQuoteAssembler(stale_after_seconds=15.0)
    assert assembler.ingest(
        {
            "service": "LEVELONE_EQUITIES",
            "content": [
                {
                    "key": "$SPX",
                    "BID_PRICE": 7499.5,
                    "ASK_PRICE": 7500.5,
                    "QUOTE_TIME_MILLIS": millis(now),
                }
            ],
        },
        received_at=now,
    ) == 1
    assert assembler.ingest(
        {
            "service": "LEVELONE_EQUITIES",
            "content": [{"key": "$SPX", "MARK": 7500.25}],
        },
        received_at=now + timedelta(seconds=1),
    ) == 1

    snapshot = assembler.drain_snapshot()

    assert snapshot is not None
    assert snapshot.provider == Provider.SCHWAB
    assert snapshot.quote_count == 1
    quote = snapshot.quotes[0]
    assert quote.instrument.canonical_id == "index:SPX"
    assert quote.bid == 7499.5
    assert quote.ask == 7500.5
    assert quote.mark == 7500.25
    assert quote.quality == MarketDataQuality.LIVE
    assert quote.sampling_mode == "schwab_stream"
    assert assembler.drain_snapshot() is None


def test_stream_assembler_normalizes_concrete_es_future() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    assembler = SchwabStreamQuoteAssembler(stale_after_seconds=15.0)
    assembler.ingest(
        {
            "service": "LEVELONE_FUTURES",
            "content": [
                {
                    "SYMBOL": "/ESU26",
                    "BID_PRICE": 7550.0,
                    "ASK_PRICE": 7550.25,
                    "TOTAL_VOLUME": 123456,
                    "OPEN_INTEREST": 234567,
                    "QUOTE_TIME_MILLIS": millis(now),
                }
            ],
        },
        received_at=now,
    )

    snapshot = assembler.drain_snapshot()

    assert snapshot is not None
    quote = snapshot.quotes[0]
    assert quote.instrument.canonical_id == "future:ES"
    assert quote.provider_symbol == "/ESU26"
    assert quote.volume == 123456
    assert quote.open_interest == 234567


def test_stream_assembler_ignores_unknown_services_and_price_less_rows() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    assembler = SchwabStreamQuoteAssembler(stale_after_seconds=15.0)

    assert assembler.ingest({"service": "ACCT_ACTIVITY", "content": []}, received_at=now) == 0
    assembler.ingest(
        {
            "service": "LEVELONE_EQUITIES",
            "content": [{"key": "SPY", "QUOTE_TIME_MILLIS": millis(now)}],
        },
        received_at=now,
    )

    assert assembler.drain_snapshot() is None
