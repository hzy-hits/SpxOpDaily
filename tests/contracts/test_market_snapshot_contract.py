from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.domain.market import MarketSnapshot, dedupe_quotes
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote


def _quote(symbol: str, provider: Provider, received_at: datetime, mark: float) -> Quote:
    return Quote(
        instrument=InstrumentId.index(symbol),
        provider=provider,
        received_at=received_at,
        quality=MarketDataQuality.LIVE,
        mark=mark,
        quote_time=received_at,
    )


def test_market_snapshot_requires_aware_utc() -> None:
    now = datetime(2026, 7, 11, 14, 0)
    snapshot = MarketSnapshot(
        schema_version=1,
        snapshot_id="snap-1",
        as_of=now,
        received_at=now,
        quotes=(),
        provider_states=(),
        source_batch_ids=(),
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        snapshot.validate()


def test_market_snapshot_rejects_duplicate_quotes() -> None:
    now = datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
    quote = _quote("SPX", Provider.SCHWAB, now, 6500.0)
    snapshot = MarketSnapshot(
        schema_version=1,
        snapshot_id="snap-1",
        as_of=now,
        received_at=now,
        quotes=(quote, quote),
        provider_states=(),
        source_batch_ids=("batch-1",),
    )
    with pytest.raises(ValueError, match="duplicate quote"):
        snapshot.validate()


def test_market_snapshot_distinguishes_option_contracts_in_same_batch() -> None:
    now = datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
    quotes = tuple(
        Quote(
            instrument=InstrumentId.option(
                "SPX",
                trading_class="SPXW",
                expiry="20260713",
                strike=strike,
                right=right,
            ),
            provider=Provider.SCHWAB,
            received_at=now,
            quality=MarketDataQuality.LIVE,
            mark=10.0,
            quote_time=now,
        )
        for strike, right in ((7400.0, "P"), (7500.0, "C"), (7600.0, "C"))
    )
    snapshot = MarketSnapshot(
        schema_version=1,
        snapshot_id="option-chain-batch",
        as_of=now,
        received_at=now,
        quotes=quotes,
        provider_states=(),
        source_batch_ids=("schwab-chain-1",),
    )

    snapshot.validate()
    assert len(snapshot.options("SPX", "20260713")) == 3


def test_dedupe_quotes_is_deterministic() -> None:
    now = datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
    first = _quote("SPX", Provider.SCHWAB, now, 6500.0)
    second = _quote("SPX", Provider.SCHWAB, now, 6501.0)
    assert dedupe_quotes([first, second]) == (second,)


def test_domain_event_validation() -> None:
    now = datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
    event = DomainEvent(
        schema_version=1,
        event_id="evt-1",
        kind=EventKind.ALERT_CANDIDATE,
        source_at=now,
        available_at=now,
        aggregate_id="alert:shock",
        sequence=1,
        payload={"kind": "price_shock"},
    )
    event.validate()
