from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.ibkr.adapter import provider_state_from_quotes, quotes_from_rows, snapshot_from_rows
from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.marketdata import MarketDataQuality, Provider, ProviderStatus


def test_quotes_from_rows_normalizes_ibkr_verify_rows():
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        market_data_type=1,
        bid=7500.0,
        ask=7501.0,
        last=7500.5,
        market_price=7500.5,
        ticker_time=received_at.isoformat(),
    )

    quotes = quotes_from_rows([row], received_at=received_at, stale_after_seconds=15.0)

    assert len(quotes) == 1
    assert quotes[0].instrument.canonical_id == "index:SPX"
    assert quotes[0].provider == Provider.IBKR
    assert quotes[0].quality == MarketDataQuality.LIVE
    assert quotes[0].effective_price == 7500.5


def test_provider_state_from_quotes_marks_available_without_errors():
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        market_data_type=1,
        bid=7500.0,
        ask=7501.0,
        market_price=7500.5,
        ticker_time=received_at.isoformat(),
    )
    quotes = quotes_from_rows([row], received_at=received_at, stale_after_seconds=15.0)

    state = provider_state_from_quotes(
        quotes,
        checked_at=received_at,
        connected=True,
        authenticated=True,
        latency_ms=123.0,
    )

    assert state.provider == Provider.IBKR
    assert state.status == ProviderStatus.AVAILABLE
    assert state.connected is True


def test_provider_state_from_quotes_marks_degraded_when_errors_exist():
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        market_data_type=1,
        bid=7500.0,
        ask=7501.0,
        market_price=7500.5,
        ticker_time=received_at.isoformat(),
    )
    quotes = quotes_from_rows([row], received_at=received_at, stale_after_seconds=15.0)

    state = provider_state_from_quotes(
        quotes,
        checked_at=received_at,
        connected=True,
        authenticated=True,
        latency_ms=123.0,
        error_count=1,
    )

    assert state.status == ProviderStatus.DEGRADED
    assert "errors" in (state.reason or "")


def test_snapshot_from_rows_returns_provider_snapshot():
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        market_data_type=1,
        bid=7500.0,
        ask=7501.0,
        market_price=7500.5,
        ticker_time=received_at.isoformat(),
    )

    snapshot = snapshot_from_rows(
        [row],
        received_at=received_at,
        stale_after_seconds=15.0,
        connected=True,
        authenticated=True,
        latency_ms=123.0,
    )

    assert snapshot.provider == Provider.IBKR
    assert snapshot.quote_count == 1
    assert snapshot.provider_state is not None
    assert snapshot.provider_state.status == ProviderStatus.AVAILABLE
