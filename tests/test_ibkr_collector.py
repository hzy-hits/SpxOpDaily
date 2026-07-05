from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.ibkr.adapter import provider_state_from_quotes, quotes_from_rows, snapshot_from_rows
from spx_spark.ibkr.collector import has_competing_session_error, provider_error_count
from spx_spark.ibkr.verifier import IbkrError, VerifyRow, parse_index_spec
from spx_spark.marketdata import MarketDataQuality, Provider, ProviderStatus


def test_parse_index_spec_defaults_and_explicit_exchange():
    assert parse_index_spec("SPX") == ("SPX", "CBOE")
    assert parse_index_spec("NDX") == ("NDX", "NASDAQ")
    assert parse_index_spec("RUT@RUSSELL") == ("RUT", "RUSSELL")
    assert parse_index_spec("DJX:CBOE") == ("DJX", "CBOE")
    assert parse_index_spec("DJU") == ("DJU", "CBOE")


def test_competing_session_error_detection() -> None:
    assert has_competing_session_error(
        [
            IbkrError(
                req_id=1,
                error_code=10197,
                message="No market data during competing live session",
                contract=None,
                ts="2026-07-06T13:30:00+00:00",
            )
        ]
    )


def test_provider_error_count_ignores_farm_status_messages() -> None:
    errors = [
        IbkrError(1, 2119, "Market data farm is connecting", None, "2026-07-06T13:30:00+00:00"),
        IbkrError(2, 2104, "Market data farm connection is OK", None, "2026-07-06T13:30:00+00:00"),
        IbkrError(3, 354, "Requested market data is not subscribed", None, "2026-07-06T13:30:00+00:00"),
    ]

    assert provider_error_count(errors) == 1


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


def test_quotes_from_rows_preserves_non_cboe_index_exchange():
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    row = VerifyRow(
        label="index:NDX",
        kind="index",
        symbol="NDX",
        exchange="NASDAQ",
        market_data_type=1,
        bid=19000.0,
        ask=19001.0,
        market_price=19000.5,
        ticker_time=received_at.isoformat(),
    )

    quote = quotes_from_rows([row], received_at=received_at, stale_after_seconds=15.0)[0]

    assert quote.instrument.canonical_id == "index:NDX"
    assert quote.instrument.exchange == "NASDAQ"


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
