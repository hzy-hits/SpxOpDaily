from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.ibkr.adapter import provider_state_from_quotes, quotes_from_rows, snapshot_from_rows
from spx_spark.ibkr.collector import (
    collection_failure_reason,
    has_competing_session_error,
    provider_error_count,
)
from spx_spark.config import IbkrSettings
from spx_spark.ibkr.verifier import (
    IbkrError,
    VerifyRow,
    build_base_contracts,
    estimate_atm_reference,
    parse_index_spec,
)
from spx_spark.marketdata import InstrumentType, MarketDataQuality, Provider, ProviderStatus


def make_settings(**overrides) -> IbkrSettings:
    defaults = dict(
        host="127.0.0.1",
        port=4001,
        client_id=171,
        market_data_type=1,
        es_expiry="202609",
        mes_expiry="202609",
        verify_indexes=[],
        verify_stocks=[],
        verify_futures=[],
        verify_cfds=[],
        option_expiry="20260706",
        option_strike_window_points=50,
        option_strike_step=5,
        max_option_lines=40,
        quote_wait_seconds=0.1,
        stale_after_seconds=10.0,
        qualify_contracts=False,
        request_timeout_seconds=30.0,
    )
    defaults.update(overrides)
    return IbkrSettings(**defaults)


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


def test_collection_failure_reason_marks_socket_disconnect_as_possible_competing_session() -> None:
    reason = collection_failure_reason(ConnectionError("Socket disconnect"), [])

    assert "competing session" in reason
    assert "Socket disconnect" in reason


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


def test_build_base_contracts_includes_cfds():
    settings = make_settings(verify_cfds=["IBUS500"])

    contracts = build_base_contracts(settings)

    assert len(contracts) == 1
    label, kind, contract = contracts[0]
    assert label == "cfd:IBUS500"
    assert kind == "cfd"
    assert contract.symbol == "IBUS500"
    assert contract.secType == "CFD"
    assert contract.exchange == "SMART"


def test_quotes_from_rows_normalizes_cfd_rows():
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    row = VerifyRow(
        label="cfd:IBUS500",
        kind="cfd",
        symbol="IBUS500",
        exchange="SMART",
        market_data_type=1,
        bid=7500.0,
        ask=7500.5,
        market_price=7500.25,
        ticker_time=received_at.isoformat(),
    )

    quote = quotes_from_rows([row], received_at=received_at, stale_after_seconds=15.0)[0]

    assert quote.instrument.canonical_id == "cfd:IBUS500"
    assert quote.instrument.instrument_type == InstrumentType.CFD
    assert quote.instrument.underlier == "SPX"
    assert quote.quality == MarketDataQuality.LIVE
    assert quote.effective_price == 7500.25


def test_estimate_atm_reference_falls_back_to_ibus500_cfd():
    cfd_row = VerifyRow(
        label="cfd:IBUS500",
        kind="cfd",
        symbol="IBUS500",
        bid=7500.0,
        ask=7500.5,
        stale=False,
    )

    reference, source = estimate_atm_reference([cfd_row])

    assert reference == 7500.25
    assert source == "IBUS500"


def test_estimate_atm_reference_prefers_spx_over_cfd():
    spx_row = VerifyRow(label="index:SPX", kind="index", symbol="SPX", last=7490.0, stale=False)
    cfd_row = VerifyRow(
        label="cfd:IBUS500", kind="cfd", symbol="IBUS500", last=7500.0, stale=False
    )

    reference, source = estimate_atm_reference([spx_row, cfd_row])

    assert reference == 7490.0
    assert source == "SPX"


def test_estimate_atm_reference_skips_stale_spx_for_fresh_es():
    # Off-hours SPX still carries yesterday's close but is flagged stale; the
    # strike window must recenter on a live source instead.
    spx_row = VerifyRow(label="index:SPX", kind="index", symbol="SPX", last=7505.0, stale=True)
    es_row = VerifyRow(label="future:ES", kind="future", symbol="ES", last=7455.0, stale=False)

    reference, source = estimate_atm_reference([spx_row, es_row])

    assert reference == 7455.0
    assert source == "ES"

    # A closed market that never ticked since subscribe has stale=None (no
    # ticker_time); it must not pass as fresh either.
    never_ticked_spx = VerifyRow(label="index:SPX", kind="index", symbol="SPX", last=7505.0)
    reference, source = estimate_atm_reference([never_ticked_spx, es_row])
    assert reference == 7455.0
    assert source == "ES"

    # With every source stale, fall back to the priority order so a plan
    # still exists at startup.
    reference, source = estimate_atm_reference([spx_row])
    assert reference == 7505.0
    assert source == "SPX_stale"


def test_estimate_atm_reference_adjusts_es_quarterly_basis():
    # 2026-07-08 overnight: SPX stale at yesterday's close 7503.85, Sep ES
    # live at 7497 with close 7551.25. Raw ES would center the window ~50
    # points above the cash market; the close-vs-close basis fixes it.
    spx_row = VerifyRow(
        label="index:SPX", kind="index", symbol="SPX", close=7503.85, stale=True
    )
    es_row = VerifyRow(
        label="future:ES", kind="future", symbol="ES",
        last=7497.0, close=7551.25, stale=False,
    )

    reference, source = estimate_atm_reference([spx_row, es_row])

    assert source == "ES_basis_adj"
    assert reference == pytest.approx(7497.0 - (7551.25 - 7503.85))

    # Implausible basis (mismatched sessions) is ignored rather than applied.
    weird_spx = VerifyRow(
        label="index:SPX", kind="index", symbol="SPX", close=7300.0, stale=True
    )
    reference, source = estimate_atm_reference([weird_spx, es_row])
    assert source == "ES"
    assert reference == 7497.0


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
