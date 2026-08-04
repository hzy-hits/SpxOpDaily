from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.ibkr.adapter import provider_state_from_quotes, quotes_from_rows, snapshot_from_rows
from spx_spark.ibkr.collector import (
    collection_failure_reason,
    has_competing_session_error,
    provider_error_count,
)
from spx_spark.config import IbkrSettings, StorageSettings
from spx_spark.ibkr.verifier import (
    IbkrError,
    VerifyRow,
    build_base_contracts,
    estimate_atm_reference,
    parse_index_spec,
)
from spx_spark.marketdata import (
    InstrumentType,
    MarketDataQuality,
    Provider,
    ProviderStatus,
    QuoteMarketSession,
    quote_from_dict,
)
from spx_spark.provider_adapter import persist_provider_snapshot
from spx_spark.storage import LatestStateStore


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


def test_rotation_scoped_competing_error_does_not_claim_session_conflict() -> None:
    error = IbkrError(
        req_id=41,
        error_code=10197,
        message="No market data during competing live session",
        contract=None,
        ts="2026-08-04T02:00:00+00:00",
        subscription_lane="rotation",
    )

    assert has_competing_session_error([error]) is False
    assert provider_error_count([error]) == 0


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


def _spxw_row(*, ticker_time: str | None) -> VerifyRow:
    return VerifyRow(
        label="option:SPXW:20260803:6300:C",
        kind="option",
        symbol="SPX",
        market_data_type=1,
        bid=5.0,
        ask=5.2,
        market_price=5.1,
        ticker_time=ticker_time,
    )


def test_ibkr_spxw_market_session_comes_from_source_timestamp() -> None:
    gth_source = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
    rth_source = datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)
    next_gth_receipt = datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)

    gth_quote = quotes_from_rows(
        [_spxw_row(ticker_time=gth_source.isoformat())],
        received_at=gth_source,
        stale_after_seconds=15.0,
    )[0]
    old_rth_quote = quotes_from_rows(
        [_spxw_row(ticker_time=rth_source.isoformat())],
        received_at=next_gth_receipt,
        stale_after_seconds=15.0,
    )[0]

    assert gth_quote.market_session is QuoteMarketSession.GTH
    assert old_rth_quote.market_session is QuoteMarketSession.REGULAR
    assert quote_from_dict(gth_quote.to_dict()).market_session is QuoteMarketSession.GTH


def test_ibkr_spx_session_fails_closed_outside_trading_segments() -> None:
    gap_source = datetime(2026, 8, 3, 13, 27, tzinfo=timezone.utc)
    weekend_source = datetime(2026, 8, 1, 14, 0, tzinfo=timezone.utc)

    for source in (gap_source, weekend_source):
        quote = quotes_from_rows(
            [_spxw_row(ticker_time=source.isoformat())],
            received_at=source,
            stale_after_seconds=15.0,
        )[0]
        assert quote.market_session is None

    missing_source = quotes_from_rows(
        [_spxw_row(ticker_time=None)],
        received_at=datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc),
        stale_after_seconds=15.0,
    )[0]
    assert missing_source.market_session is None


def test_ibkr_spx_session_does_not_relabel_cash_index_or_es_as_gth() -> None:
    gth_source = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
    rows = [
        VerifyRow(
            label="index:SPX",
            kind="index",
            symbol="SPX",
            market_data_type=1,
            last=6300.0,
            ticker_time=gth_source.isoformat(),
        ),
        VerifyRow(
            label="future:ES",
            kind="future",
            symbol="ES",
            market_data_type=1,
            last=6310.0,
            ticker_time=gth_source.isoformat(),
        ),
    ]

    spx, es = quotes_from_rows(
        rows,
        received_at=gth_source,
        stale_after_seconds=15.0,
    )

    assert spx.market_session is None
    assert es.market_session is None


def test_ibkr_cash_spx_is_regular_during_rth() -> None:
    rth_source = datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)
    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        market_data_type=1,
        last=6300.0,
        ticker_time=rth_source.isoformat(),
    )

    quote = quotes_from_rows(
        [row],
        received_at=rth_source,
        stale_after_seconds=15.0,
    )[0]

    assert quote.market_session is QuoteMarketSession.REGULAR


def test_ibkr_spxw_market_session_survives_latest_state_persistence(tmp_path) -> None:
    gth_source = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
    snapshot = snapshot_from_rows(
        [_spxw_row(ticker_time=gth_source.isoformat())],
        received_at=gth_source,
        stale_after_seconds=15.0,
        connected=True,
        authenticated=True,
        latency_ms=None,
        replace_provider_quotes=True,
    )
    settings = StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset({"index:SKEW", "index:VVIX"}),
    )

    persist_provider_snapshot(snapshot, settings)
    persisted = LatestStateStore(settings).load(now=gth_source)

    assert len(persisted.quotes) == 1
    assert persisted.quotes[0].market_session is QuoteMarketSession.GTH


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


def test_stateless_atm_reference_rejects_stale_spx_and_unadjusted_es():
    spx_row = VerifyRow(label="index:SPX", kind="index", symbol="SPX", last=7505.0, stale=True)
    es_row = VerifyRow(label="future:ES", kind="future", symbol="ES", last=7455.0, stale=False)

    reference, source = estimate_atm_reference([spx_row, es_row])

    assert reference is None
    assert source == "none"

    # A closed market that never ticked since subscribe has stale=None (no
    # ticker_time); it must not pass as fresh either.
    never_ticked_spx = VerifyRow(label="index:SPX", kind="index", symbol="SPX", last=7505.0)
    reference, source = estimate_atm_reference([never_ticked_spx, es_row])
    assert reference is None
    assert source == "none"

    reference, source = estimate_atm_reference([spx_row])
    assert reference is None
    assert source == "none"


def test_stateless_atm_reference_never_uses_mismatched_close_basis():
    spx_row = VerifyRow(
        label="index:SPX", kind="index", symbol="SPX", close=7503.85, stale=True
    )
    es_row = VerifyRow(
        label="future:ES", kind="future", symbol="ES",
        last=7497.0, close=7551.25, stale=False,
    )

    reference, source = estimate_atm_reference([spx_row, es_row])

    assert source == "none"
    assert reference is None

    # Implausible basis (mismatched sessions) is ignored rather than applied.
    weird_spx = VerifyRow(
        label="index:SPX", kind="index", symbol="SPX", close=7300.0, stale=True
    )
    reference, source = estimate_atm_reference([weird_spx, es_row])
    assert source == "none"
    assert reference is None


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
