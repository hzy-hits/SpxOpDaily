from datetime import datetime, timezone

from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import select_best_quotes


def _spxw(provider: Provider, observed_at: datetime) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260714",
            strike=7500,
            right="C",
            trading_class="SPXW",
        ),
        provider=provider,
        received_at=observed_at,
        quote_time=observed_at,
        last_update_at=observed_at,
        quality=MarketDataQuality.LIVE,
        bid=10.0,
        ask=10.2,
    )


def test_spxw_gth_best_quote_is_pinned_to_ibkr() -> None:
    observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=timezone.utc)

    selected = select_best_quotes(
        (_spxw(Provider.SCHWAB, observed_at), _spxw(Provider.IBKR, observed_at)),
        as_of=observed_at,
        provider_priority=(Provider.SCHWAB, Provider.IBKR),
    )

    assert len(selected) == 1
    assert selected[0].provider is Provider.IBKR


def test_spxw_gth_fails_closed_without_ibkr_quote() -> None:
    observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=timezone.utc)

    assert select_best_quotes(
        (_spxw(Provider.SCHWAB, observed_at),),
        as_of=observed_at,
        provider_priority=(Provider.SCHWAB, Provider.IBKR),
    ) == ()


def test_spxw_rth_keeps_configured_provider_priority() -> None:
    observed_at = datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)

    selected = select_best_quotes(
        (_spxw(Provider.SCHWAB, observed_at), _spxw(Provider.IBKR, observed_at)),
        as_of=observed_at,
        provider_priority=(Provider.SCHWAB, Provider.IBKR),
    )

    assert selected[0].provider is Provider.SCHWAB


def test_spxw_rth_pins_schwab_ahead_of_misconfigured_priority() -> None:
    observed_at = datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)

    selected = select_best_quotes(
        (_spxw(Provider.SCHWAB, observed_at), _spxw(Provider.IBKR, observed_at)),
        as_of=observed_at,
        provider_priority=(Provider.IBKR, Provider.SCHWAB),
    )

    assert selected[0].provider is Provider.SCHWAB


def test_spxw_rth_uses_ibkr_when_schwab_is_absent() -> None:
    observed_at = datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)

    selected = select_best_quotes(
        (_spxw(Provider.IBKR, observed_at),),
        as_of=observed_at,
        provider_priority=(Provider.SCHWAB, Provider.IBKR),
    )

    assert selected[0].provider is Provider.IBKR


def test_spxw_rth_failover_mode_is_authoritative() -> None:
    observed_at = datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)
    rows = (_spxw(Provider.SCHWAB, observed_at), _spxw(Provider.IBKR, observed_at))

    fallback = select_best_quotes(
        rows,
        as_of=observed_at,
        provider_priority=(Provider.SCHWAB, Provider.IBKR),
        failover_mode="ibkr_fallback",
    )
    unsafe = select_best_quotes(
        rows,
        as_of=observed_at,
        provider_priority=(Provider.SCHWAB, Provider.IBKR),
        failover_mode="recovery_pending",
    )

    assert [quote.provider for quote in fallback] == [Provider.IBKR]
    assert unsafe == ()


def test_spxw_gth_does_not_use_schwab_primary_control() -> None:
    observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=timezone.utc)

    selected = select_best_quotes(
        (_spxw(Provider.SCHWAB, observed_at), _spxw(Provider.IBKR, observed_at)),
        as_of=observed_at,
        provider_priority=(Provider.SCHWAB, Provider.IBKR),
        failover_mode="schwab_primary",
    )

    assert selected == ()
