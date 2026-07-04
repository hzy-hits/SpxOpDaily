from __future__ import annotations

from datetime import datetime

from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.marketdata import Provider, ProviderState, Quote, quote_from_ibkr_row
from spx_spark.provider_adapter import ProviderSnapshot, provider_state_from_quote_health


def quotes_from_rows(
    rows: list[VerifyRow],
    *,
    received_at: datetime,
    stale_after_seconds: float,
) -> tuple[Quote, ...]:
    return tuple(
        quote_from_ibkr_row(
            row,
            received_at=received_at,
            stale_after_seconds=stale_after_seconds,
        )
        for row in rows
    )


def provider_state_from_quotes(
    quotes: tuple[Quote, ...],
    *,
    checked_at: datetime,
    connected: bool,
    authenticated: bool | None,
    latency_ms: float | None,
    error_count: int = 0,
    reason: str | None = None,
) -> ProviderState:
    return provider_state_from_quote_health(
        Provider.IBKR,
        quotes,
        checked_at=checked_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        priority=0,
        error_count=error_count,
        reason=reason,
        unavailable_reason="IBKR not connected",
        degraded_reason="connected but no usable quotes",
    )


def snapshot_from_rows(
    rows: list[VerifyRow],
    *,
    received_at: datetime,
    stale_after_seconds: float,
    connected: bool,
    authenticated: bool | None,
    latency_ms: float | None,
    error_count: int = 0,
    reason: str | None = None,
) -> ProviderSnapshot:
    quotes = quotes_from_rows(
        rows,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
    )
    state = provider_state_from_quotes(
        quotes,
        checked_at=received_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        error_count=error_count,
        reason=reason,
    )
    return ProviderSnapshot(
        provider=Provider.IBKR,
        received_at=received_at,
        quotes=quotes,
        provider_states=(state,),
    )
