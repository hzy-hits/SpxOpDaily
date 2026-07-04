from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from spx_spark.marketdata import (
    Provider,
    Quote,
    quote_from_schwab_option_contract,
    quote_from_schwab_payload,
)
from spx_spark.provider_adapter import ProviderSnapshot, provider_state_from_quote_health


def quotes_from_quote_payload(
    payload: Mapping[str, Any] | None,
    symbols: list[str],
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
) -> tuple[Quote, ...]:
    received_at = received_at or datetime.now(tz=timezone.utc)
    payload = payload or {}
    return tuple(
        quote_from_schwab_payload(
            symbol,
            payload.get(symbol) if isinstance(payload.get(symbol), Mapping) else None,
            received_at=received_at,
            stale_after_seconds=stale_after_seconds,
        )
        for symbol in symbols
    )


def option_quotes_from_chain_payload(
    payload: Mapping[str, Any] | None,
    *,
    underlier: str,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
) -> tuple[Quote, ...]:
    if not isinstance(payload, Mapping):
        return ()

    received_at = received_at or datetime.now(tz=timezone.utc)
    quotes: list[Quote] = []
    for expiration_map_name in ("callExpDateMap", "putExpDateMap"):
        expiration_map = payload.get(expiration_map_name)
        if not isinstance(expiration_map, Mapping):
            continue
        for strikes in expiration_map.values():
            if not isinstance(strikes, Mapping):
                continue
            for contracts in strikes.values():
                if not isinstance(contracts, list):
                    continue
                for contract in contracts:
                    if isinstance(contract, Mapping):
                        quotes.append(
                            quote_from_schwab_option_contract(
                                underlier,
                                contract,
                                received_at=received_at,
                                stale_after_seconds=stale_after_seconds,
                            )
                        )
    return tuple(quotes)


def snapshot_from_quote_payload(
    payload: Mapping[str, Any] | None,
    symbols: list[str],
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
    connected: bool = True,
    authenticated: bool | None = True,
    latency_ms: float | None = None,
    error_count: int = 0,
    reason: str | None = None,
) -> ProviderSnapshot:
    received_at = received_at or datetime.now(tz=timezone.utc)
    quotes = quotes_from_quote_payload(
        payload,
        symbols,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
    )
    state = provider_state_from_quote_health(
        Provider.SCHWAB,
        quotes,
        checked_at=received_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        priority=1,
        error_count=error_count,
        reason=reason,
        unavailable_reason="Schwab not connected",
        degraded_reason="connected but no usable Schwab quotes",
    )
    return ProviderSnapshot(
        provider=Provider.SCHWAB,
        received_at=received_at,
        quotes=quotes,
        provider_states=(state,),
    )


def snapshot_from_chain_payload(
    payload: Mapping[str, Any] | None,
    *,
    underlier: str,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
    connected: bool = True,
    authenticated: bool | None = True,
    latency_ms: float | None = None,
    error_count: int = 0,
    reason: str | None = None,
) -> ProviderSnapshot:
    received_at = received_at or datetime.now(tz=timezone.utc)
    quotes = option_quotes_from_chain_payload(
        payload,
        underlier=underlier,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
    )
    state = provider_state_from_quote_health(
        Provider.SCHWAB,
        quotes,
        checked_at=received_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        priority=1,
        error_count=error_count,
        reason=reason,
        unavailable_reason="Schwab not connected",
        degraded_reason="connected but no usable Schwab option quotes",
    )
    return ProviderSnapshot(
        provider=Provider.SCHWAB,
        received_at=received_at,
        quotes=quotes,
        provider_states=(state,),
    )
