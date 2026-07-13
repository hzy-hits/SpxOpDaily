from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    MarketDataQuality,
    NormalizedSnapshot,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
    as_utc,
)
from spx_spark.storage import JsonlQuoteWriter, LatestMarketProjectionStore


class MarketDataAdapter(Protocol):
    provider: Provider

    def collect_snapshot(self) -> ProviderSnapshot:
        """Return one normalized provider snapshot."""
        ...


@dataclass(frozen=True)
class ProviderSnapshot:
    provider: Provider
    received_at: datetime
    quotes: tuple[Quote, ...] = ()
    provider_states: tuple[ProviderState, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "received_at", as_utc(self.received_at))
        object.__setattr__(self, "quotes", tuple(self.quotes))
        object.__setattr__(self, "provider_states", tuple(self.provider_states))
        object.__setattr__(self, "metadata", dict(self.metadata))
        self._validate_provider_consistency()

    @classmethod
    def from_state(
        cls,
        provider: Provider,
        state: ProviderState,
        *,
        received_at: datetime | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ProviderSnapshot:
        return cls(
            provider=provider,
            received_at=received_at or state.checked_at,
            provider_states=(state,),
            metadata=metadata or {},
        )

    @property
    def provider_state(self) -> ProviderState | None:
        return self.provider_states[0] if self.provider_states else None

    @property
    def quote_count(self) -> int:
        return len(self.quotes)

    def to_normalized_snapshot(self) -> NormalizedSnapshot:
        return NormalizedSnapshot(
            created_at=self.received_at,
            quotes=self.quotes,
            provider_states=self.provider_states,
        )

    def to_dict(self, *, include_raw: bool = False) -> dict[str, Any]:
        return {
            "provider": self.provider.value,
            "received_at": self.received_at.isoformat(),
            "quotes": [quote.to_dict(include_raw=include_raw) for quote in self.quotes],
            "provider_states": [state.to_dict() for state in self.provider_states],
            "metadata": dict(self.metadata),
        }

    def _validate_provider_consistency(self) -> None:
        quote_providers = {quote.provider for quote in self.quotes if quote.provider != self.provider}
        state_providers = {
            state.provider for state in self.provider_states if state.provider != self.provider
        }
        if quote_providers or state_providers:
            mismatches = sorted(
                {provider.value for provider in quote_providers | state_providers}
            )
            raise ValueError(
                f"ProviderSnapshot({self.provider.value}) contains mismatched provider data: "
                f"{', '.join(mismatches)}"
            )


@dataclass(frozen=True)
class ProviderSnapshotWriteResult:
    raw_paths: dict[str, int]
    latest_state: str
    provider_quote_count: int
    best_quote_count: int
    updated_quote_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_paths": self.raw_paths,
            "latest_state": self.latest_state,
            "provider_quote_count": self.provider_quote_count,
            "best_quote_count": self.best_quote_count,
            "updated_quote_count": self.updated_quote_count,
        }


def merge_provider_snapshots(
    snapshots: Iterable[ProviderSnapshot],
    *,
    created_at: datetime | None = None,
) -> NormalizedSnapshot:
    snapshots = tuple(snapshots)
    if created_at is None:
        created_at = max(
            (snapshot.received_at for snapshot in snapshots),
            default=datetime.now(tz=timezone.utc),
        )
    quotes: list[Quote] = []
    provider_states: list[ProviderState] = []
    for snapshot in snapshots:
        quotes.extend(snapshot.quotes)
        provider_states.extend(snapshot.provider_states)
    return NormalizedSnapshot(
        created_at=as_utc(created_at),
        quotes=tuple(quotes),
        provider_states=tuple(provider_states),
    )


def provider_state_from_quote_health(
    provider: Provider,
    quotes: Iterable[Quote],
    *,
    checked_at: datetime,
    connected: bool,
    authenticated: bool | None,
    latency_ms: float | None,
    priority: int,
    error_count: int = 0,
    reason: str | None = None,
    unavailable_reason: str | None = None,
    degraded_reason: str | None = None,
) -> ProviderState:
    quotes = tuple(quotes)
    usable_count = sum(1 for quote in quotes if quote.is_usable)
    current_count = sum(
        1
        for quote in quotes
        if quote.is_usable
        and quote.quality
        not in {
            MarketDataQuality.STALE,
            MarketDataQuality.UNKNOWN,
        }
    )
    error_quote_count = sum(1 for quote in quotes if quote.error)
    if not connected:
        status = ProviderStatus.UNAVAILABLE
        final_reason = reason or unavailable_reason or f"{provider.value} not connected"
    elif usable_count == 0:
        status = ProviderStatus.DEGRADED
        final_reason = reason or degraded_reason or "connected but no usable quotes"
    elif current_count == 0:
        status = ProviderStatus.DEGRADED
        final_reason = reason or "connected but all priced quotes are stale"
    elif error_count or error_quote_count:
        status = ProviderStatus.DEGRADED
        final_reason = reason or f"{error_count + error_quote_count} quote/API errors"
    else:
        status = ProviderStatus.AVAILABLE
        final_reason = reason

    return ProviderState(
        provider=provider,
        status=status,
        checked_at=as_utc(checked_at),
        reason=final_reason,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        priority=priority,
    )


def persist_provider_snapshot(
    snapshot: ProviderSnapshot,
    storage_settings: StorageSettings,
) -> ProviderSnapshotWriteResult:
    raw_result = JsonlQuoteWriter(storage_settings).write_quotes(snapshot.quotes)
    replace_providers = (
        (snapshot.provider,) if snapshot.metadata.get("replace_provider_quotes") is True else ()
    )
    latest_result = LatestMarketProjectionStore(storage_settings).update(
        snapshot.quotes,
        now=snapshot.received_at,
        provider_states=snapshot.provider_states,
        replace_providers=replace_providers,
    )
    return ProviderSnapshotWriteResult(
        raw_paths=raw_result.path_counts,
        latest_state=latest_result.path,
        provider_quote_count=latest_result.provider_quote_count,
        best_quote_count=latest_result.best_quote_count,
        updated_quote_count=latest_result.updated_quote_count,
    )
