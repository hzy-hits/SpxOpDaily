"""ProviderSnapshot persistence ports for stream state."""

from __future__ import annotations

from spx_spark.config import StorageSettings
from spx_spark.ibkr.stream.runtime_machine import account_standby_state
from spx_spark.marketdata import Provider, ProviderState, ProviderStatus
from spx_spark.provider_adapter import ProviderSnapshot, persist_provider_snapshot
from spx_spark.storage import LatestMarketProjectionStore


def persist_state_only(state: ProviderState, storage_settings: StorageSettings) -> None:
    persist_provider_snapshot(
        ProviderSnapshot.from_state(Provider.IBKR, state, received_at=state.checked_at),
        storage_settings,
    )
    if state.status == ProviderStatus.UNAVAILABLE:
        LatestMarketProjectionStore(storage_settings).purge_provider_quotes(Provider.IBKR)


def persist_account_standby_state(storage_settings: StorageSettings) -> None:
    persist_state_only(account_standby_state(), storage_settings)
    LatestMarketProjectionStore(storage_settings).purge_provider_quotes(Provider.IBKR)

