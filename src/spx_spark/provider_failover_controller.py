"""Observe provider health and persist the automatic market-data failover control state."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from spx_spark.config import StorageSettings, env_bool, env_float, env_int
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import MarketDataQuality, Provider, ProviderStatus, Quote, as_utc
from spx_spark.provider_failover import (
    FailoverMode,
    FailoverObservation,
    FailoverState,
    FailoverThresholds,
    advance_failover,
)
from spx_spark.settings import settings_value
from spx_spark.state_io import atomic_write_json_secure
from spx_spark.storage import LatestState, LatestStateStore


@dataclass(frozen=True)
class ProviderFailoverSettings:
    enabled: bool
    state_path: str
    required_instruments: tuple[str, ...]
    globex_required_instruments: tuple[str, ...]
    provider_state_max_age_seconds: float
    quote_max_age_seconds: float
    control_state_max_age_seconds: float
    transition_alert_max_age_seconds: float
    monitor_rth_only: bool
    thresholds: FailoverThresholds

    def __post_init__(self) -> None:
        if not self.required_instruments:
            raise ValueError("provider failover requires at least one direct anchor")
        if not self.globex_required_instruments:
            raise ValueError("provider failover requires at least one Globex anchor")
        if len(set(self.required_instruments)) != len(self.required_instruments):
            raise ValueError("provider failover required instruments cannot contain duplicates")
        if self.provider_state_max_age_seconds <= 0:
            raise ValueError("provider failover state max age must be positive")
        if self.quote_max_age_seconds <= 0:
            raise ValueError("provider failover quote max age must be positive")
        if self.control_state_max_age_seconds <= 0:
            raise ValueError("provider failover control state max age must be positive")
        if self.transition_alert_max_age_seconds <= 0:
            raise ValueError("provider failover transition alert max age must be positive")

    @classmethod
    def from_env(cls) -> "ProviderFailoverSettings":
        data_root = (
            os.getenv("MARKET_DATA_DATA_ROOT")
            or os.getenv("MAINTENANCE_DATA_ROOT")
            or str(settings_value("maintenance.data_root"))
        )
        configured_path = str(settings_value("provider_failover.state_path")).strip()
        return cls(
            enabled=env_bool(
                "PROVIDER_FAILOVER_ENABLED",
                bool(settings_value("provider_failover.enabled")),
            ),
            state_path=os.getenv("PROVIDER_FAILOVER_STATE_PATH")
            or configured_path
            or f"{data_root.rstrip('/')}/latest/provider_failover_state.json",
            required_instruments=tuple(
                str(item) for item in settings_value("provider_failover.required_instruments")
            ),
            globex_required_instruments=tuple(
                str(item)
                for item in settings_value("provider_failover.globex_required_instruments")
            ),
            provider_state_max_age_seconds=env_float(
                "PROVIDER_FAILOVER_STATE_MAX_AGE_SECONDS",
                float(settings_value("provider_failover.provider_state_max_age_seconds")),
            ),
            quote_max_age_seconds=env_float(
                "PROVIDER_FAILOVER_QUOTE_MAX_AGE_SECONDS",
                float(settings_value("provider_failover.quote_max_age_seconds")),
            ),
            control_state_max_age_seconds=env_float(
                "PROVIDER_FAILOVER_CONTROL_STATE_MAX_AGE_SECONDS",
                float(settings_value("provider_failover.control_state_max_age_seconds")),
            ),
            transition_alert_max_age_seconds=env_float(
                "PROVIDER_FAILOVER_TRANSITION_ALERT_MAX_AGE_SECONDS",
                float(settings_value("provider_failover.transition_alert_max_age_seconds")),
            ),
            monitor_rth_only=env_bool(
                "PROVIDER_FAILOVER_RTH_ONLY",
                bool(settings_value("provider_failover.monitor_rth_only")),
            ),
            thresholds=FailoverThresholds(
                schwab_unhealthy_observations=env_int(
                    "PROVIDER_FAILOVER_SCHWAB_UNHEALTHY_OBSERVATIONS",
                    int(settings_value("provider_failover.schwab_unhealthy_observations")),
                ),
                schwab_recovery_observations=env_int(
                    "PROVIDER_FAILOVER_SCHWAB_RECOVERY_OBSERVATIONS",
                    int(settings_value("provider_failover.schwab_recovery_observations")),
                ),
                ibkr_unhealthy_observations=env_int(
                    "PROVIDER_FAILOVER_IBKR_UNHEALTHY_OBSERVATIONS",
                    int(settings_value("provider_failover.ibkr_unhealthy_observations")),
                ),
                ibkr_recovery_observations=env_int(
                    "PROVIDER_FAILOVER_IBKR_RECOVERY_OBSERVATIONS",
                    int(settings_value("provider_failover.ibkr_recovery_observations")),
                ),
            ),
        )


@dataclass(frozen=True)
class ProviderHealth:
    healthy: bool
    reason: str


def load_failover_state(path: str | Path, *, now: datetime) -> FailoverState:
    state_path = Path(path)
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("state root is not an object")
        return FailoverState.from_dict(raw)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return FailoverState.initial(now=now)


def load_failover_control(path: str | Path) -> dict[str, object]:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_failover_state(
    path: str | Path,
    state: FailoverState,
    *,
    monitoring_active: bool,
    monitoring_context: str = "closed",
) -> None:
    payload = state.to_dict()
    payload["monitoring_active"] = monitoring_active
    payload["monitoring_context"] = monitoring_context
    payload["ibkr_market_data_required"] = bool(
        monitoring_active and state.ibkr_market_data_required
    )
    payload["new_entries_allowed"] = bool(
        monitoring_active
        and state.mode in {FailoverMode.SCHWAB_PRIMARY, FailoverMode.IBKR_FALLBACK}
    )
    atomic_write_json_secure(Path(path), payload)


def monitoring_active_at(now: datetime, *, rth_only: bool) -> bool:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return True
    return not rth_only and DEFAULT_MARKET_CALENDAR.is_globex_open(now)


def monitoring_context_at(now: datetime, *, rth_only: bool) -> str:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return "rth"
    if not rth_only and DEFAULT_MARKET_CALENDAR.is_globex_open(now):
        return "globex"
    return "closed"


def provider_health(
    state: LatestState,
    provider: Provider,
    *,
    required_instruments: tuple[str, ...],
    provider_state_max_age_seconds: float,
    quote_max_age_seconds: float,
) -> ProviderHealth:
    provider_states = [item for item in state.provider_states if item.provider == provider]
    latest_provider_state = (
        max(provider_states, key=lambda item: item.checked_at) if provider_states else None
    )
    if latest_provider_state is not None:
        provider_state_age = (state.as_of - latest_provider_state.checked_at).total_seconds()
        if 0 <= provider_state_age <= provider_state_max_age_seconds and (
            latest_provider_state.status == ProviderStatus.UNAVAILABLE
            or (
                latest_provider_state.status == ProviderStatus.DEGRADED
                and latest_provider_state.connected is False
            )
        ):
            return ProviderHealth(False, latest_provider_state.reason or "provider unavailable")

    missing: list[str] = []
    rejected: list[str] = []
    for instrument_id in required_instruments:
        quote = latest_quote_for_provider(state, instrument_id, provider)
        if quote is None:
            missing.append(instrument_id)
            continue
        source_at = quote.quote_time or quote.trade_time or quote.last_update_at or quote.received_at
        age_seconds = (as_utc(state.as_of) - as_utc(source_at)).total_seconds()
        delayed = "delayed" in str(quote.market_data_type or "").lower()
        bad_feed = quote.quality in {
            MarketDataQuality.FROZEN,
            MarketDataQuality.DELAYED,
            MarketDataQuality.DELAYED_FROZEN,
            MarketDataQuality.MISSING,
            MarketDataQuality.ERROR,
            MarketDataQuality.UNKNOWN,
            MarketDataQuality.SYNTHETIC,
        }
        if (
            quote.effective_price is None
            or delayed
            or bad_feed
            or age_seconds < 0
            or age_seconds > quote_max_age_seconds
        ):
            rejected.append(f"{instrument_id}:age={age_seconds:.1f}:quality={quote.quality.value}")
    if missing:
        return ProviderHealth(False, "missing " + ",".join(missing))
    if rejected:
        return ProviderHealth(False, "unusable " + ",".join(rejected))
    return ProviderHealth(True, "required live anchors are fresh")


def latest_quote_for_provider(
    state: LatestState,
    instrument_id: str,
    provider: Provider,
) -> Quote | None:
    matches = [
        quote
        for quote in state.quotes
        if quote.instrument.canonical_id == instrument_id and quote.provider == provider
    ]
    if not matches:
        return None
    return max(matches, key=lambda quote: as_utc(quote.received_at))


def evaluate_and_persist(
    latest: LatestState,
    settings: ProviderFailoverSettings,
) -> FailoverState:
    current = load_failover_state(settings.state_path, now=latest.as_of)
    active = settings.enabled and monitoring_active_at(
        latest.as_of,
        rth_only=settings.monitor_rth_only,
    )
    if not active:
        inactive = FailoverState.initial(now=latest.as_of)
        save_failover_state(
            settings.state_path,
            inactive,
            monitoring_active=False,
            monitoring_context="closed",
        )
        return inactive

    context = monitoring_context_at(latest.as_of, rth_only=settings.monitor_rth_only)
    required_instruments = (
        settings.required_instruments
        if context == "rth"
        else settings.globex_required_instruments
    )

    schwab = provider_health(
        latest,
        Provider.SCHWAB,
        required_instruments=required_instruments,
        provider_state_max_age_seconds=settings.provider_state_max_age_seconds,
        quote_max_age_seconds=settings.quote_max_age_seconds,
    )
    ibkr = provider_health(
        latest,
        Provider.IBKR,
        required_instruments=required_instruments,
        provider_state_max_age_seconds=settings.provider_state_max_age_seconds,
        quote_max_age_seconds=settings.quote_max_age_seconds,
    )
    updated = advance_failover(
        current,
        FailoverObservation(
            observed_at=latest.as_of,
            schwab_healthy=schwab.healthy,
            ibkr_healthy=ibkr.healthy,
            schwab_reason=schwab.reason,
            ibkr_reason=ibkr.reason,
        ),
        settings.thresholds,
    )
    save_failover_state(
        settings.state_path,
        updated,
        monitoring_active=True,
        monitoring_context=context,
    )
    return updated


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Schwab-primary market-data failover.")
    parser.add_argument("--json", action="store_true", help="Print the persisted control state.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = ProviderFailoverSettings.from_env()
    latest = LatestStateStore(StorageSettings.from_env()).load()
    state = evaluate_and_persist(latest, settings)
    if args.json:
        payload = state.to_dict()
        monitoring_active = bool(
            settings.enabled
            and monitoring_active_at(
                latest.as_of,
                rth_only=settings.monitor_rth_only,
            )
        )
        payload["monitoring_active"] = monitoring_active
        payload["monitoring_context"] = monitoring_context_at(
            latest.as_of,
            rth_only=settings.monitor_rth_only,
        )
        payload["ibkr_market_data_required"] = bool(
            monitoring_active and state.ibkr_market_data_required
        )
        payload["new_entries_allowed"] = bool(
            monitoring_active
            and state.mode in {FailoverMode.SCHWAB_PRIMARY, FailoverMode.IBKR_FALLBACK}
        )
        print(json.dumps(payload, sort_keys=True))
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
