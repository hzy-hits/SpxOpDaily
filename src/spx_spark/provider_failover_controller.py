"""Observe provider health and persist the automatic market-data failover control state."""

from __future__ import annotations

import argparse
import json
import logging
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


LOGGER = logging.getLogger(__name__)


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
    gth_min_live_option_contracts: int = 20
    gth_option_quote_max_age_seconds: float = 90.0

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
        if self.gth_option_quote_max_age_seconds <= 0:
            raise ValueError("GTH option quote max age must be positive")
        if self.control_state_max_age_seconds <= 0:
            raise ValueError("provider failover control state max age must be positive")
        if self.transition_alert_max_age_seconds <= 0:
            raise ValueError("provider failover transition alert max age must be positive")
        if self.gth_min_live_option_contracts < 2:
            raise ValueError("GTH option health requires at least one call/put pair")

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
            gth_min_live_option_contracts=env_int(
                "PROVIDER_FAILOVER_GTH_MIN_LIVE_OPTION_CONTRACTS",
                int(settings_value("provider_failover.gth_min_live_option_contracts")),
            ),
            gth_option_quote_max_age_seconds=env_float(
                "PROVIDER_FAILOVER_GTH_OPTION_QUOTE_MAX_AGE_SECONDS",
                float(
                    settings_value(
                        "provider_failover.gth_option_quote_max_age_seconds"
                    )
                ),
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


def transport_health(
    state: LatestState,
    provider: Provider,
    *,
    provider_state_max_age_seconds: float,
) -> ProviderHealth:
    provider_states = [item for item in state.provider_states if item.provider == provider]
    if not provider_states:
        return ProviderHealth(False, "provider state missing")
    latest = max(provider_states, key=lambda item: item.checked_at)
    age_seconds = (as_utc(state.as_of) - as_utc(latest.checked_at)).total_seconds()
    if not 0 <= age_seconds <= provider_state_max_age_seconds:
        return ProviderHealth(False, f"provider state age {age_seconds:.1f}s")
    if latest.connected is False:
        return ProviderHealth(False, latest.reason or "transport disconnected")
    if latest.authenticated is False:
        return ProviderHealth(False, latest.reason or "provider authentication failed")
    return ProviderHealth(
        True,
        f"transport connected and authenticated ({latest.status.value})",
    )


def health_payload(health: ProviderHealth, *, required: bool = True) -> dict[str, object]:
    return {
        "healthy": health.healthy,
        "required": required,
        "reason": health.reason,
    }


def load_failover_state(path: str | Path, *, now: datetime) -> FailoverState:
    state_path = Path(path)
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("state root is not an object")
        return FailoverState.from_dict(raw)
    except FileNotFoundError:
        return FailoverState.initial(now=now)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        # A corrupt state file must not silently restore primary semantics:
        # fail closed to recovery pending and re-derive health from scratch.
        LOGGER.warning(
            "Provider failover state %s is unreadable (%s); assuming recovery_pending",
            state_path,
            exc,
        )
        return FailoverState(
            mode=FailoverMode.RECOVERY_PENDING,
            updated_at=as_utc(now),
        )


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
    health_dimensions: dict[str, object] | None = None,
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
    payload["preferred_option_provider"] = (
        "ibkr" if monitoring_context == "gth" else "schwab"
    )
    payload["selection_policy"] = (
        "ibkr_gth_primary"
        if monitoring_context == "gth"
        else "schwab_primary_with_ibkr_fallback"
    )
    if health_dimensions is not None:
        payload["health_dimensions"] = health_dimensions
    atomic_write_json_secure(Path(path), payload)


def monitoring_active_at(now: datetime, *, rth_only: bool) -> bool:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return True
    return not rth_only and DEFAULT_MARKET_CALENDAR.is_globex_open(now)


def monitoring_context_at(now: datetime, *, rth_only: bool) -> str:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return "rth"
    if not rth_only and DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        return "gth"
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


def gth_option_health(
    state: LatestState,
    provider: Provider,
    *,
    min_contracts: int,
    quote_max_age_seconds: float,
) -> ProviderHealth:
    """Require a fresh same-session SPXW surface, not merely a live ES anchor."""

    expiry = DEFAULT_MARKET_CALENDAR.research_expiry(state.as_of).strftime("%Y%m%d")
    pairs: dict[float, set[str]] = {}
    contracts: set[str] = set()
    for quote in state.quotes:
        instrument = quote.instrument
        if (
            quote.provider is not provider
            or (instrument.trading_class or "").upper() != "SPXW"
            or instrument.expiry != expiry
            or instrument.strike is None
            or instrument.right is None
            or quote.effective_price is None
        ):
            continue
        source_at = quote.quote_time or quote.trade_time or quote.last_update_at or quote.received_at
        age_seconds = (as_utc(state.as_of) - as_utc(source_at)).total_seconds()
        if quote.quality in {
            MarketDataQuality.FROZEN,
            MarketDataQuality.DELAYED,
            MarketDataQuality.DELAYED_FROZEN,
            MarketDataQuality.MISSING,
            MarketDataQuality.ERROR,
            MarketDataQuality.UNKNOWN,
            MarketDataQuality.SYNTHETIC,
        } or not 0 <= age_seconds <= quote_max_age_seconds:
            continue
        right = str(getattr(instrument.right, "value", instrument.right)).upper()
        pairs.setdefault(float(instrument.strike), set()).add(right)
        contracts.add(instrument.canonical_id)

    complete_pairs = sum(1 for sides in pairs.values() if sides == {"C", "P"})
    required_pairs = max(min_contracts // 2, 1)
    if len(contracts) < min_contracts or complete_pairs < required_pairs:
        return ProviderHealth(
            False,
            f"GTH SPXW coverage {len(contracts)}/{min_contracts} contracts, "
            f"{complete_pairs}/{required_pairs} complete pairs",
        )
    return ProviderHealth(
        True,
        f"GTH live ES plus {len(contracts)} SPXW contracts/{complete_pairs} pairs",
    )


def evaluate_and_persist(
    latest: LatestState,
    settings: ProviderFailoverSettings,
) -> FailoverState:
    persisted_control = load_failover_control(settings.state_path)
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

    schwab_transport = transport_health(
        latest,
        Provider.SCHWAB,
        provider_state_max_age_seconds=settings.provider_state_max_age_seconds,
    )
    ibkr_transport = transport_health(
        latest,
        Provider.IBKR,
        provider_state_max_age_seconds=settings.provider_state_max_age_seconds,
    )
    schwab_anchors = provider_health(
        latest,
        Provider.SCHWAB,
        required_instruments=required_instruments,
        provider_state_max_age_seconds=settings.provider_state_max_age_seconds,
        quote_max_age_seconds=settings.quote_max_age_seconds,
    )
    ibkr_anchors = provider_health(
        latest,
        Provider.IBKR,
        required_instruments=required_instruments,
        provider_state_max_age_seconds=settings.provider_state_max_age_seconds,
        quote_max_age_seconds=settings.quote_max_age_seconds,
    )
    schwab_options = ProviderHealth(True, "not required outside GTH")
    ibkr_options = ProviderHealth(True, "not required outside GTH")
    if context == "gth":
        schwab_options = gth_option_health(
            latest,
            Provider.SCHWAB,
            min_contracts=settings.gth_min_live_option_contracts,
            quote_max_age_seconds=settings.gth_option_quote_max_age_seconds,
        )
        ibkr_options = gth_option_health(
            latest,
            Provider.IBKR,
            min_contracts=settings.gth_min_live_option_contracts,
            quote_max_age_seconds=settings.gth_option_quote_max_age_seconds,
        )
    schwab = schwab_anchors
    ibkr = ibkr_options if context == "gth" and ibkr_anchors.healthy else ibkr_anchors
    if context == "gth":
        schwab = ProviderHealth(False, "IBKR is the configured GTH SPXW primary")
    previous_context = str(persisted_control.get("monitoring_context") or "")
    # Only a known prior context counts as a session transition. A missing or
    # corrupt state file leaves no trustworthy prior context; resetting then
    # would silently restore primary semantics over the conservative default.
    context_changed = bool(previous_context) and previous_context != context
    if context_changed or (
        context == "gth" and current.mode is FailoverMode.SCHWAB_PRIMARY
    ):
        # Session boundaries re-derive the mode from the active context, but
        # the unhealthy/recovery streaks carry over: a provider that was down
        # before the boundary is still down after it, so advance_failover
        # below re-evaluates the carried state against current health instead
        # of restarting the hysteresis from zero.
        current = FailoverState(
            mode=(
                FailoverMode.IBKR_FALLBACK
                if context == "gth" and ibkr.healthy
                else FailoverMode.RECOVERY_PENDING
                if context == "gth"
                else FailoverMode.SCHWAB_PRIMARY
            ),
            updated_at=latest.as_of,
            sequence=current.sequence,
            schwab_unhealthy_streak=current.schwab_unhealthy_streak,
            schwab_recovery_streak=current.schwab_recovery_streak,
            ibkr_unhealthy_streak=current.ibkr_unhealthy_streak,
            ibkr_recovery_streak=current.ibkr_recovery_streak,
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
        health_dimensions={
            "schwab": {
                "transport": health_payload(schwab_transport),
                "anchors": health_payload(schwab_anchors),
                "gth_options": health_payload(
                    schwab_options,
                    required=context == "gth",
                ),
            },
            "ibkr": {
                "transport": health_payload(ibkr_transport),
                "anchors": health_payload(ibkr_anchors),
                "gth_options": health_payload(
                    ibkr_options,
                    required=context == "gth",
                ),
            },
        },
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
        payload = load_failover_control(settings.state_path) or state.to_dict()
        monitoring_active = bool(
            settings.enabled
            and monitoring_active_at(
                latest.as_of,
                rth_only=settings.monitor_rth_only,
            )
        )
        payload.setdefault("monitoring_active", monitoring_active)
        payload.setdefault(
            "monitoring_context",
            monitoring_context_at(latest.as_of, rth_only=settings.monitor_rth_only),
        )
        payload.setdefault(
            "ibkr_market_data_required",
            bool(monitoring_active and state.ibkr_market_data_required),
        )
        payload.setdefault(
            "new_entries_allowed",
            bool(
                monitoring_active
                and state.mode in {FailoverMode.SCHWAB_PRIMARY, FailoverMode.IBKR_FALLBACK}
            ),
        )
        print(json.dumps(payload, sort_keys=True))
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
