"""IBKR StreamCollector: long-lived connection and subscription owner."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from spx_spark.config import (
    IbkrBrokerSettings,
    IbkrSettings,
    IbkrStreamSettings,
    RuntimePolicySettings,
    SamplingSettings,
    StorageSettings,
)
from spx_spark.ibkr.atm_reference import AtmReferenceController
from spx_spark.ibkr.option_replan import OptionReplanController
from spx_spark.ibkr.slow_poll import SlowPollScheduler
from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.flush_ops import FlushOps
from spx_spark.ibkr.stream.models import OptionSubscriptionPlan
from spx_spark.ibkr.stream.option_subscription_ops import OptionSubscriptionOps
from spx_spark.ibkr.stream.session_ops import SessionOps
from spx_spark.ibkr.stream.slow_poll_ops import SlowPollOps
from spx_spark.ibkr.stream.spy_rotation_ops import SpyRotationOps
from spx_spark.ibkr.verifier import IbkrError, VerifyRow
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, MarketCalendar
from spx_spark.provider_failover_controller import ProviderFailoverSettings

FarmHealthTracker = stream_deps.FarmHealthTracker


class StreamCollector(
    SessionOps,
    SlowPollOps,
    OptionSubscriptionOps,
    SpyRotationOps,
    FlushOps,
):
    """Owns the long-lived IB connection and subscription lifecycle."""

    def __init__(
        self,
        ib: Any,
        *,
        ibkr_settings: IbkrSettings,
        stream_settings: IbkrStreamSettings,
        sampling_settings: SamplingSettings,
        storage_settings: StorageSettings,
        runtime_policy: RuntimePolicySettings,
        broker_settings: IbkrBrokerSettings | None = None,
        force: bool = False,
        skip_options: bool = False,
    ) -> None:
        self.ib = ib
        self.ibkr_settings = ibkr_settings
        self.stream_settings = stream_settings
        self.sampling_settings = sampling_settings
        self.storage_settings = storage_settings
        self.runtime_policy = runtime_policy
        self.broker_settings = broker_settings or IbkrBrokerSettings.from_env()
        self.force = force
        self.skip_options = skip_options or stream_settings.skip_options
        self.provider_failover_settings = ProviderFailoverSettings.from_env()

        self.base_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.hot_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.rotation_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.spy_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.spy_plan_key: tuple[str, int] | None = None
        self.spy_retry_at = 0.0
        self.option_plan: OptionSubscriptionPlan | None = None
        self.option_replan_controller = OptionReplanController(
            trigger_points=stream_settings.replan_drift_points,
            rearm_points=min(10.0, stream_settings.replan_drift_points / 2.0),
        )
        atm_state_path = stream_settings.atm_state_path or str(
            Path(storage_settings.data_root) / "state" / "ibkr_atm_reference.json"
        )
        self.atm_reference_controller = AtmReferenceController(Path(atm_state_path))
        self.market_calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR
        self.rotation_index = 0
        self.rotation_retry_at = 0.0
        self.errors: list[IbkrError] = []
        self.subscription_rejection_sequence = 0
        self.subscription_rejection_log: list[tuple[int, IbkrError]] = []
        self.subscription_rows_by_req_id: dict[int, VerifyRow] = {}
        self.subscription_lane_by_req_id: dict[int, str] = {}
        self.subscription_health_failed = False
        self.tws_connectivity_lost = False
        self.subscriptions_lost = False
        self.tws_connectivity_loss_sequence = 0
        self.last_policy_check = 0.0
        self.last_position_shadow_at: float | None = None
        self.market_data_retry_not_before = 0.0
        self.farm_health = FarmHealthTracker(
            broken_restart_seconds=stream_settings.farm_broken_restart_seconds,
        )
        self.slow_cache: dict[str, VerifyRow] = {}
        self.slow_contracts: list[tuple[str, str, Any]] = []
        self.slow_chunks: list[list[tuple[str, str, Any]]] = []
        self.slow_scheduler: SlowPollScheduler | None = None
        self.slow_active_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.slow_qualified_contracts: dict[str, tuple[str, str, Any]] = {}
        self.slow_unresolved_contracts: set[str] = set()
        # Rotated option rows linger here between their subscription windows so
        # every flush carries the full chain, not just the current slice.
        # Without this, walls/GEX are computed on a shifting 1/N subset of
        # strikes and jump from push to push.
        self.option_cache: dict[str, tuple[float, VerifyRow]] = {}
        self.qualified_option_contracts: dict[str, tuple[str, str, Any]] = {}
        self.connection_generation = 0

        ib.errorEvent += self._on_error

