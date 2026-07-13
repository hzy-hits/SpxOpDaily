"""Session, policy, and teardown operations for StreamCollector."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.ibkr.farm_health import (
    TWS_CONNECTIVITY_LOST_CODES,
    TWS_CONNECTIVITY_RESTORED_CODES,
)
from spx_spark.ibkr.option_replan import OptionReplanController
from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.capacity_tracker import active_market_data_lines
from spx_spark.ibkr.stream.models import MAX_TRACKED_ERRORS, SUBSCRIPTION_REJECTION_CODES
from spx_spark.ibkr.stream.models import replace_client_id
from spx_spark.ibkr.verifier import IbkrError
from spx_spark.config import env_bool
from spx_spark.provider_failover_controller import load_failover_control
from spx_spark.settings import settings_value
from spx_spark.runtime_mode import ibkr_market_data_allowed, load_override

cancel_subscriptions = stream_deps.cancel_subscriptions
connect_broker_readonly_with_positions = stream_deps.connect_broker_readonly_with_positions
connect_market_data_only = stream_deps.connect_market_data_only
discard_subscriptions = stream_deps.discard_subscriptions
fetch_positions = stream_deps.fetch_positions
log_event = stream_deps.log_event
prepare_ib_client = stream_deps.prepare_ib_client
time = stream_deps.time
write_snapshot = stream_deps.write_snapshot

class SessionOps:
    def _on_error(self, req_id: int, error_code: int, message: str, contract: Any) -> None:
        error = IbkrError(
            req_id=req_id,
            error_code=error_code,
            message=message,
            contract=str(contract) if contract is not None else None,
            ts=datetime.now(tz=timezone.utc).isoformat(),
        )
        self.errors.append(error)
        del self.errors[:-MAX_TRACKED_ERRORS]
        tracker = getattr(self, "capacity_tracker", None)
        if tracker is not None:
            tracker.observe_error(
                error_code=error_code,
                message=message,
                active_lines=active_market_data_lines(self),
            )

        if error_code in TWS_CONNECTIVITY_LOST_CODES:
            self.tws_connectivity_lost = True
            self.tws_connectivity_loss_sequence = (
                getattr(self, "tws_connectivity_loss_sequence", 0) + 1
            )
        elif error_code in TWS_CONNECTIVITY_RESTORED_CODES:
            self.tws_connectivity_lost = False
            if error_code == 1101:
                # TWS explicitly says market-data subscriptions were lost.
                self.subscriptions_lost = True
                self.subscription_health_failed = True

        if error_code in SUBSCRIPTION_REJECTION_CODES:
            self.subscription_rejection_sequence += 1
            self.subscription_rejection_log.append(
                (self.subscription_rejection_sequence, error)
            )
            del self.subscription_rejection_log[:-MAX_TRACKED_ERRORS]
            row = self.subscription_rows_by_req_id.get(req_id)
            if row is not None:
                row.error = f"IBKR {error_code}: {message}"
                row.subscribed = False
                if getattr(self, "subscription_lane_by_req_id", {}).get(req_id) in {
                    "base",
                    "hot",
                }:
                    self.subscription_health_failed = True
            elif req_id < 0:
                self.subscription_health_failed = True

        event = self.farm_health.observe(error_code, message)
        if event is not None:
            log_event(event.to_log_event(task="ibkr_stream"))

    def market_data_allowed(self) -> bool:
        if time.monotonic() < self.market_data_retry_not_before:
            return False
        if self.force:
            return True
        override = load_override(self.runtime_policy.runtime_mode_path)
        control = load_failover_control(self.provider_failover_settings.state_path)
        return ibkr_market_data_allowed(
            self.runtime_policy,
            failover_control=control,
            failover_enabled=self.provider_failover_settings.enabled,
            control_enabled=env_bool(
                "PROVIDER_FAILOVER_CONTROL_IBKR_STREAM_ENABLED",
                bool(settings_value("provider_failover.control_ibkr_stream_enabled")),
            ),
            control_max_age_seconds=(
                self.provider_failover_settings.control_state_max_age_seconds
            ),
            override=override,
        )

    def allowed(self) -> bool:
        """Backward-compatible alias for the market-data subscription gate."""

        return self.market_data_allowed()

    def connection_required(self) -> bool:
        """Keep the broker socket when account reads, execution, or fallback need it."""

        return bool(
            self.broker_settings.account_read_enabled
            or self.broker_settings.execution_mode == "live"
            or self.market_data_allowed()
        )

    def defer_market_data_after_conflict(self, *, seconds: float) -> None:
        self.market_data_retry_not_before = max(
            self.market_data_retry_not_before,
            time.monotonic() + max(seconds, 0.0),
        )

    def open_session(self) -> None:
        if self.broker_settings.account_read_enabled:
            connect_broker_readonly_with_positions(
                self.ib,
                self.ibkr_settings,
                client_id=self.stream_settings.client_id,
            )
        else:
            connect_market_data_only(
                self.ib,
                replace_client_id(self.ibkr_settings, self.stream_settings.client_id),
            )
        self.last_position_shadow_at = None
        if self.market_data_allowed():
            self.ib.reqMarketDataType(self.ibkr_settings.market_data_type)
        self.connection_generation = getattr(self, "connection_generation", 0) + 1
        log_event(
            {
                "task": "ibkr_stream",
                "event": "session_generation",
                "generation": self.connection_generation,
            }
        )

    def flush_position_shadow_if_due(
        self,
        *,
        now_monotonic: float,
    ) -> dict[str, object] | None:
        if not self.broker_settings.position_shadow_active:
            return None
        if self.last_position_shadow_at is not None and (
            now_monotonic - self.last_position_shadow_at
            < self.broker_settings.position_shadow_interval_seconds
        ):
            return None
        self.last_position_shadow_at = now_monotonic
        try:
            snapshot = fetch_positions(self.ib, storage_settings=self.storage_settings)
            write_snapshot(snapshot, self.broker_settings.position_shadow_path)
        except Exception as exc:  # noqa: BLE001 - shadow failure cannot break market data
            return {
                "task": "ibkr_stream",
                "event": "position_shadow_failed",
                "ok": False,
                "error_type": type(exc).__name__,
            }
        return {
            "task": "ibkr_stream",
            "event": "position_shadow_written",
            "ok": True,
            "fetch_complete": snapshot.fetch_complete,
            "spxw_contracts": snapshot.total_contracts,
            "raw_position_count": snapshot.raw_position_count,
        }

    def teardown(self) -> None:
        release = (
            discard_subscriptions
            if getattr(self, "tws_connectivity_lost", False)
            or getattr(self, "subscriptions_lost", False)
            or not self.ib.isConnected()
            else cancel_subscriptions
        )
        release(self.ib, self.rotation_subs)
        release(self.ib, self.hot_subs)
        release(self.ib, self.spy_subs)
        release(self.ib, self.slow_active_subs)
        release(self.ib, self.base_subs)
        self.base_subs = {}
        self.hot_subs = {}
        self.rotation_subs = {}
        self.spy_subs = {}
        self.spy_plan_key = None
        self.spy_retry_at = 0.0
        self.slow_active_subs = {}
        self.option_plan = None
        self.option_replan_controller = OptionReplanController(
            trigger_points=self.stream_settings.replan_drift_points,
            rearm_points=min(10.0, self.stream_settings.replan_drift_points / 2.0),
        )
        self.rotation_index = 0
        self.rotation_retry_at = 0.0
        self.slow_cache = {}
        self.slow_contracts = []
        self.slow_chunks = []
        self.slow_scheduler = None
        self.slow_qualified_contracts = {}
        self.slow_unresolved_contracts = set()
        self.option_cache = {}
        self.qualified_option_contracts = {}
        self.subscription_rejection_sequence = 0
        self.subscription_rejection_log = []
        self.errors = []
        self.subscription_rows_by_req_id = {}
        self.subscription_lane_by_req_id = {}
        self.subscription_health_failed = False
        self.tws_connectivity_lost = False
        self.subscriptions_lost = False
        if self.ib.isConnected():
            self.ib.disconnect()

    def drain_new_errors(self) -> list[IbkrError]:
        errors, self.errors = self.errors, []
        return errors
