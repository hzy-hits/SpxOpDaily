"""StreamRuntime: reconnect / conflict / policy lifecycle supervisor."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock

from spx_spark.config import IbkrStreamSettings, RuntimePolicySettings, StorageSettings
from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.collector import StreamCollector
from spx_spark.ibkr.stream.health import persist_stream_health
from spx_spark.ibkr.stream.models import (
    CompetingSessionCircuit,
    ReconnectPolicy,
    StreamAction,
    effective_hot_flush_sleep_seconds,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR

classify_connect_failure = stream_deps.classify_connect_failure
connected_state = stream_deps.connected_state
decide_after_flush = stream_deps.decide_after_flush
has_competing_session_error = stream_deps.has_competing_session_error
log_event = stream_deps.log_event
persist_account_standby_state = stream_deps.persist_account_standby_state
persist_state_only = stream_deps.persist_state_only
probe_data_plane = stream_deps.probe_data_plane
request_gateway_restart = stream_deps.request_gateway_restart
runtime_blocks_gateway_restart = stream_deps.runtime_blocks_gateway_restart
sleep_until_reconnect = stream_deps.sleep_until_reconnect
time = stream_deps.time
unavailable_state = stream_deps.unavailable_state


@dataclass
class StreamRuntime:
    collector: StreamCollector
    stream_settings: IbkrStreamSettings
    storage_settings: StorageSettings
    runtime_policy: RuntimePolicySettings
    reconnect: ReconnectPolicy = field(init=False)
    competing_session_circuit: CompetingSessionCircuit = field(init=False)
    deadline: float | None = None
    last_gateway_restart_at: float | None = None
    session_had_healthy_flush: bool = False
    _health_lock: RLock = field(init=False, repr=False)
    _competing_health_latched_sequence: int | None = field(
        init=False,
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        self._health_lock = RLock()
        self.collector.competing_session_health_invalidator = (
            self._invalidate_competing_session_health
        )
        self.reconnect = ReconnectPolicy(
            min_seconds=self.stream_settings.reconnect_min_seconds,
            max_seconds=self.stream_settings.reconnect_max_seconds,
        )
        conflict_min = max(
            float(getattr(self.runtime_policy, "ibkr_conflict_probe_seconds", 5.0)),
            0.1,
        )
        conflict_max = max(
            float(
                getattr(
                    self.runtime_policy,
                    "ibkr_conflict_probe_max_seconds",
                    15.0,
                )
            ),
            conflict_min,
        )
        # Stability window is independent of probe cadence: probe interval
        # controls how fast we reclaim after release; recovery_seconds only
        # decides when a healthy flush is trusted enough to close the circuit.
        recovery_seconds = max(
            float(
                getattr(
                    self.runtime_policy,
                    "ibkr_conflict_recovery_seconds",
                    60.0,
                )
            ),
            0.1,
        )
        self.competing_session_circuit = CompetingSessionCircuit(
            min_seconds=conflict_min,
            max_seconds=conflict_max,
            recovery_seconds=recovery_seconds,
        )

    def expired(self) -> bool:
        return self.deadline is not None and time.monotonic() >= self.deadline

    def run(self) -> int:
        while not self.expired():
            if not self.collector.connection_required():
                block_reason = getattr(self.collector, "market_data_block_reason", None)
                reason = block_reason() if callable(block_reason) else None
                reason = reason or "runtime policy blocks IBKR collection"
                retry_delay = getattr(
                    self.collector,
                    "market_data_retry_delay_seconds",
                    None,
                )
                retry_seconds = retry_delay() if callable(retry_delay) else None
                sleep_seconds = self.stream_settings.policy_check_seconds
                if retry_seconds is not None:
                    sleep_seconds = min(sleep_seconds, max(retry_seconds, 0.1))
                persist_state_only(
                    unavailable_state(reason),
                    self.storage_settings,
                )
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=True,
                    reason=reason,
                    retry_in_seconds=retry_seconds,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "policy_blocked",
                        "reason": reason,
                        "retry_in_seconds": sleep_seconds,
                    }
                )
                self.sleep(sleep_seconds)
                continue

            try:
                self.collector.open_session()
            except Exception as exc:  # noqa: BLE001
                delay = self.reconnect.next_delay()
                persist_state_only(
                    unavailable_state(f"connect failed: {exc}"),
                    self.storage_settings,
                )
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=False,
                    reason=f"connect failed: {exc}",
                    retry_in_seconds=delay,
                )
                connect_event: dict[str, object] = {
                    "task": "ibkr_stream",
                    "event": "connect_failed",
                    "error": str(exc),
                    "retry_in_seconds": delay,
                }
                error_class = classify_connect_failure(exc)
                if error_class is not None:
                    connect_event["error_class"] = error_class
                log_event(connect_event)
                sleep_until_reconnect(
                    host=self.collector.ibkr_settings.host,
                    port=self.collector.ibkr_settings.port,
                    delay_seconds=delay,
                )
                continue

            log_event({"task": "ibkr_stream", "event": "connected"})
            self._publish_health(
                data_plane_healthy=False,
                policy_blocked=False,
                reason="connected; awaiting first healthy data flush",
            )
            needs_reconnect_backoff = False
            self.session_had_healthy_flush = False
            try:
                if self.collector.market_data_allowed():
                    persist_state_only(connected_state(), self.storage_settings)
                    probe = probe_data_plane(
                        self.collector.ib,
                        self.collector.ibkr_settings,
                    )
                    log_event(probe.to_log_event())
                    if not probe.ok:
                        event = self.collector.farm_health.mark_probe_failed(probe)
                        log_event(event.to_log_event(task="ibkr_stream"))
                        self._publish_health(
                            data_plane_healthy=False,
                            policy_blocked=False,
                            reason=f"data-plane probe failed: {probe.reason}",
                        )
                    else:
                        self.collector.farm_health.mark_probe_succeeded()
                    self.collector.subscribe_base()
                    prime = getattr(self.collector, "prime_priority_market_data", None)
                    if callable(prime):
                        prime()
                    needs_reconnect_backoff = self.session_loop()
                else:
                    persist_account_standby_state(self.storage_settings)
                    self._publish_health(
                        data_plane_healthy=False,
                        policy_blocked=True,
                        reason="account standby connected; market data inactive",
                    )
                    log_event(
                        {
                            "task": "ibkr_stream",
                            "event": "account_standby_connected",
                        }
                    )
                    needs_reconnect_backoff = self.account_standby_loop()
            except Exception as exc:  # noqa: BLE001
                setup_errors = self.collector.drain_new_errors()
                if has_competing_session_error(setup_errors):
                    needs_reconnect_backoff = False
                    self._defer_competing_session(phase="subscription_setup")
                else:
                    needs_reconnect_backoff = True
                    persist_state_only(
                        unavailable_state(f"session failed: {exc}", connected=False),
                        self.storage_settings,
                    )
                    self._publish_health(
                        data_plane_healthy=False,
                        policy_blocked=False,
                        reason=f"session failed: {exc}",
                    )
                    log_event({"task": "ibkr_stream", "event": "session_error", "error": str(exc)})
            finally:
                self.competing_session_circuit.interrupt_recovery()
                self.collector.teardown()
            if self.session_had_healthy_flush:
                self.reconnect.reset()
            if needs_reconnect_backoff and not self.expired():
                delay = self.reconnect.next_delay()
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "session_reconnect_backoff",
                        "retry_in_seconds": delay,
                    }
                )
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=False,
                    reason="session reconnect backoff",
                    retry_in_seconds=delay,
                )
                self.sleep(delay)
        return 0

    def account_standby_loop(self) -> bool:
        """Maintain positions/account visibility without market subscriptions."""

        while not self.expired():
            self.collector.ib.sleep(self.stream_settings.policy_check_seconds)
            position_event = self.collector.flush_position_shadow_if_due(
                now_monotonic=time.monotonic()
            )
            if position_event is not None:
                log_event(position_event)
            block_reason = getattr(self.collector, "market_data_block_reason", None)
            reason = block_reason() if callable(block_reason) else None
            self._publish_health(
                data_plane_healthy=False,
                policy_blocked=reason is not None,
                reason=reason or "account standby; market data inactive",
            )
            if not self.collector.ib.isConnected() or self.collector.tws_connectivity_lost:
                persist_state_only(
                    unavailable_state("IBKR account standby disconnected"),
                    self.storage_settings,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "account_standby_disconnected",
                    }
                )
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=True,
                    reason="account standby disconnected",
                )
                return True
            self.session_had_healthy_flush = True
            if not self.collector.connection_required():
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "account_standby_not_required",
                    }
                )
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=True,
                    reason="account standby no longer required",
                )
                return False
            if self.collector.market_data_allowed():
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "market_data_activation_requested",
                    }
                )
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=False,
                    reason="market-data activation requested",
                )
                return False
        return False

    def session_loop(self) -> bool:
        flush_interval = effective_hot_flush_sleep_seconds(
            self.stream_settings.flush_interval_seconds
        )
        next_flush_at = time.monotonic() + flush_interval
        while not self.expired():
            if not self._wait_for_hot_flush(next_flush_at=next_flush_at):
                return False
            flush_started_at = time.monotonic()
            event = self.collector.flush()
            log_event(event)
            position_event = self.collector.flush_position_shadow_if_due(
                now_monotonic=time.monotonic()
            )
            if position_event is not None:
                log_event(position_event)

            # Classify newly observed 10197 errors before the generic
            # subscription-health branch.  A competing live session must use
            # the conflict cooldown instead of an ordinary reconnect loop.
            new_errors = self.collector.drain_new_errors()
            competing = has_competing_session_error(new_errors)
            if self.collector.subscription_health_failed and not competing:
                persist_state_only(
                    unavailable_state(
                        "IBKR subscription lifecycle failed; reconnecting",
                        connected=self.collector.ib.isConnected(),
                    ),
                    self.storage_settings,
                )
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=False,
                    reason="IBKR subscription lifecycle failed; reconnecting",
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "subscription_health_reconnect",
                    }
                )
                return True

            gateway_restart = self._should_restart_gateway()
            action = decide_after_flush(
                connected=self.collector.ib.isConnected(),
                allowed=self.collector.market_data_allowed(),
                competing_session=competing,
                gateway_restart=gateway_restart,
            )
            if action is StreamAction.CONTINUE:
                data_plane_healthy = bool(
                    event.get(
                        "data_plane_healthy",
                        event.get("provider_status") == "available"
                        and int(event.get("quotes") or 0) > 0
                        and event.get("farm_status", "ok") == "ok",
                    )
                )
                reason = (
                    "healthy market-data flush"
                    if data_plane_healthy
                    else str(
                        event.get("provider_reason")
                        or f"data plane {event.get('provider_status', 'unknown')}"
                    )
                )
                if data_plane_healthy:
                    self.session_had_healthy_flush = True
                    fresh_spxw_quotes = int(
                        event.get("fresh_spxw_quotes", event.get("fresh_quotes", 0)) or 0
                    )
                    circuit_recovered = False
                    if fresh_spxw_quotes > 0:
                        circuit_recovered = self.competing_session_circuit.observe_healthy(
                            now_monotonic=time.monotonic()
                        )
                    else:
                        self.competing_session_circuit.interrupt_recovery()
                    if circuit_recovered:
                        self._clear_competing_session_health_latch()
                        log_event(
                            {
                                "task": "ibkr_stream",
                                "event": "competing_session_recovered",
                                "stable_seconds": self.competing_session_circuit.recovery_seconds,
                            }
                        )
                    clear_conflict = getattr(
                        self.collector,
                        "clear_market_data_conflict",
                        None,
                    )
                    if callable(clear_conflict):
                        clear_conflict()
                else:
                    self.competing_session_circuit.interrupt_recovery()
                self._publish_health(
                    data_plane_healthy=data_plane_healthy,
                    policy_blocked=False,
                    reason=reason,
                )
                next_flush_at = flush_started_at + flush_interval
                continue

            if action is StreamAction.GATEWAY_RESTART:
                self._restart_gateway_for_farm_outage()
                return False

            if action is StreamAction.CONFLICT_WAIT:
                self._defer_competing_session(phase="active_session")
                return False

            if action is StreamAction.POLICY_BLOCKED:
                log_event({"task": "ibkr_stream", "event": "policy_blocked_mid_session"})
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=True,
                    reason="runtime policy blocked market data mid-session",
                )
                return False

            # RECONNECT: fall back to the outer loop's backoff.
            persist_state_only(
                unavailable_state("IBKR disconnected mid-session", connected=False),
                self.storage_settings,
            )
            self._publish_health(
                data_plane_healthy=False,
                policy_blocked=False,
                reason="IBKR disconnected mid-session",
            )
            log_event({"task": "ibkr_stream", "event": "disconnected"})
            return True
        return False

    def _wait_for_hot_flush(self, *, next_flush_at: float) -> bool:
        """Poll exact-leg demand without increasing the durable flush cadence."""

        demand_enabled = bool(
            getattr(self.stream_settings, "exact_leg_pin_enabled", False)
        )
        poll_seconds = min(
            max(
                float(getattr(self.stream_settings, "quote_demand_poll_seconds", 0.05)),
                0.01,
            ),
            0.05,
        )
        while not self.expired():
            if getattr(self.collector, "subscription_health_failed", False):
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=False,
                    reason="subscription health failed; awaiting error classification",
                )
                return True
            remaining = next_flush_at - time.monotonic()
            if remaining <= 1e-9:
                return True
            # Error callbacks run while ``ib.sleep`` services the event loop.
            # Always use a bounded slice, even when exact-leg demand is off, so
            # a session-wide 10197 cannot leave the prior healthy projection
            # actionable until the next ordinary hot flush.
            sleep_seconds = min(remaining, poll_seconds)
            self.collector.ib.sleep(sleep_seconds)
            if self.expired():
                return False
            if getattr(self.collector, "subscription_health_failed", False):
                self._publish_health(
                    data_plane_healthy=False,
                    policy_blocked=False,
                    reason="subscription health failed; awaiting error classification",
                )
                return True
            lifecycle_blocked = bool(
                getattr(self.collector, "tws_connectivity_lost", False)
                or not self.collector.ib.isConnected()
            )
            if demand_enabled and not lifecycle_blocked:
                reconcile = getattr(self.collector, "reconcile_exact_leg_demand", None)
                if callable(reconcile):
                    event = reconcile()
                    if event is not None:
                        log_event(event)
        return False

    def _defer_competing_session(self, *, phase: str) -> None:
        delay = self.competing_session_circuit.open(now_monotonic=time.monotonic())
        persist_state_only(
            unavailable_state(
                "competing session blocks live market data (IBKR 10197)",
                connected=self.collector.ib.isConnected(),
            ),
            self.storage_settings,
        )
        self.collector.defer_market_data_after_conflict(seconds=delay)
        self._publish_health(
            data_plane_healthy=False,
            policy_blocked=True,
            reason="competing session blocks live market data (IBKR 10197)",
            retry_in_seconds=delay,
        )
        log_event(
            {
                "task": "ibkr_stream",
                "event": "competing_session",
                "phase": phase,
                "probe_in_seconds": delay,
                "conflict_count": self.competing_session_circuit.failures,
                "circuit_state": "open",
                "account_standby_eligible": self.collector.broker_settings.account_read_enabled,
            }
        )

    def _should_restart_gateway(self) -> bool:
        if not self.stream_settings.auto_restart_gateway_on_farm_broken:
            return False
        # Error 10197 is an entitlement-owner conflict, not a broken Gateway.
        # Restarting while the conflict circuit is open/recovering can overlap
        # the old and new broker sessions and make the handoff worse. Wait for
        # continuously fresh SPXW data to close the circuit first.
        if self.competing_session_circuit.failures > 0:
            return False
        if runtime_blocks_gateway_restart(self.runtime_policy, force=self.collector.force):
            return False
        calendar = getattr(self.collector, "market_calendar", DEFAULT_MARKET_CALENDAR)
        if not calendar.is_globex_open(datetime.now(tz=timezone.utc)):
            return False
        if not self.collector.farm_health.should_restart_gateway():
            return False
        if self.last_gateway_restart_at is not None:
            elapsed = time.monotonic() - self.last_gateway_restart_at
            if elapsed < self.stream_settings.gateway_restart_cooldown_seconds:
                return False
        return True

    def _restart_gateway_for_farm_outage(self) -> None:
        broken_seconds = self.collector.farm_health.broken_duration()
        failed_farm = self.collector.farm_health.oldest_broken_farm()
        persist_state_only(
            unavailable_state(
                "IBKR data farms broken; restarting gateway",
                connected=self.collector.ib.isConnected(),
            ),
            self.storage_settings,
        )
        self._publish_health(
            data_plane_healthy=False,
            policy_blocked=False,
            reason="IBKR data farms broken; restarting gateway",
            retry_in_seconds=self.stream_settings.gateway_restart_cooldown_seconds,
        )
        restarted = request_gateway_restart()
        self.last_gateway_restart_at = time.monotonic()
        self.collector.farm_health.reset()
        log_event(
            {
                "task": "ibkr_stream",
                "event": "gateway_restart_requested",
                "restarted": restarted,
                "broken_seconds": round(broken_seconds or 0.0, 1),
                "farm": failed_farm,
                "cooldown_seconds": self.stream_settings.gateway_restart_cooldown_seconds,
            }
        )
        self.collector.teardown()
        self.sleep(self.stream_settings.gateway_restart_cooldown_seconds)

    def _publish_health(
        self,
        *,
        data_plane_healthy: bool,
        policy_blocked: bool,
        reason: str,
        retry_in_seconds: float | None = None,
    ) -> None:
        """Best-effort projection; telemetry failure must not stop collection."""

        if not getattr(self.storage_settings, "data_root", None):
            return
        with self._health_lock:
            self._persist_health_locked(
                data_plane_healthy=data_plane_healthy,
                policy_blocked=policy_blocked,
                reason=reason,
                retry_in_seconds=retry_in_seconds,
            )

    def _invalidate_competing_session_health(
        self,
        *,
        error_code: int,
        message: str,
    ) -> None:
        """Durably close GTH entry authority from the broker error callback."""

        del error_code, message
        with self._health_lock:
            self._competing_health_latched_sequence = (
                (self._competing_health_latched_sequence or 0) + 1
            )
            if not getattr(self.storage_settings, "data_root", None):
                return
            self._persist_health_locked(
                data_plane_healthy=False,
                policy_blocked=True,
                reason="competing session callback (IBKR 10197)",
            )

    def _clear_competing_session_health_latch(self) -> None:
        """Release only the incident generation proven healthy by the circuit."""

        with self._health_lock:
            if (
                self.competing_session_circuit.failures == 0
                and not getattr(self.collector, "subscription_health_failed", False)
            ):
                self._competing_health_latched_sequence = None

    def _persist_health_locked(
        self,
        *,
        data_plane_healthy: bool,
        policy_blocked: bool,
        reason: str,
        retry_in_seconds: float | None = None,
    ) -> None:
        """Write health while holding the callback/publisher ordering fence."""

        now_monotonic = time.monotonic()
        circuit_state = self.competing_session_circuit.state(
            now_monotonic=now_monotonic
        )
        circuit_remaining = self.competing_session_circuit.remaining_seconds(
            now_monotonic=now_monotonic
        )
        conflict_count = self.competing_session_circuit.failures
        if self._competing_health_latched_sequence is not None:
            data_plane_healthy = False
            policy_blocked = True
            reason = "competing live session callback latched (IBKR 10197)"
            conflict_count = max(conflict_count, 1)
            if circuit_state == "closed":
                circuit_state = "open"
        elif circuit_state == "open":
            retry_in_seconds = circuit_remaining
            policy_blocked = True
            reason = "competing live session cooldown (IBKR 10197)"
        connected = bool(
            getattr(getattr(self.collector, "ib", None), "isConnected", lambda: False)()
        )
        try:
            persist_stream_health(
                self.storage_settings,
                data_plane_healthy=data_plane_healthy,
                policy_blocked=policy_blocked,
                reason=reason,
                connected=connected,
                circuit_state=circuit_state,
                conflict_count=conflict_count,
                retry_in_seconds=retry_in_seconds,
                connection_generation=getattr(
                    self.collector,
                    "connection_generation",
                    None,
                ),
                max_age_seconds=max(
                    float(getattr(self.stream_settings, "policy_check_seconds", 30.0))
                    * 3.0,
                    30.0,
                ),
            )
        except OSError as exc:
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "health_projection_failed",
                    "error_type": type(exc).__name__,
                }
            )

    def sleep(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self.expired():
            time.sleep(min(remaining, 1.0))
            remaining -= 1.0
