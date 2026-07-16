"""StreamRuntime: reconnect / conflict / policy lifecycle supervisor."""

from __future__ import annotations

from dataclasses import dataclass, field

from spx_spark.config import IbkrStreamSettings, RuntimePolicySettings, StorageSettings
from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.collector import StreamCollector
from spx_spark.ibkr.stream.models import (
    ReconnectPolicy,
    StreamAction,
    effective_hot_flush_sleep_seconds,
)

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
    deadline: float | None = None
    last_gateway_restart_at: float | None = None
    session_had_healthy_flush: bool = False

    def __post_init__(self) -> None:
        self.reconnect = ReconnectPolicy(
            min_seconds=self.stream_settings.reconnect_min_seconds,
            max_seconds=self.stream_settings.reconnect_max_seconds,
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
                    else:
                        self.collector.farm_health.mark_probe_succeeded()
                    self.collector.subscribe_base()
                    prime = getattr(self.collector, "prime_priority_market_data", None)
                    if callable(prime):
                        prime()
                    needs_reconnect_backoff = self.session_loop()
                else:
                    persist_account_standby_state(self.storage_settings)
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
                    log_event({"task": "ibkr_stream", "event": "session_error", "error": str(exc)})
            finally:
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
                return True
            self.session_had_healthy_flush = True
            if not self.collector.connection_required():
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "account_standby_not_required",
                    }
                )
                return False
            if self.collector.market_data_allowed():
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "market_data_activation_requested",
                    }
                )
                return False
        return False

    def session_loop(self) -> bool:
        while not self.expired():
            self.collector.ib.sleep(
                effective_hot_flush_sleep_seconds(self.stream_settings.flush_interval_seconds)
            )
            event = self.collector.flush()
            log_event(event)
            position_event = self.collector.flush_position_shadow_if_due(
                now_monotonic=time.monotonic()
            )
            if position_event is not None:
                log_event(position_event)
            if self.collector.subscription_health_failed:
                persist_state_only(
                    unavailable_state(
                        "IBKR subscription lifecycle failed; reconnecting",
                        connected=self.collector.ib.isConnected(),
                    ),
                    self.storage_settings,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "subscription_health_reconnect",
                    }
                )
                return True

            new_errors = self.collector.drain_new_errors()
            competing = has_competing_session_error(new_errors)
            gateway_restart = self._should_restart_gateway()
            action = decide_after_flush(
                connected=self.collector.ib.isConnected(),
                allowed=self.collector.market_data_allowed(),
                competing_session=competing,
                gateway_restart=gateway_restart,
            )
            if action is StreamAction.CONTINUE:
                self.session_had_healthy_flush = True
                continue

            if action is StreamAction.GATEWAY_RESTART:
                self._restart_gateway_for_farm_outage()
                return False

            if action is StreamAction.CONFLICT_WAIT:
                self._defer_competing_session(phase="active_session")
                return False

            if action is StreamAction.POLICY_BLOCKED:
                log_event({"task": "ibkr_stream", "event": "policy_blocked_mid_session"})
                return False

            # RECONNECT: fall back to the outer loop's backoff.
            persist_state_only(
                unavailable_state("IBKR disconnected mid-session", connected=False),
                self.storage_settings,
            )
            log_event({"task": "ibkr_stream", "event": "disconnected"})
            return True
        return False

    def _defer_competing_session(self, *, phase: str) -> None:
        persist_state_only(
            unavailable_state(
                "competing session blocks live market data (IBKR 10197)",
                connected=self.collector.ib.isConnected(),
            ),
            self.storage_settings,
        )
        self.collector.defer_market_data_after_conflict(
            seconds=self.runtime_policy.ibkr_conflict_probe_seconds
        )
        log_event(
            {
                "task": "ibkr_stream",
                "event": "competing_session",
                "phase": phase,
                "probe_in_seconds": self.runtime_policy.ibkr_conflict_probe_seconds,
                "account_standby_eligible": self.collector.broker_settings.account_read_enabled,
            }
        )

    def _should_restart_gateway(self) -> bool:
        if not self.stream_settings.auto_restart_gateway_on_farm_broken:
            return False
        if runtime_blocks_gateway_restart(self.runtime_policy, force=self.collector.force):
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

    def sleep(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self.expired():
            time.sleep(min(remaining, 1.0))
            remaining -= 1.0
