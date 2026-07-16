"""Flush and subscription-lifecycle advance for StreamCollector."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.models import lifecycle_has_qualification_budget
from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.config import default_spxw_expiry
from spx_spark.marketdata import ProviderStatus

decide_after_flush = stream_deps.decide_after_flush
has_competing_session_error = stream_deps.has_competing_session_error
log_event = stream_deps.log_event
mark_rows_stale = stream_deps.mark_rows_stale
merge_cached_option_rows = stream_deps.merge_cached_option_rows
merge_slow_rows = stream_deps.merge_slow_rows
persist_provider_snapshot = stream_deps.persist_provider_snapshot
persist_state_only = stream_deps.persist_state_only
provider_error_count = stream_deps.provider_error_count
snapshot_from_rows = stream_deps.snapshot_from_rows
snapshot_rows = stream_deps.snapshot_rows
subscription_outage_reason = stream_deps.subscription_outage_reason
time = stream_deps.time
unavailable_state = stream_deps.unavailable_state
update_option_cache = stream_deps.update_option_cache


class FlushOps:
    def flush(self) -> dict[str, object]:
        received_at = datetime.now(tz=timezone.utc)
        freeze_on_loss = bool(
            getattr(self.stream_settings, "freeze_quotes_on_connectivity_loss", True)
        )
        if freeze_on_loss and not self.ib.isConnected():
            persist_state_only(
                unavailable_state("IBKR disconnected mid-session", connected=False),
                self.storage_settings,
            )
            return {
                "task": "ibkr_stream",
                "event": "flush",
                "quotes": 0,
                "best_quotes": 0,
                "provider_status": ProviderStatus.UNAVAILABLE.value,
                "rotation_index": self.rotation_index,
                "tws_connectivity_lost": self.tws_connectivity_lost,
            }

        subscriptions = {
            **self.base_subs,
            **self.hot_subs,
            **self.rotation_subs,
            **self.spy_subs,
        }
        rows = snapshot_rows(
            subscriptions,
            self.ibkr_settings.stale_after_seconds,
            slow_index_stale_after_seconds=self.ibkr_settings.slow_index_stale_after_seconds,
            slow_index_labels=self.ibkr_settings.slow_index_labels,
        )
        merge_slow_rows(rows, self.slow_cache, set(subscriptions))
        update_option_cache(
            self.option_cache,
            rows,
            now_monotonic=time.monotonic(),
            expiry=self.option_plan.expiry if self.option_plan is not None else None,
            active_expiries=frozenset(
                spec.expiry
                for spec in (
                    self.option_plan.hot
                    + tuple(item for rotation in self.option_plan.rotations for item in rotation)
                )
            )
            if self.option_plan is not None
            else None,
        )
        merge_cached_option_rows(rows, self.option_cache, set(subscriptions))
        if freeze_on_loss and self.tws_connectivity_lost:
            mark_rows_stale(rows)
        outage_reason = subscription_outage_reason(
            tws_connectivity_lost=self.tws_connectivity_lost,
            subscriptions_lost=self.subscriptions_lost,
        )
        if not self.farm_health.market_data_ready():
            farm_reason = "IBKR market data farms not ready"
            outage_reason = f"{outage_reason}; {farm_reason}" if outage_reason else farm_reason
        error_count = provider_error_count(self.errors)
        if outage_reason is not None:
            error_count = max(error_count, 1)
        source_session = f"ibkr-stream:{self.connection_generation}"
        snapshot = snapshot_from_rows(
            rows,
            received_at=received_at,
            stale_after_seconds=self.ibkr_settings.stale_after_seconds,
            connected=self.ib.isConnected(),
            authenticated=True,
            latency_ms=None,
            error_count=error_count,
            reason=outage_reason,
            replace_provider_quotes=True,
            source_session=source_session,
        )
        write_result = persist_provider_snapshot(snapshot, self.storage_settings)
        lifecycle_started = time.monotonic()

        self._advance_subscription_lifecycle(
            rows,
            lifecycle_started=lifecycle_started,
        )
        return {
            "task": "ibkr_stream",
            "event": "flush",
            "quotes": snapshot.quote_count,
            "best_quotes": write_result.best_quote_count,
            "provider_status": (
                snapshot.provider_state.status.value if snapshot.provider_state else "unknown"
            ),
            "provider_reason": (
                snapshot.provider_state.reason if snapshot.provider_state else None
            ),
            "farm_status": self.farm_health.status.value,
            "rotation_index": self.rotation_index,
            "tws_connectivity_lost": self.tws_connectivity_lost,
            "source_session": source_session,
        }

    def _advance_subscription_lifecycle(
        self,
        rows: list[VerifyRow],
        *,
        lifecycle_started: float,
    ) -> None:
        """Advance one bounded lifecycle slice after hot rows are persisted."""

        if self._subscription_lifecycle_blocked():
            return

        # Complete an already-held slow batch promptly, but never start a new
        # qualification before the hot-plan work has had its bounded turn.
        self.advance_slow_poll(allow_start=False)
        if self._subscription_lifecycle_blocked():
            return
        option_warmup = bool(
            self.option_plan is not None
            and getattr(self.option_plan, "rotation_count", 0) > 0
            and self.rotation_index < getattr(self.option_plan, "rotation_count", 0)
        )
        if option_warmup:
            if lifecycle_has_qualification_budget(lifecycle_started):
                self.rotate_options()
            self.advance_slow_poll(allow_start=False)
            return
        self.ensure_option_plan(rows)
        if self._subscription_lifecycle_blocked():
            return
        if lifecycle_has_qualification_budget(lifecycle_started):
            self.ensure_spy_option_plan(
                rows,
                expiry=(
                    self.option_plan.expiry
                    if self.option_plan is not None
                    else default_spxw_expiry()
                ),
            )
        if self._subscription_lifecycle_blocked():
            return
        if lifecycle_has_qualification_budget(lifecycle_started):
            if self.slow_poll_start_due():
                # A due slow chunk gets one lifecycle slice ahead of rotation;
                # otherwise continuous rotation qualification can starve it.
                self.advance_slow_poll(allow_start=True)
            else:
                self.rotate_options()
        if self._subscription_lifecycle_blocked():
            return
        # A zero-hold batch, or one whose hold elapsed during other lifecycle
        # work, can be completed without admitting another qualification.
        self.advance_slow_poll(allow_start=False)
