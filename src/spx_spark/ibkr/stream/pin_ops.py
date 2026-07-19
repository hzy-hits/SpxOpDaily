"""Dynamic, lease-bound ownership of exact GTH SPXW spread legs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.ibkr.quote_demand import (
    ExactLegQuoteDemand,
    load_exact_leg_quote_demand,
    quote_demand_ack_path,
    quote_demand_path,
    write_quote_demand_ack,
)
from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.capacity_tracker import active_market_data_lines
from spx_spark.ibkr.stream.pin_readiness import ExactLegQuoteReadiness
from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


PIN_CAPACITY_RESERVE_LINES = 6

log_event = stream_deps.log_event
option_contracts_from_specs = stream_deps.option_contracts_from_specs
option_label_distance = stream_deps.option_label_distance
qualify_and_subscribe = stream_deps.qualify_and_subscribe

Subscription = tuple[Any, VerifyRow]
Subscriptions = dict[str, Subscription]


class ExactLegPinOps:
    """Reconcile a latest-wins quote demand inside the sole IBKR owner."""

    def _initialize_exact_leg_pin(self) -> None:
        settings = self.stream_settings
        data_root = self.storage_settings.data_root
        configured_demand_path = str(getattr(settings, "quote_demand_path", "") or "")
        configured_ack_path = str(getattr(settings, "quote_demand_ack_path", "") or "")
        self.exact_leg_demand_path = (
            Path(configured_demand_path)
            if configured_demand_path
            else quote_demand_path(data_root)
        )
        self.exact_leg_demand_ack_path = (
            Path(configured_ack_path)
            if configured_ack_path
            else quote_demand_ack_path(data_root)
        )
        self._exact_leg_demand: ExactLegQuoteDemand | None = None
        self._exact_leg_pin_origins: dict[str, str] = {}
        self._exact_leg_hot_victims: Subscriptions = {}
        self._exact_leg_last_file_revision: tuple[int, int] | None = None
        self._exact_leg_readiness = ExactLegQuoteReadiness()
        self._clear_exact_leg_lifecycle()

    def _reset_exact_leg_pin(self) -> None:
        readiness = getattr(self, "_exact_leg_readiness", None)
        if readiness is not None:
            readiness.reset()
        self._exact_leg_demand = None
        self._exact_leg_pin_origins = {}
        self._exact_leg_hot_victims = {}
        self._exact_leg_last_file_revision = None
        self._clear_exact_leg_lifecycle()

    def exact_leg_pin_demand_id(self) -> str | None:
        demand = getattr(self, "_exact_leg_demand", None)
        return demand.demand_id if demand is not None else None

    def reconcile_exact_leg_demand(
        self,
        *,
        now: datetime | None = None,
    ) -> dict[str, object] | None:
        """Observe one demand revision and atomically grant or release its pair."""

        current = (now or datetime.now(tz=timezone.utc)).astimezone(timezone.utc)
        enabled = bool(getattr(self.stream_settings, "exact_leg_pin_enabled", True))
        active_demand = getattr(self, "_exact_leg_demand", None)
        if not enabled or self.skip_options:
            if getattr(self, "pinned_subs", {}):
                return self._release_exact_leg_pin(
                    now=current,
                    reason="pin_disabled" if not enabled else "options_disabled",
                )
            return None

        market_calendar = getattr(self, "market_calendar", DEFAULT_MARKET_CALENDAR)
        expected_session = market_calendar.research_expiry(current).isoformat()
        revision = self._quote_demand_file_revision()
        if revision == self._exact_leg_last_file_revision:
            if active_demand is not None:
                if current >= active_demand.valid_until:
                    return self._release_exact_leg_pin(now=current, reason="lease_expired")
                if active_demand.session_date != expected_session:
                    return self._release_exact_leg_pin(
                        now=current,
                        reason="session_expiry_rolled",
                    )
                return self._advance_exact_leg_readiness(
                    demand=active_demand,
                    now=current,
                )
            return None
        self._exact_leg_last_file_revision = revision

        demand, issue = load_exact_leg_quote_demand(
            self.exact_leg_demand_path,
            now=current,
        )
        if demand is None:
            if issue in {"tombstone", "expired"}:
                if getattr(self, "pinned_subs", {}):
                    return self._release_exact_leg_pin(now=current, reason=str(issue))
                # Replace a possibly stale active acknowledgement after a
                # collector restart, even though there is nothing local left
                # to cancel.
                self._clear_exact_leg_lifecycle()
                return self._ack_event(
                    now=current,
                    status="idle",
                    reason=str(issue),
                    demand=None,
                )
            if issue not in {"missing_or_invalid", "tombstone", "expired"}:
                return self._ack_event(
                    now=current,
                    status="rejected",
                    reason=str(issue or "invalid_demand"),
                    demand=active_demand,
                )
            return None

        if demand.session_date != expected_session:
            return self._ack_event(
                now=current,
                status="rejected",
                reason="session_expiry_mismatch",
                demand=demand,
            )

        labels = tuple(leg.label for leg in demand.legs)
        current_labels = tuple(getattr(self, "pinned_subs", {}))
        if set(labels) == set(current_labels) and len(current_labels) == 2:
            lifecycle_started = self._begin_exact_leg_lifecycle(
                demand=demand,
                now=current,
            )
            if self._exact_leg_accepted_at is None:
                self._exact_leg_accepted_at = current
            if not self._subscriptions_healthy(self.pinned_subs):
                self.subscription_health_failed = True
                return self._ack_event(
                    now=current,
                    status="blocked",
                    reason="pinned_subscription_unhealthy",
                    demand=demand,
                )
            if lifecycle_started:
                self._set_exact_leg_resolution_sources(
                    {label: "active_subscription" for label in labels}
                )
                self._arm_exact_leg_quote_watchers(
                    self.pinned_subs,
                    cold_labels=frozenset(),
                    now=current,
                )
            # A heartbeat or a new event using the same pair only extends the
            # lease. It must never churn subscriptions or request ids.
            self._exact_leg_demand = demand
            ready_metrics = self._sample_exact_leg_readiness(
                demand=demand,
                now=current,
            )
            became_ready = bool(
                ready_metrics is not None and self._exact_leg_ready_at is None
            )
            if became_ready:
                self._exact_leg_ready_at = current
                self._exact_leg_ready_metrics = ready_metrics
            return self._ack_event(
                now=current,
                status="active",
                reason="exact_legs_ready" if became_ready else "lease_refreshed",
                demand=demand,
                reused_lines=2,
                quote_state="ready" if ready_metrics is not None else "warming",
                **(ready_metrics or {}),
            )

        if current_labels:
            self._release_exact_leg_pin(now=current, reason="superseded", write_ack=False)
            self._begin_exact_leg_lifecycle(demand=demand, now=current)
            if self._subscription_lifecycle_blocked():
                return self._ack_event(
                    now=current,
                    status="blocked",
                    reason="subscription_lifecycle_blocked",
                    demand=demand,
                )
        else:
            self._begin_exact_leg_lifecycle(demand=demand, now=current)
        return self._admit_exact_leg_pin(
            demand,
            now=current,
            expected_revision=revision,
            use_wall_clock=now is None,
        )

    def _admit_exact_leg_pin(
        self,
        demand: ExactLegQuoteDemand,
        *,
        now: datetime,
        expected_revision: tuple[int, int] | None,
        use_wall_clock: bool,
    ) -> dict[str, object]:
        self._begin_exact_leg_lifecycle(demand=demand, now=now)
        if self._subscription_lifecycle_blocked():
            return self._ack_event(
                now=now,
                status="blocked",
                reason="subscription_lifecycle_blocked",
                demand=demand,
            )

        definitions = {
            label: (label, kind, contract)
            for label, kind, contract in option_contracts_from_specs(demand.specs())
        }
        desired_labels = tuple(leg.label for leg in demand.legs)
        if set(definitions) != set(desired_labels):
            return self._ack_event(
                now=now,
                status="rejected",
                reason="contract_definition_mismatch",
                demand=demand,
            )

        origins: dict[str, str] = {}
        existing: Subscriptions = {}
        for label in desired_labels:
            for lane, subscriptions in (
                ("hot", self.hot_subs),
                ("rotation", self.rotation_subs),
            ):
                if label in subscriptions:
                    if not self._subscriptions_healthy({label: subscriptions[label]}):
                        return self._ack_event(
                            now=now,
                            status="blocked",
                            reason=f"existing_{lane}_subscription_unhealthy",
                            demand=demand,
                        )
                    origins[label] = lane
                    existing[label] = subscriptions[label]
                    break
        missing_labels = [label for label in desired_labels if label not in existing]
        accepted_at = self._pin_phase_now(now=now, use_wall_clock=use_wall_clock)
        if self._exact_leg_accepted_at is None:
            self._exact_leg_accepted_at = accepted_at
        resolution_sources = {
            label: "active_subscription" for label in existing
        }
        resolution_started_at = None
        resolution_finished_at = None
        if missing_labels:
            resolution_started_at = self._pin_phase_now(
                now=now,
                use_wall_clock=use_wall_clock,
            )
            resolved = self._resolve_option_definitions(
                [definitions[label] for label in missing_labels]
            )
            resolution_finished_at = self._pin_phase_now(
                now=now,
                use_wall_clock=use_wall_clock,
            )
            resolution_sources.update(
                {
                    label: self._option_resolution_source(label)
                    for label in missing_labels
                }
            )
        else:
            resolved = []
        self._set_exact_leg_resolution_sources(resolution_sources)
        if "ibkr_qualification" in resolution_sources.values():
            if self._exact_leg_qualification_started_at is None:
                self._exact_leg_qualification_started_at = resolution_started_at
            if self._exact_leg_qualification_finished_at is None:
                self._exact_leg_qualification_finished_at = resolution_finished_at
        if len(resolved) != len(missing_labels):
            return self._ack_event(
                now=now,
                status="rejected",
                reason="exact_leg_qualification_incomplete",
                demand=demand,
                reused_lines=len(existing),
            )

        guard_issue = self._pin_commit_guard(
            demand,
            now=now,
            expected_revision=expected_revision,
            use_wall_clock=use_wall_clock,
        )
        if guard_issue is not None:
            return self._ack_event(
                now=now,
                status="expired" if guard_issue == "lease_expired" else "superseded",
                reason=guard_issue,
                demand=demand,
                reused_lines=len(existing),
            )

        rotation_victims: Subscriptions = {}
        hot_victims: Subscriptions = {}
        release_needed = self._pin_release_needed(len(missing_labels))
        if release_needed:
            rotation_victims = self._select_pair_victims(
                self.rotation_subs,
                needed=release_needed,
                excluded=set(desired_labels),
            )
            release_needed = max(release_needed - len(rotation_victims), 0)
        if release_needed:
            hot_victims = self._select_pair_victims(
                self.hot_subs,
                needed=release_needed,
                excluded=set(desired_labels),
            )
            release_needed = max(release_needed - len(hot_victims), 0)
        if release_needed:
            return self._ack_event(
                now=now,
                status="rejected",
                reason="exact_leg_capacity_unavailable",
                demand=demand,
                reused_lines=len(existing),
            )

        released: list[tuple[str, Subscriptions]] = []
        for lane, victims in (("rotation", rotation_victims), ("hot", hot_victims)):
            if not victims:
                continue
            if not self._cancel_batch(victims):
                self._restore_pin_victims(released)
                return self._ack_event(
                    now=now,
                    status="blocked",
                    reason=f"{lane}_preemption_failed",
                    demand=demand,
                    reused_lines=len(existing),
                )
            subscriptions = getattr(self, f"{lane}_subs")
            for label in victims:
                subscriptions.pop(label, None)
            released.append((lane, victims))

        if self._pin_release_needed(len(missing_labels)):
            self._restore_pin_victims(released)
            return self._ack_event(
                now=now,
                status="rejected",
                reason="capacity_reserve_not_restored",
                demand=demand,
                reused_lines=len(existing),
            )

        rejection_sequence = getattr(self, "subscription_rejection_sequence", 0)
        connectivity_sequence = getattr(self, "tws_connectivity_loss_sequence", 0)
        submitted_at = None
        if resolved:
            submitted_at = self._pin_phase_now(
                now=now,
                use_wall_clock=use_wall_clock,
            )
            if self._exact_leg_subscription_requested_at is None:
                self._exact_leg_subscription_requested_at = submitted_at
        additions = (
            qualify_and_subscribe(self.ib, resolved, qualify=False) if resolved else {}
        )
        succeeded = self._subscription_batch_succeeded(
            additions,
            expected_count=len(missing_labels),
            rejection_sequence=rejection_sequence,
            connectivity_sequence=connectivity_sequence,
            confirm_seconds=0.0,
            lane="pinned",
        )
        if not succeeded:
            self._cancel_batch(additions)
            for label in additions:
                getattr(self, "qualified_option_contracts", {}).pop(label, None)
            self._restore_pin_victims(released)
            return self._ack_event(
                now=now,
                status="blocked" if self._subscription_lifecycle_blocked() else "rejected",
                reason="exact_leg_subscription_failed",
                demand=demand,
                submitted_at=submitted_at,
                reused_lines=len(existing),
                preempted_lines=sum(len(rows) for _, rows in released),
            )

        guard_issue = self._pin_commit_guard(
            demand,
            now=now,
            expected_revision=expected_revision,
            use_wall_clock=use_wall_clock,
        )
        reused_healthy = self._subscriptions_healthy(existing)
        if guard_issue is not None or not reused_healthy:
            self._cancel_batch(additions)
            for label in additions:
                getattr(self, "qualified_option_contracts", {}).pop(label, None)
            self._restore_pin_victims(released)
            return self._ack_event(
                now=now,
                status=(
                    "expired"
                    if guard_issue == "lease_expired"
                    else "superseded"
                    if guard_issue is not None
                    else "blocked"
                ),
                reason=guard_issue or "reused_subscription_became_unhealthy",
                demand=demand,
                submitted_at=submitted_at,
                reused_lines=len(existing),
                preempted_lines=sum(len(rows) for _, rows in released),
            )

        for label, lane in origins.items():
            getattr(self, f"{lane}_subs").pop(label, None)
        if existing:
            self._register_subscription_rows(existing, lane="pinned")
        self.pinned_subs = {**existing, **additions}
        self._exact_leg_pin_origins = origins
        self._exact_leg_hot_victims = hot_victims
        self._exact_leg_demand = demand
        self.rotation_retry_at = 0.0
        readiness_now = self._pin_phase_now(now=now, use_wall_clock=use_wall_clock)
        self._arm_exact_leg_quote_watchers(
            self.pinned_subs,
            cold_labels=frozenset(additions),
            now=readiness_now,
        )
        ready_metrics = self._sample_exact_leg_readiness(
            demand=demand,
            now=readiness_now,
        )
        if ready_metrics is not None:
            self._exact_leg_ready_at = readiness_now
            self._exact_leg_ready_metrics = ready_metrics
        return self._ack_event(
            now=readiness_now,
            status="active",
            reason=(
                "exact_legs_ready"
                if ready_metrics is not None
                else "exact_legs_pinned"
            ),
            demand=demand,
            submitted_at=submitted_at,
            reused_lines=sum(1 for lane in origins.values() if lane == "hot"),
            promoted_lines=sum(1 for lane in origins.values() if lane == "rotation"),
            subscribed_lines=len(additions),
            preempted_lines=sum(len(rows) for _, rows in released),
            quote_state="ready" if ready_metrics is not None else "warming",
            **(ready_metrics or {}),
        )

    def _release_exact_leg_pin(
        self,
        *,
        now: datetime,
        reason: str,
        write_ack: bool = True,
    ) -> dict[str, object]:
        demand = getattr(self, "_exact_leg_demand", None)
        pinned = dict(getattr(self, "pinned_subs", {}))
        desired_hot = self._desired_hot_labels()
        transfer_to_hot = {
            label: subscription
            for label, subscription in pinned.items()
            if label in desired_hot
        }
        cancel = {
            label: subscription
            for label, subscription in pinned.items()
            if label not in transfer_to_hot
        }
        for label in transfer_to_hot:
            self._register_subscription_rows({label: transfer_to_hot[label]}, lane="hot")
        self.hot_subs.update(transfer_to_hot)
        release_ok = self._cancel_batch(cancel) if cancel else True
        self._detach_exact_leg_quote_watchers()
        self.pinned_subs = {}

        victims = {
            label: subscription
            for label, subscription in getattr(self, "_exact_leg_hot_victims", {}).items()
            if label in desired_hot and label not in self.hot_subs
        }
        restored = self._restore_subscriptions(victims, lane="hot") if victims else {}
        self.hot_subs.update(restored)
        if len(restored) != len(victims):
            self.subscription_health_failed = True
        self._exact_leg_demand = None
        self._exact_leg_pin_origins = {}
        self._exact_leg_hot_victims = {}
        self.rotation_retry_at = 0.0
        event = self._ack_payload(
            now=now,
            status="released" if release_ok else "blocked",
            reason=reason if release_ok else "pin_release_failed",
            demand=demand,
            released_lines=len(cancel),
            restored_lines=len(restored),
        )
        if write_ack:
            self._write_pin_ack(event)
        self._clear_exact_leg_lifecycle()
        return event

    def _begin_exact_leg_lifecycle(
        self,
        *,
        demand: ExactLegQuoteDemand,
        now: datetime,
    ) -> bool:
        """Keep first-observation telemetry stable for one demand lifecycle."""

        if getattr(self, "_exact_leg_lifecycle_demand_id", None) == demand.demand_id:
            return False
        self._clear_exact_leg_lifecycle()
        self._exact_leg_lifecycle_demand_id = demand.demand_id
        self._exact_leg_first_observed_at = now
        return True

    def _clear_exact_leg_lifecycle(self) -> None:
        self._exact_leg_lifecycle_demand_id: str | None = None
        self._exact_leg_first_observed_at: datetime | None = None
        self._exact_leg_accepted_at: datetime | None = None
        self._exact_leg_qualification_started_at: datetime | None = None
        self._exact_leg_qualification_finished_at: datetime | None = None
        self._exact_leg_subscription_requested_at: datetime | None = None
        self._exact_leg_ready_at: datetime | None = None
        self._exact_leg_ready_metrics: dict[str, object] = {}
        self._exact_leg_resolution_sources: dict[str, str] = {}
        self._exact_leg_resolution_source: str | None = None
        readiness = getattr(self, "_exact_leg_readiness", None)
        if readiness is not None:
            readiness.clear_evidence()

    @staticmethod
    def _pin_phase_now(*, now: datetime, use_wall_clock: bool) -> datetime:
        return datetime.now(tz=timezone.utc) if use_wall_clock else now

    def _option_resolution_source(self, label: str) -> str:
        query = getattr(self, "option_definition_resolution_source", None)
        source = query(label) if callable(query) else None
        if not source:
            source = getattr(self, "option_definition_resolution_sources", {}).get(
                label
            )
        return str(source or "unknown")

    def _set_exact_leg_resolution_sources(
        self,
        sources: dict[str, str],
    ) -> None:
        self._exact_leg_resolution_sources = dict(sorted(sources.items()))
        unique = set(sources.values())
        self._exact_leg_resolution_source = (
            next(iter(unique))
            if len(unique) == 1
            else "mixed"
            if unique
            else None
        )

    def _arm_exact_leg_quote_watchers(
        self,
        subscriptions: Subscriptions,
        *,
        cold_labels: frozenset[str],
        now: datetime,
    ) -> None:
        self._exact_leg_readiness.arm(
            subscriptions,
            cold_labels=cold_labels,
            now=now,
        )

    def _detach_exact_leg_quote_watchers(self) -> None:
        self._exact_leg_readiness.detach()

    def _advance_exact_leg_readiness(
        self,
        *,
        demand: ExactLegQuoteDemand,
        now: datetime,
    ) -> dict[str, object] | None:
        """Publish the one-way warming -> ready transition independently."""

        if self._exact_leg_ready_at is not None:
            return None
        ready_metrics = self._sample_exact_leg_readiness(demand=demand, now=now)
        if ready_metrics is None:
            return None
        self._exact_leg_ready_at = now
        self._exact_leg_ready_metrics = ready_metrics
        return self._ack_event(
            now=now,
            status="active",
            reason="exact_legs_ready",
            demand=demand,
            quote_state="ready",
            **ready_metrics,
        )

    def _sample_exact_leg_readiness(
        self,
        *,
        demand: ExactLegQuoteDemand,
        now: datetime,
    ) -> dict[str, object] | None:
        """Sample only the pinned tickers and apply the action-time 5s contract."""
        return self._exact_leg_readiness.sample(
            demand=demand,
            pinned=getattr(self, "pinned_subs", {}),
            now=now,
            market_calendar=getattr(self, "market_calendar", DEFAULT_MARKET_CALENDAR),
            connection_generation=getattr(self, "connection_generation", 0),
        )

    def _pin_release_needed(self, additions: int) -> int:
        if additions <= 0:
            return 0
        tracker = getattr(self, "capacity_tracker", None)
        capacity = (
            int(tracker.effective_capacity)
            if tracker is not None
            else int(getattr(self.stream_settings, "market_data_line_capacity", 100))
        )
        usable = max(capacity - PIN_CAPACITY_RESERVE_LINES, 0)
        active_lines = active_market_data_lines(self)

        # Slow context subscriptions are intermittent.  Admission must reserve
        # their largest configured chunk even while that lane is idle; using
        # only the instantaneous line count would admit two pinned legs at 88
        # lines and later peak at 96 when the six-line slow chunk starts.
        slow_chunks = getattr(self, "slow_chunks", ())
        slow_peak = max((len(chunk) for chunk in slow_chunks), default=0)
        slow_active = len(getattr(self, "slow_active_subs", {}))
        dormant_slow_lines = max(slow_peak - slow_active, 0)
        overall_release = max(
            active_lines + dormant_slow_lines + additions - usable,
            0,
        )

        # Pinned legs also share the SPXW option-line allocation.  Enforcing
        # this ceiling immediately avoids a transient over-allocation before
        # the next normal rotation slice has a chance to shrink itself.
        option_labels = {
            str(label)
            for attribute in ("hot_subs", "rotation_subs", "pinned_subs")
            for label in getattr(self, attribute, {})
        }
        option_capacity = int(
            getattr(
                self.stream_settings,
                "max_option_lines",
                len(option_labels) + additions,
            )
        )
        option_release = max(len(option_labels) + additions - option_capacity, 0)
        return max(overall_release, option_release)

    @staticmethod
    def _subscriptions_healthy(subscriptions: Subscriptions) -> bool:
        return all(row.subscribed and not row.error for _, row in subscriptions.values())

    def _pin_commit_guard(
        self,
        demand: ExactLegQuoteDemand,
        *,
        now: datetime,
        expected_revision: tuple[int, int] | None,
        use_wall_clock: bool,
    ) -> str | None:
        current = datetime.now(tz=timezone.utc) if use_wall_clock else now
        if current >= demand.valid_until:
            return "lease_expired"
        if self._quote_demand_file_revision() != expected_revision:
            return "demand_superseded"
        market_calendar = getattr(self, "market_calendar", DEFAULT_MARKET_CALENDAR)
        if demand.session_date != market_calendar.research_expiry(current).isoformat():
            return "session_expiry_mismatch"
        return None

    def _select_pair_victims(
        self,
        subscriptions: Subscriptions,
        *,
        needed: int,
        excluded: set[str],
    ) -> Subscriptions:
        groups: dict[str, Subscriptions] = {}
        for label, subscription in subscriptions.items():
            if label in excluded:
                continue
            parts = label.split(":")
            if len(parts) != 5 or parts[0:2] != ["option", "SPXW"]:
                continue
            groups.setdefault(":".join(parts[:4]), {})[label] = subscription
        atm = int(getattr(getattr(self, "option_plan", None), "atm_strike", 0) or 0)
        ordered = sorted(
            (
                group
                for group in groups.values()
                if {label.rsplit(":", 1)[-1] for label in group} >= {"C", "P"}
            ),
            key=lambda group: (
                -max(option_label_distance(label, atm) for label in group),
                sorted(group)[0],
            ),
        )
        selected: Subscriptions = {}
        for group in ordered:
            selected.update(group)
            if len(selected) >= needed:
                break
        return selected

    def _restore_pin_victims(
        self,
        released: list[tuple[str, Subscriptions]],
    ) -> None:
        for lane, victims in released:
            restored = self._restore_subscriptions(victims, lane=lane)
            getattr(self, f"{lane}_subs").update(restored)
            if len(restored) != len(victims):
                self.subscription_health_failed = True

    def _desired_hot_labels(self) -> set[str]:
        plan = getattr(self, "option_plan", None)
        if plan is None:
            return set()
        return {
            label
            for label, _kind, _contract in option_contracts_from_specs(plan.hot)
        }

    def _quote_demand_file_revision(self) -> tuple[int, int] | None:
        try:
            stat = self.exact_leg_demand_path.stat()
        except OSError:
            return None
        return stat.st_mtime_ns, stat.st_size

    def _ack_event(
        self,
        *,
        now: datetime,
        status: str,
        reason: str,
        demand: ExactLegQuoteDemand | None,
        submitted_at: datetime | None = None,
        **metrics: object,
    ) -> dict[str, object]:
        payload = self._ack_payload(
            now=now,
            status=status,
            reason=reason,
            demand=demand,
            submitted_at=submitted_at,
            **metrics,
        )
        self._write_pin_ack(payload)
        return payload

    def _ack_payload(
        self,
        *,
        now: datetime,
        status: str,
        reason: str,
        demand: ExactLegQuoteDemand | None,
        submitted_at: datetime | None = None,
        **metrics: object,
    ) -> dict[str, object]:
        tracker = getattr(self, "capacity_tracker", None)
        lifecycle_matches = bool(
            demand is not None
            and getattr(self, "_exact_leg_lifecycle_demand_id", None)
            == demand.demand_id
        )

        def lifecycle_time(attribute: str) -> str | None:
            value = getattr(self, attribute, None) if lifecycle_matches else None
            return value.isoformat() if isinstance(value, datetime) else None

        lifecycle_submitted_at = (
            getattr(self, "_exact_leg_subscription_requested_at", None)
            if lifecycle_matches
            else None
        )
        effective_submitted_at = submitted_at or lifecycle_submitted_at
        return {
            "task": "ibkr_stream",
            "event": "exact_leg_quote_demand",
            "status": status,
            "reason": reason,
            "demand_id": demand.demand_id if demand is not None else None,
            "event_id": demand.event_id if demand is not None else None,
            "demand_policy_version": (
                demand.policy_version if demand is not None else None
            ),
            "source_policy_version": (
                demand.source_policy_version if demand is not None else None
            ),
            "source_provider": demand.source_provider if demand is not None else None,
            "quote_provider": demand.quote_provider if demand is not None else "ibkr",
            "session_date": demand.session_date if demand is not None else None,
            "automatic_ordering": (
                demand.automatic_ordering if demand is not None else False
            ),
            "demand_updated_at": demand.updated_at.isoformat() if demand is not None else None,
            "valid_until": demand.valid_until.isoformat() if demand is not None else None,
            "observed_at": now.isoformat(),
            "first_observed_at": lifecycle_time("_exact_leg_first_observed_at"),
            "accepted_at": lifecycle_time("_exact_leg_accepted_at"),
            "qualification_started_at": lifecycle_time(
                "_exact_leg_qualification_started_at"
            ),
            "qualification_finished_at": lifecycle_time(
                "_exact_leg_qualification_finished_at"
            ),
            "resolution_source": (
                getattr(self, "_exact_leg_resolution_source", None)
                if lifecycle_matches
                else None
            ),
            "resolution_sources": (
                dict(getattr(self, "_exact_leg_resolution_sources", {}))
                if lifecycle_matches
                else {}
            ),
            "subscription_requested_at": lifecycle_time(
                "_exact_leg_subscription_requested_at"
            ),
            "ready_at": lifecycle_time("_exact_leg_ready_at"),
            "submitted_at": (
                effective_submitted_at.isoformat()
                if isinstance(effective_submitted_at, datetime)
                else None
            ),
            "connection_generation": getattr(self, "connection_generation", 0),
            "pinned_labels": sorted(getattr(self, "pinned_subs", {})),
            "active_lines": active_market_data_lines(self),
            "effective_capacity": (
                tracker.effective_capacity if tracker is not None else None
            ),
            **metrics,
        }

    def _write_pin_ack(self, payload: dict[str, object]) -> None:
        try:
            write_quote_demand_ack(self.exact_leg_demand_ack_path, payload)
        except (OSError, ValueError) as exc:
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "exact_leg_quote_demand_ack_failed",
                    "error_type": type(exc).__name__,
                }
            )
