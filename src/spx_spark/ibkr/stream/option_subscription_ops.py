"""Option hot-lane subscription plan and reconcile operations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.capacity_tracker import active_market_data_lines
from spx_spark.ibkr.stream.contracts import (
    chunked,
    contract_qualification_key,
    option_label_distance,
)
from spx_spark.ibkr.stream.models import (
    OptionSubscriptionPlan,
    SUBSCRIPTION_CONFIRM_SECONDS,
)
from spx_spark.ibkr.stream.quota_plan import plan_ibkr_option_allocation
from spx_spark.ibkr.slow_poll import SlowPollScheduler
from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.market_calendar import ET
from spx_spark.provider_failover import FailoverMode
from spx_spark.provider_failover_controller import load_failover_control

build_base_contracts = stream_deps.build_base_contracts
build_option_subscription_plan = stream_deps.build_option_subscription_plan
cancel_subscriptions = stream_deps.cancel_subscriptions
contract_has_con_id = stream_deps.contract_has_con_id
discard_subscriptions = stream_deps.discard_subscriptions
log_event = stream_deps.log_event
option_contracts_from_specs = stream_deps.option_contracts_from_specs
qualify_and_subscribe = stream_deps.qualify_and_subscribe
reference_quote_from_row = stream_deps.reference_quote_from_row
should_replan = stream_deps.should_replan
snapshot_rows = stream_deps.snapshot_rows
split_base_contracts = stream_deps.split_base_contracts
time = stream_deps.time


class OptionSubscriptionOps:
    def subscribe_base(self) -> None:
        setup_connectivity_sequence = getattr(self, "tws_connectivity_loss_sequence", 0)
        contracts = build_base_contracts(self.ibkr_settings)
        persistent, slow = split_base_contracts(
            contracts,
            self.stream_settings.slow_poll_labels,
        )
        self.slow_contracts = slow
        log_event(
            {
                "task": "ibkr_stream",
                "event": "subscribe_base_start",
                "contracts": len(contracts),
            }
        )

        def on_progress(**payload: object) -> None:
            log_event({"task": "ibkr_stream", "event": "subscribe_progress", **payload})

        rejection_sequence = self.subscription_rejection_sequence
        self.base_subs = qualify_and_subscribe(
            self.ib,
            persistent,
            qualify=self.ibkr_settings.qualify_contracts,
            on_progress=on_progress,
        )
        self._raise_if_subscription_setup_interrupted(
            setup_connectivity_sequence,
            phase="base_subscribe",
        )
        self._register_subscription_rows(
            {
                label: subscription
                for label, subscription in self.base_subs.items()
                if subscription[1].subscribed and not subscription[1].error
            },
            lane="base",
        )
        if self._apply_subscription_rejections(
            self.base_subs,
            rejection_sequence=rejection_sequence,
        ):
            self.subscription_health_failed = True
        subscribed = sum(1 for _, row in self.base_subs.values() if row.subscribed)
        failed = sum(1 for _, row in self.base_subs.values() if row.error)
        log_event(
            {
                "task": "ibkr_stream",
                "event": "subscribe_base_done",
                "subscribed": subscribed,
                "failed": failed,
                "total": len(contracts),
            }
        )
        if subscribed == 0:
            raise RuntimeError(f"no base contracts subscribed ({failed} failed)")
        self.slow_chunks = chunked(
            self.slow_contracts,
            self.stream_settings.slow_poll_chunk_size,
        )
        self.slow_scheduler = SlowPollScheduler(
            chunk_count=len(self.slow_chunks),
            cycle_seconds=self.stream_settings.slow_poll_interval_seconds,
            hold_seconds=self.stream_settings.slow_poll_hold_seconds,
        )
        self.slow_scheduler.reset(now=time.monotonic())

    def prime_priority_market_data(self) -> None:
        """Recover ES and the SPXW hot lane before any slow context work."""

        started_at = time.monotonic()
        deadline = started_at + max(float(self.ibkr_settings.quote_wait_seconds), 0.0)
        rows: list[VerifyRow] = []
        while True:
            self._raise_if_subscription_setup_interrupted(
                getattr(self, "tws_connectivity_loss_sequence", 0),
                phase="priority_anchor_wait",
            )
            rows = snapshot_rows(
                self.base_subs,
                self.ibkr_settings.stale_after_seconds,
                slow_index_stale_after_seconds=self.ibkr_settings.slow_index_stale_after_seconds,
                slow_index_labels=self.ibkr_settings.slow_index_labels,
            )
            by_label = {row.label: row for row in rows}
            reference = reference_quote_from_row(
                by_label.get("future:ES"),
                as_of=datetime.now(tz=timezone.utc),
            )
            if (
                reference is not None
                and reference.value is not None
                and reference.freshness == "fresh"
            ):
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self.ib.sleep(min(remaining, 0.25))

        self.ensure_option_plan(rows)
        self._raise_if_subscription_setup_interrupted(
            getattr(self, "tws_connectivity_loss_sequence", 0),
            phase="priority_option_subscribe",
        )
        log_event(
            {
                "task": "ibkr_stream",
                "event": "priority_market_data_primed",
                "elapsed_seconds": round(max(time.monotonic() - started_at, 0.0), 3),
                "es_ready": any(row.label == "future:ES" and row.stale is False for row in rows),
                "hot_contracts": len(self.hot_subs),
                "expiry": self.option_plan.expiry if self.option_plan is not None else None,
            }
        )

    def _raise_if_subscription_setup_interrupted(
        self,
        connectivity_sequence: int,
        *,
        phase: str,
    ) -> None:
        connectivity_changed = self._connectivity_changed_since(connectivity_sequence)
        subscription_failed = getattr(self, "subscription_health_failed", False)
        if not connectivity_changed and not subscription_failed:
            return
        if connectivity_changed:
            self.subscriptions_lost = True
            self.subscription_health_failed = True
        log_event(
            {
                "task": "ibkr_stream",
                "event": "subscription_setup_interrupted",
                "phase": phase,
                "cause": (
                    "connectivity_changed" if connectivity_changed else "subscription_rejected"
                ),
            }
        )
        raise RuntimeError(f"IBKR subscription setup interrupted during {phase}")

    def ensure_option_plan(self, rows: list[VerifyRow]) -> None:
        if self.skip_options:
            return
        decision_at = datetime.now(tz=timezone.utc)
        current_expiry, next_expiry = self.market_calendar.option_collection_expiries(decision_at)
        self._prepare_option_definition_cache(now=decision_at)
        next_session_prefetch = self.market_calendar.is_next_expiry_prefetch_window(decision_at)
        today = current_expiry.strftime("%Y%m%d")
        next_expiry_text = next_expiry.strftime("%Y%m%d")
        by_label = {row.label: row for row in rows}
        es_ticker = self.base_subs.get("future:ES", (None, None))[0]
        es_contract = getattr(
            getattr(es_ticker, "contract", None),
            "lastTradeDateOrContractMonth",
            None,
        )
        basis_state = self.atm_reference_controller.basis_tracker.state
        trading_date = decision_at.astimezone(ET).date()
        basis_age = (
            self.market_calendar.trading_days_elapsed(
                basis_state.trading_date,
                trading_date,
            )
            if basis_state is not None
            else None
        )
        stable = self.atm_reference_controller.stable_atm
        expiry_rollover = bool(
            stable is not None and stable.expiry is not None and stable.expiry != today
        ) or bool(
            self.option_replan_controller.accepted_expiry is not None
            and self.option_replan_controller.accepted_expiry != today
        )
        atm_result = self.atm_reference_controller.resolve(
            strike_step=max(int(self.sampling_settings.strike_step), 1),
            is_rth=self.market_calendar.is_rth_open(decision_at),
            trading_date=trading_date,
            trading_days_since_basis=basis_age,
            spx=reference_quote_from_row(by_label.get("index:SPX"), as_of=decision_at),
            ibus500=reference_quote_from_row(by_label.get("cfd:IBUS500"), as_of=decision_at),
            es=reference_quote_from_row(
                by_label.get("future:ES"),
                contract=str(es_contract) if es_contract else None,
                as_of=decision_at,
            ),
            spy=reference_quote_from_row(by_label.get("stock:SPY"), as_of=decision_at),
            expiry_rollover=expiry_rollover,
        )
        candidate = atm_result.candidate
        decision = self.option_replan_controller.observe(
            atm_strike=candidate.rounded_strike if candidate is not None else None,
            source=candidate.source if candidate is not None else None,
            observed_at=candidate.observed_at if candidate is not None else decision_at,
            expiry=today,
            decision_at=decision_at,
        )
        basis = atm_result.basis
        log_event(
            {
                "task": "ibkr_stream",
                "event": "option_replan_decision",
                "raw_atm": candidate.value if candidate is not None else None,
                "raw_strike": candidate.rounded_strike if candidate is not None else None,
                "raw_source": candidate.source if candidate is not None else None,
                "raw_observed_at": (
                    candidate.observed_at.isoformat() if candidate is not None else None
                ),
                "raw_freshness": candidate.freshness if candidate is not None else None,
                "accepted_atm": self.option_replan_controller.accepted_atm,
                "accepted_source": self.option_replan_controller.accepted_source,
                "accepted_expiry": self.option_replan_controller.accepted_expiry,
                "collection_expiry": today,
                "next_session_prefetch": next_session_prefetch,
                "state": decision.state,
                "reason": decision.reason,
                "confirmations": decision.confirmation_count,
                "basis_value": basis.median if basis is not None else None,
                "basis_as_of": (
                    basis.observed_at.isoformat()
                    if basis is not None and basis.observed_at is not None
                    else None
                ),
                "basis_contract": basis.es_contract if basis is not None else None,
            }
        )
        proposal = decision.proposal
        if proposal is None:
            return

        control = load_failover_control(self.provider_failover_settings.state_path)
        fallback = (
            self.market_calendar.is_spx_gth_open(decision_at)
            or next_session_prefetch
            or (
                isinstance(control, dict)
                and control.get("mode")
                in {
                    FailoverMode.RECOVERY_PENDING.value,
                    FailoverMode.IBKR_FALLBACK.value,
                    FailoverMode.BOTH_UNAVAILABLE.value,
                }
            )
        )
        capacity_tracker = getattr(self, "capacity_tracker", None)
        discovered_capacity = (
            capacity_tracker.effective_capacity
            if capacity_tracker is not None
            else int(getattr(self.stream_settings, "market_data_line_capacity", 100))
        )
        allocation = plan_ibkr_option_allocation(
            discovered_capacity=discovered_capacity,
            fallback=fallback,
            base_lines=max(len(self.base_subs), 4),
            temporary_lines=max(int(self.stream_settings.slow_poll_chunk_size), 0),
        )
        configured_option_ceiling = max(int(self.stream_settings.max_option_lines), 0)
        option_lines = min(allocation.option_lines, configured_option_ceiling)
        hot_lines = min(allocation.hot_option_lines, option_lines)
        hot_share = hot_lines / option_lines if option_lines else 0.0
        plan = build_option_subscription_plan(
            atm_reference=float(proposal.atm_strike),
            expiry=proposal.expiry,
            next_expiry=next_expiry_text,
            mode=self.sampling_settings.default_mode,
            sampling_settings=self.sampling_settings,
            max_option_lines=option_lines,
            hot_lane_share=hot_share,
        )
        success = self.reconcile_option_plan(plan)
        completed_at = datetime.now(tz=timezone.utc)
        self.option_replan_controller.record_result(
            proposal,
            success=success,
            applied_at=completed_at,
        )
        if not success:
            return
        if candidate is not None:
            self.atm_reference_controller.record_accepted(
                candidate,
                expiry=proposal.expiry,
            )
        log_event(
            {
                "task": "ibkr_stream",
                "event": "option_replan",
                "atm_strike": plan.atm_strike,
                "expiry": plan.expiry,
                "next_session_prefetch": next_session_prefetch,
                "hot_contracts": len(plan.hot),
                "rotation_slices": plan.rotation_count,
                "quota_mode": allocation.mode.value,
                "discovered_capacity": allocation.discovered_capacity,
                "option_line_budget": option_lines,
                "line_reserve": allocation.reserve_lines,
                "reason": proposal.reason,
                "source": proposal.source,
                "confirmations": proposal.confirmation_count,
            }
        )

    def reconcile_option_plan(self, plan: OptionSubscriptionPlan) -> bool:
        cache = getattr(self, "qualified_option_contracts", None)
        if cache is not None:
            expiry_token = f":{plan.expiry}:"
            self.qualified_option_contracts = {
                label: definition for label, definition in cache.items() if expiry_token in label
            }
        desired_contracts = option_contracts_from_specs(plan.hot)
        desired_by_label = {
            label: (label, kind, contract) for label, kind, contract in desired_contracts
        }
        pinned_labels = set(getattr(self, "pinned_subs", {}))
        retained_labels = set(self.hot_subs) & set(desired_by_label)
        added_labels = set(desired_by_label) - retained_labels - pinned_labels
        obsolete_labels = set(self.hot_subs) - retained_labels

        if not self._cancel_batch(self.rotation_subs):
            return False
        self.rotation_subs = {}
        max_lines = getattr(
            getattr(self, "stream_settings", None),
            "max_option_lines",
            len(self.hot_subs) + len(added_labels),
        )
        # Pinned exact legs share the SPXW option-line ceiling.  A plan refresh
        # must not silently add normal hot coverage back above that ceiling.
        hot_line_budget = max(int(max_lines) - len(pinned_labels), 0)
        free_lines = max(hot_line_budget - len(self.hot_subs), 0)
        release_count = max(len(added_labels) - free_lines, 0)
        release_labels = set(
            sorted(
                obsolete_labels,
                key=lambda label: (-option_label_distance(label, plan.atm_strike), label),
            )[:release_count]
        )
        released_subs = {label: self.hot_subs[label] for label in release_labels}
        if released_subs and not self._cancel_batch(released_subs):
            return False
        remaining_hot = {
            label: subscription
            for label, subscription in self.hot_subs.items()
            if label not in release_labels
        }
        rejection_sequence = getattr(self, "subscription_rejection_sequence", 0)
        connectivity_sequence = getattr(self, "tws_connectivity_loss_sequence", 0)
        addition_definitions = self._resolve_option_definitions(
            [desired_by_label[label] for label in sorted(added_labels)]
        )
        additions = qualify_and_subscribe(
            self.ib,
            addition_definitions,
            qualify=False,
        )
        additions_ok = self._subscription_batch_succeeded(
            additions,
            expected_count=len(added_labels),
            rejection_sequence=rejection_sequence,
            connectivity_sequence=connectivity_sequence,
            lane="hot",
        )
        if not additions_ok:
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="hot")
            self.hot_subs = {**remaining_hot, **restored}
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "option_replan_failed",
                    "retained": len(retained_labels),
                    "added": len(added_labels),
                    "removed": len(released_subs) - len(restored),
                    "restored": len(restored),
                }
            )
            return False

        obsolete_subs = {label: remaining_hot[label] for label in obsolete_labels - release_labels}
        if not self._cancel_batch(obsolete_subs):
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="hot")
            self.hot_subs = {**remaining_hot, **restored}
            return False
        self.hot_subs = {
            **{label: remaining_hot[label] for label in retained_labels},
            **additions,
        }
        self.option_plan = plan
        self.rotation_index = 0
        return True

    def _resolve_option_definitions(
        self,
        definitions: list[tuple[str, str, Any]],
    ) -> list[tuple[str, str, Any]]:
        """Batch-qualify unseen options and reuse resolved contracts by label."""

        self._prepare_option_definition_cache()
        cache = getattr(self, "qualified_option_contracts", None)
        if cache is None:
            # Lightweight unit-test collectors built with object.__new__ do
            # not own a session cache; their mocked transport resolves rows.
            return definitions
        resolved: dict[str, tuple[str, str, Any]] = {}
        pending: list[tuple[str, str, Any]] = []
        sources = getattr(self, "option_definition_resolution_sources", None)
        if sources is None:
            sources = {}
            self.option_definition_resolution_sources = sources
        for label, kind, contract in definitions:
            sources.pop(label, None)
            definition_expiry = self._spxw_definition_expiry(label, kind=kind)
            cached = cache.get(label)
            if cached is not None:
                resolved[label] = cached
                sources[label] = "memory_cache"
            elif self._apply_persisted_option_con_id(
                label,
                contract,
                expiry=definition_expiry,
            ):
                resolved[label] = (label, kind, contract)
                cache[label] = resolved[label]
                sources[label] = "durable_cache"
            elif contract_has_con_id(contract):
                resolved[label] = (label, kind, contract)
                cache[label] = resolved[label]
                sources[label] = "provided_con_id"
            else:
                pending.append((label, kind, contract))

        qualify = getattr(self.ib, "qualifyContracts", None)
        if pending and callable(qualify):
            try:
                qualified = self._batch_qualify([contract for _, _, contract in pending])
            except Exception as exc:  # noqa: BLE001
                qualified = []
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "option_batch_qualification_failed",
                        "contracts": len(pending),
                        "error": str(exc),
                    }
                )
            qualified_by_key: dict[tuple[object, ...], list[Any]] = {}
            for contract in qualified:
                qualified_by_key.setdefault(contract_qualification_key(contract), []).append(
                    contract
                )
            for label, kind, contract in pending:
                matches = qualified_by_key.get(contract_qualification_key(contract), [])
                if not matches:
                    continue
                definition = (label, kind, matches.pop(0))
                resolved[label] = definition
                cache[label] = definition
                sources[label] = "ibkr_qualification"
        elif pending:
            # Test doubles without an IB qualification surface still exercise
            # lifecycle logic through the mocked qualify_and_subscribe call.
            for item in pending:
                resolved[item[0]] = item
                sources[item[0]] = "unqualified_passthrough"

        ordered = [resolved[label] for label, _, _ in definitions if label in resolved]
        self._remember_option_definitions(ordered)
        return ordered

    def _prepare_option_definition_cache(
        self,
        *,
        now: datetime | None = None,
    ) -> None:
        persistent = getattr(self, "option_conid_cache", None)
        calendar = getattr(self, "market_calendar", None)
        if persistent is None or calendar is None:
            return
        at = now or datetime.now(tz=timezone.utc)
        collection_expiries_at = getattr(calendar, "option_collection_expiries", None)
        if callable(collection_expiries_at):
            active_expiries = frozenset(
                expiry.strftime("%Y%m%d") for expiry in collection_expiries_at(at)
            )
        else:
            collection_expiry_at = getattr(calendar, "option_collection_expiry", None)
            collection_expiry = (
                collection_expiry_at(at)
                if callable(collection_expiry_at)
                else calendar.research_expiry(at)
            )
            active_expiries = frozenset({collection_expiry.strftime("%Y%m%d")})
        previous_expiries = getattr(self, "_option_conid_cache_expiries", frozenset())
        persistent.prepare(active_expiries)
        prepared_expiries = getattr(persistent, "active_expiries", frozenset())
        if prepared_expiries != active_expiries:
            self._option_conid_cache_expiries = frozenset()
            return
        self._option_conid_cache_expiries = prepared_expiries
        if not previous_expiries or previous_expiries == active_expiries:
            return
        cache = getattr(self, "qualified_option_contracts", None)
        if cache is not None:
            self.qualified_option_contracts = {
                label: definition
                for label, definition in cache.items()
                if (
                    (label_expiry := self._spxw_definition_expiry(label, kind="option")) is None
                    or label_expiry in active_expiries
                )
            }

    @staticmethod
    def _spxw_definition_expiry(
        label: str,
        *,
        kind: str,
    ) -> str | None:
        parts = label.split(":")
        if (
            kind != "option"
            or len(parts) != 5
            or parts[:2] != ["option", "SPXW"]
            or len(parts[2]) != 8
            or not parts[2].isdigit()
        ):
            return None
        return parts[2]

    def _apply_persisted_option_con_id(
        self,
        label: str,
        contract: Any,
        *,
        expiry: str | None,
    ) -> bool:
        persistent = getattr(self, "option_conid_cache", None)
        if (
            persistent is None
            or expiry is None
            or expiry
            not in getattr(self, "_option_conid_cache_expiries", frozenset())
        ):
            return False
        con_id = persistent.cached_con_id(label, contract, expiry=expiry)
        if con_id is None:
            return False
        contract.conId = con_id
        return True

    def _remember_option_definitions(
        self,
        definitions: list[tuple[str, str, Any]],
    ) -> None:
        persistent = getattr(self, "option_conid_cache", None)
        if persistent is None:
            return
        by_expiry: dict[str, list[tuple[str, str, Any]]] = {}
        for definition in definitions:
            label, kind, _ = definition
            expiry = self._spxw_definition_expiry(label, kind=kind)
            if expiry is not None:
                by_expiry.setdefault(expiry, []).append(definition)
        active_expiries = getattr(self, "_option_conid_cache_expiries", frozenset())
        for expiry, group in by_expiry.items():
            if expiry in active_expiries:
                persistent.remember(group, expiry=expiry)

    def option_definition_resolution_source(self, label: str) -> str | None:
        return getattr(self, "option_definition_resolution_sources", {}).get(label)

    def _evict_option_definition(self, label: str) -> None:
        cache = getattr(self, "qualified_option_contracts", None)
        if cache is not None:
            cache.pop(label, None)
        persistent = getattr(self, "option_conid_cache", None)
        expiry = self._spxw_definition_expiry(label, kind="option")
        if persistent is not None and not getattr(
            self,
            "_option_conid_cache_expiries",
            frozenset(),
        ):
            self._prepare_option_definition_cache()
        if (
            persistent is not None
            and expiry is not None
            and expiry
            in getattr(self, "_option_conid_cache_expiries", frozenset())
        ):
            persistent.evict(label, expiry=expiry)

    def _subscription_batch_succeeded(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
        *,
        expected_count: int,
        rejection_sequence: int,
        connectivity_sequence: int | None = None,
        confirm_seconds: float = SUBSCRIPTION_CONFIRM_SECONDS,
        lane: str = "hot",
    ) -> bool:
        if len(subscriptions) != expected_count or any(
            not row.subscribed or row.error for _, row in subscriptions.values()
        ):
            return False
        if subscriptions and confirm_seconds > 0:
            sleep = getattr(self.ib, "sleep", None)
            if callable(sleep):
                sleep(confirm_seconds)
        connectivity_changed = (
            connectivity_sequence is not None
            and self._connectivity_changed_since(connectivity_sequence)
        )
        if connectivity_changed:
            self.subscriptions_lost = True
            self.subscription_health_failed = True
        if connectivity_changed or self._subscription_lifecycle_blocked():
            return False
        if self._apply_subscription_rejections(
            subscriptions,
            rejection_sequence=rejection_sequence,
        ):
            return False
        self._register_subscription_rows(subscriptions, lane=lane)
        capacity_tracker = getattr(self, "capacity_tracker", None)
        if capacity_tracker is not None:
            capacity_tracker.observe_success(active_lines=active_market_data_lines(self))
        return True

    def _apply_subscription_rejections(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
        *,
        rejection_sequence: int,
    ) -> bool:
        rows_by_request_id = {
            row.request_id: row for _, row in subscriptions.values() if row.request_id is not None
        }
        rejected = False
        for sequence, error in getattr(self, "subscription_rejection_log", []):
            if sequence <= rejection_sequence:
                continue
            row = rows_by_request_id.get(error.req_id)
            if row is None and error.req_id >= 0:
                continue
            rejected = True
            if row is not None:
                row.error = f"IBKR {error.error_code}: {error.message}"
                row.subscribed = False
                if error.error_code == 200:
                    self._evict_option_definition(row.label)
        return rejected

    def _register_subscription_rows(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
        *,
        lane: str,
    ) -> None:
        tracked = getattr(self, "subscription_rows_by_req_id", None)
        lanes = getattr(self, "subscription_lane_by_req_id", None)
        contract_cache = getattr(self, "qualified_option_contracts", None)
        if tracked is None:
            return
        for label, (ticker, row) in subscriptions.items():
            if row.request_id is not None:
                tracked[row.request_id] = row
                if lanes is not None:
                    lanes[row.request_id] = lane
            if contract_cache is not None and row.kind == "option":
                contract = getattr(ticker, "contract", None)
                if contract is not None:
                    contract_cache[label] = (label, row.kind, contract)

    def _cancel_batch(self, subscriptions: dict[str, tuple[Any, VerifyRow]]) -> bool:
        local_only = self._subscription_lifecycle_blocked()
        if local_only:
            result = discard_subscriptions(self.ib, subscriptions)
            self._unregister_subscription_rows(subscriptions)
            self.subscriptions_lost = True
            self.subscription_health_failed = True
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "subscription_lifecycle_interrupted",
                    "contracts": len(subscriptions),
                    "local_cleanup_ok": result,
                }
            )
            return False
        connectivity_sequence = getattr(self, "tws_connectivity_loss_sequence", 0)
        result = cancel_subscriptions(self.ib, subscriptions)
        if self._connectivity_changed_since(connectivity_sequence):
            self._unregister_subscription_rows(subscriptions)
            self.subscriptions_lost = True
            self.subscription_health_failed = True
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "subscription_cancel_interrupted",
                    "contracts": len(subscriptions),
                }
            )
            return False
        if result is False:
            self.subscription_health_failed = True
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "subscription_cancel_failed",
                    "contracts": len(subscriptions),
                }
            )
            return False
        self._unregister_subscription_rows(subscriptions)
        return True

    def _unregister_subscription_rows(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
    ) -> None:
        tracked = getattr(self, "subscription_rows_by_req_id", None)
        lanes = getattr(self, "subscription_lane_by_req_id", None)
        if tracked is not None:
            for _, row in subscriptions.values():
                if row.request_id is not None:
                    tracked.pop(row.request_id, None)
                    if lanes is not None:
                        lanes.pop(row.request_id, None)

    def _subscription_lifecycle_blocked(self) -> bool:
        ib = getattr(self, "ib", None)
        is_connected = getattr(ib, "isConnected", None)
        disconnected = callable(is_connected) and not is_connected()
        return bool(
            getattr(self, "tws_connectivity_lost", False)
            or getattr(self, "subscriptions_lost", False)
            or getattr(self, "subscription_health_failed", False)
            or disconnected
        )

    def _connectivity_changed_since(self, sequence: int) -> bool:
        ib = getattr(self, "ib", None)
        is_connected = getattr(ib, "isConnected", None)
        disconnected = callable(is_connected) and not is_connected()
        return bool(
            getattr(self, "tws_connectivity_loss_sequence", 0) != sequence
            or getattr(self, "tws_connectivity_lost", False)
            or getattr(self, "subscriptions_lost", False)
            or disconnected
        )

    def _restore_subscriptions(
        self,
        released: dict[str, tuple[Any, VerifyRow]],
        *,
        lane: str,
    ) -> dict[str, tuple[Any, VerifyRow]]:
        if self._subscription_lifecycle_blocked():
            return {}
        definitions: list[tuple[str, str, Any]] = []
        for label, (ticker, row) in released.items():
            contract = getattr(ticker, "contract", None)
            if contract is not None:
                definitions.append((label, row.kind, contract))
        if not definitions:
            return {}
        rejection_sequence = getattr(self, "subscription_rejection_sequence", 0)
        connectivity_sequence = getattr(self, "tws_connectivity_loss_sequence", 0)
        restored = qualify_and_subscribe(self.ib, definitions, qualify=False)
        if not self._subscription_batch_succeeded(
            restored,
            expected_count=len(definitions),
            rejection_sequence=rejection_sequence,
            connectivity_sequence=connectivity_sequence,
            lane=lane,
        ):
            self._cancel_batch(restored)
            self.subscription_health_failed = True
            return {}
        return {
            label: subscription
            for label, subscription in restored.items()
            if subscription[1].subscribed and not subscription[1].error
        }
