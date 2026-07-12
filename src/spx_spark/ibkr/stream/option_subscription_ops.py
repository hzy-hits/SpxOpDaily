"""Option hot-lane subscription plan and reconcile operations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.contracts import (
    chunked,
    contract_qualification_key,
    option_label_distance,
)
from spx_spark.ibkr.stream.models import (
    OptionSubscriptionPlan,
    SUBSCRIPTION_CONFIRM_SECONDS,
)
from spx_spark.ibkr.slow_poll import SlowPollScheduler
from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.market_calendar import ET

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
        self._qualify_slow_contracts()
        self._raise_if_subscription_setup_interrupted(
            setup_connectivity_sequence,
            phase="slow_qualification",
        )
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
        self.ib.sleep(self.ibkr_settings.quote_wait_seconds)
        self._raise_if_subscription_setup_interrupted(
            setup_connectivity_sequence,
            phase="base_quote_wait",
        )

    def _raise_if_subscription_setup_interrupted(
        self,
        connectivity_sequence: int,
        *,
        phase: str,
    ) -> None:
        if not self._connectivity_changed_since(connectivity_sequence):
            return
        self.subscriptions_lost = True
        self.subscription_health_failed = True
        log_event(
            {
                "task": "ibkr_stream",
                "event": "subscription_setup_interrupted",
                "phase": phase,
            }
        )
        raise RuntimeError(f"TWS connectivity changed during {phase}")

    def ensure_option_plan(self, rows: list[VerifyRow]) -> None:
        if self.skip_options:
            return
        decision_at = datetime.now(tz=timezone.utc)
        current_expiry, next_expiry = self.market_calendar.research_expiries(decision_at)
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
            stable is not None
            and stable.expiry is not None
            and stable.expiry != today
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
            ibus500=reference_quote_from_row(
                by_label.get("cfd:IBUS500"), as_of=decision_at
            ),
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

        plan = build_option_subscription_plan(
            atm_reference=float(proposal.atm_strike),
            expiry=proposal.expiry,
            next_expiry=next_expiry_text,
            mode=self.sampling_settings.default_mode,
            sampling_settings=self.sampling_settings,
            max_option_lines=self.stream_settings.max_option_lines,
            hot_lane_share=self.stream_settings.hot_lane_share,
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
                "hot_contracts": len(plan.hot),
                "rotation_slices": plan.rotation_count,
                "reason": proposal.reason,
                "source": proposal.source,
                "confirmations": proposal.confirmation_count,
            }
        )

    def reconcile_option_plan(self, plan: OptionSubscriptionPlan) -> bool:
        desired_contracts = option_contracts_from_specs(plan.hot)
        desired_by_label = {label: (label, kind, contract) for label, kind, contract in desired_contracts}
        retained_labels = set(self.hot_subs) & set(desired_by_label)
        added_labels = set(desired_by_label) - retained_labels
        obsolete_labels = set(self.hot_subs) - retained_labels

        if not self._cancel_batch(self.rotation_subs):
            return False
        self.rotation_subs = {}
        max_lines = getattr(
            getattr(self, "stream_settings", None),
            "max_option_lines",
            len(self.hot_subs) + len(added_labels),
        )
        free_lines = max(int(max_lines) - len(self.hot_subs), 0)
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

        obsolete_subs = {
            label: remaining_hot[label]
            for label in obsolete_labels - release_labels
        }
        if not self._cancel_batch(obsolete_subs):
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="hot")
            self.hot_subs = {**remaining_hot, **restored}
            return False
        self.hot_subs = {
            **{
                label: remaining_hot[label]
                for label in retained_labels
            },
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

        cache = getattr(self, "qualified_option_contracts", None)
        if cache is None:
            # Lightweight unit-test collectors built with object.__new__ do
            # not own a session cache; their mocked transport resolves rows.
            return definitions
        resolved: dict[str, tuple[str, str, Any]] = {}
        pending: list[tuple[str, str, Any]] = []
        for label, kind, contract in definitions:
            cached = cache.get(label)
            if cached is not None:
                resolved[label] = cached
            elif contract_has_con_id(contract):
                resolved[label] = (label, kind, contract)
                cache[label] = resolved[label]
            else:
                pending.append((label, kind, contract))

        qualify = getattr(self.ib, "qualifyContracts", None)
        if pending and callable(qualify):
            try:
                qualified = self._batch_qualify(
                    [contract for _, _, contract in pending]
                )
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
        elif pending:
            # Test doubles without an IB qualification surface still exercise
            # lifecycle logic through the mocked qualify_and_subscribe call.
            for item in pending:
                resolved[item[0]] = item

        return [resolved[label] for label, _, _ in definitions if label in resolved]

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
            not row.subscribed or row.error
            for _, row in subscriptions.values()
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
        return True

    def _apply_subscription_rejections(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
        *,
        rejection_sequence: int,
    ) -> bool:
        rows_by_request_id = {
            row.request_id: row
            for _, row in subscriptions.values()
            if row.request_id is not None
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
