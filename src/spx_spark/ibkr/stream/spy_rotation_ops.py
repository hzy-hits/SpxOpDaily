"""SPY option lane and SPXW rotation operations."""

from __future__ import annotations


from spx_spark.ibkr.verifier import VerifyRow

from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.models import (
    OPTION_ROTATION_RETRY_SECONDS,
)

build_spy_option_strikes = stream_deps.build_spy_option_strikes
cancel_subscriptions = stream_deps.cancel_subscriptions
discard_subscriptions = stream_deps.discard_subscriptions
estimate_spy_reference = stream_deps.estimate_spy_reference
log_event = stream_deps.log_event
option_contracts_from_specs = stream_deps.option_contracts_from_specs
option_spec_label = stream_deps.option_spec_label
qualify_and_subscribe = stream_deps.qualify_and_subscribe
spy_option_contracts = stream_deps.spy_option_contracts
time = stream_deps.time


class SpyRotationOps:
    def ensure_spy_option_plan(self, rows: list[VerifyRow], *, expiry: str) -> None:
        if self.skip_options or self.stream_settings.spy_option_lines < 2:
            return
        unhealthy_spy = {
            label: subscription
            for label, subscription in self.spy_subs.items()
            if not subscription[1].subscribed or subscription[1].error
        }
        if unhealthy_spy:
            self._cancel_batch(unhealthy_spy)
            self.spy_subs = {
                label: subscription
                for label, subscription in self.spy_subs.items()
                if label not in unhealthy_spy
            }
            self.spy_plan_key = None
            self.spy_retry_at = time.monotonic() + OPTION_ROTATION_RETRY_SECONDS
            return
        if time.monotonic() < getattr(self, "spy_retry_at", 0.0):
            return
        spy_price = estimate_spy_reference(rows)
        if spy_price is None:
            return
        strike_step = max(self.stream_settings.spy_strike_step, 1)
        strikes = build_spy_option_strikes(
            spy_price,
            lines=self.stream_settings.spy_option_lines,
            step=strike_step,
        )
        if not strikes:
            return
        rounded_atm = round(spy_price / strike_step) * strike_step
        plan_key = (expiry, int(rounded_atm))
        if plan_key == self.spy_plan_key:
            return
        desired_contracts = spy_option_contracts(expiry, strikes)
        desired_by_label = {
            label: (label, kind, contract)
            for label, kind, contract in desired_contracts
        }
        retained_labels = set(self.spy_subs) & set(desired_by_label)
        added_labels = set(desired_by_label) - retained_labels
        obsolete_labels = set(self.spy_subs) - retained_labels
        line_budget = max(int(self.stream_settings.spy_option_lines), len(desired_by_label))
        free_lines = max(line_budget - len(self.spy_subs), 0)
        release_count = max(len(added_labels) - free_lines, 0)
        release_labels = set(sorted(obsolete_labels)[:release_count])
        released_subs = {label: self.spy_subs[label] for label in release_labels}
        if released_subs and not self._cancel_batch(released_subs):
            return
        remaining_spy = {
            label: subscription
            for label, subscription in self.spy_subs.items()
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
        if not self._subscription_batch_succeeded(
            additions,
            expected_count=len(added_labels),
            rejection_sequence=rejection_sequence,
            connectivity_sequence=connectivity_sequence,
            lane="spy",
        ):
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="spy")
            self.spy_subs = {**remaining_spy, **restored}
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "spy_option_replan_failed",
                    "spy_atm": rounded_atm,
                    "added": len(added_labels),
                    "restored": len(restored),
                }
            )
            self.spy_retry_at = time.monotonic() + OPTION_ROTATION_RETRY_SECONDS
            return
        obsolete_subs = {
            label: remaining_spy[label]
            for label in obsolete_labels - release_labels
        }
        if not self._cancel_batch(obsolete_subs):
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="spy")
            self.spy_subs = {**remaining_spy, **restored}
            return
        self.spy_subs = {
            **{label: remaining_spy[label] for label in retained_labels},
            **additions,
        }
        self.spy_plan_key = plan_key
        self.spy_retry_at = 0.0
        log_event(
            {
                "task": "ibkr_stream",
                "event": "spy_option_replan",
                "spy_atm": rounded_atm,
                "retained": len(retained_labels),
                "added": len(added_labels),
                "removed": len(obsolete_labels),
            }
        )

    def rotate_options(self) -> None:
        plan = self.option_plan
        if plan is None or not plan.rotations:
            return
        now_monotonic = time.monotonic()
        if now_monotonic < getattr(self, "rotation_retry_at", 0.0):
            return
        if any(
            not row.subscribed or row.error
            for _, row in self.rotation_subs.values()
        ):
            self._cancel_batch(self.rotation_subs)
            self.rotation_subs = {}
            self.rotation_index = max(self.rotation_index - 1, 0)
            self.rotation_retry_at = now_monotonic + OPTION_ROTATION_RETRY_SECONDS
            return
        if not self._cancel_batch(self.rotation_subs):
            return
        self.rotation_subs = {}
        slice_index = self.rotation_index % plan.rotation_count
        slice_specs = plan.rotations[slice_index]
        pinned_labels = set(getattr(self, "pinned_subs", {}))
        max_option_lines = int(
            getattr(
                self.stream_settings,
                "max_option_lines",
                len(getattr(self, "hot_subs", {})) + len(pinned_labels) + len(slice_specs),
            )
        )
        available_lines = max(
            max_option_lines
            - len(getattr(self, "hot_subs", {}))
            - len(pinned_labels),
            0,
        )
        eligible_specs = tuple(
            spec
            for spec in slice_specs
            if option_spec_label(spec) not in pinned_labels
        )[:available_lines]
        rejection_sequence = self.subscription_rejection_sequence
        connectivity_sequence = getattr(self, "tws_connectivity_loss_sequence", 0)
        definitions = self._resolve_option_definitions(
            option_contracts_from_specs(eligible_specs)
        )
        replacement = qualify_and_subscribe(
            self.ib,
            definitions,
            qualify=False,
        )
        if not self._subscription_batch_succeeded(
            replacement,
            expected_count=len(eligible_specs),
            rejection_sequence=rejection_sequence,
            connectivity_sequence=connectivity_sequence,
            lane="rotation",
        ):
            self._cancel_batch(replacement)
            self.rotation_retry_at = now_monotonic + OPTION_ROTATION_RETRY_SECONDS
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "option_rotation_failed",
                    "slice_index": slice_index,
                    "contracts": len(eligible_specs),
                }
            )
            return
        self.rotation_subs = replacement
        self.rotation_index += 1
        self.rotation_retry_at = 0.0
