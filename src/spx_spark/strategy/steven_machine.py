"""Explicit state handlers for the Steven guidance state machine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from collections.abc import Mapping

from spx_spark.strategy.steven_models import WATCH_STATES, StevenInputs


@dataclass
class TransitionContext:
    inputs: StevenInputs
    regime: str
    map_levels: Mapping[str, Any]
    trigger: Mapping[str, Any]
    flow: Mapping[str, Any]
    invalidation: Mapping[str, Any]
    data_healthy_since: datetime | None
    watch_exit_since: datetime | None
    lockout_until: datetime | None
    daily_setup_count: int

    @classmethod
    def create(
        cls,
        inputs: StevenInputs,
        *,
        regime: str,
        map_levels: Mapping[str, Any],
        trigger: Mapping[str, Any],
        flow: Mapping[str, Any],
        invalidation: Mapping[str, Any],
    ) -> "TransitionContext":
        return cls(
            inputs=inputs,
            regime=regime,
            map_levels=map_levels,
            trigger=trigger,
            flow=flow,
            invalidation=invalidation,
            data_healthy_since=inputs.data_healthy_since,
            watch_exit_since=inputs.watch_exit_since,
            lockout_until=inputs.lockout_until,
            daily_setup_count=inputs.daily_setup_count,
        )

    def result(
        self,
        state: str,
        rule: str | None = None,
        *,
        watch_exit_since: datetime | None = None,
        lockout_until: datetime | None | object = ...,
        setup_delta: int = 0,
    ) -> tuple[str, str | None, datetime | None, datetime | None, datetime | None, int]:
        resolved_lockout = self.lockout_until if lockout_until is ... else lockout_until
        return (
            state,
            rule,
            self.data_healthy_since,
            watch_exit_since,
            resolved_lockout,
            self.daily_setup_count + setup_delta,
        )


WATCH_CONFIRMATIONS: dict[str, tuple[str, bool, str]] = {
    "BULLISH_DIP_WATCH": ("dip_hold", True, "T9"),
    "BEARISH_BREAK_WATCH": ("break_hold", True, "T10"),
    "RANGE_PIN_WATCH": ("range_reject", False, "T11"),
}

WATCH_ENTRY_RULES = {
    "BULLISH_DIP_WATCH": "T6",
    "BEARISH_BREAK_WATCH": "T7",
    "RANGE_PIN_WATCH": "T8",
}


def advance_state(
    inputs: StevenInputs,
    *,
    regime: str,
    map_levels: Mapping[str, Any],
    trigger: Mapping[str, Any],
    flow: Mapping[str, Any],
    invalidation: Mapping[str, Any],
) -> tuple[str, str | None, datetime | None, datetime | None, datetime | None, int]:
    from spx_spark.strategy import steven as rules

    context = TransitionContext.create(
        inputs,
        regime=regime,
        map_levels=map_levels,
        trigger=trigger,
        flow=flow,
        invalidation=invalidation,
    )
    previous = inputs.previous_state
    invalid = rules.data_invalid_conditions(inputs)
    if invalid:
        if previous == "SETUP_CONFIRMED":
            return context.result("EXIT_REVIEW", "T14")
        context.data_healthy_since = None
        return context.result("DATA_INVALID", "T1")

    if context.data_healthy_since is None:
        context.data_healthy_since = inputs.as_of
    if inputs.trading_date and inputs.trading_date != rules.trading_date_et(inputs.as_of):
        context.daily_setup_count = 0
        return context.result("OBSERVE_ONLY", "T17", lockout_until=None)

    if previous in {"OBSERVE_ONLY", "REGIME_UNKNOWN", *WATCH_STATES} and rules._event_wait_active(
        inputs
    ):
        return context.result("EVENT_WAIT", "T3")

    handler = STATE_HANDLERS.get(previous, _handle_default)
    return handler(context, rules)


def _handle_exit_review(context: TransitionContext, _rules: Any):
    lockout = context.inputs.as_of + timedelta(minutes=context.inputs.settings.lockout_minutes)
    return context.result("LOCKOUT_OR_REMAP", "T15", lockout_until=lockout)


def _handle_data_invalid(context: TransitionContext, _rules: Any):
    held = (
        context.data_healthy_since is not None
        and (context.inputs.as_of - context.data_healthy_since).total_seconds()
        >= context.inputs.settings.data_recovery_hold_seconds
    )
    return context.result("OBSERVE_ONLY", "T2") if held else context.result("DATA_INVALID")


def _handle_event_wait(context: TransitionContext, rules: Any):
    if not rules._event_wait_active(context.inputs) and rules._event_stabilized(context.inputs):
        return context.result("OBSERVE_ONLY", "T4")
    return context.result("EVENT_WAIT")


def _handle_lockout(context: TransitionContext, _rules: Any):
    cooled = context.lockout_until is None or context.inputs.as_of >= context.lockout_until
    under_cap = context.daily_setup_count < context.inputs.settings.max_daily_setups
    if cooled and under_cap:
        return context.result("OBSERVE_ONLY", "T16", lockout_until=None)
    return context.result("LOCKOUT_OR_REMAP")


def _handle_setup(context: TransitionContext, rules: Any):
    exited = rules._target_hit(
        context.inputs, context.map_levels, context.regime
    ) or rules._invalidation_confirmed(
        context.inputs,
        context.invalidation,
    )
    return context.result("EXIT_REVIEW", "T13") if exited else context.result("SETUP_CONFIRMED")


def _handle_watch(context: TransitionContext, rules: Any):
    expected_kind, needs_flow, rule = WATCH_CONFIRMATIONS[context.inputs.previous_state]
    confirmed = context.trigger.get("kind") == expected_kind and bool(
        context.trigger.get("confirmed")
    )
    flow_ok = not needs_flow or context.flow.get("status") != "opposed"
    if confirmed and flow_ok:
        return context.result("SETUP_CONFIRMED", rule, setup_delta=1)

    if rules._watch_still_valid(
        context.inputs,
        context.regime,
        context.map_levels,
        context.inputs.previous_state,
    ):
        return context.result(context.inputs.previous_state)
    since = context.watch_exit_since or context.inputs.as_of
    elapsed = (context.inputs.as_of - since).total_seconds()
    if elapsed >= context.inputs.settings.watch_exit_hold_seconds:
        return context.result("OBSERVE_ONLY", "T12")
    return context.result(context.inputs.previous_state, watch_exit_since=since)


def _handle_observe(context: TransitionContext, rules: Any):
    watch = rules._watch_entry_ok(context.inputs, context.regime, context.map_levels)
    if watch in WATCH_ENTRY_RULES:
        return context.result(watch, WATCH_ENTRY_RULES[watch])
    if context.inputs.previous_state == "OBSERVE_ONLY" and context.regime in {"unknown", "mixed"}:
        return context.result("REGIME_UNKNOWN", "T5")
    return context.result(context.inputs.previous_state)


def _handle_default(context: TransitionContext, _rules: Any):
    return context.result(
        context.inputs.previous_state,
        watch_exit_since=context.watch_exit_since,
    )


STATE_HANDLERS = {
    "EXIT_REVIEW": _handle_exit_review,
    "DATA_INVALID": _handle_data_invalid,
    "EVENT_WAIT": _handle_event_wait,
    "LOCKOUT_OR_REMAP": _handle_lockout,
    "SETUP_CONFIRMED": _handle_setup,
    "OBSERVE_ONLY": _handle_observe,
    "REGIME_UNKNOWN": _handle_observe,
    **{state: _handle_watch for state in WATCH_STATES},
}
