"""Receipt-aligned RTH shadow execution observations and cost labels."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Mapping

from spx_spark.application.market_features.trade_intent import (
    live_trade_intent_authority_issues,
)
from spx_spark.application.market_features.virtual_strategy_support import (
    _action_underlier_snapshot,
    _contract_snapshot,
    _entry_observed_at,
    _episode,
    _latest_created_at,
    _level_reached,
    _number,
    _time,
    _utc,
)
from spx_spark.notifier.model import ExternalDeliveryReceipt
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState, configured_quote_use_decision
from spx_spark.strategy_contract import (
    actionable_strategy_contract_issues,
    normalize_block_reasons,
    parse_aware_time,
    policy_version,
    strategy_event_fields,
)


def evaluate_trade_intent_entry(
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    expected_policy_version: str | None,
    require_external_receipt: bool = False,
    external_receipt: ExternalDeliveryReceipt | None = None,
    external_receipt_observable: bool = True,
    external_receipt_error: str | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    """Start an RTH shadow episode only from a post-receipt executable ask."""

    now = _utc(now)
    source_id = str(trade_intent.get("intent_id") or "")
    contract_id = str(trade_intent.get("contract_id") or "")
    delivery_event_id = str(trade_intent.get("notification_event_id") or "")
    decision_policy = policy_version(
        "virtual_rth_action_revalidation.v3",
        {
            "market_features": policy,
            "expected_source_policy_version": expected_policy_version,
        },
    )

    def result(
        reasons: tuple[str, ...] | list[str],
        *,
        terminal: bool,
        snapshot: Mapping[str, object] | None = None,
        episode: Mapping[str, object] | None = None,
        shadow_execution_label: str | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        raw_coordinate = trade_intent.get("coordinate")
        coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
        normalized = normalize_block_reasons(reasons)
        return dict(episode or {}), {
            **strategy_event_fields(
                policy_version_value=decision_policy,
                valid_until=parse_aware_time(trade_intent.get("valid_until")),
                coordinate=coordinate,
                block_reasons=normalized,
            ),
            "event": "virtual_entry_decision",
            "decision_id": f"virtual-entry:{source_id or 'unavailable'}",
            "source_signal_id": source_id or None,
            "source_kind": "trade_intent",
            "source_schema_version": trade_intent.get("schema_version"),
            "source_policy_version": trade_intent.get("policy_version"),
            "source_evaluated_at": trade_intent.get("evaluated_at"),
            "entry_observed_at": _entry_observed_at(trade_intent),
            "action_revalidated_at": now.isoformat(),
            "quote_state_created_at": _latest_created_at(latest),
            "evaluated_at": now.isoformat(),
            "status": "trade_ready" if episode else "blocked" if terminal else "observing",
            "terminal": bool(terminal or episode),
            "contract_id": contract_id or None,
            "entry_limit": trade_intent.get("entry_limit"),
            "action_quote_snapshot": dict(snapshot) if snapshot else None,
            "episode_id": episode.get("episode_id") if episode else None,
            "shadow_execution_label": shadow_execution_label,
            "external_delivery_event_id": delivery_event_id or None,
            "external_delivery_receipt": external_receipt_fields(external_receipt),
            "external_delivery_receipt_observable": external_receipt_observable,
            "external_delivery_receipt_error": external_receipt_error,
            "broker_fill_status": "not_observed",
            "broker_order_state": "not_connected",
            "automatic_ordering": False,
        }

    if not source_id:
        return result(["source_signal_id_unavailable"], terminal=True)
    if not contract_id:
        return result(["execution_contract_unavailable"], terminal=True)
    authority_issues = live_trade_intent_authority_issues(trade_intent)
    if authority_issues:
        return result(authority_issues, terminal=True)
    valid_until = parse_aware_time(trade_intent.get("valid_until"))
    if require_external_receipt:
        if not delivery_event_id:
            return result(
                ["external_delivery_event_id_unavailable"],
                terminal=True,
                shadow_execution_label="censored",
            )
        if not external_receipt_observable:
            expired = valid_until is not None and now >= valid_until
            return result(
                ["external_delivery_receipt_observation_unavailable"],
                terminal=expired,
                shadow_execution_label=(
                    "censored" if expired else "receipt_observation_degraded"
                ),
            )
        if external_receipt is None:
            expired = valid_until is not None and now >= valid_until
            return result(
                [
                    "external_delivery_receipt_missing_before_expiry"
                    if expired
                    else "external_delivery_receipt_pending"
                ],
                terminal=expired,
                shadow_execution_label=("no_fill" if expired else "waiting_receipt"),
            )
        if external_receipt.event_id != delivery_event_id:
            return result(
                ["external_delivery_receipt_event_mismatch"],
                terminal=True,
                shadow_execution_label="no_fill",
            )
        if valid_until is not None and external_receipt.delivered_at >= valid_until:
            return result(
                ["external_delivery_receipt_after_expiry"],
                terminal=True,
                shadow_execution_label="no_fill",
            )
        if now < external_receipt.delivered_at:
            return result(
                ["external_delivery_receipt_in_future"],
                terminal=False,
                shadow_execution_label="waiting_receipt",
            )
        if valid_until is not None and now >= valid_until:
            return result(
                ["entry_limit_not_reached_after_external_receipt"],
                terminal=True,
                shadow_execution_label="no_fill",
            )
    contract_issues = list(actionable_strategy_contract_issues(trade_intent, now=now))
    if contract_issues:
        reasons = [
            "intent_expired" if issue == "strategy_event_expired" else issue
            for issue in contract_issues
        ]
        return result(reasons, terminal=True)
    source_policy = str(trade_intent.get("policy_version") or "")
    if not source_policy.startswith("rth_trade_intent.v3+sha256:"):
        return result(["source_policy_incompatible"], terminal=True)
    if expected_policy_version and source_policy != expected_policy_version:
        return result(["source_policy_version_drift"], terminal=True)
    coordinate = trade_intent.get("coordinate")
    if not isinstance(coordinate, Mapping) or coordinate.get("kind") != "official_spx":
        return result(["source_coordinate_mismatch"], terminal=True)

    snapshot, quote_reasons = trade_intent_action_snapshot(
        latest,
        trade_intent=trade_intent,
        now=now,
        max_quote_age_seconds=policy.trade_quote_max_age_seconds,
        future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
        not_before=(external_receipt.delivered_at if require_external_receipt else None),
    )
    if not snapshot:
        return result(
            quote_reasons,
            terminal=False,
            shadow_execution_label="waiting_limit_fill",
        )
    underlier, underlier_reasons = _action_underlier_snapshot(
        latest,
        instrument_id="index:SPX",
        now=now,
        max_quote_age_seconds=policy.trade_quote_max_age_seconds,
        future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
    )
    if not underlier:
        return result(underlier_reasons, terminal=False, snapshot=snapshot)
    direction = str(trade_intent.get("direction") or "")
    target_spx = _number(trade_intent.get("target_spx"))
    invalidation_spx = _number(trade_intent.get("invalidation_spx"))
    if target_spx is None or invalidation_spx is None:
        return result(["action_underlier_guard_unavailable"], terminal=True, snapshot=snapshot)
    spx = _number(underlier.get("price"))
    if _level_reached(spx, target_spx, direction=direction, target=True):
        snapshot["action_underlier"] = underlier
        return result(["target_reached_before_entry_quote"], terminal=True, snapshot=snapshot)
    if _level_reached(spx, invalidation_spx, direction=direction, target=False):
        snapshot["action_underlier"] = underlier
        return result(
            ["invalidation_reached_before_entry_quote"], terminal=True, snapshot=snapshot
        )
    snapshot["action_underlier"] = underlier
    stop = _time(trade_intent.get("time_stop_at")) or now + timedelta(
        minutes=policy.trade_time_stop_minutes
    )
    if stop <= now:
        return result(["trade_time_stop_elapsed"], terminal=True, snapshot=snapshot)
    episode = _episode(
        source_id=source_id,
        source_kind="trade_intent",
        direction=direction,
        contract_id=contract_id,
        snapshot=snapshot,
        now=now,
        stop=stop,
        invalidation_spx=invalidation_spx,
        target_spx=target_spx,
        invalidation_es=None,
        source_contract=trade_intent,
        lifecycle_policy=policy,
    )
    if not episode:
        return result(["trade_direction_invalid"], terminal=True, snapshot=snapshot)
    episode.update(
        {
            "decision_evaluated_at": trade_intent.get("evaluated_at"),
            "entry_observed_at": _entry_observed_at(trade_intent),
            "action_revalidated_at": now.isoformat(),
            "quote_state_created_at": _latest_created_at(latest),
            "entry_limit": trade_intent.get("entry_limit"),
            "entry_basis": (
                "external_receipt_then_executable_ask"
                if require_external_receipt
                else "action_revalidated_quote_snapshot"
            ),
            "shadow_execution_label": "ask_entry_observed",
            "shadow_entry_semantics": (
                "displayed_ask_at_or_below_limit_after_external_receipt;"
                "not_a_broker_fill"
            ),
            "shadow_entry_ask": snapshot.get("ask"),
            "external_delivery_event_id": delivery_event_id or None,
            "external_delivery_receipt": external_receipt_fields(external_receipt),
            "shadow_fee_per_side_usd": policy.virtual_shadow_fee_per_side_usd,
            "shadow_slippage_per_side_points": (
                policy.virtual_shadow_slippage_per_side_points
            ),
            "broker_fill_status": "not_observed",
            "broker_order_state": "not_connected",
            "execution_assumption": "shadow_only_no_order_submission",
        }
    )
    return result(
        [],
        terminal=True,
        snapshot=snapshot,
        episode=episode,
        shadow_execution_label="ask_entry_observed",
    )


def trade_intent_action_snapshot(
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    now: datetime,
    max_quote_age_seconds: float,
    future_tolerance_seconds: float,
    not_before: datetime | None = None,
) -> tuple[dict[str, object], list[str]]:
    """Reload-sensitive NBBO/limit check for the final virtual-entry boundary."""

    now = _utc(now)
    contract_id = str(trade_intent.get("contract_id") or "")
    quote = latest.best_quote(contract_id) if contract_id else None
    if quote is None:
        return {}, ["action_quote_unavailable"]
    entry_limit = _number(trade_intent.get("entry_limit"))
    if entry_limit is None or entry_limit <= 0:
        return {}, ["action_entry_limit_invalid"]
    observation = trade_intent.get("entry_observation")
    if not isinstance(observation, Mapping):
        return {}, ["entry_observation_unavailable"]
    observation_limit = _number(observation.get("entry_limit"))
    if (
        observation.get("entry_condition") != "displayed_ask_at_or_below_limit"
        or str(observation.get("contract_id") or "") != contract_id
        or observation_limit is None
        or not math.isclose(observation_limit, entry_limit)
    ):
        return {}, ["entry_observation_contract_invalid"]

    provider = str(trade_intent.get("provider") or "")
    if not provider:
        return {}, ["action_quote_provider_unavailable"]
    if quote.provider.value != provider:
        return {}, ["action_quote_provider_mismatch"]
    bid = _number(quote.bid)
    mid = _number(quote.mid)
    ask = _number(quote.ask)
    if bid is None or mid is None or ask is None or not 0 <= bid <= mid <= ask:
        return {}, ["action_quote_nbbo_invalid"]
    source_at = quote.quote_time
    transport_at = quote.last_update_at or quote.received_at
    if source_at is None:
        return {}, ["action_quote_source_time_unavailable"]
    source_age = (now - _utc(source_at)).total_seconds()
    transport_age = (now - _utc(transport_at)).total_seconds()
    if not_before is not None and _utc(transport_at) < _utc(not_before):
        return {}, ["action_quote_precedes_external_receipt"]
    time_reasons: list[str] = []
    tolerance = max(0.0, future_tolerance_seconds)
    if source_age < -tolerance:
        time_reasons.append("action_quote_source_in_future")
    elif source_age > max_quote_age_seconds:
        time_reasons.append("action_quote_source_stale")
    if transport_age < -tolerance:
        time_reasons.append("action_quote_transport_in_future")
    elif transport_age > max_quote_age_seconds:
        time_reasons.append("action_quote_transport_stale")
    if time_reasons:
        return {}, time_reasons
    use = configured_quote_use_decision(quote, as_of=now)
    if not use.pricing_allowed:
        return {}, [f"action_quote_quality_{use.reason}"]
    if ask > entry_limit:
        return {}, ["action_entry_limit_not_reached"]

    snapshot = _contract_snapshot(latest, contract_id, now=now)
    if not snapshot:
        return {}, ["action_contract_snapshot_unavailable"]
    snapshot.update(
        {
            "action_revalidated_at": now.isoformat(),
            "source_age_seconds": source_age,
            "transport_age_seconds": transport_age,
            "entry_limit": entry_limit,
            "entry_limit_satisfied": True,
        }
    )
    return snapshot, []


def external_receipt_fields(
    receipt: ExternalDeliveryReceipt | None,
) -> dict[str, object] | None:
    if receipt is None:
        return None
    return {
        "receipt_id": receipt.receipt_id,
        "delivered_at": receipt.delivered_at.isoformat(),
        "sink": receipt.sink,
        "channel": receipt.channel,
        "ledger": receipt.ledger,
    }


def shadow_execution_costs(
    active: Mapping[str, object],
    *,
    exit_bid: float | None,
) -> dict[str, object]:
    """Return transparent ask-to-bid shadow PnL; never claim a broker fill."""

    if active.get("shadow_execution_label") != "ask_entry_observed" or exit_bid is None:
        return {}
    entry_ask = _number(active.get("shadow_entry_ask") or active.get("entry_ask"))
    fee_per_side = _number(active.get("shadow_fee_per_side_usd"))
    slippage_per_side = _number(active.get("shadow_slippage_per_side_points"))
    if entry_ask is None or fee_per_side is None or slippage_per_side is None:
        return {
            "pnl_status": "shadow_cost_inputs_unavailable",
            "broker_fill_status": "not_observed",
        }
    multiplier = 100.0
    gross = (exit_bid - entry_ask) * multiplier
    fees = fee_per_side * 2.0
    slippage = slippage_per_side * multiplier * 2.0
    return {
        "shadow_execution_label": "closed_ask_to_bid",
        "shadow_contract_multiplier": multiplier,
        "shadow_gross_pnl_usd": round(gross, 2),
        "shadow_fee_cost_usd": round(fees, 2),
        "shadow_slippage_cost_usd": round(slippage, 2),
        "shadow_net_pnl_usd": round(gross - fees - slippage, 2),
        "pnl_status": "shadow_cost_adjusted",
        "pnl_basis": "external_receipt_then_ask_entry_bid_exit",
        "broker_fill_status": "not_observed",
        "broker_order_state": "not_connected",
    }
