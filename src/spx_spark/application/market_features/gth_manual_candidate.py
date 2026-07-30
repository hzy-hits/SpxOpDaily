"""Manual-only GTH candidate built from exact SPXW quotes and live coordinates."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.analytics.options.quote_policy import (
    option_field_live_entitlement,
    option_field_live_entitlement_source,
)
from spx_spark.application.market_features.virtual_strategy_spread import (
    spread_snapshot_decision,
)
from spx_spark.application.market_features.virtual_strategy_state import (
    flush_pending_notifications,
)
from spx_spark.application.market_features.virtual_strategy_support import (
    _gth_spread_contract_ids,
    _number,
    _time,
    _utc,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import Provider, as_utc
from spx_spark.notifier.dispatcher import cancel_pending_notification
from spx_spark.notifier.operator_cards import (
    beijing_time,
    option_contract_label,
    remaining_seconds,
)
from spx_spark.options_map import actionable_chain_implied_reference
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import (
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)
from spx_spark.storage import LatestState, configured_quote_use_decision
from spx_spark.strategy_contract import (
    actionable_strategy_contract_issues,
    policy_version,
)


CONTRACT_VERSION = "gth_manual_candidate.v1"
SOURCE_KIND = "gth_dip_reclaim_call"
NET_DEBIT_PRICE_INCREMENT = 0.05


def evaluate_gth_manual_candidate(
    latest: LatestState,
    gth_signal: Mapping[str, object],
    *,
    macro_event: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    new_entries_allowed: bool,
    new_entries_block_reason: str,
) -> dict[str, object]:
    """Return a candidate for an operator, never live broker authority."""

    now = _utc(now)
    source_id = str(gth_signal.get("event_id") or "")
    candidate_policy_version = policy_version(
        CONTRACT_VERSION,
        {
            "quote_max_age_seconds": (
                policy.gth_manual_candidate_quote_max_age_seconds
            ),
            "ttl_seconds": policy.gth_manual_candidate_ttl_seconds,
            "max_debit_fraction": (
                policy.gth_manual_candidate_max_debit_fraction
            ),
            "max_net_spread_fraction": (
                policy.gth_manual_candidate_max_net_spread_fraction
            ),
            "min_parity_pairs": policy.gth_manual_candidate_min_parity_pairs,
            "max_parity_dispersion_points": (
                policy.gth_manual_candidate_max_parity_dispersion_points
            ),
            "max_parity_interval_points": (
                policy.gth_manual_candidate_max_parity_interval_points
            ),
            "target_room_buffer_points": (
                policy.gth_manual_candidate_target_room_buffer_points
            ),
            "close_buffer_seconds": (
                policy.gth_manual_candidate_close_buffer_seconds
            ),
            "max_reclaim_age_seconds": (
                policy.gth_manual_candidate_max_reclaim_age_seconds
            ),
            "min_reward_risk": policy.gth_manual_candidate_min_reward_risk,
        },
    )
    base: dict[str, object] = {
        "schema_version": 1,
        "kind": "gth_spxw_manual_spread_candidate",
        "contract_version": CONTRACT_VERSION,
        "candidate_id": None,
        "policy_version": candidate_policy_version,
        "source_signal_id": source_id or None,
        "source_kind": str(gth_signal.get("kind") or "") or None,
        "evaluated_at": now.isoformat(),
        "status": "observing",
        "candidate_scope": "manual_live",
        "execution_mode": "manual_only",
        "manual_action_eligible": False,
        "execution_eligible": False,
        "automatic_ordering": False,
        "simulation_only": False,
        "broker_submission_allowed": False,
        "rth_trade_ready_authority": False,
        "broker_position_effect": "none",
        "must_requote_before_submit": True,
        "account_gth_permission_status": "unverified",
        "quantity": None,
        "quantity_policy": "operator_selected",
        "block_reasons": [],
    }
    if not policy.gth_manual_candidate_enabled:
        return {**base, "status": "disabled", "block_reasons": ["disabled"]}
    if not source_id:
        return {**base, "block_reasons": ["source_signal_unavailable"]}

    reasons: list[str] = []
    if not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        reasons.append("spx_gth_session_required")
    if gth_signal.get("kind") != SOURCE_KIND:
        reasons.append("source_signal_kind_mismatch")
    if not str(gth_signal.get("policy_version") or "").startswith(
        "gth_dip_reclaim.v4+sha256:"
    ):
        reasons.append("source_policy_incompatible")
    reasons.extend(actionable_strategy_contract_issues(gth_signal, now=now))
    entry_quality = gth_signal.get("entry_quality")
    if not isinstance(entry_quality, Mapping):
        reasons.append("source_entry_quality_unavailable")
    else:
        if entry_quality.get("mode") != "decision_grade":
            reasons.append("source_entry_quality_not_decision_grade")
        if not str(entry_quality.get("policy_version") or "").startswith(
            "gth_trend_alignment_live_v2"
        ):
            reasons.append("source_entry_quality_policy_incompatible")
        if entry_quality.get("verdict") != "pass":
            reasons.append("source_entry_quality_blocked")
        if entry_quality.get("block_reasons"):
            reasons.append("source_entry_quality_has_block_reasons")
    coordinate = gth_signal.get("coordinate")
    if not isinstance(coordinate, Mapping) or coordinate.get("kind") != "raw_es":
        reasons.append("source_coordinate_mismatch")
    session_date = str(gth_signal.get("session_date") or "")
    if session_date != DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat():
        reasons.append("signal_session_mismatch")
    if macro_event.get("entry_allowed") is not True:
        reasons.append("macro_entry_blocked")
    if not new_entries_allowed:
        reasons.append(f"provider_entry_blocked:{new_entries_block_reason}")

    spread = gth_signal.get("spread")
    if not isinstance(spread, Mapping):
        reasons.append("exact_spread_contract_unavailable")
        return _blocked(base, reasons)
    if spread.get("right") != "C" or spread.get("expiry_date") != session_date:
        reasons.append("spread_contract_mismatch")
    contract_ids = _gth_spread_contract_ids(spread, session_date=session_date)
    if contract_ids is None:
        reasons.append("spread_contract_invalid")
        return _blocked(base, reasons)
    long_contract_id, short_contract_id = contract_ids
    identity = "|".join(
        (
            CONTRACT_VERSION,
            candidate_policy_version,
            str(gth_signal.get("policy_version") or ""),
            source_id,
            long_contract_id,
            short_contract_id,
        )
    )
    base["candidate_id"] = (
        "gth-manual:" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    )

    snapshot, quote_reasons = spread_snapshot_decision(
        latest,
        long_contract_id=long_contract_id,
        short_contract_id=short_contract_id,
        now=now,
        max_quote_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
        max_quote_skew_seconds=policy.provider_sync_tolerance_seconds,
        required_provider=Provider.IBKR.value,
        contract_snapshot=_gth_bbo_contract_snapshot,
    )
    reasons.extend(quote_reasons)
    declared_width = _number(spread.get("width_points"))
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    width = (
        short_strike - long_strike
        if long_strike is not None and short_strike is not None
        else None
    )
    bid = _number(snapshot.get("bid"))
    mid = _number(snapshot.get("mid"))
    ask = _number(snapshot.get("ask"))
    if width is None or width <= 0:
        reasons.append("spread_width_invalid")
    elif declared_width is None or not math.isclose(declared_width, width):
        reasons.append("spread_width_contract_mismatch")
    if (
        bid is None
        or mid is None
        or ask is None
        or not 0 <= bid <= mid <= ask
    ):
        reasons.append("spread_net_nbbo_invalid")
    elif width is not None:
        if ask >= width * policy.gth_manual_candidate_max_debit_fraction:
            reasons.append("spread_debit_risk_cap_exceeded")
        if (
            ask - bid
            > width * policy.gth_manual_candidate_max_net_spread_fraction
        ):
            reasons.append("spread_net_market_too_wide")
    entry_limit = (
        math.floor(mid / NET_DEBIT_PRICE_INCREMENT + 1e-12)
        * NET_DEBIT_PRICE_INCREMENT
        if mid is not None
        else None
    )
    if entry_limit is None or entry_limit <= 0:
        reasons.append("spread_entry_limit_invalid")

    expiry = session_date.replace("-", "")
    parity = actionable_chain_implied_reference(
        latest,
        expiry=expiry,
        as_of=now,
        required_provider=Provider.IBKR,
        max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
        max_leg_skew_seconds=policy.provider_sync_tolerance_seconds,
        min_pair_count=policy.gth_manual_candidate_min_parity_pairs,
        max_dispersion_points=(
            policy.gth_manual_candidate_max_parity_dispersion_points
        ),
        max_pair_interval_points=(
            policy.gth_manual_candidate_max_parity_interval_points
        ),
    )
    if parity is None:
        reasons.append("chain_implied_target_unavailable")

    es_reference = _direct_es_reference(
        latest,
        now=now,
        max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
    )
    if es_reference is None:
        reasons.append("direct_es_invalidation_unavailable")
    invalidation_es = _number(spread.get("invalidation_es"))
    signal_trough = _number(gth_signal.get("trough"))
    if invalidation_es is None or signal_trough is None:
        reasons.append("gth_invalidation_unavailable")
    elif not math.isclose(invalidation_es, signal_trough, abs_tol=1e-9):
        reasons.append("gth_invalidation_contract_mismatch")
    elif (
        es_reference is not None
        and float(es_reference["price"]) <= invalidation_es
    ):
        reasons.append("invalidation_reached_before_candidate")

    target_spx = _number(spread.get("target_wall"))
    parity_upper_bound = None
    if target_spx is None:
        reasons.append("target_wall_unavailable")
    elif spread.get("anchor") != "structure_wall":
        reasons.append("target_wall_anchor_invalid")
    elif spread.get("target_wall_kind") not in {"flip_high", "call_wall"}:
        reasons.append("target_wall_kind_invalid")
    elif short_strike is None or target_spx < short_strike:
        reasons.append("target_wall_below_short_strike")
    elif parity is not None:
        parity_upper_bound = float(parity["upper_bound"]) + (
            policy.gth_manual_candidate_target_room_buffer_points
        )
        if parity_upper_bound >= target_spx:
            reasons.append("target_room_below_parity_uncertainty_bound")

    signal_valid_until = _time(gth_signal.get("valid_until"))
    trough_at = _time(gth_signal.get("trough_at"))
    if trough_at is None:
        reasons.append("gth_trough_at_unavailable")
    elif trough_at > now:
        reasons.append("gth_trough_at_in_future")
    elif (
        now - trough_at
    ).total_seconds() > policy.gth_manual_candidate_max_reclaim_age_seconds:
        reasons.append("gth_reclaim_too_old")
    spread_exit_at = _time(spread.get("exit_at"))
    if spread_exit_at is None:
        reasons.append("spread_exit_at_unavailable")
    elif spread_exit_at <= now:
        reasons.append("spread_exit_at_elapsed")
    gth_end = _gth_end(now)
    candidate_cutoff = (
        gth_end
        - timedelta(seconds=policy.gth_manual_candidate_close_buffer_seconds)
        if gth_end is not None
        else None
    )
    if signal_valid_until is None:
        reasons.append("signal_valid_until_unavailable")
    if candidate_cutoff is None or now >= candidate_cutoff:
        reasons.append("gth_entry_clock_closed")
    reward_risk = (
        (width - entry_limit) / entry_limit
        if (
            width is not None
            and entry_limit is not None
            and 0 < entry_limit < width
        )
        else None
    )
    if reward_risk is None:
        reasons.append("spread_reward_risk_unavailable")
    elif reward_risk < policy.gth_manual_candidate_min_reward_risk:
        reasons.append("spread_reward_risk_insufficient")

    if (
        reasons
        or bid is None
        or mid is None
        or ask is None
        or width is None
        or entry_limit is None
    ):
        return _blocked(
            {
                **base,
                "long_contract_id": long_contract_id,
                "short_contract_id": short_contract_id,
                "exact_spread_snapshot": snapshot or None,
                "target_coordinate": parity,
                "invalidation_coordinate": es_reference,
            },
            reasons,
        )

    quote_remaining = _quote_remaining_seconds(
        snapshot,
        parity=parity,
        es_reference=es_reference,
        now=now,
        max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
    )
    valid_until_candidates = [
        now
        + timedelta(
            seconds=min(
                policy.gth_manual_candidate_ttl_seconds,
                quote_remaining,
            )
        ),
        signal_valid_until,
        candidate_cutoff,
    ]
    if spread_exit_at is not None:
        valid_until_candidates.append(spread_exit_at)
    valid_until = min(item for item in valid_until_candidates if item is not None)
    if valid_until <= now:
        return _blocked(base, ["candidate_ttl_elapsed"])

    max_loss = entry_limit * 100.0
    max_profit = (width - entry_limit) * 100.0
    return {
        **base,
        "status": "manual_ready",
        "manual_action_eligible": True,
        "valid_until": valid_until.isoformat(),
        "direction": "up",
        "position_type": "call_debit_spread",
        "long_contract_id": long_contract_id,
        "short_contract_id": short_contract_id,
        "contract_id": f"{long_contract_id}|-{short_contract_id}",
        "entry_limit": entry_limit,
        "suggested_debit": entry_limit,
        "max_debit": entry_limit,
        "price_increment": NET_DEBIT_PRICE_INCREMENT,
        "price_increment_source": "gth_manual_net_debit_policy",
        "order_type": "NET_DEBIT_LIMIT",
        "entry_rule": "manual_debit_limit_at_or_below_decision_mid",
        "quote_basis": "synthetic_from_leg_nbbo",
        "synthetic_combo_warning": "not_native_combo_bbo",
        "decision_bid": bid,
        "decision_mid": mid,
        "decision_ask": ask,
        "spread_width_points": width,
        "max_loss_per_spread": round(max_loss, 2),
        "max_profit_per_spread": round(max_profit, 2),
        "breakeven_spx_at_expiry": (
            round(long_strike + entry_limit, 2)
            if long_strike is not None
            else None
        ),
        "reward_risk_at_limit": (
            round(reward_risk, 4) if reward_risk is not None else None
        ),
        "signal_coordinate": dict(coordinate),
        "target_spx": target_spx,
        "target_wall_kind": spread.get("target_wall_kind"),
        "current_parity_spx": float(parity["price"]),
        "current_parity_lower_bound": float(parity["lower_bound"]),
        "current_parity_upper_bound": float(parity["upper_bound"]),
        "target_parity_upper_bound": parity_upper_bound,
        "target_coordinate": parity,
        "invalidation_es": invalidation_es,
        "invalidation_coordinate": es_reference,
        "exit_at": spread.get("exit_at"),
        "exact_spread_snapshot": snapshot,
        "block_reasons": [],
    }


def process_gth_manual_candidate(
    storage: StorageSettings,
    latest: LatestState,
    gth_signal: Mapping[str, object],
    *,
    macro_event: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    new_entries_allowed: bool,
    new_entries_block_reason: str,
    notification: NotificationSettings | None = None,
) -> dict[str, object]:
    """Persist and idempotently notify a newly ready manual candidate."""

    now = _utc(now)
    candidate = evaluate_gth_manual_candidate(
        latest,
        gth_signal,
        macro_event=macro_event,
        now=now,
        policy=policy,
        new_entries_allowed=new_entries_allowed,
        new_entries_block_reason=new_entries_block_reason,
    )
    state_path = (
        Path(storage.data_root)
        / "latest"
        / "gth_manual_candidate_state.json"
    )
    projection_path = (
        Path(storage.data_root) / "latest" / "gth_manual_candidate.json"
    )
    notification_event_id = (
        f"{candidate['candidate_id']}:ready"
        if candidate.get("status") == "manual_ready"
        else None
    )
    notification_settings = notification or NotificationSettings.from_env()
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        accepted = {
            str(item)
            for item in (
                list(state.get("accepted_notification_event_ids") or [])
                + list(state.get("notified_event_ids") or [])
            )
            if item
        }
        settled = {
            str(item)
            for item in state.get("settled_notification_event_ids") or []
            if item
        }
        pending = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
        ]
        lifecycle_events = {
            str(item.get("event_id") or ""): str(
                item.get("source_signal_id") or ""
            )
            for item in state.get("notification_lifecycle_events") or []
            if isinstance(item, Mapping)
            and item.get("event_id")
            and item.get("source_signal_id")
        }
        cancellation_pending = {
            str(item)
            for item in state.get(
                "pending_notification_cancellation_event_ids"
            )
            or []
            if item
        }
        previous_candidate = state.get("last_candidate")
        if isinstance(previous_candidate, Mapping):
            previous_candidate_id = str(
                previous_candidate.get("candidate_id") or ""
            )
            previous_source_id = str(
                previous_candidate.get("source_signal_id") or ""
            )
            if previous_candidate_id and previous_source_id:
                lifecycle_events.setdefault(
                    f"{previous_candidate_id}:ready",
                    previous_source_id,
                )
        for item in pending:
            event_id = str(item.get("event_id") or "")
            source_signal_id = str(item.get("source_signal_id") or "")
            if event_id and source_signal_id:
                lifecycle_events.setdefault(event_id, source_signal_id)
        if notification_event_id:
            lifecycle_events.setdefault(
                notification_event_id,
                str(candidate.get("source_signal_id") or ""),
            )
        if candidate.get("status") != "manual_ready":
            # This state machine owns one current GTH candidate projection.
            # Losing/replacing the source signal must revoke every still-live
            # green card, including one whose new evaluation has no source id.
            cancellation_pending.update(lifecycle_events)
        for event_id in sorted(cancellation_pending):
            try:
                cancel_pending_notification(
                    notification_settings,
                    event_id,
                    now=now,
                    reason="source_candidate_no_longer_manual_ready",
                )
            except Exception:
                # Keep an explicit cancellation intent even after producer
                # acknowledgement removed the original pending notification.
                continue
            cancellation_pending.discard(event_id)
            settled.add(event_id)
            accepted.discard(event_id)
            lifecycle_events.pop(event_id, None)
            pending = [
                item
                for item in pending
                if str(item.get("event_id") or "") != event_id
            ]
        pending_ids = {str(item.get("event_id") or "") for item in pending}
        if (
            notification_event_id
            and not cancellation_pending
            and notification_event_id not in accepted
            and notification_event_id not in settled
            and notification_event_id not in pending_ids
        ):
            pending.append(
                _notification_intent(
                    candidate,
                    event_id=notification_event_id,
                    now=now,
                )
            )
        state.update(
            {
                "schema_version": 3,
                "updated_at": now.isoformat(),
                "last_candidate": candidate,
                "accepted_notification_event_ids": sorted(accepted)[-200:],
                "settled_notification_event_ids": sorted(settled)[-200:],
                "pending_notifications": pending,
                "notification_lifecycle_events": [
                    {
                        "event_id": event_id,
                        "source_signal_id": source_signal_id,
                    }
                    for event_id, source_signal_id in sorted(
                        lifecycle_events.items()
                    )[-200:]
                ],
                "pending_notification_cancellation_event_ids": sorted(
                    cancellation_pending
                )[-200:],
            }
        )
        atomic_write_json_secure(state_path, state)
        atomic_write_json_secure(projection_path, candidate)

    result = {"attempted": False, "accepted": False}
    if notification_event_id:
        result = flush_pending_notifications(
            state_path,
            settings=notification_settings,
            now=now,
            only_event_id=notification_event_id,
        )
    return {
        **candidate,
        "notification_attempted": bool(result.get("attempted")),
        "notification_accepted": bool(result.get("accepted")),
        "notification_outcome": result.get("outcome"),
    }


def _blocked(
    base: Mapping[str, object],
    reasons: list[str],
) -> dict[str, object]:
    return {
        **base,
        "status": "blocked",
        "manual_action_eligible": False,
        "block_reasons": list(dict.fromkeys(reasons)),
    }


def _gth_bbo_contract_snapshot(
    latest: LatestState,
    contract_id: str,
    *,
    now: datetime,
) -> dict[str, object]:
    """Return execution BBO facts; Greeks are optional enrichment in GTH."""

    quote = latest.best_quote(contract_id)
    if (
        quote is None
        or quote.bid is None
        or quote.mid is None
        or quote.ask is None
        or quote.quote_time is None
        or not 0 <= quote.bid <= quote.mid <= quote.ask
    ):
        return {}
    use = configured_quote_use_decision(quote, as_of=now)
    if (
        not use.pricing_allowed
        or not option_field_live_entitlement(quote, field="pricing")
    ):
        return {}
    pricing_entitlement_source = option_field_live_entitlement_source(
        quote,
        field="pricing",
    )
    source_at = as_utc(quote.quote_time)
    transport_at = as_utc(quote.last_update_at or quote.received_at)
    greeks = quote.greeks
    iv = greeks.implied_vol if greeks is not None else None
    underlier = greeks.underlier_price if greeks is not None else None
    return {
        "at": _utc(now).isoformat(),
        "mid": float(quote.mid),
        "bid": float(quote.bid),
        "ask": float(quote.ask),
        "provider": quote.provider.value,
        "source_at": source_at.isoformat(),
        "transport_at": transport_at.isoformat(),
        "iv": iv,
        "underlier": underlier,
        "quality": {
            "status": "ok",
            "pricing_decision": use.reason,
            "pricing_live_entitlement_source": pricing_entitlement_source,
            "greeks": (
                "available"
                if iv is not None and underlier is not None
                else "optional_unavailable"
            ),
        },
    }


def _direct_es_reference(
    latest: LatestState,
    *,
    now: datetime,
    max_age_seconds: float,
) -> dict[str, object] | None:
    quote = latest.best_quote("future:ES")
    if quote is None:
        return None
    if (
        quote.bid is not None
        and quote.mid is not None
        and quote.ask is not None
        and 0 < quote.bid <= quote.mid <= quote.ask
        and quote.quote_time is not None
    ):
        price = float(quote.mid)
        price_kind = "mid"
        source_at = quote.quote_time
    elif quote.last is not None and quote.last > 0 and quote.trade_time is not None:
        price = float(quote.last)
        price_kind = "last"
        source_at = quote.trade_time
    else:
        return None
    transport_at = quote.last_update_at or quote.received_at
    source_age = (now - as_utc(source_at)).total_seconds()
    transport_age = (now - as_utc(transport_at)).total_seconds()
    if (
        source_age < -1.0
        or source_age > max_age_seconds
        or transport_age < -1.0
        or transport_age > max_age_seconds
        or not configured_quote_use_decision(quote, as_of=now).pricing_allowed
    ):
        return None
    return {
        "kind": "raw_es",
        "instrument_id": "future:ES",
        "price": price,
        "price_kind": price_kind,
        "provider": quote.provider.value,
        "source_at": as_utc(source_at).isoformat(),
        "transport_at": as_utc(transport_at).isoformat(),
        "source_age_seconds": source_age,
        "transport_age_seconds": transport_age,
    }


def _gth_end(now: datetime) -> datetime | None:
    session_day = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    return datetime.combine(session_day, time(9, 25), tzinfo=ET).astimezone(
        timezone.utc
    )


def _quote_remaining_seconds(
    snapshot: Mapping[str, object],
    *,
    parity: Mapping[str, object] | None,
    es_reference: Mapping[str, object] | None,
    now: datetime,
    max_age_seconds: float,
) -> float:
    ages = [
        _number(snapshot.get(field))
        for field in (
            "long_quote_age_seconds",
            "short_quote_age_seconds",
            "long_transport_age_seconds",
            "short_transport_age_seconds",
        )
    ]
    for reference in (parity, es_reference):
        if not isinstance(reference, Mapping):
            continue
        for field in ("source_at", "transport_at"):
            observed_at = _time(reference.get(field))
            if observed_at is not None:
                ages.append((now - observed_at).total_seconds())
    finite_ages = [max(float(age), 0.0) for age in ages if age is not None]
    return max(max_age_seconds - max(finite_ages or [max_age_seconds]), 0.0)


def _notification_intent(
    candidate: Mapping[str, object],
    *,
    event_id: str,
    now: datetime,
) -> dict[str, object]:
    long_id = str(candidate["long_contract_id"])
    short_id = str(candidate["short_contract_id"])
    long_label = option_contract_label(long_id)
    short_label = option_contract_label(short_id)
    ttl = remaining_seconds(candidate.get("valid_until"), now=now)
    ttl_text = f"剩余 {ttl} 秒" if ttl is not None else "时效未知"
    is_put = str(candidate.get("position_type") or "").startswith("put_")
    level_path = str(candidate.get("path_kind") or "")
    side = "PUT" if is_put else "CALL"
    structure = "Put 借记价差" if is_put else "Call 借记价差"
    if level_path == "flip_low_breakdown_put":
        trigger_text = (
            f"SPX 跌破 Flip Low {float(candidate['trigger_level']):.2f} 并确认；"
            f"当前隐含 SPX {float(candidate['current_parity_spx']):.2f}"
        )
        explanation = "Flip Low 向下路径已确认，用限定亏损的 Put 借记价差表达"
    elif level_path == "lower_rejection_call":
        trigger_text = (
            f"SPX 拒绝下沿并收复 {float(candidate['trigger_level']):.2f}；"
            f"当前隐含 SPX {float(candidate['current_parity_spx']):.2f}"
        )
        explanation = "下沿拒绝与收复已确认，用限定亏损的 Call 借记价差表达"
    elif level_path == "upper_acceptance_call":
        trigger_text = (
            f"SPX 接受上沿 {float(candidate['trigger_level']):.2f} 并确认；"
            f"当前隐含 SPX {float(candidate['current_parity_spx']):.2f}"
        )
        explanation = "上沿接受路径已确认，用限定亏损的 Call 借记价差表达"
    else:
        trigger_text = (
            "GTH Dip-Reclaim 已确认；"
            f"SPX parity {float(candidate['current_parity_spx']):.2f}"
        )
        explanation = "夜盘回收结构已确认，用限定亏损的 Call 借记价差表达向上机会"
    invalidation_text = (
        f"止损  SPX {'收回' if is_put else '跌回'} "
        f"{float(candidate['invalidation_spx']):.2f}；"
        f"ES {'升至' if is_put else '跌至'} "
        f"{float(candidate['invalidation_es']):.2f}"
        if candidate.get("invalidation_spx") is not None
        else f"止损  ES 跌至或低于 {float(candidate['invalidation_es']):.2f}"
    )
    target_label = {
        "put_wall": "Put Wall",
        "call_wall": "Call Wall",
        "flip_low": "Flip Low",
        "time_stop": "时间退出",
    }.get(
        str(candidate.get("target_wall_kind") or ""),
        "结构目标",
    )
    text = "\n".join(
        (
            f"🟢 MANUAL READY · {side} SPREAD",
            f"类型  {structure} · 仅人工提交",
            f"买入  {long_label}",
            f"卖出  {short_label}",
            f"NBBO  {float(candidate['decision_bid']):.2f} / "
            f"{float(candidate['decision_ask']):.2f}；"
            "两腿合成，不是交易所原生组合 BBO",
            f"限价  净借记 ≤ {float(candidate['entry_limit']):.2f}",
            f"触发  {trigger_text}",
            invalidation_text,
            f"目标  SPX {float(candidate['target_spx']):.2f}"
            f"（{target_label}）",
            f"退出  {beijing_time(candidate.get('exit_at'))}",
            f"有效  {ttl_text}（至 "
            f"{beijing_time(candidate.get('valid_until'), seconds=True)}）；提交前重新报价",
            f"风险  每组最大损失 ${float(candidate['max_loss_per_spread']):.0f}；"
            "数量由人工确认",
            f"解释  {explanation}",
            "权限  自动下单关闭；账户 GTH 权限未验证；禁止市价提交",
        )
    )
    level_lane = bool(level_path)
    return {
        "event_id": event_id,
        "source": (
            "gth_level_manual_candidate"
            if level_lane
            else "gth_manual_candidate"
        ),
        "kind": str(
            candidate.get("kind")
            or "gth_spxw_manual_spread_candidate"
        ),
        "lane": (
            "gth_level_manual_candidate"
            if level_lane
            else "gth_manual_candidate"
        ),
        "occurred_at": now.isoformat(),
        "expires_at": str(candidate["valid_until"]),
        "candidate_id": candidate["candidate_id"],
        "source_signal_id": candidate["source_signal_id"],
        "title": "SPX GTH OPERATOR CANDIDATE · MANUAL ONLY",
        "text": text,
        "friend": True,
        "feishu_text": text,
        "enqueued_at": now.isoformat(),
    }
