"""Lifecycle classification for manual GTH candidate delivery and replay."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Literal, Mapping

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


_REFRESH_BLOCKERS = frozenset(
    {
        "chain_implied_target_unavailable",
        "direct_es_invalidation_unavailable",
        "es_basis_unavailable",
        "long_leg_quote_in_future",
        "long_leg_quote_stale",
        "long_leg_quote_unavailable",
        "long_leg_transport_in_future",
        "long_leg_transport_stale",
        "provider_entry_control_blocked",
        "quote_unavailable",
        "short_leg_quote_in_future",
        "short_leg_quote_stale",
        "short_leg_quote_unavailable",
        "short_leg_transport_in_future",
        "short_leg_transport_stale",
        "spread_entry_limit_invalid",
        "spread_leg_nbbo_invalid",
        "spread_leg_provider_mismatch",
        "spread_leg_provider_unavailable",
        "spread_leg_source_time_unavailable",
        "spread_leg_source_timestamp_skew",
        "spread_leg_transport_time_unavailable",
        "spread_leg_transport_timestamp_skew",
        "spread_net_debit_invalid",
        "spread_net_nbbo_invalid",
        "spread_provider_not_ibkr",
        "spread_reward_risk_unavailable",
        "trend_anchor_geometry_unavailable",
        "trigger_level_unavailable",
    }
)

SourceLifecycleClass = Literal[
    "identified",
    "explicit_absence",
    "transient_absence",
]

_ACTIVE_PLAN_FIELDS = (
    "candidate_id",
    "direction",
    "path_kind",
    "invalidation_spx",
    "target_spx",
    "exit_at",
)

_EXPLICIT_SOURCE_ABSENCE_REASONS = frozenset(
    {
        "level_source_expired",
        "level_source_expiry_unavailable",
        "level_source_formal_signal_absent",
        "level_source_invalidated",
        "level_source_not_confirmed",
        "level_source_quality_invalid",
        "trend_transition_session_mismatch",
    }
)

_GLOBAL_SOURCE_LIFECYCLE_END_REASONS = frozenset(
    {
        "trend_transition_session_mismatch",
    }
)


def classify_source_lifecycle(
    candidate: Mapping[str, object],
) -> SourceLifecycleClass:
    """Distinguish a source tombstone from a temporarily incomplete frame."""

    if candidate.get("source_signal_id"):
        return "identified"
    reasons = {str(item) for item in candidate.get("block_reasons") or () if item}
    if reasons & _EXPLICIT_SOURCE_ABSENCE_REASONS:
        return "explicit_absence"
    return "transient_absence"


def mark_refresh_pending(candidate: Mapping[str, object]) -> dict[str, object]:
    result = dict(candidate)
    reasons = {str(item) for item in result.get("block_reasons") or () if item}
    if (
        result.get("status") == "blocked"
        and result.get("source_signal_id")
        and reasons
        and reasons <= _REFRESH_BLOCKERS
    ):
        result.update(
            {
                "status": "refresh_pending",
                "manual_action_eligible": False,
                "signal_absence_reason": "market_data_refresh_pending",
            }
        )
    return result


def active_manual_plan_snapshot(
    candidate: Mapping[str, object],
    *,
    activated_at: object,
) -> dict[str, object]:
    return {
        **{key: candidate.get(key) for key in _ACTIVE_PLAN_FIELDS},
        "activated_at": activated_at,
    }


def recover_active_manual_plan(
    state: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Close the accepted-outbox/active-plan crash window on the next tick."""

    saved = state.get("active_manual_plan")
    if isinstance(saved, Mapping) and saved:
        return dict(saved)
    previous = state.get("last_candidate")
    if not isinstance(previous, Mapping):
        return {}
    candidate_id = str(previous.get("candidate_id") or "")
    event_id = f"{candidate_id}:ready"
    accepted = {
        str(item)
        for key in ("accepted_notification_event_ids", "notified_event_ids")
        for item in state.get(key) or ()
        if item
    }
    settled = {str(item) for item in state.get("settled_notification_event_ids") or () if item}
    if not candidate_id or event_id not in accepted or event_id in settled:
        return {}
    return active_manual_plan_snapshot(
        previous,
        activated_at=state.get("updated_at") or now.isoformat(),
    )


def cancellation_scope(
    candidate: Mapping[str, object],
    lifecycle_events: Mapping[str, str],
    *,
    now: datetime,
) -> set[str]:
    """Cancel only invalidated source lifecycles; transient states preserve them."""

    status = str(candidate.get("status") or "")
    reasons = {str(item) for item in candidate.get("block_reasons") or () if item}
    if status in {"manual_ready", "refresh_pending"}:
        return set()
    if (
        not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now)
        or reasons & _GLOBAL_SOURCE_LIFECYCLE_END_REASONS
    ):
        return set(lifecycle_events)
    if "opposite_signal_conflicts_with_active_plan" in reasons:
        return set()
    source_id = str(candidate.get("source_signal_id") or "")
    if source_id:
        return {
            event_id
            for event_id, lifecycle_source_id in lifecycle_events.items()
            if lifecycle_source_id == source_id
        }
    if classify_source_lifecycle(candidate) == "explicit_absence":
        tombstone_id = str(candidate.get("source_tombstone_id") or "")
        if tombstone_id:
            return {
                event_id
                for event_id, lifecycle_source_id in lifecycle_events.items()
                if lifecycle_source_id == tombstone_id
            }
    return set()


def seed_replayed_candidate_ids(
    state: Mapping[str, object],
    *,
    replay_journal_path: Path,
) -> set[str]:
    """Upgrade legacy state without replaying an already-recorded READY candidate."""

    replayed = {
        str(item) for item in state.get("replayed_candidate_ids") or () if item
    }
    if "replayed_candidate_ids" in state:
        return replayed

    previous = state.get("last_candidate")
    if isinstance(previous, Mapping) and previous.get("status") == "manual_ready":
        candidate_id = str(previous.get("candidate_id") or "")
        if candidate_id:
            replayed.add(candidate_id)

    try:
        rows = Path(replay_journal_path).read_text(encoding="utf-8").splitlines()
    except OSError:
        return replayed
    for row in rows:
        try:
            record = json.loads(row)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, Mapping) or record.get("status") != "manual_ready":
            continue
        candidate_id = str(record.get("candidate_id") or "")
        if candidate_id:
            replayed.add(candidate_id)
    return replayed


def unreplayed_candidate(
    candidate: Mapping[str, object],
    replayed_candidate_ids: set[str],
) -> str | None:
    if candidate.get("status") != "manual_ready":
        return None
    candidate_id = str(candidate.get("candidate_id") or "")
    if not candidate_id or candidate_id in replayed_candidate_ids:
        return None
    return candidate_id
